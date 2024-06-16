import json
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from utils import Logger

KEY, VALUE = 0, 1
DIST, NEIGH = 0, 1
T_SOURCE, T_LOOKUP = 0, 1

SOURCE = "idealista"
LOOKUP = "lookup_tables"


def flatten(row: pyspark.sql.Row) -> dict[str]:
    """Flatten a set of nested `pyspark.sql.Row` into a dictionary, discarding null values"""
    out = {}
    for k, v in row.asDict().items():
        if isinstance(v, pyspark.sql.Row):
            out |= flatten(v)
        elif v is not None:
            out[k] = v
    return out


def commit(spark: SparkSession, landing: Path, formatted: Path):
    log = Logger(formatted / SOURCE)
    loaded = log.get_log()

    with open(
        formatted / SOURCE / "schema.json", mode="r", encoding="utf-8"
    ) as handler:
        full_schema = pyspark.sql.types.StructType.fromJson(json.load(handler))
        default = {field.name: None for field in full_schema}

    idealista_files = {
        path.stem:
        # Load idealista files
        spark.read.parquet(path.as_posix())
        .rdd
        # Standardise and version idealista files
        .map(lambda x: default | flatten(x) | {"queryDate": path.stem[:10]})
        # Prevent PySpark from using queryDate with pass by reference
        .cache()
        # For all files left to load
        for path in (landing / SOURCE).glob("*_idealista")
        if path.stem not in loaded
    }

    if len(idealista_files) > 0:
        # Join lookup tables (small tables) first
        lookup = (
            spark.read.json(str(landing / LOOKUP / "rent_lookup_district.json"))
            .rdd.flatMap(lambda x: [(ne_id, x) for ne_id in x.ne_id])
            .join(
                spark.read.json(
                    str(landing / LOOKUP / "rent_lookup_neighborhood.json")
                ).rdd.keyBy(lambda x: x._id)
            )
            .map(lambda x: ((x[VALUE][DIST].di, x[VALUE][NEIGH].ne), x[VALUE]))
        )

        # Perform lookup on idealista data
        idealista = (
            # Perform union of all new files
            spark.sparkContext.union(list(idealista_files.values()))
            # Add district and neighbourhood recoinciliation
            .keyBy(lambda x: (x["district"], x["neighborhood"]))
            .join(lookup)
            .map(
                lambda x: x[VALUE][T_SOURCE]
                | {
                    "district": x[VALUE][T_LOOKUP][DIST].di_re,
                    "district_id": x[VALUE][T_LOOKUP][DIST]._id,
                    "neighborhood": x[VALUE][T_LOOKUP][NEIGH].ne_re,
                    "neighborhood_id": x[VALUE][T_LOOKUP][NEIGH]._id,
                }
            )
            # Ready to store it
            .map(lambda x: pyspark.sql.Row(**x))
            .toDF(schema=full_schema)
        )

        # Append only if file already exists
        mode = "overwrite" if len(loaded) == 0 else "append"
        idealista.write.parquet((formatted / SOURCE / "out").as_posix(), mode=mode)
        _ = [rdd.unpersist() for rdd in idealista_files.values()]
        commited = list(idealista_files.keys())
        with log.get_log_file() as log_file:
            print(*commited, sep="\n", file=log_file)
        return commited
    return []


def reset(formatted: Path):
    log = Logger(formatted / SOURCE)
    log.clear()
