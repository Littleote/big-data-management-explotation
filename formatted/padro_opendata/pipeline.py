import os
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from utils import Logger

T_SOURCE, T_LOOKUP = 0, 1
KEY, VALUE = 0, 1
DIST, NEIGH, RECON, BARRI = 0, 1, 0, 1
T_SOURCE, T_LOOKUP = 0, 1

SOURCE = "padro_opendata"
LOOKUP = "lookup_tables"


def commit(spark: SparkSession, landing: Path, formatted: Path):
    log = Logger(formatted / SOURCE)
    loaded = log.get_log()

    padro_files = {
        path.stem:
        # Load idealista files
        spark.read.option("header", True).csv(path.as_posix()).rdd
        # For all files left to load
        for path in (landing / SOURCE).glob("*_pad_cdo_b_barri-des.csv")
        if path.stem not in loaded
    }
    lookup = landing / LOOKUP

    if len(padro_files) > 0:
        # Join lookup tables (small tables) first
        lookup = (
            spark.read.json(str(landing / LOOKUP / "income_lookup_district.json"))
            .rdd.flatMap(lambda x: [(ne_id, x) for ne_id in x.neighborhood_id])
            .join(
                spark.read.json(
                    str(landing / LOOKUP / "padro_lookup_neighborhood.json")
                ).rdd.keyBy(lambda x: x._id)
            )
            .map(
                lambda x: (
                    (x[VALUE][DIST].district, x[VALUE][NEIGH].neighborhood),
                    x[VALUE],
                )
            )
            .join(
                spark.read.json(
                    str(landing / LOOKUP / "BarcelonaCiutat_Barri.json")
                ).rdd.keyBy(lambda x: (x.nom_districte, x.nom_barri))
            )
            .map(
                lambda x: (
                    int(x[VALUE][BARRI].codi_barri),
                    x[VALUE][RECON],
                )
            )
        )

        lookup.cache()

        # Perform lookup on idealista data
        padro = (
            # Perform union of all new files
            spark.sparkContext.union(list(padro_files.values()))
            # Add district and neighbourhood recoinciliation
            .keyBy(lambda x: int(x.Codi_Barri))
            .join(lookup)
            .map(
                lambda x: (
                    int(x[VALUE][T_SOURCE].CODI_BARRI_DEST),
                    {
                        "year": int(x[VALUE][T_SOURCE].Any),
                        "moved": None
                        if x[VALUE][T_SOURCE].Valor == ".."
                        else int(x[VALUE][T_SOURCE].Valor),
                        "neighborhood_name": x[VALUE][T_LOOKUP][NEIGH][
                            "neighborhood_reconciled"
                        ],
                        "neighborhood_id": x[VALUE][T_LOOKUP][NEIGH]["_id"],
                        "district_name": x[VALUE][T_LOOKUP][DIST][
                            "district_reconciled"
                        ],
                        "district_id": x[VALUE][T_LOOKUP][DIST]["_id"],
                    },
                )
            )
            .join(lookup)
            .map(
                lambda x: x[VALUE][T_SOURCE]
                | {
                    "dest_neighborhood_name": x[VALUE][T_LOOKUP][NEIGH][
                        "neighborhood_reconciled"
                    ],
                    "dest_neighborhood_id": x[VALUE][T_LOOKUP][NEIGH]["_id"],
                    "dest_district_name": x[VALUE][T_LOOKUP][DIST][
                        "district_reconciled"
                    ],
                    "dest_district_id": x[VALUE][T_LOOKUP][DIST]["_id"],
                },
            )
            # Ready to store it
            .map(lambda x: pyspark.sql.Row(**x))
            .toDF()
        )

        # Append only if file already exists
        mode = "overwrite" if len(loaded) == 0 else "append"
        padro.write.parquet((formatted / SOURCE / "out").as_posix(), mode=mode)
        lookup.unpersist()
        commited = list(padro_files.keys())
        with log.get_log_file() as log_file:
            print(*commited, sep="\n", file=log_file)
        return commited
    return []
