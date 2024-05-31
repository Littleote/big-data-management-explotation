import os
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from utils import Logger

T_SOURCE, T_LOOKUP = 0, 1

SOURCE = "income_opendata"
LOOKUP = "lookup_tables"


def commit(spark: SparkSession, landing: Path, formatted: Path):
    log = Logger(formatted / SOURCE)
    loaded = log.get_log()

    income_file = landing / SOURCE / "income_opendata_neighborhood.json"
    lookup = landing / LOOKUP

    modification = os.path.getmtime(income_file)
    if all(map(lambda x: float(x) < modification, loaded)):
        income = spark.read.json(str(income_file)).rdd
        neighbour = spark.read.json(str(lookup / "income_lookup_neighborhood.json")).rdd
        district = spark.read.json(str(lookup / "income_lookup_district.json")).rdd

        income = (
            income
            # Standardise naming
            .map(
                lambda x: {
                    "neighborhood_name": x["neigh_name "],
                    "district_name": x["district_name"],
                    "info": x["info"],
                }
            )
            # Add district recoinciliation
            .keyBy(lambda x: x["district_name"])
            .join(district.keyBy(lambda x: x["district"]))
            .values()
            .map(
                lambda x: x[T_SOURCE]
                | {
                    "district_name": x[T_LOOKUP]["district_reconciled"],
                    "district_id": x[T_LOOKUP]["_id"],
                }
            )
            # Add neighbourhood recoinciliation
            .keyBy(lambda x: x["neighborhood_name"])
            .join(neighbour.keyBy(lambda x: x["neighborhood"]))
            .values()
            .map(
                lambda x: x[T_SOURCE]
                | {
                    "neighborhood_name": x[T_LOOKUP]["neighborhood_reconciled"],
                    "neighborhood_id": x[T_LOOKUP]["_id"],
                }
            )
            # Flatten dataset
            .map(lambda x: ({k: v for k, v in x.items() if k != "info"}, x["info"]))
            .flatMap(lambda x: [x[0] | entry.asDict() for entry in x[1]])
            # Deduplicate
            .keyBy(lambda x: (x["district_id"], x["neighborhood_id"], x["year"]))
            .groupByKey()
            .mapValues(lambda x: [y for i, y in enumerate(x) if i == 0][0])
            .values()
            # Ready to store it
            .map(lambda x: pyspark.sql.Row(**x))
            .toDF()
        )

        income.write.parquet((formatted / SOURCE / "out").as_posix(), mode="overwrite")
        with log.get_log_file() as log_file:
            print(modification, sep="\n", file=log_file)
        return [modification]
    return []
