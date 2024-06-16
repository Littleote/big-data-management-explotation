import importlib
from pathlib import Path

from pyspark.sql import SparkSession


def commit(*pipes: str):
    """Run pipelines (by default all defined pipelines) from landing to formatted"""
    spark: SparkSession = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    landing = Path("landing/persistent").absolute()
    formatted = Path("formatted").absolute()
    if len(pipes) == 0:
        pipes = [
            dir_.stem
            for dir_ in formatted.iterdir()
            if dir_.is_dir() and (dir_ / "pipeline.py").exists()
        ]
    for pipe in pipes:
        try:
            pipeline = importlib.import_module(f"formatted.{pipe}.pipeline")
            print(f"Excecuting {pipe}'s pipeline...")
            new_files = pipeline.commit(spark, landing, formatted)
            print(
                f"Committed {len(new_files)} new file{'' if len(new_files) == 1 else 's'} of pipeline {pipe}"
            )
        except ModuleNotFoundError:
            print(f"Failed to load {pipe}'s pipeline")
        except Exception as e:
            print(f"Failed to run pipeline {pipe} due to: {e}")


def reset(*pipes: str):
    """Reset pipelines (by default all defined pipelines) in formatted"""
    formatted = Path("formatted").absolute()
    if len(pipes) == 0:
        pipes = [
            dir_.stem
            for dir_ in formatted.iterdir()
            if dir_.is_dir() and (dir_ / "pipeline.py").exists()
        ]
    print("Reseting...")
    for pipe in pipes:
        try:
            pipeline = importlib.import_module(f"formatted.{pipe}.pipeline")
            pipeline.reset(formatted)
            print(f"Reset pipeline {pipe}")
        except ModuleNotFoundError:
            print(f"Failed to load {pipe}'s pipeline")
        except Exception as e:
            print(f"Failed to reset pipeline {pipe} due to: {e}")
