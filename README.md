# Big Data Management System

## Running

### Enviroment

This program uses PySpark, this means that the minimum required version of python is 3.8 and that the user has to have set up SPARK on it's computer. (Guide on [Linux](https://medium.com/@agusmahari/pyspark-step-by-step-guide-to-installing-pyspark-on-linux-bb8af96ea5e8), [Windows](https://medium.com/@deepaksrawat1906/a-step-by-step-guide-to-installing-pyspark-on-windows-3589f0139a30), [macOS](https://medium.com/@jpurrutia95/install-and-set-up-pyspark-in-5-minutes-m1-mac-eb415fe623f3))

To install all required dependecies run the following:

```bash
pip install -r requirements.txt
```

or if using conda, install them through conda forge:

```bash
conda install -c conda-forge -r requirements-conda.txt
```

### Executing

To run the program, launch the main entry `run.py` point with the desired options: retrive, commit (landing $\rightarrow$ formatted), extract (formatted $\rightarrow$ exploitation). The specifics options of the commands are the following:

- retrive: To show the orchestrator could work along side the one from the previous project.
- commit: To commit changes or additions in the persistent landing to the formatted zone.
  - pipe: To only run specific pipelines use the pipe option followed by the corresponding pipes separated by comma (e.g. `--pipe {Pipe1},{Pipe2}`).
  - reset: To discard the current version of the files in the formatted zone, use the reset option (e.g. `--reset`).
- extract: To apply all the necessary transformation to go from some of the files of the formatted zone to the appropiate table in the exploitation.
  - pipe: To only run specific pipelines use the pipe option followed by the corresponding pipes separated by comma (e.g. `--pipe {Pipe1},{Pipe2}`).
  - reset: To discard the current table, use the reset option (e.g. `--reset`).

### Examples

```bash
# Run all pipelines in the formatted
python run.py commit

# Force rexecution of padro_opendata pipeline
python run.py commit --reset --pipe padro_opendata

# Generate the exploitation tables of neighbourhood and sales
python run.py extract --pipe neighbourhood,sales

# Force rexecution of all explotation pipelines (needs confirmation)
python run.py extract --reset
```
