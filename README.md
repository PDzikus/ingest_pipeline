## Installation instruction
1) clone repository, install virtual environment
2) install poetry: ```pip install poetry```
3) download dependencies: ```poetry install```

In case you want to develop it and use current dev setup, install and configure pre-commit:
```
pip install pre-commit 
pre-commit install
```
This will install all the github hooks. If you want to run all code checks and auto-formatting before you commit the code:

```
# Run all hooks against currently staged files,
# this is what pre-commit runs by default when committing:
pre-commit run

# Run all the hooks against all the files:
pre-commit run --all-files

# Run a specific hook against all staged files:
pre-commit run black
pre-commit run flake8
pre-commit run sqlfluff
pre-commit run pylint
```

It's also useful to install sqlfluff locally:

```pip install sqlfluff```

You can use it by specifying mode (lint or fix) and file:

```sqlfluff fix main/event/templates/staging_table.sql```

## Running the pipeline

Before starting the pipeline you need to have a running postgres instance. Connection parameters should be set in ```main/configs/config_local.ini``` file. 
In ```bin``` folder you can find scripts setting up and shutting down a docker-compose instance of postgres. You can start them with ```bin/start_local.sh```.

Entry point for pipeline is ```main/ingest_data.py``` script. It requires one parameter:

--input_file <file_name>

Sample file is in data folder, so you can run it with ```python main/ingest_data.py --input_file data/sample_data.json```
