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
