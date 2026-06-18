# Sparqy

[![CodeQL](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/github-code-scanning/codeql)
[![Dependabot Updates](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/dependabot/dependabot-updates)
[![Prek checks](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/pre-commit.yml)

Sparqy downloads specimen data from a StarLIMS database using [Pandas](https://pandas.pydata.org/docs), [SQLAlchemy](https://www.sqlalchemy.org/), and [PyODBC](https://learn.microsoft.com/en-us/sql/connect/python/pyodbc). Data is stored in local [Parquet files](https://parquet.apache.org/) for further analysis.

It supports **parameterized SQL queries** using SQLAlchemy's named parameter syntax (e.g., `:trial_code`).

## Installation

You'll need to install the uv package manager, instructions for which can be found [here](https://docs.astral.sh/uv/getting-started/installation/).

Once the prerequisites are installed, you can run sparqy using the following command:

```powershell
uv run .\main.py --trial_code "10KFS" --add_trial_to_path --include_dsn_in_filename
```

## Testing

You can run the test suite using pytest via uv. Ensure you set the `PYTHONPATH` to include the current directory so the tests can import the main module.

```powershell
$env:PYTHONPATH="."
uv run pytest tests.py
```

You can edit the command-line arguments as needed to customize the behavior of sparqy for your specific use case. For example, you can change the `--trial_code` argument to process a different trial or modify the `--output_dir` argument to specify a different output location for the Parquet files.

Variables can also be set in the `.env` file located in the root directory of the sparqy project. This allows you to define default values for the command-line arguments and avoid having to specify them every time you run the script.

```
# .env example

DB_HOST=starlimsdb2019.ahc.umn.edu
DB_NAME=lsprod_data
DB_PORT=1443
SQL_FILE=trial_inventory.sql
TRIAL_CODE=10KFS
OUTPUT_DIR=G:\\Shared drives\\ARDL Biorepository\\Studies
EXCLUDE_CONDITIONS=["SNR","QNSR","QNS","NSI"]
EXCLUDE_MATCODES=["100x100Box", None]
PARQUET_COMPRESSION="zstd"
DB_DRIVER="SQL Server"
```
