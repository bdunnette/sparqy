# Sparqy

[![CodeQL](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/github-code-scanning/codeql)
[![Dependabot Updates](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/UMN-ARDL-Biorepository/sparqy/actions/workflows/dependabot/dependabot-updates)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/UMN-ARDL-Biorepository/sparqy/main.svg?badge_token=yhcp_p6pQIWA3mlafHyVew)](https://results.pre-commit.ci/latest/github/UMN-ARDL-Biorepository/sparqy/main?badge_token=yhcp_p6pQIWA3mlafHyVew)

Sparqy downloads specimen data from a StarLIMS database using [Pandas](https://pandas.pydata.org/docs) and [PyODBC](https://learn.microsoft.com/en-us/sql/connect/python/pyodbc) and stores it in a local [Parquet file](https://parquet.apache.org/) for further analysis.

## Installation

You'll need to install the uv package manager, instructions for which can be found [here](https://docs.astral.sh/uv/getting-started/installation/).

Once the prerequisites are installed, you can run sparqy using the following command:

```powershell
cd C:\Users\<your_username>\Documents\GitHub\sparqy
uv run .\main.py --trial_code "10KFS" --add_trial_to_path --include_dsn_in_filename
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
