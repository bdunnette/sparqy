import logging
import os
from pathlib import Path
from logging import basicConfig, INFO, DEBUG, getLogger
import argparse

import ibis
import pyodbc
from dotenv import load_dotenv
from slugify import slugify

logger = getLogger(__name__)


def parse_args():
    """
    Parse command line arguments.
    Returns:
        Namespace: Parsed arguments.
    """
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Process SQL file and save to parquet."
    )
    parser.add_argument(
        "--db_host",
        type=str,
        default=os.getenv("DB_HOST", "starlimsdb2019.ahc.umn.edu"),
        help="Database host",
    )
    parser.add_argument(
        "--db_name",
        type=str,
        default=os.getenv("DB_NAME", "lsprod_data"),
        help="Database name",
    )
    parser.add_argument(
        "--db_port", type=int, default=os.getenv("DB_PORT", 1433), help="Database port"
    )
    parser.add_argument(
        "--trusted_connection",
        type=bool,
        default=os.getenv("TRUSTED_CONNECTION", True),
        help="Use trusted connection",
    )
    parser.add_argument(
        "--sql_file",
        type=str,
        default=os.getenv("SQL_FILE", "trial_inventory.sql"),
        help="SQL file to execute",
    )
    parser.add_argument(
        "--trial_code",
        type=str,
        default=os.getenv("TRIAL_CODE", "COVID BRAIN"),
        help="Trial code to filter the data",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=os.getenv(
            "OUTPUT_DIR", "G:\\Shared drives\\ARDL Biorepository\\Studies"
        ),
        help="Output directory for the parquet file",
    )
    parser.add_argument(
        "--add_trial_to_path",
        action="store_true",
        default=os.getenv("ADD_TRIAL_TO_PATH", False),
        help="Add trial code to the output path",
    )
    parser.add_argument(
        "--include_dsn_in_filename",
        action="store_true",
        default=os.getenv("INCLUDE_DSN_IN_FILENAME", False),
        help="Include DSN in the filename",
    )
    parser.add_argument(
        "--no_viable",
        action="store_true",
        default=os.getenv("NO_VIABLE", False),
        help="Don't try to flag non-viable samples",
    )
    parser.add_argument(
        "--exclude_conditions",
        nargs="+",
        default=os.getenv("EXCLUDE_CONDITIONS", "SNR, QNSR, QNS, NSI").split(","),
        help="List of conditions to exclude",
    )
    parser.add_argument(
        "--exclude_matcodes",
        nargs="+",
        default=os.getenv("EXCLUDE_MATCODES", "100x100Box").split(","),
        help="List of matcodes to exclude",
    )
    parser.add_argument(
        "--parquet_compression",
        type=str,
        default=os.getenv("PARQUET_COMPRESSION", "zstd"),
        help="Parquet compression type",
    )
    parser.add_argument(
        "--db_driver",
        type=str,
        default=os.getenv("DB_DRIVER", pyodbc.drivers()[0]),
        help="Database driver",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=os.getenv("DEBUG", False),
        help="Enable debug mode",
    )

    return parser.parse_args()


def connect_mssql(db_host, db_name, db_port, trusted_connection, db_driver):
    """
    Connect to the SQL Server database.
    """
    con = ibis.mssql.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        driver=db_driver,
        trusted_connection=trusted_connection,
    )
    return con


def parse_sql_file(sql_file, trial_code):
    """
    Parse the SQL file and return the SQL query.
    """
    sql_file = Path(sql_file)
    if sql_file.exists():
        with open(sql_file, "r") as file:
            raw_sql = file.read()
            # Replace placeholders in the SQL query
            sql = raw_sql.format(
                TRIAL_CODE=trial_code,
            )
            return sql
    else:
        logger.error(f"SQL file not found: {sql_file}")
        return


def flag_viable(df, exclude_conditions, exclude_matcodes):
    # Assume all specimens are viable initially
    df["VIABLE"] = True
    # Define conditions for non-viable specimens
    df["VIABLE"] = (
        ~df["MATCODE"].isin(exclude_matcodes)
        & ~df["RECEIVEDCONDITION"].isin(exclude_conditions)
        & ~df["Sample Condition"].isin(exclude_conditions)
        & ~df["AMOUNTLEFT"].isnull()
        & ~df["AMOUNTLEFT"].le(0)
    )
    logging.info(
        f"Flagging non-viable specimens based on conditions: {exclude_conditions} and matcodes: {exclude_matcodes}"
    )
    not_viable = df[~df["VIABLE"]]
    not_viable_count = not_viable.shape[0]
    percent_not_viable = (
        round((not_viable_count / df.shape[0]) * 100, 2) if df.shape[0] > 0 else 0
    )
    logging.info(
        f"Flagged {not_viable_count} non-viable specimens ({percent_not_viable}%)."
    )
    return df


def extract_sampleid(df):
    df["SAMPLEID"] = df["Comments"].str.extract(r"SAMPLEID:(.*?),")
    return df


def parquet_path(trial_code, output_dir, include_dsn_in_filename, add_trial_to_path):
    # Add DSN to filename if specified
    if include_dsn_in_filename:
        final_parquet_file = f"{trial_code}_PROD"
    else:
        final_parquet_file = f"{trial_code}"
    final_parquet_file = (
        slugify(text=final_parquet_file, separator="_", lowercase=False) + ".parquet"
    )
    final_parquet_path = Path(output_dir)
    # Store the parquet file in a subdirectory named after the trial code if specified
    if add_trial_to_path:
        final_parquet_path = final_parquet_path / trial_code
    final_parquet_path.mkdir(parents=True, exist_ok=True)
    final_parquet_file_path = final_parquet_path / final_parquet_file
    return final_parquet_file_path


def main(
    db_host,
    db_name,
    db_port,
    trusted_connection,
    sql_file,
    trial_code,
    output_dir,
    add_trial_to_path,
    include_dsn_in_filename,
    no_viable,
    exclude_conditions,
    exclude_matcodes,
    parquet_compression,
    db_driver,
    debug=False,
):
    basicConfig(level=INFO if not debug else DEBUG)
    logger.info(f"Connecting to {db_host}:{db_port}/{db_name}")
    con = connect_mssql(
        db_host=db_host,
        db_name=db_name,
        db_port=db_port,
        trusted_connection=trusted_connection,
        db_driver=db_driver,
    )
    sql = parse_sql_file(sql_file, trial_code)
    trial_inventory = con.sql(sql).execute()
    if not no_viable:
        trial_inventory = flag_viable(
            trial_inventory, exclude_conditions, exclude_matcodes
        )
    trial_inventory = extract_sampleid(trial_inventory)

    final_parquet_file_path = parquet_path(
        trial_code=trial_code,
        output_dir=output_dir,
        include_dsn_in_filename=include_dsn_in_filename,
        add_trial_to_path=add_trial_to_path,
    )

    trial_inventory.to_parquet(final_parquet_file_path, compression=parquet_compression)
    logging.info(
        f"{len(trial_inventory)} {trial_code} records saved to {final_parquet_file_path.absolute()} with {parquet_compression} compression."
    )


if __name__ == "__main__":
    args = parse_args()
    main(
        db_host=args.db_host,
        db_name=args.db_name,
        db_port=args.db_port,
        trusted_connection=args.trusted_connection,
        sql_file=args.sql_file,
        trial_code=args.trial_code,
        output_dir=args.output_dir,
        add_trial_to_path=args.add_trial_to_path,
        include_dsn_in_filename=args.include_dsn_in_filename,
        no_viable=args.no_viable,
        exclude_conditions=args.exclude_conditions,
        exclude_matcodes=args.exclude_matcodes,
        parquet_compression=args.parquet_compression,
        db_driver=args.db_driver,
        debug=args.debug,
    )
