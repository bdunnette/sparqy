import logging
import os
from pathlib import Path
from logging import basicConfig, INFO, DEBUG, getLogger
import argparse
import asyncio

import pyodbc
import aioodbc
import pandas as pd
import environ
from slugify import slugify

logger = getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent
env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False)
)
environ.Env.read_env(env_file=BASE_DIR / ".env")


def parse_args():
    """
    Parse command line arguments.
    Returns:
        Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Process SQL file and save to parquet."
    )
    parser.add_argument(
        "--db_host",
        type=str,
        default=os.getenv("DB_HOST", None),
        help="Database host",
    )
    parser.add_argument(
        "--db_name",
        type=str,
        default=os.getenv("DB_NAME", None),
        help="Database name",
    )
    parser.add_argument(
        "--db_port", type=int, default=os.getenv("DB_PORT", 1443), help="Database port"
    )
    parser.add_argument(
        "--db_user",
        type=str,
        default=os.getenv("DB_USER", None),
        help="Database username",
    )
    parser.add_argument(
        "--db_password",
        type=str,
        default=os.getenv("DB_PASSWORD", None),
        help="Database password",
    )
    parser.add_argument(
        "--trusted_connection",
        type=bool,
        default=env.bool("TRUSTED_CONNECTION", default=True),  # type: ignore
        help="Use trusted connection",
    )
    parser.add_argument(
        "--sql_file",
        type=str,
        default=env("SQL_FILE", default="trial_inventory.sql"),  # type: ignore
        help="SQL file to execute",
    )
    parser.add_argument(
        "--trial_code",
        type=str,
        default=env("TRIAL_CODE", default=None),  # type: ignore
        help="Trial code to filter the data",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=env("OUTPUT_DIR", default=Path.home() / "sparqy" / "output"),  # type: ignore
        help="Output directory for the parquet file",
    )
    parser.add_argument(
        "--add_trial_to_path",
        action="store_true",
        default=env.bool("ADD_TRIAL_TO_PATH", False),  # type: ignore
        help="Add trial code to the output path",
    )
    parser.add_argument(
        "--include_dsn_in_filename",
        action="store_true",
        default=env.bool("INCLUDE_DSN_IN_FILENAME", False),  # type: ignore
        help="Include DSN in the filename",
    )
    parser.add_argument(
        "--no_viable",
        action="store_true",
        default=env.bool("NO_VIABLE", False),  # type: ignore
        help="Don't try to flag non-viable samples",
    )
    parser.add_argument(
        "--exclude_conditions",
        nargs="+",
        default=env.list("EXCLUDE_CONDITIONS", default=["SNR", "QNSR", "QNS", "NSI"]),  # type: ignore
        help="List of conditions to exclude",
    )
    parser.add_argument(
        "--exclude_matcodes",
        nargs="+",
        default=env.list("EXCLUDE_MATCODES", default=["100x100Box", None]),  # type: ignore
        help="List of matcodes to exclude",
    )
    parser.add_argument(
        "--parquet_compression",
        type=str,
        default=env("PARQUET_COMPRESSION", default="zstd"),  # type: ignore
        help="Parquet compression type",
    )
    parser.add_argument(
        "--db_driver",
        type=str,
        default=env("DB_DRIVER", default=pyodbc.drivers()[0]),  # type: ignore
        help="Database driver",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=env.bool("DEBUG", default=False),  # type: ignore
        help="Enable debug mode",
    )

    return parser.parse_args()


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


async def query_to_df(dsn, query):
    async with aioodbc.create_pool(dsn=dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                rows = await cur.fetchall()
                df = pd.DataFrame.from_records(
                    rows, columns=[desc[0] for desc in cur.description]
                )
    return df


def flag_viable(df, exclude_conditions, exclude_matcodes):
    logging.info(
        f"Flagging non-viable specimens based on conditions: {exclude_conditions} and matcodes: {exclude_matcodes}"
    )
    # Assume all specimens are viable initially
    df["VIABLE"] = True
    # Define conditions for non-viable specimens
    df["VIABLE"] = (
        ~df["MATCODE"].isin(exclude_matcodes)
        # NA MATCODE indicates specimen is not allocated to storage box
        & ~df["MATCODE"].isna()
        & ~df["RECEIVEDCONDITION"].isin(exclude_conditions)
        & ~df["Sample Condition"].isin(exclude_conditions)
        & ~df["AMOUNTLEFT"].isnull()
        & ~df["AMOUNTLEFT"].le(0)
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


async def main(
    db_host,
    db_name,
    db_port,
    db_user,
    db_password,
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
    if not trial_code:
        logger.error("No trial code provided.")
        return
    try:
        logger.info(f"Processing trial inventory for {trial_code}...")
        query = parse_sql_file(sql_file, trial_code)
        if not query:
            logger.error("Failed to parse SQL file.")
            return
        logger.debug(f"SQL Query: {query}")
        dsn = f"Driver={db_driver};SERVER={db_host};DATABASE={db_name};"
        if db_user and db_password:
            dsn += f"UID={db_user};PWD={db_password};"
        else:
            dsn += "Trusted_Connection=yes;"
        logger.info(f"Connecting to {dsn}")
        trial_inventory = await query_to_df(dsn, query)
        trial_inventory = extract_sampleid(trial_inventory)
        if not no_viable:
            trial_inventory = flag_viable(
                trial_inventory, exclude_conditions, exclude_matcodes
            )
        final_parquet_file_path = parquet_path(
            trial_code=trial_code,
            output_dir=output_dir,
            include_dsn_in_filename=include_dsn_in_filename,
            add_trial_to_path=add_trial_to_path,
        )
        trial_inventory.to_parquet(
            final_parquet_file_path, compression=parquet_compression
        )
        logging.info(
            f"{len(trial_inventory)} {trial_code} records saved to {final_parquet_file_path.absolute()} with {parquet_compression} compression."
        )
    except Exception as e:
        logger.error(f"Error processing trial inventory: {e}")


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        main(
            db_host=args.db_host,
            db_name=args.db_name,
            db_port=args.db_port,
            db_user=args.db_user,
            db_password=args.db_password,
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
    )
