import logging
import os
import re
from pathlib import Path
from logging import basicConfig, INFO, DEBUG, getLogger
import argparse
import pyodbc
import pandas as pd
import environ
from slugify import slugify
from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL

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
        "--db_port",
        type=int,
        default=os.getenv("DB_PORT", None),
        help="Database port (optional)",
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


def parse_sql_file(sql_file):
    """
    Read the contents of a SQL file and return as a string.

    Args:
        sql_file (Union[str, Path]): Path to the .sql file.

    Returns:
        Optional[str]: The SQL query text, or None if the file is missing.
    """
    sql_file = Path(sql_file)
    if sql_file.exists():
        with open(sql_file, "r") as file:
            return file.read()
    else:
        logger.error(f"SQL file not found: {sql_file}")
        return None


def query_to_df(connection_url, query, trial_code=None):
    """
    Execute a database query and return the results as a DataFrame.

    Args:
        connection_url (URL): SQLAlchemy connection URL.
        query (str): SQL query text to execute.
        trial_code (Optional[str]): Trial code parameter for the query.

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame.
    """
    logger.debug(f"Creating engine with URL: {connection_url}")
    engine = create_engine(connection_url)
    logger.debug(f"Engine created: {engine}")
    try:
        with engine.connect() as conn:
            # SQLAlchemy text() handles named parameters like :trial_code
            logger.debug(f"Executing query: {query}")
            params = {"trial_code": trial_code} if trial_code else {}
            logger.debug(f"Query parameters: {params}")
            result = conn.execute(text(query), params)
            logger.debug(f"Query result object: {result}")
            # Fetch all rows and create a DataFrame
            rows = result.fetchall()
            logger.debug(f"Fetched {len(rows)} rows")
            df = pd.DataFrame(rows, columns=result.keys())
            logger.debug(f"DataFrame created with shape: {df.shape}")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise
    finally:
        engine.dispose()
    return df


def flag_viable(df, exclude_conditions, exclude_matcodes):
    """
    Apply viability rules to filter or mark specimens in the DataFrame.

    Args:
        df (pd.DataFrame): Input specimen data.
        exclude_conditions (list[str]): List of RECEIVED_CONDITION or SAMPLE_CONDITION values to exclude.
        exclude_matcodes (list[str]): List of MATCODE values to exclude.

    Returns:
        pd.DataFrame: DataFrame with an added 'VIABLE' boolean column.
    """
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
        & ~df["RECEIVED_CONDITION"].isin(exclude_conditions)
        & ~df["SAMPLE_CONDITION"].isin(exclude_conditions)
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
    """
    Extract an alternative sample identifier from the COMMENTS column based on LAB_ID.

    This function searches the `COMMENTS` column for a pattern of the form
    ``LAB_ID:<digits>`` and extracts the numeric portion into a new ``SAMPLEID2``
    column. It is intended for use when sample identifiers are encoded as numeric
    lab IDs rather than in the ``SAMPLEID:...`` format handled by
    :func:`extract_sampleid`.

    Parameters
    ----------
    df : pandas.DataFrame
        A DataFrame that must contain a ``COMMENTS`` column from which the
        ``LAB_ID``-based identifier will be extracted.

    Returns
    -------
    pandas.DataFrame
        The same DataFrame with an additional ``SAMPLEID2`` column containing
        the extracted numeric lab ID values (or NaN where no match is found).
    """
    df["SAMPLEID"] = df["COMMENTS"].str.extract(r"SAMPLEID:(.*?),")
    df["SAMPLEID2"] = df["COMMENTS"].str.extract(r"LAB_ID:(\d+)")
    return df


def parquet_path(trial_code, output_dir, include_dsn_in_filename, add_trial_to_path):
    """
    Construct the final filesystem path for the output parquet file.

    Args:
        trial_code (str): The trial code used for naming.
        output_dir (Union[str, Path]): Base directory for output.
        include_dsn_in_filename (bool): Whether to append '_PROD' to the filename.
        add_trial_to_path (bool): Whether to nest the file in a trial-named subdirectory.

    Returns:
        Path: The absolute or relative Path object for the target file.
    """
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


def redact_dsn_password(dsn: str) -> str:
    """
    Redact the PWD parameter in a DSN string for safe logging.

    Args:
        dsn (str): Original ODBC connection string.

    Returns:
        str: Connection string with the password replaced by asterisks.
    """
    # Replace PWD=...; with PWD=****;
    # handles case like PWD=password123;
    return re.sub(r"(PWD=)[^;]*", r"\1****", dsn, flags=re.IGNORECASE)


def main(
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
    """
    Main orchestration function for the Sparqy data extraction process.

    This function sets up logging, parses the SQL query, connects to the database,
    fetches data, flags viability, and saves the final result to Parquet.

    Args:
        db_host (str): Database server address.
        db_name (str): SQL database name.
        db_port (int): Server port.
        db_user (Optional[str]): Database username.
        db_password (Optional[str]): Database password.
        sql_file (str): Path to the source SQL file.
        trial_code (str): Filter parameter for the query.
        output_dir (str): Destination directory for parquet output.
        add_trial_to_path (bool): Nested directory flag.
        include_dsn_in_filename (bool): Filename suffix flag.
        no_viable (bool): Flag to skip viability processing.
        exclude_conditions (list[str]): Viability filters.
        exclude_matcodes (list[str]): Matcode filters.
        parquet_compression (str): Compression algo for the output file.
        db_driver (str): Name of the ODBC driver to use.
        debug (bool): Enable verbose logging.
    """
    basicConfig(level=INFO if not debug else DEBUG)
    if not trial_code:
        logger.error("No trial code provided.")
        return
    try:
        logger.info(f"Processing trial inventory for {trial_code}...")
        query = parse_sql_file(sql_file)
        if not query:
            logger.error("Failed to parse SQL file.")
            return
        logger.debug(f"SQL Query: {query}")
        # Common MSSQL parameters to prevent timeouts or SSL issues
        query_params = {
            "driver": db_driver,
            "TrustServerCertificate": "yes",
            "Encrypt": "no",
            "timeout": "30",  # seconds
        }
        username = None
        password = None
        if db_user and db_password:
            username = db_user
            password = db_password
        else:
            query_params["Trusted_Connection"] = "yes"
        connection_url = URL.create(
            "mssql+pyodbc",
            username=username,
            password=password,
            host=db_host,
            port=db_port,
            database=db_name,
            query=query_params,
        )
        logger.info(
            f"Connecting to database '{db_name}' on host '{db_host}'"
            + (f" at port {db_port}" if db_port else " using dynamic/default port")
            + f" using driver '{db_driver}'"
        )
        # Pre-check: try a simple socket connection to verify network reachability
        import socket

        check_port = db_port or 1433  # Default to 1433 for reachability check if None
        try:
            with socket.create_connection((db_host, check_port), timeout=5):
                logger.info(f"Port {check_port} on {db_host} is reachable.")
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            if db_port:
                logger.error(
                    f"Cannot reach {db_host}:{db_port}. Check your VPN or network connection. Error: {e}"
                )
                return
            else:
                logger.warning(
                    f"Could not reach {db_host} on default port 1433. Dynamic port resolution might still work via the ODBC driver. Proceeding... (Error: {e})"
                )

        logger.debug(
            f"Connection URL: {connection_url.render_as_string(hide_password=True)}"
        )
        trial_inventory = query_to_df(connection_url, query, trial_code=trial_code)
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

        # Download inventory history
        history_sql_file = Path(sql_file).parent / "inventory_history.sql"
        if history_sql_file.exists():
            logger.info(f"Downloading history for {trial_code}...")
            history_query = parse_sql_file(history_sql_file)
            if history_query:
                history_df = query_to_df(
                    connection_url, history_query, trial_code=trial_code
                )
                # Create history filename with '_history' suffix
                history_parquet_file_name = (
                    final_parquet_file_path.stem + "_history.parquet"
                )
                history_parquet_file_path = (
                    final_parquet_file_path.parent / history_parquet_file_name
                )

                history_df.to_parquet(
                    history_parquet_file_path, compression=parquet_compression
                )
                logging.info(
                    f"{len(history_df)} history records for {trial_code} saved to {history_parquet_file_path.absolute()}."
                )
            else:
                logger.warning("History query was empty.")
        else:
            logger.warning(f"History SQL file not found: {history_sql_file}")

    except Exception as e:
        logger.error(f"Error processing trial inventory: {e}")


if __name__ == "__main__":
    args = parse_args()
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
