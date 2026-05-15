import logging
import os
import re
import tempfile
from pathlib import Path
from logging import basicConfig, INFO, DEBUG, getLogger
import argparse
import pyodbc
import polars as pl
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
PARQUET_COMPRESSION_CANDIDATES = ("zstd", "gzip")


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
        default=env("PARQUET_COMPRESSION", default=None),  # type: ignore
        help="Parquet compression type, or auto to pick the smallest gzip/zstd output",
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
    parser.add_argument(
        "--download_history",
        action="store_true",
        default=env.bool("DOWNLOAD_HISTORY", default=False),  # type: ignore
        help="Enable downloading of inventory history",
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
        pl.DataFrame: Query results as a Polars DataFrame.
    """
    logger.debug(f"Creating engine with URL: {connection_url}")
    engine = create_engine(connection_url)
    logger.debug(f"Engine created: {engine}")
    try:
        # SQLAlchemy text() handles named parameters like :trial_code
        logger.debug(f"Executing query: {query}")
        params = {"trial_code": trial_code} if trial_code else {}
        logger.debug(f"Query parameters: {params}")

        # Polars can read from SQLAlchemy engines
        df = pl.read_database(
            query=text(query),
            connection=engine.connect(),
            execute_options={"parameters": params},
        )
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
        df (pl.DataFrame): Input specimen data.
        exclude_conditions (list[str]): List of RECEIVED_CONDITION or SAMPLE_CONDITION values to exclude.
        exclude_matcodes (list[str]): List of MATCODE values to exclude.

    Returns:
        pl.DataFrame: DataFrame with an added 'VIABLE' boolean column.
    """
    logging.info(
        f"Flagging non-viable specimens based on conditions: {exclude_conditions} and matcodes: {exclude_matcodes}"
    )

    # Define conditions for non-viable specimens
    df = df.with_columns(
        VIABLE=(
            ~pl.col("MATCODE").is_in(exclude_matcodes)
            & pl.col("MATCODE").is_not_null()
            & ~pl.col("RECEIVED_CONDITION").is_in(exclude_conditions)
            & ~pl.col("SAMPLE_CONDITION").is_in(exclude_conditions)
            & pl.col("AMOUNTLEFT").is_not_null()
            & (pl.col("AMOUNTLEFT") > 0)
        )
    )

    not_viable_count = df.filter(~pl.col("VIABLE")).height
    total_count = df.height
    percent_not_viable = (
        round((not_viable_count / total_count) * 100, 2) if total_count > 0 else 0
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
    df : polars.DataFrame
        A DataFrame that must contain a ``COMMENTS`` column from which the
        ``LAB_ID``-based identifier will be extracted.

    Returns
    -------
    polars.DataFrame
        The same DataFrame with additional ``SAMPLEID`` and ``SAMPLEID2`` columns.
    """
    return df.with_columns(
        SAMPLEID=pl.col("COMMENTS").str.extract(r"SAMPLEID:(.*?),", 1),
        SAMPLEID2=pl.col("COMMENTS").str.extract(r"LAB_ID:(\d+)", 1),
    )


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


def select_smallest_parquet_compression(df, candidates=PARQUET_COMPRESSION_CANDIDATES):
    """
    Compare parquet output sizes for the given DataFrame and return the smallest codec.

    Args:
        df (pl.DataFrame): Data to probe.
        candidates (tuple[str, ...]): Compression codecs to compare.

    Returns:
        str: The codec that yields the smallest parquet file.
    """
    if not candidates:
        raise ValueError("At least one parquet compression candidate is required.")

    best_compression = candidates[0]
    best_size = None
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        for compression in candidates:
            probe_path = temp_dir_path / f"probe_{compression}.parquet"
            df.write_parquet(probe_path, compression=compression)
            size = probe_path.stat().st_size
            logger.debug(
                "Probe parquet size with %s compression: %s bytes", compression, size
            )
            if best_size is None or size < best_size:
                best_compression = compression
                best_size = size

    logger.info(
        "Selected parquet compression '%s' for smallest output size (%s bytes).",
        best_compression,
        best_size,
    )
    return best_compression


def resolve_parquet_compression(df, parquet_compression):
    """
    Resolve the parquet compression codec, auto-selecting the smallest gzip/zstd output.

    Args:
        df (pl.DataFrame): Data to write.
        parquet_compression (Optional[str]): Requested codec or auto selector.

    Returns:
        str: Compression codec to use when writing parquet.
    """
    if not parquet_compression or str(parquet_compression).strip().lower() == "auto":
        return select_smallest_parquet_compression(df)
    return parquet_compression


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
    download_history=False,
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
        # Common MSSQL parameters. Default to secure TLS settings and allow
        # explicit environment-based overrides for legacy deployments.
        encrypt = os.getenv("DB_ENCRYPT", "yes").strip().lower()
        trust_server_certificate = (
            os.getenv("DB_TRUST_SERVER_CERTIFICATE", "yes").strip().lower()
        )
        if encrypt not in {"yes", "no"}:
            raise ValueError("DB_ENCRYPT must be either 'yes' or 'no'.")
        if trust_server_certificate not in {"yes", "no"}:
            raise ValueError(
                "DB_TRUST_SERVER_CERTIFICATE must be either 'yes' or 'no'."
            )
        if encrypt == "no" or trust_server_certificate == "yes":
            logger.warning(
                "Using insecure SQL Server TLS settings: Encrypt=%s, TrustServerCertificate=%s. "
                "This should only be used for legacy environments.",
                encrypt,
                trust_server_certificate,
            )
        query_params = {
            "driver": db_driver,
            "TrustServerCertificate": trust_server_certificate,
            "Encrypt": encrypt,
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
        resolved_parquet_compression = resolve_parquet_compression(
            trial_inventory, parquet_compression
        )
        trial_inventory.write_parquet(
            final_parquet_file_path, compression=resolved_parquet_compression
        )
        logging.info(
            f"{len(trial_inventory)} {trial_code} records saved to {final_parquet_file_path.absolute()} with {resolved_parquet_compression} compression."
        )

        # Download inventory history
        if download_history:
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

                    resolved_history_compression = resolve_parquet_compression(
                        history_df, parquet_compression
                    )
                    history_df.write_parquet(
                        history_parquet_file_path,
                        compression=resolved_history_compression,
                    )
                    logging.info(
                        f"{len(history_df)} history records for {trial_code} saved to {history_parquet_file_path.absolute()} with {resolved_history_compression} compression."
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
        download_history=args.download_history,
    )
