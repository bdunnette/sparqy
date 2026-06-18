import os
import pandas as pd
import pytest
from pathlib import Path
import environ
from main import (
    extract_sampleid,
    flag_viable,
    redact_dsn_password,
    parquet_path,
    parse_sql_file,
    URL,
    query_to_df,
)

BASE_DIR = Path(__file__).resolve().parent
env = environ.Env()
if (BASE_DIR / ".env").exists():
    environ.Env.read_env(env_file=BASE_DIR / ".env")

# Local-only database tests
RUN_DB_TESTS = (
    os.getenv("GITHUB_ACTIONS") != "true"
    and bool(os.getenv("DB_HOST"))
    and bool(os.getenv("DB_NAME"))
)


def test_extract_sampleid():
    df = pd.DataFrame(
        {
            "COMMENTS": [
                "SAMPLEID:ABC-123, other text",
                "LAB_ID:999, more text",
                "Both SAMPLEID:XYZ-789, and LAB_ID:888",
                "Nothing here",
            ]
        }
    )
    result = extract_sampleid(df)
    assert result["SAMPLEID"][0] == "ABC-123"
    assert result["SAMPLEID2"][1] == "999"
    assert result["SAMPLEID"][2] == "XYZ-789"
    assert result["SAMPLEID2"][2] == "888"
    assert pd.isna(result["SAMPLEID"][3])
    assert pd.isna(result["SAMPLEID2"][3])


def test_flag_viable():
    df = pd.DataFrame(
        {
            "MATCODE": ["Box", None, "100x100Box", "Box", "Box", "Box"],
            "RECEIVED_CONDITION": ["Good", "Good", "Good", "SNR", "Good", "Good"],
            "SAMPLE_CONDITION": ["Good", "Good", "Good", "Good", "QNS", "Good"],
            "AMOUNTLEFT": [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
        }
    )
    exclude_conditions = ["SNR", "QNS"]
    exclude_matcodes = ["100x100Box"]

    result = flag_viable(df, exclude_conditions, exclude_matcodes)

    # Row 0: Valid
    assert result["VIABLE"][0]
    # Row 1: MATCODE is None
    assert not result["VIABLE"][1]
    # Row 2: Excluded MATCODE
    assert not result["VIABLE"][2]
    # Row 3: Excluded RECEIVED_CONDITION (SNR)
    assert not result["VIABLE"][3]
    # Row 4: Excluded SAMPLE_CONDITION (QNS)
    assert not result["VIABLE"][4]
    # Row 5: AMOUNTLEFT is 0
    assert not result["VIABLE"][5]


def test_redact_dsn_password():
    dsn = "Driver={ODBC Driver 17 for SQL Server};Server=myServer;Database=myDB;UID=myUser;PWD=secretPassword123;"
    redacted = redact_dsn_password(dsn)
    assert "PWD=****" in redacted
    assert "secretPassword123" not in redacted
    assert "UID=myUser" in redacted


def test_parquet_path(tmp_path):
    trial_code = "TRIAL-001"
    output_dir = tmp_path / "output"

    # Case 1: Simple path
    path = parquet_path(trial_code, output_dir, False, False)
    assert path.name == "TRIAL_001.parquet"
    assert path.parent == output_dir
    assert path.exists() is False  # Path returned, dirs created but file not yet

    # Case 2: Include DSN in filename
    path = parquet_path(trial_code, output_dir, True, False)
    assert "TRIAL_001_PROD" in path.name

    # Case 3: Add trial to path
    path = parquet_path(trial_code, output_dir, False, True)
    assert path.parent == output_dir / trial_code


def test_parse_sql_file(tmp_path):
    sql_content = "SELECT * FROM TABLE WHERE CODE = :trial_code"
    sql_file = tmp_path / "test.sql"
    sql_file.write_text(sql_content)

    query = parse_sql_file(sql_file)
    assert query == sql_content

    # Test non-existent file
    assert parse_sql_file(tmp_path / "missing.sql") is None


@pytest.mark.skipif(
    not RUN_DB_TESTS,
    reason="Skipping DB tests in GitHub Actions or if DB_HOST is missing.",
)
def test_trial_inventory_query():
    """Verify that the trial_inventory.sql query can be executed."""
    import pyodbc

    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_port_str = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    trial_code = os.getenv("TRIAL_CODE", "*TEST-SCOTT*")
    db_driver = os.getenv("DB_DRIVER")
    if not db_driver:
        available_drivers = pyodbc.drivers()
        if not available_drivers:
            pytest.skip(
                "Skipping DB tests because no ODBC drivers are installed "
                "and DB_DRIVER is not set."
            )
        db_driver = available_drivers[0]

    db_port = None
    if db_port_str is not None:
        db_port_str = db_port_str.strip()
        if db_port_str:
            db_port = int(db_port_str)
    query_params = {
        "driver": db_driver,
        "TrustServerCertificate": "yes",
        "Encrypt": "no",
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

    # Test main query
    sql_file = BASE_DIR / "trial_inventory.sql"
    query = parse_sql_file(sql_file)
    assert query is not None
    df = query_to_df(connection_url, query, trial_code=trial_code)
    assert isinstance(df, pd.DataFrame)


@pytest.mark.skipif(
    not RUN_DB_TESTS,
    reason="Skipping DB tests in GitHub Actions or if DB_HOST is missing.",
)
def test_inventory_history_query():
    """Verify that the inventory_history.sql query can be executed."""
    import pyodbc

    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    trial_code = os.getenv("TRIAL_CODE", "*TEST-SCOTT*")
    db_driver = os.getenv("DB_DRIVER")
    if not db_driver:
        available_drivers = pyodbc.drivers()
        if not available_drivers:
            pytest.skip(
                "Skipping DB tests because no ODBC drivers are installed "
                "and DB_DRIVER is not set."
            )
        db_driver = available_drivers[0]

    query_params = {
        "driver": db_driver,
        "TrustServerCertificate": "yes",
        "Encrypt": "no",
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

    # Test history query
    sql_file = BASE_DIR / "inventory_history.sql"
    query = parse_sql_file(sql_file)
    assert query is not None
    df = query_to_df(connection_url, query, trial_code=trial_code)
    assert isinstance(df, pd.DataFrame)
