# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "deepmerge",
#     "duckdb",
#     "loguru",
#     "sqlfluff",
#     "streamlit",
#     "streamlit-code-editor",
# ]
# ///

import json
import os
import re

import duckdb
import sqlfluff
import streamlit as st
from code_editor import code_editor
from deepmerge import always_merger
from loguru import logger

DUCKDB_FILE_EXTENSIONS = [".duckdb", ".ddb", ".db"]

# Constants
SCHEMA_SIDE_EFFECT_DDLS = [
    "alter",
    "checkpoint",
    "copy",
    "create",
    "delete",
    "drop",
    "import",
    "use",
]

CUSTOM_CSS = """
    <style>
    .stAppDeployButton {
        visibility: hidden;
    }
    .block-container {
        padding-top: 3rem;
        padding-bottom: 0rem;
        padding-left: 5rem;
        padding-right: 5rem;
    }
    div[data-testid="stJson"] div.react-json-view {
        font-size: 0.75em !important;
    }
    </style>
"""

# SVG data in base64 format
SVG_ICON = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxZW0iIGhlaWdodD0iMWVtIiB2aWV3Qm94PSIwIDAgMjQgMjQiPjxwYXRoIGZpbGw9ImN1cnJlbnRDb2xvciIgZD0iTTIwIDEzLjA5VjdjMC0yLjIxLTMuNTgtNC04LTRTNCA0Ljc5IDQgN3YxMGMwIDIuMjEgMy41OSA0IDggNGMuNDYgMCAuOSAwIDEuMzMtLjA2QTYgNiAwIDAgMSAxMyAxOXYtLjA1Yy0uMzIuMDUtLjY1LjA1LTEgLjA1Yy0zLjg3IDAtNi0xLjUtNi0ydi0yLjIzYzEuNjEuNzggMy43MiAxLjIzIDYgMS4yM2MuNjUgMCAxLjI3LS4wNCAxLjg4LS4xMUE1Ljk5IDUuOTkgMCAwIDEgMTkgMTNjLjM0IDAgLjY3LjA0IDEgLjA5bS0yLS42NGMtMS4zLjk1LTMuNTggMS41NS02IDEuNTVzLTQuNy0uNi02LTEuNTVWOS42NGMxLjQ3LjgzIDMuNjEgMS4zNiA2IDEuMzZzNC41My0uNTMgNi0xLjM2ek0xMiA5QzguMTMgOSA2IDcuNSA2IDdzMi4xMy0yIDYtMnM2IDEuNSA2IDJzLTIuMTMgMi02IDJtMTEgOXYyaC0zdjNoLTJ2LTNoLTN2LTJoM3YtM2gydjN6Ii8+PC9zdmc+"



EXAMPLE_QUERIES = {
    "Empty": "",
    "parquet file": """
CREATE TABLE IF NOT EXISTS price as FROM read_parquet('https://duckdb.org/data/prices.parquet');
SELECT * FROM price;
""",
    "Excel": """
INSTALL httpfs;
LOAD httpfs;
INSTALL spatial;
LOAD spatial;
CREATE TABLE movies AS
  FROM st_read('https://raw.githubusercontent.com/duckdb/duckdb-rs/main/crates/duckdb/examples/Movies_Social_metadata.xlsx');
SELECT * FROM movies;
""",
    "S3": """
-- see https://duckdb.org/docs/extensions/httpfs/s3api.html
INSTALL httpfs;
LOAD httpfs;
CREATE TABLE train_service AS FROM 's3://duckdb-blobs/train_services.parquet';
-- CREATE SECRET (
--     TYPE S3,
--     KEY_ID 'AKIAIOSFODNN7EXAMPLE',
--     SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
--     REGION 'us-east-1'
-- );
-- CREATE TABLE s3_data AS SELECT *
-- FROM 's3://my-bucket/file.parquet';
""",
    "DuckDB database over https": """
-- see https://duckdb.org/docs/guides/network_cloud_storage/duckdb_over_https_or_s3.html
INSTALL httpfs;
LOAD httpfs;
ATTACH 'https://blobs.duckdb.org/databases/stations.duckdb' AS stations_db;
SELECT count(*) AS num_stations FROM stations_db.stations;
""",
    "PostgreSQL": """
-- see https://duckdb.org/docs/extensions/postgres.html
INSTALL postgres;
LOAD postgres;
SELECT * FROM postgres_scan('host=localhost port=5432 dbname=mydb user=u password=p', 'public', 'mytable');
-- or set schema into duckdb
-- ATTACH 'dbname=mydb user=u password=p host=localhost' AS db (TYPE POSTGRES, SCHEMA 'public');
""",
    "BigQuery": """
-- https://community-extensions.duckdb.org/extensions/bigquery.html
-- https://github.com/hafenkran/duckdb-bigquery
-- see open datasets: https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset&;q=public%20data
INSTALL 'bigquery' FROM community;
LOAD 'bigquery';
""",
}


editor_btns = [
    {
        "name": "Linter",
        "hasText": True,
        "alwaysOn": True,
        "feather": "Edit",
        "commands": [["response", "lint-exec"]],
        "style": {"bottom": "2.4rem", "right": "0.4rem"},
    },
    {
        "name": "Run",
        "feather": "Play",
        "primary": True,
        "hasText": True,
        "alwaysOn": True,
        "showWithIcon": True,
        "commands": ["submit"],
        "style": {"bottom": "0.44rem", "right": "0.4rem"},
    },
]

INIT_SESSION_VALUE = {
    "database_connect": False,
    "database_schema": {},
    "current_database": "",
    "code_text": "SELECT 1+1;",
    "code_id": "",
    "response_dict": {},
    "last_executed_query": "",
    "last_executed_results": [],
    "code_linter_executed": False,
    "code_submit_executed": False,
}


## session state
def initialize_session_state() -> None:
    for key in INIT_SESSION_VALUE:
        if key not in st.session_state:
            st.session_state[key] = INIT_SESSION_VALUE[key]


def force_reset_session_state(keys: list[str]) -> None:
    for key in keys:
        st.session_state[key] = INIT_SESSION_VALUE[key]


def use_state(key: str, default_value=None):
    return st.session_state.get(key, default_value)


def set_state(key: str, value) -> None:
    st.session_state[key] = value


## helper
@st.cache_data
def remove_comment(input_str: str) -> str:
    # Remove single-line comments
    no_single_comments = re.sub(r"--.*$", "", input_str, flags=re.MULTILINE)

    # Remove multi-line comments
    no_comments = re.sub(r"/\*[\s\S]*?\*/", "", no_single_comments)

    # Remove empty lines and leading/trailing whitespace
    cleaned_lines = [line.strip() for line in no_comments.split("\n") if line.strip()]
    return "\n".join(cleaned_lines)


@st.cache_data
def lint_fix_sql_code(code: str) -> str:
    return sqlfluff.fix(code, dialect="duckdb")


def is_allowed_database_name(dbname: str) -> bool:
    return any(dbname.endswith(ext) for ext in DUCKDB_FILE_EXTENSIONS)


def is_database_name_renewed(dbname: str) -> bool:
    current_database = use_state("current_database")
    return dbname != current_database


## SQL Execution
def execute_queries(
    con: duckdb.DuckDBPyConnection, clean_text: str
) -> list[duckdb.DuckDBPyRelation | None]:
    queries = clean_text.split(";")
    try:
        con.execute("BEGIN;")
        results = []
        for query in queries:
            # query might be '\n", then pass exection here
            if q := query.strip():
                res = con.sql(q)
                results.append(res)
        con.execute("COMMIT;")
        return results
    except Exception as e:
        con.execute("ROLLBACK;")
        logger.error(f"sql execute error: {e}")
        raise RuntimeError(f"Failed to execute queries: {e}") from e


## code editor
def update_editor_session_state(response_dict):
    set_state("code_text", response_dict["text"])
    set_state("code_id", response_dict["id"])
    set_state("response_dict", response_dict)


def on_selector_change():
    selected_value = use_state("key_selectbox_example_query")
    sql = EXAMPLE_QUERIES[selected_value].strip()
    set_state("code_text", sql)


def new_code_submitted(response_dict):
    return (
        response_dict["type"] == "submit"
        and response_dict["text"]
        and response_dict["id"] != st.session_state.code_id
    ) and not st.session_state.code_submit_executed


def linter_execute(response_dict):
    return (
        response_dict["type"] == "lint-exec"
        and response_dict["text"]
        and response_dict["id"] != st.session_state.code_id
    ) and not st.session_state.code_linter_executed


## connection handling


@st.cache_resource  # Before calling cache reset, you need to explicitly close connection
def duckdb_con(database: str) -> duckdb.DuckDBPyConnection:
    try:
        con = duckdb.connect(database=database)
        logger.info(
            json.dumps({"message": {"database": database, "status": "connected"}})
        )
        return con
    except duckdb.Error as e:
        logger.error(f"Failed to connect to database {database}: {e}")
        raise


def handle_database_connection(database: str) -> tuple[str, str]:
    connection_closed_msg = ""
    connection_set_msg = ""

    if is_database_name_renewed(database):
        connection_closed_msg = close_existing_connection()
        set_state("current_database", database)

    if "con" not in st.session_state:
        connection_set_msg = create_new_connection(database)

    return connection_closed_msg, connection_set_msg


def create_new_connection(database: str) -> str:
    st.session_state.con = duckdb_con(database=database)
    force_reset_session_state(
        [
            "last_executed_query",
            "last_executed_results",
            "database_schema",
        ]
    )
    msg = f"""connection {use_state("current_database")} is set."""
    logger.info(json.dumps({"message": msg}))
    return msg


def close_existing_connection() -> str:
    if "con" in st.session_state:
        logger.info('if "con" in st.session_state:')
        st.session_state.con.close()
        duckdb_con.clear()  # explicitly need here
        del st.session_state.con
        msg = f"""connection {use_state("current_database")} was closed"""
        logger.info(json.dumps({"message": msg}))
    else:
        msg = "connection not exists (first session)"

    force_reset_session_state(
        ["database_schema", "last_executed_query", "last_executed_results"]
    )
    return msg


## schema info
def update_duckdb_schema(con: duckdb.DuckDBPyConnection) -> None:
    schemas = get_schemas(con)
    schema = {}
    for s in schemas:
        schema = always_merger.merge(schema, s)
    set_state("database_schema", schema)


def get_schemas(
    con: duckdb.DuckDBPyConnection,
) -> list[str : dict[str : dict[str, dict[str, str]]]]:
    try:
        tables = con.sql(
            "SELECT table_catalog, table_schema, table_name FROM information_schema.tables;"
        )
        structure = []
        for catalog, _schema, table in tables.fetchall():
            columns = con.sql(f"PRAGMA table_info('{catalog}.{_schema}.{table}');")
            schema_info = {
                catalog: {
                    _schema: {table: {col[1]: col[2] for col in columns.fetchall()}}
                }
            }
            structure.append(schema_info)
        return structure
    except Exception as e:
        raise RuntimeError(f"error in get_schemas(): {e}") from e


def main() -> None:
    # Streamlit method `st.` should be written within main()

    st.set_page_config(layout="wide", page_title="Meager SQL Editor", page_icon=SVG_ICON)
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    initialize_session_state()

    # handle connection
    with st.sidebar:
        with st.form("my_form", clear_on_submit=False):
            st.write("Input your local database file.")
            extensions = ", ".join([f"**{x}**" for x in DUCKDB_FILE_EXTENSIONS])
            new_database = st.text_input(
                label=f"Note: file name should have one of these extensions: {extensions}",
                value=use_state("current_database"),
            )
            submit_button = st.form_submit_button("Enter")

        if new_database and not is_allowed_database_name(new_database):
            st.error(
                "Invalid file name. Please provide a valid file name with the correct extension."
            )
        if (
            new_database
            and is_allowed_database_name(new_database)
            and not os.path.exists(new_database)
        ):
            if submit_button:
                st.info(
                    f"File '{new_database}' does not exist. Do you want to create it?"
                )

                col1, col2 = st.columns(2)
                with col1:
                    if st.button(
                        "Yes, create file",
                        on_click=set_state,
                        args=("database_connect", True),
                    ):
                        pass  # never here because submit_button == False
                with col2:
                    if st.button(
                        "No, type another file",
                        on_click=set_state,
                        args=("database_connect", False),
                    ):
                        pass  # never here because submit_button == False

        database = (
            new_database
            if is_allowed_database_name(new_database)
            and (os.path.exists(new_database) or use_state("database_connect"))
            else ""
        )

        if not database:
            st.stop()

        connection_closed_msg, connection_set_msg = handle_database_connection(database)
        if connection_closed_msg:
            st.info(connection_closed_msg)
        if connection_set_msg:
            st.info(connection_set_msg)

    con = st.session_state.con
    update_duckdb_schema(con)

    st.selectbox(
        label="show example query",
        options=EXAMPLE_QUERIES.keys(),
        key="key_selectbox_example_query",
        on_change=on_selector_change,
    )

    st.write("Run: Ctrl + Enter")
    response_dict = code_editor(
        st.session_state.code_text,
        lang="sql",
        theme="dark",
        height=[5, 10],
        allow_reset=True,
        options={"wrap": True, "Focus": True},
        key="code",
        buttons=editor_btns,
    )

    logger.info(f"{response_dict=}")
    if linter_execute(response_dict):
        st.write("linter executed!")
        set_state("code_linter_executed", True)
        text = response_dict.get("text", "")
        try:
            clean_text = lint_fix_sql_code(remove_comment(text))
        except Exception:
            st.error(f"sqlfluff lint fix error: {remove_comment(text)}")
        set_state("code_text", clean_text)
        st.rerun()

    if new_code_submitted(response_dict):
        update_editor_session_state(response_dict)
        set_state("code_submit_executed", True)
        st.rerun()  # Required! update code_editor input

    with st.spinner("Running sql..."):
        if st.session_state.code_submit_executed:
            try:
                code_text = use_state("code_text")
                last_executed_query = use_state("last_executed_query")
                if use_state("last_executed_query") == code_text:
                    results: list[duckdb.DuckDBPyRelation | None] = last_executed_query
                    st.info("query cache")
                else:
                    results = execute_queries(con, code_text)
                    if any(
                        (ddl in code_text.lower()) for ddl in SCHEMA_SIDE_EFFECT_DDLS
                    ):
                        update_duckdb_schema(con)
                    set_state("last_executed_query", code_text)
                    set_state("last_executed_results", results)
                    st.info("query executed")
                for i, r in enumerate(res for res in results if res):
                    if r is None:
                        st.write(f"query {i}: No result (execution success)")
                    else:
                        st.write(f"query {i}:")
                        st.dataframe(r)
            except Exception as e:
                st.error(e)

    with st.sidebar:
        st.text("table schema:")
        j = st.session_state.database_schema
        st.json(j, expanded=3)

    # Reset here
    force_reset_session_state(
        ["code_linter_executed", "code_submit_executed", "database_connect"]
    )
    logger.info("-------- tail --------")


if __name__ == "__main__":
    # https://stackoverflow.com/questions/62760929/how-can-i-run-a-streamlit-app-from-within-a-python-script
    # `sys.exit(stcli.main())` does not run code_editor
    from streamlit import runtime

    if runtime.exists():
        # `streamlit run xxx.py` pass here
        main()
    else:
        import subprocess
        import sys

        args = [
            "streamlit",
            "run",
            __file__,  # self
            "--global.developmentMode=false",
            "--browser.gatherUsageStats=false",
        ]
        result = subprocess.run(args)
        sys.exit(result.returncode)
