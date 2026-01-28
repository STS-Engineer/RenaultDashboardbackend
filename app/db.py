import os
import psycopg2


def get_conn():
    host = "avo-adb-001.postgres.database.azure.com"
    dbname = "renault_tests"
    user = "adminavo"
    password = "$#fKcdXPg4@ue8AW"  # no default on purpose
    port =5432

    if password is None or password == "":
        raise RuntimeError(
            "DB_PASS is not set. Set DB_HOST/DB_NAME/DB_USER/DB_PASS (and optional DB_PORT) "
            "before starting FastAPI."
        )

    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
    )
def get_conn_bt():
    """
    New DB connection (BT DB). Use env vars; don't hardcode secrets.
    Required env vars:
    BT_DB_HOST, BT_DB_NAME, BT_DB_USER, BT_DB_PASS, BT_DB_PORT(optional)
    """
    host ="avo-adb-002.postgres.database.azure.com"
    dbname ="cyclame_test1"
    user ="administrationSTS"
    password ="St$@0987"
    port =5432
    
    
    if not host or not dbname or not user or not password:
        raise RuntimeError("Missing BT DB env vars: BT_DB_HOST/NAME/USER/PASS (and optional PORT).")
    
    
    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        sslmode="require",
    )
