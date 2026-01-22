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
