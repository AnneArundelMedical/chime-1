#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import os, os.path, subprocess
from aamc import *

import pyodbc
from typing import *

DB_LOGIN_SERVER = "AAMCVEPCNDW01"
DB_LOGIN_DATABASE = "CovidModel"

OUTPUT_PATH = "OUTPUT_PATH.txt"
TRUNCATE_SQL = "CovidResultsTruncate.sql"
BULK_LOAD_TEMPLATE = "CovidResultsBulkLoadTemplate.sql"
BULK_LOAD_GENERATED = "CovidResultsBulkLoadGenerated.sql"

VAR_NAME_CSV_PATH = "CSV_PATH"

def _connect(server: str, database: Optional[str] = None):
    connstr = (
        "Driver={SQL Server};Server=%s;Trusted Connection=yes"
        % server)
    if database:
        connstr += ";Database=%s" % database
    return pyodbc.connect(connstr)

def get_paths():
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    with open(OUTPUT_PATH) as f:
        output_path = f.read().strip()
    full_output_path = os.path.join(script_dir, output_path).replace("/", os.sep)
    return script_dir, full_output_path

def generate_sql_from_template(script_dir, full_output_path):
    with open(BULK_LOAD_TEMPLATE) as f:
        template_sql = f.read()
    sql = template_sql.replace("${%s}" % VAR_NAME_CSV_PATH, full_output_path)
    with open(BULK_LOAD_GENERATED, "w") as f:
        f.write(sql)
    return sql

def get_truncate_sql():
    with open(TRUNCATE_SQL) as f:
        return f.read().strip()

def load_data_sqlcmd(full_output_path):
    cmd = ["sqlcmd", "-E", "-S", DB_LOGIN_SERVER, "-d", "CovidModel", "-i", full_output_path]
    subprocess.run(cmd, check=True)

def load_data_direct(truncate_sql, load_sql):
    print("Database:", DB_LOGIN_SERVER, DB_LOGIN_DATABASE)
    conn = _connect(DB_LOGIN_SERVER, DB_LOGIN_DATABASE)
    cursor = conn.cursor()
    print("TRUNCATE:")
    print(truncate_sql)
    cursor.execute(truncate_sql)
    print("LOAD:")
    print(load_sql)
    cursor.execute(load_sql)
    print("Load complete.")

def main():
    script_dir, full_output_path = get_paths()
    truncate_sql = get_truncate_sql()
    load_sql = generate_sql_from_template(script_dir, full_output_path)
    #load_data_sqlcmd(full_output_path)
    load_data_direct(truncate_sql, load_sql)

if __name__ == "__main__":
    main()
