#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

from aamc import *

OUTPUT_PATH = "OUTPUT_PATH.txt"
BULK_LOAD_TEMPLATE = "CovidResultsBulkLoadTemplate.sql"
BULK_LOAD_GENERATED = "CovidResultsBulkLoadGenerated.sql"

VAR_NAME_CSV_PATH = "CSV_PATH"

def main():
    with open(OUTPUT_PATH) as f:
        output_path = f.read()
    with open(BULK_LOAD_TEMPLATE) as f:
        template_sql = f.read()
    sql = template_sql.replace("${%s}" % VAR_NAME_CSV_PATH, output_path)
    with open(BULK_LOAD_GENERATED, "w") as f:
        f.write(sql)

if __name__ == "__main__":
    main()
