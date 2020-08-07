#!/usr/bin/env python3

import argparse
import json
import typing

ROOT_IDENTIFIER_FIELD = "$schema"
PROPERTIES_FIELD = "properties"
TYPE_FIELD = "type"
REQUIRED_FIELD = "required"
ITEMS_FIELD = "items"
FORMAT_FIELD = "format"
SCALE_FIELD = "scale"
PRECISION_FIELD = "precision"

# dictionary with keys (json type, format) mapped to sql type
JSON_TYPE_TO_SQL_TYPE = {
    # string
    ("string", None): "varchar",
    ("string", "date-time"): "timestamp",
    ("string", "date-time-string"): "varchar",
    ("string", "date"): "date",
    ("string", "date-string"): "varchar",
    ("string", "time"): "time",
    ("string", "time-string"): "varchar",
    ("string", "decimal"): "decimal",
    # number
    ("number", None): "double",
    ("number", "double"): "double",
    ("number", "float"): "float",
    ("number", "decimal"): "decimal",
    # integer
    ("integer", None): "bigint",
    # boolean
    ("boolean", None): "boolean",
    # array
    ("array", None): "array",
    # object
    ("object", None): "row",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="Generate Create Table",
        description="Generates CREATE TABLE statments from JSON Schema",
    )

    parser.add_argument(
        "--jsonschema",
        dest="jsonschema",
        type=str,
        help="path to file containing JSON Schema",
        required=True,
    )
    parser.add_argument(
        "--table",
        dest="table",
        type=str,
        help="name of table to use in CREATE TABLE",
        required=True,
    )
    parser.add_argument(
        "--schema",
        dest="schema",
        type=str,
        help="schema in which to create table",
        required=True,
    )
    parser.add_argument(
        "--location",
        dest="location",
        type=str,
        help="location to use for table",
        required=True,
    )
    parser.add_argument(
        "--partition-columns",
        dest="partition_columns",
        type=str,
        nargs='+',
        help="list of partition columns",
        required=False,
    )
    parser.add_argument(
        "--table-format",
        dest="table_format",
        type=str,
        choices={"JSON", "PARQUET"},
        help="format of table",
        required=True,
    )

    parsed_args = parser.parse_args()
    return parsed_args


def load_schema(schema_path: str) -> typing.Dict[str, typing.Any]:
    with open(schema_path, "r") as f:
        return json.load(f)


def generate_create_table(
    table: str,
    schema: str,
    location: str,
    partition_columns: typing.List[str],
    table_format: str,
    jsonschema: typing.Dict[str, typing.Any],
) -> str:
    sql = """CREATE TABLE "{schema}"."{table}" (
    {columns}
) WITH (
    external_location = '{location}',
    partitioned_by = ARRAY[{partition_columns}],
    format = '{format}'
)"""

    # if table is unpartitioned, then we need can provide an empty list
    if partition_columns is None or len(partition_columns) == 0:
        partition_columns = []

    # enclose each partition column in quotes
    partition_columns = ["'{}'".format(col) for col in partition_columns]

    format_map = {
        "schema": schema,
        "table": table,
        "columns": get_columns(None, jsonschema),
        "location": location,
        "partition_columns": ", ".join(partition_columns),
        "format": table_format,
    }

    sql = sql.format(**format_map)
    return sql


# TODO: Refactor this method to return a list(column, column_sql). That will
# allow post-processing to mark columns as NOT NULL etc. and also make it
# easier to write tests
def get_columns(field: str, jsonschema: typing.Dict[str, typing.Any]) -> str:
    # if we are at the root of schema (ie. $schema exists) we need to start from it's properties
    if jsonschema.get(ROOT_IDENTIFIER_FIELD) is not None:
        return get_columns(field, jsonschema.get(PROPERTIES_FIELD))

    # if properties are available, we need to recurse again
    if jsonschema.get(PROPERTIES_FIELD) is not None:
        sql = get_columns(field, jsonschema.get(PROPERTIES_FIELD))
        # for nested fields we normalize the sql by removing newlines
        sql = " ".join(sql.split())

        # if we came here from within an array, field name is not needed
        if field:
            return '"{field}" ROW({inner_columns})'.format(
                field=field, inner_columns=sql
            )
        else:
            return "ROW({inner_columns})".format(inner_columns=sql)

    # if items are available, we are within an array and need to recurse again
    if jsonschema.get(ITEMS_FIELD) is not None:
        # pass None as field name to prevent it from appearing in inner column sql
        sql = get_columns(None, jsonschema.get(ITEMS_FIELD))
        # for nested fields we normalize the sql by removing newlines
        sql = " ".join(sql.split())
        return '"{field}" array({inner_columns})'.format(field=field, inner_columns=sql)

    # if we are within an object (ie. type doesn't exist), we need to iterate over all fields
    if jsonschema.get(TYPE_FIELD) is None:
        first = True
        sql = ""
        for field in jsonschema:
            if not first:
                sql += ",\n    "
            sql += get_columns(field, jsonschema[field])
            first = False

        return sql

    # if we are within a field, we can create a column
    key = (jsonschema[TYPE_FIELD], jsonschema.get(FORMAT_FIELD, None))
    sql_type = JSON_TYPE_TO_SQL_TYPE[key]

    # if decimal, we need the type arguments too
    if jsonschema.get(FORMAT_FIELD) == "decimal":
        sql_type += "({precision}, {scale})".format(
            precision=jsonschema.get(PRECISION_FIELD), scale=jsonschema.get(SCALE_FIELD)
        )

    # if we came here from within an array, field name is not needed
    if field:
        return '"{field}" {sql_type}'.format(field=field, sql_type=sql_type)
    else:
        return "{sql_type}".format(sql_type=sql_type)


if __name__ == "__main__":
    args = parse_args()

    jsonschema = load_schema(args.jsonschema)
    sql = generate_create_table(
        args.table,
        args.schema,
        args.location,
        args.partition_columns,
        args.table_format,
        jsonschema,
    )
    print(sql)
