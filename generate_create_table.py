import argparse
import json
import typing

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
        action="append",
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
        "columns": get_columns(jsonschema),
        "location": location,
        "partition_columns": ", ".join(partition_columns),
        "format": table_format,
    }

    sql = sql.format(**format_map)
    return sql


def get_columns(jsonschema: typing.Dict[str, typing.Any]) -> str:
    columns_sql = ""
    first = True
    for field in jsonschema.get(PROPERTIES_FIELD):
        # all columns except first have trailing comma and newline indent
        if not first:
            columns_sql += ",\n    "

        columns_sql += "{}".format(
            get_column_sql(field, jsonschema.get(PROPERTIES_FIELD).get(field))
        )
        first = False
    return columns_sql


def get_column_sql(field: str, jsonschema: typing.Dict[str, typing.Any]) -> str:
    # create key to lookup matching SQL type
    key = (jsonschema.get(TYPE_FIELD), jsonschema.get(FORMAT_FIELD, None))

    sql_type = JSON_TYPE_TO_SQL_TYPE.get(key)
    # TODO: for container SQL types (row and array) we need some more processing
    return '"{field}" {sql_type}'.format(field=field, sql_type=sql_type)


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
