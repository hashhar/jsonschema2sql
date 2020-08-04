import argparse
import json
import typing


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
    return ""


if __name__ == "__main__":
    args = parse_args()

    jsonschema = load_schema(args.jsonschema)
    print(args.partition_columns)
    sql = generate_create_table(
        args.table,
        args.schema,
        args.location,
        args.partition_columns,
        args.table_format,
        jsonschema,
    )
    print(sql)
