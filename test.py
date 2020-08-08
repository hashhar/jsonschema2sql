#!/usr/bin/env python3

import jsonschema2sql

jsonschema = jsonschema2sql.load_schema("test.json")
sql = jsonschema2sql.generate_create_table(
    "test_table", "default", "s3://some-bucket/", ["ad"], "PARQUET", False, jsonschema
)

expected_sql = """CREATE TABLE "default"."test_table" (
    "string_col" varchar,
    "datetime_col" timestamp,
    "datetime_string_col" varchar,
    "date_col" date,
    "date_string_col" varchar,
    "time_col" time,
    "time_string_col" varchar,
    "decimal_string_col" decimal(10, 2),
    "double_col" double,
    "double_double_col" double,
    "float_col" float,
    "decimal_col" decimal(5, 3),
    "action_date" bigint,
    "boolean_col" boolean,
    "array_col" array(varchar),
    "array_object_col" array(ROW("string_col" varchar, "datetime_col" timestamp)),
    "object_col" ROW("string_col" varchar, "integer_col" bigint),
    "ad" varchar
) WITH (
    external_location = 's3://some-bucket/',
    partitioned_by = ARRAY['ad'],
    format = 'PARQUET'
)"""

assert sql == expected_sql
print("** TESTS PASS! **")
