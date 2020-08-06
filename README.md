# JSON Schema to Presto SQL

This is a command line utility to generate `CREATE TABLE` statements compatible
with [Presto](https://prestosql.io) from a table schema provided as [JSON
Schema](https://json-schema.org).

## Getting Started

1. Grab the script.

   ```sh
   $ # Or download just the Python script :)
   $ git clone https://github.com/hashhar/jsonschema2prestosql.git
   ```

1. Run it against a schema
   ([test.json](https://github.com/hashhar/jsonschema2prestosql/raw/master/test.json)
   is a sample schema present in the root of this repo).

   ```sh
   $ python3 generate_create_table.py --jsonschema test.json \
       --table my_test_table \
       --schema my_test_db \
       --location 's3://my/test/bucket' \
       --table-format 'JSON' \
       --partition-columns year month date
   ```

   This generates a `CREATE TABLE` DDL as below

   ```sql
   CREATE TABLE "my_test_db"."my_test_table" (
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
       "object_col" ROW("string_col" varchar, "integer_col" bigint)
   ) WITH (
       external_location = 's3://my/test/bucket',
       partitioned_by = ARRAY['year', 'month', 'date'],
       format = 'JSON'
   )
   ```

For all the possible command line arguments, pass the `-h` or `--help` command
line argument.

## JSON Schema to SQL Types Mapping

The JSON Schema to SQL types mapping is defined in code via a mapping of the
JSON Schema `type` and `format` pair against the corresponding SQL type.

For the complete mapping refer to the `JSON_TYPE_TO_SQL_TYPE` dictionary in the
script.

## Contributing

I welcome you to use this tool (without any implied support contract
:stuck_out_tongue:). In case you find any issues feel free to [create one on
GitHub](https://github.com/hashhar/jsonschema2prestosql/issues/new).

For feature requests keep the following expectations in mind (I am open to
being persuaded otherwise):

* The project aims to only generate DDL for `CREATE TABLE` statements.
* The project doesn't aim to cover entirety of JSON Schema.
* The `precision`, `scale` and some values for the `format` are extensions to
  the JSON Schema and not part of the standard JSON Schema and as such they
  might not work with other tools in the JSON Schema landscape.
* The target system for the DDL is expected to be
  [PrestoSQL](https://prestosql.io) and while the generated SQL might work on
  other systems they are explicitly unsupported (help welcome to implement
  dialects support).

### Workflow

For contributing changes I recommend to [open an
issue](https://github.com/hashhar/jsonschema2prestosql/issues/new) before
implementing anything. That makes it more likely to avoid repeated effort and
allows other people to see if somebody else is already working on an issue or
not.

1. Fork the repo.
1. Create a new branch from `master` in your fork.
1. Do your changes.
1. Run `tools/reformat.sh` and `tools/build.sh` (this might change the files
   for reformatting).
1. Commit your changes and open a pull-request.

## Contact

Join us on [PrestoSQL slack](https://prestosql.io/slack.html) for more about
PrestoSQL. I can be found as `hashhar` over there.

Create an issue in this repo or drop me an email (not that hard to find).
