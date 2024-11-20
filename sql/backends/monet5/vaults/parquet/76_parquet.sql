create function sys.parquet_schema(fname string)
returns table(
    "name" string,
    "path" string,
    "physical_type" string,
    "length" int,
    "repetition_type" string,
    "num_children" int,
    "converted_type" string,
    "logical_type" string,
    "precision" int,
    "scale" int
)
external name parquet.schema;

create function sys.parquet_file_metadata(fname string)
returns table(
    "created_by" string,
    "format_version" int,
    "num_columns" int,
    "num_row_groups" int,
    "num_rows" int,
    "encryption_algorithm" string,
    "footer_signing_key_metadata" string
)
external name parquet.file_metadata;
