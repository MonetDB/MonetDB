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
