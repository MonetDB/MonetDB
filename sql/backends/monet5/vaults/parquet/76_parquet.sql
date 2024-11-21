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

create function sys.parquet_metadata(fname string)
returns table(
    row_group_id INT,
    row_group_num_rows INT,
    row_group_num_columns INT,
    row_group_bytes	INT,
    column_id INT,
    file_offset INT,
    num_values INT,
    path_in_schema VARCHAR,
    type VARCHAR,
    stats_min VARCHAR,
    stats_max VARCHAR,
    stats_null_count INT,
    stats_distinct_count INT,
    stats_min_value VARCHAR,
    stats_max_value	VARCHAR,
    compression	VARCHAR,
    encodings VARCHAR,
    index_page_offset INT,
    dictionary_page_offset INT,
    data_page_offset INT,
    total_compressed_size INT,
    total_uncompressed_size	INT
)
external name parquet.metadata;
