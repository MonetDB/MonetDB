-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at https://mozilla.org/MPL/2.0/.
--
-- For copyright information, see the file debian/copyright.

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
    row_group_id BIGINT,
    row_group_num_rows BIGINT,
    row_group_num_columns BIGINT,
    row_group_bytes	BIGINT,
    column_id BIGINT,
    file_offset BIGINT,
    num_values BIGINT,
    path_in_schema VARCHAR,
    type VARCHAR,
    stats_min VARCHAR,
    stats_max VARCHAR,
    stats_null_count BIGINT,
    stats_distinct_count BIGINT,
    stats_min_value VARCHAR,
    stats_max_value	VARCHAR,
    compression	VARCHAR,
    encodings VARCHAR,
    index_page_offset BIGINT,
    dictionary_page_offset BIGINT,
    data_page_offset BIGINT,
    total_compressed_size BIGINT,
    total_uncompressed_size	BIGINT
)
external name parquet.metadata;
