CREATE PROCEDURE bam_loader_repos(bam_repos STRING, dbschema SMALLINT, nr_threads SMALLINT)
EXTERNAL NAME bam.bam_loader_repos;

CREATE PROCEDURE bam_loader_files(bam_files STRING, dbschema SMALLINT, nr_threads SMALLINT)
EXTERNAL NAME bam.bam_loader_files;

CREATE PROCEDURE bam_loader_file(bam_file STRING, dbschema SMALLINT)
EXTERNAL NAME bam.bam_loader_file;

CREATE PROCEDURE bam_drop_file(file_id BIGINT, dbschema SMALLINT)
EXTERNAL NAME bam.bam_drop_file;


CREATE FUNCTION bam_flag(flag SMALLINT, name STRING)
RETURNS BOOLEAN EXTERNAL NAME bam.bam_flag;

CREATE FUNCTION reverse_seq(seq STRING)
RETURNS STRING EXTERNAL NAME bam.reverse_seq;

CREATE FUNCTION reverse_qual(qual STRING)
RETURNS STRING EXTERNAL NAME bam.reverse_qual;

CREATE FUNCTION seq_length(cigar STRING)
RETURNS INT EXTERNAL NAME bam.seq_length;


CREATE PROCEDURE sam_export(output_path STRING)
EXTERNAL NAME bam.sam_export;

CREATE PROCEDURE bam_export(output_path STRING)
EXTERNAL NAME bam.bam_export;
