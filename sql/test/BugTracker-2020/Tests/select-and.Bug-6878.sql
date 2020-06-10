start transaction;
CREATE TABLE temp_table(norm_min float, norm_max float, mmin float, mmax float);
INSERT INTO temp_table VALUES (0.0,0.17424371009,6.0,12.25);
INSERT INTO temp_table VALUES (0.17424371009,0.2865480811,12.5,18.5);
INSERT INTO temp_table VALUES (0.2865480811,0.7201958679,18.5,24.75);
INSERT INTO temp_table VALUES (0.7201958679,1.0,24.75,31.0);
CREATE TABLE temp_value(vvalue float);
COPY 10 RECORDS INTO "sys"."temp_value" FROM stdin USING DELIMITERS E'\t',E'\n','"';
0.72
0.524
0.782
0.936
0.94
0.111
0.776
0.715
0.809
0.504


select (temp_table.mmax - temp_table.mmin)+temp_table.mmin as col1 from temp_value, temp_table where temp_value.vvalue<=temp_table.norm_max and temp_value.vvalue>temp_table.norm_min order by col1;
select (0.72/2147483648.0)*(temp_table.mmax - temp_table.mmin)+temp_table.mmin as col1 from temp_value, temp_table where temp_value.vvalue<=temp_table.norm_max and temp_value.vvalue>temp_table.norm_min order by col1;
rollback;
