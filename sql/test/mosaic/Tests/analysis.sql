create table tmp4 (i int);

insert into tmp4 select * from generate_series(0, 10000000);

-- tmp4 is currently uncompressed

select technique, factor from mosaic.analysis('sys', 'tmp4', 'i') order by factor desc, technique;

select technique, factor from mosaic.analysis('sys', 'tmp4', 'i', 'capped, runlength') order by factor desc, technique;

-- should be materialized as the graph of a cutoff function.

insert into tmp4 select 10000000 from tmp4;

select technique, factor from mosaic.analysis('sys', 'tmp4', 'i') order by factor desc, technique;

select technique, factor from mosaic.analysis('sys', 'tmp4', 'i', 'linear, runlength') order by factor desc, technique;

-- Make sure that mosaic.analysis also works correctly on a column with compression

set optimizer='mosaic_pipe';

alter table tmp4 alter column i set storage 'capped';

select technique, factor from mosaic.analysis('sys', 'tmp4', 'i', 'linear, runlength') order by factor desc, technique;

drop table tmp4;
