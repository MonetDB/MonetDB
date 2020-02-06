START TRANSACTION;

create table if not exists tmp4 (i int);

truncate TABLE tmp4;

insert into tmp4 select * from generate_series(0, 10000000);

-- tmp4 is currently uncompressed

select technique, factor, json.filter(layout, 'blks') as blocks, json.filter(layout, 'elms') as elements from mosaic.analysis('sys', 'tmp4', 'i') order by technique, factor desc;

-- should be materialized as the graph of a cutoff function.

insert into tmp4 select 10000000 from tmp4;

select technique, factor, json.filter(layout, 'blks') as blocks, json.filter(layout, 'elms') as elements from mosaic.analysis('sys', 'tmp4', 'i') order by technique, factor desc;

select technique, factor, json.filter(layout, 'blks') as blocks, json.filter(layout, 'elms') as elements from mosaic.analysis('sys', 'tmp4', 'i', 'linear, runlength') order by technique, factor desc;

-- Make sure that mosaic.analysis also works correctly on a column with compression

set optimizer='mosaic_pipe';

alter table tmp4 alter column i set storage 'dict';

select technique, factor, json.filter(layout, 'blks') as blocks, json.filter(layout, 'elms') as elements from mosaic.analysis('sys', 'tmp4', 'i') order by technique, factor desc;

select technique, factor, json.filter(layout, 'blks') as blocks, json.filter(layout, 'elms') as elements  from mosaic.analysis('sys', 'tmp4', 'i', 'linear, runlength') order by technique, factor desc;

drop table tmp4;

ROLLBACK;
