CREATE TABLE "sys"."cache_kv1" (
        "doc_lpn_jpc_tdgc"    VARCHAR(43)
);
CREATE TABLE "sys"."cache_kv1_3" (
        "daytype"              SMALLINT      NOT NULL,
        "validfrom"            DATE          NOT NULL,
        "validthru"            DATE          NOT NULL,
        "doc_lpn_jpc_tdgc"     VARCHAR(43)
);

insert into cache_kv1 values ('1');
insert into cache_kv1_3 values (8, '2012-02-01', '2012-04-01', '1');
insert into cache_kv1_3 values (8, '2012-02-01', '2012-04-01', '2');
insert into cache_kv1 values ('4');
insert into cache_kv1_3 values (8, '2012-01-01', '2012-01-02', '1');
insert into cache_kv1_3 values (1, '2012-01-01', '2012-01-02', '1');

select *
FROM cache_kv1, cache_kv1_3 WHERE
cache_kv1.doc_lpn_jpc_tdgc = cache_kv1_3.doc_lpn_jpc_tdgc and
bit_and(cache_kv1_3.daytype, 8) = 8 and
'2012-03-20' BETWEEN cache_kv1_3.validfrom AND cache_kv1_3.validthru;

drop table cache_kv1_3;
drop table cache_kv1;
