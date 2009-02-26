
CREATE TABLE t_base (
	DT_UOM int
);

CREATE VIEW eur_kg_test AS
SELECT t_base.*
, 'EUR' AS DT_CURR
, 'kg' AS DT_UOM
FROM t_base
;

select * from eur_kg_test;

drop view eur_kg_test;
drop table t_base;
