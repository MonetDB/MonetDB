CREATE TABLE PARTSUPP_6418 ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL );
select  
  ref_2.key_type_id as c0, 
  ref_3.ps_availqty as c1, 
  sample_0.name as c2
from 
  tmp.idxs as sample_0
      right join sys.sequences as sample_1
      on ((true) 
          or ((sample_0.name is NULL) 
            or (sample_0.type is not NULL)))
    inner join sys.key_types as ref_2
      left join sys.partsupp_6418 as ref_3
      on (ref_2.key_type_id is not NULL)
    on (sample_1.schema_id is NULL)
where true
limit 116;

drop table partsupp_6418;
