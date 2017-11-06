select  
  ref_3.name as c0, 
  ref_4.index_type_name as c1, 
  ref_3.name as c2, 
  ref_3.value as c3
from 
  sys.environment as ref_3
    inner join sys.index_types as ref_4
    on (ref_3.value is NULL)
where EXISTS (
  select  
      ref_3.value as c0
    from 
      (select  
            ref_5.table_id as c0, 
            ref_5.table_id as c1, 
            ref_6.lb as c2, 
            ref_4.index_type_name as c3, 
            ref_4.index_type_id as c4
          from 
            tmp._columns as ref_5
              left join bam.rg as ref_6
              on (ref_5.id is not NULL)
          where false
          limit 97) as subq_0
    where true)
limit 136;
