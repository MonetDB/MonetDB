select  
  cast(nullif(sample_4.id,
    ref_5.name) as clob) as c0, 
  ref_7.type as c1, 
  sample_4.ds as c2, 
  65 as c3
from 
  tmp.objects as sample_0
    inner join sys.environment as ref_5
        left join sys.tables as ref_6
        on ((76 is not NULL) 
            or ((true) 
              and ((ref_6.name is NULL) 
                or (ref_6.access is NULL))))
      inner join sys.storagemodel as ref_7
        left join bam.rg as sample_4
          right join sys.key_types as ref_8
          on (true)
        on (true)
      on (ref_6.commit_action = ref_8.key_type_id )
    on (sample_4.lb is not NULL)
where ref_7.columnsize is not NULL
limit 85;
