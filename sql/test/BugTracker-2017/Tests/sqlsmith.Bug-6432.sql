select  
  ref_0.name as c0
from 
  sys.objects as ref_0
      inner join sys.columns as ref_1
        inner join sys.dependencies as ref_2
          inner join sys.columns as sample_0
          on (sample_0.type_digits is NULL)
        on ((ref_1.type is NULL) 
            or (ref_2.id is not NULL))
      on (12 is NULL)
    left join sys.queue as ref_3
    on (ref_1.number = ref_3.progress )
where ref_3.estimate is not NULL
limit 156;
