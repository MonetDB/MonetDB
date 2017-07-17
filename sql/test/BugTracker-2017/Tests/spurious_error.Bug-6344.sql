select  
  sample_2.name as c0, 
  sample_2.id as c1, 
  sample_2.name as c2, 
  cast(coalesce(sample_2.id,
    sample_2.id) as int) as c3, 
  sample_2.id as c4, 
  11 as c5, 
  sample_2.id as c6
from 
  sys.objects as sample_2
where sample_2.name is not NULL
limit 61;

