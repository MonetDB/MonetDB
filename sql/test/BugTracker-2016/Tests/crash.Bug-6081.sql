select distinct 
  ref_1.dependency_type_name as c0, 
  ref_1.dependency_type_id as c1, 
  ref_1.dependency_type_name as c2, 
  ref_1.dependency_type_name as c3, 
  ref_1.dependency_type_id as c4
from 
  sys.dependency_types as ref_1
where ref_1.dependency_type_name is NULL
limit 96;
