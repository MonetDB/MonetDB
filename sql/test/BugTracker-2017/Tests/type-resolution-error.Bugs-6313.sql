select auxiliary 
from sys.tablestoragemodel 
where ((select r_regionkey from sys.region) is NULL)
  or ((select index_type_name from sys.index_types) is not NULL);

