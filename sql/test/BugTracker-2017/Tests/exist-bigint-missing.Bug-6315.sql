select 
        case when EXISTS ( select hashes from sys.tablestoragemodel )
                then (true)
                else (false)
        end
from sys.tables;


insert into sys.privileges values (
        1,
        31,
        87,
        case when EXISTS (
              select (select io from sys.querylog_calls) as c0
              from
                sys.rejects as ref_0
              where ref_0.fldid is NULL) 
			then 56 else 56 end ,
        case when (select dependency_type_name from sys.dependency_types)
               is not NULL then 7 else 7 end
          );
