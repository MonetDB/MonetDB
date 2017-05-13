select 
        case when EXISTS ( select hashes from sys.tablestoragemodel )
                then (true)
                else (false)
        end
from sys.tables;

