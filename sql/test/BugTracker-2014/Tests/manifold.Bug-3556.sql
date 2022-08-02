set optimizer='sequential_pipe';
explain select replace(a1,a2,a3), id
from (
  select name as a1, 'a' as a2, 'A' as a3, id as id 
  from sys.functions
) as x;

explain select replace(a1,a2,a3), id
from (
  select name as a1, 'a' as a2, 'A' as a3, id + 1 as id 
  from sys.functions
) as x;

explain select replace(a1,a2,a3), id
from (
  select name as a1, 'a' as a2, 'A' as a3, abs(id) as id 
  from sys.functions
) as x;

