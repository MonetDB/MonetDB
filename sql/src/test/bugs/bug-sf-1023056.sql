select 
(( case 
	when name='tables' or name<>'views' 
		then 1 
	else 0 
end )) 
from tables;
