start transaction;

create table t1023056 (name varchar(1024));
insert into t1023056 values ('niels'), ('fabian'), ('martin');

select 
(( case 
	when name='fabian' or name<>'martin' 
		then 1 
	else 0 
end )) 
from t1023056;
