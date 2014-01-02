select person.head, name_string.tail
from   X01050 person, X01052 name, X01053 name_cdata, X01054 name_string
where  person.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
and    not exists (select *
	   	   from X01061 homepage
		   where person.tail = homepage.head);

