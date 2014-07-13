-- query 1

select name_string.tail
from   X01050 person, X01051 personid, X01052 name, X01053 name_cdata,
       X01054 name_string
where  person.tail = personid.head
and    personid.tail = 'person0'
and    person.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head;
