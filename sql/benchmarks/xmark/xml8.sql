-- query 8

select name.head, name_string.tail, count(buyerid.head)
from   X01052 name, X01053 name_cdata, X01054 name_string,
       X01051 personid, X01252 buyerid
where  personid.head = name.head
and    buyerid.tail = personid.tail
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
group by name.head, name_string.tail;

