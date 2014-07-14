-- query 11

select name_string.tail, cnt
from   (select person.tail as oid, count(initial_string.tail) as cnt
        from X01050 person, X01067 profile, X01068 income,  X01107 initial_string
        where person.tail = profile.head 
        and profile.tail = income.head
        and cast (initial_string.tail as real)* 5000 < cast(income.tail as real)
        group by person.tail) as subq, 
       X01052 name, X01053 name_cdata, X01054 name_string
where  oid = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head;

