-- query 19

select * from (
select name_string.tail as itail, location_string.tail as ltail
from X00003 item, X00005 location, X00006 location_cdata, X00007 location_string,
     X00011 name, X00012 name_cdata, X00013 name_string
where item.tail = location.head
and location.tail = location_cdata.head
and location_cdata.tail = location_string.head
and item.tail = name.head
and name.tail = name_cdata.head
and name_cdata.tail = name_string.head
union all
select name_string.tail as itail, location_string.tail as ltail
from   X00474 item, X00476 location, X00477 location_cdata, X00478 location_string,
       X00482 name, X00483 name_cdata, X00484 name_string
where  item.tail = location.head
and    location.tail = location_cdata.head
and    location_cdata.tail = location_string.head
and    item.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
union all
select name_string.tail as itail, location_string.tail as ltail
from   X00631 item, X00633 location, X00634 location_cdata, X00635 location_string,
       X00639 name, X00640 name_cdata, X00641 name_string
where  item.tail = location.head
and    location.tail = location_cdata.head
and    location_cdata.tail = location_string.head
and    item.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
union all
select name_string.tail as itail, location_string.tail as ltail
from   X00788 item, X00790 location, X00791 location_cdata, X00792 location_string,
       X00796 name, X00797 name_cdata, X00798 name_string
where  item.tail = location.head
and    location.tail = location_cdata.head
and    location_cdata.tail = location_string.head
and    item.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
union all
select name_string.tail as itail, location_string.tail as ltail
from   X00317 item, X00319 location, X00320 location_cdata, X00321 location_string,
       X00325 name, X00326 name_cdata, X00327 name_string
where  item.tail = location.head
and    location.tail = location_cdata.head
and    location_cdata.tail = location_string.head
and    item.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
union all
select name_string.tail as itail, location_string.tail as ltail
from   X00160 item, X00162 location, X00163 location_cdata, X00164 location_string,
       X00168 name, X00169 name_cdata, X00170 name_string
where  item.tail = location.head
and    location.tail = location_cdata.head
and    location_cdata.tail = location_string.head
and    item.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
) as subq
order by itail;

