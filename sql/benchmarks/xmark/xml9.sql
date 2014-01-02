-- query 9

select person.head, name_string.tail, item_name_string.tail
from   X01050 person, X01051 personid,
       X01248 auction, X01253 itemref, X01254 itemrefid, X01251 buyer, X01252 buyerid,
       X01052 name, X01053 name_cdata, X01054 name_string,
       X00474 item, X00475 itemid, X00482 item_name, X00483 item_name_cdata, X00484 item_name_string
where  person.tail = personid.head
and    auction.tail = buyer.head
and    buyer.tail = buyerid.head
and    buyerid.tail = personid.tail
and    item.tail = itemid.head
and    auction.tail = itemref.head
and    itemref.tail = itemrefid.head
and    itemrefid.tail = itemid.tail
and    item.tail = item_name.head
and    item_name.tail = item_name_cdata.head
and    item_name_cdata.tail = item_name_string.head
and    person.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head;

