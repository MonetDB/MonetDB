-- SQL version of select queries of the XML-Benchmark 
-- using a vertical fragmentation approach

-- query 1

select name_string.tail
from   X01050 person, X01051 personid, X01052 name, X01053 name_cdata,
       X01054 name_string
where  person.tail = personid.head
and    personid.tail = 'person0'
and    person.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head;

-- query 2

select increase_string.tail 
from   X01111 bidder, X01120 increase, X01121 increase_cdata, X01122 increase_string,
       X01103 auction 
where  bidder.tail = increase.head
and    increase.tail = increase_cdata.head
and    increase_cdata.tail = increase_string.head
and    auction.tail = bidder.head
and    bidder.rank = (select min(allbids.rank)
		      from X01111 allbids
		      where auction.tail = allbids.head);

-- query 3 

select increase_string_fst.tail, increase_string_lst.tail
from   X01103 auction,
       X01111 bidder_fst, 
       X01120 increase_fst, X01121 increase_cdata_fst, X01122 increase_string_fst,
       X01111 bidder_lst,
       X01120 increase_lst, X01121 increase_cdata_lst, X01122 increase_string_lst
where  auction.tail = bidder_fst.head
and    bidder_fst.tail = increase_fst.head
and    increase_fst.tail = increase_cdata_fst.head
and    increase_cdata_fst.tail = increase_string_fst.head
and    bidder_fst.rank = (select min(allbids.rank)
		       	  from X01111 allbids
			  where auction.tail = allbids.head)
and    auction.tail = bidder_lst.head
and    bidder_lst.tail = increase_lst.head
and    increase_lst.tail = increase_cdata_lst.head
and    increase_cdata_lst.tail = increase_string_lst.head
and    bidder_lst.rank = (select max(allbids.rank)
			  from X01111 allbids
			  where auction.tail = allbids.head)
and    2 * CAST(increase_string_fst.tail as real) < CAST(increase_string_lst.tail as real);

-- query 4

select reserve_string.tail
from   X01103 auction, X01108 reserve, X01109 reserve_cdata, X01110 reserve_string,
       X01111 bidder_one, X01118 personref_one, X01119 personid_one,
       X01111 bidder_two, X01118 personref_two, X01119 personid_two
where  auction.tail = reserve.head
and    reserve.tail = reserve_cdata.head
and    reserve_cdata.tail = reserve_string.head
and    auction.tail = bidder_one.head
and    bidder_one.tail = personref_one.head
and    personref_one.tail = personid_one.head
and    personid_one.tail = 'person18829'
and    auction.tail = bidder_two.head
and    bidder_two.tail = personref_two.head
and    personref_two.tail = personid_two.head
and    personid_two.tail = 'person10487'
and    bidder_one.rank < bidder_two.rank;

-- query 5

select count(*)
from   X01257 price
where  cast(price.tail as real) >= 40;

-- query 6

select ((select count(*) from X00003) +
        (select count(*) from X00160) +
	(select count(*) from X00317) +
	(select count(*) from X00474) +
	(select count(*) from X00631) +
	(select count(*) from X00788));

-- query 7

select ((select count(*) from X00017) +
        (select count(*) from X00174) +
	(select count(*) from X00331) +
	(select count(*) from X00488) +
	(select count(*) from X00645) +
	(select count(*) from X00802) +
	(select count(*) from X00950) +
	(select count(*) from X01133) +
	(select count(*) from X01270) +
	(select count(*) from X00032) +
	(select count(*) from X00193) +
	(select count(*) from X00358) +
	(select count(*) from X00504) +
	(select count(*) from X00671) +
	(select count(*) from X00818));

-- query 8

select name.head, name_string.tail, count(buyerid.head)
from   X01052 name, X01053 name_cdata, X01054 name_string,
       X01051 personid, X01252 buyerid
where  personid.head = name.head
and    buyerid.tail = personid.tail
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
group by name.head, name_string.tail;

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

-- query 10

select *
from (select distinct X01070.tail
      from X01070) as interests,
     (select names.head,
	     names.name,
	     income.income,
	     gender.gender,
	     interest.interest,
	     age.age,
	     education.education,
	     address.address,
	     zipcode.zipcode,
	     province.province,
	     country.country,
	     city.city,
	     street.street,
	     email.email,
	     homepage.homepage,
	     creditcard.creditcard
from
(select X01052.head, X01054.tail as name
 from   X01052, X01053, X01054
 where  X01052.tail = X01053.head
 and    X01053.tail = X01054.head) as names
 left outer join
 ((select X01067.head, X01068.tail as income
   from   X01067, X01068
   where  X01067.tail = X01068.head) as income
   left outer join
   ((select X01067.head, X01098.tail as gender
     from X01067, X01096, X01097, X01098	
     where X01067.tail = X01096.head
     and   X01096.tail = X01097.head
     and   X01097.tail = X01098.head) as gender
     left outer join
     ((select X01067.head, X01070.tail as interest
       from X01067, X01069, X01070
       where X01067.tail = X01069.head
       and   X01069.tail = X01070.head) as interest
       left outer join
       ((select X01067.head, X01101.tail as age
         from X01067, X01099, X01100, X01101	
         where X01067.tail = X01099.head
         and   X01099.tail = X01100.head
         and   X01100.tail = X01101.head) as age
	 left outer join
	 ((select X01067.head, X01073.tail as education
	   from X01067, X01071, X01072, X01073
           where X01067.tail = X01071.head
           and   X01071.tail = X01072.head
           and   X01072.tail = X01073.head) as education
	   left outer join
           ((select X01080.head, X01083.tail as address
	     from   X01080, X01081, X01082, X01083
	     where  X01080.tail = X01081.head
             and    X01081.tail = X01082.head
             and    X01082.tail = X01083.head) as address
	     left outer join
	     ((select X01080.head, X01095.tail as zipcode
	       from   X01080, X01093, X01094, X01095
	       where  X01080.tail = X01093.head
	       and    X01093.tail = X01094.head
	       and    X01094.tail = X01095.head) as zipcode
	       left outer join
	       ((select X01080.head, X01092.tail as province
	         from   X01080, X01090, X01091, X01092
	         where  X01080.tail = X01090.head
	         and    X01090.tail = X01091.head
	         and    X01091.tail = X01092.head) as province
		 left outer join
		 ((select X01080.head, X01089.tail as country
		   from   X01080, X01087, X01088, X01089
		   where  X01080.tail = X01087.head
		   and    X01087.tail = X01088.head
		   and    X01088.tail = X01089.head) as country
		   left outer join
		   ((select X01080.head, X01086.tail as city
		     from   X01080, X01084, X01085, X01086
		     where  X01080.tail = X01084.head
		     and    X01084.tail = X01085.head
		     and    X01085.tail = X01086.head) as city
		     left outer join
		     ((select X01080.head, X01083.tail as street
		       from   X01080, X01081, X01082, X01083
		       where  X01080.tail = X01081.head
		       and    X01081.tail = X01082.head
		       and    X01082.tail = X01083.head) as street
		       left outer join
		       ((select X01055.head, X01057.tail as email
		         from   X01055, X01056, X01057
			 where  X01055.tail = X01056.head
			 and    X01056.tail = X01057.head) as email
			 left outer join
			 ((select X01061.head, X01063.tail as homepage
			   from   X01061, X01062, X01063
			   where  X01061.tail = X01062.head
			   and    X01062.tail = X01063.head) as homepage
			   left outer join
			   (select X01064.head, X01066.tail as creditcard
			    from   X01064, X01065, X01066
			    where  X01064.tail = X01065.head
			    and    X01065.tail = X01066.head) as creditcard
			   on homepage.head = creditcard.head)
			 on email.head = homepage.head)
		       on street.head = email.head)
		     on city.head = street.head)
		   on country.head = city.head)
		 on province.head = country.head)
	       on zipcode.head = province.head)
	     on	address.head = zipcode.head)
	   on education.head = address.head)
         on age.head = education.head)
       on interest.head = age.head)
     on gender.head = interest.head)
   on income.head = gender.head)
 on names.head = income.head
) as records
where interests.tail = records.interest;

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

-- query 12

-- use shred1

select name_string.tail, cnt
from   (select person.tail as oid, count(initial_string.tail) as cnt
	from X01050 person, X01067 profile, X01068 income,  X01107 initial_string
	where person.tail = profile.head 
	and profile.tail = income.head
	and cast (initial_string.tail as real)* 5000 < cast(income.tail as real)
	and cast (income.tail as real) > 50000
	group by person.tail) as subq, 
	X01052 name, X01053 name_cdata, X01054 name_string
where	oid = name.head
and	name.tail = name_cdata.head
and	name_cdata.tail = name_string.head;

-- query 13
-- requires full-text support

-- query 14
-- requires full-text support

-- query 15

select txt.tail
from X01358 txt;

-- query 16 

select X01250.tail
from   X01267, X01270, X01277, X01278, X01324,
       X01325, X01326, X01332, X01356, X01357, X01358,
       X01249, X01250
where  X01267.tail = X01270.head
and    X01270.tail = X01277.head
and    X01277.tail = X01278.head
and    X01278.tail = X01324.head
and    X01324.tail = X01325.head
and    X01325.tail = X01326.head
and    X01326.tail = X01332.head
and    X01332.tail = X01356.head
and    X01356.tail = X01357.head
and    X01357.tail = X01358.head
and    X01249.tail = X01250.head
and    X01249.head = X01267.head;

-- query 17

select person.head, name_string.tail
from   X01050 person, X01052 name, X01053 name_cdata, X01054 name_string
where  person.tail = name.head
and    name.tail = name_cdata.head
and    name_cdata.tail = name_string.head
and    not exists (select *
	   	   from X01061 homepage
		   where person.tail = homepage.head);

-- query 18
-- requires user-defined functions

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

-- query 20

select
(select count(income.tail)
 from X01068 income
 where cast(income.tail as real) >= 100000),
(select count(income.tail)
 from X01068 income
 where cast(income.tail as real) >= 30000
 and cast(income.tail as real) <= 100000),
(select count(income.tail)
 from X01068 income
 where cast(income.tail as real) < 30000),
(select count(profile.tail)
 from X01067 profile
 where not exists (select *
		   from X01068 income
		   where profile.tail = income.head));










