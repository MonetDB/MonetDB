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
