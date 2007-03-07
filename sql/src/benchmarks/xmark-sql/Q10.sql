select *
from
(select distinct p1070.tail
 from   attx p1070
 where  p1070.tblid = 1070) as interests,
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
(select p1052.head, p1054.tail as name
 from   hidx p1052, hidx p1053, attx p1054
 where  p1052.tblid = 1052
 and	p1053.tblid = 1053
 and	p1054.tblid = 1054
 and    p1052.tail = p1053.head
 and    p1053.tail = p1054.head) as names
 left outer join
 ((select p1067.head, p1068.tail as income
   from   hidx p1067, attx p1068
   where  p1067.tblid = 1067
   and	  p1068.tblid = 1068
   and	  p1067.tail = p1068.head) as income
   left outer join
   ((select p1067.head, p1098.tail as gender
     from   hidx p1067, hidx p1096, hidx p1097, attx p1098	
     where  p1067.tblid = 1067
     and    p1096.tblid = 1096
     and    p1097.tblid = 1097
     and    p1098.tblid = 1098	
     and    p1067.tail = p1096.head
     and    p1096.tail = p1097.head
     and    p1097.tail = p1098.head) as gender
     left outer join
     ((select p1067.head, p1070.tail as interest
       from   hidx p1067, hidx p1069, attx p1070
       where  p1067.tblid = 1067
       and    p1069.tblid = 1069
       and    p1070.tblid = 1070
       and    p1067.tail = p1069.head
       and    p1069.tail = p1070.head) as interest
       left outer join
       ((select p1067.head, p1101.tail as age
         from   hidx p1067, hidx p1099, hidx p1100, attx p1101
         where  p1067.tblid = 1067
	 and    p1099.tblid = 1099
	 and    p1100.tblid = 1100
	 and	p1101.tblid = 1101	 
	 and    p1067.tail = p1099.head
         and    p1099.tail = p1100.head
         and    p1100.tail = p1101.head) as age
	 left outer join
	 ((select p1067.head, p1073.tail as education
	   from   hidx p1067, hidx p1071, hidx p1072, attx p1073
           where  p1067.tblid = 1067
	   and    p1071.tblid = 1071
	   and	  p1072.tblid = 1072
	   and	  p1073.tblid = 1073
	   and	  p1067.tail = p1071.head
           and    p1071.tail = p1072.head
           and    p1072.tail = p1073.head) as education
	   left outer join
           ((select p1080.head, p1083.tail as address
	     from   hidx p1080, hidx p1081, hidx p1082, attx p1083
	     where  p1080.tblid = 1080
	     and    p1081.tblid = 1081
	     and    p1082.tblid = 1082
	     and    p1083.tblid = 1083
	     and    p1080.tail = p1081.head
             and    p1081.tail = p1082.head
             and    p1082.tail = p1083.head) as address
	     left outer join
	     ((select p1080.head, p1095.tail as zipcode
	       from   hidx p1080, hidx p1093, hidx p1094, attx p1095
	       where  p1080.tblid = 1080
	       and    p1093.tblid = 1093
	       and    p1094.tblid = 1094
	       and    p1095.tblid = 1095
	       and    p1080.tail = p1093.head
	       and    p1093.tail = p1094.head
	       and    p1094.tail = p1095.head) as zipcode
	       left outer join
	       ((select p1080.head, p1092.tail as province
	         from   hidx p1080, hidx p1090, hidx p1091, attx p1092
	         where  p1080.tblid = 1080
		 and	p1090.tblid = 1090
		 and    p1091.tblid = 1091
		 and	p1092.tblid = 1092
		 and    p1080.tail = p1090.head
	         and    p1090.tail = p1091.head
	         and    p1091.tail = p1092.head) as province
		 left outer join
		 ((select p1080.head, p1089.tail as country
		   from   hidx p1080, hidx p1087, hidx p1088, attx p1089
		   where  p1080.tblid = 1080
		   and    p1087.tblid = 1087
		   and    p1088.tblid = 1088
		   and    p1089.tblid = 1089
		   and	  p1080.tail = p1087.head
		   and    p1087.tail = p1088.head
		   and    p1088.tail = p1089.head) as country
		   left outer join
		   ((select p1080.head, p1086.tail as city
		     from   hidx p1080, hidx p1084, hidx p1085, attx p1086
		     where  p1080.tblid = 1080
		     and    p1084.tblid = 1084
		     and    p1085.tblid = 1085
		     and    p1086.tblid = 1086
		     and    p1080.tail = p1084.head
		     and    p1084.tail = p1085.head
		     and    p1085.tail = p1086.head) as city
		     left outer join
		     ((select p1080.head, p1083.tail as street
		       from   hidx p1080, hidx p1081, hidx p1082, attx p1083
		       where  p1080.tblid = 1080
		       and    p1081.tblid = 1081
		       and    p1082.tblid = 1082
		       and    p1083.tblid = 1083
		       and    p1080.tail = p1081.head
		       and    p1081.tail = p1082.head
		       and    p1082.tail = p1083.head) as street
		       left outer join
		       ((select p1055.head, p1057.tail as email
		         from   hidx p1055, hidx p1056, attx p1057
			 where  p1055.tblid = 1055
			 and	p1056.tblid = 1056
			 and    p1057.tblid = 1057
			 and    p1055.tail = p1056.head
			 and    p1056.tail = p1057.head) as email
			 left outer join
			 ((select p1061.head, p1063.tail as homepage
			   from   hidx p1061, hidx p1062, attx p1063
			   where  p1061.tblid = 1061
			   and    p1062.tblid = 1062
			   and    p1063.tblid = 1063
			   and    p1061.tail = p1062.head
			   and    p1062.tail = p1063.head) as homepage
			   left outer join
			   (select p1064.head, p1066.tail as creditcard
			    from   hidx p1064, hidx p1065, attx p1066
			    where  p1064.tblid = 1064
			    and    p1065.tblid = 1065
			    and    p1066.tblid = 1066
			    and    p1064.tail = p1065.head
			    and    p1065.tail = p1066.head) as creditcard
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
where interests.tail = records.interest
order by interests.tail;
