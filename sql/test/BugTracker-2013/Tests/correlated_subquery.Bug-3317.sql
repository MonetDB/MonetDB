create table prices (currency char(3) , 
	valid_from char(8), price decimal(15,2));

insert into prices values('USD', '20130101' , 1.2);
insert into prices values('USD', '20130201' , 1.3);

create table vouchers (vid int, date char(8), cur char(3), val decimal(15,2)); 

insert into vouchers values(1, '20130110' , 'USD' , 1000.0);
insert into vouchers values(1, '20130210' , 'USD' , 2000.0);

select vid, date, cur, val, 
	(select max(price) from prices p 
	  where p.valid_from = 
	        (select max(q.valid_from) from prices q
		  where q.valid_from <= v.date  
		    and q.currency = v.cur)
	) as Preis 
  from vouchers v;

drop table vouchers;
drop table prices;
