From mzukows  Fri Jan 18 14:11:19 2002
Received: from mast.ins.cwi.nl (mast.ins.cwi.nl [192.16.196.17]) by hera.cwi.nl with ESMTP
	id OAA22780 for <niels@mail.cwi.nl>; Fri, 18 Jan 2002 14:11:18 +0100 (MET)
Received: from hek.ins.cwi.nl (IDENT:fs7KnegrVt0V0ZhuLQMS+LxGxcrdcD8M@hek.ins.cwi.nl [192.16.196.133])
	by mast.ins.cwi.nl (8.11.2/8.9.3/FLW-3.11M) with ESMTP id g0IDBIc29163
	for <niels@hek.ins.cwi.nl>; Fri, 18 Jan 2002 14:11:18 +0100
Received: from localhost (mzukows@localhost)
	by hek.ins.cwi.nl (8.11.6/8.9.3/FLW-3.2C) with ESMTP id g0IDBIM26205
	for <niels@hek.ins.cwi.nl>; Fri, 18 Jan 2002 14:11:18 +0100
X-Authentication-Warning: hek.ins.cwi.nl: mzukows owned process doing -bs
Date: Fri, 18 Jan 2002 14:11:18 +0100 (CET)
From: Marcin Zukowski <Marcin.Zukowski@cwi.nl>
To: Niels Nes <Niels.Nes@cwi.nl>
Subject: some new bugs
Message-ID: <Pine.LNX.4.44.0201181408480.26127-100000@hek.ins.cwi.nl>
MIME-Version: 1.0
Content-Type: TEXT/PLAIN; charset=US-ASCII
Status: RO
Content-Length: 843
Lines: 35

Hi,
some new bugs (?) i've found follow..
I'm trying to read sql.c and other files, but it will take me some more 
time to understand it :)
regards,
marcin

--

drop table t3;
create table t3(id int, val int);
insert into t3 values(2,6);
insert into t3 values(2,NULL);
insert into t3 values(2,5);
insert into t3 values(1,NULL);
insert into t3 values(1,5);
insert into t3 values(1,6);
insert into t3 values(NULL,5);
insert into t3 values(NULL,6);
insert into t3 values(NULL,NULL);

#those 2 don't sort (bad server_output?)
select * from t3 order by val;
select * from t3 order by id;

#but those 2 do
select * from t3 order by val,id;
select * from t3 order by id,val;

#this one works, and I think it shouldn't 
select sum(*) from t3;

#this one, although wrong, results in core-dump of sql_client :)
select sum(*),val from t3 group by val ;


