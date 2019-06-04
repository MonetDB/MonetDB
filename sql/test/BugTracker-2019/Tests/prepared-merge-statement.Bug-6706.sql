create schema test;
create table test.share_daily_history (id string, timeid int, c1 int, c2 int, c3 int, c4 int, volume int);

prepare merge into test.share_daily_history as target
using (select * from (values('BHP',1,1 ,1 ,1 ,1 ,1)) as a(id,timeid,c1,c2,c3,c4,volume)) as source
on source.id=target.id and source.timeid=target.timeid
when not matched then insert (id,timeid,c1,c2,c3,c4,volume) values(source.id,source.timeid,source.c1,source.c2,source.c3,source.c4,source.volume);
exec **();
exec **();

prepare merge into test.share_daily_history as target
using (select * from (values('BHP',?,?,?,?,?,?)) as a(id,timeid,c1,c2,c3,c4,volume)) as source
on source.id=target.id and source.timeid=target.timeid
when not matched then insert (id,timeid,c1,c2,c3,c4,volume) values(source.id,source.timeid,source.c1,source.c2,source.c3,source.c4,source.volume); --error

prepare select * from test.share_daily_history
inner join (values('BHP',?,?,?,?,?,?)) as source(id,timeid,c1,c2,c3,c4,volume)
on source.id=share_daily_history.id and source.timeid=share_daily_history.timeid; --error

prepare select * from test.share_daily_history
inner join (values('BHP',?)) as source(id,timeid)
on source.id=share_daily_history.id and source.timeid=share_daily_history.timeid; --error

prepare select * from test.share_daily_history
inner join (values('BHP')) as source(id)
on source.id=share_daily_history.id;
exec **();

prepare select * from test.share_daily_history
inner join (values('BHP'), (?)) as source(id)
on source.id=share_daily_history.id; --error

drop schema test cascade;
