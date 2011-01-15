create table skycrash (id serial, boom boolean);
prepare select * from skycrash where boom = ? and boom = ?;
drop table skycrash;
