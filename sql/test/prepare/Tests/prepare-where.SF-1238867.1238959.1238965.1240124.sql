-- bug 1238867
prepare select * from env() as env where 1 = ?;
-- bug 1238959
prepare select * from env() as env where ? = ?;
-- bug 1238965
prepare select ? from env() as env;
-- bug 1240124
prepare select * from env() as env where name in (?);
