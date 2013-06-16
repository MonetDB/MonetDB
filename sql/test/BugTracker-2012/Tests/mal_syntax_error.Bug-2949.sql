create table r (u varchar(32), v varchar(32), r varchar(5));
CREATE FUNCTION refine ()
RETURNS TABLE (u varchar(32), v varchar(32), r varchar(5))
BEGIN
       RETURN TABLE (

                       (       SELECT R.u, R.v, R.r FROM R

                               INTERSECT

                               SELECT R.u, R.v, R.r FROM R
                       )
               );
END;
select * from refine();

drop function refine;
drop table r;
