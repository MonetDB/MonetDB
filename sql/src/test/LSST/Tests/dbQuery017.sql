-- http://dev.lsstcorp.org/trac/wiki/dbQuery017
declare @RightShift12 bigint;
set @RightShift12 = power(2,24);
select (htmID /@RightShift12) as htm_8 ,        -- group by 8-deep HTMID (rshift HTM by 12)
        avg(ra) as ra, 
        avg(dec) as [dec], 
        count(*) as pop                         -- return center point and count for display
from  Galaxy                                    -- only look at galaxies
where  (0.7*u - 0.5*g - 0.2*i) < 1.25           -- meeting this color cut
and  r < 21.75                                  -- brightr than 21.75 magnitude in red band.
group by  (htmID /@RightShift12);                -- group into 8-deep HTM buckets.

