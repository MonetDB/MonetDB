-- The n-gram case was proposed by DLR
-- it shows the use of expanded tiling

create array img( x integer dimension[5], y integer dimension[5], val float);

select v1,v2,v3,v4, count( *) 
from ( select img.val as (v1,v2,v3,v4) from img group by img[x:x+3][y]) as x
group by v1,v2,v3,v4;
