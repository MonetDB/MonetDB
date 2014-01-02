select sum(bin1)
from histogram256_tab
where rownum < &1;

