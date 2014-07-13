select sum(bin1+bin50+bin100+bin150+bin200)
from histogram256_tab
where rownum < &1;

