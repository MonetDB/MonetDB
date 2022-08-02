select sum(bin1+bin25+bin50+bin75+bin100+bin125+bin150+bin175+bin200+bin225)
from histogram256_tab
where rownum < &1;

