select sum(bin1+bin10+bin25+bin50+bin60+bin75+bin100+bin110+bin125+bin150+bin160+bin175+bin200+bin210+bin225)
from histogram256_tab
where rownum < &1;

