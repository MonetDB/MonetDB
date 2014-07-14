select * from (
select p7.tail as location, p13.tail as name 
from   hidx p5, hidx p6, attx p7,
       hidx p11, hidx p12, attx p13
where  p5.tblid = 5
and    p6.tblid = 6
and    p7.tblid = 7
and    p11.tblid = 11
and    p12.tblid = 12
and    p13.tblid = 13
and    p5.head = p11.head
and    p5.tail = p6.head
and    p6.tail = p7.head
and    p11.tail = p12.head
and    p12.tail = p13.head
union all
(select p164.tail as location, p170.tail as name
from   hidx p162, hidx p163, attx p164,
       hidx p168, hidx p169, attx p170
where  p162.tblid = 162
and    p163.tblid = 163
and    p164.tblid = 164
and    p168.tblid = 168
and    p169.tblid = 169
and    p170.tblid = 170
and    p162.head = p168.head
and    p162.tail = p163.head
and    p163.tail = p164.head
and    p168.tail = p169.head
and    p169.tail = p170.head)
union all
(select p321.tail as location, p327.tail as name
from   hidx p319, hidx p320, attx p321,
       hidx p325, hidx p326, attx p327
where  p319.tblid = 319
and    p320.tblid = 320
and    p321.tblid = 321
and    p325.tblid = 325
and    p326.tblid = 326
and    p327.tblid = 327
and    p319.head = p325.head
and    p319.tail = p320.head
and    p320.tail = p321.head
and    p325.tail = p326.head
and    p326.tail = p327.head)
union all
(select p478.tail as location, p484.tail as name
from   hidx p476, hidx p477, attx p478,
       hidx p482, hidx p483, attx p484
where  p476.tblid = 476
and    p477.tblid = 477
and    p478.tblid = 478
and    p482.tblid = 482
and    p483.tblid = 483
and    p484.tblid = 484
and    p476.head = p482.head
and    p476.tail = p477.head
and    p477.tail = p478.head
and    p482.tail = p483.head
and    p483.tail = p484.head)
union all
(select p635.tail as location, p641.tail as name
from   hidx p633, hidx p634, attx p635,
       hidx p639, hidx p640, attx p641
where  p633.tblid = 633
and    p634.tblid = 634
and    p635.tblid = 635
and    p639.tblid = 639
and    p640.tblid = 640
and    p641.tblid = 641
and    p633.head = p639.head
and    p633.tail = p634.head
and    p634.tail = p635.head
and    p639.tail = p640.head
and    p640.tail = p641.head)
union all
(select p792.tail as location, p798.tail as name
from   hidx p790, hidx p791, attx p792,
       hidx p796, hidx p797, attx p798
where  p790.tblid = 790
and    p791.tblid = 791
and    p792.tblid = 792
and    p796.tblid = 796
and    p797.tblid = 797
and    p798.tblid = 798
and    p790.head = p796.head
and    p790.tail = p791.head
and    p791.tail = p792.head
and    p796.tail = p797.head
and    p797.tail = p798.head)
) as names
order by name;
