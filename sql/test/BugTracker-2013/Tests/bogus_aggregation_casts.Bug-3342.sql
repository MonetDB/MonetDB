create table part (p_partkey integer not null, p_name varchar(55) not null, p_mfgr char(25) not null, p_brand char(10) not null, p_type varchar(25) not null, p_size integer not null, p_container char(10) not null, p_retailprice decimal(12,2) not null, p_comment varchar(23) not null);
copy 5 records into part from stdin;
1|goldenrod lavender spring chocolate lace|Manufacturer#1|Brand#13|PROMO BURNISHED COPPER|7|JUMBO PKG|901.00|ly. slyly ironi|
2|maroon sky cream royal snow|Manufacturer#1|Brand#13|LARGE BRUSHED BRASS|1|LG CASE|902.00|lar accounts amo|
3|brown blue puff midnight black|Manufacturer#4|Brand#42|STANDARD POLISHED BRASS|21|WRAP CASE|903.00|egular deposits hag|
4|orange goldenrod peach misty seashell|Manufacturer#3|Brand#34|SMALL PLATED BRASS|14|MED DRUM|904.00|p furiously r|
5|midnight linen almond tomato plum|Manufacturer#3|Brand#32|STANDARD POLISHED TIN|15|SM PKG|905.00| wake carefully |

select sys.median(cast (p_retailprice as double)) from part;
select sys.median(p_retailprice) from part;
drop table part;
