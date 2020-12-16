import os, sys, socket, glob, pymonetdb, threading, time, codecs, tempfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

nworkers = 5
shardtable = 'lineorder'
repltable  = 'dwdate'

shardedtabledef = """ (
LO_ORDERKEY int,
LO_LINENUMBER int,
LO_CUSTKEY int,
LO_PARTKEY int,
LO_SUPPKEY int,
LO_ORDERDATE int,
LO_ORDERPRIORITY string,
LO_SHIPPRIORITY string,
LO_QUANTITY int,
LO_EXTENDEDPRICE int,
LO_ORDTOTALPRICE int,
LO_DISCOUNT int,
LO_REVENUE int,
LO_SUPPLYCOST int,
LO_TAX int,
LO_COMMITDATE int,
LO_SHIPMODE string)
"""

replicatedtabledef = """ (
D_DATEKEY int,
D_DATE string,
D_DAYOFWEEK string,
D_MONTH string,
D_YEAR int,
D_YEARMONTHNUM int,
D_YEARMONTH string,
D_DAYNUMINWEEK int,
D_DAYNUMINMONTH int,
D_DAYNUMINYEAR int,
D_MONTHNUMINYEAR int,
D_WEEKNUMINYEAR int,
D_SELLINGSEASON string,
D_LASTDAYINWEEKFL int,
D_LASTDAYINMONTHFL int,
D_HOLIDAYFL int,
D_WEEKDAYFL int)"""

dimensiontabledef = """
create table SUPPLIER (
S_SUPPKEY int,
S_NAME string,
S_ADDRESS string,
S_CITY string,
S_NATION string,
S_REGION string,
S_PHONE string);

create table CUSTOMER (
C_CUSTKEY int,
C_NAME string,
C_ADDRESS string,
C_CITY string,
C_NATION string,
C_REGION string,
C_PHONE string,
C_MKTSEGMENT string);

create table PART (
P_PARTKEY int,
P_NAME string,
P_MFGR string,
P_CATEGORY string,
P_BRAND1 string,
P_COLOR string,
P_TYPE string,
P_SIZE int,
P_CONTAINER string);
"""

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

# load data (in parallel)
def worker_load(workerrec):
    c = workerrec['conn'].cursor()
    stable = shardtable + workerrec['tpf']

    screateq = 'create table %s %s' % (stable, shardedtabledef)
    sloadq = 'copy into %s from \'%s\'' % (stable, workerrec['split'].replace('\\', '\\\\'))
    c.execute(screateq)
    c.execute(sloadq)

    rtable = repltable + workerrec['tpf']
    rcreateq = 'create table %s %s' % (rtable, replicatedtabledef)
    rloadq = 'copy into %s from \'%s\'' % (rtable, workerrec['repldata'].replace('\\', '\\\\'))
    c.execute(rcreateq)
    c.execute(rloadq)

ssbmpath = os.path.join(os.environ['TSTSRCBASE'], 'sql', 'benchmarks', 'ssbm', 'Tests')
ssbmdatapath = os.path.join(ssbmpath, 'SF-0.01')

results = {
    1:[(4199635969,)],
    2:[(927530712,)],
    3:[(213477639,)],
    4:[(35741623, 1992, "MFGR#1211"),
(13582129, 1992, "MFGR#1212"),
(2420150, 1992, "MFGR#1217"),
(7015912, 1992, "MFGR#1219"),
(2641607, 1992, "MFGR#122"),
(2563912, 1992, "MFGR#1221"),
(6003315, 1992, "MFGR#1223"),
(8704784, 1992, "MFGR#1224"),
(13700463, 1992, "MFGR#1225"),
(12610793, 1992, "MFGR#1226"),
(5995739, 1992, "MFGR#1228"),
(7552882, 1992, "MFGR#1230"),
(13184783, 1992, "MFGR#1232"),
(3231894, 1992, "MFGR#1240"),
(7214844, 1992, "MFGR#125"),
(35054814, 1992, "MFGR#127"),
(15102503, 1992, "MFGR#128"),
(7639101, 1992, "MFGR#129"),
(3339630, 1993, "MFGR#121"),
(11514911, 1993, "MFGR#1211"),
(4294668, 1993, "MFGR#1212"),
(8122643, 1993, "MFGR#1213"),
(3678145, 1993, "MFGR#1214"),
(7325684, 1993, "MFGR#1217"),
(16038869, 1993, "MFGR#1218"),
(26560659, 1993, "MFGR#1219"),
(5551142, 1993, "MFGR#1220"),
(3484096, 1993, "MFGR#1222"),
(21747351, 1993, "MFGR#1223"),
(5762459, 1993, "MFGR#1224"),
(10279155, 1993, "MFGR#1225"),
(9952878, 1993, "MFGR#1226"),
(6572535, 1993, "MFGR#1228"),
(10193031, 1993, "MFGR#1230"),
(12137253, 1993, "MFGR#1233"),
(6960104, 1993, "MFGR#1236"),
(10329821, 1993, "MFGR#1239"),
(4258048, 1993, "MFGR#1240"),
(1382981, 1993, "MFGR#125"),
(2821764, 1993, "MFGR#126"),
(14693311, 1993, "MFGR#127"),
(1799648, 1993, "MFGR#128"),
(5071393, 1993, "MFGR#129"),
(18995020, 1994, "MFGR#121"),
(4586682, 1994, "MFGR#1211"),
(19530708, 1994, "MFGR#1212"),
(1728488, 1994, "MFGR#1213"),
(13010765, 1994, "MFGR#1218"),
(10997176, 1994, "MFGR#1219"),
(4554495, 1994, "MFGR#122"),
(8557774, 1994, "MFGR#1220"),
(5584069, 1994, "MFGR#1222"),
(10104525, 1994, "MFGR#1223"),
(4775592, 1994, "MFGR#1224"),
(10713992, 1994, "MFGR#1225"),
(3750100, 1994, "MFGR#1226"),
(1794201, 1994, "MFGR#123"),
(5980941, 1994, "MFGR#1232"),
(7859272, 1994, "MFGR#1233"),
(3550796, 1994, "MFGR#1236"),
(2106877, 1994, "MFGR#1238"),
(14223208, 1994, "MFGR#1239"),
(4027318, 1994, "MFGR#1240"),
(17777153, 1994, "MFGR#125"),
(4869251, 1994, "MFGR#126"),
(15392246, 1994, "MFGR#127"),
(900044, 1994, "MFGR#128"),
(5656126, 1994, "MFGR#129"),
(1097742, 1995, "MFGR#1211"),
(2707025, 1995, "MFGR#1213"),
(4200351, 1995, "MFGR#1214"),
(2509211, 1995, "MFGR#1217"),
(5595420, 1995, "MFGR#1218"),
(9841612, 1995, "MFGR#1219"),
(4624540, 1995, "MFGR#1220"),
(19279359, 1995, "MFGR#1222"),
(5452000, 1995, "MFGR#1223"),
(18723136, 1995, "MFGR#1224"),
(14590136, 1995, "MFGR#1225"),
(8338345, 1995, "MFGR#1226"),
(8260673, 1995, "MFGR#1228"),
(97819, 1995, "MFGR#1230"),
(6377178, 1995, "MFGR#1232"),
(14117772, 1995, "MFGR#1233"),
(22661695, 1995, "MFGR#1236"),
(5142893, 1995, "MFGR#1239"),
(17227363, 1995, "MFGR#1240"),
(7873139, 1995, "MFGR#125"),
(14385634, 1995, "MFGR#128"),
(4383708, 1995, "MFGR#129"),
(12158316, 1996, "MFGR#121"),
(13534131, 1996, "MFGR#1211"),
(27850979, 1996, "MFGR#1212"),
(784827, 1996, "MFGR#1218"),
(1069800, 1996, "MFGR#122"),
(4348542, 1996, "MFGR#1221"),
(8295332, 1996, "MFGR#1222"),
(11183976, 1996, "MFGR#1224"),
(19971623, 1996, "MFGR#1225"),
(10005626, 1996, "MFGR#1232"),
(5135891, 1996, "MFGR#1236"),
(4613081, 1996, "MFGR#1239"),
(16358592, 1996, "MFGR#1240"),
(18374286, 1996, "MFGR#125"),
(4288542, 1996, "MFGR#126"),
(32750117, 1996, "MFGR#127"),
(128577, 1996, "MFGR#128"),
(6040770, 1996, "MFGR#129"),
(4313787, 1997, "MFGR#121"),
(15464411, 1997, "MFGR#1211"),
(17725171, 1997, "MFGR#1212"),
(2137125, 1997, "MFGR#1213"),
(4188998, 1997, "MFGR#1214"),
(4172057, 1997, "MFGR#1217"),
(15556327, 1997, "MFGR#1219"),
(5953230, 1997, "MFGR#122"),
(10529427, 1997, "MFGR#1220"),
(9097752, 1997, "MFGR#1221"),
(8668003, 1997, "MFGR#1222"),
(18232285, 1997, "MFGR#1223"),
(13859956, 1997, "MFGR#1224"),
(2793729, 1997, "MFGR#1225"),
(19174753, 1997, "MFGR#1226"),
(5128426, 1997, "MFGR#123"),
(4304076, 1997, "MFGR#1230"),
(8866362, 1997, "MFGR#1232"),
(5328122, 1997, "MFGR#1233"),
(13735907, 1997, "MFGR#1236"),
(10755017, 1997, "MFGR#1240"),
(327046, 1997, "MFGR#125"),
(13218597, 1997, "MFGR#127"),
(10434877, 1997, "MFGR#128"),
(8052920, 1997, "MFGR#129"),
(10864281, 1998, "MFGR#121"),
(2140471, 1998, "MFGR#1211"),
(15949227, 1998, "MFGR#1212"),
(9091580, 1998, "MFGR#1217"),
(7834997, 1998, "MFGR#1219"),
(2319634, 1998, "MFGR#1220"),
(19485508, 1998, "MFGR#1222"),
(1919741, 1998, "MFGR#1223"),
(7617265, 1998, "MFGR#1224"),
(4074189, 1998, "MFGR#1225"),
(7341274, 1998, "MFGR#123"),
(2607543, 1998, "MFGR#1230"),
(13307853, 1998, "MFGR#1232"),
(5316438, 1998, "MFGR#1233"),
(1879252, 1998, "MFGR#1236"),
(2738610, 1998, "MFGR#1238"),
(733435, 1998, "MFGR#1239"),
(5151646, 1998, "MFGR#1240"),
(3651938, 1998, "MFGR#126"),
(8693908, 1998, "MFGR#127"),
(2768015, 1998, "MFGR#128"),
(11569138, 1998, "MFGR#129")],
    5:[(10306232,1992,"MFGR#2221"),
(7096296,1992,"MFGR#2222"),
(13538169,1992,"MFGR#2224"),
(588657,1992,"MFGR#2225"),
(2064857,1992,"MFGR#2226"),
(19175615,1992,"MFGR#2227"),
(5597060,1993,"MFGR#2221"),
(1012379,1993,"MFGR#2222"),
(13096133,1993,"MFGR#2223"),
(8200828,1993,"MFGR#2224"),
(880669,1993,"MFGR#2225"),
(6530070,1993,"MFGR#2226"),
(8174486,1993,"MFGR#2227"),
(20407818,1994,"MFGR#2221"),
(288819,1994,"MFGR#2222"),
(6642402,1994,"MFGR#2223"),
(13970991,1994,"MFGR#2224"),
(11207503,1994,"MFGR#2226"),
(14038987,1994,"MFGR#2227"),
(9206253,1995,"MFGR#2222"),
(10583880,1995,"MFGR#2223"),
(4168114,1995,"MFGR#2224"),
(4480694,1995,"MFGR#2226"),
(5318366,1995,"MFGR#2227"),
(7037757,1996,"MFGR#2221"),
(4626343,1996,"MFGR#2222"),
(6680802,1996,"MFGR#2223"),
(7899650,1996,"MFGR#2224"),
(3943542,1996,"MFGR#2225"),
(8618649,1996,"MFGR#2226"),
(21285135,1996,"MFGR#2227"),
(14925990,1997,"MFGR#2221"),
(9139251,1997,"MFGR#2222"),
(307521,1997,"MFGR#2223"),
(8791563,1997,"MFGR#2224"),
(5652967,1997,"MFGR#2225"),
(10920254,1997,"MFGR#2226"),
(3463448,1997,"MFGR#2227"),
(3697632,1998,"MFGR#2221"),
(17884495,1998,"MFGR#2222"),
(6450682,1998,"MFGR#2223"),
(8158474,1998,"MFGR#2224"),
(4449696,1998,"MFGR#2225"),
(5931644,1998,"MFGR#2226"),
(18442636,1998,"MFGR#2227")],
    6:[(2535744, 1992, "MFGR#2221"),
(5646414, 1993, "MFGR#2221"),
(9516564, 1994, "MFGR#2221"),
(11180484, 1995, "MFGR#2221"),
(6838192, 1996, "MFGR#2221"),
(4605666, 1997, "MFGR#2221"),
(9102972, 1998, "MFGR#2221")],
    7:[("INDONESIA", "VIETNAM", 1992, 172784923),
("INDONESIA", "INDIA", 1992, 121272321),
("VIETNAM", "VIETNAM", 1992, 116846322),
("CHINA", "VIETNAM", 1992, 105383218),
("INDONESIA", "INDONESIA", 1992, 96566652),
("INDONESIA", "CHINA", 1992, 92251585),
("CHINA", "INDIA", 1992, 83126967),
("VIETNAM", "INDIA", 1992, 79112303),
("CHINA", "CHINA", 1992, 72681328),
("VIETNAM", "CHINA", 1992, 71505221),
("INDONESIA", "JAPAN", 1992, 68661048),
("CHINA", "INDONESIA", 1992, 67197002),
("INDIA", "CHINA", 1992, 65583175),
("CHINA", "JAPAN", 1992, 62226114),
("VIETNAM", "JAPAN", 1992, 61894943),
("INDIA", "VIETNAM", 1992, 59819419),
("JAPAN", "INDIA", 1992, 56751348),
("INDIA", "INDONESIA", 1992, 55637439),
("INDIA", "JAPAN", 1992, 49558176),
("INDIA", "INDIA", 1992, 48849419),
("VIETNAM", "INDONESIA", 1992, 47838667),
("JAPAN", "VIETNAM", 1992, 44269137),
("JAPAN", "CHINA", 1992, 42183228),
("JAPAN", "INDONESIA", 1992, 28041654),
("JAPAN", "JAPAN", 1992, 26509605),
("INDONESIA", "CHINA", 1993, 182426844),
("INDONESIA", "VIETNAM", 1993, 136511542),
("INDONESIA", "INDONESIA", 1993, 133046814),
("CHINA", "VIETNAM", 1993, 132740933),
("CHINA", "INDONESIA", 1993, 130804709),
("INDONESIA", "INDIA", 1993, 103329006),
("VIETNAM", "INDIA", 1993, 100433752),
("JAPAN", "CHINA", 1993, 100244771),
("CHINA", "CHINA", 1993, 93076520),
("VIETNAM", "INDONESIA", 1993, 86075637),
("VIETNAM", "VIETNAM", 1993, 83636193),
("VIETNAM", "CHINA", 1993, 79839586),
("CHINA", "INDIA", 1993, 75476522),
("INDIA", "CHINA", 1993, 73358891),
("CHINA", "JAPAN", 1993, 72859116),
("INDIA", "INDIA", 1993, 62417109),
("JAPAN", "INDIA", 1993, 51402586),
("INDONESIA", "JAPAN", 1993, 49099054),
("JAPAN", "JAPAN", 1993, 48470872),
("VIETNAM", "JAPAN", 1993, 47136044),
("JAPAN", "INDONESIA", 1993, 46575202),
("JAPAN", "VIETNAM", 1993, 41104620),
("INDIA", "VIETNAM", 1993, 39181293),
("INDIA", "INDONESIA", 1993, 36383474),
("INDIA", "JAPAN", 1993, 25046025),
("INDONESIA", "CHINA", 1994, 149657390),
("INDONESIA", "INDONESIA", 1994, 139096654),
("VIETNAM", "CHINA", 1994, 128092591),
("INDONESIA", "INDIA", 1994, 125691779),
("CHINA", "CHINA", 1994, 117704661),
("INDONESIA", "VIETNAM", 1994, 110447941),
("VIETNAM", "VIETNAM", 1994, 104634280),
("INDONESIA", "JAPAN", 1994, 94202309),
("INDIA", "INDIA", 1994, 87559304),
("CHINA", "VIETNAM", 1994, 79257773),
("INDIA", "VIETNAM", 1994, 74763775),
("CHINA", "JAPAN", 1994, 74223281),
("CHINA", "INDIA", 1994, 73710746),
("JAPAN", "INDIA", 1994, 73084308),
("CHINA", "INDONESIA", 1994, 71058355),
("JAPAN", "CHINA", 1994, 66486212),
("VIETNAM", "INDIA", 1994, 65195504),
("INDIA", "JAPAN", 1994, 61964866),
("INDIA", "CHINA", 1994, 54001697),
("JAPAN", "VIETNAM", 1994, 52353726),
("JAPAN", "JAPAN", 1994, 49841356),
("INDIA", "INDONESIA", 1994, 48115599),
("VIETNAM", "JAPAN", 1994, 44189643),
("VIETNAM", "INDONESIA", 1994, 36735867),
("JAPAN", "INDONESIA", 1994, 35787387),
("INDONESIA", "VIETNAM", 1995, 124703211),
("INDONESIA", "CHINA", 1995, 117855042),
("INDONESIA", "INDIA", 1995, 109188170),
("INDONESIA", "JAPAN", 1995, 108783269),
("INDIA", "CHINA", 1995, 97134968),
("CHINA", "CHINA", 1995, 93816347),
("INDONESIA", "INDONESIA", 1995, 93576365),
("INDIA", "INDIA", 1995, 91123995),
("CHINA", "VIETNAM", 1995, 90475804),
("INDIA", "INDONESIA", 1995, 69534495),
("VIETNAM", "JAPAN", 1995, 68797666),
("VIETNAM", "VIETNAM", 1995, 66580589),
("CHINA", "JAPAN", 1995, 66302585),
("VIETNAM", "INDIA", 1995, 62102121),
("JAPAN", "CHINA", 1995, 61193429),
("JAPAN", "VIETNAM", 1995, 58603113),
("CHINA", "INDONESIA", 1995, 58187663),
("JAPAN", "INDONESIA", 1995, 49743113),
("JAPAN", "JAPAN", 1995, 44550237),
("VIETNAM", "CHINA", 1995, 41392680),
("JAPAN", "INDIA", 1995, 41310627),
("INDIA", "JAPAN", 1995, 39744126),
("VIETNAM", "INDONESIA", 1995, 36856824),
("INDIA", "VIETNAM", 1995, 26912726),
("CHINA", "INDIA", 1995, 26410808),
("INDONESIA", "CHINA", 1996, 170564179),
("INDONESIA", "VIETNAM", 1996, 132832962),
("INDIA", "CHINA", 1996, 125561517),
("CHINA", "CHINA", 1996, 125063673),
("INDONESIA", "INDIA", 1996, 113425624),
("CHINA", "VIETNAM", 1996, 107966382),
("CHINA", "INDONESIA", 1996, 90778317),
("CHINA", "INDIA", 1996, 90587354),
("INDONESIA", "JAPAN", 1996, 83945841),
("JAPAN", "INDIA", 1996, 79834486),
("VIETNAM", "INDIA", 1996, 76217556),
("INDONESIA", "INDONESIA", 1996, 75239402),
("JAPAN", "CHINA", 1996, 70068123),
("VIETNAM", "CHINA", 1996, 68126043),
("VIETNAM", "VIETNAM", 1996, 62083854),
("INDIA", "VIETNAM", 1996, 59632099),
("JAPAN", "VIETNAM", 1996, 57798460),
("JAPAN", "INDONESIA", 1996, 56821991),
("CHINA", "JAPAN", 1996, 55813462),
("VIETNAM", "INDONESIA", 1996, 47757057),
("JAPAN", "JAPAN", 1996, 47539552),
("VIETNAM", "JAPAN", 1996, 46963772),
("INDIA", "INDIA", 1996, 45565344),
("INDIA", "JAPAN", 1996, 43954767),
("INDIA", "INDONESIA", 1996, 38874394),
("INDONESIA", "VIETNAM", 1997, 135976970),
("INDONESIA", "CHINA", 1997, 132991130),
("VIETNAM", "VIETNAM", 1997, 121862533),
("INDONESIA", "JAPAN", 1997, 104037763),
("CHINA", "INDIA", 1997, 94813262),
("CHINA", "CHINA", 1997, 91303155),
("INDONESIA", "INDONESIA", 1997, 90786987),
("CHINA", "VIETNAM", 1997, 83798439),
("INDONESIA", "INDIA", 1997, 79939537),
("JAPAN", "CHINA", 1997, 77980070),
("VIETNAM", "INDONESIA", 1997, 70148929),
("INDIA", "INDONESIA", 1997, 65059235),
("CHINA", "INDONESIA", 1997, 60725848),
("JAPAN", "VIETNAM", 1997, 59928838),
("JAPAN", "INDONESIA", 1997, 58928479),
("CHINA", "JAPAN", 1997, 56743739),
("INDIA", "VIETNAM", 1997, 53785095),
("INDIA", "INDIA", 1997, 51362167),
("VIETNAM", "CHINA", 1997, 51118956),
("INDIA", "CHINA", 1997, 46625339),
("VIETNAM", "JAPAN", 1997, 37131543),
("VIETNAM", "INDIA", 1997, 36662875),
("JAPAN", "INDIA", 1997, 33111440),
("JAPAN", "JAPAN", 1997, 26883620),
("INDIA", "JAPAN", 1997, 15498069),],
    8:[],
    9:[],
    10:[],
    11:[(1992, "ARGENTINA", 60236596),
(1992, "BRAZIL", 50116740),
(1992, "CANADA", 158594332),
(1992, "PERU", 122782525),
(1993, "ARGENTINA", 101488494),
(1993, "BRAZIL", 18300780),
(1993, "CANADA", 220466287),
(1993, "PERU", 138221761),
(1994, "ARGENTINA", 96762374),
(1994, "BRAZIL", 41192541),
(1994, "CANADA", 142838983),
(1994, "PERU", 81585186),
(1995, "ARGENTINA", 77733294),
(1995, "BRAZIL", 48321419),
(1995, "CANADA", 132723304),
(1995, "PERU", 120699540),
(1996, "ARGENTINA", 77775674),
(1996, "BRAZIL", 30868579),
(1996, "CANADA", 227015896),
(1996, "PERU", 82556019),
(1997, "ARGENTINA", 70072123),
(1997, "BRAZIL", 33222386),
(1997, "CANADA", 118280072),
(1997, "PERU", 68393464),
(1998, "ARGENTINA", 29902049),
(1998, "BRAZIL", 17035775),
(1998, "CANADA", 93061401),
(1998, "PERU", 41688725)],
    12:[(1997, "ARGENTINA", "MFGR#11", 6011959),
(1997, "ARGENTINA", "MFGR#12", 10930453),
(1997, "ARGENTINA", "MFGR#13", 7326904),
(1997, "ARGENTINA", "MFGR#15", 9178983),
(1997, "ARGENTINA", "MFGR#22", 4054952),
(1997, "ARGENTINA", "MFGR#23", 446826),
(1997, "ARGENTINA", "MFGR#24", 5812266),
(1997, "ARGENTINA", "MFGR#25", 3775371),
(1997, "BRAZIL", "MFGR#11", 14560154),
(1997, "BRAZIL", "MFGR#13", 4608821),
(1997, "BRAZIL", "MFGR#15", 6905625),
(1997, "BRAZIL", "MFGR#21", 3245284),
(1997, "BRAZIL", "MFGR#22", 7989912),
(1997, "BRAZIL", "MFGR#23", 2350639),
(1997, "BRAZIL", "MFGR#24", 5426027),
(1997, "BRAZIL", "MFGR#25", 8673663),
(1997, "CANADA", "MFGR#12", 2235973),
(1997, "CANADA", "MFGR#13", 11852524),
(1997, "CANADA", "MFGR#14", 3537619),
(1997, "CANADA", "MFGR#15", 19768006),
(1997, "CANADA", "MFGR#21", 529875),
(1997, "CANADA", "MFGR#22", 5832162),
(1997, "CANADA", "MFGR#23", 4225150),
(1997, "CANADA", "MFGR#24", 1169920),
(1997, "PERU", "MFGR#11", 13469329),
(1997, "PERU", "MFGR#12", 9877074),
(1997, "PERU", "MFGR#13", 2144336),
(1997, "PERU", "MFGR#14", 7772794),
(1997, "PERU", "MFGR#15", 2684121),
(1997, "PERU", "MFGR#21", 8676879),
(1997, "PERU", "MFGR#22", 4211661),
(1997, "PERU", "MFGR#23", 5841679),
(1997, "PERU", "MFGR#24", 4164126),
(1997, "PERU", "MFGR#25", 1726140),
(1997, "UNITED STATES", "MFGR#11", 13695414),
(1997, "UNITED STATES", "MFGR#12", 1570892),
(1997, "UNITED STATES", "MFGR#13", 7934509),
(1997, "UNITED STATES", "MFGR#14", 10506122),
(1997, "UNITED STATES", "MFGR#15", 13962239),
(1997, "UNITED STATES", "MFGR#21", 11608519),
(1997, "UNITED STATES", "MFGR#22", 1580597),
(1997, "UNITED STATES", "MFGR#23", 12019786),
(1997, "UNITED STATES", "MFGR#24", 2306608),
(1997, "UNITED STATES", "MFGR#25", 3766152),
(1998, "ARGENTINA", "MFGR#11", 6899027),
(1998, "ARGENTINA", "MFGR#15", 370411),
(1998, "ARGENTINA", "MFGR#23", 3551367),
(1998, "ARGENTINA", "MFGR#24", 4336186),
(1998, "ARGENTINA", "MFGR#25", 1685570),
(1998, "BRAZIL", "MFGR#11", 5658181),
(1998, "BRAZIL", "MFGR#22", 6949723),
(1998, "BRAZIL", "MFGR#23", 4321193),
(1998, "BRAZIL", "MFGR#25", 467968),
(1998, "CANADA", "MFGR#11", 2364615),
(1998, "CANADA", "MFGR#12", 4435133),
(1998, "CANADA", "MFGR#14", 9800212),
(1998, "CANADA", "MFGR#21", 2472544),
(1998, "CANADA", "MFGR#23", 3513299),
(1998, "CANADA", "MFGR#25", 5291352),
(1998, "PERU", "MFGR#12", 11097433),
(1998, "PERU", "MFGR#13", 4421179),
(1998, "PERU", "MFGR#15", 5199023),
(1998, "PERU", "MFGR#21", 16677298),
(1998, "PERU", "MFGR#22", 1765905),
(1998, "PERU", "MFGR#23", 4453957),
(1998, "PERU", "MFGR#24", 9196951),
(1998, "PERU", "MFGR#25", 8748774),
(1998, "UNITED STATES", "MFGR#11", 3987678),
(1998, "UNITED STATES", "MFGR#12", 6531198),
(1998, "UNITED STATES", "MFGR#14", 2347354),
(1998, "UNITED STATES", "MFGR#15", 5297506),
(1998, "UNITED STATES", "MFGR#21", 2829259),
(1998, "UNITED STATES", "MFGR#22", 13322385),
(1998, "UNITED STATES", "MFGR#23", 11917571),
(1998, "UNITED STATES", "MFGR#24", 8482069),
(1998, "UNITED STATES", "MFGR#25", 3295629)],
    13:[(1997, "UNITED ST2", "MFGR#1433", 2636841),
(1997, "UNITED ST6", "MFGR#1415", 5022945),
(1997, "UNITED ST7", "MFGR#1415", 2846336),
(1998, "UNITED ST0", "MFGR#1418", 1457152),
(1998, "UNITED ST2", "MFGR#1414", 442555),
(1998, "UNITED ST7", "MFGR#142", 306182),
(1998, "UNITED ST9", "MFGR#1433", 141465)]
}


class SSBMClient(threading.Thread):

    def __init__(self, port, query, i):
        threading.Thread.__init__ (self)
        self._port = port
        self._query = query
        self._i = i

    def run(self):
        client1 = pymonetdb.connect(database='master', port=self._port, autocommit=True)
        cur1 = client1.cursor()
        f = open(self._query, 'r')
        q = f.read()
        f.close()

        cur1.execute(q)
        res = cur1.fetchall()
        if res != results[self._i]:
            sys.stderr.write('Query %s with wrong result: %s\n' % (q, str(res)))

        cur1.close()
        client1.close()


masterport = freeport()
masterproc = None
workers = []
with tempfile.TemporaryDirectory() as tmpdir:
    os.mkdir(os.path.join(tmpdir, 'master'))
    with process.server(mapiport=masterport, dbname="master", dbfarm=os.path.join(tmpdir, 'master'), stdin = process.PIPE, stdout = process.PIPE, stderr=process.PIPE) as masterproc:
        masterconn = pymonetdb.connect(database='', port=masterport, autocommit=True)

        # split lineorder table into one file for each worker
        # this is as portable as an anvil
        lineordertbl = os.path.join(ssbmdatapath, 'lineorder.tbl')
        lineorderdir = os.path.join(tmpdir, 'lineorder')
        if os.path.exists(lineorderdir):
            import shutil
            shutil.rmtree(lineorderdir)
        if not os.path.exists(lineorderdir):
            os.makedirs(lineorderdir)
        inputData = open(lineordertbl, 'r').read().split('\n')
        linesperslice = len(inputData) // nworkers + 1
        i = 0
        for lines in range(0, len(inputData), linesperslice):
            outputData = inputData[lines:lines+linesperslice]
            outputStr = '\n'.join(outputData)
            if outputStr[-1] != '\n':
                outputStr += '\n'
            outputFile = open(os.path.join(lineorderdir, 'split-%d' % i), 'w')
            outputFile.write(outputStr)
            outputFile.close()
            i += 1
        loadsplits =  glob.glob(os.path.join(lineorderdir, 'split-*'))
        loadsplits.sort()

        # setup and start workers
        try:
            for i in range(nworkers):
                workerport = freeport()
                workerdbname = 'worker_%d' % i
                workerrec = {
                    'no'       : i,
                    'port'     : workerport,
                    'dbname'   : workerdbname,
                    'dbfarm'   : os.path.join(tmpdir, workerdbname),
                    'mapi'     : 'mapi:monetdb://localhost:%d/%s' % (workerport, workerdbname),
                    'split'    : loadsplits[i],
                    'repldata' : os.path.join(ssbmdatapath, 'date.tbl'),
                    'tpf'      : '_%d' % i
                }
                workers.append(workerrec)
                os.mkdir(workerrec['dbfarm'])
                workerrec['proc'] = process.server(mapiport=workerrec['port'], dbname=workerrec['dbname'], dbfarm=workerrec['dbfarm'], stdin = process.PIPE, stdout = process.PIPE, stderr=process.PIPE)
                workerrec['conn'] = pymonetdb.connect(database=workerrec['dbname'], port=workerrec['port'], autocommit=True)
                t = threading.Thread(target=worker_load, args = [workerrec])
                t.start()
                workerrec['loadthread'] = t

            # load dimension tables into master
            c = masterconn.cursor()
            c.execute(dimensiontabledef)
            c.execute("""
            COPY INTO SUPPLIER  FROM 'PWD/supplier.tbl';
            COPY INTO CUSTOMER  FROM 'PWD/customer.tbl';
            COPY INTO PART      FROM 'PWD/part.tbl';
            """.replace('PWD', ssbmdatapath.replace('\\', '\\\\')))

            # wait until they are finished loading
            for workerrec in workers:
                workerrec['loadthread'].join()

            # glue everything together on the master
            mtable = 'create merge table %s %s' % (shardtable, shardedtabledef)
            c.execute(mtable)
            rptable = 'create replica table %s %s' % (repltable, replicatedtabledef)
            c.execute(rptable)
            for workerrec in workers:
                rtable = "create remote table %s%s %s on '%s'" % (shardtable, workerrec['tpf'], shardedtabledef, workerrec['mapi'])
                atable = 'alter table %s add table %s%s' % (shardtable, shardtable, workerrec['tpf'])
                c.execute(rtable)
                c.execute(atable)
                rtable = "create remote table %s%s %s on '%s'" % (repltable, workerrec['tpf'], replicatedtabledef, workerrec['mapi'])
                atable = 'alter table %s add table %s%s' % (repltable, repltable, workerrec['tpf'])
                c.execute(rtable)
                c.execute(atable)

            # sanity check
            c.execute("select count(*) from lineorder_0")
            if c.fetchall()[0][0] != 12036:
                sys.stderr.write('12036 rows in remote table expected')

            c.execute("select count(*) from lineorder")
            if c.fetchall()[0][0] != 60175:
                sys.stderr.write('60175 rows in merge table expected')

            c.execute("select * from lineorder where lo_orderkey=356")
            if c.fetchall()[0][0] != 356:
                sys.stderr.write('356 rows expected')

            c.execute("select * from " + shardtable + workers[0]['tpf'] + " where lo_orderkey=356")
            if c.fetchall()[0][0] != 356:
                sys.stderr.write('356 rows expected')

            # run queries, use mclient so output is comparable
            queries = glob.glob(os.path.join(ssbmpath, '[0-1][0-9].sql'))
            queries.sort()

            clients = []
            i = 1
            for q in queries:
                onethread = SSBMClient(masterport, q, i)
                onethread.start()
                clients.append(onethread)
                i += 1
            for onethread in clients:
                onethread.join()

            for workerrec in workers:
                workerrec['proc'].communicate()
            masterproc.communicate()
        finally:
            for wrec in workers:
                p = wrec.get('proc')
                if p is not None:
                    p.terminate()
