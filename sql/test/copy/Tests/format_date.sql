CREATE TABLE orders_1 (o_orderkey INT NOT NULL, o_custkey INT NOT NULL, o_orderstatus VARCHAR(1) NOT NULL, o_totalprice FLOAT NOT NULL, o_orderdate DATE NOT NULL, o_orderpriority VARCHAR(15) NOT NULL, o_clerk VARCHAR(15) NOT NULL, o_shippriority INT NOT NULL, o_comment VARCHAR(79) NOT NULL) ;
CREATE TABLE orders_2 (o_orderkey INT NOT NULL, o_custkey INT NOT NULL, o_orderstatus VARCHAR(1) NOT NULL, o_totalprice FLOAT NOT NULL, o_orderdate DATE NOT NULL, o_orderpriority VARCHAR(15) NOT NULL, o_clerk VARCHAR(15) NOT NULL, o_shippriority INT NOT NULL, o_comment VARCHAR(79) NOT NULL) ;

COPY 150 RECORDS INTO orders_1 from STDIN USING DELIMITERS '|', '|\n';
1|370|O|172799.49|1996-01-02|5-LOW|Clerk#000000951|0|blithely final dolphins solve-- blithely blithe packages nag blith|
2|781|O|38426.09|1996-12-01|1-URGENT|Clerk#000000880|0|quickly regular depend|
3|1234|F|205654.30|1993-10-14|5-LOW|Clerk#000000955|0|deposits alongside of the dependencies are slowly about |
4|1369|O|56000.91|1995-10-11|5-LOW|Clerk#000000124|0|final requests detect slyly across the blithely bold pinto beans. eve|
5|445|F|105367.67|1994-07-30|5-LOW|Clerk#000000925|0|even deposits cajole furiously. quickly spe|
6|557|F|45523.10|1992-02-21|4-NOT SPECIFIED|Clerk#000000058|0|ironically bold asymptotes sleep blithely beyond the regular, clos|
7|392|O|271885.66|1996-01-10|2-HIGH|Clerk#000000470|0|ironic, regular deposits are. ironic foxes sl|
32|1301|O|198665.57|1995-07-16|2-HIGH|Clerk#000000616|0|slyly final foxes are slyly. packag|
33|670|F|146567.24|1993-10-27|3-MEDIUM|Clerk#000000409|0|packages maintain about the deposits; foxes hang after |
34|611|O|73315.48|1998-07-21|3-MEDIUM|Clerk#000000223|0|quickly express asymptotes use. carefully final packages sleep f|
35|1276|O|194641.93|1995-10-23|4-NOT SPECIFIED|Clerk#000000259|0|fluffily regular pinto beans |
36|1153|O|42011.04|1995-11-03|1-URGENT|Clerk#000000358|0|carefully ironic accounts nag|
37|862|F|131896.49|1992-06-03|3-MEDIUM|Clerk#000000456|0|express requests ar|
38|1249|O|71553.08|1996-08-21|4-NOT SPECIFIED|Clerk#000000604|0|slyly quick pinto beans detect flu|
39|818|O|326565.37|1996-09-20|3-MEDIUM|Clerk#000000659|0|furiously unusual pinto beans above the furiously ironic asymptot|
64|322|F|35831.73|1994-07-16|3-MEDIUM|Clerk#000000661|0|final deposits nag. blithely special deposits a|
65|163|P|95469.44|1995-03-18|1-URGENT|Clerk#000000632|0|furiously even platelets boost ironic theodolites. even |
66|1292|F|104190.66|1994-01-20|5-LOW|Clerk#000000743|0|ironic requests are quickly about the carefully unusual a|
67|568|O|182481.16|1996-12-19|4-NOT SPECIFIED|Clerk#000000547|0|regular, bold foxes across the even requests detect a|
68|286|O|301968.79|1998-04-18|3-MEDIUM|Clerk#000000440|0|stealthy decoys nag; furiously|
69|845|F|204110.73|1994-06-04|4-NOT SPECIFIED|Clerk#000000330|0|carefully regular theodolites exce|
70|644|F|125705.32|1993-12-18|5-LOW|Clerk#000000322|0|blithely unusual pack|
71|34|O|260603.38|1998-01-24|4-NOT SPECIFIED|Clerk#000000271|0|furiously ironic dolphins sleep slyly. carefully special notornis cajole c|
96|1078|F|64364.30|1994-04-17|2-HIGH|Clerk#000000395|0|carefully regular accounts |
97|211|F|100572.55|1993-01-29|3-MEDIUM|Clerk#000000547|0|carefully even packages believe sly|
98|1045|F|71721.40|1994-09-25|1-URGENT|Clerk#000000448|0|carefully even dinos sleep blithely. regular, bold deposits|
99|890|F|108594.87|1994-03-13|4-NOT SPECIFIED|Clerk#000000973|0|carefully regular theodolites may believe unu|
100|1471|O|198978.27|1998-02-28|4-NOT SPECIFIED|Clerk#000000577|0|regular deposits sleep closely regular, regular packages. carefully si|
101|280|O|118448.39|1996-03-17|3-MEDIUM|Clerk#000000419|0|blithely ironic accounts lose slyly about the pending, regular accounts|
102|8|O|184806.58|1997-05-09|2-HIGH|Clerk#000000596|0|unusual deposits dazzle furiously blithely regular pinto beans. pending foxes|
103|292|O|118745.16|1996-06-20|4-NOT SPECIFIED|Clerk#000000090|0|carefully ironic deposits are quickly blithely even|
128|740|F|34997.04|1992-06-15|1-URGENT|Clerk#000000385|0|carefully special e|
129|712|F|254281.41|1992-11-19|5-LOW|Clerk#000000859|0|slyly bold dolphins cajole c|
130|370|F|140213.54|1992-05-08|2-HIGH|Clerk#000000036|0|slyly final accounts among|
131|928|F|140726.47|1994-06-08|3-MEDIUM|Clerk#000000625|0|special courts wake blithely accordin|
132|265|F|133485.89|1993-06-11|3-MEDIUM|Clerk#000000488|0|ironic platelets according to the evenly regula|
133|440|O|95971.06|1997-11-29|1-URGENT|Clerk#000000738|0|slyly silent deposits haggle carefully fluffi|
134|62|F|208201.46|1992-05-01|4-NOT SPECIFIED|Clerk#000000711|0|silently even deposits wake about the fluff|
135|605|O|230472.84|1995-10-21|4-NOT SPECIFIED|Clerk#000000804|0|accounts cajole. final, pending dependencies a|
160|826|O|114742.32|1996-12-19|4-NOT SPECIFIED|Clerk#000000342|0|unusual dependencie|
161|167|F|17668.60|1994-08-31|2-HIGH|Clerk#000000322|0|ironic, even attainments cajole closely|
162|142|O|3553.15|1995-05-08|3-MEDIUM|Clerk#000000378|0|instructions nag slyly. fluffily ironic sau|
163|878|O|177809.13|1997-09-05|3-MEDIUM|Clerk#000000379|0|carefully express pinto beans serve carefully final as|
164|8|F|250417.20|1992-10-21|5-LOW|Clerk#000000209|0|fluffily unusual requests al|
165|274|F|193302.35|1993-01-30|4-NOT SPECIFIED|Clerk#000000292|0|furiously enticing accounts cajole sometimes. slyly express plat|
166|1079|O|158207.39|1995-09-12|2-HIGH|Clerk#000000440|0|bold dependencies wake furiously regula|
167|1195|F|64017.85|1993-01-04|4-NOT SPECIFIED|Clerk#000000731|0|express warhorses wake carefully furiously ironic deposits. c|
192|826|O|194637.57|1997-11-25|5-LOW|Clerk#000000483|0|silent requests above the furiously even pinto beans sleep bl|
193|791|F|80834.26|1993-08-08|1-URGENT|Clerk#000000025|0|slyly blithe instructions cajole carefully ironic, fina|
194|619|F|154284.73|1992-04-05|3-MEDIUM|Clerk#000000352|0|carefully dogged excuses use abou|
195|1355|F|216638.92|1993-12-28|3-MEDIUM|Clerk#000000216|0|ironic, final notornis are fluffily across the carefull|
196|649|F|38660.64|1993-03-17|2-HIGH|Clerk#000000988|0|even deposits wake |
197|326|P|155247.48|1995-04-07|2-HIGH|Clerk#000000969|0|theodolites above the furiously regular deposits sleep blithely abo|
198|1103|O|149551.63|1998-01-02|4-NOT SPECIFIED|Clerk#000000331|0|deposits haggle carefully after the furiously fi|
199|530|O|95867.70|1996-03-07|2-HIGH|Clerk#000000489|0|unusual, regular requests c|
224|25|F|234050.44|1994-06-18|4-NOT SPECIFIED|Clerk#000000642|0|quickly final accounts use even requests. ironic ac|
225|331|P|226028.98|1995-05-25|1-URGENT|Clerk#000000177|0|blithely express cou|
226|1276|F|256459.40|1993-03-10|2-HIGH|Clerk#000000756|0|even, ironic theodolites detect fluffily final instructions-- fi|
227|100|O|69020.68|1995-11-10|5-LOW|Clerk#000000919|0|asymptotes are special, special requests. spec|
228|442|F|2744.06|1993-02-25|1-URGENT|Clerk#000000562|0|blithely ironic requests boost pending theodolites. even deposits affix fluf|
229|1118|F|195619.74|1993-12-29|1-URGENT|Clerk#000000628|0|blithely thin requests along the fluffily regular packages e|
230|1027|F|147711.01|1993-10-27|1-URGENT|Clerk#000000520|0|ironic, silent tithes wake carefully until the even theodolites. special|
231|910|F|234383.86|1994-09-29|2-HIGH|Clerk#000000446|0|express requests use always at the unusual deposits. silently final acc|
256|1249|F|132718.67|1993-10-19|4-NOT SPECIFIED|Clerk#000000834|0|special dependencies boost furiously. pendin|
257|1228|O|9255.12|1998-03-28|3-MEDIUM|Clerk#000000680|0|final, regular packages nag furiously fluffily f|
258|419|F|259466.78|1993-12-29|1-URGENT|Clerk#000000167|0|regularly ironic grouches against the quickly express p|
259|433|F|110611.59|1993-09-29|4-NOT SPECIFIED|Clerk#000000601|0|ironic packages haggle among the furiously brave deposits. final, final d|
260|1048|O|268084.69|1996-12-10|3-MEDIUM|Clerk#000000960|0|quickly special ideas against the furiously final accounts affix deposits. sl|
261|461|F|278279.30|1993-06-29|3-MEDIUM|Clerk#000000310|0|final accounts nag fluffily about|
262|304|O|173401.63|1995-11-25|4-NOT SPECIFIED|Clerk#000000551|0|express, regular theodolites wake special instructions. slyly express |
263|1162|F|104961.32|1994-05-17|2-HIGH|Clerk#000000088|0|fluffily final ideas use quickly slyly final foxes? fluffily express dolphi|
288|71|O|239366.68|1997-02-21|1-URGENT|Clerk#000000109|0|quickly ruthless instructions cajole |
289|1039|O|174624.55|1997-02-10|3-MEDIUM|Clerk#000000103|0|slyly express excuses d|
290|1180|F|67636.54|1994-01-01|4-NOT SPECIFIED|Clerk#000000735|0|pending instructions against the furiously express d|
291|1411|F|88375.89|1994-03-13|1-URGENT|Clerk#000000923|0|express requests according to the carefully regular deposits run|
292|223|F|54152.77|1992-01-13|2-HIGH|Clerk#000000193|0|furiously special theodolites wake blith|
293|301|F|46128.56|1992-10-02|2-HIGH|Clerk#000000629|0|regular instructions grow bold, u|
294|505|F|46889.54|1993-07-16|3-MEDIUM|Clerk#000000499|0|idly ironic deposits must have to haggle deposits. blithel|
295|190|F|148569.49|1994-09-29|2-HIGH|Clerk#000000155|0|doggedly final requests nag carefull|
320|4|O|50202.60|1997-11-21|2-HIGH|Clerk#000000573|0|carefully silent ideas do solve final, express instructions. quickly final p|
321|1226|F|73024.50|1993-03-21|3-MEDIUM|Clerk#000000289|0|quickly silent requests affix sl|
322|1336|F|165992.05|1992-03-19|1-URGENT|Clerk#000000158|0|carefully unusual pinto beans lose carefully. even instructions ac|
323|392|F|121127.17|1994-03-26|1-URGENT|Clerk#000000959|0|even, regular instructions|
324|1052|F|46327.90|1992-03-20|1-URGENT|Clerk#000000352|0|regular theodolites boost quickly along the ironic, quick realms.|
325|401|F|94638.59|1993-10-17|5-LOW|Clerk#000000844|0|carefully fluffy forges about the express, ir|
326|760|O|325448.68|1995-06-04|2-HIGH|Clerk#000000466|0|regular theodolites was car|
327|1447|P|32302.12|1995-04-17|5-LOW|Clerk#000000992|0|fluffily ironic deposits across the ironically regular ideas are |
352|1066|F|25542.02|1994-03-08|2-HIGH|Clerk#000000932|0|regular, regular pinto beans haggle sly|
353|19|F|224983.69|1993-12-31|5-LOW|Clerk#000000449|0|even ideas haggle excuses? slyly ironic packages wake alongside of the qu|
354|1384|O|231311.22|1996-03-14|2-HIGH|Clerk#000000511|0|quickly special packages inside the slyly unusual pain|
355|701|F|103949.82|1994-06-14|5-LOW|Clerk#000000532|0|carefully even instructio|
356|1469|F|189160.02|1994-06-30|4-NOT SPECIFIED|Clerk#000000944|0|fluffily pending theo|
357|604|O|138936.83|1996-10-09|2-HIGH|Clerk#000000301|0|carefully bold theodolites cajole f|
358|23|F|362024.17|1993-09-20|2-HIGH|Clerk#000000392|0|deposits sublate carefully at t|
359|776|F|214770.97|1994-12-19|3-MEDIUM|Clerk#000000934|0|furiously final foxes are. regular,|
384|1132|F|191275.12|1992-03-03|5-LOW|Clerk#000000206|0|regular packages haggle furiously; idle requests wake carefu|
385|331|O|75866.47|1996-03-22|5-LOW|Clerk#000000600|0|asymptotes wake silent, silent|
386|602|F|119718.02|1995-01-25|2-HIGH|Clerk#000000648|0|quickly pending instructions unwind furiously theodolites. final package|
387|34|O|197839.44|1997-01-26|4-NOT SPECIFIED|Clerk#000000768|0|blithely even accounts according to the even packag|
388|448|F|161560.04|1992-12-16|4-NOT SPECIFIED|Clerk#000000356|0|accounts wake against the braids. silent accounts snooze slyly blithely ironi|
389|1270|F|3266.69|1994-02-17|2-HIGH|Clerk#000000062|0|pending, bold packages boost blithely final package|
390|1027|O|232256.36|1998-04-07|5-LOW|Clerk#000000404|0|blithely even pinto beans against the ironic packages boost qu|
391|1103|F|14517.91|1994-11-17|2-HIGH|Clerk#000000256|0|furiously special deposits wake blithely. qu|
416|403|F|106818.50|1993-09-27|5-LOW|Clerk#000000294|0|excuses boost permanently around the carefully pe|
417|547|F|132531.73|1994-02-06|3-MEDIUM|Clerk#000000468|0|pending, regular pinto beans after the final, express accounts boost|
418|949|P|39431.46|1995-04-13|4-NOT SPECIFIED|Clerk#000000643|0|quiet, bold ideas a|
419|1163|O|159079.22|1996-10-01|3-MEDIUM|Clerk#000000376|0|accounts sleep quickly slyly bo|
420|902|O|269064.47|1995-10-31|4-NOT SPECIFIED|Clerk#000000756|0|slyly final deposits sublate after the quickly pending deposits|
421|392|F|1292.21|1992-02-22|5-LOW|Clerk#000000405|0|ironic, even account|
422|731|O|155533.71|1997-05-31|4-NOT SPECIFIED|Clerk#000000049|0|carefully even packages use|
423|1034|O|31900.60|1996-06-01|1-URGENT|Clerk#000000674|0|blithely unusual dugouts play quickly along the blithely regular theo|
448|1498|O|157247.56|1995-08-21|3-MEDIUM|Clerk#000000597|0|furiously even requests nag carefully. |
449|958|O|55082.33|1995-07-20|2-HIGH|Clerk#000000841|0|final, express requests sleep permanent requests. spe|
450|475|P|213638.07|1995-03-05|4-NOT SPECIFIED|Clerk#000000293|0|deposits wake regular, ironic instructions. bli|
451|988|O|142756.81|1998-05-25|5-LOW|Clerk#000000048|0|final foxes nag. regul|
452|596|O|2072.79|1997-10-14|1-URGENT|Clerk#000000498|0|theodolites should n|
453|442|O|343004.49|1997-05-26|5-LOW|Clerk#000000504|0|furiously even deposits use inside the excuses.|
454|488|O|24543.95|1995-12-27|5-LOW|Clerk#000000890|0|fluffily final accounts after the special, ironic pinto |
455|121|O|190711.32|1996-12-04|1-URGENT|Clerk#000000796|0|even instructions hagg|
480|715|F|30644.49|1993-05-08|5-LOW|Clerk#000000004|0|final accounts poach carefully. quickly final platelets boost quickly even ide|
481|304|F|160370.14|1992-10-08|2-HIGH|Clerk#000000230|0|ruthlessly ironic packages nag furiously across the slyly regula|
482|1252|O|197194.23|1996-03-26|1-URGENT|Clerk#000000295|0|blithely regular as|
483|349|O|66194.38|1995-07-11|2-HIGH|Clerk#000000025|0|bold theodolites sl|
484|544|O|331553.32|1997-01-03|3-MEDIUM|Clerk#000000545|0|fluffily even deposits run foxes; regular packages afte|
485|1006|O|142389.70|1997-03-26|2-HIGH|Clerk#000000105|0|regular, bold asymptotes sleep boldly. carefu|
486|509|O|286150.09|1996-03-11|4-NOT SPECIFIED|Clerk#000000803|0|quickly final foxes across the expre|
487|1079|F|88805.07|1992-08-18|1-URGENT|Clerk#000000086|0|ironic, express pinto be|
512|631|P|183939.48|1995-05-20|5-LOW|Clerk#000000814|0|quickly unusual foxes was fluffily slyly even accounts. pac|
513|607|O|78769.71|1995-05-01|2-HIGH|Clerk#000000522|0|always final sentiments haggle furiously around the fluffily ruthles|
514|749|O|123202.51|1996-04-04|2-HIGH|Clerk#000000094|0|enticingly quick escapades wake slyly. final acc|
515|1420|F|177231.12|1993-08-29|4-NOT SPECIFIED|Clerk#000000700|0|slyly unusual ideas subla|
516|440|O|13277.79|1998-04-21|2-HIGH|Clerk#000000305|0|quickly final foxes accord|
517|94|O|109269.47|1997-04-07|5-LOW|Clerk#000000359|0|deposits wake always slyly regular requests. blithely |
518|1444|O|335285.37|1998-02-08|2-HIGH|Clerk#000000768|0|slyly even ideas hang quickly. carefully final instructi|
519|631|O|109395.60|1997-10-31|1-URGENT|Clerk#000000985|0|quick depths are! slyly express requests along the carefully ironic |
544|934|F|58960.45|1993-02-17|2-HIGH|Clerk#000000145|0|slyly ironic attainments sleep blith|
545|632|O|35129.54|1995-11-07|2-HIGH|Clerk#000000537|0|even, regular packa|
546|1433|O|26227.74|1996-11-01|2-HIGH|Clerk#000000041|0|final notornis detect slyly fluffily express deposits. brav|
547|983|O|137852.72|1996-06-22|3-MEDIUM|Clerk#000000976|0|bold instructions print fluffily carefully id|
548|1240|F|139094.89|1994-09-21|1-URGENT|Clerk#000000435|0|quickly regular accounts daz|
549|1100|F|211787.30|1992-07-13|1-URGENT|Clerk#000000196|0|carefully regular foxes integrate ironic, fina|
550|236|O|54818.45|1995-08-02|1-URGENT|Clerk#000000204|0|carefully even asymptotes sleep furiously sp|
551|898|O|64301.40|1995-05-30|1-URGENT|Clerk#000000179|0|unusual, final accounts use above the special excuses. final depo|
576|296|O|24722.97|1997-05-13|3-MEDIUM|Clerk#000000955|0|pending theodolites about the carefu|
577|553|F|47860.53|1994-12-19|5-LOW|Clerk#000000154|0|blithely unusual packages sl|
578|926|O|103543.00|1997-01-10|5-LOW|Clerk#000000281|0|blithely pending asymptotes wake quickly across the carefully final|
579|671|O|146610.11|1998-03-11|2-HIGH|Clerk#000000862|0|slyly even requests cajole slyly. sil|
580|593|O|144557.44|1997-07-05|2-HIGH|Clerk#000000314|0|final ideas must have to are carefully quickly furious requests|
581|688|O|175985.28|1997-02-23|4-NOT SPECIFIED|Clerk#000000239|0|carefully regular dolphins cajole ruthlessl|
582|494|O|181813.20|1997-10-21|1-URGENT|Clerk#000000378|0|quietly ironic pinto beans wake carefully. ironic accounts across the dol|


COPY 150 RECORDS INTO orders_2 from STDIN (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate '%Y-%m-%d', o_orderpriority, o_clerk, o_shippriority, o_comment ) USING DELIMITERS '|', '|\n';
1|370|O|172799.49|1996-01-02|5-LOW|Clerk#000000951|0|blithely final dolphins solve-- blithely blithe packages nag blith|
2|781|O|38426.09|1996-12-01|1-URGENT|Clerk#000000880|0|quickly regular depend|
3|1234|F|205654.30|1993-10-14|5-LOW|Clerk#000000955|0|deposits alongside of the dependencies are slowly about |
4|1369|O|56000.91|1995-10-11|5-LOW|Clerk#000000124|0|final requests detect slyly across the blithely bold pinto beans. eve|
5|445|F|105367.67|1994-07-30|5-LOW|Clerk#000000925|0|even deposits cajole furiously. quickly spe|
6|557|F|45523.10|1992-02-21|4-NOT SPECIFIED|Clerk#000000058|0|ironically bold asymptotes sleep blithely beyond the regular, clos|
7|392|O|271885.66|1996-01-10|2-HIGH|Clerk#000000470|0|ironic, regular deposits are. ironic foxes sl|
32|1301|O|198665.57|1995-07-16|2-HIGH|Clerk#000000616|0|slyly final foxes are slyly. packag|
33|670|F|146567.24|1993-10-27|3-MEDIUM|Clerk#000000409|0|packages maintain about the deposits; foxes hang after |
34|611|O|73315.48|1998-07-21|3-MEDIUM|Clerk#000000223|0|quickly express asymptotes use. carefully final packages sleep f|
35|1276|O|194641.93|1995-10-23|4-NOT SPECIFIED|Clerk#000000259|0|fluffily regular pinto beans |
36|1153|O|42011.04|1995-11-03|1-URGENT|Clerk#000000358|0|carefully ironic accounts nag|
37|862|F|131896.49|1992-06-03|3-MEDIUM|Clerk#000000456|0|express requests ar|
38|1249|O|71553.08|1996-08-21|4-NOT SPECIFIED|Clerk#000000604|0|slyly quick pinto beans detect flu|
39|818|O|326565.37|1996-09-20|3-MEDIUM|Clerk#000000659|0|furiously unusual pinto beans above the furiously ironic asymptot|
64|322|F|35831.73|1994-07-16|3-MEDIUM|Clerk#000000661|0|final deposits nag. blithely special deposits a|
65|163|P|95469.44|1995-03-18|1-URGENT|Clerk#000000632|0|furiously even platelets boost ironic theodolites. even |
66|1292|F|104190.66|1994-01-20|5-LOW|Clerk#000000743|0|ironic requests are quickly about the carefully unusual a|
67|568|O|182481.16|1996-12-19|4-NOT SPECIFIED|Clerk#000000547|0|regular, bold foxes across the even requests detect a|
68|286|O|301968.79|1998-04-18|3-MEDIUM|Clerk#000000440|0|stealthy decoys nag; furiously|
69|845|F|204110.73|1994-06-04|4-NOT SPECIFIED|Clerk#000000330|0|carefully regular theodolites exce|
70|644|F|125705.32|1993-12-18|5-LOW|Clerk#000000322|0|blithely unusual pack|
71|34|O|260603.38|1998-01-24|4-NOT SPECIFIED|Clerk#000000271|0|furiously ironic dolphins sleep slyly. carefully special notornis cajole c|
96|1078|F|64364.30|1994-04-17|2-HIGH|Clerk#000000395|0|carefully regular accounts |
97|211|F|100572.55|1993-01-29|3-MEDIUM|Clerk#000000547|0|carefully even packages believe sly|
98|1045|F|71721.40|1994-09-25|1-URGENT|Clerk#000000448|0|carefully even dinos sleep blithely. regular, bold deposits|
99|890|F|108594.87|1994-03-13|4-NOT SPECIFIED|Clerk#000000973|0|carefully regular theodolites may believe unu|
100|1471|O|198978.27|1998-02-28|4-NOT SPECIFIED|Clerk#000000577|0|regular deposits sleep closely regular, regular packages. carefully si|
101|280|O|118448.39|1996-03-17|3-MEDIUM|Clerk#000000419|0|blithely ironic accounts lose slyly about the pending, regular accounts|
102|8|O|184806.58|1997-05-09|2-HIGH|Clerk#000000596|0|unusual deposits dazzle furiously blithely regular pinto beans. pending foxes|
103|292|O|118745.16|1996-06-20|4-NOT SPECIFIED|Clerk#000000090|0|carefully ironic deposits are quickly blithely even|
128|740|F|34997.04|1992-06-15|1-URGENT|Clerk#000000385|0|carefully special e|
129|712|F|254281.41|1992-11-19|5-LOW|Clerk#000000859|0|slyly bold dolphins cajole c|
130|370|F|140213.54|1992-05-08|2-HIGH|Clerk#000000036|0|slyly final accounts among|
131|928|F|140726.47|1994-06-08|3-MEDIUM|Clerk#000000625|0|special courts wake blithely accordin|
132|265|F|133485.89|1993-06-11|3-MEDIUM|Clerk#000000488|0|ironic platelets according to the evenly regula|
133|440|O|95971.06|1997-11-29|1-URGENT|Clerk#000000738|0|slyly silent deposits haggle carefully fluffi|
134|62|F|208201.46|1992-05-01|4-NOT SPECIFIED|Clerk#000000711|0|silently even deposits wake about the fluff|
135|605|O|230472.84|1995-10-21|4-NOT SPECIFIED|Clerk#000000804|0|accounts cajole. final, pending dependencies a|
160|826|O|114742.32|1996-12-19|4-NOT SPECIFIED|Clerk#000000342|0|unusual dependencie|
161|167|F|17668.60|1994-08-31|2-HIGH|Clerk#000000322|0|ironic, even attainments cajole closely|
162|142|O|3553.15|1995-05-08|3-MEDIUM|Clerk#000000378|0|instructions nag slyly. fluffily ironic sau|
163|878|O|177809.13|1997-09-05|3-MEDIUM|Clerk#000000379|0|carefully express pinto beans serve carefully final as|
164|8|F|250417.20|1992-10-21|5-LOW|Clerk#000000209|0|fluffily unusual requests al|
165|274|F|193302.35|1993-01-30|4-NOT SPECIFIED|Clerk#000000292|0|furiously enticing accounts cajole sometimes. slyly express plat|
166|1079|O|158207.39|1995-09-12|2-HIGH|Clerk#000000440|0|bold dependencies wake furiously regula|
167|1195|F|64017.85|1993-01-04|4-NOT SPECIFIED|Clerk#000000731|0|express warhorses wake carefully furiously ironic deposits. c|
192|826|O|194637.57|1997-11-25|5-LOW|Clerk#000000483|0|silent requests above the furiously even pinto beans sleep bl|
193|791|F|80834.26|1993-08-08|1-URGENT|Clerk#000000025|0|slyly blithe instructions cajole carefully ironic, fina|
194|619|F|154284.73|1992-04-05|3-MEDIUM|Clerk#000000352|0|carefully dogged excuses use abou|
195|1355|F|216638.92|1993-12-28|3-MEDIUM|Clerk#000000216|0|ironic, final notornis are fluffily across the carefull|
196|649|F|38660.64|1993-03-17|2-HIGH|Clerk#000000988|0|even deposits wake |
197|326|P|155247.48|1995-04-07|2-HIGH|Clerk#000000969|0|theodolites above the furiously regular deposits sleep blithely abo|
198|1103|O|149551.63|1998-01-02|4-NOT SPECIFIED|Clerk#000000331|0|deposits haggle carefully after the furiously fi|
199|530|O|95867.70|1996-03-07|2-HIGH|Clerk#000000489|0|unusual, regular requests c|
224|25|F|234050.44|1994-06-18|4-NOT SPECIFIED|Clerk#000000642|0|quickly final accounts use even requests. ironic ac|
225|331|P|226028.98|1995-05-25|1-URGENT|Clerk#000000177|0|blithely express cou|
226|1276|F|256459.40|1993-03-10|2-HIGH|Clerk#000000756|0|even, ironic theodolites detect fluffily final instructions-- fi|
227|100|O|69020.68|1995-11-10|5-LOW|Clerk#000000919|0|asymptotes are special, special requests. spec|
228|442|F|2744.06|1993-02-25|1-URGENT|Clerk#000000562|0|blithely ironic requests boost pending theodolites. even deposits affix fluf|
229|1118|F|195619.74|1993-12-29|1-URGENT|Clerk#000000628|0|blithely thin requests along the fluffily regular packages e|
230|1027|F|147711.01|1993-10-27|1-URGENT|Clerk#000000520|0|ironic, silent tithes wake carefully until the even theodolites. special|
231|910|F|234383.86|1994-09-29|2-HIGH|Clerk#000000446|0|express requests use always at the unusual deposits. silently final acc|
256|1249|F|132718.67|1993-10-19|4-NOT SPECIFIED|Clerk#000000834|0|special dependencies boost furiously. pendin|
257|1228|O|9255.12|1998-03-28|3-MEDIUM|Clerk#000000680|0|final, regular packages nag furiously fluffily f|
258|419|F|259466.78|1993-12-29|1-URGENT|Clerk#000000167|0|regularly ironic grouches against the quickly express p|
259|433|F|110611.59|1993-09-29|4-NOT SPECIFIED|Clerk#000000601|0|ironic packages haggle among the furiously brave deposits. final, final d|
260|1048|O|268084.69|1996-12-10|3-MEDIUM|Clerk#000000960|0|quickly special ideas against the furiously final accounts affix deposits. sl|
261|461|F|278279.30|1993-06-29|3-MEDIUM|Clerk#000000310|0|final accounts nag fluffily about|
262|304|O|173401.63|1995-11-25|4-NOT SPECIFIED|Clerk#000000551|0|express, regular theodolites wake special instructions. slyly express |
263|1162|F|104961.32|1994-05-17|2-HIGH|Clerk#000000088|0|fluffily final ideas use quickly slyly final foxes? fluffily express dolphi|
288|71|O|239366.68|1997-02-21|1-URGENT|Clerk#000000109|0|quickly ruthless instructions cajole |
289|1039|O|174624.55|1997-02-10|3-MEDIUM|Clerk#000000103|0|slyly express excuses d|
290|1180|F|67636.54|1994-01-01|4-NOT SPECIFIED|Clerk#000000735|0|pending instructions against the furiously express d|
291|1411|F|88375.89|1994-03-13|1-URGENT|Clerk#000000923|0|express requests according to the carefully regular deposits run|
292|223|F|54152.77|1992-01-13|2-HIGH|Clerk#000000193|0|furiously special theodolites wake blith|
293|301|F|46128.56|1992-10-02|2-HIGH|Clerk#000000629|0|regular instructions grow bold, u|
294|505|F|46889.54|1993-07-16|3-MEDIUM|Clerk#000000499|0|idly ironic deposits must have to haggle deposits. blithel|
295|190|F|148569.49|1994-09-29|2-HIGH|Clerk#000000155|0|doggedly final requests nag carefull|
320|4|O|50202.60|1997-11-21|2-HIGH|Clerk#000000573|0|carefully silent ideas do solve final, express instructions. quickly final p|
321|1226|F|73024.50|1993-03-21|3-MEDIUM|Clerk#000000289|0|quickly silent requests affix sl|
322|1336|F|165992.05|1992-03-19|1-URGENT|Clerk#000000158|0|carefully unusual pinto beans lose carefully. even instructions ac|
323|392|F|121127.17|1994-03-26|1-URGENT|Clerk#000000959|0|even, regular instructions|
324|1052|F|46327.90|1992-03-20|1-URGENT|Clerk#000000352|0|regular theodolites boost quickly along the ironic, quick realms.|
325|401|F|94638.59|1993-10-17|5-LOW|Clerk#000000844|0|carefully fluffy forges about the express, ir|
326|760|O|325448.68|1995-06-04|2-HIGH|Clerk#000000466|0|regular theodolites was car|
327|1447|P|32302.12|1995-04-17|5-LOW|Clerk#000000992|0|fluffily ironic deposits across the ironically regular ideas are |
352|1066|F|25542.02|1994-03-08|2-HIGH|Clerk#000000932|0|regular, regular pinto beans haggle sly|
353|19|F|224983.69|1993-12-31|5-LOW|Clerk#000000449|0|even ideas haggle excuses? slyly ironic packages wake alongside of the qu|
354|1384|O|231311.22|1996-03-14|2-HIGH|Clerk#000000511|0|quickly special packages inside the slyly unusual pain|
355|701|F|103949.82|1994-06-14|5-LOW|Clerk#000000532|0|carefully even instructio|
356|1469|F|189160.02|1994-06-30|4-NOT SPECIFIED|Clerk#000000944|0|fluffily pending theo|
357|604|O|138936.83|1996-10-09|2-HIGH|Clerk#000000301|0|carefully bold theodolites cajole f|
358|23|F|362024.17|1993-09-20|2-HIGH|Clerk#000000392|0|deposits sublate carefully at t|
359|776|F|214770.97|1994-12-19|3-MEDIUM|Clerk#000000934|0|furiously final foxes are. regular,|
384|1132|F|191275.12|1992-03-03|5-LOW|Clerk#000000206|0|regular packages haggle furiously; idle requests wake carefu|
385|331|O|75866.47|1996-03-22|5-LOW|Clerk#000000600|0|asymptotes wake silent, silent|
386|602|F|119718.02|1995-01-25|2-HIGH|Clerk#000000648|0|quickly pending instructions unwind furiously theodolites. final package|
387|34|O|197839.44|1997-01-26|4-NOT SPECIFIED|Clerk#000000768|0|blithely even accounts according to the even packag|
388|448|F|161560.04|1992-12-16|4-NOT SPECIFIED|Clerk#000000356|0|accounts wake against the braids. silent accounts snooze slyly blithely ironi|
389|1270|F|3266.69|1994-02-17|2-HIGH|Clerk#000000062|0|pending, bold packages boost blithely final package|
390|1027|O|232256.36|1998-04-07|5-LOW|Clerk#000000404|0|blithely even pinto beans against the ironic packages boost qu|
391|1103|F|14517.91|1994-11-17|2-HIGH|Clerk#000000256|0|furiously special deposits wake blithely. qu|
416|403|F|106818.50|1993-09-27|5-LOW|Clerk#000000294|0|excuses boost permanently around the carefully pe|
417|547|F|132531.73|1994-02-06|3-MEDIUM|Clerk#000000468|0|pending, regular pinto beans after the final, express accounts boost|
418|949|P|39431.46|1995-04-13|4-NOT SPECIFIED|Clerk#000000643|0|quiet, bold ideas a|
419|1163|O|159079.22|1996-10-01|3-MEDIUM|Clerk#000000376|0|accounts sleep quickly slyly bo|
420|902|O|269064.47|1995-10-31|4-NOT SPECIFIED|Clerk#000000756|0|slyly final deposits sublate after the quickly pending deposits|
421|392|F|1292.21|1992-02-22|5-LOW|Clerk#000000405|0|ironic, even account|
422|731|O|155533.71|1997-05-31|4-NOT SPECIFIED|Clerk#000000049|0|carefully even packages use|
423|1034|O|31900.60|1996-06-01|1-URGENT|Clerk#000000674|0|blithely unusual dugouts play quickly along the blithely regular theo|
448|1498|O|157247.56|1995-08-21|3-MEDIUM|Clerk#000000597|0|furiously even requests nag carefully. |
449|958|O|55082.33|1995-07-20|2-HIGH|Clerk#000000841|0|final, express requests sleep permanent requests. spe|
450|475|P|213638.07|1995-03-05|4-NOT SPECIFIED|Clerk#000000293|0|deposits wake regular, ironic instructions. bli|
451|988|O|142756.81|1998-05-25|5-LOW|Clerk#000000048|0|final foxes nag. regul|
452|596|O|2072.79|1997-10-14|1-URGENT|Clerk#000000498|0|theodolites should n|
453|442|O|343004.49|1997-05-26|5-LOW|Clerk#000000504|0|furiously even deposits use inside the excuses.|
454|488|O|24543.95|1995-12-27|5-LOW|Clerk#000000890|0|fluffily final accounts after the special, ironic pinto |
455|121|O|190711.32|1996-12-04|1-URGENT|Clerk#000000796|0|even instructions hagg|
480|715|F|30644.49|1993-05-08|5-LOW|Clerk#000000004|0|final accounts poach carefully. quickly final platelets boost quickly even ide|
481|304|F|160370.14|1992-10-08|2-HIGH|Clerk#000000230|0|ruthlessly ironic packages nag furiously across the slyly regula|
482|1252|O|197194.23|1996-03-26|1-URGENT|Clerk#000000295|0|blithely regular as|
483|349|O|66194.38|1995-07-11|2-HIGH|Clerk#000000025|0|bold theodolites sl|
484|544|O|331553.32|1997-01-03|3-MEDIUM|Clerk#000000545|0|fluffily even deposits run foxes; regular packages afte|
485|1006|O|142389.70|1997-03-26|2-HIGH|Clerk#000000105|0|regular, bold asymptotes sleep boldly. carefu|
486|509|O|286150.09|1996-03-11|4-NOT SPECIFIED|Clerk#000000803|0|quickly final foxes across the expre|
487|1079|F|88805.07|1992-08-18|1-URGENT|Clerk#000000086|0|ironic, express pinto be|
512|631|P|183939.48|1995-05-20|5-LOW|Clerk#000000814|0|quickly unusual foxes was fluffily slyly even accounts. pac|
513|607|O|78769.71|1995-05-01|2-HIGH|Clerk#000000522|0|always final sentiments haggle furiously around the fluffily ruthles|
514|749|O|123202.51|1996-04-04|2-HIGH|Clerk#000000094|0|enticingly quick escapades wake slyly. final acc|
515|1420|F|177231.12|1993-08-29|4-NOT SPECIFIED|Clerk#000000700|0|slyly unusual ideas subla|
516|440|O|13277.79|1998-04-21|2-HIGH|Clerk#000000305|0|quickly final foxes accord|
517|94|O|109269.47|1997-04-07|5-LOW|Clerk#000000359|0|deposits wake always slyly regular requests. blithely |
518|1444|O|335285.37|1998-02-08|2-HIGH|Clerk#000000768|0|slyly even ideas hang quickly. carefully final instructi|
519|631|O|109395.60|1997-10-31|1-URGENT|Clerk#000000985|0|quick depths are! slyly express requests along the carefully ironic |
544|934|F|58960.45|1993-02-17|2-HIGH|Clerk#000000145|0|slyly ironic attainments sleep blith|
545|632|O|35129.54|1995-11-07|2-HIGH|Clerk#000000537|0|even, regular packa|
546|1433|O|26227.74|1996-11-01|2-HIGH|Clerk#000000041|0|final notornis detect slyly fluffily express deposits. brav|
547|983|O|137852.72|1996-06-22|3-MEDIUM|Clerk#000000976|0|bold instructions print fluffily carefully id|
548|1240|F|139094.89|1994-09-21|1-URGENT|Clerk#000000435|0|quickly regular accounts daz|
549|1100|F|211787.30|1992-07-13|1-URGENT|Clerk#000000196|0|carefully regular foxes integrate ironic, fina|
550|236|O|54818.45|1995-08-02|1-URGENT|Clerk#000000204|0|carefully even asymptotes sleep furiously sp|
551|898|O|64301.40|1995-05-30|1-URGENT|Clerk#000000179|0|unusual, final accounts use above the special excuses. final depo|
576|296|O|24722.97|1997-05-13|3-MEDIUM|Clerk#000000955|0|pending theodolites about the carefu|
577|553|F|47860.53|1994-12-19|5-LOW|Clerk#000000154|0|blithely unusual packages sl|
578|926|O|103543.00|1997-01-10|5-LOW|Clerk#000000281|0|blithely pending asymptotes wake quickly across the carefully final|
579|671|O|146610.11|1998-03-11|2-HIGH|Clerk#000000862|0|slyly even requests cajole slyly. sil|
580|593|O|144557.44|1997-07-05|2-HIGH|Clerk#000000314|0|final ideas must have to are carefully quickly furious requests|
581|688|O|175985.28|1997-02-23|4-NOT SPECIFIED|Clerk#000000239|0|carefully regular dolphins cajole ruthlessl|
582|494|O|181813.20|1997-10-21|1-URGENT|Clerk#000000378|0|quietly ironic pinto beans wake carefully. ironic accounts across the dol|

select * from orders_1 o1, orders_2 o2 where
o1.o_orderkey = o2.o_orderkey and o1.o_orderdate <> o2.o_orderdate;

select count(*) from orders_1;
select count(*) from orders_2;

drop table orders_1;
drop table orders_2;
