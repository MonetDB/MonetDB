START TRANSACTION;

CREATE TABLE subject_term (a1 VARCHAR(1000), prob DOUBLE);
INSERT INTO subject_term VALUES ('reference', 1.000000 );
INSERT INTO subject_term VALUES ('description', 1.000000 );

CREATE TABLE "docdict" (
	"docid" int NOT NULL,
	"doc" varchar(1000),
	"type" varchar(30),
	"prob" double DEFAULT 1.000000,
	CONSTRAINT "docdict_docid_pkey" PRIMARY KEY ("docid")
);
COPY 62 RECORDS INTO "docdict" FROM stdin USING DELIMITERS '\t';
0	"EP-00101575-A"	"patent-document"	1
1	"EP-00103038-A"	"patent-document"	1
2	"EP-00103043-A"	"patent-document"	1
3	"EP-00103260-A"	"patent-document"	1
4	"EP-00301156-A"	"patent-document"	1
5	"EP-00301228-A"	"patent-document"	1
6	"EP-0295148-A1"	"patent-document"	1
7	"EP-0567080-A1"	"patent-document"	1
8	"EP-0588728-A1"	"patent-document"	1
9	"EP-0639927-A2"	"patent-document"	1
10	"EP-0644692-A2"	"patent-document"	1
11	"EP-0676819-A2"	"patent-document"	1
12	"EP-0809245-A2"	"patent-document"	1
13	"EP-0833335-A1"	"patent-document"	1
14	"EP-0836311-A2"	"patent-document"	1
15	"EP-0847093-A1"	"patent-document"	1
16	"EP-0849734-B1"	"patent-document"	1
17	"EP-0903742-A2"	"patent-document"	1
18	"EP-0924704-A2"	"patent-document"	1
19	"EP-1030303-A2"	"patent-document"	1
20	"EP-1030307-A1"	"patent-document"	1
21	"EP-1030307-B1"	"patent-document"	1
22	"EP-1030352-A2"	"patent-document"	1
23	"EP-1030377-A2"	"patent-document"	1
24	"EP-1030380-A2"	"patent-document"	1
25	"EP-1030380-B1"	"patent-document"	1
26	"EP-1030381-A1"	"patent-document"	1
27	"EP-1030381-B1"	"patent-document"	1
28	"EP-1030386-A1"	"patent-document"	1
29	"EP-99955564-A"	"patent-document"	1
30	"FR-2762429-A1"	"patent-document"	1
31	"IT-VR990017-A"	"patent-document"	1
32	"JP-10105974-A"	"patent-document"	1
33	"JP-10105975-A"	"patent-document"	1
34	"JP-10150224-A"	"patent-document"	1
35	"JP-10188361-A"	"patent-document"	1
36	"JP-10247624-A"	"patent-document"	1
37	"JP-11121449-A"	"patent-document"	1
38	"JP-15793698-A"	"patent-document"	1
39	"JP-22460899-A"	"patent-document"	1
40	"JP-2272799-A"	"patent-document"	1
41	"JP-3837099-A"	"patent-document"	1
42	"JP-3929099-A"	"patent-document"	1
43	"JP-4014199-A"	"patent-document"	1
44	"JP-8064872-A"	"patent-document"	1
45	"JP-9283803-A"	"patent-document"	1
46	"JP-9903029-W"	"patent-document"	1
47	"US-25170199-A"	"patent-document"	1
48	"US-4543271-A"	"patent-document"	1
49	"US-4567383-A"	"patent-document"	1
50	"US-4810204-A"	"patent-document"	1
51	"US-5289451-A"	"patent-document"	1
52	"US-5374613-A"	"patent-document"	1
53	"US-5376628-A"	"patent-document"	1
54	"US-5671202-A"	"patent-document"	1
55	"US-5760479-A"	"patent-document"	1
56	"US-5812403-A"	"patent-document"	1
57	"US-5862117-A"	"patent-document"	1
58	"WO-1988000985-A1"	"patent-document"	1
59	"WO-1997003443-A1"	"patent-document"	1
60	"WO-1997041562-A1"	"patent-document"	1
61	"WO-1998052191-A1"	"patent-document"	1
CREATE TABLE "nedict" (
	"neid" int NOT NULL,
	"ne" varchar(1000),
	"type" varchar(30),
	"prob" double DEFAULT 1.000000,
	CONSTRAINT "nedict_neid_pkey" PRIMARY KEY ("neid")
);
COPY 175 RECORDS INTO "nedict" FROM stdin USING DELIMITERS '\t';
0	""	"country"	1
1	"AT"	"country"	1
2	"BE"	"country"	1
3	"CH"	"country"	1
4	"CY"	"country"	1
5	"DE"	"country"	1
6	"DK"	"country"	1
7	"EN"	"country"	1
8	"EP"	"country"	1
9	"ES"	"country"	1
10	"FI"	"country"	1
11	"FR"	"country"	1
12	"GB"	"country"	1
13	"GR"	"country"	1
14	"IE"	"country"	1
15	"IT"	"country"	1
16	"JP"	"country"	1
17	"LI"	"country"	1
18	"LU"	"country"	1
19	"MC"	"country"	1
20	"NL"	"country"	1
21	"PT"	"country"	1
22	"SE"	"country"	1
23	"US"	"country"	1
24	"WO"	"country"	1
25	"AOYAGI YOSHINOBU"	"person"	1
26	"BATTILANI GIANCARLO"	"person"	1
27	"HANZAWA MASAKI"	"person"	1
28	"HIRANO ATSUO"	"person"	1
29	"ISHIBASHI KOJI"	"person"	1
30	"KANDA AKINOBU"	"person"	1
31	"KUMASAKA KATSUNORI"	"person"	1
32	"MURASE KAORU"	"person"	1
33	"OKADA TOMOYUKI"	"person"	1
34	"SAITO KOICHI"	"person"	1
35	"SUGANO TAKUO"	"person"	1
36	"SUGIMOTO NORIKO"	"person"	1
37	"TAKESHITA TOSHIO"	"person"	1
38	"TESHIMA KIYOTAKA"	"person"	1
39	"TSUGA KAZUHIRO"	"person"	1
40	"XIA LI QUN"	"person"	1
41	"YASUKAWA TAKEMASA"	"person"	1
42	"YIEH ELLIE"	"person"	1
43	"YOSHIKAWA YUKIO"	"person"	1
44	"EC"	"ecla_scheme"	1
45	"ICO"	"ecla_scheme"	1
46	"C23C  16/452"	"ecla_code"	1
47	"C30B  25/02"	"ecla_code"	1
48	"C30B  25/08"	"ecla_code"	1
49	"G11B  20/10C"	"ecla_code"	1
50	"G11B  23/00D1A2A"	"ecla_code"	1
51	"G11B  23/30"	"ecla_code"	1
52	"G11B  23/40"	"ecla_code"	1
53	"G11B  27/036"	"ecla_code"	1
54	"G11B  27/10A1"	"ecla_code"	1
55	"G11B  27/32D2"	"ecla_code"	1
56	"G11B  27/34"	"ecla_code"	1
57	"G11B  27/36"	"ecla_code"	1
58	"G11B20/10C"	"ecla_code"	1
59	"G11B27/036"	"ecla_code"	1
60	"G11B27/10A1"	"ecla_code"	1
61	"G11B27/32D2"	"ecla_code"	1
62	"G11B27/34"	"ecla_code"	1
63	"G11B27/36"	"ecla_code"	1
64	"H01L  21/28E2C2D"	"ecla_code"	1
65	"H01L  21/28E2C2N"	"ecla_code"	1
66	"H01L  21/314B1"	"ecla_code"	1
67	"H01L  21/316C2B2"	"ecla_code"	1
68	"H01L  29/51N"	"ecla_code"	1
69	"H01L  33/00B2"	"ecla_code"	1
70	"H01L  33/00B4C"	"ecla_code"	1
71	"H01L  39/22D"	"ecla_code"	1
72	"H01L  41/053"	"ecla_code"	1
73	"H01L  41/107"	"ecla_code"	1
74	"H01L39/22D"	"ecla_code"	1
75	"H01L41/053"	"ecla_code"	1
76	"H01L41/107"	"ecla_code"	1
77	"H01M   2/10C2"	"ecla_code"	1
78	"H01M   2/10C2C2"	"ecla_code"	1
79	"H01M   2/10C2D2"	"ecla_code"	1
80	"H01M   2/30"	"ecla_code"	1
81	"H04N   9/804B"	"ecla_code"	1
82	"H04N9/804B"	"ecla_code"	1
83	"S06K  19:06W6"	"ecla_code"	1
84	"S11B  27:034"	"ecla_code"	1
85	"S11B  27:30C"	"ecla_code"	1
86	"S11B27:034"	"ecla_code"	1
87	"S11B27:30C"	"ecla_code"	1
88	"T01M   6:50S6"	"ecla_code"	1
89	"T04N   5:05"	"ecla_code"	1
90	"T04N   5:85"	"ecla_code"	1
91	"T04N   9:806S"	"ecla_code"	1
92	"T04N5:05"	"ecla_code"	1
93	"T04N5:85"	"ecla_code"	1
94	"T04N9:806S"	"ecla_code"	1
95	"T05K   1:02E"	"ecla_code"	1
96	"T05K   3:30C"	"ecla_code"	1
97	"T05K1:02E"	"ecla_code"	1
98	"T05K3:30C"	"ecla_code"	1
99	"Y01N   4:00"	"ecla_code"	1
100	"Y01N4:00"	"ecla_code"	1
101	"EP-00101575"	"patent"	1
102	"EP-00103038"	"patent"	1
103	"EP-00103043"	"patent"	1
104	"EP-00103260"	"patent"	1
105	"EP-00301156"	"patent"	1
106	"EP-00301228"	"patent"	1
107	"EP-0295148"	"patent"	1
108	"EP-0567080"	"patent"	1
109	"EP-0588728"	"patent"	1
110	"EP-0639927"	"patent"	1
111	"EP-0644692"	"patent"	1
112	"EP-0676819"	"patent"	1
113	"EP-0809245"	"patent"	1
114	"EP-0833335"	"patent"	1
115	"EP-0836311"	"patent"	1
116	"EP-0847093"	"patent"	1
117	"EP-0849734"	"patent"	1
118	"EP-0903742"	"patent"	1
119	"EP-0924704"	"patent"	1
120	"EP-1030303"	"patent"	1
121	"EP-1030307"	"patent"	1
122	"EP-1030352"	"patent"	1
123	"EP-1030377"	"patent"	1
124	"EP-1030380"	"patent"	1
125	"EP-1030381"	"patent"	1
126	"EP-1030386"	"patent"	1
127	"EP-99955564"	"patent"	1
128	"FR-2762429"	"patent"	1
129	"IT-VR990017"	"patent"	1
130	"JP-10105974"	"patent"	1
131	"JP-10105975"	"patent"	1
132	"JP-10150224"	"patent"	1
133	"JP-10188361"	"patent"	1
134	"JP-10247624"	"patent"	1
135	"JP-11121449"	"patent"	1
136	"JP-15793698"	"patent"	1
137	"JP-22460899"	"patent"	1
138	"JP-2272799"	"patent"	1
139	"JP-3837099"	"patent"	1
140	"JP-3929099"	"patent"	1
141	"JP-4014199"	"patent"	1
142	"JP-8064872"	"patent"	1
143	"JP-9283803"	"patent"	1
144	"JP-9903029"	"patent"	1
145	"US-25170199"	"patent"	1
146	"US-4543271"	"patent"	1
147	"US-4567383"	"patent"	1
148	"US-4810204"	"patent"	1
149	"US-5289451"	"patent"	1
150	"US-5374613"	"patent"	1
151	"US-5376628"	"patent"	1
152	"US-5671202"	"patent"	1
153	"US-5760479"	"patent"	1
154	"US-5812403"	"patent"	1
155	"US-5862117"	"patent"	1
156	"WO-1988000985"	"patent"	1
157	"WO-1997003443"	"patent"	1
158	"WO-1997041562"	"patent"	1
159	"WO-1998052191"	"patent"	1
160	"APPLIED MATERIALS INC"	"company"	1
161	"KOHA CO LTD"	"company"	1
162	"MATSUSHITA ELECTRIC IND CO LTD"	"company"	1
163	"MATSUSHITA ELECTRIC INDUSTRIAL CO LTD"	"company"	1
164	"NEC TOKIN CORP"	"company"	1
165	"NEC TOKIN CORPORATION"	"company"	1
166	"PERSONAL VIDEO ITALIA DI BATTI"	"company"	1
167	"PERSONAL VIDEO ITALIA DI BATTILANI GIANCARLO"	"company"	1
168	"RIKEN"	"company"	1
169	"SONY CORP"	"company"	1
170	"SONY CORPORATION"	"company"	1
171	"TOKIN CORP"	"company"	1
172	"TOKIN CORPORATION"	"company"	1
173	"TOYODA GOSEI CO LTD"	"company"	1
174	"TOYODA GOSEI KK"	"company"	1
CREATE TABLE "doc_string" (
	"docid" int,
	"attribute" varchar(30),
	"value" varchar(10000),
	"prob" double DEFAULT 1.000000
);
COPY 288 RECORDS INTO "doc_string" FROM stdin USING DELIMITERS '\t';
0	"date"	"20000127"	1
0	"doc-number"	"00101575"	1
0	"kind"	"A"	1
0	"ucid"	"EP-00101575-A"	1
1	"date"	"20000215"	1
1	"doc-number"	"00103038"	1
1	"kind"	"A"	1
1	"ucid"	"EP-00103038-A"	1
2	"date"	"20000215"	1
2	"doc-number"	"00103043"	1
2	"kind"	"A"	1
2	"ucid"	"EP-00103043-A"	1
3	"date"	"20000217"	1
3	"doc-number"	"00103260"	1
3	"kind"	"A"	1
3	"ucid"	"EP-00103260-A"	1
4	"date"	"20000215"	1
4	"doc-number"	"00301156"	1
4	"kind"	"A"	1
4	"ucid"	"EP-00301156-A"	1
5	"date"	"20000216"	1
5	"doc-number"	"00301228"	1
5	"kind"	"A"	1
5	"ucid"	"EP-00301228-A"	1
6	"date"	""	1
6	"doc-number"	"0295148"	1
6	"kind"	"A1"	1
6	"ucid"	"EP-0295148-A1"	1
7	"date"	""	1
7	"doc-number"	"0567080"	1
7	"kind"	"A1"	1
7	"ucid"	"EP-0567080-A1"	1
8	"date"	""	1
8	"doc-number"	"0588728"	1
8	"kind"	"A1"	1
8	"ucid"	"EP-0588728-A1"	1
9	"date"	""	1
9	"doc-number"	"0639927"	1
9	"kind"	"A2"	1
9	"ucid"	"EP-0639927-A2"	1
10	"date"	""	1
10	"doc-number"	"0644692"	1
10	"kind"	"A2"	1
10	"ucid"	"EP-0644692-A2"	1
11	"date"	""	1
11	"doc-number"	"0676819"	1
11	"kind"	"A2"	1
11	"ucid"	"EP-0676819-A2"	1
12	"date"	""	1
12	"doc-number"	"0809245"	1
12	"kind"	"A2"	1
12	"ucid"	"EP-0809245-A2"	1
13	"date"	""	1
13	"doc-number"	"0833335"	1
13	"kind"	"A1"	1
13	"ucid"	"EP-0833335-A1"	1
14	"date"	""	1
14	"doc-number"	"0836311"	1
14	"kind"	"A2"	1
14	"ucid"	"EP-0836311-A2"	1
15	"date"	""	1
15	"doc-number"	"0847093"	1
15	"kind"	"A1"	1
15	"ucid"	"EP-0847093-A1"	1
16	"date"	""	1
16	"doc-number"	"0849734"	1
16	"kind"	"B1"	1
16	"ucid"	"EP-0849734-B1"	1
17	"date"	""	1
17	"doc-number"	"0903742"	1
17	"kind"	"A2"	1
17	"ucid"	"EP-0903742-A2"	1
18	"date"	""	1
18	"doc-number"	"0924704"	1
18	"kind"	"A2"	1
18	"ucid"	"EP-0924704-A2"	1
19	"date"	"20000823"	1
19	"doc-number"	"1030303"	1
19	"kind"	"A2"	1
19	"title_DE"	"Plattenförmiger Aufzeichnungsträger mit kreisförmigem Identifikationskode"	1
19	"title"	"Disk-shaped record carrier provided with a circular identification code"	1
19	"title_EN"	"Disk-shaped record carrier provided with a circular identification code"	1
19	"title_FR"	"Support d'enregistrement en forme de disque pourvu d'un code d'identification circulaire"	1
19	"ucid"	"EP-1030303-A2"	1
20	"date"	"20000823"	1
20	"doc-number"	"1030307"	1
20	"kind"	"A1"	1
20	"title_DE"	"Informationsaufzeichnungsmedium, Vorrichtung und Verfahren zur nachträglichen Aufzeichnung auf dem Informationsaufzeichnungsmedium"	1
20	"title_EN"	"Information recording medium, apparatus and method for performing after-recording on the recording medium"	1
20	"title_FR"	"Support d'enregistrement d'informations, dispositif et procédé d'enregistrement rétractif sur le support d'enregistrement d'informations"	1
20	"title"	"Information recording medium, apparatus and method for performing after-recording on the recording medium"	1
20	"ucid"	"EP-1030307-A1"	1
21	"date"	"20010912"	1
21	"doc-number"	"1030307"	1
21	"kind"	"B1"	1
21	"title_DE"	"Informationsaufzeichnungsmedium, Vorrichtung und Verfahren zur nachträglichen Aufzeichnung auf dem Informationsaufzeichnungsmedium"	1
21	"title_EN"	"Information recording medium, apparatus and method for performing after-recording on the recording medium"	1
21	"title_FR"	"Support d'enregistrement d'informations, dispositif et procédé d'enregistrement rétractif sur le support d'enregistrement d'informations"	1
21	"title"	"Information recording medium, apparatus and method for performing after-recording on the recording medium"	1
21	"ucid"	"EP-1030307-B1"	1
22	"date"	"20000823"	1
22	"doc-number"	"1030352"	1
22	"kind"	"A2"	1
22	"title_DE"	"Verfahren und Apparat zum Herstellen von Materialenschichten mittels atomarer Gase"	1
22	"title_EN"	"Method and apparatus for forming materials layers from atomic gases"	1
22	"title_FR"	"Méthode et appareil pour former des couches de matériaux à partir de gaz atomiques"	1
22	"title"	"Method and apparatus for forming materials layers from atomic gases"	1
22	"ucid"	"EP-1030352-A2"	1
23	"date"	"20000823"	1
23	"doc-number"	"1030377"	1
23	"kind"	"A2"	1
23	"title_DE"	"Licht-emittierende Diode"	1
23	"title_EN"	"Light-emitting diode"	1
23	"title_FR"	"Diode émettrice de lumière"	1
23	"title"	"Light-emitting diode"	1
23	"ucid"	"EP-1030377-A2"	1
24	"date"	"20000823"	1
24	"doc-number"	"1030380"	1
24	"kind"	"A2"	1
24	"title_DE"	"Digitales Einzelflussquantenbauelement"	1
24	"title_EN"	"Single-flux-quantum digital device"	1
24	"title_FR"	"Dispositif digital à flux quantique unique"	1
24	"title"	"Single-flux-quantum digital device"	1
24	"ucid"	"EP-1030380-A2"	1
25	"date"	"20051109"	1
25	"doc-number"	"1030380"	1
25	"kind"	"B1"	1
25	"title_DE"	"Digitales Einzelflussquantenbauelement"	1
25	"title_EN"	"Single-flux-quantum digital device"	1
25	"title_FR"	"Dispositif digital à flux quantique unique"	1
25	"title"	"Single-flux-quantum digital device"	1
25	"ucid"	"EP-1030380-B1"	1
26	"date"	"20000823"	1
26	"doc-number"	"1030381"	1
26	"kind"	"A1"	1
26	"title_DE"	"BEFESTIGUNGSSTRUKTUR FÜR PIEZOELEKTRISCHEN TRANSFORMATOR UND VERFAHREN ZUR BEFESTIGUNG IN EINEM GEHAUSE EINER PIEZOELEKTRISCHER TRANSFORMATOR"	1
26	"title_EN"	"MOUNTING STRUCTURE OF PIEZOELECTRIC TRANSFORMER AND METHOD OF MOUNTING PIEZOELECTRIC TRANSFORMER"	1
26	"title_FR"	"STRUCTURE ET PROCEDE DE MONTAGE D'UN TRANSFORMATEUR PIEZO-ELECTRIQUE"	1
26	"title"	"MOUNTING STRUCTURE OF PIEZOELECTRIC TRANSFORMER AND METHOD OF MOUNTING PIEZOELECTRIC TRANSFORMER"	1
26	"ucid"	"EP-1030381-A1"	1
27	"date"	"20021106"	1
27	"doc-number"	"1030381"	1
27	"kind"	"B1"	1
27	"title_DE"	"BEFESTIGUNGSSTRUKTUR FÜR PIEZOELEKTRISCHEN TRANSFORMATOR UND  VERFAHREN ZUR BEFESTIGUNG EINES PIEZOELEKTRISCHEN TRANSFORMATORS"	1
27	"title_EN"	"MOUNTING STRUCTURE OF PIEZOELECTRIC TRANSFORMER AND METHOD OF MOUNTING PIEZOELECTRIC TRANSFORMER"	1
27	"title_FR"	"STRUCTURE ET PROCEDE DE MONTAGE D'UN TRANSFORMATEUR PIEZO-ELECTRIQUE"	1
27	"title"	"MOUNTING STRUCTURE OF PIEZOELECTRIC TRANSFORMER AND METHOD OF MOUNTING PIEZOELECTRIC TRANSFORMER"	1
27	"ucid"	"EP-1030381-B1"	1
28	"date"	"20000823"	1
28	"doc-number"	"1030386"	1
28	"kind"	"A1"	1
28	"title"	"Battery pack, battery loading device, power supplying device and electronic equipment"	1
28	"title_DE"	"Batteriesatz, Batterielader, Stromversorgungseinrichtung und elektronisches Gerät"	1
28	"title_EN"	"Battery pack, battery loading device, power supplying device and electronic equipment"	1
28	"title_FR"	"Bloc de batteries, chargeur, source d'énergie électrique et appareil électronique"	1
28	"ucid"	"EP-1030386-A1"	1
29	"date"	"19990607"	1
29	"doc-number"	"99955564"	1
29	"kind"	"A"	1
29	"ucid"	"EP-99955564-A"	1
30	"date"	""	1
30	"doc-number"	"2762429"	1
30	"kind"	"A1"	1
30	"ucid"	"FR-2762429-A1"	1
31	"date"	"19990218"	1
31	"doc-number"	"VR990017"	1
31	"kind"	"A"	1
31	"ucid"	"IT-VR990017-A"	1
32	"date"	""	1
32	"doc-number"	"10105974"	1
32	"kind"	"A"	1
32	"ucid"	"JP-10105974-A"	1
33	"date"	""	1
33	"doc-number"	"10105975"	1
33	"kind"	"A"	1
33	"ucid"	"JP-10105975-A"	1
34	"date"	""	1
34	"doc-number"	"10150224"	1
34	"kind"	"A"	1
34	"ucid"	"JP-10150224-A"	1
35	"date"	""	1
35	"doc-number"	"10188361"	1
35	"kind"	"A"	1
35	"ucid"	"JP-10188361-A"	1
36	"date"	""	1
36	"doc-number"	"10247624"	1
36	"kind"	"A"	1
36	"ucid"	"JP-10247624-A"	1
37	"date"	""	1
37	"doc-number"	"11121449"	1
37	"kind"	"A"	1
37	"ucid"	"JP-11121449-A"	1
38	"date"	"19980605"	1
38	"doc-number"	"15793698"	1
38	"kind"	"A"	1
38	"ucid"	"JP-15793698-A"	1
39	"date"	"19990806"	1
39	"doc-number"	"22460899"	1
39	"kind"	"A"	1
39	"ucid"	"JP-22460899-A"	1
40	"date"	"19990129"	1
40	"doc-number"	"2272799"	1
40	"kind"	"A"	1
40	"ucid"	"JP-2272799-A"	1
41	"date"	"19990217"	1
41	"doc-number"	"3837099"	1
41	"kind"	"A"	1
41	"ucid"	"JP-3837099-A"	1
42	"date"	"19990217"	1
42	"doc-number"	"3929099"	1
42	"kind"	"A"	1
42	"ucid"	"JP-3929099-A"	1
43	"date"	"19990218"	1
43	"doc-number"	"4014199"	1
43	"kind"	"A"	1
43	"ucid"	"JP-4014199-A"	1
44	"date"	""	1
44	"doc-number"	"8064872"	1
44	"kind"	"A"	1
44	"ucid"	"JP-8064872-A"	1
45	"date"	""	1
45	"doc-number"	"9283803"	1
45	"kind"	"A"	1
45	"ucid"	"JP-9283803-A"	1
46	"date"	"19990607"	1
46	"doc-number"	"9903029"	1
46	"kind"	"W"	1
46	"ucid"	"JP-9903029-W"	1
47	"date"	"19990217"	1
47	"doc-number"	"25170199"	1
47	"kind"	"A"	1
47	"ucid"	"US-25170199-A"	1
48	"date"	""	1
48	"doc-number"	"4543271"	1
48	"kind"	"A"	1
48	"ucid"	"US-4543271-A"	1
49	"date"	""	1
49	"doc-number"	"4567383"	1
49	"kind"	"A"	1
49	"ucid"	"US-4567383-A"	1
50	"date"	""	1
50	"doc-number"	"4810204"	1
50	"kind"	"A"	1
50	"ucid"	"US-4810204-A"	1
51	"date"	""	1
51	"doc-number"	"5289451"	1
51	"kind"	"A"	1
51	"ucid"	"US-5289451-A"	1
52	"date"	""	1
52	"doc-number"	"5374613"	1
52	"kind"	"A"	1
52	"ucid"	"US-5374613-A"	1
53	"date"	""	1
53	"doc-number"	"5376628"	1
53	"kind"	"A"	1
53	"ucid"	"US-5376628-A"	1
54	"date"	""	1
54	"doc-number"	"5671202"	1
54	"kind"	"A"	1
54	"ucid"	"US-5671202-A"	1
55	"date"	""	1
55	"doc-number"	"5760479"	1
55	"kind"	"A"	1
55	"ucid"	"US-5760479-A"	1
56	"date"	""	1
56	"doc-number"	"5812403"	1
56	"kind"	"A"	1
56	"ucid"	"US-5812403-A"	1
57	"date"	""	1
57	"doc-number"	"5862117"	1
57	"kind"	"A"	1
57	"ucid"	"US-5862117-A"	1
58	"date"	""	1
58	"doc-number"	"1988000985"	1
58	"kind"	"A1"	1
58	"ucid"	"WO-1988000985-A1"	1
59	"date"	""	1
59	"doc-number"	"1997003443"	1
59	"kind"	"A1"	1
59	"ucid"	"WO-1997003443-A1"	1
60	"date"	""	1
60	"doc-number"	"1997041562"	1
60	"kind"	"A1"	1
60	"ucid"	"WO-1997041562-A1"	1
61	"date"	""	1
61	"doc-number"	"1998052191"	1
61	"kind"	"A1"	1
61	"ucid"	"WO-1998052191-A1"	1
CREATE TABLE "ne_string" (
	"neid" int,
	"attribute" varchar(30),
	"value" varchar(10000),
	"prob" double DEFAULT 1.000000
);
COPY 298 RECORDS INTO "ne_string" FROM stdin USING DELIMITERS '\t';
0	"code"	""	1
1	"code"	"AT"	1
2	"code"	"BE"	1
3	"code"	"CH"	1
4	"code"	"CY"	1
5	"code"	"DE"	1
6	"code"	"DK"	1
7	"code"	"EN"	1
8	"code"	"EP"	1
9	"code"	"ES"	1
10	"code"	"FI"	1
11	"code"	"FR"	1
12	"code"	"GB"	1
13	"code"	"GR"	1
14	"code"	"IE"	1
15	"code"	"IT"	1
16	"code"	"JP"	1
17	"code"	"LI"	1
18	"code"	"LU"	1
19	"code"	"MC"	1
20	"code"	"NL"	1
21	"code"	"PT"	1
22	"code"	"SE"	1
23	"code"	"US"	1
24	"code"	"WO"	1
25	"address"	"JP"	1
25	"name"	"AOYAGI YOSHINOBU"	1
25	"normalized_name"	"AOYAGI YOSHINOBU"	1
26	"address"	"IT"	1
26	"name"	"BATTILANI GIANCARLO"	1
26	"normalized_name"	"BATTILANI GIANCARLO"	1
27	"address"	"JP"	1
27	"name"	"HANZAWA MASAKI"	1
27	"normalized_name"	"HANZAWA MASAKI"	1
28	"address"	"US"	1
28	"name"	"HIRANO ATSUO"	1
28	"normalized_name"	"HIRANO ATSUO"	1
29	"address"	"JP"	1
29	"name"	"ISHIBASHI KOJI"	1
29	"normalized_name"	"ISHIBASHI KOJI"	1
30	"address"	"JP"	1
30	"name"	"KANDA AKINOBU"	1
30	"normalized_name"	"KANDA AKINOBU"	1
31	"address"	"JP"	1
31	"name"	"KUMASAKA KATSUNORI"	1
31	"normalized_name"	"KUMASAKA KATSUNORI"	1
32	"address"	"JP"	1
32	"name"	"MURASE KAORU"	1
32	"normalized_name"	"MURASE KAORU"	1
33	"address"	"JP"	1
33	"name"	"OKADA TOMOYUKI"	1
33	"normalized_name"	"OKADA TOMOYUKI"	1
34	"address"	"JP"	1
34	"name"	"SAITO KOICHI"	1
34	"normalized_name"	"SAITO KOICHI"	1
35	"address"	"JP"	1
35	"name"	"SUGANO TAKUO"	1
35	"normalized_name"	"SUGANO TAKUO"	1
36	"address"	"JP"	1
36	"name"	"SUGIMOTO NORIKO"	1
36	"normalized_name"	"SUGIMOTO NORIKO"	1
37	"address"	"JP"	1
37	"name"	"TAKESHITA TOSHIO"	1
37	"normalized_name"	"TAKESHITA TOSHIO"	1
38	"address"	"JP"	1
38	"name"	"TESHIMA KIYOTAKA"	1
38	"normalized_name"	"TESHIMA KIYOTAKA"	1
39	"address"	"JP"	1
39	"name"	"TSUGA KAZUHIRO"	1
39	"normalized_name"	"TSUGA KAZUHIRO"	1
40	"address"	"US"	1
40	"name"	"XIA LI-QUN"	1
40	"normalized_name"	"XIA LI QUN"	1
41	"address"	""	1
41	"name"	"YASUKAWA, TAKEMASA"	1
41	"normalized_name"	"YASUKAWA TAKEMASA"	1
42	"address"	"US"	1
42	"name"	"YIEH ELLIE"	1
42	"normalized_name"	"YIEH ELLIE"	1
43	"address"	"JP"	1
43	"name"	"YOSHIKAWA YUKIO"	1
43	"normalized_name"	"YOSHIKAWA YUKIO"	1
44	"name"	"EC"	1
45	"name"	"ICO"	1
46	"basecode"	"C23C  16/452"	1
46	"name"	"C23C  16/452"	1
47	"basecode"	"C30B  25/02"	1
47	"name"	"C30B  25/02"	1
48	"basecode"	"C30B  25/08"	1
48	"name"	"C30B  25/08"	1
49	"basecode"	"G11B  20/10C"	1
49	"name"	"G11B  20/10C"	1
50	"basecode"	"G11B  23/00D1A2A"	1
50	"name"	"G11B  23/00D1A2A"	1
51	"basecode"	"G11B  23/30"	1
51	"name"	"G11B  23/30"	1
52	"basecode"	"G11B  23/40"	1
52	"name"	"G11B  23/40"	1
53	"basecode"	"G11B  27/036"	1
53	"name"	"G11B  27/036"	1
54	"basecode"	"G11B  27/10A1"	1
54	"name"	"G11B  27/10A1"	1
55	"basecode"	"G11B  27/32D2"	1
55	"name"	"G11B  27/32D2"	1
56	"basecode"	"G11B  27/34"	1
56	"name"	"G11B  27/34"	1
57	"basecode"	"G11B  27/36"	1
57	"name"	"G11B  27/36"	1
58	"basecode"	"G11B20/10C"	1
58	"name"	"G11B20/10C"	1
59	"basecode"	"G11B27/036"	1
59	"name"	"G11B27/036"	1
60	"basecode"	"G11B27/10A1"	1
60	"name"	"G11B27/10A1"	1
61	"basecode"	"G11B27/32D2"	1
61	"name"	"G11B27/32D2"	1
62	"basecode"	"G11B27/34"	1
62	"name"	"G11B27/34"	1
63	"basecode"	"G11B27/36"	1
63	"name"	"G11B27/36"	1
64	"basecode"	"H01L  21/28E2C2D"	1
64	"name"	"H01L  21/28E2C2D"	1
65	"basecode"	"H01L  21/28E2C2N"	1
65	"name"	"H01L  21/28E2C2N"	1
66	"basecode"	"H01L  21/314B1"	1
66	"name"	"H01L  21/314B1"	1
67	"basecode"	"H01L  21/316C2B2"	1
67	"name"	"H01L  21/316C2B2"	1
68	"basecode"	"H01L  29/51N"	1
68	"name"	"H01L  29/51N"	1
69	"basecode"	"H01L  33/00B2"	1
69	"name"	"H01L  33/00B2"	1
70	"basecode"	"H01L  33/00B4C"	1
70	"name"	"H01L  33/00B4C"	1
71	"basecode"	"H01L  39/22D"	1
71	"name"	"H01L  39/22D"	1
72	"basecode"	"H01L  41/053"	1
72	"name"	"H01L  41/053"	1
73	"basecode"	"H01L  41/107"	1
73	"name"	"H01L  41/107"	1
74	"basecode"	"H01L39/22D"	1
74	"name"	"H01L39/22D"	1
75	"basecode"	"H01L41/053"	1
75	"name"	"H01L41/053"	1
76	"basecode"	"H01L41/107"	1
76	"name"	"H01L41/107"	1
77	"basecode"	"H01M   2/10C2"	1
77	"name"	"H01M   2/10C2"	1
78	"basecode"	"H01M   2/10C2C2"	1
78	"name"	"H01M   2/10C2C2"	1
79	"basecode"	"H01M   2/10C2D2"	1
79	"name"	"H01M   2/10C2D2"	1
80	"basecode"	"H01M   2/30"	1
80	"name"	"H01M   2/30"	1
81	"basecode"	"H04N   9/804B"	1
81	"name"	"H04N   9/804B"	1
82	"basecode"	"H04N9/804B"	1
82	"name"	"H04N9/804B"	1
83	"basecode"	"S06K  19:06W6"	1
83	"name"	"S06K  19:06W6"	1
84	"basecode"	"S11B  27:034"	1
84	"name"	"S11B  27:034"	1
85	"basecode"	"S11B  27:30C"	1
85	"name"	"S11B  27:30C"	1
86	"basecode"	"S11B27:034"	1
86	"name"	"S11B27:034"	1
87	"basecode"	"S11B27:30C"	1
87	"name"	"S11B27:30C"	1
88	"basecode"	"T01M   6:50S6"	1
88	"name"	"T01M   6:50S6"	1
89	"basecode"	"T04N   5:05"	1
89	"name"	"T04N   5:05"	1
90	"basecode"	"T04N   5:85"	1
90	"name"	"T04N   5:85"	1
91	"basecode"	"T04N   9:806S"	1
91	"name"	"T04N   9:806S"	1
92	"basecode"	"T04N5:05"	1
92	"name"	"T04N5:05"	1
93	"basecode"	"T04N5:85"	1
93	"name"	"T04N5:85"	1
94	"basecode"	"T04N9:806S"	1
94	"name"	"T04N9:806S"	1
95	"basecode"	"T05K   1:02E"	1
95	"name"	"T05K   1:02E"	1
96	"basecode"	"T05K   3:30C"	1
96	"name"	"T05K   3:30C"	1
97	"basecode"	"T05K1:02E"	1
97	"name"	"T05K1:02E"	1
98	"basecode"	"T05K3:30C"	1
98	"name"	"T05K3:30C"	1
99	"basecode"	"Y01N   4:00"	1
99	"name"	"Y01N   4:00"	1
100	"basecode"	"Y01N4:00"	1
100	"name"	"Y01N4:00"	1
101	"name"	"EP-00101575"	1
102	"name"	"EP-00103038"	1
103	"name"	"EP-00103043"	1
104	"name"	"EP-00103260"	1
105	"name"	"EP-00301156"	1
106	"name"	"EP-00301228"	1
107	"name"	"EP-0295148"	1
108	"name"	"EP-0567080"	1
109	"name"	"EP-0588728"	1
110	"name"	"EP-0639927"	1
111	"name"	"EP-0644692"	1
112	"name"	"EP-0676819"	1
113	"name"	"EP-0809245"	1
114	"name"	"EP-0833335"	1
115	"name"	"EP-0836311"	1
116	"name"	"EP-0847093"	1
117	"name"	"EP-0849734"	1
118	"name"	"EP-0903742"	1
119	"name"	"EP-0924704"	1
120	"name"	"EP-1030303"	1
121	"name"	"EP-1030307"	1
122	"name"	"EP-1030352"	1
123	"name"	"EP-1030377"	1
124	"name"	"EP-1030380"	1
125	"name"	"EP-1030381"	1
126	"name"	"EP-1030386"	1
127	"name"	"EP-99955564"	1
128	"name"	"FR-2762429"	1
129	"name"	"IT-VR990017"	1
130	"name"	"JP-10105974"	1
131	"name"	"JP-10105975"	1
132	"name"	"JP-10150224"	1
133	"name"	"JP-10188361"	1
134	"name"	"JP-10247624"	1
135	"name"	"JP-11121449"	1
136	"name"	"JP-15793698"	1
137	"name"	"JP-22460899"	1
138	"name"	"JP-2272799"	1
139	"name"	"JP-3837099"	1
140	"name"	"JP-3929099"	1
141	"name"	"JP-4014199"	1
142	"name"	"JP-8064872"	1
143	"name"	"JP-9283803"	1
144	"name"	"JP-9903029"	1
145	"name"	"US-25170199"	1
146	"name"	"US-4543271"	1
147	"name"	"US-4567383"	1
148	"name"	"US-4810204"	1
149	"name"	"US-5289451"	1
150	"name"	"US-5374613"	1
151	"name"	"US-5376628"	1
152	"name"	"US-5671202"	1
153	"name"	"US-5760479"	1
154	"name"	"US-5812403"	1
155	"name"	"US-5862117"	1
156	"name"	"WO-1988000985"	1
157	"name"	"WO-1997003443"	1
158	"name"	"WO-1997041562"	1
159	"name"	"WO-1998052191"	1
160	"address"	"US"	1
160	"name"	"APPLIED MATERIALS INC"	1
160	"normalized_name"	"APPLIED MATERIALS INC"	1
161	"address"	"JP"	1
161	"name"	"KOHA CO LTD"	1
161	"normalized_name"	"KOHA CO LTD"	1
162	"address"	"JP"	1
162	"name"	"MATSUSHITA ELECTRIC IND CO LTD"	1
162	"normalized_name"	"MATSUSHITA ELECTRIC IND CO LTD"	1
163	"address"	""	1
163	"name"	"MATSUSHITA ELECTRIC INDUSTRIAL CO., LTD."	1
163	"normalized_name"	"MATSUSHITA ELECTRIC INDUSTRIAL CO LTD"	1
164	"address"	"JP"	1
164	"name"	"NEC TOKIN CORP"	1
164	"normalized_name"	"NEC TOKIN CORP"	1
165	"address"	""	1
165	"name"	"NEC TOKIN CORPORATION"	1
165	"normalized_name"	"NEC TOKIN CORPORATION"	1
166	"address"	"IT"	1
166	"name"	"PERSONAL VIDEO ITALIA DI BATTI"	1
166	"normalized_name"	"PERSONAL VIDEO ITALIA DI BATTI"	1
167	"address"	""	1
167	"name"	"PERSONAL VIDEO ITALIA DI BATTILANI GIANCARLO"	1
167	"normalized_name"	"PERSONAL VIDEO ITALIA DI BATTILANI GIANCARLO"	1
168	"address"	"JP"	1
168	"name"	"RIKEN"	1
168	"normalized_name"	"RIKEN"	1
169	"address"	"JP"	1
169	"name"	"SONY CORP"	1
169	"normalized_name"	"SONY CORP"	1
170	"address"	""	1
170	"name"	"SONY CORPORATION"	1
170	"normalized_name"	"SONY CORPORATION"	1
171	"address"	"JP"	1
171	"name"	"TOKIN CORP"	1
171	"normalized_name"	"TOKIN CORP"	1
172	"address"	""	1
172	"name"	"TOKIN CORPORATION"	1
172	"normalized_name"	"TOKIN CORPORATION"	1
173	"address"	""	1
173	"name"	"TOYODA GOSEI CO., LTD."	1
173	"normalized_name"	"TOYODA GOSEI CO LTD"	1
174	"address"	"JP"	1
174	"name"	"TOYODA GOSEI KK"	1
174	"normalized_name"	"TOYODA GOSEI KK"	1
CREATE TABLE "doc_doc" (
	"docid1" int,
	"predicate" varchar(30),
	"docid2" int,
	"prob" double DEFAULT 1.000000
);
COPY 69 RECORDS INTO "doc_doc" FROM stdin USING DELIMITERS '\t';
19	"application-reference"	2	1
19	"cited_by"	12	1
19	"cited_by"	13	1
19	"cited_by"	16	1
19	"cited_by"	30	1
19	"cited_by"	32	1
19	"cited_by"	33	1
19	"cited_by"	35	1
19	"cited_by"	51	1
19	"cited_by"	54	1
19	"cited_by"	57	1
19	"cited_by"	60	1
19	"cited_by"	61	1
19	"cited_by"	7	1
19	"priority-claim"	31	1
19	"publication-reference"	19	1
20	"application-reference"	1	1
20	"cited_by"	10	1
20	"cited_by"	17	1
20	"cited_by"	18	1
20	"cited_by"	59	1
20	"cited_by"	6	1
20	"cited_by"	9	1
20	"priority-claim"	41	1
20	"publication-reference"	20	1
21	"application-reference"	1	1
21	"priority-claim"	41	1
21	"publication-reference"	21	1
22	"application-reference"	4	1
22	"cited_by"	36	1
22	"cited_by"	37	1
22	"cited_by"	48	1
22	"cited_by"	52	1
22	"cited_by"	53	1
22	"cited_by"	56	1
22	"cited_by"	58	1
22	"priority-claim"	47	1
22	"publication-reference"	22	1
23	"application-reference"	0	1
23	"cited_by"	34	1
23	"cited_by"	44	1
23	"cited_by"	45	1
23	"cited_by"	55	1
23	"priority-claim"	39	1
23	"priority-claim"	40	1
23	"publication-reference"	23	1
24	"application-reference"	3	1
24	"cited_by"	49	1
24	"priority-claim"	43	1
24	"publication-reference"	24	1
25	"application-reference"	3	1
25	"priority-claim"	43	1
25	"publication-reference"	25	1
26	"application-reference"	29	1
26	"cited_by"	15	1
26	"priority-claim"	38	1
26	"priority-claim"	46	1
26	"publication-reference"	26	1
27	"application-reference"	29	1
27	"priority-claim"	38	1
27	"priority-claim"	46	1
27	"publication-reference"	27	1
28	"application-reference"	5	1
28	"cited_by"	11	1
28	"cited_by"	14	1
28	"cited_by"	50	1
28	"cited_by"	8	1
28	"priority-claim"	42	1
28	"publication-reference"	28	1
CREATE TABLE "ne_doc" (
	"neid" int,
	"predicate" varchar(30),
	"docid" int,
	"prob" double DEFAULT 1.000000
);
COPY 302 RECORDS INTO "ne_doc" FROM stdin USING DELIMITERS '\t';
1	"ep-contracting-states"	19	1
2	"ep-contracting-states"	19	1
3	"ep-contracting-states"	19	1
4	"ep-contracting-states"	19	1
5	"ep-contracting-states"	19	1
5	"ep-contracting-states"	20	1
5	"ep-contracting-states"	21	1
5	"ep-contracting-states"	22	1
5	"ep-contracting-states"	23	1
5	"ep-contracting-states"	26	1
5	"ep-contracting-states"	27	1
5	"ep-contracting-states"	28	1
6	"ep-contracting-states"	19	1
8	"country"	0	1
8	"country"	1	1
8	"country"	10	1
8	"country"	11	1
8	"country"	12	1
8	"country"	13	1
8	"country"	14	1
8	"country"	15	1
8	"country"	16	1
8	"country"	17	1
8	"country"	18	1
8	"country"	19	1
8	"country"	2	1
8	"country"	20	1
8	"country"	21	1
8	"country"	22	1
8	"country"	23	1
8	"country"	24	1
8	"country"	25	1
8	"country"	26	1
8	"country"	27	1
8	"country"	28	1
8	"country"	29	1
8	"country"	3	1
8	"country"	4	1
8	"country"	5	1
8	"country"	6	1
8	"country"	7	1
8	"country"	8	1
8	"country"	9	1
9	"ep-contracting-states"	19	1
10	"ep-contracting-states"	19	1
11	"country"	30	1
11	"ep-contracting-states"	19	1
11	"ep-contracting-states"	20	1
11	"ep-contracting-states"	21	1
11	"ep-contracting-states"	23	1
11	"ep-contracting-states"	24	1
11	"ep-contracting-states"	25	1
11	"ep-contracting-states"	26	1
11	"ep-contracting-states"	27	1
11	"ep-contracting-states"	28	1
12	"ep-contracting-states"	19	1
12	"ep-contracting-states"	20	1
12	"ep-contracting-states"	21	1
12	"ep-contracting-states"	22	1
12	"ep-contracting-states"	23	1
12	"ep-contracting-states"	26	1
12	"ep-contracting-states"	27	1
12	"ep-contracting-states"	28	1
13	"ep-contracting-states"	19	1
14	"ep-contracting-states"	19	1
15	"country"	31	1
15	"ep-contracting-states"	19	1
15	"ep-contracting-states"	26	1
15	"ep-contracting-states"	27	1
16	"country"	32	1
16	"country"	33	1
16	"country"	34	1
16	"country"	35	1
16	"country"	36	1
16	"country"	37	1
16	"country"	38	1
16	"country"	39	1
16	"country"	40	1
16	"country"	41	1
16	"country"	42	1
16	"country"	43	1
16	"country"	44	1
16	"country"	45	1
16	"country"	46	1
17	"ep-contracting-states"	19	1
18	"ep-contracting-states"	19	1
19	"ep-contracting-states"	19	1
20	"ep-contracting-states"	19	1
20	"ep-contracting-states"	28	1
21	"ep-contracting-states"	19	1
22	"ep-contracting-states"	19	1
23	"country"	47	1
23	"country"	48	1
23	"country"	49	1
23	"country"	50	1
23	"country"	51	1
23	"country"	52	1
23	"country"	53	1
23	"country"	54	1
23	"country"	55	1
23	"country"	56	1
23	"country"	57	1
24	"country"	58	1
24	"country"	59	1
24	"country"	60	1
24	"country"	61	1
25	"inventor_of"	24	1
25	"inventor_of"	24	1
25	"inventor_of"	25	1
25	"inventor_of"	25	1
26	"inventor_of"	19	1
26	"inventor_of"	19	1
27	"inventor_of"	28	1
27	"inventor_of"	28	1
28	"inventor_of"	23	1
28	"inventor_of"	23	1
29	"inventor_of"	24	1
29	"inventor_of"	24	1
29	"inventor_of"	25	1
29	"inventor_of"	25	1
30	"inventor_of"	24	1
30	"inventor_of"	24	1
30	"inventor_of"	25	1
30	"inventor_of"	25	1
31	"inventor_of"	26	1
31	"inventor_of"	26	1
31	"inventor_of"	27	1
31	"inventor_of"	27	1
32	"inventor_of"	20	1
32	"inventor_of"	20	1
32	"inventor_of"	21	1
32	"inventor_of"	21	1
33	"inventor_of"	20	1
33	"inventor_of"	20	1
33	"inventor_of"	21	1
33	"inventor_of"	21	1
34	"inventor_of"	26	1
34	"inventor_of"	26	1
34	"inventor_of"	27	1
34	"inventor_of"	27	1
35	"inventor_of"	24	1
35	"inventor_of"	24	1
35	"inventor_of"	25	1
35	"inventor_of"	25	1
36	"inventor_of"	20	1
36	"inventor_of"	20	1
36	"inventor_of"	21	1
36	"inventor_of"	21	1
37	"inventor_of"	28	1
37	"inventor_of"	28	1
38	"inventor_of"	23	1
38	"inventor_of"	23	1
39	"inventor_of"	20	1
39	"inventor_of"	20	1
39	"inventor_of"	21	1
39	"inventor_of"	21	1
40	"inventor_of"	22	1
40	"inventor_of"	22	1
41	"inventor_of"	23	1
42	"inventor_of"	22	1
42	"inventor_of"	22	1
43	"inventor_of"	23	1
43	"inventor_of"	23	1
46	"ecla-classification"	22	1
47	"ecla-classification"	22	1
48	"ecla-classification"	22	1
49	"ecla-classification"	20	1
50	"ecla-classification"	19	1
51	"ecla-classification"	19	1
52	"ecla-classification"	19	1
53	"ecla-classification"	20	1
54	"ecla-classification"	20	1
55	"ecla-classification"	20	1
56	"ecla-classification"	20	1
57	"ecla-classification"	20	1
58	"ecla-classification"	21	1
59	"ecla-classification"	21	1
60	"ecla-classification"	21	1
61	"ecla-classification"	21	1
62	"ecla-classification"	21	1
63	"ecla-classification"	21	1
64	"ecla-classification"	22	1
65	"ecla-classification"	22	1
66	"ecla-classification"	22	1
67	"ecla-classification"	22	1
68	"ecla-classification"	22	1
69	"ecla-classification"	23	1
70	"ecla-classification"	23	1
71	"ecla-classification"	24	1
72	"ecla-classification"	26	1
73	"ecla-classification"	26	1
74	"ecla-classification"	25	1
75	"ecla-classification"	27	1
76	"ecla-classification"	27	1
77	"ecla-classification"	28	1
78	"ecla-classification"	28	1
79	"ecla-classification"	28	1
80	"ecla-classification"	28	1
81	"ecla-classification"	20	1
82	"ecla-classification"	21	1
83	"ecla-classification"	19	1
84	"ecla-classification"	20	1
85	"ecla-classification"	20	1
86	"ecla-classification"	21	1
87	"ecla-classification"	21	1
88	"ecla-classification"	28	1
89	"ecla-classification"	20	1
90	"ecla-classification"	20	1
91	"ecla-classification"	20	1
92	"ecla-classification"	21	1
93	"ecla-classification"	21	1
94	"ecla-classification"	21	1
95	"ecla-classification"	26	1
96	"ecla-classification"	26	1
97	"ecla-classification"	27	1
98	"ecla-classification"	27	1
99	"ecla-classification"	24	1
100	"ecla-classification"	25	1
101	"contains_document"	0	1
102	"contains_document"	1	1
103	"contains_document"	2	1
104	"contains_document"	3	1
105	"contains_document"	4	1
106	"contains_document"	5	1
107	"contains_document"	6	1
108	"contains_document"	7	1
109	"contains_document"	8	1
110	"contains_document"	9	1
111	"contains_document"	10	1
112	"contains_document"	11	1
113	"contains_document"	12	1
114	"contains_document"	13	1
115	"contains_document"	14	1
116	"contains_document"	15	1
117	"contains_document"	16	1
118	"contains_document"	17	1
119	"contains_document"	18	1
120	"contains_document"	19	1
121	"contains_document"	20	1
121	"contains_document"	21	1
122	"contains_document"	22	1
123	"contains_document"	23	1
124	"contains_document"	24	1
124	"contains_document"	25	1
125	"contains_document"	26	1
125	"contains_document"	27	1
126	"contains_document"	28	1
127	"contains_document"	29	1
128	"contains_document"	30	1
129	"contains_document"	31	1
130	"contains_document"	32	1
131	"contains_document"	33	1
132	"contains_document"	34	1
133	"contains_document"	35	1
134	"contains_document"	36	1
135	"contains_document"	37	1
136	"contains_document"	38	1
137	"contains_document"	39	1
138	"contains_document"	40	1
139	"contains_document"	41	1
140	"contains_document"	42	1
141	"contains_document"	43	1
142	"contains_document"	44	1
143	"contains_document"	45	1
144	"contains_document"	46	1
145	"contains_document"	47	1
146	"contains_document"	48	1
147	"contains_document"	49	1
148	"contains_document"	50	1
149	"contains_document"	51	1
150	"contains_document"	52	1
151	"contains_document"	53	1
152	"contains_document"	54	1
153	"contains_document"	55	1
154	"contains_document"	56	1
155	"contains_document"	57	1
156	"contains_document"	58	1
157	"contains_document"	59	1
158	"contains_document"	60	1
159	"contains_document"	61	1
160	"assignee_of"	22	1
160	"assignee_of"	22	1
161	"assignee_of"	23	1
161	"assignee_of"	23	1
162	"assignee_of"	20	1
162	"assignee_of"	21	1
163	"assignee_of"	20	1
163	"assignee_of"	21	1
164	"assignee_of"	27	1
165	"assignee_of"	27	1
166	"assignee_of"	19	1
167	"assignee_of"	19	1
168	"assignee_of"	24	1
168	"assignee_of"	24	1
168	"assignee_of"	25	1
168	"assignee_of"	25	1
169	"assignee_of"	28	1
170	"assignee_of"	28	1
171	"assignee_of"	26	1
172	"assignee_of"	26	1
173	"assignee_of"	23	1
174	"assignee_of"	23	1
CREATE TABLE "ne_ne" (
	"neid1" int,
	"predicate" varchar(30),
	"neid2" int,
	"prob" double DEFAULT 1.000000
);
COPY 74 RECORDS INTO "ne_ne" FROM stdin USING DELIMITERS '\t';
25	"country"	16	1
26	"country"	15	1
27	"country"	16	1
28	"country"	23	1
29	"country"	16	1
30	"country"	16	1
31	"country"	16	1
32	"country"	16	1
33	"country"	16	1
34	"country"	16	1
35	"country"	16	1
36	"country"	16	1
37	"country"	16	1
38	"country"	16	1
39	"country"	16	1
40	"country"	23	1
41	"country"	0	1
42	"country"	23	1
43	"country"	16	1
46	"part_of"	44	1
47	"part_of"	44	1
48	"part_of"	44	1
49	"part_of"	44	1
50	"part_of"	44	1
51	"part_of"	44	1
52	"part_of"	44	1
53	"part_of"	44	1
54	"part_of"	44	1
55	"part_of"	44	1
56	"part_of"	44	1
57	"part_of"	44	1
58	"part_of"	44	1
59	"part_of"	44	1
60	"part_of"	44	1
61	"part_of"	44	1
62	"part_of"	44	1
63	"part_of"	44	1
64	"part_of"	44	1
65	"part_of"	44	1
66	"part_of"	44	1
67	"part_of"	44	1
68	"part_of"	44	1
69	"part_of"	44	1
70	"part_of"	44	1
71	"part_of"	44	1
72	"part_of"	44	1
73	"part_of"	44	1
74	"part_of"	44	1
75	"part_of"	44	1
76	"part_of"	44	1
77	"part_of"	44	1
78	"part_of"	44	1
79	"part_of"	44	1
80	"part_of"	44	1
81	"part_of"	44	1
82	"part_of"	44	1
83	"part_of"	45	1
84	"part_of"	45	1
85	"part_of"	45	1
86	"part_of"	45	1
87	"part_of"	45	1
88	"part_of"	45	1
89	"part_of"	45	1
90	"part_of"	45	1
91	"part_of"	45	1
92	"part_of"	45	1
93	"part_of"	45	1
94	"part_of"	45	1
95	"part_of"	45	1
96	"part_of"	45	1
97	"part_of"	45	1
98	"part_of"	45	1
99	"part_of"	45	1
100	"part_of"	45	1
CREATE TABLE "termdict" (
	"termid" int NOT NULL,
	"term" varchar(1000),
	"prob" double,
	CONSTRAINT "termdict_termid_pkey" PRIMARY KEY ("termid")
);
COPY 4302 RECORDS INTO "termdict" FROM stdin USING DELIMITERS '\t';
0	"#STOPWORD#"	1
1	"a"	1
2	"about"	1
3	"above"	1
4	"across"	1
5	"after"	1
6	"again"	1
7	"against"	1
8	"all"	1
9	"almost"	1
10	"alone"	1
11	"along"	1
12	"already"	1
13	"also"	1
14	"although"	1
15	"always"	1
16	"among"	1
17	"an"	1
18	"and"	1
19	"another"	1
20	"any"	1
21	"anybody"	1
22	"anyone"	1
23	"anything"	1
24	"anywhere"	1
25	"are"	1
26	"area"	1
27	"areas"	1
28	"around"	1
29	"as"	1
30	"ask"	1
31	"asked"	1
32	"asking"	1
33	"asks"	1
34	"at"	1
35	"away"	1
36	"b"	1
37	"back"	1
38	"backed"	1
39	"backing"	1
40	"backs"	1
41	"be"	1
42	"became"	1
43	"because"	1
44	"become"	1
45	"becomes"	1
46	"been"	1
47	"before"	1
48	"began"	1
49	"behind"	1
50	"being"	1
51	"beings"	1
52	"best"	1
53	"better"	1
54	"between"	1
55	"big"	1
56	"both"	1
57	"but"	1
58	"by"	1
59	"c"	1
60	"came"	1
61	"can"	1
62	"cannot"	1
63	"case"	1
64	"cases"	1
65	"certain"	1
66	"certainly"	1
67	"clear"	1
68	"clearly"	1
69	"come"	1
70	"could"	1
71	"d"	1
72	"did"	1
73	"differ"	1
74	"different"	1
75	"differently"	1
76	"do"	1
77	"does"	1
78	"done"	1
79	"down"	1
80	"down"	1
81	"downed"	1
82	"downing"	1
83	"downs"	1
84	"during"	1
85	"e"	1
86	"each"	1
87	"early"	1
88	"either"	1
89	"end"	1
90	"ended"	1
91	"ending"	1
92	"ends"	1
93	"enough"	1
94	"even"	1
95	"evenly"	1
96	"ever"	1
97	"every"	1
98	"everybody"	1
99	"everyone"	1
100	"everything"	1
101	"everywhere"	1
102	"f"	1
103	"face"	1
104	"faces"	1
105	"fact"	1
106	"facts"	1
107	"far"	1
108	"felt"	1
109	"few"	1
110	"find"	1
111	"finds"	1
112	"first"	1
113	"for"	1
114	"four"	1
115	"from"	1
116	"full"	1
117	"fully"	1
118	"further"	1
119	"furthered"	1
120	"furthering"	1
121	"furthers"	1
122	"g"	1
123	"gave"	1
124	"general"	1
125	"generally"	1
126	"get"	1
127	"gets"	1
128	"give"	1
129	"given"	1
130	"gives"	1
131	"go"	1
132	"going"	1
133	"good"	1
134	"goods"	1
135	"got"	1
136	"great"	1
137	"greater"	1
138	"greatest"	1
139	"group"	1
140	"grouped"	1
141	"grouping"	1
142	"groups"	1
143	"h"	1
144	"had"	1
145	"has"	1
146	"have"	1
147	"having"	1
148	"he"	1
149	"her"	1
150	"here"	1
151	"herself"	1
152	"high"	1
153	"high"	1
154	"high"	1
155	"higher"	1
156	"highest"	1
157	"him"	1
158	"himself"	1
159	"his"	1
160	"how"	1
161	"however"	1
162	"i"	1
163	"if"	1
164	"important"	1
165	"in"	1
166	"interest"	1
167	"interested"	1
168	"interesting"	1
169	"interests"	1
170	"into"	1
171	"is"	1
172	"it"	1
173	"its"	1
174	"itself"	1
175	"j"	1
176	"just"	1
177	"k"	1
178	"keep"	1
179	"keeps"	1
180	"kind"	1
181	"knew"	1
182	"know"	1
183	"known"	1
184	"knows"	1
185	"l"	1
186	"large"	1
187	"largely"	1
188	"last"	1
189	"later"	1
190	"latest"	1
191	"least"	1
192	"less"	1
193	"let"	1
194	"lets"	1
195	"like"	1
196	"likely"	1
197	"long"	1
198	"longer"	1
199	"longest"	1
200	"m"	1
201	"made"	1
202	"make"	1
203	"making"	1
204	"man"	1
205	"many"	1
206	"may"	1
207	"me"	1
208	"member"	1
209	"members"	1
210	"men"	1
211	"might"	1
212	"more"	1
213	"most"	1
214	"mostly"	1
215	"mr"	1
216	"mrs"	1
217	"much"	1
218	"must"	1
219	"my"	1
220	"myself"	1
221	"n"	1
222	"necessary"	1
223	"need"	1
224	"needed"	1
225	"needing"	1
226	"needs"	1
227	"never"	1
228	"new"	1
229	"new"	1
230	"newer"	1
231	"newest"	1
232	"next"	1
233	"no"	1
234	"nobody"	1
235	"non"	1
236	"noone"	1
237	"not"	1
238	"nothing"	1
239	"now"	1
240	"nowhere"	1
241	"number"	1
242	"numbers"	1
243	"o"	1
244	"of"	1
245	"off"	1
246	"often"	1
247	"old"	1
248	"older"	1
249	"oldest"	1
250	"on"	1
251	"once"	1
252	"one"	1
253	"only"	1
254	"open"	1
255	"opened"	1
256	"opening"	1
257	"opens"	1
258	"or"	1
259	"order"	1
260	"ordered"	1
261	"ordering"	1
262	"orders"	1
263	"other"	1
264	"others"	1
265	"our"	1
266	"out"	1
267	"over"	1
268	"p"	1
269	"part"	1
270	"parted"	1
271	"parting"	1
272	"parts"	1
273	"per"	1
274	"perhaps"	1
275	"place"	1
276	"places"	1
277	"point"	1
278	"pointed"	1
279	"pointing"	1
280	"points"	1
281	"possible"	1
282	"present"	1
283	"presented"	1
284	"presenting"	1
285	"presents"	1
286	"problem"	1
287	"problems"	1
288	"put"	1
289	"puts"	1
290	"q"	1
291	"quite"	1
292	"r"	1
293	"rather"	1
294	"really"	1
295	"right"	1
296	"right"	1
297	"room"	1
298	"rooms"	1
299	"s"	1
300	"said"	1
301	"same"	1
302	"saw"	1
303	"say"	1
304	"says"	1
305	"second"	1
306	"seconds"	1
307	"see"	1
308	"seem"	1
309	"seemed"	1
310	"seeming"	1
311	"seems"	1
312	"sees"	1
313	"sveral"	1
314	"shall"	1
315	"she"	1
316	"should"	1
317	"show"	1
318	"showed"	1
319	"showing"	1
320	"shows"	1
321	"side"	1
322	"sides"	1
323	"since"	1
324	"small"	1
325	"smaller"	1
326	"smallest"	1
327	"so"	1
328	"some"	1
329	"somebody"	1
330	"someone"	1
331	"something"	1
332	"somewhere"	1
333	"state"	1
334	"states"	1
335	"still"	1
336	"still"	1
337	"such"	1
338	"sure"	1
339	"t"	1
340	"take"	1
341	"taken"	1
342	"than"	1
343	"that"	1
344	"the"	1
345	"their"	1
346	"them"	1
347	"then"	1
348	"there"	1
349	"therefore"	1
350	"these"	1
351	"they"	1
352	"thing"	1
353	"things"	1
354	"think"	1
355	"thinks"	1
356	"this"	1
357	"those"	1
358	"though"	1
359	"thought"	1
360	"thoughts"	1
361	"three"	1
362	"through"	1
363	"thus"	1
364	"to"	1
365	"today"	1
366	"together"	1
367	"too"	1
368	"took"	1
369	"toward"	1
370	"turn"	1
371	"turned"	1
372	"turning"	1
373	"turns"	1
374	"two"	1
375	"u"	1
376	"under"	1
377	"until"	1
378	"up"	1
379	"upon"	1
380	"us"	1
381	"use"	1
382	"used"	1
383	"uses"	1
384	"v"	1
385	"very"	1
386	"w"	1
387	"want"	1
388	"wanted"	1
389	"wanting"	1
390	"wants"	1
391	"was"	1
392	"way"	1
393	"ways"	1
394	"we"	1
395	"well"	1
396	"wells"	1
397	"went"	1
398	"were"	1
399	"what"	1
400	"when"	1
401	"where"	1
402	"whether"	1
403	"which"	1
404	"while"	1
405	"who"	1
406	"whole"	1
407	"whose"	1
408	"why"	1
409	"will"	1
410	"with"	1
411	"within"	1
412	"without"	1
413	"work"	1
414	"worked"	1
415	"working"	1
416	"works"	1
417	"would"	1
418	"x"	1
419	"y"	1
420	"year"	1
421	"years"	1
422	"yet"	1
423	"you"	1
424	"young"	1
425	"younger"	1
426	"youngest"	1
427	"your"	1
428	"yours"	1
429	"z"	1
430	"ep"	1
431	"1030307"	1
432	"a1"	1
433	"20000823"	1
434	"en"	1
435	"00103038"	1
436	"20000215"	1
437	"jp"	1
438	"3837099"	1
439	"19990217"	1
440	"g11b"	1
441	"20"	1
442	"10"	1
443	"20060101a"	1
444	"i20051008rmep"	1
445	"20060101c"	1
446	"12"	1
447	"27"	1
448	"00"	1
449	"20060101ali20051220rmjp"	1
450	"20060101cli20051220rmjp"	1
451	"02"	1
452	"031"	1
453	"034"	1
454	"036"	1
455	"30"	1
456	"n20051008rmep"	1
457	"32"	1
458	"34"	1
459	"i20060722rmep"	1
460	"36"	1
461	"h04n"	1
462	"04"	1
463	"05"	1
464	"84"	1
465	"85"	1
466	"91"	1
467	"92"	1
468	"804"	1
469	"806"	1
470	"10c"	1
471	"10a1"	1
472	"32d2"	1
473	"804b"	1
474	"s11b"	1
475	"30c"	1
476	"t04n"	1
477	"806s"	1
478	"informationsaufzeichnungsmedium"	1
479	"vorrichtung"	1
480	"und"	1
481	"verfahren"	1
482	"zur"	1
483	"nachträglichen"	1
484	"aufzeichnung"	1
485	"auf"	1
486	"dem"	1
487	"information"	1
488	"recording"	1
489	"medium"	1
490	"apparatus"	1
491	"method"	1
492	"performing"	1
493	"support"	1
494	"enregistrement"	1
495	"informations"	1
496	"dispositif"	1
497	"et"	1
498	"procédé"	1
499	"rétractif"	1
500	"sur"	1
501	"le"	1
502	"0295148"	1
503	"0639927"	1
504	"a2"	1
505	"0644692"	1
506	"0903742"	1
507	"0924704"	1
508	"wo"	1
509	"1997003443"	1
510	"matsushita"	1
511	"electric"	1
512	"ind"	1
513	"co"	1
514	"ltd"	1
515	"industrial"	1
516	"1006"	1
517	"ohaza"	1
518	"kadoma"	1
519	"shi"	1
520	"osaka"	1
521	"571"	1
522	"8501"	1
523	"murase"	1
524	"kaoru"	1
525	"okada"	1
526	"tomoyuki"	1
527	"sugimoto"	1
528	"noriko"	1
529	"tsuga"	1
530	"kazuhiro"	1
531	"829"	1
532	"105"	1
533	"meyasukita"	1
534	"ikaruga"	1
535	"cho"	1
536	"ikoma"	1
537	"gun"	1
538	"nara"	1
539	"636"	1
540	"0133"	1
541	"6101"	1
542	"myokenzaka"	1
543	"katano"	1
544	"576"	1
545	"0021"	1
546	"933"	1
547	"hanayashiki"	1
548	"tsutsujigaoka"	1
549	"takarazuka"	1
550	"hyogo"	1
551	"665"	1
552	"0803"	1
553	"113"	1
554	"11"	1
555	"nakayamadai"	1
556	"0876"	1
557	"eisenführ"	1
558	"speiser"	1
559	"amp"	1
560	"partner"	1
561	"martinistrasse"	1
562	"24"	1
563	"28195"	1
564	"bremen"	1
565	"de"	1
566	"fr"	1
567	"gb"	1
568	"capacity"	1
569	"capable"	1
570	"read"	1
571	"write"	1
572	"operation"	1
573	"speed"	1
574	"optical"	1
575	"disc"	1
576	"includes"	1
577	"audio"	1
578	"stream"	1
579	"prepared"	1
580	"data"	1
581	"attribute"	1
582	"bit"	1
583	"rate"	1
584	"recorded"	1
585	"management"	1
586	"recorder"	1
587	"check"	1
588	"unit"	1
589	"checking"	1
590	"advance"	1
591	"possibility"	1
592	"reference"	1
593	"background"	1
594	"invention"	1
595	"field"	1
596	"relates"	1
597	"written"	1
598	"particularly"	1
599	"perform"	1
600	"thereto"	1
601	"related"	1
602	"art"	1
603	"writable"	1
604	"upper"	1
605	"bound"	1
606	"approximately"	1
607	"650"	1
608	"mb"	1
609	"phase"	1
610	"change"	1
611	"type"	1
612	"dvd"	1
613	"ram"	1
614	"several"	1
615	"appeared"	1
616	"moreover"	1
617	"addition"	1
618	"practical"	1
619	"mpeg"	1
620	"mpeg2"	1
621	"coding"	1
622	"standard"	1
623	"digital"	1
624	"av"	1
625	"expected"	1
626	"reproducing"	1
627	"media"	1
628	"computer"	1
629	"application"	1
630	"words"	1
631	"spread"	1
632	"magnetic"	1
633	"tape"	1
634	"conventionally"	1
635	"typical"	1
636	"description"	1
637	"recent"	1
638	"enhancement"	1
639	"density"	1
640	"developed"	1
641	"record"	1
642	"video"	1
643	"example"	1
644	"convexo"	1
645	"concavo"	1
646	"shaped"	1
647	"guide"	1
648	"groove"	1
649	"formed"	1
650	"signal"	1
651	"land"	1
652	"portion"	1
653	"portions"	1
654	"consequently"	1
655	"enhanced"	1
656	"approximate"	1
657	"twice"	1
658	"japanese"	1
659	"patent"	1
660	"laid"	1
661	"publication"	1
662	"87282"	1
663	"devised"	1
664	"practically"	1
665	"zone"	1
666	"clv"	1
667	"control"	1
668	"constant"	1
669	"linear"	1
670	"velocity"	1
671	"effective"	1
672	"simplified"	1
673	"easily"	1
674	"793873"	1
675	"future"	1
676	"including"	1
677	"using"	1
678	"intended"	1
679	"increase"	1
680	"implement"	1
681	"performance"	1
682	"greatly"	1
683	"exceeding"	1
684	"conventional"	1
685	"functions"	1
686	"appearance"	1
687	"supposed"	1
688	"mainstream"	1
689	"reproduction"	1
690	"conversion"	1
691	"various"	1
692	"influences"	1
693	"function"	1
694	"feature"	1
695	"random"	1
696	"access"	1
697	"considerably"	1
698	"subjected"	1
699	"usually"	1
700	"time"	1
701	"minutes"	1
702	"rewinding"	1
703	"extraordinarily"	1
704	"late"	1
705	"compared"	1
706	"seek"	1
707	"2060"	1
708	"ms"	1
709	"accordingly"	1
710	"act"	1
711	"device"	1
712	"respect"	1
713	"distributed"	1
714	"performed"	1
715	"implemented"	1
716	"fig"	1
717	"block"	1
718	"diagram"	1
719	"drive"	1
720	"drawing"	1
721	"numeral"	1
722	"denotes"	1
723	"pick"	1
724	"reading"	1
725	"ecc"	1
726	"error"	1
727	"correcting"	1
728	"code"	1
729	"processing"	1
730	"section"	1
731	"13"	1
732	"track"	1
733	"buffer"	1
734	"14"	1
735	"switch"	1
736	"switching"	1
737	"input"	1
738	"output"	1
739	"15"	1
740	"encoder"	1
741	"16"	1
742	"decoder"	1
743	"17"	1
744	"enlarged"	1
745	"shown"	1
746	"sector"	1
747	"2kb"	1
748	"minimum"	1
749	"executed"	1
750	"sectors"	1
751	"serves"	1
752	"variable"	1
753	"efficiently"	1
754	"va"	1
755	"fixed"	1
756	"vb"	1
757	"according"	1
758	"complexity"	1
759	"contents"	1
760	"thereof"	1
761	"image"	1
762	"absorb"	1
763	"difference"	1
764	"required"	1
765	"set"	1
766	"cd"	1
767	"utilizing"	1
768	"effectively"	1
769	"discretely"	1
770	"provided"	1
771	"35"	1
772	"35a"	1
773	"address"	1
774	"space"	1
775	"separately"	1
776	"continuous"	1
777	"region"	1
778	"a3"	1
779	"a4"	1
780	"continuously"	1
781	"reproduced"	1
782	"supplying"	1
783	"stored"	1
784	"carried"	1
785	"status"	1
786	"obtained"	1
787	"35b"	1
788	"t1"	1
789	"continues"	1
790	"t2"	1
791	"amount"	1
792	"period"	1
793	"represented"	1
794	"sufficient"	1
795	"consumed"	1
796	"supplied"	1
797	"till"	1
798	"t3"	1
799	"corresponding"	1
800	"start"	1
801	"kept"	1
802	"generated"	1
803	"playback"	1
804	"picture"	1
805	"considered"	1
806	"similarly"	1
807	"described"	1
808	"earlier"	1
809	"international"	1
810	"referred"	1
811	"iso"	1
812	"iec13818"	1
813	"gbs"	1
814	"exactly"	1
815	"compressed"	1
816	"compressing"	1
817	"widely"	1
818	"world"	1
819	"lsi"	1
820	"technology"	1
821	"improved"	1
822	"codec"	1
823	"expansion"	1
824	"compression"	1
825	"mainly"	1
826	"following"	1
827	"features"	1
828	"highly"	1
829	"efficient"	1
830	"correlation"	1
831	"characteristic"	1
832	"frames"	1
833	"introduced"	1
834	"frequency"	1
835	"motion"	1
836	"frame"	1
837	"classified"	1
838	"kinds"	1
839	"intra"	1
840	"relationship"	1
841	"past"	1
842	"relationships"	1
843	"thereby"	1
844	"pictures"	1
845	"refers"	1
846	"closest"	1
847	"display"	1
848	"coincident"	1
849	"assigned"	1
850	"dynamically"	1
851	"accordance"	1
852	"comprises"	1
853	"assign"	1
854	"complex"	1
855	"hard"	1
856	"compress"	1
857	"storing"	1
858	"selected"	1
859	"carrying"	1
860	"dolby"	1
861	"ac"	1
862	"lpcm"	1
863	"size"	1
864	"plural"	1
865	"sizes"	1
866	"multiplexed"	1
867	"system"	1
868	"37"	1
869	"structure"	1
870	"41"	1
871	"pack"	1
872	"header"	1
873	"42"	1
874	"packet"	1
875	"43"	1
876	"payload"	1
877	"hierarchical"	1
878	"divided"	1
879	"proper"	1
880	"head"	1
881	"stores"	1
882	"id"	1
883	"identifying"	1
884	"decoding"	1
885	"dts"	1
886	"stamp"	1
887	"omitted"	1
888	"presentation"	1
889	"pts"	1
890	"included"	1
891	"precision"	1
892	"90"	1
893	"khz"	1
894	"plurality"	1
895	"packets"	1
896	"scr"	1
897	"clock"	1
898	"representing"	1
899	"mhz"	1
900	"2048"	1
901	"mentioned"	1
902	"38"	1
903	"model"	1
904	"pstd"	1
905	"51"	1
906	"stc"	1
907	"acting"	1
908	"52"	1
909	"demultiplexer"	1
910	"demultiplexing"	1
911	"53"	1
912	"54"	1
913	"55"	1
914	"reorder"	1
915	"temporarily"	1
916	"56"	1
917	"adjusting"	1
918	"outputs"	1
919	"57"	1
920	"58"	1
921	"process"	1
922	"manner"	1
923	"inputs"	1
924	"interpret"	1
925	"transfer"	1
926	"fetches"	1
927	"carry"	1
928	"decode"	1
929	"displays"	1
930	"decoded"	1
931	"connected"	1
932	"previous"	1
933	"decodes"	1
934	"cast"	1
935	"multiplexing"	1
936	"figs"	1
937	"39a"	1
938	"39d"	1
939	"39b"	1
940	"39c"	1
941	"axis"	1
942	"abscissa"	1
943	"indicates"	1
944	"base"	1
945	"common"	1
946	"ordinate"	1
947	"usage"	1
948	"storage"	1
949	"thick"	1
950	"line"	1
951	"transition"	1
952	"basis"	1
953	"furthermore"	1
954	"gradient"	1
955	"equivalent"	1
956	"reduction"	1
957	"interval"	1
958	"besides"	1
959	"intersection"	1
960	"oblique"	1
961	"dotted"	1
962	"started"	1
963	"hereinafter"	1
964	"requires"	1
965	"vbv"	1
966	"delay"	1
967	"result"	1
968	"position"	1
969	"hand"	1
970	"require"	1
971	"dynamic"	1
972	"reason"	1
973	"little"	1
974	"preceded"	1
975	"restricted"	1
976	"defined"	1
977	"shift"	1
978	"maximum"	1
979	"strictly"	1
980	"speaking"	1
981	"followed"	1
982	"theory"	1
983	"simple"	1
984	"ratio"	1
985	"transferred"	1
986	"unnecessary"	1
987	"quickly"	1
988	"created"	1
989	"intentionally"	1
990	"precedence"	1
991	"based"	1
992	"restrictions"	1
993	"40"	1
994	"illustrating"	1
995	"relation"	1
996	"figure"	1
997	"parallel"	1
998	"running"	1
999	"direction"	1
1000	"easy"	1
1001	"independently"	1
1002	"analog"	1
1003	"simultaneously"	1
1004	"zero"	1
1005	"generation"	1
1006	"biggest"	1
1007	"caused"	1
1008	"mechanism"	1
1009	"prior"	1
1010	"channels"	1
1011	"sound"	1
1012	"writing"	1
1013	"operations"	1
1014	"implementing"	1
1015	"ups"	1
1016	"operated"	1
1017	"changing"	1
1018	"rotating"	1
1019	"accessed"	1
1020	"zones"	1
1021	"synchronous"	1
1022	"inconsistent"	1
1023	"existing"	1
1024	"normally"	1
1025	"processed"	1
1026	"worst"	1
1027	"hang"	1
1028	"store"	1
1029	"streams"	1
1030	"format"	1
1031	"apply"	1
1032	"recorders"	1
1033	"analyzed"	1
1034	"summary"	1
1035	"directed"	1
1036	"provide"	1
1037	"actualize"	1
1038	"determination"	1
1039	"aspect"	1
1040	"replaced"	1
1041	"indicative"	1
1042	"therein"	1
1043	"original"	1
1044	"starts"	1
1045	"third"	1
1046	"referring"	1
1047	"determining"	1
1048	"able"	1
1049	"encode"	1
1050	"deciding"	1
1051	"determined"	1
1052	"determine"	1
1053	"warning"	1
1054	"user"	1
1055	"ability"	1
1056	"noted"	1
1057	"1138370"	1
1058	"filed"	1
1059	"japan"	1
1060	"herein"	1
1061	"incorporated"	1
1062	"brief"	1
1063	"drawings"	1
1064	"logical"	1
1065	"embodiment"	1
1066	"file"	1
1067	"movie"	1
1068	"rtr"	1
1069	"vmg"	1
1070	"vmgi"	1
1071	"vern"	1
1072	"tm"	1
1073	"formats"	1
1074	"pl"	1
1075	"srp"	1
1076	"ty"	1
1077	"create"	1
1078	"ptm"	1
1079	"vob"	1
1080	"entn"	1
1081	"avfit"	1
1082	"atr"	1
1083	"sp"	1
1084	"plt"	1
1085	"avfi"	1
1086	"vobi"	1
1087	"18"	1
1088	"tmapi"	1
1089	"19"	1
1090	"vobu"	1
1091	"ent"	1
1092	"21"	1
1093	"oa"	1
1094	"22"	1
1095	"23"	1
1096	"25"	1
1097	"26"	1
1098	"ud"	1
1099	"pgcit"	1
1100	"txtdt"	1
1101	"mg"	1
1102	"28"	1
1103	"pgci"	1
1104	"29"	1
1105	"pg"	1
1106	"ci"	1
1107	"31"	1
1108	"epi"	1
1109	"33"	1
1110	"ty1"	1
1111	"chart"	1
1112	"band"	1
1113	"41a"	1
1114	"directory"	1
1115	"41b"	1
1116	"physical"	1
1117	"allocation"	1
1118	"42a"	1
1119	"42b"	1
1120	"date"	1
1121	"44"	1
1122	"inserting"	1
1123	"45"	1
1124	"partial"	1
1125	"46"	1
1126	"restoring"	1
1127	"47"	1
1128	"48"	1
1129	"49"	1
1130	"50"	1
1131	"buffers"	1
1132	"52a"	1
1133	"timing"	1
1134	"52b"	1
1135	"controller"	1
1136	"flowchart"	1
1137	"dubbing"	1
1138	"routine"	1
1139	"detailed"	1
1140	"preferred"	1
1141	"embodiments"	1
1142	"detail"	1
1143	"seen"	1
1144	"lead"	1
1145	"stabilizing"	1
1146	"servo"	1
1147	"identification"	1
1148	"logically"	1
1149	"finally"	1
1150	"volume"	1
1151	"directly"	1
1152	"dealt"	1
1153	"rt"	1
1154	"root"	1
1155	"roughly"	1
1156	"ordinarily"	1
1157	"files"	1
1158	"table"	1
1159	"pgc"	1
1160	"object"	1
1161	"means"	1
1162	"program"	1
1163	"defines"	1
1164	"cell"	1
1165	"sections"	1
1166	"significant"	1
1167	"player"	1
1168	"plays"	1
1169	"vobs"	1
1170	"name"	1
1171	"identifier"	1
1172	"length"	1
1173	"audio0"	1
1174	"audio1"	1
1175	"mode"	1
1176	"flag"	1
1177	"quantization"	1
1178	"coefficient"	1
1179	"sampling"	1
1180	"pgcs"	1
1181	"cells"	1
1182	"specifying"	1
1183	"played"	1
1184	"managed"	1
1185	"accessing"	1
1186	"addresses"	1
1187	"freely"	1
1188	"edit"	1
1189	"simply"	1
1190	"actual"	1
1191	"actually"	1
1192	"accesses"	1
1193	"adding"	1
1194	"calculate"	1
1195	"respectively"	1
1196	"identical"	1
1197	"units"	1
1198	"value"	1
1199	"scrs"	1
1200	"references"	1
1201	"copy"	1
1202	"fields"	1
1203	"pst"	1
1204	"values"	1
1205	"course"	1
1206	"explicitly"	1
1207	"indicate"	1
1208	"dummy"	1
1209	"flags"	1
1210	"putting"	1
1211	"remain"	1
1212	"purpose"	1
1213	"cording"	1
1214	"heater"	1
1215	"completely"	1
1216	"exchanging"	1
1217	"neither"	1
1218	"underflows"	1
1219	"nor"	1
1220	"overflows"	1
1221	"modes"	1
1222	"rates"	1
1223	"ensure"	1
1224	"prevent"	1
1225	"overflow"	1
1226	"underflow"	1
1227	"impossible"	1
1228	"replacement"	1
1229	"sets"	1
1230	"algorithms"	1
1231	"changed"	1
1232	"rewritten"	1
1233	"apparent"	1
1234	"completed"	1
1235	"satisfy"	1
1236	"conditions"	1
1237	"silent"	1
1238	"insignificant"	1
1239	"switched"	1
1240	"boundary"	1
1241	"designate"	1
1242	"designated"	1
1243	"form"	1
1244	"host"	1
1245	"microprocessor"	1
1246	"play"	1
1247	"solved"	1
1248	"erase"	1
1249	"overwritten"	1
1250	"tries"	1
1251	"except"	1
1252	"restored"	1
1253	"copying"	1
1254	"modified"	1
1255	"individual"	1
1256	"contrary"	1
1257	"return"	1
1258	"regarded"	1
1259	"independent"	1
1260	"causes"	1
1261	"interface"	1
1262	"7801"	1
1263	"7802"	1
1264	"7803"	1
1265	"7804"	1
1266	"7805"	1
1267	"7806"	1
1268	"7807"	1
1269	"7808"	1
1270	"transfers"	1
1271	"displayed"	1
1272	"accepts"	1
1273	"request"	1
1274	"wholly"	1
1275	"ad"	1
1276	"converter"	1
1277	"transmits"	1
1278	"interprets"	1
1279	"module"	1
1280	"encoding"	1
1281	"carries"	1
1282	"sent"	1
1283	"encoded"	1
1284	"driver"	1
1285	"records"	1
1286	"fetched"	1
1287	"users"	1
1288	"stop"	1
1289	"transmitted"	1
1290	"stops"	1
1291	"informs"	1
1292	"termination"	1
1293	"empty"	1
1294	"modifies"	1
1295	"clip"	1
1296	"sequence"	1
1297	"particular"	1
1298	"inserted"	1
1299	"outputting"	1
1300	"7804a"	1
1301	"7804b"	1
1302	"multiplexer"	1
1303	"7804c"	1
1304	"encodes"	1
1305	"performs"	1
1306	"packing"	1
1307	"packetizing"	1
1308	"copied"	1
1309	"immediately"	1
1310	"called"	1
1311	"seeking"	1
1312	"regions"	1
1313	"blocks"	1
1314	"subsequently"	1
1315	"buffer1"	1
1316	"buffer3"	1
1317	"buffer2"	1
1318	"buffer4"	1
1319	"illustrated"	1
1320	"series"	1
1321	"taking"	1
1322	"constituted"	1
1323	"amounts"	1
1324	"tb1"	1
1325	"tb3"	1
1326	"increased"	1
1327	"decreased"	1
1328	"tb2"	1
1329	"tb4"	1
1330	"ta"	1
1331	"reads"	1
1332	"tb"	1
1333	"overwrites"	1
1334	"onto"	1
1335	"overwriting"	1
1336	"t4"	1
1337	"repeating"	1
1338	"flow"	1
1339	"replaces"	1
1340	"consecutively"	1
1341	"notifies"	1
1342	"completion"	1
1343	"sequentially"	1
1344	"processes"	1
1345	"remaining"	1
1346	"terminated"	1
1347	"changes"	1
1348	"playing"	1
1349	"message"	1
1350	"prevents"	1
1351	"thinking"	1
1352	"failed"	1
1353	"expects"	1
1354	"payloads"	1
1355	"packetized"	1
1356	"comes"	1
1357	"arranged"	1
1358	"precedes"	1
1359	"sharing"	1
1360	"concretely"	1
1361	"pcm"	1
1362	"channel"	1
1363	"rarely"	1
1364	"operate"	1
1365	"suitable"	1
1366	"merchandise"	1
1367	"target"	1
1368	"applied"	1
1369	"decide"	1
1370	"operable"	1
1371	"basically"	1
1372	"characterized"	1
1373	"disk"	1
1374	"ordinary"	1
1375	"mov"	1
1376	"vro"	1
1377	"sto"	1
1378	"pck"	1
1379	"sub"	1
1380	"kb"	1
1381	"gop"	1
1382	"mutually"	1
1383	"successively"	1
1384	"vogi"	1
1385	"vog"	1
1386	"lump"	1
1387	"linearity"	1
1388	"vbr"	1
1389	"uniquely"	1
1390	"correspond"	1
1391	"filter"	1
1392	"tmap"	1
1393	"converting"	1
1394	"entries"	1
1395	"selects"	1
1396	"definitions"	1
1397	"layer"	1
1398	"bundled"	1
1399	"therebetween"	1
1400	"list"	1
1401	"ifo"	1
1402	"real"	1
1403	"seven"	1
1404	"tables"	1
1405	"org"	1
1406	"mnfit"	1
1407	"mat"	1
1408	"srpt"	1
1409	"mt"	1
1410	"obtain"	1
1411	"structural"	1
1412	"vmg0"	1
1413	"indicating"	1
1414	"ea"	1
1415	"version"	1
1416	"tz"	1
1417	"offset"	1
1418	"tx"	1
1419	"greenwich"	1
1420	"mean"	1
1421	"universal"	1
1422	"static"	1
1423	"displaying"	1
1424	"soundless"	1
1425	"chrs"	1
1426	"character"	1
1427	"primary"	1
1428	"text"	1
1429	"below"	1
1430	"sa"	1
1431	"search"	1
1432	"pointer"	1
1433	"comprising"	1
1434	"srpti"	1
1435	"srps"	1
1436	"ns"	1
1437	"giving"	1
1438	"0000b"	1
1439	"0001b"	1
1440	"0010b"	1
1441	"hybrid"	1
1442	"pgcn"	1
1443	"prm"	1
1444	"txti"	1
1445	"television"	1
1446	"ascii"	1
1447	"specified"	1
1448	"txt"	1
1449	"srpn"	1
1450	"optionally"	1
1451	"link"	1
1452	"thm"	1
1453	"ptri"	1
1454	"thumb"	1
1455	"nail"	1
1456	"representative"	1
1457	"cn"	1
1458	"corresponds"	1
1459	"pt"	1
1460	"indicated"	1
1461	"entry"	1
1462	"avfiti"	1
1463	"sti"	1
1464	"succeeding"	1
1465	"exists"	1
1466	"presence"	1
1467	"00b"	1
1468	"01b"	1
1469	"tv"	1
1470	"525"	1
1471	"60"	1
1472	"ntsc"	1
1473	"625"	1
1474	"pal"	1
1475	"resolution"	1
1476	"line21"	1
1477	"closed"	1
1478	"caption"	1
1479	"1b"	1
1480	"0b"	1
1481	"000b"	1
1482	"720"	1
1483	"480"	1
1484	"001b"	1
1485	"702"	1
1486	"010b"	1
1487	"352"	1
1488	"011b"	1
1489	"240"	1
1490	"288"	1
1491	"100b"	1
1492	"544"	1
1493	"101b"	1
1494	"ast"	1
1495	"spst"	1
1496	"atr0"	1
1497	"extended"	1
1498	"mixed"	1
1499	"10b"	1
1500	"auxiliary"	1
1501	"voice"	1
1502	"drc"	1
1503	"range"	1
1504	"bits"	1
1505	"fs"	1
1506	"monophonic"	1
1507	"stereo"	1
1508	"0011b"	1
1509	"0100b"	1
1510	"five"	1
1511	"0101b"	1
1512	"six"	1
1513	"0110b"	1
1514	"0111b"	1
1515	"eight"	1
1516	"1001b"	1
1517	"dual"	1
1518	"bitrate"	1
1519	"0000"	1
1520	"64"	1
1521	"kbps"	1
1522	"89"	1
1523	"96"	1
1524	"112"	1
1525	"128"	1
1526	"160"	1
1527	"192"	1
1528	"1000b"	1
1529	"224"	1
1530	"256"	1
1531	"1010b"	1
1532	"320"	1
1533	"1011b"	1
1534	"384"	1
1535	"1100b"	1
1536	"448"	1
1537	"1101b"	1
1538	"768"	1
1539	"1110b"	1
1540	"1536"	1
1541	"basic"	1
1542	"excluding"	1
1543	"expressed"	1
1544	"atr1"	1
1545	"subtitles"	1
1546	"animation"	1
1547	"color"	1
1548	"palette"	1
1549	"gi"	1
1550	"smli"	1
1551	"agapi"	1
1552	"cp"	1
1553	"mngi"	1
1554	"te"	1
1555	"normal"	1
1556	"temporary"	1
1557	"erasing"	1
1558	"a0"	1
1559	"11b"	1
1560	"aps"	1
1561	"preventing"	1
1562	"sml"	1
1563	"flg"	1
1564	"seamlessly"	1
1565	"seamless"	1
1566	"gap"	1
1567	"loc"	1
1568	"gaps"	1
1569	"rec"	1
1570	"corrected"	1
1571	"erasure"	1
1572	"synchronously"	1
1573	"cam"	1
1574	"coder"	1
1575	"elapsed"	1
1576	"absorbing"	1
1577	"month"	1
1578	"day"	1
1579	"hour"	1
1580	"minute"	1
1581	"fraction"	1
1582	"stin"	1
1583	"prev"	1
1584	"stp"	1
1585	"len"	1
1586	"cpg"	1
1587	"cpgi"	1
1588	"protecting"	1
1589	"free"	1
1590	"tmpai"	1
1591	"map"	1
1592	"ofs"	1
1593	"adr"	1
1594	"follows"	1
1595	"tmu"	1
1596	"600"	1
1597	"500"	1
1598	"nth"	1
1599	"diff"	1
1600	"desirable"	1
1601	"istref"	1
1602	"sz"	1
1603	"packs"	1
1604	"pb"	1
1605	"types"	1
1606	"depending"	1
1607	"pgciti"	1
1608	"constituting"	1
1609	"pointers"	1
1610	"pcg"	1
1611	"details"	1
1612	"txtdti"	1
1613	"idcd"	1
1614	"tmcd"	1
1615	"genre"	1
1616	"30h"	1
1617	"31h"	1
1618	"music"	1
1619	"32h"	1
1620	"drama"	1
1621	"33h"	1
1622	"34h"	1
1623	"sports"	1
1624	"35h"	1
1625	"documentary"	1
1626	"36h"	1
1627	"news"	1
1628	"37h"	1
1629	"weather"	1
1630	"38h"	1
1631	"education"	1
1632	"39h"	1
1633	"hobby"	1
1634	"3ah"	1
1635	"entertainment"	1
1636	"3bh"	1
1637	"opera"	1
1638	"3ch"	1
1639	"shopping"	1
1640	"source"	1
1641	"60h"	1
1642	"broadcasting"	1
1643	"station"	1
1644	"61h"	1
1645	"62h"	1
1646	"photograph"	1
1647	"63h"	1
1648	"memo"	1
1649	"64h"	1
1650	"pgi"	1
1651	"programs"	1
1652	"protect"	1
1653	"mi"	1
1654	"locations"	1
1655	"stilt"	1
1656	"differs"	1
1657	"78021"	1
1658	"78022"	1
1659	"checks"	1
1660	"comparing"	1
1661	"notified"	1
1662	"via"	1
1663	"predetermined"	1
1664	"charts"	1
1665	"receiving"	1
1666	"desired"	1
1667	"s1"	1
1668	"s2"	1
1669	"checked"	1
1670	"s3"	1
1671	"s4"	1
1672	"s5"	1
1673	"s6"	1
1674	"step"	1
1675	"s31"	1
1676	"s32"	1
1677	"recordable"	1
1678	"s33"	1
1679	"s34"	1
1680	"atri"	1
1681	"s35"	1
1682	"s36"	1
1683	"s37"	1
1684	"s38"	1
1685	"decided"	1
1686	"s39"	1
1687	"otherwise"	1
1688	"s40"	1
1689	"firstly"	1
1690	"s321"	1
1691	"s322"	1
1692	"notice"	1
1693	"served"	1
1694	"response"	1
1695	"waited"	1
1696	"s323"	1
1697	"s324"	1
1698	"s325"	1
1699	"creates"	1
1700	"especially"	1
1701	"limited"	1
1702	"applicable"	1
1703	"rewritable"	1
1704	"compares"	1
1705	"advantage"	1
1706	"connection"	1
1707	"modifications"	1
1708	"corrections"	1
1709	"applications"	1
1710	"skilled"	1
1711	"disclosure"	1
1712	"scope"	1
1713	"appended"	1
1714	"claims"	1
1715	"claim"	1
1716	"wherein"	1
1717	"notifying"	1
1718	"determines"	1
1719	"decides"	1
1720	"steam"	1
1721	"opposition"	1
1722	"translation"	1
1723	"national"	1
1724	"ref"	1
1725	"country"	1
1726	"legal"	1
1727	"event"	1
1728	"if02"	1
1729	"document"	1
1730	"60000011"	1
1731	"20011018"	1
1732	"contracting"	1
1733	"b1"	1
1734	"defrgb"	1
1735	"payment"	1
1736	"designation"	1
1737	"fees"	1
1738	"examination"	1
1739	"report"	1
1740	"20010202"	1
1741	"20000404"	1
1742	"extension"	1
1743	"european"	1
1744	"al"	1
1745	"lt"	1
1746	"lv"	1
1747	"mk"	1
1748	"ro"	1
1749	"si"	1
1750	"20010912"	1
1751	"20010221"	1
1752	"g11b20"	1
1753	"g11b27"	1
1754	"h04n9"	1
1755	"s11b27"	1
1756	"t04n5"	1
1757	"t04n9"	1
1758	"anoblique"	1
1759	"afterrecord"	1
1760	"97"	1
1761	"034443"	1
1762	"replace"	1
1763	"bitstream"	1
1764	"describing"	1
1765	"attributes"	1
1766	"monaural"	1
1767	"surround"	1
1768	"specific"	1
1769	"language"	1
1770	"claimed"	1
1771	"proposed"	1
1772	"cl"	1
1773	"vmgo"	1
1774	"ao"	1
1775	"afterrecording"	1
1776	"mit"	1
1777	"einem"	1
1778	"bereich"	1
1779	"zumindest"	1
1780	"ein"	1
1781	"videoobjekt"	1
1782	"gespeichert"	1
1783	"ist"	1
1784	"eine"	1
1785	"bearbeitungsinformation"	1
1786	"wobei"	1
1787	"jedes"	1
1788	"enthält"	1
1789	"einen"	1
1790	"videostrom"	1
1791	"der"	1
1792	"durch"	1
1793	"codieren"	1
1794	"eines"	1
1795	"videosignals"	1
1796	"erhalten"	1
1797	"wird"	1
1798	"ersten"	1
1799	"audiostrom"	1
1800	"audiosignals"	1
1801	"zweiten"	1
1802	"die"	1
1803	"zahl"	1
1804	"enthaltenen"	1
1805	"audioströme"	1
1806	"codierbetriebsart"	1
1807	"des"	1
1808	"audiostroms"	1
1809	"dadurch"	1
1810	"gekennzeichnet"	1
1811	"dass"	1
1812	"weiteren"	1
1813	"statusinformation"	1
1814	"angibt"	1
1815	"ob"	1
1816	"zweite"	1
1817	"für"	1
1818	"nachaufzeichnungsoperation"	1
1819	"bereitgestellt"	1
1820	"oder"	1
1821	"nicht"	1
1822	"nachaufzeichnungsdaten"	1
1823	"zu"	1
1824	"aufgezeichnet"	1
1825	"werden"	1
1826	"wenn"	1
1827	"nach"	1
1828	"anspruch"	1
1829	"codierungsbetriebsart"	1
1830	"erste"	1
1831	"dieselbe"	1
1832	"wie"	1
1833	"aufweist"	1
1834	"nachaufzeichnung"	1
1835	"informationsaufzeichnungsvorrichtung"	1
1836	"durchführung"	1
1837	"einer"	1
1838	"ansprüche"	1
1839	"gespeicherten"	1
1840	"enthalten"	1
1841	"umfasst"	1
1842	"extraktionseinrichtung"	1
1843	"zum"	1
1844	"extrahieren"	1
1845	"sind"	1
1846	"dritte"	1
1847	"vierte"	1
1848	"überprüfungsteil"	1
1849	"vorabprüfen"	1
1850	"durchführen"	1
1851	"basierend"	1
1852	"betreibbar"	1
1853	"vor"	1
1854	"startteil"	1
1855	"starten"	1
1856	"den"	1
1857	"entschieden"	1
1858	"extrahiereinrichtung"	1
1859	"das"	1
1860	"aufzeichnungsmedium"	1
1861	"aufgezeichneten"	1
1862	"unter"	1
1863	"verwendung"	1
1864	"aufzeichnungsvorrichtung"	1
1865	"prüfung"	1
1866	"extracting"	1
1867	"forth"	1
1868	"starting"	1
1869	"comprenant"	1
1870	"une"	1
1871	"mémorisant"	1
1872	"au"	1
1873	"moins"	1
1874	"un"	1
1875	"objet"	1
1876	"vidéo"	1
1877	"traitement"	1
1878	"dans"	1
1879	"lequel"	1
1880	"chaque"	1
1881	"comprend"	1
1882	"flux"	1
1883	"qui"	1
1884	"est"	1
1885	"obtenu"	1
1886	"codant"	1
1887	"premier"	1
1888	"deuxième"	1
1889	"les"	1
1890	"comprennent"	1
1891	"nombre"	1
1892	"compris"	1
1893	"codage"	1
1894	"du"	1
1895	"caractérisé"	1
1896	"ce"	1
1897	"que"	1
1898	"outre"	1
1899	"état"	1
1900	"indiquant"	1
1901	"ou"	1
1902	"destiné"	1
1903	"opération"	1
1904	"données"	1
1905	"sont"	1
1906	"enregistrées"	1
1907	"lorsque"	1
1908	"débit"	1
1909	"binaire"	1
1910	"débitbinaire"	1
1911	"selon"	1
1912	"la"	1
1913	"revendication"	1
1914	"possède"	1
1915	"même"	1
1916	"appareil"	1
1917	"réaliser"	1
1918	"mémorisé"	1
1919	"quelconque"	1
1920	"revendications"	1
1921	"moyen"	1
1922	"extraction"	1
1923	"extraire"	1
1924	"a1status"	1
1925	"mémorisées"	1
1926	"troisième"	1
1927	"quatrième"	1
1928	"vérification"	1
1929	"destinée"	1
1930	"vérifier"	1
1931	"avance"	1
1932	"exploitable"	1
1933	"afin"	1
1934	"se"	1
1935	"basant"	1
1936	"avant"	1
1937	"démarrage"	1
1938	"démarrer"	1
1939	"lorsquil"	1
1940	"décidé"	1
1941	"par"	1
1942	"pour"	1
1943	"enregistré"	1
1944	"utilisant"	1
1945	"déterminé"	1
1946	"effectuer"	1
1947	"1030380"	1
1948	"20051109"	1
1949	"00103260"	1
1950	"20000217"	1
1951	"4014199"	1
1952	"19990218"	1
1953	"20050513"	1
1954	"b82b"	1
1955	"h01l"	1
1956	"39"	1
1957	"h03k"	1
1958	"20060101cfi20051220rmjp"	1
1959	"195"	1
1960	"20060101afi20051220rmjp"	1
1961	"h01l39"	1
1962	"22d"	1
1963	"y01n4"	1
1964	"digitales"	1
1965	"einzelflussquantenbauelement"	1
1966	"single"	1
1967	"quantum"	1
1968	"quantique"	1
1969	"unique"	1
1970	"riken"	1
1971	"sugano"	1
1972	"takuo"	1
1973	"aoyagi"	1
1974	"yoshinobu"	1
1975	"ishibashi"	1
1976	"koji"	1
1977	"kanda"	1
1978	"akinobu"	1
1979	"hirosawa"	1
1980	"wako"	1
1981	"saitama"	1
1982	"ken"	1
1983	"351"	1
1984	"0198"	1
1985	"liesegang"	1
1986	"eva"	1
1987	"forrester"	1
1988	"boehmert"	1
1989	"pettenkoferstrasse"	1
1990	"2022"	1
1991	"80336"	1
1992	"münchen"	1
1993	"superconductors"	1
1994	"employing"	1
1995	"quanta"	1
1996	"preamble"	1
1997	"567"	1
1998	"383"	1
1999	"similar"	1
2000	"characterization"	1
2001	"superconducing"	1
2002	"rings"	1
2003	"devices"	1
2004	"abstracts"	1
2005	"conference"	1
2006	"solid"	1
2007	"materials"	1
2008	"september"	1
2009	"1998"	1
2010	"09"	1
2011	"pages"	1
2012	"208"	1
2013	"209"	1
2014	"xp"	1
2015	"000823144"	1
2016	"society"	1
2017	"physics"	1
2018	"josephson"	1
2019	"junction"	1
2020	"fast"	1
2021	"low"	1
2022	"power"	1
2023	"consumption"	1
2024	"connecting"	1
2025	"thin"	1
2026	"insulating"	1
2027	"film"	1
2028	"cooper"	1
2029	"pairs"	1
2030	"cause"	1
2031	"supercurrent"	1
2032	"pass"	1
2033	"due"	1
2034	"tunneling"	1
2035	"effect"	1
2036	"principle"	1
2037	"voltage"	1
2038	"logic"	1
2039	"latching"	1
2040	"strong"	1
2041	"nonlinear"	1
2042	"current"	1
2043	"characteristics"	1
2044	"move"	1
2045	"increases"	1
2046	"origin"	1
2047	"path"	1
2048	"remains"	1
2049	"superconducting"	1
2050	"limit"	1
2051	"intensity"	1
2052	"flows"	1
2053	"reaches"	1
2054	"critical"	1
2055	"switches"	1
2056	"suddenly"	1
2057	"thereafter"	1
2058	"monotonously"	1
2059	"varies"	1
2060	"cbdo"	1
2061	"appear"	1
2062	"hysteretic"	1
2063	"discrete"	1
2064	"initial"	1
2065	"increasing"	1
2066	"beyond"	1
2067	"unable"	1
2068	"removed"	1
2069	"sin"	1
2070	"wave"	1
2071	"opposite"	1
2072	"insulator"	1
2073	"instantly"	1
2074	"methods"	1
2075	"realize"	1
2076	"controlled"	1
2077	"coupling"	1
2078	"driving"	1
2079	"direct"	1
2080	"passes"	1
2081	"coil"	1
2082	"disposed"	1
2083	"near"	1
2084	"reduce"	1
2085	"applying"	1
2086	"flowing"	1
2087	"drawbacks"	1
2088	"terminal"	1
2089	"special"	1
2090	"contrivance"	1
2091	"isolate"	1
2092	"drawback"	1
2093	"driven"	1
2094	"circuit"	1
2095	"isolator"	1
2096	"isolating"	1
2097	"relatively"	1
2098	"isolated"	1
2099	"ground"	1
2100	"additional"	1
2101	"install"	1
2102	"neighboring"	1
2103	"spaced"	1
2104	"apart"	1
2105	"affected"	1
2106	"difficult"	1
2107	"construct"	1
2108	"integrated"	1
2109	"level"	1
2110	"integration"	1
2111	"employed"	1
2112	"operating"	1
2113	"semiconductor"	1
2114	"associated"	1
2115	"fan"	1
2116	"load"	1
2117	"inductance"	1
2118	"returned"	1
2119	"returning"	1
2120	"achieved"	1
2121	"previously"	1
2122	"acdriven"	1
2123	"dcdriven"	1
2124	"implementation"	1
2125	"rsfq"	1
2126	"rapid"	1
2127	"likharev"	1
2128	"1991"	1
2129	"solve"	1
2130	"provides"	1
2131	"considering"	1
2132	"foregoing"	1
2133	"raising"	1
2134	"dealing"	1
2135	"signals"	1
2136	"realizing"	1
2137	"design"	1
2138	"sfq"	1
2139	"ring"	1
2140	"divide"	1
2141	"regulating"	1
2142	"detecting"	1
2143	"decreases"	1
2144	"decrease"	1
2145	"detected"	1
2146	"detection"	1
2147	"facilitating"	1
2148	"designing"	1
2149	"objects"	1
2150	"advantages"	1
2151	"accompanying"	1
2152	"graph"	1
2153	"electron"	1
2154	"transistor"	1
2155	"3a"	1
2156	"3b"	1
2157	"dependency"	1
2158	"energy"	1
2159	"aluminum"	1
2160	"shielding"	1
2161	"fluxoid"	1
2162	"4a"	1
2163	"4b"	1
2164	"graphs"	1
2165	"assistance"	1
2166	"explaining"	1
2167	"setting"	1
2168	"gate"	1
2169	"stage"	1
2170	"view"	1
2171	"1a"	1
2172	"tunnel"	1
2173	"forms"	1
2174	"abcda"	1
2175	"divides"	1
2176	"aefda"	1
2177	"ebcfe"	1
2178	"width"	1
2179	"penetration"	1
2180	"depth"	1
2181	"uniformly"	1
2182	"substantially"	1
2183	"shape"	1
2184	"joint"	1
2185	"charging"	1
2186	"variation"	1
2187	"pair"	1
2188	"junctions"	1
2189	"electrode"	1
2190	"varied"	1
2191	"sandwiching"	1
2192	"regulated"	1
2193	"detects"	1
2194	"supply"	1
2195	"supplies"	1
2196	"bias"	1
2197	"pa"	1
2198	"metal"	1
2199	"superconductor"	1
2200	"conductor"	1
2201	"resistance"	1
2202	"kω"	1
2203	"configuration"	1
2204	"suppose"	1
2205	"center"	1
2206	"lies"	1
2207	"fdae"	1
2208	"ef"	1
2209	"ebcf"	1
2210	"realized"	1
2211	"sensitive"	1
2212	"results"	1
2213	"experiment"	1
2214	"conducted"	1
2215	"verify"	1
2216	"dimensions"	1
2217	"interposed"	1
2218	"100"	1
2219	"obvious"	1
2220	"drain"	1
2221	"appears"	1
2222	"expressions"	1
2223	"cross"	1
2224	"london"	1
2225	"parameter"	1
2226	"lengths"	1
2227	"ae"	1
2228	"eb"	1
2229	"bc"	1
2230	"cf"	1
2231	"fd"	1
2232	"da"	1
2233	"induced"	1
2234	"circulating"	1
2235	"negligible"	1
2236	"condition"	1
2237	"expression"	1
2238	"double"	1
2239	"placed"	1
2240	"meets"	1
2241	"expressing"	1
2242	"217"	1
2243	"120"	1
2244	"242"	1
2245	"understood"	1
2246	"calculations"	1
2247	"lower"	1
2248	"curve"	1
2249	"local"	1
2250	"interrelated"	1
2251	"gates"	1
2252	"stages"	1
2253	"nand"	1
2254	"modification"	1
2255	"maintaining"	1
2256	"rapidity"	1
2257	"owing"	1
2258	"symmetry"	1
2259	"stable"	1
2260	"temperature"	1
2261	"atmosphere"	1
2262	"trapping"	1
2263	"employs"	1
2264	"mis"	1
2265	"instead"	1
2266	"supercurrents"	1
2267	"dependent"	1
2268	"forming"	1
2269	"lines"	1
2270	"sectional"	1
2271	"amplitude"	1
2272	"equal"	1
2273	"na"	1
2274	"selectively"	1
2275	"degree"	1
2276	"particularity"	1
2277	"obviously"	1
2278	"variations"	1
2279	"practiced"	1
2280	"specifically"	1
2281	"departing"	1
2282	"digitale"	1
2283	"einzelflußquantenvorrichtung"	1
2284	"umfassend"	1
2285	"supraleitfähige"	1
2286	"leitung"	1
2287	"sich"	1
2288	"erstreckt"	1
2289	"supraleitfähigen"	1
2290	"verbunden"	1
2291	"um"	1
2292	"zwei"	1
2293	"kleinere"	1
2294	"ringe"	1
2295	"unterteilen"	1
2296	"schalter"	1
2297	"regulieren"	1
2298	"suprastromflusses"	1
2299	"hindurch"	1
2300	"erfassungseinrichtung"	1
2301	"detektieren"	1
2302	"änderung"	1
2303	"beiden"	1
2304	"kleineren"	1
2305	"im"	1
2306	"wesentlichen"	1
2307	"gleiche"	1
2308	"fläche"	1
2309	"aufweisen"	1
2310	"daß"	1
2311	"verbindungsstelle"	1
2312	"bei"	1
2313	"welcher"	1
2314	"spannungsgesteuerte"	1
2315	"einrichtung"	1
2316	"suprastromfluß"	1
2317	"gemäß"	1
2318	"spannungsänderung"	1
2319	"erhöht"	1
2320	"absenkt"	1
2321	"supraleitfähiger"	1
2322	"einzelelektronentransistor"	1
2323	"spannung"	1
2324	"abgesenkt"	1
2325	"torelektrode"	1
2326	"angelegt"	1
2327	"vorangegangenen"	1
2328	"kleine"	1
2329	"tunnelübergangseinrichtung"	1
2330	"erfassen"	1
2331	"kann"	1
2332	"joined"	1
2333	"preceding"	1
2334	"numérique"	1
2335	"première"	1
2336	"ligne"	1
2337	"supraconductrice"	1
2338	"étendue"	1
2339	"anneau"	1
2340	"seconde"	1
2341	"connectée"	1
2342	"façon"	1
2343	"diviser"	1
2344	"deux"	1
2345	"anneaux"	1
2346	"plus"	1
2347	"petits"	1
2348	"commutation"	1
2349	"réguler"	1
2350	"supracourant"	1
2351	"circulant"	1
2352	"détection"	1
2353	"détecter"	1
2354	"changement"	1
2355	"grande"	1
2356	"partie"	1
2357	"identiques"	1
2358	"termes"	1
2359	"forme"	1
2360	"taille"	1
2361	"connecté"	1
2362	"raccord"	1
2363	"niveau"	1
2364	"duquel"	1
2365	"raccordées"	1
2366	"commandé"	1
2367	"tension"	1
2368	"augmente"	1
2369	"réduit"	1
2370	"fonction"	1
2371	"supraconducteur"	1
2372	"électron"	1
2373	"augmenté"	1
2374	"appliquée"	1
2375	"électrode"	1
2376	"grille"	1
2377	"comprise"	1
2378	"précédentes"	1
2379	"petit"	1
2380	"jonction"	1
2381	"effet"	1
2382	"20060810"	1
2383	"20040413"	1
2384	"8566"	1
2385	"20011105"	1
2386	"atbechcydedkesfifrgbgrieitlilumcnlptse"	1
2387	"y01n"	1
2388	"4567383"	1
2389	"xp000823144"	1
2390	"franz"	1
2391	"josef"	1
2392	"strasse"	1
2393	"80801"	1
2394	"bds"	1
2395	"2π"	1
2396	"trapped"	1
2397	"trap"	1
2398	"stably"	1
2399	"symmetrical"	1
2400	"hence"	1
2401	"optional"	1
2402	"spirit"	1
2403	"1030352"	1
2404	"00301156"	1
2405	"25170199"	1
2406	"c23c"	1
2407	"452"	1
2408	"c30b"	1
2409	"08"	1
2410	"314"	1
2411	"316"	1
2412	"i20070721rmep"	1
2413	"28e2c2d"	1
2414	"28e2c2n"	1
2415	"314b1"	1
2416	"316c2b2"	1
2417	"51n"	1
2418	"apparat"	1
2419	"herstellen"	1
2420	"von"	1
2421	"materialenschichten"	1
2422	"mittels"	1
2423	"atomarer"	1
2424	"gase"	1
2425	"layers"	1
2426	"atomic"	1
2427	"gases"	1
2428	"méthode"	1
2429	"former"	1
2430	"couches"	1
2431	"matériaux"	1
2432	"partir"	1
2433	"gaz"	1
2434	"atomiques"	1
2435	"10247624"	1
2436	"11121449"	1
2437	"4543271"	1
2438	"5374613"	1
2439	"5376628"	1
2440	"5812403"	1
2441	"1988000985"	1
2442	"vol"	1
2443	"december"	1
2444	"1231"	1
2445	"247624"	1
2446	"asahi"	1
2447	"chem"	1
2448	"0914"	1
2449	"1999"	1
2450	"july"	1
2451	"0730"	1
2452	"121449"	1
2453	"corp"	1
2454	"april"	1
2455	"0430"	1
2456	"inc"	1
2457	"3050"	1
2458	"bowers"	1
2459	"avenue"	1
2460	"santa"	1
2461	"clara"	1
2462	"california"	1
2463	"95054"	1
2464	"xia"	1
2465	"liqun"	1
2466	"yieh"	1
2467	"ellie"	1
2468	"5358"	1
2469	"carryback"	1
2470	"san"	1
2471	"jose"	1
2472	"ca"	1
2473	"95111"	1
2474	"shamrock"	1
2475	"court"	1
2476	"millbrae"	1
2477	"94030"	1
2478	"allard"	1
2479	"susan"	1
2480	"joyce"	1
2481	"boult"	1
2482	"wade"	1
2483	"tennant"	1
2484	"furnival"	1
2485	"street"	1
2486	"ec4a"	1
2487	"1pq"	1
2488	"material"	1
2489	"substrate"	1
2490	"gas"	1
2491	"heated"	1
2492	"elevated"	1
2493	"exposed"	1
2494	"reacts"	1
2495	"surface"	1
2496	"thereon"	1
2497	"preferably"	1
2498	"molecular"	1
2499	"operatively"	1
2500	"coupled"	1
2501	"remote"	1
2502	"microwave"	1
2503	"plasma"	1
2504	"dissociates"	1
2505	"reactive"	1
2506	"quality"	1
2507	"silicon"	1
2508	"dioxide"	1
2509	"oxynitride"	1
2510	"nitride"	1
2511	"dissociation"	1
2512	"nh"	1
2513	"reduced"	1
2514	"temperatures"	1
2515	"formation"	1
2516	"uniform"	1
2517	"heating"	1
2518	"ceramic"	1
2519	"recombination"	1
2520	"atoms"	1
2521	"inert"	1
2522	"dilute"	1
2523	"spatially"	1
2524	"separate"	1
2525	"coated"	1
2526	"protective"	1
2527	"coating"	1
2528	"atom"	1
2529	"gasses"	1
2530	"electronics"	1
2531	"continual"	1
2532	"scaling"	1
2533	"lateral"	1
2534	"oxide"	1
2535	"mos"	1
2536	"dielectric"	1
2537	"thickness"	1
2538	"maintain"	1
2539	"charge"	1
2540	"modern"	1
2541	"dimension"	1
2542	"requirements"	1
2543	"forced"	1
2544	"dielectrics"	1
2545	"angstrom"	1
2546	"regime"	1
2547	"proportional"	1
2548	"combination"	1
2549	"thinner"	1
2550	"voltages"	1
2551	"successive"	1
2552	"hot"	1
2553	"carrier"	1
2554	"damage"	1
2555	"breakdown"	1
2556	"strength"	1
2557	"major"	1
2558	"concerns"	1
2559	"regard"	1
2560	"additionally"	1
2561	"dimensionality"	1
2562	"led"	1
2563	"extensive"	1
2564	"fabrication"	1
2565	"techniques"	1
2566	"ebeam"	1
2567	"lithography"	1
2568	"ion"	1
2569	"etching"	1
2570	"employ"	1
2571	"energetic"	1
2572	"particles"	1
2573	"produce"	1
2574	"ionizing"	1
2575	"radiation"	1
2576	"furnace"	1
2577	"grown"	1
2578	"sio"	1
2579	"poor"	1
2580	"barrier"	1
2581	"boron"	1
2582	"diffusion"	1
2583	"doped"	1
2584	"polycrystalline"	1
2585	"electrodes"	1
2586	"problematic"	1
2587	"excess"	1
2588	"900"	1
2589	"growth"	1
2590	"exceeds"	1
2591	"thermal"	1
2592	"budget"	1
2593	"uniformity"	1
2594	"wafer"	1
2595	"oxygen"	1
2596	"inherent"	1
2597	"oxidation"	1
2598	"furnaces"	1
2599	"unacceptably"	1
2600	"properties"	1
2601	"compensated"	1
2602	"repeatable"	1
2603	"challenge"	1
2604	"potential"	1
2605	"alternative"	1
2606	"pure"	1
2607	"nitrided"	1
2608	"oxides"	1
2609	"oxynitrides"	1
2610	"ammonia"	1
2611	"nitrous"	1
2612	"annealed"	1
2613	"stack"	1
2614	"deposited"	1
2615	"incorporates"	1
2616	"percent"	1
2617	"nitrogen"	1
2618	"post"	1
2619	"annealing"	1
2620	"rich"	1
2621	"environment"	1
2622	"interfacial"	1
2623	"improves"	1
2624	"enhances"	1
2625	"typically"	1
2626	"suffer"	1
2627	"generates"	1
2628	"hydrogen"	1
2629	"traps"	1
2630	"deleteriously"	1
2631	"affect"	1
2632	"thicker"	1
2633	"scaleable"	1
2634	"exhibits"	1
2635	"superior"	1
2636	"term"	1
2637	"reliability"	1
2638	"moisture"	1
2639	"dopant"	1
2640	"suffers"	1
2641	"render"	1
2642	"commercial"	1
2643	"impractical"	1
2644	"despite"	1
2645	"decades"	1
2646	"research"	1
2647	"pressure"	1
2648	"chemical"	1
2649	"vapor"	1
2650	"deposition"	1
2651	"lpcvd"	1
2652	"tetrachloride"	1
2653	"sicl"	1
2654	"reacted"	1
2655	"hydrochloric"	1
2656	"acid"	1
2657	"hcl"	1
2658	"liberates"	1
2659	"severely"	1
2660	"impact"	1
2661	"electrical"	1
2662	"ineffective"	1
2663	"environmentally"	1
2664	"unfriendly"	1
2665	"incompatible"	1
2666	"green"	1
2667	"initiatives"	1
2668	"manufactures"	1
2669	"disadvantage"	1
2670	"exceed"	1
2671	"etc"	1
2672	"uniformities"	1
2673	"constraints"	1
2674	"novel"	1
2675	"readily"	1
2676	"surfaces"	1
2677	"suited"	1
2678	"growing"	1
2679	"films"	1
2680	"tetraethyl"	1
2681	"orthosilicate"	1
2682	"teos"	1
2683	"currently"	1
2684	"ozone"	1
2685	"adopted"	1
2686	"commercially"	1
2687	"oxidant"	1
2688	"election"	1
2689	"prefer"	1
2690	"molecules"	1
2691	"route"	1
2692	"enhance"	1
2693	"precise"	1
2694	"stochiometry"	1
2695	"minimized"	1
2696	"diluted"	1
2697	"argon"	1
2698	"separates"	1
2699	"reduces"	1
2700	"available"	1
2701	"sites"	1
2702	"elevational"	1
2703	"configured"	1
2704	"top"	1
2705	"plan"	1
2706	"automated"	1
2707	"tool"	1
2708	"fabricating"	1
2709	"chamber"	1
2710	"pipe"	1
2711	"pump"	1
2712	"foreline"	1
2713	"throttle"	1
2714	"valve"	1
2715	"gigafill"	1
2716	"manufactured"	1
2717	"commonly"	1
2718	"serial"	1
2719	"748"	1
2720	"883"	1
2721	"nov"	1
2722	"1996"	1
2723	"hereby"	1
2724	"entirety"	1
2725	"inlet"	1
2726	"distribution"	1
2727	"plate"	1
2728	"distributing"	1
2729	"located"	1
2730	"mounted"	1
2731	"supporting"	1
2732	"800"	1
2733	"mechanisms"	1
2734	"maxima"	1
2735	"magnetron"	1
2736	"tuner"	1
2737	"applicator"	1
2738	"waveguide"	1
2739	"43a"	1
2740	"guides"	1
2741	"pulsed"	1
2742	"centered"	1
2743	"ghz"	1
2744	"03000"	1
2745	"watts"	1
2746	"microwaves"	1
2747	"travel"	1
2748	"43b"	1
2749	"43c"	1
2750	"tuning"	1
2751	"elements"	1
2752	"stub"	1
2753	"tuners"	1
2754	"allow"	1
2755	"match"	1
2756	"impedance"	1
2757	"reflection"	1
2758	"delivered"	1
2759	"grow"	1
2760	"loaded"	1
2761	"evacuated"	1
2762	"torr"	1
2763	"presently"	1
2764	"pressures"	1
2765	"evacuation"	1
2766	"raise"	1
2767	"occurs"	1
2768	"significantly"	1
2769	"reached"	1
2770	"stabilized"	1
2771	"flowed"	1
2772	"generate"	1
2773	"concentration"	1
2774	"appropriate"	1
2775	"depends"	1
2776	"factors"	1
2777	"distance"	1
2778	"encountered"	1
2779	"travels"	1
2780	"1000"	1
2781	"3000"	1
2782	"leads"	1
2783	"friendly"	1
2784	"dissociated"	1
2785	"window"	1
2786	"allows"	1
2787	"outer"	1
2788	"interact"	1
2789	"ignition"	1
2790	"ultra"	1
2791	"violet"	1
2792	"light"	1
2793	"ionization"	1
2794	"sustains"	1
2795	"ionized"	1
2796	"species"	1
2797	"diluting"	1
2798	"ions"	1
2799	"recombine"	1
2800	"spacially"	1
2801	"proximate"	1
2802	"transport"	1
2803	"recombines"	1
2804	"close"	1
2805	"providing"	1
2806	"connector"	1
2807	"mounting"	1
2808	"adjacent"	1
2809	"adhering"	1
2810	"serving"	1
2811	"subsequent"	1
2812	"entire"	1
2813	"resists"	1
2814	"adhesion"	1
2815	"generating"	1
2816	"therewith"	1
2817	"undesirable"	1
2818	"complicated"	1
2819	"precursors"	1
2820	"reducing"	1
2821	"dilution"	1
2822	"reach"	1
2823	"relative"	1
2824	"identically"	1
2825	"cleaned"	1
2826	"fluorine"	1
2827	"812"	1
2828	"403"	1
2829	"exsitu"	1
2830	"wet"	1
2831	"cleaning"	1
2832	"loading"	1
2833	"improve"	1
2834	"efficiency"	1
2835	"cvd"	1
2836	"react"	1
2837	"requiring"	1
2838	"intermediate"	1
2839	"locks"	1
2840	"49a"	1
2841	"49b"	1
2842	"handler"	1
2843	"containing"	1
2844	"chambers"	1
2845	"importantly"	1
2846	"59"	1
2847	"controls"	1
2848	"contains"	1
2849	"parameters"	1
2850	"relevant"	1
2851	"removing"	1
2852	"47s"	1
2853	"vacuum"	1
2854	"contamination"	1
2855	"yield"	1
2856	"discloses"	1
2857	"disclosed"	1
2858	"fall"	1
2859	"skill"	1
2860	"instance"	1
2861	"sources"	1
2862	"exact"	1
2863	"depend"	1
2864	"etch"	1
2865	"person"	1
2866	"understand"	1
2867	"vary"	1
2868	"compensate"	1
2869	"elevating"	1
2870	"transferring"	1
2871	"placing"	1
2872	"dissociating"	1
2873	"depositing"	1
2874	"supported"	1
2875	"generator"	1
2876	"lining"	1
2877	"lock"	1
2878	"tc"	1
2879	"programmed"	1
2880	"20061220"	1
2881	"20020409"	1
2882	"degb"	1
2883	"1030386"	1
2884	"00301228"	1
2885	"20000216"	1
2886	"3929099"	1
2887	"h01m"	1
2888	"10c2"	1
2889	"10c2c2"	1
2890	"10c2d2"	1
2891	"t01m"	1
2892	"50s6"	1
2893	"batteriesatz"	1
2894	"batterielader"	1
2895	"stromversorgungseinrichtung"	1
2896	"elektronisches"	1
2897	"gerät"	1
2898	"battery"	1
2899	"electronic"	1
2900	"equipment"	1
2901	"bloc"	1
2902	"batteries"	1
2903	"chargeur"	1
2904	"énergie"	1
2905	"électrique"	1
2906	"électronique"	1
2907	"0588728"	1
2908	"0676819"	1
2909	"0836311"	1
2910	"4810204"	1
2911	"sony"	1
2912	"corporation"	1
2913	"735"	1
2914	"kitashinagawa"	1
2915	"6chome"	1
2916	"shinagawa"	1
2917	"ku"	1
2918	"tokyo"	1
2919	"takeshita"	1
2920	"toshio"	1
2921	"hanzawa"	1
2922	"masaki"	1
2923	"boden"	1
2924	"keith"	1
2925	"mcmurray"	1
2926	"fetter"	1
2927	"lane"	1
2928	"1da"	1
2929	"nl"	1
2930	"positively"	1
2931	"prohibits"	1
2932	"tilted"	1
2933	"casing"	1
2934	"housing"	1
2935	"terminals"	1
2936	"2123"	1
2937	"bottom"	1
2938	"angles"	1
2939	"front"	1
2940	"continuation"	1
2941	"discriminating"	1
2942	"recess"	1
2943	"centerline"	1
2944	"perpendicular"	1
2945	"recesses"	1
2946	"corners"	1
2947	"engaging"	1
2948	"bent"	1
2949	"housed"	1
2950	"external"	1
2951	"furnished"	1
2952	"hitherto"	1
2953	"sort"	1
2954	"detachably"	1
2955	"main"	1
2956	"body"	1
2957	"chargeable"	1
2958	"depletion"	1
2959	"charged"	1
2960	"grooves"	1
2961	"longitudinal"	1
2962	"slightly"	1
2963	"larger"	1
2964	"facing"	1
2965	"projections"	1
2966	"engaged"	1
2967	"inner"	1
2968	"rim"	1
2969	"permit"	1
2970	"explained"	1
2971	"respective"	1
2972	"furnishes"	1
2973	"occasionally"	1
2974	"lengthwise"	1
2975	"wise"	1
2976	"latter"	1
2977	"tilt"	1
2978	"401"	1
2979	"402"	1
2980	"tote"	1
2981	"mistaken"	1
2982	"405"	1
2983	"projection"	1
2984	"411"	1
2985	"thrust"	1
2986	"force"	1
2987	"occurrence"	1
2988	"409"	1
2989	"rides"	1
2990	"413"	1
2991	"imperfect"	1
2992	"optimum"	1
2993	"risk"	1
2994	"inadvertently"	1
2995	"descending"	1
2996	"destroy"	1
2997	"erroneously"	1
2998	"restricting"	1
2999	"prohibiting"	1
3000	"possibly"	1
3001	"leading"	1
3002	"aim"	1
3003	"prohibited"	1
3004	"destruction"	1
3005	"exemplified"	1
3006	"engagement"	1
3007	"whereby"	1
3008	"correctly"	1
3009	"oriented"	1
3010	"prevented"	1
3011	"inappropriate"	1
3012	"hereinbelow"	1
3013	"perspective"	1
3014	"camera"	1
3015	"looking"	1
3016	"14a"	1
3017	"14b"	1
3018	"views"	1
3019	"discrimination"	1
3020	"15a"	1
3021	"15b"	1
3022	"16a"	1
3023	"16d"	1
3024	"17a"	1
3025	"17c"	1
3026	"fourth"	1
3027	"fifth"	1
3028	"illumination"	1
3029	"adapted"	1
3030	"sorts"	1
3031	"synthetic"	1
3032	"resin"	1
3033	"guiding"	1
3034	"arrayed"	1
3035	"byside"	1
3036	"mid"	1
3037	"furnish"	1
3038	"residual"	1
3039	"outwardly"	1
3040	"positioned"	1
3041	"rectangular"	1
3042	"destroyed"	1
3043	"abutment"	1
3044	"symmetrically"	1
3045	"extending"	1
3046	"lshaped"	1
3047	"towards"	1
3048	"inside"	1
3049	"steps"	1
3050	"serve"	1
3051	"profile"	1
3052	"adjacency"	1
3053	"holds"	1
3054	"intruded"	1
3055	"protection"	1
3056	"proximal"	1
3057	"fractionated"	1
3058	"cut"	1
3059	"distal"	1
3060	"elastically"	1
3061	"flexible"	1
3062	"semi"	1
3063	"tubular"	1
3064	"inbetween"	1
3065	"flexibly"	1
3066	"protruded"	1
3067	"folded"	1
3068	"plates"	1
3069	"flexed"	1
3070	"establish"	1
3071	"tenacity"	1
3072	"thrusting"	1
3073	"height"	1
3074	"abutted"	1
3075	"safeguarded"	1
3076	"inadvertent"	1
3077	"cover"	1
3078	"rotation"	1
3079	"arrows"	1
3080	"piece"	1
3081	"61"	1
3082	"pieces"	1
3083	"62"	1
3084	"inclined"	1
3085	"63"	1
3086	"rotated"	1
3087	"arrow"	1
3088	"rotatably"	1
3089	"rotary"	1
3090	"pivot"	1
3091	"shaft"	1
3092	"periphery"	1
3093	"torsion"	1
3094	"spring"	1
3095	"retained"	1
3096	"biased"	1
3097	"moved"	1
3098	"overlying"	1
3099	"covered"	1
3100	"protected"	1
3101	"65"	1
3102	"66"	1
3103	"astride"	1
3104	"restrict"	1
3105	"assuming"	1
3106	"obliquely"	1
3107	"68"	1
3108	"insertion"	1
3109	"engageable"	1
3110	"prohibit"	1
3111	"abutting"	1
3112	"exhibit"	1
3113	"toughness"	1
3114	"meanwhile"	1
3115	"70"	1
3116	"pawls"	1
3117	"72"	1
3118	"selling"	1
3119	"centrally"	1
3120	"73"	1
3121	"lug"	1
3122	"74"	1
3123	"75"	1
3124	"holding"	1
3125	"flat"	1
3126	"76"	1
3127	"lever"	1
3128	"77"	1
3129	"causing"	1
3130	"movement"	1
3131	"biasing"	1
3132	"upstanding"	1
3133	"pawl"	1
3134	"79"	1
3135	"80"	1
3136	"projected"	1
3137	"81"	1
3138	"82"	1
3139	"protuberant"	1
3140	"disable"	1
3141	"hold"	1
3142	"fist"	1
3143	"dismounting"	1
3144	"disengaged"	1
3145	"rendered"	1
3146	"movable"	1
3147	"dismounted"	1
3148	"alternatively"	1
3149	"whilst"	1
3150	"disables"	1
3151	"intrusion"	1
3152	"axe"	1
3153	"satisfactorily"	1
3154	"tilting"	1
3155	"extremely"	1
3156	"detaching"	1
3157	"stated"	1
3158	"improper"	1
3159	"discriminated"	1
3160	"86"	1
3161	"tshaped"	1
3162	"88"	1
3163	"yshaped"	1
3164	"mating"	1
3165	"94"	1
3166	"98"	1
3167	"components"	1
3168	"depicted"	1
3169	"symbols"	1
3170	"101"	1
3171	"107"	1
3172	"103"	1
3173	"109"	1
3174	"intruding"	1
3175	"111"	1
3176	"controlling"	1
3177	"104"	1
3178	"115"	1
3179	"essential"	1
3180	"116"	1
3181	"edge"	1
3182	"118"	1
3183	"119"	1
3184	"121"	1
3185	"122"	1
3186	"124"	1
3187	"130"	1
3188	"126"	1
3189	"132"	1
3190	"134"	1
3191	"135"	1
3192	"127"	1
3193	"137"	1
3194	"138"	1
3195	"139"	1
3196	"140"	1
3197	"142"	1
3198	"143"	1
3199	"144"	1
3200	"146"	1
3201	"147"	1
3202	"household"	1
3203	"148"	1
3204	"variety"	1
3205	"149"	1
3206	"153"	1
3207	"155"	1
3208	"wiring"	1
3209	"cord"	1
3210	"adapter"	1
3211	"157"	1
3212	"150"	1
3213	"161"	1
3214	"151"	1
3215	"163"	1
3216	"165"	1
3217	"167"	1
3218	"168"	1
3219	"planar"	1
3220	"contiguous"	1
3221	"169"	1
3222	"171"	1
3223	"172"	1
3224	"constructed"	1
3225	"sized"	1
3226	"177"	1
3227	"175"	1
3228	"broken"	1
3229	"181"	1
3230	"183"	1
3231	"176"	1
3232	"185"	1
3233	"187"	1
3234	"188"	1
3235	"190"	1
3236	"191"	1
3237	"193"	1
3238	"194"	1
3239	"196"	1
3240	"197"	1
3241	"198"	1
3242	"422"	1
3243	"201"	1
3244	"204"	1
3245	"202"	1
3246	"106"	1
3247	"129"	1
3248	"182"	1
3249	"207"	1
3250	"205"	1
3251	"211"	1
3252	"212"	1
3253	"213"	1
3254	"215"	1
3255	"218"	1
3256	"peripheral"	1
3257	"220"	1
3258	"222"	1
3259	"integrally"	1
3260	"226"	1
3261	"228"	1
3262	"230"	1
3263	"232"	1
3264	"holes"	1
3265	"231"	1
3266	"actuating"	1
3267	"movably"	1
3268	"passed"	1
3269	"234"	1
3270	"verified"	1
3271	"compresses"	1
3272	"loadable"	1
3273	"238"	1
3274	"241"	1
3275	"239"	1
3276	"244"	1
3277	"246"	1
3278	"248"	1
3279	"250"	1
3280	"252"	1
3281	"253"	1
3282	"255"	1
3283	"257"	1
3284	"260"	1
3285	"262"	1
3286	"264"	1
3287	"263"	1
3288	"265"	1
3289	"268"	1
3290	"271"	1
3291	"269"	1
3292	"275"	1
3293	"272"	1
3294	"277"	1
3295	"279"	1
3296	"282"	1
3297	"rotationally"	1
3298	"285"	1
3299	"286"	1
3300	"290"	1
3301	"291"	1
3302	"site"	1
3303	"293"	1
3304	"295"	1
3305	"298"	1
3306	"hole"	1
3307	"296"	1
3308	"299"	1
3309	"casings"	1
3310	"permits"	1
3311	"appropriated"	1
3312	"abuts"	1
3313	"demonstrate"	1
3314	"loadability"	1
3315	"intrudable"	1
3316	"301"	1
3317	"illuminating"	1
3318	"changeover"	1
3319	"302"	1
3320	"303"	1
3321	"304"	1
3322	"309"	1
3323	"306"	1
3324	"307"	1
3325	"311"	1
3326	"313"	1
3327	"315"	1
3328	"317"	1
3329	"asone"	1
3330	"319"	1
3331	"321"	1
3332	"323"	1
3333	"325"	1
3334	"326"	1
3335	"328"	1
3336	"330"	1
3337	"332"	1
3338	"331"	1
3339	"333"	1
3340	"demonstrates"	1
3341	"consumes"	1
3342	"336"	1
3343	"337"	1
3344	"338"	1
3345	"removably"	1
3346	"339"	1
3347	"344"	1
3348	"341"	1
3349	"342"	1
3350	"346"	1
3351	"348"	1
3352	"350"	1
3353	"353"	1
3354	"355"	1
3355	"357"	1
3356	"protuberantly"	1
3357	"358"	1
3358	"360"	1
3359	"361"	1
3360	"363"	1
3361	"364"	1
3362	"366"	1
3363	"365"	1
3364	"368"	1
3365	"shapes"	1
3366	"conforming"	1
3367	"specifications"	1
3368	"suffices"	1
3369	"suitably"	1
3370	"modify"	1
3371	"manufacture"	1
3372	"molds"	1
3373	"curtail"	1
3374	"manufacturing"	1
3375	"cost"	1
3376	"secondary"	1
3377	"merely"	1
3378	"illustrative"	1
3379	"exchangeably"	1
3380	"dry"	1
3381	"lshape"	1
3382	"inverted"	1
3383	"half"	1
3384	"tube"	1
3385	"longitudinally"	1
3386	"protruding"	1
3387	"resiliently"	1
3388	"openings"	1
3389	"farther"	1
3390	"20040617"	1
3391	"20010129"	1
3392	"defrgbnl"	1
3393	"1030377"	1
3394	"00101575"	1
3395	"20000127"	1
3396	"22460899"	1
3397	"19990806"	1
3398	"2272799"	1
3399	"19990129"	1
3400	"00b2"	1
3401	"00b4c"	1
3402	"licht"	1
3403	"emittierende"	1
3404	"diode"	1
3405	"emitting"	1
3406	"émettrice"	1
3407	"lumière"	1
3408	"10150224"	1
3409	"8064872"	1
3410	"9283803"	1
3411	"5760479"	1
3412	"toyoda"	1
3413	"gosei"	1
3414	"kk"	1
3415	"koha"	1
3416	"426"	1
3417	"higashi"	1
3418	"oizumi"	1
3419	"nerima"	1
3420	"178"	1
3421	"0063"	1
3422	"nagahata"	1
3423	"ochiai"	1
3424	"haruhi"	1
3425	"nishikasugai"	1
3426	"aichi"	1
3427	"8564"	1
3428	"yoshikawa"	1
3429	"yukio"	1
3430	"hirano"	1
3431	"atsuo"	1
3432	"teshima"	1
3433	"kiyotaka"	1
3434	"yasukawa"	1
3435	"takemasa"	1
3436	"4603"	1
3437	"higashioike"	1
3438	"niiya"	1
3439	"jimokuji"	1
3440	"ama"	1
3441	"491"	1
3442	"0836"	1
3443	"pellmann"	1
3444	"hans"	1
3445	"bernd"	1
3446	"dipl"	1
3447	"ing"	1
3448	"patentanwaltsbüro"	1
3449	"tiedtke"	1
3450	"bühling"	1
3451	"kinne"	1
3452	"bavariaring"	1
3453	"square"	1
3454	"flip"	1
3455	"chip"	1
3456	"mount"	1
3457	"posture"	1
3458	"superposition"	1
3459	"triangular"	1
3460	"coincides"	1
3461	"parabola"	1
3462	"breakage"	1
3463	"excessive"	1
3464	"element"	1
3465	"widened"	1
3466	"vertical"	1
3467	"schematically"	1
3468	"depicts"	1
3469	"570"	1
3470	"520"	1
3471	"composed"	1
3472	"stem"	1
3473	"mold"	1
3474	"encloses"	1
3475	"527"	1
3476	"bonded"	1
3477	"silver"	1
3478	"paste"	1
3479	"electrically"	1
3480	"521"	1
3481	"toned"	1
3482	"528"	1
3483	"wire"	1
3484	"bonding"	1
3485	"gold"	1
3486	"emitted"	1
3487	"reflects"	1
3488	"positive"	1
3489	"sapphire"	1
3490	"radiates"	1
3491	"outside"	1
3492	"orientation"	1
3493	"downward"	1
3494	"12a"	1
3495	"attachment"	1
3496	"12b"	1
3497	"12c"	1
3498	"conductive"	1
3499	"insulation"	1
3500	"524"	1
3501	"523"	1
3502	"micro"	1
3503	"bump"	1
3504	"533"	1
3505	"soldered"	1
3506	"establishing"	1
3507	"negative"	1
3508	"pad"	1
3509	"531"	1
3510	"circular"	1
3511	"diameter"	1
3512	"p2"	1
3513	"p501"	1
3514	"inevitably"	1
3515	"parabolic"	1
3516	"enable"	1
3517	"deviates"	1
3518	"luminous"	1
3519	"viewed"	1
3520	"left"	1
3521	"secured"	1
3522	"luminance"	1
3523	"regardless"	1
3524	"maximized"	1
3525	"secure"	1
3526	"overall"	1
3527	"durability"	1
3528	"fabricated"	1
3529	"achieve"	1
3530	"shorter"	1
3531	"diagonal"	1
3532	"intersects"	1
3533	"angle"	1
3534	"parallelogram"	1
3535	"trapezoid"	1
3536	"quadrangle"	1
3537	"shifted"	1
3538	"degrees"	1
3539	"sixth"	1
3540	"placement"	1
3541	"seventh"	1
3542	"eighth"	1
3543	"insulative"	1
3544	"ninth"	1
3545	"tenth"	1
3546	"mark"	1
3547	"eleventh"	1
3548	"reflecting"	1
3549	"twelfth"	1
3550	"refection"	1
3551	"thirteenth"	1
3552	"fourteenth"	1
3553	"lens"	1
3554	"lamp"	1
3555	"sacrifice"	1
3556	"centers"	1
3557	"coincide"	1
3558	"necessity"	1
3559	"decreasing"	1
3560	"zener"	1
3561	"expectedly"	1
3562	"heat"	1
3563	"radiated"	1
3564	"bumps"	1
3565	"eliminates"	1
3566	"selection"	1
3567	"constituent"	1
3568	"alignment"	1
3569	"facilitated"	1
3570	"diodes"	1
3571	"aspects"	1
3572	"reflect"	1
3573	"1a1d"	1
3574	"2a2c"	1
3575	"explanatory"	1
3576	"4a4d"	1
3577	"6a6d"	1
3578	"10a"	1
3579	"11a"	1
3580	"gallium"	1
3581	"compound"	1
3582	"102"	1
3583	"aln"	1
3584	"ntype"	1
3585	"gan"	1
3586	"active"	1
3587	"ga"	1
3588	"ptype"	1
3589	"cladding"	1
3590	"contact"	1
3591	"110"	1
3592	"nickel"	1
3593	"ni"	1
3594	"131"	1
3595	"ag"	1
3596	"wall"	1
3597	"extends"	1
3598	"upward"	1
3599	"represents"	1
3600	"passing"	1
3601	"p1"	1
3602	"1c"	1
3603	"1d"	1
3604	"2a"	1
3605	"2b"	1
3606	"2c"	1
3607	"strip"	1
3608	"angled"	1
3609	"isosceles"	1
3610	"triangles"	1
3611	"hatched"	1
3612	"diagonally"	1
3613	"corner"	1
3614	"assembled"	1
3615	"45degree"	1
3616	"fixedly"	1
3617	"enclosed"	1
3618	"enables"	1
3619	"axes"	1
3620	"plane"	1
3621	"4c"	1
3622	"270"	1
3623	"p201"	1
3624	"4d"	1
3625	"243"	1
3626	"doping"	1
3627	"iii"	1
3628	"nlayer"	1
3629	"constitute"	1
3630	"pnjunction"	1
3631	"forward"	1
3632	"reverse"	1
3633	"221"	1
3634	"223"	1
3635	"reaching"	1
3636	"233"	1
3637	"225"	1
3638	"227"	1
3639	"encased"	1
3640	"disposition"	1
3641	"6a"	1
3642	"6b"	1
3643	"6c"	1
3644	"370"	1
3645	"p301"	1
3646	"6d"	1
3647	"343"	1
3648	"action"	1
3649	"324"	1
3650	"327"	1
3651	"420"	1
3652	"layered"	1
3653	"470"	1
3654	"p401"	1
3655	"rhodium"	1
3656	"rh"	1
3657	"denoted"	1
3658	"alloy"	1
3659	"443"	1
3660	"impurity"	1
3661	"441"	1
3662	"existed"	1
3663	"421"	1
3664	"433"	1
3665	"424"	1
3666	"windows"	1
3667	"431a"	1
3668	"431b"	1
3669	"431c"	1
3670	"positions"	1
3671	"425"	1
3672	"prevention"	1
3673	"attaining"	1
3674	"reflected"	1
3675	"427"	1
3676	"undergo"	1
3677	"mechanically"	1
3678	"deformed"	1
3679	"satisfactory"	1
3680	"brightness"	1
3681	"consisting"	1
3682	"platinum"	1
3683	"cobalt"	1
3684	"palladium"	1
3685	"pd"	1
3686	"magnesium"	1
3687	"vanadium"	1
3688	"manganese"	1
3689	"mn"	1
3690	"bismuth"	1
3691	"bi"	1
3692	"rhenium"	1
3693	"re"	1
3694	"copper"	1
3695	"cu"	1
3696	"tin"	1
3697	"sn"	1
3698	"multi"	1
3699	"metals"	1
3700	"titanium"	1
3701	"ti"	1
3702	"chromium"	1
3703	"cr"	1
3704	"niobium"	1
3705	"nb"	1
3706	"zinc"	1
3707	"zn"	1
3708	"tantalum"	1
3709	"molybdenum"	1
3710	"mo"	1
3711	"tungsten"	1
3712	"hafnium"	1
3713	"hf"	1
3714	"arbitrary"	1
3715	"insofar"	1
3716	"triangle"	1
3717	"reflectance"	1
3718	"polarities"	1
3719	"reversed"	1
3720	"nlayers"	1
3721	"avalanche"	1
3722	"vf"	1
3723	"mass"	1
3724	"produced"	1
3725	"electrostatic"	1
3726	"multiple"	1
3727	"laser"	1
3728	"spinel"	1
3729	"carbide"	1
3730	"phosphide"	1
3731	"arsenide"	1
3732	"20070125"	1
3733	"countries"	1
3734	"concerned"	1
3735	"alltlvmkrosi"	1
3736	"inventor"	1
3737	"correction"	1
3738	"1030381"	1
3739	"20021106"	1
3740	"99955564"	1
3741	"19990607"	1
3742	"ja"	1
3743	"15793698"	1
3744	"19980605"	1
3745	"9903029"	1
3746	"20011129"	1
3747	"053"	1
3748	"h05k"	1
3749	"h01l41"	1
3750	"t05k1"	1
3751	"02e"	1
3752	"t05k3"	1
3753	"befestigungsstruktur"	1
3754	"piezoelektrischen"	1
3755	"transformator"	1
3756	"befestigung"	1
3757	"transformators"	1
3758	"piezoelectric"	1
3759	"transformer"	1
3760	"procede"	1
3761	"montage"	1
3762	"transformateur"	1
3763	"piezo"	1
3764	"electrique"	1
3765	"nec"	1
3766	"tokin"	1
3767	"saito"	1
3768	"koichi"	1
3769	"kumasaka"	1
3770	"katsunori"	1
3771	"koriyama"	1
3772	"taihaku"	1
3773	"sendai"	1
3774	"miyagi"	1
3775	"982"	1
3776	"8510"	1
3777	"prüfer"	1
3778	"lutz"	1
3779	"phys"	1
3780	"gbr"	1
3781	"patentanwälte"	1
3782	"harthauser"	1
3783	"25d"	1
3784	"81545"	1
3785	"technical"	1
3786	"inverter"	1
3787	"backlight"	1
3788	"liquid"	1
3789	"crystal"	1
3790	"board"	1
3791	"equipped"	1
3792	"contained"	1
3793	"soldering"	1
3794	"component"	1
3795	"avoid"	1
3796	"vibration"	1
3797	"node"	1
3798	"elastic"	1
3799	"silicone"	1
3800	"adjust"	1
3801	"adjustment"	1
3802	"burst"	1
3803	"audible"	1
3804	"nodal"	1
3805	"deviated"	1
3806	"847"	1
3807	"093"	1
3808	"981"	1
3809	"adhered"	1
3810	"minimize"	1
3811	"frequencies"	1
3812	"advantageous"	1
3813	"subject"	1
3814	"matters"	1
3815	"nodes"	1
3816	"spot"	1
3817	"fashion"	1
3818	"spacer"	1
3819	"transmit"	1
3820	"excitation"	1
3821	"suppress"	1
3822	"embodying"	1
3823	"facilitate"	1
3824	"understanding"	1
3825	"multilayer"	1
3826	"internal"	1
3827	"extend"	1
3828	"hatching"	1
3829	"wires"	1
3830	"fixing"	1
3831	"adhesive"	1
3832	"beneath"	1
3833	"resonance"	1
3834	"resonates"	1
3835	"raised"	1
3836	"rubber"	1
3837	"total"	1
3838	"flexibility"	1
3839	"examples"	1
3840	"dimensional"	1
3841	"83"	1
3842	"mm"	1
3843	"boards"	1
3844	"comparison"	1
3845	"item"	1
3846	"product"	1
3847	"products"	1
3848	"db"	1
3849	"substantial"	1
3850	"deterioration"	1
3851	"observed"	1
3852	"acceptable"	1
3853	"none"	1
3854	"warping"	1
3855	"resulting"	1
3856	"deviation"	1
3857	"71"	1
3858	"87"	1
3859	"93"	1
3860	"95"	1
3861	"structures"	1
3862	"obtaining"	1
3863	"applicability"	1
3864	"transformatorelementes"	1
3865	"schaltplatine"	1
3866	"befestigt"	1
3867	"wenigstens"	1
3868	"schaltkreiskomponente"	1
3869	"ansteuern"	1
3870	"ausgestattet"	1
3871	"piezoelektrische"	1
3872	"transformatorelement"	1
3873	"elastischen"	1
3874	"befestigungsbauteil"	1
3875	"zwischen"	1
3876	"eingelegt"	1
3877	"schwingungsknotenabschnitte"	1
3878	"transformatoreiementes"	1
3879	"bedecken"	1
3880	"abstützbauteil"	1
3881	"aus"	1
3882	"hergestellt"	1
3883	"vorgesehen"	1
3884	"teil"	1
3885	"verbleibenden"	1
3886	"abschnittes"	1
3887	"außerhälb"	1
3888	"abzudecken"	1
3889	"kantenabschnitt"	1
3890	"ende"	1
3891	"streifenförmig"	1
3892	"gestaltet"	1
3893	"scheibenförmige"	1
3894	"gestalt"	1
3895	"streifenförmige"	1
3896	"hat"	1
3897	"schwingungsknotenabschnitten"	1
3898	"nut"	1
3899	"versehen"	1
3900	"abschnitten"	1
3901	"ausgebildet"	1
3902	"entsprechen"	1
3903	"nuten"	1
3904	"auβerhalb"	1
3905	"oben"	1
3906	"erwähnten"	1
3907	"abschnitte"	1
3908	"über"	1
3909	"breite"	1
3910	"enden"	1
3911	"hinaus"	1
3912	"erstrecken"	1
3913	"befestigungsverfahren"	1
3914	"transformacoreiement"	1
3915	"befestigungsbauteils"	1
3916	"stützbauteil"	1
3917	"angeordnet"	1
3918	"befestigtist"	1
3919	"außerhalb"	1
3920	"élément"	1
3921	"piézoélectrique"	1
3922	"monte"	1
3923	"carte"	1
3924	"équipée"	1
3925	"composant"	1
3926	"commander"	1
3927	"cet"	1
3928	"piézo"	1
3929	"laquelle"	1
3930	"fixé"	1
3931	"fixation"	1
3932	"élastique"	1
3933	"interposé"	1
3934	"entre"	1
3935	"manière"	1
3936	"couvrir"	1
3937	"parties"	1
3938	"noeuds"	1
3939	"caractérisée"	1
3940	"qu"	1
3941	"réalisé"	1
3942	"matériau"	1
3943	"prévu"	1
3944	"restante"	1
3945	"extérieur"	1
3946	"sous"	1
3947	"bord"	1
3948	"extrémité"	1
3949	"bande"	1
3950	"présente"	1
3951	"disque"	1
3952	"étendant"	1
3953	"parallèlement"	1
3954	"monté"	1
3955	"celui"	1
3956	"étant"	1
3957	"endroit"	1
3958	"plézo"	1
3959	"électrtque"	1
3960	"munie"	1
3961	"rainure"	1
3962	"formée"	1
3963	"correspondant"	1
3964	"aux"	1
3965	"rainures"	1
3966	"formées"	1
3967	"indiquées"	1
3968	"cidessus"	1
3969	"étendre"	1
3970	"largeur"	1
3971	"située"	1
3972	"audel"	1
3973	"extrémités"	1
3974	"disposé"	1
3975	"clans"	1
3976	"tonne"	1
3977	"trouve"	1
3978	"ceased"	1
3979	"renewal"	1
3980	"fee"	1
3981	"20060607"	1
3982	"20030807"	1
3983	"69903789"	1
3984	"20021212"	1
3985	"defrgbit"	1
3986	"fg4d"	1
3987	"applicant"	1
3988	"reassignment"	1
3989	"owner"	1
3990	"20010227"	1
3991	"20000128"	1
3992	"supplementary"	1
3993	"20000522"	1
3994	"t05k"	1
3995	"gehause"	1
3996	"piezoelektrischer"	1
3997	"0847093"	1
3998	"9965089a1"	1
3999	"documents"	1
4000	"depressed"	1
4001	"piezoelectiic"	1
4002	"halt"	1
4003	"nearer"	1
4004	"fix"	1
4005	"feast"	1
4006	"throughout"	1
4007	"1030303"	1
4008	"00103043"	1
4009	"vr990017"	1
4010	"g06k"	1
4011	"06"	1
4012	"n20070721rmep"	1
4013	"00d1a2a"	1
4014	"s06k"	1
4015	"06w6"	1
4016	"plattenförmiger"	1
4017	"aufzeichnungsträger"	1
4018	"kreisförmigem"	1
4019	"identifikationskode"	1
4020	"pourvu"	1
4021	"circulaire"	1
4022	"0567080"	1
4023	"0809245"	1
4024	"0833335"	1
4025	"0849734"	1
4026	"2762429"	1
4027	"10105974"	1
4028	"10105975"	1
4029	"10188361"	1
4030	"5289451"	1
4031	"5671202"	1
4032	"5862117"	1
4033	"1997041562"	1
4034	"1998052191"	1
4035	"0731"	1
4036	"105974"	1
4037	"victor"	1
4038	"0424"	1
4039	"105975"	1
4040	"october"	1
4041	"1031"	1
4042	"188361"	1
4043	"0721"	1
4044	"personal"	1
4045	"italia"	1
4046	"di"	1
4047	"batti"	1
4048	"battilani"	1
4049	"giancarlo"	1
4050	"lombardia"	1
4051	"fraz"	1
4052	"vito"	1
4053	"mantico"	1
4054	"37012"	1
4055	"bussolengo"	1
4056	"vr"	1
4057	"petraz"	1
4058	"gilberto"	1
4059	"glp"	1
4060	"piazzale"	1
4061	"cavedalis"	1
4062	"33100"	1
4063	"udine"	1
4064	"ch"	1
4065	"cy"	1
4066	"dk"	1
4067	"es"	1
4068	"fi"	1
4069	"gr"	1
4070	"ie"	1
4071	"li"	1
4072	"lu"	1
4073	"mc"	1
4074	"cdrom"	1
4075	"rom"	1
4076	"coaxial"	1
4077	"bar"	1
4078	"concentric"	1
4079	"reader"	1
4080	"unbalancing"	1
4081	"weight"	1
4082	"microchip"	1
4083	"radio"	1
4084	"transponder"	1
4085	"antenna"	1
4086	"recognized"	1
4087	"automatic"	1
4088	"vending"	1
4089	"renting"	1
4090	"machines"	1
4091	"carriers"	1
4092	"juke"	1
4093	"boxes"	1
4094	"tracks"	1
4095	"movies"	1
4096	"codes"	1
4097	"recognition"	1
4098	"systems"	1
4099	"remarkable"	1
4100	"life"	1
4101	"wares"	1
4102	"ware"	1
4103	"price"	1
4104	"expiry"	1
4105	"therof"	1
4106	"sticked"	1
4107	"packagings"	1
4108	"packaging"	1
4109	"improving"	1
4110	"aesthetical"	1
4111	"attractive"	1
4112	"luring"	1
4113	"purchase"	1
4114	"manufacturers"	1
4115	"market"	1
4116	"imposing"	1
4117	"prices"	1
4118	"disclosing"	1
4119	"supermarkets"	1
4120	"retail"	1
4121	"malls"	1
4122	"cashiers"	1
4123	"digiting"	1
4124	"purchased"	1
4125	"articles"	1
4126	"converts"	1
4127	"readable"	1
4128	"digits"	1
4129	"mail"	1
4130	"sorting"	1
4131	"simplify"	1
4132	"correct"	1
4133	"addressing"	1
4134	"letters"	1
4135	"destination"	1
4136	"readers"	1
4137	"involve"	1
4138	"disadvantages"	1
4139	"run"	1
4140	"rectilinear"	1
4141	"detect"	1
4142	"impressed"	1
4143	"operator"	1
4144	"conveyors"	1
4145	"paths"	1
4146	"somewhat"	1
4147	"standardised"	1
4148	"allowing"	1
4149	"save"	1
4150	"memory"	1
4151	"article"	1
4152	"convey"	1
4153	"newspapers"	1
4154	"magazines"	1
4155	"matter"	1
4156	"printed"	1
4157	"paper"	1
4158	"hardly"	1
4159	"irregular"	1
4160	"roll"	1
4161	"simplier"	1
4162	"laying"	1
4163	"true"	1
4164	"fruit"	1
4165	"vegetables"	1
4166	"packaged"	1
4167	"sacks"	1
4168	"bags"	1
4169	"makes"	1
4170	"conveyor"	1
4171	"orient"	1
4172	"aside"	1
4173	"1949"	1
4174	"bernard"	1
4175	"nj"	1
4176	"woodland"	1
4177	"invented"	1
4178	"label"	1
4179	"beam"	1
4180	"solution"	1
4181	"implies"	1
4182	"purchasing"	1
4183	"mantainance"	1
4184	"costs"	1
4185	"procedures"	1
4186	"printing"	1
4187	"659"	1
4188	"415"	1
4189	"706"	1
4190	"095"	1
4191	"056"	1
4192	"429"	1
4193	"395"	1
4194	"308"	1
4195	"483"	1
4196	"cited"	1
4197	"mentions"	1
4198	"compact"	1
4199	"describes"	1
4200	"sounds"	1
4201	"astonishing"	1
4202	"considers"	1
4203	"universally"	1
4204	"roms"	1
4205	"becoming"	1
4206	"popular"	1
4207	"identified"	1
4208	"box"	1
4209	"container"	1
4210	"alternately"	1
4211	"radially"	1
4212	"silk"	1
4213	"screen"	1
4214	"comprised"	1
4215	"centre"	1
4216	"assure"	1
4217	"referenced"	1
4218	"seal"	1
4219	"crossing"	1
4220	"aware"	1
4221	"spite"	1
4222	"disks"	1
4223	"rotate"	1
4224	"beams"	1
4225	"sticking"	1
4226	"unavoidably"	1
4227	"lacks"	1
4228	"balance"	1
4229	"reductions"	1
4230	"lack"	1
4231	"electromagnetic"	1
4232	"rfid"	1
4233	"transponders"	1
4234	"car"	1
4235	"motor"	1
4236	"key"	1
4237	"immobilizers"	1
4238	"anti"	1
4239	"burglar"	1
4240	"useful"	1
4241	"aims"	1
4242	"inconveniences"	1
4243	"outline"	1
4244	"advantageously"	1
4245	"automatically"	1
4246	"illustration"	1
4247	"limiting"	1
4248	"help"	1
4249	"figures"	1
4250	"attached"	1
4251	"schematical"	1
4252	"sign"	1
4253	"purposes"	1
4254	"wording"	1
4255	"images"	1
4256	"coded"	1
4257	"central"	1
4258	"axle"	1
4259	"turntable"	1
4260	"advantgeously"	1
4261	"quick"	1
4262	"spatial"	1
4263	"overcoming"	1
4264	"solutions"	1
4265	"follow"	1
4266	"arcuated"	1
4267	"bars"	1
4268	"withstanding"	1
4269	"own"	1
4270	"passive"	1
4271	"embedded"	1
4272	"activate"	1
4273	"agent"	1
4274	"submitted"	1
4275	"department"	1
4276	"exclusively"	1
4277	"machine"	1
4278	"cartridges"	1
4279	"withdrawn"	1
4280	"customer"	1
4281	"select"	1
4282	"keyboards"	1
4283	"thanks"	1
4284	"crossed"	1
4285	"activated"	1
4286	"transponde"	1
4287	"grasping"	1
4288	"seize"	1
4289	"withdraw"	1
4290	"seat"	1
4291	"insert"	1
4292	"musical"	1
4293	"stay"	1
4294	"recognize"	1
4295	"variant"	1
4296	"mechanical"	1
4297	"equivalences"	1
4298	"characterised"	1
4299	"agents"	1
4300	"deemed"	1
4301	"20010419"	1
CREATE TABLE "tf_sum" (
	"termid" int,
	"docid" int,
	"prob" double
);
COPY 1000 RECORDS INTO "tf_sum" FROM stdin USING DELIMITERS '\t';
430	20	0.00095683606208802892
430	21	0.00081625249410484314
430	27	0.000293513354857646
432	20	0.00095683606208802892
432	21	0.0014511155450752766
432	28	0.00023038820412394885
434	21	0.001723199709776891
434	25	0.0032921810699588477
434	22	0.00030646644192460924
434	27	0.0035221602582917524
434	19	0.00077821011673151756
437	27	0.000293513354857646
437	19	0.0015564202334630351
441	20	0.00042526047203912394
441	21	0.00036277888626881915
441	25	0.00041152263374485596
441	24	0.00049825610363726954
441	22	0.00030646644192460924
441	28	0.0027646584494873862
441	23	0.0031152647975077881
442	20	0.00085052094407824788
442	21	0.00045347360783602395
442	25	0.0057613168724279839
442	24	0.0069755854509217742
442	28	0.0013823292247436931
442	23	0.00038940809968847351
442	19	0.00077821011673151756
446	20	0.00085052094407824788
446	21	0.00063486305097043355
446	22	0.00091939932577382772
446	28	0.007142034327842414
446	23	0.00038940809968847351
447	20	0.00063789070805868591
447	21	0.00054416832940322869
447	22	0.00091939932577382772
447	28	0.00034558230618592328
447	23	0.00019470404984423675
447	27	0.0096859407103023192
447	26	0.0054234459741343347
455	20	0.00063789070805868591
455	21	0.00054416832940322869
455	28	0.0046077640824789771
457	20	0.00074420582606846695
457	21	0.00063486305097043355
457	22	0.0064357952804167942
457	28	0.00057597051030987213
458	20	0.00021263023601956197
458	21	0.00018138944313440957
458	28	0.00080635871443382099
460	20	0.00042526047203912394
460	21	0.00036277888626881915
460	28	0.00034558230618592328
463	19	0.00077821011673151756
465	28	0.00011519410206197443
465	27	0.0020545934840035221
465	26	0.0016687526074259491
466	28	0.00034558230618592328
466	27	0.002348106838861168
466	26	0.0012515644555694619
467	28	0.00023038820412394885
470	23	0.00058411214953271024
478	20	0.00021263023601956197
478	21	0.00045347360783602395
479	20	0.00010631511800978098
479	21	0.00063486305097043355
480	20	0.00010631511800978098
480	21	0.0024487574823145292
480	25	0.0016460905349794238
480	22	0.00030646644192460924
480	28	0.00011519410206197443
480	27	0.0038156736131493983
480	26	0.00041718815185648727
481	20	0.00010631511800978098
481	21	0.00036277888626881915
481	22	0.00030646644192460924
481	27	0.000293513354857646
481	26	0.00041718815185648727
482	20	0.00010631511800978098
482	21	0.00027208416470161435
482	27	0.000293513354857646
482	26	0.00041718815185648727
483	20	0.00010631511800978098
483	21	9.0694721567204786e-05
484	20	0.00010631511800978098
484	21	9.0694721567204786e-05
485	20	0.00010631511800978098
485	21	0.00045347360783602395
485	27	0.001174053419430584
486	20	0.00010631511800978098
486	21	0.0015418102666424813
486	25	0.00041152263374485596
486	27	0.0020545934840035221
487	20	0.026366149266425686
487	21	0.023036459278070017
487	28	0.00011519410206197443
487	19	0.0038910505836575876
488	20	0.022751435254093132
488	21	0.019408670415381826
489	20	0.0019136721241760578
489	21	0.0010883366588064574
490	20	0.0030831384222836486
490	21	0.0012697261019408671
490	22	0.0045969966288691389
490	28	0.00069116461237184656
491	20	0.0026578779502445249
491	21	0.0019952838744785053
491	25	0.0028806584362139919
491	24	0.0034877927254608871
491	22	0.0070487281642660129
491	27	0.0049897270325799823
491	26	0.008343763037129746
492	20	0.001169466298107591
492	21	0.00063486305097043355
492	22	0.00030646644192460924
492	28	0.00011519410206197443
493	20	0.00021263023601956197
493	21	0.00054416832940322869
493	22	0.0024517315353968739
493	27	0.001174053419430584
493	26	0.00041718815185648727
493	19	0.00077821011673151756
494	20	0.00031894535402934295
494	21	0.0028115363685833486
494	19	0.00077821011673151756
495	20	0.00021263023601956197
495	21	0.0012697261019408671
496	20	0.00010631511800978098
496	21	9.0694721567204786e-05
496	25	0.0053497942386831277
496	24	0.00049825610363726954
497	20	0.00010631511800978098
497	21	0.0024487574823145292
497	25	0.0024691358024691358
497	24	0.00049825610363726954
497	22	0.00030646644192460924
497	28	0.00011519410206197443
497	27	0.0035221602582917524
497	26	0.00041718815185648727
498	20	0.00010631511800978098
498	21	0.00036277888626881915
498	27	0.001174053419430584
499	20	0.00010631511800978098
499	21	0.0020859785960457102
500	20	0.00010631511800978098
500	21	0.00045347360783602395
500	27	0.0017610801291458762
501	20	0.00010631511800978098
501	21	0.0048975149646290584
501	25	0.0037037037037037038
504	20	0.00042526047203912394
504	21	0.00036277888626881915
504	28	0.00023038820412394885
508	21	9.0694721567204786e-05
511	22	0.00061293288384921848
513	23	0.00038940809968847351
515	27	0.000293513354857646
515	26	0.00041718815185648727
532	28	0.00069116461237184656
532	23	0.00019470404984423675
544	20	0.00053157559004890498
544	21	0.00045347360783602395
553	28	0.0023038820412394885
554	20	0.00063789070805868591
554	21	0.00054416832940322869
554	22	0.0027581979773214833
554	28	0.0056445110010367467
554	27	0.021719988259465806
554	26	0.01543596161869003
562	20	0.00042526047203912394
562	21	0.00036277888626881915
562	22	0.001225865767698437
562	28	0.0062204815113466186
565	21	0.0036277888626881916
565	25	0.0049382716049382715
565	22	0.00061293288384921848
565	28	0.00011519410206197443
565	23	0.00019470404984423675
565	27	0.031699442324625772
565	26	0.00041718815185648727
565	19	0.00077821011673151756
567	20	0.00010631511800978098
567	21	9.0694721567204786e-05
568	20	0.00074420582606846695
568	21	0.00054416832940322869
568	22	0.00061293288384921848
568	28	0.0011519410206197443
569	20	0.00074420582606846695
569	25	0.0032921810699588477
569	24	0.0034877927254608871
569	19	0.00077821011673151756
570	20	0.0022326174782054007
570	21	0.0018138944313440958
570	19	0.014007782101167316
571	20	0.00021263023601956197
571	21	9.0694721567204786e-05
572	20	0.0094620455028705079
572	21	0.007890440776346817
572	25	0.0012345679012345679
572	24	0.0014947683109118087
572	22	0.00061293288384921848
572	28	0.00034558230618592328
572	23	0.0011682242990654205
572	19	0.00077821011673151756
573	20	0.00031894535402934295
573	21	0.00018138944313440957
573	25	0.00041152263374485596
573	24	0.00049825610363726954
573	19	0.0023346303501945525
574	20	0.0017010418881564958
574	21	0.0013604208235080718
574	23	0.00077881619937694702
574	19	0.0054474708171206223
575	20	0.0044652349564108015
575	21	0.0037184835842553965
576	20	0.001169466298107591
576	21	0.0011790313803736623
576	24	0.00049825610363726954
576	28	0.0039165994701071301
576	19	0.00077821011673151756
577	20	0.032000850520944077
577	21	0.034191910030836208
577	19	0.0031128404669260703
578	20	0.023814586434190942
578	21	0.020859785960457101
578	22	0.00091939932577382772
579	20	0.00031894535402934295
579	21	0.00018138944313440957
580	20	0.022857750372102914
580	21	0.019590059858516234
580	19	0.0031128404669260703
581	20	0.0048904954284499256
581	21	0.0031743152548521676
582	20	0.0044652349564108015
582	21	0.0031743152548521676
583	20	0.0048904954284499256
583	21	0.0035370941411209866
583	22	0.0015323322096230463
584	20	0.025728258558366999
584	21	0.020678396517322693
584	19	0.0046692607003891049
585	20	0.0045715500744205823
585	21	0.0044440413567930345
586	20	0.0066978524346162022
586	21	0.0054416832940322873
587	20	0.00095683606208802892
587	21	0.00072555777253763829
588	20	0.002338932596215182
588	21	0.0015418102666424813
588	28	0.0027646584494873862
589	20	0.00095683606208802892
589	21	0.00063486305097043355
590	20	0.00053157559004890498
590	21	0.00054416832940322869
591	20	0.00085052094407824788
591	21	0.00045347360783602395
591	19	0.00077821011673151756
592	20	0.005209440782479269
592	21	0.0043533466352258295
592	25	0.00041152263374485596
592	24	0.00049825610363726954
592	22	0.00091939932577382772
592	28	0.00034558230618592328
592	23	0.0011682242990654205
592	27	0.000587026709715292
592	26	0.00083437630371297454
592	19	0.0015564202334630351
593	20	0.00010631511800978098
593	21	9.0694721567204786e-05
593	25	0.00041152263374485596
593	24	0.00049825610363726954
593	23	0.00038940809968847351
593	27	0.000293513354857646
593	26	0.00041718815185648727
593	19	0.00077821011673151756
594	20	0.0020199872421858387
594	21	0.0013604208235080718
594	25	0.0061728395061728392
594	24	0.0069755854509217742
594	22	0.0036775973030953109
594	28	0.0024190761433014627
594	23	0.0056464174454828658
594	27	0.011153507484590548
594	26	0.018356278681685441
594	19	0.017120622568093387
595	20	0.0021263023601956199
595	21	0.0019952838744785053
595	25	0.0061728395061728392
595	24	0.0074738415545590429
595	23	0.00019470404984423675
595	27	0.000293513354857646
595	26	0.00041718815185648727
595	19	0.00077821011673151756
596	20	0.00010631511800978098
596	21	0.00018138944313440957
596	25	0.00041152263374485596
596	24	0.00049825610363726954
596	22	0.00030646644192460924
596	28	0.00023038820412394885
596	23	0.00019470404984423675
596	27	0.000293513354857646
596	26	0.00041718815185648727
597	20	0.00031894535402934295
597	21	0.00027208416470161435
598	20	0.00021263023601956197
598	21	0.00018138944313440957
598	25	0.0016460905349794238
598	24	0.0019930244145490781
598	22	0.00030646644192460924
598	19	0.0054474708171206223
599	20	0.00106315118009781
599	21	0.00099764193723925264
599	23	0.00038940809968847351
600	20	0.00010631511800978098
600	21	9.0694721567204786e-05
600	22	0.00061293288384921848
600	23	0.0009735202492211838
600	27	0.000293513354857646
600	26	0.00083437630371297454
601	20	0.00074420582606846695
601	21	0.00063486305097043355
601	25	0.00041152263374485596
601	24	0.00049825610363726954
601	23	0.00019470404984423675
602	20	0.00053157559004890498
602	21	0.00045347360783602395
602	25	0.00041152263374485596
602	24	0.00049825610363726954
602	22	0.00091939932577382772
602	23	0.00019470404984423675
602	27	0.000293513354857646
602	26	0.00041718815185648727
602	19	0.0046692607003891049
603	20	0.00031894535402934295
603	21	0.00027208416470161435
604	20	0.00010631511800978098
604	21	9.0694721567204786e-05
604	23	0.0062305295950155761
604	27	0.0014675667742882301
604	26	0.0020859407592824365
605	20	0.00010631511800978098
605	21	9.0694721567204786e-05
606	20	0.00010631511800978098
606	21	9.0694721567204786e-05
606	25	0.00041152263374485596
606	24	0.00049825610363726954
606	22	0.00061293288384921848
606	28	0.00046077640824789771
606	23	0.00058411214953271024
607	20	0.00010631511800978098
607	21	9.0694721567204786e-05
607	22	0.003371130861170702
608	20	0.00010631511800978098
608	21	9.0694721567204786e-05
609	20	0.00010631511800978098
609	21	9.0694721567204786e-05
609	25	0.0028806584362139919
609	24	0.0034877927254608871
610	20	0.00021263023601956197
610	21	0.00018138944313440957
610	25	0.0090534979423868307
610	24	0.0114598903836572
610	28	0.00011519410206197443
610	27	0.000293513354857646
610	26	0.00041718815185648727
610	19	0.00077821011673151756
611	20	0.0048904954284499256
611	21	0.0042626519136586254
611	25	0.00041152263374485596
611	24	0.00049825610363726954
611	22	0.00030646644192460924
611	28	0.0011519410206197443
611	23	0.0017523364485981308
612	20	0.0095683606208802896
612	21	0.008162524941048431
612	19	0.0031128404669260703
613	20	0.004146289602381459
613	21	0.0035370941411209866
614	20	0.00031894535402934295
614	21	0.00027208416470161435
614	22	0.00030646644192460924
614	19	0.0023346303501945525
615	20	0.00010631511800978098
615	21	9.0694721567204786e-05
616	20	0.0020199872421858387
616	21	0.001723199709776891
616	28	0.00057597051030987213
616	23	0.0009735202492211838
616	19	0.0015564202334630351
617	20	0.00106315118009781
617	21	0.00090694721567204789
617	25	0.00041152263374485596
617	24	0.00049825610363726954
617	22	0.00061293288384921848
617	23	0.00019470404984423675
617	27	0.0026416201937188143
617	26	0.0033375052148518982
618	20	0.00031894535402934295
618	21	0.00027208416470161435
619	20	0.0057410163725281739
619	21	0.0048975149646290584
620	20	0.00010631511800978098
620	21	9.0694721567204786e-05
621	20	0.0026578779502445249
621	21	0.0056230727371666973
622	20	0.00031894535402934295
622	21	0.00027208416470161435
622	28	0.00046077640824789771
623	20	0.00042526047203912394
623	21	0.00045347360783602395
623	25	0.013168724279835391
623	24	0.015944195316392625
624	20	0.010418881564958538
624	21	0.008888082713586069
625	20	0.00031894535402934295
625	21	0.00027208416470161435
626	20	0.0025515628322347436
626	21	0.0021766733176129148
627	20	0.00106315118009781
627	21	0.00090694721567204789
628	20	0.00021263023601956197
628	21	0.00018138944313440957
629	20	0.0019136721241760578
629	21	0.0016325049882096863
629	25	0.00082304526748971192
629	24	0.00049825610363726954
629	22	0.00030646644192460924
629	23	0.0009735202492211838
629	19	0.0031128404669260703
630	20	0.00042526047203912394
630	21	0.00036277888626881915
630	23	0.00019470404984423675
630	19	0.00077821011673151756
631	20	0.00021263023601956197
631	21	0.00018138944313440957
632	20	0.00010631511800978098
632	21	9.0694721567204786e-05
632	25	0.0032921810699588477
632	24	0.0039860488290981563
633	20	0.0015947267701467149
633	21	0.0013604208235080718
634	20	0.00042526047203912394
634	21	0.00036277888626881915
634	22	0.00061293288384921848
634	23	0.00019470404984423675
635	20	0.00010631511800978098
635	21	9.0694721567204786e-05
635	25	0.00041152263374485596
635	24	0.00049825610363726954
635	23	0.00019470404984423675
635	19	0.0015564202334630351
636	20	0.0026578779502445249
636	21	0.0022673680391801197
636	25	0.0016460905349794238
636	24	0.0019930244145490781
636	22	0.00061293288384921848
636	28	0.00011519410206197443
636	23	0.0009735202492211838
636	27	0.00088054006457293811
636	26	0.0012515644555694619
636	19	0.0023346303501945525
637	20	0.00021263023601956197
637	21	0.00018138944313440957
638	20	0.00021263023601956197
638	21	0.00018138944313440957
639	20	0.00031894535402934295
639	21	0.00027208416470161435
639	25	0.00041152263374485596
639	24	0.00049825610363726954
639	22	0.00061293288384921848
639	23	0.00019470404984423675
640	20	0.00010631511800978098
640	21	9.0694721567204786e-05
641	20	0.00085052094407824788
641	21	0.00063486305097043355
641	19	0.031906614785992216
642	20	0.0097809908568998512
642	21	0.0097043352076909128
642	28	0.00080635871443382099
642	19	0.0023346303501945525
643	20	0.0020199872421858387
643	21	0.001723199709776891
643	25	0.00041152263374485596
643	24	0.00049825610363726954
643	22	0.001225865767698437
643	28	0.00057597051030987213
643	23	0.00038940809968847351
643	27	0.0014675667742882301
643	26	0.00041718815185648727
643	19	0.0015564202334630351
644	20	0.00010631511800978098
644	21	9.0694721567204786e-05
645	20	0.00010631511800978098
645	21	9.0694721567204786e-05
646	20	0.00010631511800978098
646	21	9.0694721567204786e-05
646	28	0.00034558230618592328
646	27	0.0017610801291458762
646	26	0.00083437630371297454
646	19	0.024902723735408562
647	20	0.00010631511800978098
647	21	9.0694721567204786e-05
647	28	0.010482663287639672
648	20	0.00042526047203912394
648	21	0.00036277888626881915
648	28	0.0041469876742310794
648	27	0.00088054006457293811
648	26	0.00041718815185648727
649	20	0.00010631511800978098
649	21	9.0694721567204786e-05
649	25	0.0016460905349794238
649	24	0.0019930244145490781
649	22	0.003371130861170702
649	28	0.016472756594862342
649	23	0.028037383177570093
649	27	0.0049897270325799823
649	26	0.0079265748852732579
649	19	0.0015564202334630351
650	20	0.00106315118009781
650	21	0.0025394522038817342
650	25	0.0061728395061728392
650	24	0.0074738415545590429
651	20	0.00031894535402934295
651	21	0.00027208416470161435
652	20	0.00085052094407824788
652	21	0.00072555777253763829
652	22	0.0021452650934722646
652	28	0.0047229581845409513
652	23	0.0081775700934579431
652	27	0.0029351335485764602
652	26	0.0045890696704213602
653	20	0.00010631511800978098
653	21	9.0694721567204786e-05
653	28	0.00080635871443382099
653	23	0.00038940809968847351
653	27	0.004696213677722336
653	26	0.017521902377972465
654	20	0.00031894535402934295
654	21	0.00027208416470161435
655	20	0.00021263023601956197
655	21	0.00018138944313440957
655	22	0.0015323322096230463
656	20	0.00010631511800978098
656	21	9.0694721567204786e-05
656	23	0.00038940809968847351
656	27	0.0029351335485764602
656	26	0.004171881518564873
657	20	0.00010631511800978098
657	21	9.0694721567204786e-05
658	20	0.00021263023601956197
658	21	0.00018138944313440957
659	20	0.00021263023601956197
659	21	0.00018138944313440957
659	22	0.00061293288384921848
659	19	0.0015564202334630351
660	20	0.00021263023601956197
660	21	0.00018138944313440957
661	20	0.00021263023601956197
661	21	0.00018138944313440957
662	20	0.00010631511800978098
662	21	9.0694721567204786e-05
663	20	0.00010631511800978098
663	21	9.0694721567204786e-05
664	20	0.00021263023601956197
664	21	0.00018138944313440957
664	27	0.000293513354857646
664	26	0.00041718815185648727
665	20	0.00095683606208802892
665	21	0.00099764193723925264
666	20	0.00021263023601956197
666	21	0.00018138944313440957
667	20	0.00063789070805868591
667	21	0.00054416832940322869
667	22	0.00061293288384921848
667	28	0.01198018661444534
667	23	0.00038940809968847351
667	27	0.000293513354857646
667	26	0.00041718815185648727
667	19	0.00077821011673151756
668	20	0.00063789070805868591
668	21	0.00054416832940322869
668	25	0.00041152263374485596
668	24	0.00049825610363726954
668	22	0.00030646644192460924
668	23	0.0013629283489096573
668	19	0.00077821011673151756
669	20	0.00031894535402934295
669	21	0.00027208416470161435
669	19	0.0023346303501945525
670	20	0.00010631511800978098
670	21	9.0694721567204786e-05
671	20	0.00042526047203912394
671	21	0.00036277888626881915
672	20	0.00010631511800978098
672	21	9.0694721567204786e-05
672	23	0.0009735202492211838
673	20	0.00021263023601956197
673	21	0.00018138944313440957
673	25	0.0016460905349794238
673	24	0.0019930244145490781
673	28	0.00011519410206197443
673	23	0.00019470404984423675
673	19	0.0015564202334630351
674	20	0.00010631511800978098
674	21	9.0694721567204786e-05
675	20	0.00042526047203912394
675	21	0.00036277888626881915
675	22	0.0018387986515476554
676	20	0.0012757814161173718
676	21	0.00090694721567204789
676	25	0.00082304526748971192
676	24	0.00099651220727453907
676	22	0.00030646644192460924
676	28	0.00034558230618592328
676	23	0.00019470404984423675
676	27	0.0014675667742882301
676	26	0.0020859407592824365
676	19	0.0023346303501945525
677	20	0.0020199872421858387
677	21	0.0016325049882096863
677	25	0.0032921810699588477
677	24	0.0039860488290981563
677	22	0.00061293288384921848
677	23	0.00077881619937694702
678	20	0.00021263023601956197
678	21	0.00018138944313440957
678	28	0.00011519410206197443
678	23	0.00019470404984423675
679	20	0.00010631511800978098
679	21	9.0694721567204786e-05
679	25	0.0024691358024691358
679	24	0.0029895366218236174
679	23	0.0017523364485981308
679	27	0.000293513354857646
679	26	0.00041718815185648727
680	20	0.00053157559004890498
680	21	0.00045347360783602395
681	20	0.00042526047203912394
681	21	0.00036277888626881915
681	22	0.00061293288384921848
682	20	0.00010631511800978098
682	21	9.0694721567204786e-05
682	25	0.00041152263374485596
682	24	0.00049825610363726954
683	20	0.00010631511800978098
683	21	9.0694721567204786e-05
684	20	0.00053157559004890498
684	21	0.00045347360783602395
684	25	0.0012345679012345679
684	24	0.0014947683109118087
684	22	0.0021452650934722646
684	28	0.00057597051030987213
684	23	0.00077881619937694702
684	27	0.0038156736131493983
684	26	0.0050062578222778474
685	20	0.00010631511800978098
685	21	9.0694721567204786e-05
685	25	0.00041152263374485596
685	24	0.00049825610363726954
685	23	0.00077881619937694702
686	20	0.00010631511800978098
686	21	9.0694721567204786e-05
686	23	0.00077881619937694702
687	20	0.00010631511800978098
687	21	9.0694721567204786e-05
687	25	0.00041152263374485596
687	24	0.00049825610363726954
688	20	0.00010631511800978098
688	21	9.0694721567204786e-05
689	20	0.00106315118009781
689	21	0.00090694721567204789
690	20	0.00021263023601956197
690	21	0.00018138944313440957
691	20	0.00053157559004890498
691	21	0.00045347360783602395
691	28	0.00011519410206197443
692	20	0.00010631511800978098
692	21	9.0694721567204786e-05
693	20	0.00010631511800978098
693	21	9.0694721567204786e-05
693	27	0.000293513354857646
693	26	0.00041718815185648727
694	20	0.00031894535402934295
694	21	0.00027208416470161435
694	19	0.00077821011673151756
695	20	0.00042526047203912394
695	21	0.00036277888626881915
696	20	0.0017010418881564958
696	21	0.0014511155450752766
697	20	0.00010631511800978098
697	21	9.0694721567204786e-05
698	20	0.00010631511800978098
698	21	9.0694721567204786e-05
698	22	0.00030646644192460924
698	19	0.00077821011673151756
699	20	0.00010631511800978098
699	21	9.0694721567204786e-05
700	20	0.013502019987242186
700	21	0.011518229639035009
700	22	0.00061293288384921848
700	28	0.00011519410206197443
701	20	0.00010631511800978098
701	21	9.0694721567204786e-05
702	20	0.00010631511800978098
702	21	9.0694721567204786e-05
703	20	0.00010631511800978098
703	21	9.0694721567204786e-05
704	20	0.00010631511800978098
704	21	9.0694721567204786e-05
705	20	0.00010631511800978098
705	21	9.0694721567204786e-05
705	22	0.00030646644192460924
705	27	0.000587026709715292
705	26	0.00083437630371297454
706	20	0.0013820965341271529
706	21	0.0011790313803736623
707	20	0.00010631511800978098
707	21	9.0694721567204786e-05
708	20	0.00010631511800978098
708	21	9.0694721567204786e-05
709	20	0.00031894535402934295
709	21	0.00027208416470161435
709	25	0.0016460905349794238
709	24	0.0019930244145490781
709	22	0.0018387986515476554
709	28	0.00011519410206197443
709	23	0.00019470404984423675
710	20	0.00010631511800978098
710	21	9.0694721567204786e-05
711	20	0.00031894535402934295
711	21	0.00027208416470161435
711	25	0.045267489711934158
711	24	0.056302939711011461
711	22	0.003371130861170702
711	28	0.026609837576316093
711	19	0.0038910505836575876
712	20	0.00021263023601956197
712	21	0.00018138944313440957
712	28	0.0017279115309296164
712	23	0.00019470404984423675
713	20	0.00021263023601956197
713	21	0.00018138944313440957
713	25	0.00041152263374485596
713	24	0.00049825610363726954
713	19	0.00077821011673151756
714	20	0.00053157559004890498
714	21	0.00045347360783602395
714	22	0.001225865767698437
714	23	0.00038940809968847351
715	20	0.00021263023601956197
715	21	0.00018138944313440957
716	20	0.021263023601956199
716	21	0.018138944313440958
716	25	0.0090534979423868307
716	24	0.01096163428001993
716	22	0.0024517315353968739
716	28	0.011749798410321392
716	23	0.011292834890965732
716	27	0.019078368065746993
716	26	0.027117229870671673
717	20	0.0026578779502445249
717	21	0.0022673680391801197
718	20	0.0072294280246651072
718	21	0.0061672410665699253
718	25	0.00082304526748971192
718	24	0.00099651220727453907
718	23	0.00038940809968847351
719	20	0.0024452477142249628
719	21	0.0020859785960457102
719	22	0.00091939932577382772
719	23	0.00077881619937694702
720	20	0.00095683606208802892
720	21	0.00081625249410484314
720	27	0.000293513354857646
720	26	0.00041718815185648727
721	20	0.0021263023601956199
721	21	0.0018138944313440958
721	23	0.00019470404984423675
722	20	0.0019136721241760578
722	21	0.0016325049882096863
722	23	0.001557632398753894
723	20	0.00053157559004890498
723	21	0.00045347360783602395
724	20	0.00095683606208802892
724	21	0.00081625249410484314
724	19	0.0023346303501945525
725	20	0.00031894535402934295
725	21	0.00027208416470161435
726	20	0.00042526047203912394
726	21	0.00036277888626881915
727	20	0.00021263023601956197
727	21	0.00018138944313440957
728	20	0.001169466298107591
728	21	0.0011790313803736623
728	19	0.040466926070038912
729	20	0.00053157559004890498
729	21	0.00045347360783602395
729	22	0.017468587189702726
730	20	0.0024452477142249628
730	21	0.0019952838744785053
730	25	0.00205761316872428
730	24	0.0024912805181863478
730	22	0.0018387986515476554
730	28	0.017509503513420111
730	23	0.00019470404984423675
731	20	0.0018073570061662768
731	21	0.0015418102666424813
731	25	0.00041152263374485596
731	24	0.00049825610363726954
731	22	0.0070487281642660129
731	28	0.001612717428867642
731	23	0.00077881619937694702
731	27	0.0041091869680070442
731	26	0.0058406341259908219
732	20	0.006804167552625983
732	21	0.0058044621803011063
733	20	0.0058473314905379548
733	21	0.0049882096861962634
733	25	0.00205761316872428
733	24	0.0024912805181863478
733	23	0.00019470404984423675
734	20	0.00042526047203912394
734	21	0.00036277888626881915
735	20	0.00074420582606846695
735	21	0.00063486305097043355
735	28	0.00023038820412394885
736	20	0.00063789070805868591
736	21	0.00054416832940322869
736	25	0.00411522633744856
736	24	0.0049825610363726956
736	28	0.00011519410206197443
737	20	0.0025515628322347436
737	21	0.0021766733176129148
737	25	0.0090534979423868307
737	24	0.01096163428001993
737	22	0.0018387986515476554
737	27	0.0014675667742882301
737	26	0.0020859407592824365
738	20	0.001169466298107591
738	21	0.00099764193723925264
738	25	0.009876543209876543
738	24	0.01195814648729447
738	22	0.00061293288384921848
738	28	0.0069116461237184656
738	27	0.001174053419430584
738	26	0.0016687526074259491
739	20	0.00053157559004890498
739	21	0.00045347360783602395
739	22	0.00091939932577382772
739	28	0.001612717428867642
739	27	0.004696213677722336
739	26	0.0066750104297037963
740	20	0.002870508186264087
740	21	0.0024487574823145292
741	20	0.0012757814161173718
741	21	0.0010883366588064574
742	20	0.0042526047203912398
742	21	0.0036277888626881916
743	20	0.00053157559004890498
743	21	0.00045347360783602395
743	22	0.0049034630707937479
743	28	0.0014975233268056676
743	27	0.0049897270325799823
743	26	0.0070921985815602835
744	20	0.00010631511800978098
744	21	9.0694721567204786e-05
745	20	0.0054220710184988306
745	21	0.0046254307999274444
745	25	0.0045267489711934153
745	24	0.0054808171400099652
745	22	0.00061293288384921848
745	28	0.010943439695887571
745	23	0.0019470404984423676
745	27	0.001174053419430584
745	26	0.0016687526074259491
745	19	0.0015564202334630351
746	20	0.00053157559004890498
746	21	0.00045347360783602395
747	20	0.00010631511800978098
747	21	9.0694721567204786e-05
748	20	0.00010631511800978098
748	21	9.0694721567204786e-05
748	25	0.0016460905349794238
748	24	0.0019930244145490781
749	20	0.00053157559004890498
749	21	0.00045347360783602395
750	20	0.00010631511800978098
750	21	9.0694721567204786e-05
751	20	0.00053157559004890498
751	21	0.00045347360783602395
751	23	0.0025311526479750777
751	27	0.000293513354857646
751	26	0.00041718815185648727
752	20	0.00053157559004890498
752	21	0.00045347360783602395
753	20	0.00010631511800978098
753	21	9.0694721567204786e-05
753	22	0.00030646644192460924
753	23	0.00038940809968847351
754	20	0.00031894535402934295
754	21	0.00027208416470161435
755	20	0.00042526047203912394
755	21	0.00036277888626881915
755	25	0.00082304526748971192
755	24	0.00099651220727453907
755	27	0.0017610801291458762
755	26	0.0045890696704213602
755	19	0.0023346303501945525
756	20	0.00031894535402934295
756	21	0.00027208416470161435
757	20	0.0020199872421858387
757	21	0.00090694721567204789
757	25	0.0074074074074074077
757	24	0.0084703537618335822
757	28	0.0033406289597972582
757	23	0.0095404984423676006
757	27	0.0064572938068682122
757	26	0.010012515644555695
757	19	0.016342412451361869
758	20	0.00021263023601956197
758	21	0.00018138944313440957
759	20	0.0027641930682543057
759	21	0.0023580627607473247
759	19	0.00077821011673151756
760	20	0.00021263023601956197
760	21	0.00018138944313440957
760	24	0.00049825610363726954
760	22	0.00030646644192460924
760	28	0.0012671351226817187
760	23	0.00058411214953271024
760	27	0.001174053419430584
760	26	0.0016687526074259491
760	19	0.0023346303501945525
761	20	0.00042526047203912394
761	21	0.00036277888626881915
762	20	0.00021263023601956197
762	21	0.00018138944313440957
763	20	0.001169466298107591
763	21	0.00099764193723925264
763	25	0.0016460905349794238
763	24	0.0019930244145490781
763	27	0.000293513354857646
763	26	0.00041718815185648727
763	19	0.00077821011673151756
764	20	0.00031894535402934295
764	21	0.00027208416470161435
764	22	0.00091939932577382772
764	23	0.00058411214953271024
764	27	0.000293513354857646
764	26	0.00041718815185648727
765	20	0.0014884116521369339
765	21	0.0012697261019408671
765	25	0.0069958847736625515
765	24	0.0084703537618335822
765	22	0.00030646644192460924
765	28	0.0010367469185577698
765	23	0.00058411214953271024
765	19	0.0015564202334630351
766	20	0.00010631511800978098
766	21	9.0694721567204786e-05
767	20	0.00010631511800978098
767	21	9.0694721567204786e-05
768	20	0.00010631511800978098
768	21	9.0694721567204786e-05
768	23	0.00077881619937694702
769	20	0.00010631511800978098
769	21	9.0694721567204786e-05
770	20	0.0021263023601956199
770	21	0.0024487574823145292
770	25	0.0012345679012345679
770	24	0.0014947683109118087
770	22	0.001225865767698437
770	28	0.0046077640824789771
770	23	0.0011682242990654205
770	27	0.0026416201937188143
770	26	0.0033375052148518982
770	19	0.0077821011673151752
771	20	0.00021263023601956197
771	21	0.00018138944313440957
771	22	0.00091939932577382772
771	28	0.00011519410206197443
771	27	0.0058702670971529205
771	26	0.0033375052148518982
772	20	0.00031894535402934295
772	21	0.00027208416470161435
773	20	0.0081862640867531363
773	21	0.0069834935606747691
773	22	0.00030646644192460924
774	20	0.00031894535402934295
774	21	0.00027208416470161435
774	25	0.00041152263374485596
774	24	0.00049825610363726954
775	20	0.00021263023601956197
775	21	0.00018138944313440957
775	23	0.00019470404984423675
776	20	0.00085052094407824788
776	21	0.00072555777253763829
776	22	0.00030646644192460924
776	28	0.00011519410206197443
777	20	0.0015947267701467149
777	21	0.0013604208235080718
CREATE TABLE "idf_bm25" (
	"termid" int,
	"prob" float(53,1)
);
COPY 3424 RECORDS INTO "idf_bm25" FROM stdin USING DELIMITERS '\t';
430	1.0986122886681098
432	1.0986122886681098
434	0.64662716492505246
437	1.4350845252893227
441	0.33647223662121289
442	0.33647223662121289
446	0.64662716492505246
447	0.33647223662121289
455	1.0986122886681098
457	0.84729786038720367
458	1.0986122886681098
460	1.0986122886681098
463	1.9459101490553132
465	1.0986122886681098
466	1.0986122886681098
467	1.9459101490553132
470	1.9459101490553132
478	1.4350845252893227
479	1.4350845252893227
480	0.33647223662121289
481	0.64662716492505246
482	0.84729786038720367
483	1.4350845252893227
484	1.4350845252893227
485	1.0986122886681098
486	0.84729786038720367
487	0.84729786038720367
488	1.4350845252893227
489	1.4350845252893227
490	0.84729786038720367
491	0.33647223662121289
492	0.84729786038720367
493	0.47957308026188628
494	1.0986122886681098
495	1.4350845252893227
496	0.84729786038720367
497	0.21130909366720696
498	1.0986122886681098
499	1.4350845252893227
500	1.0986122886681098
501	1.0986122886681098
504	1.0986122886681098
508	1.9459101490553132
511	1.9459101490553132
513	1.9459101490553132
515	1.4350845252893227
532	1.4350845252893227
544	1.4350845252893227
553	1.9459101490553132
554	0.47957308026188628
562	0.84729786038720367
565	0.21130909366720696
567	1.4350845252893227
568	0.84729786038720367
569	0.84729786038720367
570	1.0986122886681098
571	1.4350845252893227
572	0.21130909366720696
573	0.64662716492505246
574	0.84729786038720367
575	1.4350845252893227
576	0.64662716492505246
577	1.0986122886681098
578	1.0986122886681098
579	1.4350845252893227
580	1.0986122886681098
581	1.4350845252893227
582	1.4350845252893227
583	1.0986122886681098
584	1.0986122886681098
585	1.4350845252893227
586	1.4350845252893227
587	1.4350845252893227
588	1.0986122886681098
589	1.4350845252893227
590	1.4350845252893227
591	1.0986122886681098
592	0.2
593	0.21130909366720696
594	0
595	0.21130909366720696
596	0.10008345855698263
597	1.4350845252893227
598	0.47957308026188628
599	1.0986122886681098
600	0.47957308026188628
601	0.64662716492505246
602	0.10008345855698263
603	1.4350845252893227
604	0.64662716492505246
605	1.4350845252893227
606	0.33647223662121289
607	1.0986122886681098
608	1.4350845252893227
609	0.84729786038720367
610	0.21130909366720696
611	0.33647223662121289
612	1.0986122886681098
613	1.4350845252893227
614	0.84729786038720367
615	1.4350845252893227
616	0.64662716492505246
617	0.21130909366720696
618	1.4350845252893227
619	1.4350845252893227
620	1.4350845252893227
621	1.4350845252893227
622	1.0986122886681098
623	0.84729786038720367
624	1.4350845252893227
625	1.4350845252893227
626	1.4350845252893227
627	1.4350845252893227
628	1.4350845252893227
629	0.33647223662121289
630	0.84729786038720367
631	1.4350845252893227
632	0.84729786038720367
633	1.4350845252893227
634	0.84729786038720367
635	0.47957308026188628
636	0.1
637	1.4350845252893227
638	1.4350845252893227
639	0.47957308026188628
640	1.4350845252893227
641	1.0986122886681098
642	0.84729786038720367
643	0
644	1.4350845252893227
645	1.4350845252893227
646	0.47957308026188628
647	1.0986122886681098
648	0.64662716492505246
649	0
650	0.84729786038720367
651	1.4350845252893227
652	0.33647223662121289
653	0.47957308026188628
654	1.4350845252893227
655	1.0986122886681098
656	0.64662716492505246
657	1.4350845252893227
658	1.4350845252893227
659	0.84729786038720367
660	1.4350845252893227
661	1.4350845252893227
662	1.4350845252893227
663	1.4350845252893227
664	0.84729786038720367
665	1.4350845252893227
666	1.4350845252893227
667	0.21130909366720696
668	0.33647223662121289
669	1.0986122886681098
670	1.4350845252893227
671	1.4350845252893227
672	1.0986122886681098
673	0.33647223662121289
674	1.4350845252893227
675	1.0986122886681098
676	0
677	0.47957308026188628
678	0.84729786038720367
679	0.33647223662121289
680	1.4350845252893227
681	1.0986122886681098
682	0.84729786038720367
683	1.4350845252893227
684	0.10008345855698263
685	0.64662716492505246
686	1.0986122886681098
687	0.84729786038720367
688	1.4350845252893227
689	1.4350845252893227
690	1.4350845252893227
691	1.0986122886681098
692	1.4350845252893227
693	0.84729786038720367
694	1.0986122886681098
695	1.4350845252893227
696	1.4350845252893227
697	1.4350845252893227
698	0.84729786038720367
699	1.4350845252893227
700	0.84729786038720367
701	1.4350845252893227
702	1.4350845252893227
703	1.4350845252893227
704	1.4350845252893227
705	0.64662716492505246
706	1.4350845252893227
707	1.4350845252893227
708	1.4350845252893227
709	0.33647223662121289
710	1.4350845252893227
711	0.33647223662121289
712	0.84729786038720367
713	0.64662716492505246
714	0.84729786038720367
715	1.4350845252893227
716	0.10008345855698263
717	1.4350845252893227
718	0.64662716492505246
719	0.84729786038720367
720	0.84729786038720367
721	1.0986122886681098
722	1.0986122886681098
723	1.4350845252893227
724	1.0986122886681098
725	1.4350845252893227
726	1.4350845252893227
727	1.4350845252893227
728	1.0986122886681098
729	1.0986122886681098
730	0.33647223662121289
731	0.10008345855698263
732	1.4350845252893227
733	0.64662716492505246
734	1.4350845252893227
735	1.0986122886681098
736	0.64662716492505246
737	0.33647223662121289
738	0.21130909366720696
739	0.47957308026188628
740	1.4350845252893227
741	1.4350845252893227
742	1.4350845252893227
743	0.47957308026188628
744	1.4350845252893227
745	0
746	1.4350845252893227
747	1.4350845252893227
748	0.84729786038720367
749	1.4350845252893227
750	1.4350845252893227
751	0.64662716492505246
752	1.4350845252893227
753	0.84729786038720367
754	1.4350845252893227
755	0.33647223662121289
756	1.4350845252893227
757	0.10008345855698263
758	1.4350845252893227
759	1.0986122886681098
760	0.10008345855698263
761	1.4350845252893227
762	1.4350845252893227
763	0.33647223662121289
764	0.47957308026188628
765	0.21130909366720696
766	1.4350845252893227
767	1.4350845252893227
768	1.0986122886681098
769	1.4350845252893227
770	0
771	0.47957308026188628
772	1.4350845252893227
773	1.0986122886681098
774	0.84729786038720367
775	1.0986122886681098
776	0.84729786038720367
777	1.0986122886681098
778	1.4350845252893227
779	1.4350845252893227
780	1.4350845252893227
781	1.4350845252893227
782	0.47957308026188628
783	1.0986122886681098
784	0.64662716492505246
785	1.4350845252893227
786	0.33647223662121289
787	1.4350845252893227
788	1.4350845252893227
789	1.4350845252893227
790	1.4350845252893227
791	1.0986122886681098
792	1.4350845252893227
793	1.0986122886681098
794	0.84729786038720367
795	1.4350845252893227
796	0.64662716492505246
797	1.4350845252893227
798	1.4350845252893227
799	0.21130909366720696
800	1.4350845252893227
801	0.84729786038720367
802	0.84729786038720367
803	1.4350845252893227
804	1.4350845252893227
805	1.4350845252893227
806	0.10008345855698263
807	0
808	1.4350845252893227
809	1.0986122886681098
810	0.21130909366720696
811	1.4350845252893227
812	1.4350845252893227
813	1.4350845252893227
814	1.4350845252893227
815	1.4350845252893227
816	1.0986122886681098
817	0.84729786038720367
818	1.4350845252893227
819	1.4350845252893227
820	1.4350845252893227
821	0.64662716492505246
822	1.4350845252893227
823	1.4350845252893227
824	1.4350845252893227
825	1.4350845252893227
826	0
827	0.33647223662121289
828	1.0986122886681098
829	1.4350845252893227
830	1.4350845252893227
831	0.64662716492505246
832	1.4350845252893227
833	0.64662716492505246
834	0.64662716492505246
835	1.4350845252893227
836	1.0986122886681098
837	1.4350845252893227
838	1.4350845252893227
839	1.4350845252893227
840	1.4350845252893227
841	1.4350845252893227
842	1.4350845252893227
843	0.47957308026188628
844	1.4350845252893227
845	1.0986122886681098
846	1.4350845252893227
847	0.64662716492505246
848	1.4350845252893227
849	1.0986122886681098
850	1.4350845252893227
851	1.0986122886681098
852	0.10008345855698263
853	1.4350845252893227
854	1.4350845252893227
855	0.84729786038720367
856	1.4350845252893227
857	1.4350845252893227
858	0.84729786038720367
859	0.33647223662121289
860	1.4350845252893227
861	0.64662716492505246
862	1.4350845252893227
863	0.47957308026188628
864	1.0986122886681098
865	1.4350845252893227
866	1.4350845252893227
867	0.21130909366720696
868	0.47957308026188628
869	0.47957308026188628
870	0.64662716492505246
871	1.0986122886681098
872	1.4350845252893227
873	1.4350845252893227
874	1.4350845252893227
875	0.64662716492505246
876	1.4350845252893227
877	1.4350845252893227
878	0.84729786038720367
879	1.0986122886681098
880	1.0986122886681098
881	1.0986122886681098
882	1.0986122886681098
883	1.0986122886681098
884	1.4350845252893227
885	1.4350845252893227
886	1.4350845252893227
887	0.84729786038720367
888	1.4350845252893227
889	1.4350845252893227
890	0.64662716492505246
891	1.4350845252893227
892	0.84729786038720367
893	0.84729786038720367
894	0.64662716492505246
895	1.4350845252893227
896	1.4350845252893227
897	1.4350845252893227
898	1.4350845252893227
899	1.4350845252893227
900	1.4350845252893227
901	0.47957308026188628
902	1.0986122886681098
903	1.4350845252893227
904	1.4350845252893227
905	0.33647223662121289
906	1.4350845252893227
907	1.0986122886681098
908	0.84729786038720367
909	1.4350845252893227
910	1.4350845252893227
911	0.33647223662121289
912	1.0986122886681098
913	0.33647223662121289
914	1.4350845252893227
915	0.84729786038720367
916	1.0986122886681098
917	0.64662716492505246
918	1.0986122886681098
919	0.33647223662121289
920	1.0986122886681098
921	0.84729786038720367
922	0.64662716492505246
923	1.4350845252893227
924	1.4350845252893227
925	1.0986122886681098
926	1.4350845252893227
927	0.64662716492505246
928	1.4350845252893227
929	1.0986122886681098
930	1.4350845252893227
931	0.21130909366720696
932	1.4350845252893227
933	1.4350845252893227
934	1.0986122886681098
935	1.4350845252893227
936	0.47957308026188628
937	1.4350845252893227
938	1.4350845252893227
939	1.4350845252893227
940	1.4350845252893227
941	0.84729786038720367
942	1.4350845252893227
943	1.0986122886681098
944	0.64662716492505246
945	0.84729786038720367
946	1.4350845252893227
947	1.4350845252893227
948	0.64662716492505246
949	1.0986122886681098
950	0.33647223662121289
951	0.84729786038720367
952	0.84729786038720367
953	0.64662716492505246
954	1.4350845252893227
955	1.4350845252893227
956	1.0986122886681098
957	1.4350845252893227
958	1.4350845252893227
959	1.4350845252893227
960	1.4350845252893227
961	1.4350845252893227
962	1.4350845252893227
963	0.21130909366720696
964	0.84729786038720367
965	1.4350845252893227
966	1.4350845252893227
967	1.0986122886681098
968	0.33647223662121289
969	1.0986122886681098
970	1.4350845252893227
971	1.4350845252893227
972	0.84729786038720367
973	1.4350845252893227
974	1.4350845252893227
975	1.0986122886681098
976	0.84729786038720367
977	1.4350845252893227
978	0.64662716492505246
979	1.4350845252893227
980	1.0986122886681098
981	1.4350845252893227
982	0.84729786038720367
983	1.0986122886681098
984	0.64662716492505246
985	1.4350845252893227
986	0.84729786038720367
987	1.4350845252893227
988	0.84729786038720367
989	1.4350845252893227
990	1.4350845252893227
991	0.33647223662121289
992	1.4350845252893227
993	1.0986122886681098
994	1.4350845252893227
995	0.84729786038720367
996	0.84729786038720367
997	0.47957308026188628
998	1.4350845252893227
999	0.84729786038720367
1000	1.0986122886681098
1001	1.0986122886681098
1002	1.4350845252893227
1003	1.0986122886681098
1004	0.84729786038720367
1005	0.47957308026188628
1006	1.4350845252893227
1007	0.64662716492505246
1008	0.84729786038720367
1009	0.84729786038720367
1010	1.4350845252893227
1011	0.84729786038720367
1012	1.4350845252893227
1013	0.84729786038720367
1014	1.4350845252893227
1015	1.4350845252893227
1016	0.84729786038720367
1017	0.33647223662121289
1018	1.0986122886681098
1019	1.4350845252893227
1020	1.4350845252893227
1021	1.4350845252893227
1022	1.4350845252893227
1023	1.4350845252893227
1024	1.0986122886681098
1025	1.4350845252893227
1026	1.4350845252893227
1027	1.4350845252893227
1028	1.4350845252893227
1029	1.4350845252893227
1030	1.4350845252893227
1031	1.0986122886681098
1032	1.4350845252893227
1033	1.4350845252893227
1034	0.64662716492505246
1035	1.0986122886681098
1036	0
1037	1.4350845252893227
1038	1.4350845252893227
1039	1.0986122886681098
1040	1.0986122886681098
1041	1.4350845252893227
1042	0.33647223662121289
1043	1.0986122886681098
1044	1.4350845252893227
1045	0.10008345855698263
1046	0.33647223662121289
1047	1.9459101490553132
1048	1.0986122886681098
1049	1.4350845252893227
1050	1.9459101490553132
1051	1.4350845252893227
1052	0.84729786038720367
1053	1.4350845252893227
1054	1.4350845252893227
1055	1.4350845252893227
1056	0.84729786038720367
1057	1.4350845252893227
1058	1.0986122886681098
1059	1.0986122886681098
1060	0.64662716492505246
1061	1.0986122886681098
1062	0.33647223662121289
1063	0.21130909366720696
1064	1.4350845252893227
1065	0.10008345855698263
1066	1.4350845252893227
1067	1.4350845252893227
1068	1.4350845252893227
1069	1.4350845252893227
1070	1.4350845252893227
1071	1.4350845252893227
1072	1.4350845252893227
1073	1.4350845252893227
1074	1.4350845252893227
1075	1.4350845252893227
1076	1.4350845252893227
1077	1.4350845252893227
1078	1.4350845252893227
1079	1.4350845252893227
1080	1.4350845252893227
1081	1.4350845252893227
1082	1.4350845252893227
1083	1.4350845252893227
1084	1.4350845252893227
1085	1.4350845252893227
1086	1.4350845252893227
1087	0.47957308026188628
1088	1.4350845252893227
1089	0.47957308026188628
1090	1.4350845252893227
1091	1.4350845252893227
1092	0.10008345855698263
1093	0.84729786038720367
1094	0.64662716492505246
1095	0.33647223662121289
1096	0.33647223662121289
1097	1.0986122886681098
1098	1.4350845252893227
1099	1.4350845252893227
1100	1.4350845252893227
1101	1.0986122886681098
1102	0.84729786038720367
1103	1.4350845252893227
1104	0.21130909366720696
1105	1.4350845252893227
1106	1.0986122886681098
1107	0.47957308026188628
1108	1.4350845252893227
1109	0.47957308026188628
1110	1.4350845252893227
1111	1.4350845252893227
1112	1.4350845252893227
1113	1.4350845252893227
1114	1.4350845252893227
1115	1.4350845252893227
1116	1.4350845252893227
1117	1.4350845252893227
1118	1.4350845252893227
1119	1.4350845252893227
1120	1.0986122886681098
1121	1.0986122886681098
1122	1.0986122886681098
1123	0.33647223662121289
1124	1.4350845252893227
1125	1.0986122886681098
1126	1.4350845252893227
1127	0.47957308026188628
1128	1.4350845252893227
1129	0.84729786038720367
1130	0.84729786038720367
1131	1.4350845252893227
1132	1.4350845252893227
1133	1.4350845252893227
1134	1.4350845252893227
1135	1.0986122886681098
1136	1.4350845252893227
1137	1.4350845252893227
1138	1.4350845252893227
1139	1.0986122886681098
1140	0.33647223662121289
1141	0.10008345855698263
1142	1.0986122886681098
1143	1.0986122886681098
1144	0.33647223662121289
1145	1.4350845252893227
1146	1.4350845252893227
1147	1.0986122886681098
1148	1.4350845252893227
1149	1.0986122886681098
1150	1.0986122886681098
1151	0.21130909366720696
1152	1.4350845252893227
1153	1.4350845252893227
1154	1.4350845252893227
1155	0.84729786038720367
1156	1.4350845252893227
1157	1.4350845252893227
1158	0.84729786038720367
1159	1.4350845252893227
1160	0.21130909366720696
1161	0.64662716492505246
1162	1.0986122886681098
1163	1.4350845252893227
1164	1.0986122886681098
1165	0.84729786038720367
1166	0.64662716492505246
1167	1.0986122886681098
1168	1.4350845252893227
1169	1.4350845252893227
1170	1.4350845252893227
1171	1.4350845252893227
1172	0.21130909366720696
1173	1.4350845252893227
1174	1.4350845252893227
1175	0.84729786038720367
1176	1.4350845252893227
1177	0.84729786038720367
1178	1.4350845252893227
1179	1.4350845252893227
1180	1.4350845252893227
1181	1.4350845252893227
1182	1.4350845252893227
1183	1.0986122886681098
1184	1.4350845252893227
1185	1.4350845252893227
1186	1.4350845252893227
1187	1.0986122886681098
1188	1.4350845252893227
1189	1.0986122886681098
1190	1.4350845252893227
1191	1.0986122886681098
1192	1.4350845252893227
1193	1.4350845252893227
1194	1.4350845252893227
1195	0.10008345855698263
1196	0.84729786038720367
1197	1.4350845252893227
1198	0.84729786038720367
1199	1.4350845252893227
1200	1.4350845252893227
1201	1.4350845252893227
1202	1.0986122886681098
1203	1.4350845252893227
1204	0.84729786038720367
1205	1.0986122886681098
1206	1.4350845252893227
1207	1.4350845252893227
1208	1.4350845252893227
1209	1.4350845252893227
1210	1.4350845252893227
1211	1.4350845252893227
1212	1.4350845252893227
1213	1.4350845252893227
1214	1.0986122886681098
1215	0.84729786038720367
1216	1.4350845252893227
1217	1.4350845252893227
1218	1.4350845252893227
1219	0.84729786038720367
1220	1.4350845252893227
1221	0.84729786038720367
1222	1.4350845252893227
1223	1.4350845252893227
1224	0.33647223662121289
1225	1.4350845252893227
1226	1.4350845252893227
1227	0.84729786038720367
1228	1.4350845252893227
1229	1.0986122886681098
1230	1.4350845252893227
1231	0.84729786038720367
1232	1.4350845252893227
1233	0.47957308026188628
1234	1.0986122886681098
1235	1.4350845252893227
1236	0.64662716492505246
1237	1.4350845252893227
1238	1.4350845252893227
1239	1.4350845252893227
1240	1.4350845252893227
1241	1.4350845252893227
1242	1.4350845252893227
1243	0.21130909366720696
1244	1.4350845252893227
1245	1.4350845252893227
1246	1.4350845252893227
1247	1.4350845252893227
1248	1.4350845252893227
1249	1.4350845252893227
1250	1.4350845252893227
1251	0.64662716492505246
1252	1.4350845252893227
1253	1.4350845252893227
1254	1.0986122886681098
1255	1.0986122886681098
1256	1.4350845252893227
1257	0.84729786038720367
1258	1.4350845252893227
1259	1.4350845252893227
1260	0.64662716492505246
1261	1.0986122886681098
1262	1.4350845252893227
1263	1.4350845252893227
1264	1.4350845252893227
1265	1.4350845252893227
1266	1.4350845252893227
1267	1.4350845252893227
1268	1.4350845252893227
1269	1.4350845252893227
1270	1.4350845252893227
1271	1.4350845252893227
1272	1.4350845252893227
1273	1.4350845252893227
1274	1.4350845252893227
1275	1.4350845252893227
1276	1.4350845252893227
1277	1.4350845252893227
1278	1.4350845252893227
1279	1.4350845252893227
1280	1.4350845252893227
1281	1.4350845252893227
1282	1.4350845252893227
1283	1.4350845252893227
1284	1.4350845252893227
1285	1.4350845252893227
1286	1.4350845252893227
1287	1.4350845252893227
1288	1.4350845252893227
1289	1.4350845252893227
1290	1.4350845252893227
1291	1.4350845252893227
1292	1.4350845252893227
1293	1.4350845252893227
1294	1.4350845252893227
1295	1.4350845252893227
1296	1.4350845252893227
1297	0.84729786038720367
1298	0.47957308026188628
1299	1.0986122886681098
1300	1.4350845252893227
1301	1.4350845252893227
1302	1.4350845252893227
1303	1.4350845252893227
1304	1.4350845252893227
1305	1.4350845252893227
1306	1.4350845252893227
1307	1.4350845252893227
1308	1.4350845252893227
1309	1.4350845252893227
1310	1.4350845252893227
1311	1.4350845252893227
1312	1.0986122886681098
1313	1.4350845252893227
1314	0.64662716492505246
1315	1.4350845252893227
1316	1.4350845252893227
1317	1.4350845252893227
1318	1.4350845252893227
1319	0.84729786038720367
1320	1.0986122886681098
1321	1.4350845252893227
1322	1.0986122886681098
1323	1.0986122886681098
1324	1.4350845252893227
1325	1.4350845252893227
1326	0.47957308026188628
1327	0.84729786038720367
1328	1.4350845252893227
1329	1.4350845252893227
1330	1.0986122886681098
1331	1.0986122886681098
1332	1.4350845252893227
1333	1.4350845252893227
1334	1.0986122886681098
1335	1.4350845252893227
1336	1.4350845252893227
1337	1.4350845252893227
1338	0.64662716492505246
1339	1.4350845252893227
1340	1.4350845252893227
1341	1.4350845252893227
1342	1.4350845252893227
1343	1.4350845252893227
1344	1.0986122886681098
1345	0.64662716492505246
1346	1.4350845252893227
1347	0.64662716492505246
1348	1.4350845252893227
1349	1.4350845252893227
1350	1.0986122886681098
1351	1.4350845252893227
1352	1.4350845252893227
1353	1.4350845252893227
1354	1.4350845252893227
1355	1.4350845252893227
1356	1.4350845252893227
1357	0.64662716492505246
1358	1.4350845252893227
1359	1.4350845252893227
1360	0.84729786038720367
1361	1.4350845252893227
1362	1.4350845252893227
1363	1.4350845252893227
1364	1.0986122886681098
1365	0.33647223662121289
1366	1.4350845252893227
1367	1.0986122886681098
1368	0.10008345855698263
1369	1.4350845252893227
1370	1.4350845252893227
1371	1.4350845252893227
1372	0.64662716492505246
1373	0.64662716492505246
1374	1.0986122886681098
1375	1.4350845252893227
1376	1.4350845252893227
1377	1.4350845252893227
1378	1.4350845252893227
1379	0.84729786038720367
1380	1.4350845252893227
1381	1.4350845252893227
1382	1.4350845252893227
1383	1.4350845252893227
1384	1.4350845252893227
1385	1.4350845252893227
1386	1.4350845252893227
1387	1.4350845252893227
1388	1.4350845252893227
1389	1.4350845252893227
1390	0.84729786038720367
1391	1.4350845252893227
1392	1.4350845252893227
1393	1.4350845252893227
1394	1.4350845252893227
1395	1.4350845252893227
1396	1.4350845252893227
1397	0.64662716492505246
1398	1.4350845252893227
1399	0.64662716492505246
1400	1.4350845252893227
1401	1.4350845252893227
1402	1.4350845252893227
1403	1.4350845252893227
1404	1.4350845252893227
1405	1.4350845252893227
1406	1.4350845252893227
1407	1.4350845252893227
1408	1.4350845252893227
1409	1.4350845252893227
1410	1.0986122886681098
1411	1.4350845252893227
1412	1.9459101490553132
1413	0.84729786038720367
1414	1.4350845252893227
1415	1.4350845252893227
1416	1.4350845252893227
1417	1.0986122886681098
1418	1.9459101490553132
1419	1.4350845252893227
1420	1.4350845252893227
1421	1.4350845252893227
1422	1.4350845252893227
1423	1.4350845252893227
1424	1.4350845252893227
1425	1.4350845252893227
1426	1.4350845252893227
1427	1.0986122886681098
1428	1.4350845252893227
1429	0.33647223662121289
1430	1.4350845252893227
1431	1.4350845252893227
1432	1.4350845252893227
1433	0
1434	1.4350845252893227
1435	1.4350845252893227
1436	1.4350845252893227
1437	1.4350845252893227
1438	1.4350845252893227
1439	1.4350845252893227
1440	1.4350845252893227
1441	1.4350845252893227
1442	1.4350845252893227
1443	1.4350845252893227
1444	1.4350845252893227
1445	1.4350845252893227
1446	1.4350845252893227
1447	1.4350845252893227
1448	1.4350845252893227
1449	1.4350845252893227
1450	1.4350845252893227
1451	1.4350845252893227
1452	1.4350845252893227
1453	1.4350845252893227
1454	1.4350845252893227
1455	1.4350845252893227
1456	1.4350845252893227
1457	1.4350845252893227
1458	1.4350845252893227
1459	1.0986122886681098
1460	0.84729786038720367
1461	1.4350845252893227
1462	1.4350845252893227
1463	1.4350845252893227
1464	1.4350845252893227
1465	1.0986122886681098
1466	1.0986122886681098
1467	1.4350845252893227
1468	1.4350845252893227
1469	1.4350845252893227
1470	1.4350845252893227
1471	0.64662716492505246
1472	1.4350845252893227
1473	1.4350845252893227
1474	1.4350845252893227
1475	1.4350845252893227
1476	1.4350845252893227
1477	1.4350845252893227
1478	1.4350845252893227
1479	0.64662716492505246
1480	1.4350845252893227
1481	1.4350845252893227
1482	1.4350845252893227
1483	1.4350845252893227
1484	1.4350845252893227
1485	1.4350845252893227
1486	1.4350845252893227
1487	1.0986122886681098
1488	1.4350845252893227
1489	1.0986122886681098
1490	1.0986122886681098
1491	1.4350845252893227
1492	1.4350845252893227
1493	1.4350845252893227
1494	1.4350845252893227
1495	1.4350845252893227
1496	1.4350845252893227
1497	0.47957308026188628
1498	1.4350845252893227
1499	1.0986122886681098
1500	0.84729786038720367
1501	1.4350845252893227
1502	1.4350845252893227
1503	0.84729786038720367
1504	1.4350845252893227
1505	1.4350845252893227
1506	1.4350845252893227
1507	1.4350845252893227
1508	1.4350845252893227
1509	1.4350845252893227
1510	1.4350845252893227
1511	1.4350845252893227
1512	1.4350845252893227
1513	1.4350845252893227
1514	1.4350845252893227
1515	1.4350845252893227
1516	1.4350845252893227
1517	1.4350845252893227
1518	1.4350845252893227
1519	1.4350845252893227
1520	0.84729786038720367
1521	1.4350845252893227
1522	0.64662716492505246
1523	1.0986122886681098
1524	1.0986122886681098
1525	1.0986122886681098
1526	1.0986122886681098
1527	1.4350845252893227
1528	1.4350845252893227
1529	0.84729786038720367
1530	1.0986122886681098
1531	1.4350845252893227
1532	1.0986122886681098
1533	1.4350845252893227
1534	1.4350845252893227
1535	1.4350845252893227
1536	1.4350845252893227
1537	1.4350845252893227
1538	1.4350845252893227
1539	1.4350845252893227
1540	1.4350845252893227
1541	0.84729786038720367
1542	1.4350845252893227
1543	0.84729786038720367
1544	1.4350845252893227
1545	1.4350845252893227
1546	1.4350845252893227
1547	1.0986122886681098
1548	1.4350845252893227
1549	1.4350845252893227
1550	1.4350845252893227
1551	1.4350845252893227
1552	1.4350845252893227
1553	1.4350845252893227
1554	1.4350845252893227
1555	0.84729786038720367
1556	1.4350845252893227
1557	1.4350845252893227
1558	1.4350845252893227
1559	1.0986122886681098
1560	1.4350845252893227
1561	1.4350845252893227
1562	1.4350845252893227
1563	1.4350845252893227
1564	1.4350845252893227
1565	1.4350845252893227
1566	0.84729786038720367
1567	1.4350845252893227
1568	1.4350845252893227
1569	1.4350845252893227
1570	1.4350845252893227
1571	1.4350845252893227
1572	1.4350845252893227
1573	1.4350845252893227
1574	1.4350845252893227
1575	1.4350845252893227
1576	1.4350845252893227
1577	1.4350845252893227
1578	1.4350845252893227
1579	1.4350845252893227
1580	1.4350845252893227
1581	1.4350845252893227
1582	1.4350845252893227
1583	1.4350845252893227
1584	1.4350845252893227
1585	1.4350845252893227
1586	1.4350845252893227
1587	1.4350845252893227
1588	1.0986122886681098
1589	1.4350845252893227
1590	1.4350845252893227
1591	1.4350845252893227
1592	1.4350845252893227
1593	1.4350845252893227
1594	0.84729786038720367
1595	1.4350845252893227
1596	1.0986122886681098
1597	1.4350845252893227
1598	1.4350845252893227
1599	1.4350845252893227
1600	0.84729786038720367
1601	1.4350845252893227
1602	1.4350845252893227
1603	1.0986122886681098
1604	1.4350845252893227
1605	1.4350845252893227
1606	0.84729786038720367
1607	1.4350845252893227
1608	0.84729786038720367
1609	1.4350845252893227
1610	1.4350845252893227
1611	1.4350845252893227
1612	1.4350845252893227
1613	1.4350845252893227
1614	1.4350845252893227
1615	1.4350845252893227
1616	1.4350845252893227
1617	1.4350845252893227
1618	1.4350845252893227
1619	1.4350845252893227
1620	1.4350845252893227
1621	1.4350845252893227
1622	1.4350845252893227
1623	1.4350845252893227
1624	1.4350845252893227
1625	1.4350845252893227
1626	1.4350845252893227
1627	1.4350845252893227
1628	1.4350845252893227
1629	1.4350845252893227
1630	1.4350845252893227
1631	1.4350845252893227
1632	1.4350845252893227
1633	1.4350845252893227
1634	1.4350845252893227
1635	1.4350845252893227
1636	1.4350845252893227
1637	1.4350845252893227
1638	1.4350845252893227
1639	1.0986122886681098
1640	0.47957308026188628
1641	1.4350845252893227
1642	1.4350845252893227
1643	1.4350845252893227
1644	1.4350845252893227
1645	1.4350845252893227
1646	1.4350845252893227
1647	1.4350845252893227
1648	1.4350845252893227
1649	1.4350845252893227
1650	1.4350845252893227
1651	1.4350845252893227
1652	1.0986122886681098
1653	1.4350845252893227
1654	1.4350845252893227
1655	1.9459101490553132
1656	1.0986122886681098
1657	1.4350845252893227
1658	1.4350845252893227
1659	1.0986122886681098
1660	1.0986122886681098
1661	1.4350845252893227
1662	0.33647223662121289
1663	1.0986122886681098
1664	1.4350845252893227
1665	0.84729786038720367
1666	1.0986122886681098
1667	1.4350845252893227
1668	1.4350845252893227
1669	1.4350845252893227
1670	1.4350845252893227
1671	1.4350845252893227
1672	1.4350845252893227
1673	1.4350845252893227
1674	0.47957308026188628
1675	1.4350845252893227
1676	1.4350845252893227
1677	1.4350845252893227
1678	1.4350845252893227
1679	1.4350845252893227
1680	1.9459101490553132
1681	1.4350845252893227
1682	1.4350845252893227
1683	1.4350845252893227
1684	1.4350845252893227
1685	0.84729786038720367
1686	1.4350845252893227
1687	0.84729786038720367
1688	1.4350845252893227
1689	1.4350845252893227
1690	1.4350845252893227
1691	1.4350845252893227
1692	1.4350845252893227
1693	1.4350845252893227
1694	0.84729786038720367
1695	1.4350845252893227
1696	1.4350845252893227
1697	1.4350845252893227
1698	1.4350845252893227
1699	1.0986122886681098
1700	1.0986122886681098
1701	0.33647223662121289
1702	0.84729786038720367
1703	1.4350845252893227
1704	1.4350845252893227
1705	1.0986122886681098
1706	0.33647223662121289
1707	0.84729786038720367
1708	1.4350845252893227
1709	0.84729786038720367
1710	1.4350845252893227
1711	0.64662716492505246
1712	0.47957308026188628
1713	0.84729786038720367
1714	0.21130909366720696
1715	0
1716	0.10008345855698263
1717	1.9459101490553132
1718	1.9459101490553132
1719	1.9459101490553132
1720	1.9459101490553132
1729	1.9459101490553132
1744	1.0986122886681098
1749	1.0986122886681098
1758	1.9459101490553132
1759	1.9459101490553132
1760	1.4350845252893227
1761	1.9459101490553132
1762	1.9459101490553132
1763	1.9459101490553132
1764	1.0986122886681098
1765	1.9459101490553132
1766	1.9459101490553132
1767	1.9459101490553132
1768	0.64662716492505246
1769	1.9459101490553132
1770	0.84729786038720367
1771	1.0986122886681098
1772	1.9459101490553132
1773	1.9459101490553132
1774	1.9459101490553132
1775	1.9459101490553132
1776	0.84729786038720367
1777	0.84729786038720367
1778	1.9459101490553132
1779	1.9459101490553132
1780	1.0986122886681098
1781	1.9459101490553132
1782	1.9459101490553132
1783	1.0986122886681098
1784	1.0986122886681098
1785	1.9459101490553132
1786	1.0986122886681098
1787	1.9459101490553132
1788	1.9459101490553132
1789	1.0986122886681098
1790	1.9459101490553132
1791	1.0986122886681098
1792	1.0986122886681098
1793	1.9459101490553132
1794	1.0986122886681098
1795	1.9459101490553132
1796	1.9459101490553132
1797	1.9459101490553132
1798	1.4350845252893227
1799	1.9459101490553132
1800	1.9459101490553132
1801	1.9459101490553132
1802	1.0986122886681098
1803	1.9459101490553132
1804	1.9459101490553132
1805	1.9459101490553132
1806	1.9459101490553132
1807	0.84729786038720367
1808	1.9459101490553132
1809	1.0986122886681098
1810	1.0986122886681098
1811	1.4350845252893227
1812	1.9459101490553132
1813	1.9459101490553132
1814	1.9459101490553132
1815	1.9459101490553132
1816	1.4350845252893227
1817	1.0986122886681098
1818	1.9459101490553132
1819	1.9459101490553132
1820	1.0986122886681098
1821	1.9459101490553132
1822	1.9459101490553132
1823	1.0986122886681098
1824	1.9459101490553132
1825	1.9459101490553132
1826	1.9459101490553132
1827	1.4350845252893227
1828	1.0986122886681098
1829	1.9459101490553132
1830	1.4350845252893227
1831	1.9459101490553132
1832	1.9459101490553132
1833	1.9459101490553132
1834	1.9459101490553132
1835	1.9459101490553132
1836	1.9459101490553132
1837	0.84729786038720367
1838	1.4350845252893227
1839	1.9459101490553132
1840	1.4350845252893227
1841	1.9459101490553132
1842	1.9459101490553132
1843	0.84729786038720367
1844	1.9459101490553132
1845	1.0986122886681098
1846	1.9459101490553132
1847	1.9459101490553132
1848	1.9459101490553132
1849	1.9459101490553132
1850	1.9459101490553132
1851	1.9459101490553132
1852	1.9459101490553132
1853	1.9459101490553132
1854	1.9459101490553132
1855	1.9459101490553132
1856	1.0986122886681098
1857	1.9459101490553132
1858	1.9459101490553132
1859	1.4350845252893227
1860	1.9459101490553132
1861	1.9459101490553132
1862	1.4350845252893227
1863	1.4350845252893227
1864	1.9459101490553132
1865	1.9459101490553132
1866	1.9459101490553132
1867	1.9459101490553132
1868	1.9459101490553132
1869	1.4350845252893227
1870	1.0986122886681098
1871	1.9459101490553132
1872	0.84729786038720367
1873	1.4350845252893227
1874	0.64662716492505246
1875	1.9459101490553132
1876	1.9459101490553132
1877	1.9459101490553132
1878	1.0986122886681098
1879	1.0986122886681098
1880	1.9459101490553132
1881	1.9459101490553132
1882	1.0986122886681098
1883	1.0986122886681098
1884	1.0986122886681098
1885	1.9459101490553132
1886	1.9459101490553132
1887	1.9459101490553132
1888	1.9459101490553132
1889	1.4350845252893227
1890	1.9459101490553132
1891	1.9459101490553132
1892	1.9459101490553132
1893	1.9459101490553132
1894	1.9459101490553132
1895	1.4350845252893227
1896	1.0986122886681098
1897	1.0986122886681098
1898	1.9459101490553132
1899	1.9459101490553132
1900	1.9459101490553132
1901	1.0986122886681098
1902	1.4350845252893227
1903	1.9459101490553132
1904	1.9459101490553132
1905	1.4350845252893227
1906	1.9459101490553132
1907	1.9459101490553132
1908	1.9459101490553132
1909	1.9459101490553132
1910	1.9459101490553132
1911	1.0986122886681098
1912	1.0986122886681098
1913	1.0986122886681098
1914	1.9459101490553132
1915	1.9459101490553132
1916	1.0986122886681098
1917	1.9459101490553132
1918	1.9459101490553132
1919	1.9459101490553132
1920	1.4350845252893227
1921	1.9459101490553132
1922	1.9459101490553132
1923	1.9459101490553132
1924	1.9459101490553132
1925	1.9459101490553132
1926	1.9459101490553132
1927	1.9459101490553132
1928	1.9459101490553132
1929	1.9459101490553132
1930	1.9459101490553132
1931	1.9459101490553132
1932	1.9459101490553132
1933	1.9459101490553132
1934	1.0986122886681098
1935	1.9459101490553132
1936	1.9459101490553132
1937	1.9459101490553132
1938	1.9459101490553132
1939	1.9459101490553132
1940	1.9459101490553132
1941	1.4350845252893227
1942	0.84729786038720367
1943	1.9459101490553132
1944	1.4350845252893227
1945	1.9459101490553132
1946	1.9459101490553132
1956	0.84729786038720367
1959	1.9459101490553132
1964	1.4350845252893227
1965	1.4350845252893227
1966	0.64662716492505246
1967	1.0986122886681098
1968	1.4350845252893227
1969	1.0986122886681098
1977	1.9459101490553132
1993	1.4350845252893227
1994	1.0986122886681098
1995	1.4350845252893227
1996	1.4350845252893227
1997	1.9459101490553132
1998	1.9459101490553132
1999	0.47957308026188628
2000	1.9459101490553132
2001	1.9459101490553132
2002	1.4350845252893227
2003	0.64662716492505246
2004	1.9459101490553132
2005	1.9459101490553132
2006	1.9459101490553132
2007	1.0986122886681098
2008	1.9459101490553132
2009	1.9459101490553132
2010	1.9459101490553132
2011	1.9459101490553132
2012	1.9459101490553132
2013	1.9459101490553132
2014	1.9459101490553132
2015	1.9459101490553132
2016	1.9459101490553132
2017	1.9459101490553132
2018	1.4350845252893227
2019	1.4350845252893227
2020	1.4350845252893227
2021	0.64662716492505246
2022	0.47957308026188628
2023	1.4350845252893227
2024	0.84729786038720367
2025	1.0986122886681098
2026	1.0986122886681098
2027	0.84729786038720367
2028	1.4350845252893227
2029	1.4350845252893227
2030	1.0986122886681098
2031	1.4350845252893227
2032	1.0986122886681098
2033	0.21130909366720696
2034	1.4350845252893227
2035	0.84729786038720367
2036	1.4350845252893227
2037	0.47957308026188628
2038	1.4350845252893227
2039	1.4350845252893227
2040	1.4350845252893227
2041	1.4350845252893227
2042	1.4350845252893227
2043	0.33647223662121289
2044	1.4350845252893227
2045	1.4350845252893227
2046	1.0986122886681098
2047	0.84729786038720367
2048	1.0986122886681098
2049	1.4350845252893227
2050	1.0986122886681098
2051	1.0986122886681098
2052	1.0986122886681098
2053	1.0986122886681098
2054	1.4350845252893227
2055	1.4350845252893227
2056	1.4350845252893227
2057	0.84729786038720367
2058	1.4350845252893227
2059	1.4350845252893227
2060	1.4350845252893227
2061	1.4350845252893227
2062	1.4350845252893227
2063	1.4350845252893227
2064	1.0986122886681098
2065	1.0986122886681098
2066	0.84729786038720367
2067	1.4350845252893227
2068	1.0986122886681098
2069	1.4350845252893227
2070	1.0986122886681098
2071	0.84729786038720367
2072	1.4350845252893227
2073	1.4350845252893227
2074	0.84729786038720367
2075	1.4350845252893227
2076	0.84729786038720367
2077	1.0986122886681098
2078	0.64662716492505246
2079	1.0986122886681098
2080	1.0986122886681098
2081	1.0986122886681098
2082	0.47957308026188628
2083	1.4350845252893227
2084	0.64662716492505246
2085	1.0986122886681098
2086	1.4350845252893227
2087	1.0986122886681098
2088	0.47957308026188628
2089	1.4350845252893227
2090	1.4350845252893227
2091	1.4350845252893227
2092	0.84729786038720367
2093	0.84729786038720367
2094	0.64662716492505246
2095	1.4350845252893227
2096	1.4350845252893227
2097	1.0986122886681098
2098	1.4350845252893227
2099	1.4350845252893227
2100	0.84729786038720367
2101	1.4350845252893227
2102	1.4350845252893227
2103	0.84729786038720367
2104	1.4350845252893227
2105	1.0986122886681098
2106	0.84729786038720367
2107	1.4350845252893227
2108	1.4350845252893227
2109	0.47957308026188628
2110	1.4350845252893227
2111	0.84729786038720367
2112	0.84729786038720367
2113	0.84729786038720367
2114	0.64662716492505246
2115	1.4350845252893227
2116	1.0986122886681098
2117	1.4350845252893227
2118	1.4350845252893227
2119	1.4350845252893227
2120	1.0986122886681098
2121	0.84729786038720367
2122	1.4350845252893227
2123	1.4350845252893227
2124	1.4350845252893227
2125	1.4350845252893227
2126	1.4350845252893227
2127	1.4350845252893227
2128	1.4350845252893227
2129	1.4350845252893227
2130	1.0986122886681098
2131	1.4350845252893227
2132	0.84729786038720367
2133	1.4350845252893227
2134	1.4350845252893227
2135	1.0986122886681098
2136	1.4350845252893227
2137	0.84729786038720367
2138	1.4350845252893227
2139	1.4350845252893227
2140	1.4350845252893227
2141	1.0986122886681098
2142	1.0986122886681098
2143	1.0986122886681098
2144	1.0986122886681098
2145	1.0986122886681098
2146	1.4350845252893227
2147	1.4350845252893227
2148	1.4350845252893227
2149	0.84729786038720367
2150	0.84729786038720367
2151	0.84729786038720367
2152	1.4350845252893227
2153	1.0986122886681098
2154	1.4350845252893227
2155	1.0986122886681098
2156	1.0986122886681098
2157	1.4350845252893227
2158	1.0986122886681098
2159	0.84729786038720367
2160	1.4350845252893227
2161	1.4350845252893227
2162	1.0986122886681098
2163	1.0986122886681098
2164	1.4350845252893227
2165	1.4350845252893227
2166	1.4350845252893227
2167	1.0986122886681098
2168	1.0986122886681098
2169	1.4350845252893227
2170	0.21130909366720696
2171	1.0986122886681098
2172	1.4350845252893227
2173	1.0986122886681098
2174	1.4350845252893227
2175	1.4350845252893227
2176	1.4350845252893227
2177	1.4350845252893227
2178	0.64662716492505246
2179	1.4350845252893227
2180	1.0986122886681098
2181	1.0986122886681098
2182	0.64662716492505246
2183	0.33647223662121289
2184	1.4350845252893227
2185	1.0986122886681098
2186	1.0986122886681098
2187	0.84729786038720367
2188	1.4350845252893227
2189	0.64662716492505246
2190	1.4350845252893227
2191	1.4350845252893227
2192	1.4350845252893227
2193	1.4350845252893227
2194	0.47957308026188628
2195	1.0986122886681098
2196	1.4350845252893227
2197	1.4350845252893227
2198	0.64662716492505246
2199	1.4350845252893227
2200	1.4350845252893227
2201	1.0986122886681098
2202	1.4350845252893227
2203	1.0986122886681098
2204	1.4350845252893227
2205	0.64662716492505246
2206	1.4350845252893227
2207	1.4350845252893227
2208	1.4350845252893227
2209	1.4350845252893227
2210	1.4350845252893227
2211	1.0986122886681098
2212	0.64662716492505246
2213	1.4350845252893227
2214	1.4350845252893227
2215	1.4350845252893227
2216	0.84729786038720367
2217	0.84729786038720367
2218	0.84729786038720367
2219	1.4350845252893227
2220	1.4350845252893227
2221	1.0986122886681098
2222	1.4350845252893227
2223	0.64662716492505246
2224	1.4350845252893227
2225	1.4350845252893227
2226	1.4350845252893227
2227	1.4350845252893227
2228	1.4350845252893227
2229	1.4350845252893227
2230	1.4350845252893227
2231	1.4350845252893227
2232	1.4350845252893227
2233	1.0986122886681098
2234	1.4350845252893227
2235	1.4350845252893227
2236	1.4350845252893227
2237	1.4350845252893227
2238	1.4350845252893227
2239	0.64662716492505246
2240	1.4350845252893227
2241	1.4350845252893227
2242	1.0986122886681098
2243	0.84729786038720367
2244	1.0986122886681098
2245	1.0986122886681098
2246	1.4350845252893227
2247	0.33647223662121289
2248	1.4350845252893227
2249	1.4350845252893227
2250	1.4350845252893227
2251	1.4350845252893227
2252	1.4350845252893227
2253	1.4350845252893227
2254	1.4350845252893227
2255	1.4350845252893227
2256	1.4350845252893227
2257	1.0986122886681098
2258	1.4350845252893227
2259	1.0986122886681098
2260	0.84729786038720367
2261	1.4350845252893227
2262	1.4350845252893227
2263	1.0986122886681098
2264	1.4350845252893227
2265	1.0986122886681098
2266	1.4350845252893227
2267	0.84729786038720367
2268	1.0986122886681098
2269	1.0986122886681098
2270	1.0986122886681098
2271	1.4350845252893227
2272	0.64662716492505246
2273	1.4350845252893227
2274	1.4350845252893227
2275	1.4350845252893227
2276	1.4350845252893227
2277	1.4350845252893227
2278	1.0986122886681098
2279	1.4350845252893227
2280	0.33647223662121289
2281	1.4350845252893227
2282	1.9459101490553132
2283	1.9459101490553132
2284	1.9459101490553132
2285	1.9459101490553132
2286	1.9459101490553132
2287	1.4350845252893227
2288	1.4350845252893227
2289	1.9459101490553132
2290	1.9459101490553132
2291	1.4350845252893227
2292	1.9459101490553132
2293	1.9459101490553132
2294	1.9459101490553132
2295	1.9459101490553132
2296	1.9459101490553132
2297	1.9459101490553132
2298	1.9459101490553132
2299	1.9459101490553132
2300	1.9459101490553132
2301	1.9459101490553132
2302	1.9459101490553132
2303	1.9459101490553132
2304	1.9459101490553132
2305	1.9459101490553132
2306	1.9459101490553132
2307	1.9459101490553132
2308	1.9459101490553132
2309	1.9459101490553132
2310	1.9459101490553132
2311	1.9459101490553132
2312	1.9459101490553132
2313	1.9459101490553132
2314	1.9459101490553132
2315	1.9459101490553132
2316	1.9459101490553132
2317	1.4350845252893227
2318	1.9459101490553132
2319	1.9459101490553132
2320	1.9459101490553132
2321	1.9459101490553132
2322	1.9459101490553132
2323	1.9459101490553132
2324	1.9459101490553132
2325	1.9459101490553132
2326	1.9459101490553132
2327	1.9459101490553132
2328	1.9459101490553132
2329	1.9459101490553132
2330	1.9459101490553132
2331	1.9459101490553132
2332	1.0986122886681098
2333	0.84729786038720367
2334	1.9459101490553132
2335	1.9459101490553132
2336	1.9459101490553132
2337	1.9459101490553132
2338	1.9459101490553132
2339	1.9459101490553132
2340	1.9459101490553132
2341	1.9459101490553132
2342	1.9459101490553132
2343	1.9459101490553132
2344	1.9459101490553132
2345	1.9459101490553132
2346	1.9459101490553132
2347	1.9459101490553132
2348	1.9459101490553132
2349	1.9459101490553132
2350	1.9459101490553132
2351	1.9459101490553132
2352	1.9459101490553132
2353	1.9459101490553132
2354	1.9459101490553132
2355	1.9459101490553132
2356	1.4350845252893227
2357	1.9459101490553132
2358	1.9459101490553132
2359	1.0986122886681098
2360	1.9459101490553132
2361	1.9459101490553132
2362	1.9459101490553132
2363	1.9459101490553132
2364	1.9459101490553132
2365	1.9459101490553132
2366	1.9459101490553132
2367	1.9459101490553132
2368	1.9459101490553132
2369	1.9459101490553132
2370	1.9459101490553132
2371	1.9459101490553132
2372	1.9459101490553132
2373	1.9459101490553132
2374	1.9459101490553132
2375	1.9459101490553132
2376	1.9459101490553132
2377	1.4350845252893227
2378	1.9459101490553132
2379	1.9459101490553132
2380	1.9459101490553132
2381	1.9459101490553132
2394	1.9459101490553132
2395	1.9459101490553132
2396	1.9459101490553132
2397	1.4350845252893227
2398	1.9459101490553132
2399	1.4350845252893227
2400	1.4350845252893227
2401	1.4350845252893227
2402	1.4350845252893227
2409	1.9459101490553132
2411	1.9459101490553132
2418	1.9459101490553132
2419	1.9459101490553132
2420	1.4350845252893227
2421	1.9459101490553132
2422	1.9459101490553132
2423	1.9459101490553132
2424	1.9459101490553132
2425	0.84729786038720367
2426	1.9459101490553132
2427	1.9459101490553132
2428	1.9459101490553132
2429	1.4350845252893227
2430	1.9459101490553132
2431	1.9459101490553132
2432	1.9459101490553132
2433	1.9459101490553132
2434	1.9459101490553132
2456	1.9459101490553132
2488	0.47957308026188628
2489	1.0986122886681098
2490	1.9459101490553132
2491	1.9459101490553132
2492	1.9459101490553132
2493	1.4350845252893227
2494	1.9459101490553132
2495	0.47957308026188628
2496	1.0986122886681098
2497	0.64662716492505246
2498	1.9459101490553132
2499	1.9459101490553132
2500	1.4350845252893227
2501	1.9459101490553132
2502	1.9459101490553132
2503	1.9459101490553132
2504	1.9459101490553132
2505	1.9459101490553132
2506	1.4350845252893227
2507	1.4350845252893227
2508	1.9459101490553132
2509	1.9459101490553132
2510	1.4350845252893227
2511	1.9459101490553132
2512	1.9459101490553132
2513	0.84729786038720367
2514	1.9459101490553132
2515	1.4350845252893227
2516	1.4350845252893227
2517	1.9459101490553132
2518	0.84729786038720367
2519	1.9459101490553132
2520	1.9459101490553132
2521	1.9459101490553132
2522	1.9459101490553132
2523	1.9459101490553132
2524	1.9459101490553132
2525	1.9459101490553132
2526	1.0986122886681098
2527	1.9459101490553132
2528	1.9459101490553132
2529	1.9459101490553132
2530	1.9459101490553132
2531	1.9459101490553132
2532	1.9459101490553132
2533	1.4350845252893227
2534	1.4350845252893227
2535	1.9459101490553132
2536	1.9459101490553132
2537	1.4350845252893227
2538	1.9459101490553132
2539	1.9459101490553132
2540	1.4350845252893227
2541	1.9459101490553132
2542	1.9459101490553132
2543	1.9459101490553132
2544	1.9459101490553132
2545	1.9459101490553132
2546	1.9459101490553132
2547	1.9459101490553132
2548	1.9459101490553132
2549	1.9459101490553132
2550	1.9459101490553132
2551	1.9459101490553132
2552	1.9459101490553132
2553	1.0986122886681098
2554	1.9459101490553132
2555	1.4350845252893227
2556	1.9459101490553132
2557	1.4350845252893227
2558	1.9459101490553132
2559	1.9459101490553132
2560	1.9459101490553132
2561	1.9459101490553132
2562	1.9459101490553132
2563	1.9459101490553132
2564	1.4350845252893227
2565	1.9459101490553132
2566	1.9459101490553132
2567	1.9459101490553132
2568	1.9459101490553132
2569	1.4350845252893227
2570	1.4350845252893227
2571	1.9459101490553132
2572	1.9459101490553132
2573	1.9459101490553132
2574	1.9459101490553132
2575	1.9459101490553132
2576	1.9459101490553132
2577	1.9459101490553132
2578	1.4350845252893227
2579	1.9459101490553132
2580	1.4350845252893227
2581	1.9459101490553132
2582	1.9459101490553132
2583	1.4350845252893227
2584	1.9459101490553132
2585	0.84729786038720367
2586	1.9459101490553132
2587	1.9459101490553132
2588	1.9459101490553132
2589	1.9459101490553132
2590	1.4350845252893227
2591	1.0986122886681098
2592	1.9459101490553132
2593	1.9459101490553132
2594	1.9459101490553132
2595	1.9459101490553132
2596	1.9459101490553132
2597	1.9459101490553132
2598	1.9459101490553132
2599	1.9459101490553132
2600	1.9459101490553132
2601	1.9459101490553132
2602	1.9459101490553132
2603	1.9459101490553132
2604	1.9459101490553132
2605	1.9459101490553132
2606	1.9459101490553132
2607	1.9459101490553132
2608	1.9459101490553132
2609	1.9459101490553132
2610	1.9459101490553132
2611	1.9459101490553132
2612	1.9459101490553132
2613	1.9459101490553132
2614	1.4350845252893227
2615	1.9459101490553132
2616	1.9459101490553132
2617	1.9459101490553132
2618	1.4350845252893227
2619	1.9459101490553132
2620	1.9459101490553132
2621	1.9459101490553132
2622	1.9459101490553132
2623	1.9459101490553132
2624	1.9459101490553132
2625	1.9459101490553132
2626	1.9459101490553132
2627	1.9459101490553132
2628	1.9459101490553132
2629	1.9459101490553132
2630	1.9459101490553132
2631	1.9459101490553132
2632	1.9459101490553132
2633	1.9459101490553132
2634	1.9459101490553132
2635	1.9459101490553132
2636	1.4350845252893227
2637	1.9459101490553132
2638	1.9459101490553132
2639	1.9459101490553132
2640	1.9459101490553132
2641	1.4350845252893227
2642	1.9459101490553132
2643	1.9459101490553132
2644	1.9459101490553132
2645	1.9459101490553132
2646	1.9459101490553132
2647	1.4350845252893227
2648	1.9459101490553132
2649	1.4350845252893227
2650	1.4350845252893227
2651	1.9459101490553132
2652	1.9459101490553132
2653	1.9459101490553132
2654	1.9459101490553132
2655	1.9459101490553132
2656	1.9459101490553132
2657	1.9459101490553132
2658	1.9459101490553132
2659	1.9459101490553132
2660	1.4350845252893227
2661	1.0986122886681098
2662	1.9459101490553132
2663	1.9459101490553132
2664	1.9459101490553132
2665	1.9459101490553132
2666	1.9459101490553132
2667	1.9459101490553132
2668	1.9459101490553132
2669	1.9459101490553132
2670	1.9459101490553132
2671	1.0986122886681098
2672	1.9459101490553132
2673	1.9459101490553132
2674	1.9459101490553132
2675	1.9459101490553132
2676	0.64662716492505246
2677	1.4350845252893227
2678	1.9459101490553132
2679	1.4350845252893227
2680	1.9459101490553132
2681	1.9459101490553132
2682	1.9459101490553132
2683	1.9459101490553132
2684	1.9459101490553132
2685	1.9459101490553132
2686	1.9459101490553132
2687	1.9459101490553132
2688	1.9459101490553132
2689	1.9459101490553132
2690	1.9459101490553132
2691	1.9459101490553132
2692	1.9459101490553132
2693	1.9459101490553132
2694	1.9459101490553132
2695	1.9459101490553132
2696	1.9459101490553132
2697	1.9459101490553132
2698	1.9459101490553132
2699	1.9459101490553132
2700	1.9459101490553132
2701	1.9459101490553132
2702	1.9459101490553132
2703	1.4350845252893227
2704	1.9459101490553132
2705	0.64662716492505246
2706	1.9459101490553132
2707	1.9459101490553132
2708	1.9459101490553132
2709	1.9459101490553132
2710	1.9459101490553132
2711	1.9459101490553132
2712	1.9459101490553132
2713	1.9459101490553132
2714	1.9459101490553132
2715	1.9459101490553132
2716	1.9459101490553132
2717	1.4350845252893227
2718	1.9459101490553132
2719	1.9459101490553132
2720	1.9459101490553132
2721	1.9459101490553132
2722	1.9459101490553132
2723	1.9459101490553132
2724	1.4350845252893227
2725	1.9459101490553132
2726	1.4350845252893227
2727	0.84729786038720367
2728	1.4350845252893227
2729	1.0986122886681098
2730	0.47957308026188628
2731	0.64662716492505246
2732	1.9459101490553132
2733	1.9459101490553132
2734	1.9459101490553132
2735	1.9459101490553132
2736	1.9459101490553132
2737	1.9459101490553132
2738	1.9459101490553132
2739	1.9459101490553132
2740	1.9459101490553132
2741	1.9459101490553132
2742	1.9459101490553132
2743	1.9459101490553132
2744	1.9459101490553132
2745	1.9459101490553132
2746	1.9459101490553132
2747	1.9459101490553132
2748	1.9459101490553132
2749	1.9459101490553132
2750	1.9459101490553132
2751	0.64662716492505246
2752	1.9459101490553132
2753	1.9459101490553132
2754	1.0986122886681098
2755	1.9459101490553132
2756	1.9459101490553132
2757	1.4350845252893227
2758	1.4350845252893227
2759	1.9459101490553132
2760	1.4350845252893227
2761	1.9459101490553132
2762	1.9459101490553132
2763	1.9459101490553132
2764	1.9459101490553132
2765	1.9459101490553132
2766	1.9459101490553132
2767	1.9459101490553132
2768	1.9459101490553132
2769	1.9459101490553132
2770	1.9459101490553132
2771	1.9459101490553132
2772	1.9459101490553132
2773	1.9459101490553132
2774	1.4350845252893227
2775	1.9459101490553132
2776	1.9459101490553132
2777	1.4350845252893227
2778	1.9459101490553132
2779	1.9459101490553132
2780	1.9459101490553132
2781	1.4350845252893227
2782	1.9459101490553132
2783	1.9459101490553132
2784	1.9459101490553132
2785	1.4350845252893227
2786	1.4350845252893227
2787	0.84729786038720367
2788	1.9459101490553132
2789	1.4350845252893227
2790	1.9459101490553132
2791	1.9459101490553132
2792	1.0986122886681098
2793	1.9459101490553132
2794	1.9459101490553132
2795	1.9459101490553132
2796	1.9459101490553132
2797	1.9459101490553132
2798	1.9459101490553132
2799	1.9459101490553132
2800	1.9459101490553132
2801	1.4350845252893227
2802	1.4350845252893227
2803	1.9459101490553132
2804	1.9459101490553132
2805	1.0986122886681098
2806	1.9459101490553132
2807	1.0986122886681098
2808	1.4350845252893227
2809	1.9459101490553132
2810	0.84729786038720367
2811	1.4350845252893227
2812	0.84729786038720367
2813	1.9459101490553132
2814	1.4350845252893227
2815	1.9459101490553132
2816	1.9459101490553132
2817	1.9459101490553132
2818	1.9459101490553132
2819	1.9459101490553132
2820	1.9459101490553132
2821	1.9459101490553132
2822	1.4350845252893227
2823	0.84729786038720367
2824	1.9459101490553132
2825	1.9459101490553132
2826	1.9459101490553132
2827	1.9459101490553132
2828	1.9459101490553132
2829	1.9459101490553132
2830	1.9459101490553132
2831	1.9459101490553132
2832	1.4350845252893227
2833	1.9459101490553132
2834	1.9459101490553132
2835	1.9459101490553132
2836	1.9459101490553132
2837	1.9459101490553132
2838	1.9459101490553132
2839	1.9459101490553132
2840	1.9459101490553132
2841	1.9459101490553132
2842	1.9459101490553132
2843	1.4350845252893227
2844	1.9459101490553132
2845	1.9459101490553132
2846	1.0986122886681098
2847	1.9459101490553132
2848	1.4350845252893227
2849	1.4350845252893227
2850	1.9459101490553132
2851	1.9459101490553132
2852	1.9459101490553132
2853	1.9459101490553132
2854	1.9459101490553132
2855	1.9459101490553132
2856	1.9459101490553132
2857	1.9459101490553132
2858	1.9459101490553132
2859	1.9459101490553132
2860	1.4350845252893227
2861	1.4350845252893227
2862	1.9459101490553132
2863	1.9459101490553132
2864	1.9459101490553132
2865	1.9459101490553132
2866	1.9459101490553132
2867	1.9459101490553132
2868	1.9459101490553132
2869	1.9459101490553132
2870	1.9459101490553132
2871	1.4350845252893227
2872	1.9459101490553132
2873	1.9459101490553132
2874	0.84729786038720367
2875	1.9459101490553132
2876	1.9459101490553132
2877	1.4350845252893227
2878	1.9459101490553132
2879	1.9459101490553132
2893	1.9459101490553132
2894	1.9459101490553132
2895	1.9459101490553132
2896	1.9459101490553132
2897	1.9459101490553132
2898	1.9459101490553132
2899	1.4350845252893227
2900	1.9459101490553132
2901	1.9459101490553132
2902	1.9459101490553132
2903	1.9459101490553132
2904	1.9459101490553132
2905	1.4350845252893227
2906	1.9459101490553132
2930	1.9459101490553132
2931	1.9459101490553132
2932	1.9459101490553132
2933	1.0986122886681098
2934	1.9459101490553132
2935	1.9459101490553132
2936	1.9459101490553132
2937	1.4350845252893227
2938	1.9459101490553132
2939	1.0986122886681098
2940	1.9459101490553132
2941	1.9459101490553132
2942	1.9459101490553132
2943	1.9459101490553132
2944	1.4350845252893227
2945	1.9459101490553132
2946	1.4350845252893227
2947	1.9459101490553132
2948	1.9459101490553132
2949	1.4350845252893227
2950	0.84729786038720367
2951	1.9459101490553132
2952	1.9459101490553132
2953	1.9459101490553132
2954	1.9459101490553132
2955	1.0986122886681098
2956	1.9459101490553132
2957	1.9459101490553132
2958	1.9459101490553132
2959	1.9459101490553132
2960	1.0986122886681098
2961	1.9459101490553132
2962	1.4350845252893227
2963	1.9459101490553132
2964	1.9459101490553132
2965	1.9459101490553132
2966	1.9459101490553132
2967	1.9459101490553132
2968	1.9459101490553132
2969	1.9459101490553132
2970	1.9459101490553132
2971	1.4350845252893227
2972	1.9459101490553132
2973	1.9459101490553132
2974	1.9459101490553132
2975	1.9459101490553132
2976	1.4350845252893227
2977	1.9459101490553132
2978	1.9459101490553132
2979	1.9459101490553132
2980	1.9459101490553132
2981	1.9459101490553132
2982	1.9459101490553132
2983	1.9459101490553132
2984	1.9459101490553132
2985	1.9459101490553132
2986	1.9459101490553132
2987	1.9459101490553132
2988	1.9459101490553132
2989	1.9459101490553132
2990	1.9459101490553132
2991	1.9459101490553132
2992	1.9459101490553132
2993	1.9459101490553132
2994	1.9459101490553132
2995	1.9459101490553132
2996	1.9459101490553132
2997	1.9459101490553132
2998	1.9459101490553132
2999	1.9459101490553132
3000	1.9459101490553132
3001	1.9459101490553132
3002	1.9459101490553132
3003	1.9459101490553132
3004	1.9459101490553132
3005	1.9459101490553132
3006	1.9459101490553132
3007	1.9459101490553132
3008	1.4350845252893227
3009	1.9459101490553132
3010	1.4350845252893227
3011	1.9459101490553132
3012	1.9459101490553132
3013	1.9459101490553132
3014	1.9459101490553132
3015	1.9459101490553132
3016	1.9459101490553132
3017	1.9459101490553132
3018	1.4350845252893227
3019	1.9459101490553132
3020	1.9459101490553132
3021	1.9459101490553132
3022	1.9459101490553132
3023	1.9459101490553132
3024	1.9459101490553132
3025	1.9459101490553132
3026	0.84729786038720367
3027	0.84729786038720367
3028	1.9459101490553132
3029	0.84729786038720367
3030	1.9459101490553132
3031	1.4350845252893227
3032	1.4350845252893227
3033	1.9459101490553132
3034	1.9459101490553132
3035	1.9459101490553132
3036	1.9459101490553132
3037	1.9459101490553132
3038	1.9459101490553132
3039	1.9459101490553132
3040	1.9459101490553132
3041	0.84729786038720367
3042	1.9459101490553132
3043	1.9459101490553132
3044	1.9459101490553132
3045	0.84729786038720367
3046	1.9459101490553132
3047	1.4350845252893227
3048	1.4350845252893227
3049	1.4350845252893227
3050	1.4350845252893227
3051	1.9459101490553132
3052	1.9459101490553132
3053	1.9459101490553132
3054	1.9459101490553132
3055	1.4350845252893227
3056	1.9459101490553132
3057	1.9459101490553132
3058	1.9459101490553132
3059	1.9459101490553132
3060	1.9459101490553132
3061	1.0986122886681098
3062	1.9459101490553132
3063	1.9459101490553132
3064	1.9459101490553132
3065	1.9459101490553132
3066	1.9459101490553132
3067	1.9459101490553132
3068	1.9459101490553132
3069	1.9459101490553132
3070	1.4350845252893227
3071	1.9459101490553132
3072	1.9459101490553132
3073	1.9459101490553132
3074	1.9459101490553132
3075	1.9459101490553132
3076	1.9459101490553132
3077	0.84729786038720367
3078	1.0986122886681098
3079	1.9459101490553132
3080	1.9459101490553132
3081	1.0986122886681098
3082	1.9459101490553132
3083	1.0986122886681098
3084	1.9459101490553132
3085	1.9459101490553132
3086	1.0986122886681098
3087	1.9459101490553132
3088	1.9459101490553132
3089	1.9459101490553132
3090	1.9459101490553132
3091	1.9459101490553132
3092	1.9459101490553132
3093	1.9459101490553132
3094	1.9459101490553132
3095	1.9459101490553132
3096	1.9459101490553132
3097	1.9459101490553132
3098	1.9459101490553132
3099	1.4350845252893227
3100	1.9459101490553132
3101	1.9459101490553132
3102	1.9459101490553132
3103	1.9459101490553132
3104	1.9459101490553132
3105	1.9459101490553132
3106	1.9459101490553132
3107	1.9459101490553132
3108	1.9459101490553132
3109	1.9459101490553132
3110	1.9459101490553132
3111	1.9459101490553132
3112	1.9459101490553132
3113	1.9459101490553132
3114	1.9459101490553132
3115	0.84729786038720367
3116	1.9459101490553132
3117	1.9459101490553132
3118	1.4350845252893227
3119	1.9459101490553132
3120	1.0986122886681098
3121	1.9459101490553132
3122	1.9459101490553132
3123	1.0986122886681098
3124	1.9459101490553132
3125	1.0986122886681098
3126	1.9459101490553132
3127	1.9459101490553132
3128	1.0986122886681098
3129	1.4350845252893227
3130	1.4350845252893227
3131	1.9459101490553132
3132	1.9459101490553132
3133	1.9459101490553132
3134	1.0986122886681098
3135	1.9459101490553132
3136	1.9459101490553132
3137	1.0986122886681098
3138	1.9459101490553132
3139	1.9459101490553132
3140	1.9459101490553132
3141	1.9459101490553132
3142	1.9459101490553132
3143	1.9459101490553132
3144	1.9459101490553132
3145	1.9459101490553132
3146	1.9459101490553132
3147	1.9459101490553132
3148	1.9459101490553132
3149	1.9459101490553132
3150	1.9459101490553132
3151	1.9459101490553132
3152	1.9459101490553132
3153	1.9459101490553132
3154	1.9459101490553132
3155	1.9459101490553132
3156	1.9459101490553132
3157	1.9459101490553132
3158	1.9459101490553132
3159	1.9459101490553132
3160	1.9459101490553132
3161	1.9459101490553132
3162	1.9459101490553132
3163	1.9459101490553132
3164	1.9459101490553132
3165	1.9459101490553132
3166	1.9459101490553132
3167	1.0986122886681098
3168	1.0986122886681098
3169	1.9459101490553132
3170	1.4350845252893227
3171	1.4350845252893227
3172	1.4350845252893227
3173	1.9459101490553132
3174	1.9459101490553132
3175	1.9459101490553132
3176	1.9459101490553132
3177	1.4350845252893227
3178	1.9459101490553132
3179	1.4350845252893227
3180	1.9459101490553132
3181	1.0986122886681098
3182	1.9459101490553132
3183	1.9459101490553132
3184	1.9459101490553132
3185	1.9459101490553132
3186	1.9459101490553132
3187	1.4350845252893227
3188	1.9459101490553132
3189	1.4350845252893227
3190	1.9459101490553132
3191	1.9459101490553132
3192	1.9459101490553132
3193	1.9459101490553132
3194	1.9459101490553132
3195	1.9459101490553132
3196	1.9459101490553132
3197	1.9459101490553132
3198	1.9459101490553132
3199	1.9459101490553132
3200	1.9459101490553132
3201	1.9459101490553132
3202	1.9459101490553132
3203	1.9459101490553132
3204	1.9459101490553132
3205	1.9459101490553132
3206	1.9459101490553132
3207	1.9459101490553132
3208	1.4350845252893227
3209	1.9459101490553132
3210	1.9459101490553132
3211	1.9459101490553132
3212	1.9459101490553132
3213	1.9459101490553132
3214	1.9459101490553132
3215	1.9459101490553132
3216	1.9459101490553132
3217	1.9459101490553132
3218	1.9459101490553132
3219	1.9459101490553132
3220	1.9459101490553132
3221	1.9459101490553132
3222	1.9459101490553132
3223	1.9459101490553132
3224	1.9459101490553132
3225	1.9459101490553132
3226	1.9459101490553132
3227	1.9459101490553132
3228	1.4350845252893227
3229	1.4350845252893227
3230	1.9459101490553132
3231	1.9459101490553132
3232	1.9459101490553132
3233	1.9459101490553132
3234	1.9459101490553132
3235	1.9459101490553132
3236	1.9459101490553132
3237	1.9459101490553132
3238	1.9459101490553132
3239	1.9459101490553132
3240	1.9459101490553132
3241	1.9459101490553132
3242	1.4350845252893227
3243	1.9459101490553132
3244	1.9459101490553132
3245	1.9459101490553132
3246	1.4350845252893227
3247	1.9459101490553132
3248	1.9459101490553132
3249	1.9459101490553132
3250	1.9459101490553132
3251	1.9459101490553132
3252	1.9459101490553132
3253	1.9459101490553132
3254	1.9459101490553132
3255	1.9459101490553132
3256	1.9459101490553132
3257	1.4350845252893227
3258	1.9459101490553132
3259	1.9459101490553132
3260	1.9459101490553132
3261	1.9459101490553132
3262	1.9459101490553132
3263	1.9459101490553132
3264	1.9459101490553132
3265	1.4350845252893227
3266	1.9459101490553132
3267	1.9459101490553132
3268	1.9459101490553132
3269	1.9459101490553132
3270	1.9459101490553132
3271	1.9459101490553132
3272	1.9459101490553132
3273	1.9459101490553132
3274	1.4350845252893227
3275	1.9459101490553132
3276	1.9459101490553132
3277	1.9459101490553132
3278	1.9459101490553132
3279	1.9459101490553132
3280	1.9459101490553132
3281	1.9459101490553132
3282	1.9459101490553132
3283	1.9459101490553132
3284	1.9459101490553132
3285	1.9459101490553132
3286	1.9459101490553132
3287	1.9459101490553132
3288	1.9459101490553132
3289	1.9459101490553132
3290	1.9459101490553132
3291	1.9459101490553132
3292	1.9459101490553132
3293	1.9459101490553132
3294	1.9459101490553132
3295	1.9459101490553132
3296	1.9459101490553132
3297	1.9459101490553132
3298	1.9459101490553132
3299	1.9459101490553132
3300	1.9459101490553132
3301	1.9459101490553132
3302	1.9459101490553132
3303	1.9459101490553132
3304	1.4350845252893227
3305	1.9459101490553132
3306	1.4350845252893227
3307	1.9459101490553132
3308	1.9459101490553132
3309	1.9459101490553132
3310	1.9459101490553132
3311	1.9459101490553132
3312	1.9459101490553132
3313	1.9459101490553132
3314	1.9459101490553132
3315	1.9459101490553132
3316	1.9459101490553132
3317	1.9459101490553132
3318	1.9459101490553132
3319	1.9459101490553132
3320	1.9459101490553132
3321	1.9459101490553132
3322	1.9459101490553132
3323	1.4350845252893227
3324	1.9459101490553132
3325	1.9459101490553132
3326	1.9459101490553132
3327	1.9459101490553132
3328	1.9459101490553132
3329	1.9459101490553132
3330	1.9459101490553132
3331	1.4350845252893227
3332	1.4350845252893227
3333	1.4350845252893227
3334	1.9459101490553132
3335	1.9459101490553132
3336	1.9459101490553132
3337	1.9459101490553132
3338	1.4350845252893227
3339	1.4350845252893227
3340	1.9459101490553132
3341	1.9459101490553132
3342	1.9459101490553132
3343	1.9459101490553132
3344	1.9459101490553132
3345	1.9459101490553132
3346	1.9459101490553132
3347	1.9459101490553132
3348	1.4350845252893227
3349	1.9459101490553132
3350	1.9459101490553132
3351	1.9459101490553132
3352	1.9459101490553132
3353	1.9459101490553132
3354	1.9459101490553132
3355	1.9459101490553132
3356	1.9459101490553132
3357	1.9459101490553132
3358	1.9459101490553132
3359	1.9459101490553132
3360	1.9459101490553132
3361	1.9459101490553132
3362	1.9459101490553132
3363	1.9459101490553132
3364	1.9459101490553132
3365	1.4350845252893227
3366	1.9459101490553132
3367	1.9459101490553132
3368	1.9459101490553132
3369	1.9459101490553132
3370	1.9459101490553132
3371	1.9459101490553132
3372	1.9459101490553132
3373	1.9459101490553132
3374	1.9459101490553132
3375	1.9459101490553132
3376	1.9459101490553132
3377	1.9459101490553132
3378	1.9459101490553132
3379	1.9459101490553132
3380	1.0986122886681098
3381	1.9459101490553132
3382	1.9459101490553132
3383	0.84729786038720367
3384	1.9459101490553132
3385	1.9459101490553132
3386	1.9459101490553132
3387	1.9459101490553132
3388	1.9459101490553132
3389	1.9459101490553132
3402	1.9459101490553132
3403	1.9459101490553132
3404	1.9459101490553132
3405	1.9459101490553132
3406	1.9459101490553132
3407	1.9459101490553132
3453	1.9459101490553132
3454	1.9459101490553132
3455	1.9459101490553132
3456	1.0986122886681098
3457	1.9459101490553132
3458	1.9459101490553132
3459	1.9459101490553132
3460	1.4350845252893227
3461	1.9459101490553132
3462	1.9459101490553132
3463	1.9459101490553132
3464	1.0986122886681098
3465	1.9459101490553132
3466	1.9459101490553132
3467	1.9459101490553132
3468	1.9459101490553132
3469	1.9459101490553132
3470	1.4350845252893227
3471	1.9459101490553132
3472	1.9459101490553132
3473	1.9459101490553132
3474	1.9459101490553132
3475	1.9459101490553132
3476	1.0986122886681098
3477	1.4350845252893227
3478	1.9459101490553132
3479	1.0986122886681098
3480	1.9459101490553132
3481	1.9459101490553132
3482	1.9459101490553132
3483	1.0986122886681098
3484	1.4350845252893227
3485	1.9459101490553132
3486	1.9459101490553132
3487	1.9459101490553132
3488	1.9459101490553132
3489	1.9459101490553132
3490	1.9459101490553132
3491	1.0986122886681098
3492	1.9459101490553132
3493	1.9459101490553132
3494	1.9459101490553132
3495	1.9459101490553132
3496	1.9459101490553132
3497	1.9459101490553132
3498	1.9459101490553132
3499	1.9459101490553132
3500	1.9459101490553132
3501	1.9459101490553132
3502	1.9459101490553132
3503	1.9459101490553132
3504	1.9459101490553132
3505	1.9459101490553132
3506	1.9459101490553132
3507	1.9459101490553132
3508	1.9459101490553132
3509	1.9459101490553132
3510	1.4350845252893227
3511	1.9459101490553132
3512	1.9459101490553132
3513	1.9459101490553132
3514	1.9459101490553132
3515	1.9459101490553132
3516	1.9459101490553132
3517	1.9459101490553132
3518	1.9459101490553132
3519	1.9459101490553132
3520	1.9459101490553132
3521	1.9459101490553132
3522	1.9459101490553132
3523	1.9459101490553132
3524	1.9459101490553132
3525	1.9459101490553132
3526	1.9459101490553132
3527	1.9459101490553132
3528	1.9459101490553132
3529	1.9459101490553132
3530	1.9459101490553132
3531	1.9459101490553132
3532	1.9459101490553132
3533	1.9459101490553132
3534	1.9459101490553132
3535	1.9459101490553132
3536	1.9459101490553132
3537	1.9459101490553132
3538	1.9459101490553132
3539	1.0986122886681098
3540	1.4350845252893227
3541	1.0986122886681098
3542	1.0986122886681098
3543	1.9459101490553132
3544	1.9459101490553132
3545	1.9459101490553132
3546	1.9459101490553132
3547	1.9459101490553132
3548	1.9459101490553132
3549	1.9459101490553132
3550	1.9459101490553132
3551	1.9459101490553132
3552	1.9459101490553132
3553	1.9459101490553132
3554	1.9459101490553132
3555	1.9459101490553132
3556	1.9459101490553132
3557	1.9459101490553132
3558	1.9459101490553132
3559	1.9459101490553132
3560	1.9459101490553132
3561	1.9459101490553132
3562	1.0986122886681098
3563	1.9459101490553132
3564	1.9459101490553132
3565	1.9459101490553132
3566	1.9459101490553132
3567	1.9459101490553132
3568	1.9459101490553132
3569	1.9459101490553132
3570	1.9459101490553132
3571	1.9459101490553132
3572	1.9459101490553132
3573	1.9459101490553132
3574	1.9459101490553132
3575	1.9459101490553132
3576	1.9459101490553132
3577	1.9459101490553132
3578	1.9459101490553132
3579	1.9459101490553132
3580	1.9459101490553132
3581	1.9459101490553132
3582	1.9459101490553132
3583	1.9459101490553132
3584	1.9459101490553132
3585	1.9459101490553132
3586	1.9459101490553132
3587	1.9459101490553132
3588	1.9459101490553132
3589	1.9459101490553132
3590	1.0986122886681098
3591	1.9459101490553132
3592	1.9459101490553132
3593	1.9459101490553132
3594	1.9459101490553132
3595	1.9459101490553132
3596	1.9459101490553132
3597	1.9459101490553132
3598	1.9459101490553132
3599	1.9459101490553132
3600	1.9459101490553132
3601	1.9459101490553132
3602	1.9459101490553132
3603	1.9459101490553132
3604	1.9459101490553132
3605	1.9459101490553132
3606	1.9459101490553132
3607	1.0986122886681098
3608	1.9459101490553132
3609	1.9459101490553132
3610	1.9459101490553132
3611	1.9459101490553132
3612	1.9459101490553132
3613	1.9459101490553132
3614	1.9459101490553132
3615	1.9459101490553132
3616	1.0986122886681098
3617	1.9459101490553132
3618	1.9459101490553132
3619	1.9459101490553132
3620	1.0986122886681098
3621	1.9459101490553132
3622	1.9459101490553132
3623	1.9459101490553132
3624	1.9459101490553132
3625	1.9459101490553132
3626	1.9459101490553132
3627	1.9459101490553132
3628	1.9459101490553132
3629	1.9459101490553132
3630	1.9459101490553132
3631	1.9459101490553132
3632	1.9459101490553132
3633	1.9459101490553132
3634	1.9459101490553132
3635	1.9459101490553132
3636	1.9459101490553132
3637	1.9459101490553132
3638	1.9459101490553132
3639	1.9459101490553132
3640	1.9459101490553132
3641	1.9459101490553132
3642	1.9459101490553132
3643	1.9459101490553132
3644	1.9459101490553132
3645	1.9459101490553132
3646	1.9459101490553132
3647	1.9459101490553132
3648	1.4350845252893227
3649	1.9459101490553132
3650	1.9459101490553132
3651	1.9459101490553132
3652	1.9459101490553132
3653	1.9459101490553132
3654	1.9459101490553132
3655	1.9459101490553132
3656	1.9459101490553132
3657	1.9459101490553132
3658	1.9459101490553132
3659	1.9459101490553132
3660	1.9459101490553132
3661	1.9459101490553132
3662	1.9459101490553132
3663	1.9459101490553132
3664	1.9459101490553132
3665	1.9459101490553132
3666	1.9459101490553132
3667	1.9459101490553132
3668	1.9459101490553132
3669	1.9459101490553132
3670	1.0986122886681098
3671	1.9459101490553132
3672	1.9459101490553132
3673	1.9459101490553132
3674	1.9459101490553132
3675	1.9459101490553132
3676	1.9459101490553132
3677	1.9459101490553132
3678	1.9459101490553132
3679	1.9459101490553132
3680	1.0986122886681098
3681	1.9459101490553132
3682	1.9459101490553132
3683	1.9459101490553132
3684	1.9459101490553132
3685	1.9459101490553132
3686	1.9459101490553132
3687	1.9459101490553132
3688	1.9459101490553132
3689	1.9459101490553132
3690	1.9459101490553132
3691	1.9459101490553132
3692	1.9459101490553132
3693	1.9459101490553132
3694	1.9459101490553132
3695	1.9459101490553132
3696	1.9459101490553132
3697	1.9459101490553132
3698	1.9459101490553132
3699	1.9459101490553132
3700	1.9459101490553132
3701	1.9459101490553132
3702	1.9459101490553132
3703	1.9459101490553132
3704	1.9459101490553132
3705	1.9459101490553132
3706	1.9459101490553132
3707	1.9459101490553132
3708	1.9459101490553132
3709	1.9459101490553132
3710	1.9459101490553132
3711	1.9459101490553132
3712	1.9459101490553132
3713	1.9459101490553132
3714	1.9459101490553132
3715	1.9459101490553132
3716	1.9459101490553132
3717	1.9459101490553132
3718	1.9459101490553132
3719	1.9459101490553132
3720	1.9459101490553132
3721	1.9459101490553132
3722	1.9459101490553132
3723	1.4350845252893227
3724	0.84729786038720367
3725	1.9459101490553132
3726	1.9459101490553132
3727	1.4350845252893227
3728	1.9459101490553132
3729	1.9459101490553132
3730	1.9459101490553132
3731	1.9459101490553132
3753	1.4350845252893227
3754	1.4350845252893227
3755	1.4350845252893227
3756	1.4350845252893227
3757	1.9459101490553132
3758	1.4350845252893227
3759	1.4350845252893227
3760	1.4350845252893227
3761	1.4350845252893227
3762	1.4350845252893227
3763	1.4350845252893227
3764	1.4350845252893227
3785	1.4350845252893227
3786	1.4350845252893227
3787	1.4350845252893227
3788	1.4350845252893227
3789	1.4350845252893227
3790	1.4350845252893227
3791	1.4350845252893227
3792	1.0986122886681098
3793	1.4350845252893227
3794	1.4350845252893227
3795	1.4350845252893227
3796	1.4350845252893227
3797	1.4350845252893227
3798	1.4350845252893227
3799	1.4350845252893227
3800	1.4350845252893227
3801	1.4350845252893227
3802	1.4350845252893227
3803	1.4350845252893227
3804	1.4350845252893227
3805	1.4350845252893227
3806	1.9459101490553132
3807	1.9459101490553132
3808	1.9459101490553132
3809	1.4350845252893227
3810	1.4350845252893227
3811	1.4350845252893227
3812	1.4350845252893227
3813	1.9459101490553132
3814	1.9459101490553132
3815	1.4350845252893227
3816	1.4350845252893227
3817	1.4350845252893227
3818	1.4350845252893227
3819	1.4350845252893227
3820	1.4350845252893227
3821	1.4350845252893227
3822	1.4350845252893227
3823	1.4350845252893227
3824	1.4350845252893227
3825	1.4350845252893227
3826	1.4350845252893227
3827	1.4350845252893227
3828	1.4350845252893227
3829	1.4350845252893227
3830	1.4350845252893227
3831	1.4350845252893227
3832	1.4350845252893227
3833	1.4350845252893227
3834	1.4350845252893227
3835	1.4350845252893227
3836	1.4350845252893227
3837	1.4350845252893227
3838	1.4350845252893227
3839	1.4350845252893227
3840	1.4350845252893227
3841	1.4350845252893227
3842	1.4350845252893227
3843	1.4350845252893227
3844	1.4350845252893227
3845	1.4350845252893227
3846	1.4350845252893227
3847	1.0986122886681098
3848	1.4350845252893227
3849	1.0986122886681098
3850	1.4350845252893227
3851	1.4350845252893227
3852	1.4350845252893227
3853	1.0986122886681098
3854	1.4350845252893227
3855	1.4350845252893227
3856	1.4350845252893227
3857	1.4350845252893227
3858	1.4350845252893227
3859	1.4350845252893227
3860	1.4350845252893227
3861	1.4350845252893227
3862	1.4350845252893227
3863	1.4350845252893227
3864	1.9459101490553132
3865	1.9459101490553132
3866	1.9459101490553132
3867	1.9459101490553132
3868	1.9459101490553132
3869	1.9459101490553132
3870	1.9459101490553132
3871	1.9459101490553132
3872	1.9459101490553132
3873	1.9459101490553132
3874	1.9459101490553132
3875	1.9459101490553132
3876	1.9459101490553132
3877	1.9459101490553132
3878	1.9459101490553132
3879	1.9459101490553132
3880	1.9459101490553132
3881	1.9459101490553132
3882	1.9459101490553132
3883	1.9459101490553132
3884	1.9459101490553132
3885	1.9459101490553132
3886	1.9459101490553132
3887	1.9459101490553132
3888	1.9459101490553132
3889	1.9459101490553132
3890	1.9459101490553132
3891	1.9459101490553132
3892	1.9459101490553132
3893	1.9459101490553132
3894	1.9459101490553132
3895	1.9459101490553132
3896	1.9459101490553132
3897	1.9459101490553132
3898	1.9459101490553132
3899	1.9459101490553132
3900	1.9459101490553132
3901	1.9459101490553132
3902	1.9459101490553132
3903	1.9459101490553132
3904	1.9459101490553132
3905	1.9459101490553132
3906	1.9459101490553132
3907	1.9459101490553132
3908	1.9459101490553132
3909	1.9459101490553132
3910	1.9459101490553132
3911	1.9459101490553132
3912	1.9459101490553132
3913	1.9459101490553132
3914	1.9459101490553132
3915	1.9459101490553132
3916	1.9459101490553132
3917	1.9459101490553132
3918	1.9459101490553132
3919	1.9459101490553132
3920	1.9459101490553132
3921	1.9459101490553132
3922	1.9459101490553132
3923	1.9459101490553132
3924	1.9459101490553132
3925	1.9459101490553132
3926	1.9459101490553132
3927	1.9459101490553132
3928	1.9459101490553132
3929	1.9459101490553132
3930	1.9459101490553132
3931	1.9459101490553132
3932	1.9459101490553132
3933	1.9459101490553132
3934	1.9459101490553132
3935	1.9459101490553132
3936	1.9459101490553132
3937	1.9459101490553132
3938	1.9459101490553132
3939	1.9459101490553132
3940	1.9459101490553132
3941	1.9459101490553132
3942	1.9459101490553132
3943	1.9459101490553132
3944	1.9459101490553132
3945	1.9459101490553132
3946	1.9459101490553132
3947	1.9459101490553132
3948	1.9459101490553132
3949	1.9459101490553132
3950	1.9459101490553132
3951	1.4350845252893227
3952	1.9459101490553132
3953	1.9459101490553132
3954	1.9459101490553132
3955	1.9459101490553132
3956	1.9459101490553132
3957	1.9459101490553132
3958	1.9459101490553132
3959	1.9459101490553132
3960	1.9459101490553132
3961	1.9459101490553132
3962	1.9459101490553132
3963	1.9459101490553132
3964	1.9459101490553132
3965	1.9459101490553132
3966	1.9459101490553132
3967	1.9459101490553132
3968	1.9459101490553132
3969	1.9459101490553132
3970	1.9459101490553132
3971	1.9459101490553132
3972	1.9459101490553132
3973	1.9459101490553132
3974	1.9459101490553132
3975	1.9459101490553132
3976	1.9459101490553132
3977	1.9459101490553132
3987	1.9459101490553132
3995	1.9459101490553132
3996	1.9459101490553132
3999	1.9459101490553132
4000	1.9459101490553132
4001	1.9459101490553132
4002	1.9459101490553132
4003	1.9459101490553132
4004	1.9459101490553132
4005	1.9459101490553132
4006	1.9459101490553132
4016	1.9459101490553132
4017	1.9459101490553132
4018	1.9459101490553132
4019	1.9459101490553132
4020	1.9459101490553132
4021	1.9459101490553132
4074	1.9459101490553132
4075	1.9459101490553132
4076	1.9459101490553132
4077	1.9459101490553132
4078	1.9459101490553132
4079	1.9459101490553132
4080	1.9459101490553132
4081	1.9459101490553132
4082	1.9459101490553132
4083	1.9459101490553132
4084	1.9459101490553132
4085	1.9459101490553132
4086	1.9459101490553132
4087	1.9459101490553132
4088	1.9459101490553132
4089	1.9459101490553132
4090	1.9459101490553132
4091	1.9459101490553132
4092	1.9459101490553132
4093	1.9459101490553132
4094	1.9459101490553132
4095	1.9459101490553132
4096	1.9459101490553132
4097	1.9459101490553132
4098	1.9459101490553132
4099	1.9459101490553132
4100	1.9459101490553132
4101	1.9459101490553132
4102	1.9459101490553132
4103	1.9459101490553132
4104	1.9459101490553132
4105	1.9459101490553132
4106	1.9459101490553132
4107	1.9459101490553132
4108	1.9459101490553132
4109	1.9459101490553132
4110	1.9459101490553132
4111	1.9459101490553132
4112	1.9459101490553132
4113	1.9459101490553132
4114	1.9459101490553132
4115	1.9459101490553132
4116	1.9459101490553132
4117	1.9459101490553132
4118	1.9459101490553132
4119	1.9459101490553132
4120	1.9459101490553132
4121	1.9459101490553132
4122	1.9459101490553132
4123	1.9459101490553132
4124	1.9459101490553132
4125	1.9459101490553132
4126	1.9459101490553132
4127	1.9459101490553132
4128	1.9459101490553132
4129	1.9459101490553132
4130	1.9459101490553132
4131	1.9459101490553132
4132	1.9459101490553132
4133	1.9459101490553132
4134	1.9459101490553132
4135	1.9459101490553132
4136	1.9459101490553132
4137	1.9459101490553132
4138	1.9459101490553132
4139	1.9459101490553132
4140	1.9459101490553132
4141	1.9459101490553132
4142	1.9459101490553132
4143	1.9459101490553132
4144	1.9459101490553132
4145	1.9459101490553132
4146	1.9459101490553132
4147	1.9459101490553132
4148	1.9459101490553132
4149	1.9459101490553132
4150	1.9459101490553132
4151	1.9459101490553132
4152	1.9459101490553132
4153	1.9459101490553132
4154	1.9459101490553132
4155	1.9459101490553132
4156	1.9459101490553132
4157	1.9459101490553132
4158	1.9459101490553132
4159	1.9459101490553132
4160	1.9459101490553132
4161	1.9459101490553132
4162	1.9459101490553132
4163	1.9459101490553132
4164	1.9459101490553132
4165	1.9459101490553132
4166	1.9459101490553132
4167	1.9459101490553132
4168	1.9459101490553132
4169	1.9459101490553132
4170	1.9459101490553132
4171	1.9459101490553132
4172	1.9459101490553132
4173	1.9459101490553132
4174	1.9459101490553132
4175	1.9459101490553132
4176	1.9459101490553132
4177	1.9459101490553132
4178	1.9459101490553132
4179	1.9459101490553132
4180	1.9459101490553132
4181	1.9459101490553132
4182	1.9459101490553132
4183	1.9459101490553132
4184	1.9459101490553132
4185	1.9459101490553132
4186	1.9459101490553132
4187	1.9459101490553132
4188	1.9459101490553132
4189	1.9459101490553132
4190	1.9459101490553132
4191	1.9459101490553132
4192	1.9459101490553132
4193	1.9459101490553132
4194	1.9459101490553132
4195	1.9459101490553132
4196	1.9459101490553132
4197	1.9459101490553132
4198	1.9459101490553132
4199	1.9459101490553132
4200	1.9459101490553132
4201	1.9459101490553132
4202	1.9459101490553132
4203	1.9459101490553132
4204	1.9459101490553132
4205	1.9459101490553132
4206	1.9459101490553132
4207	1.9459101490553132
4208	1.9459101490553132
4209	1.9459101490553132
4210	1.9459101490553132
4211	1.9459101490553132
4212	1.9459101490553132
4213	1.9459101490553132
4214	1.9459101490553132
4215	1.9459101490553132
4216	1.9459101490553132
4217	1.9459101490553132
4218	1.9459101490553132
4219	1.9459101490553132
4220	1.9459101490553132
4221	1.9459101490553132
4222	1.9459101490553132
4223	1.9459101490553132
4224	1.9459101490553132
4225	1.9459101490553132
4226	1.9459101490553132
4227	1.9459101490553132
4228	1.9459101490553132
4229	1.9459101490553132
4230	1.9459101490553132
4231	1.9459101490553132
4232	1.9459101490553132
4233	1.9459101490553132
4234	1.9459101490553132
4235	1.9459101490553132
4236	1.9459101490553132
4237	1.9459101490553132
4238	1.9459101490553132
4239	1.9459101490553132
4240	1.9459101490553132
4241	1.9459101490553132
4242	1.9459101490553132
4243	1.9459101490553132
4244	1.9459101490553132
4245	1.9459101490553132
4246	1.9459101490553132
4247	1.9459101490553132
4248	1.9459101490553132
4249	1.9459101490553132
4250	1.9459101490553132
4251	1.9459101490553132
4252	1.9459101490553132
4253	1.9459101490553132
4254	1.9459101490553132
4255	1.9459101490553132
4256	1.9459101490553132
4257	1.9459101490553132
4258	1.9459101490553132
4259	1.9459101490553132
4260	1.9459101490553132
4261	1.9459101490553132
4262	1.9459101490553132
4263	1.9459101490553132
4264	1.9459101490553132
4265	1.9459101490553132
4266	1.9459101490553132
4267	1.9459101490553132
4268	1.9459101490553132
4269	1.9459101490553132
4270	1.9459101490553132
4271	1.9459101490553132
4272	1.9459101490553132
4273	1.9459101490553132
4274	1.9459101490553132
4275	1.9459101490553132
4276	1.9459101490553132
4277	1.9459101490553132
4278	1.9459101490553132
4279	1.9459101490553132
4280	1.9459101490553132
4281	1.9459101490553132
4282	1.9459101490553132
4283	1.9459101490553132
4284	1.9459101490553132
4285	1.9459101490553132
4286	1.9459101490553132
4287	1.9459101490553132
4288	1.9459101490553132
4289	1.9459101490553132
4290	1.9459101490553132
4291	1.9459101490553132
4292	1.9459101490553132
4293	1.9459101490553132
4294	1.9459101490553132
4295	1.9459101490553132
4296	1.9459101490553132
4297	1.9459101490553132
4298	1.9459101490553132
4299	1.9459101490553132
SET SCHEMA "sys";
ALTER TABLE "doc_doc" ADD CONSTRAINT "doc_doc_docid1_fkey" FOREIGN KEY ("docid1") REFERENCES "docdict" ("docid");
ALTER TABLE "doc_doc" ADD CONSTRAINT "doc_doc_docid2_fkey" FOREIGN KEY ("docid2") REFERENCES "docdict" ("docid");
ALTER TABLE "doc_string" ADD CONSTRAINT "doc_string_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid");
ALTER TABLE "ne_doc" ADD CONSTRAINT "ne_doc_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid");
ALTER TABLE "ne_doc" ADD CONSTRAINT "ne_doc_neid_fkey" FOREIGN KEY ("neid") REFERENCES "nedict" ("neid");
ALTER TABLE "ne_ne" ADD CONSTRAINT "ne_ne_neid1_fkey" FOREIGN KEY ("neid1") REFERENCES "nedict" ("neid");
ALTER TABLE "ne_ne" ADD CONSTRAINT "ne_ne_neid2_fkey" FOREIGN KEY ("neid2") REFERENCES "nedict" ("neid");
ALTER TABLE "ne_string" ADD CONSTRAINT "ne_string_neid_fkey" FOREIGN KEY ("neid") REFERENCES "nedict" ("neid");
COMMIT;
