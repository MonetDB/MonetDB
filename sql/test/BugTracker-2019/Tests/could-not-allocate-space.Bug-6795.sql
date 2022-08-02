START TRANSACTION;

CREATE TABLE ingestion (
	ingestion_id 		INTEGER NOT NULL,
	sourcepath  		VARCHAR(1024),
	destinationpath		VARCHAR(1024),
	sourcereference     VARCHAR(4096),
	operator 		VARCHAR(64),
	tilesizerow		INTEGER,
	tilesizecolumn		INTEGER,
	gridlevels		INTEGER,
	multitemporal		boolean,
	overlaprows		INTEGER,
	overlapcolumns		INTEGER,
	timeStartUTC		VARCHAR(30),
	timeStoptUTC		VARCHAR(30),
	"status"			INTEGER,
	CONSTRAINT ingestion_id_pkey PRIMARY KEY (ingestion_id)
);

COPY 22 RECORDS INTO "sys"."ingestion" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1	"/usr/src/S_product/GRANULE/L1C_T31TCJ_A012232_20190710T105335"	"https://platform.candela-h2020.eu/rest/img/L1C_T31TCJ_A012232_20190710T105335/L1C_T31TCJ_A012232_20190710T105335"	"/usr/src/dmg/L1C_T31TCJ_A012232_20190710T105335/L1C_T31TCJ_A012232_20190710T105335"	"test"	120	120	1	false	120	120	"2019-11-05T11:03:51.000Z"	"2019-11-05T11:06:58.000Z"	2
2	"/usr/src/S_product/GRANULE/L1C_T33UVR_A021326_20190723T101347"	"https://platform.candela-h2020.eu/rest/img/L1C_T33UVR_A021326_20190723T101347/L1C_T33UVR_A021326_20190723T101347"	"/usr/src/dmg/L1C_T33UVR_A021326_20190723T101347/L1C_T33UVR_A021326_20190723T101347"	"test"	120	120	1	false	120	120	"2019-11-05T11:13:26.000Z"	"2019-11-05T11:16:58.000Z"	2
4	"/usr/src/S_product/GRANULE/L1C_T32TMT_A012432_20190724T103030"	"https://platform.candela-h2020.eu/rest/img/L1C_T32TMT_A012432_20190724T103030/L1C_T32TMT_A012432_20190724T103030"	"/usr/src/dmg/L1C_T32TMT_A012432_20190724T103030/L1C_T32TMT_A012432_20190724T103030"	"test"	120	120	1	false	120	120	"2019-11-05T11:13:40.000Z"	"2019-11-05T11:17:13.000Z"	2
5	"/usr/src/S_product/GRANULE/L1C_T31UES_A021784_20190824T105344"	"https://platform.candela-h2020.eu/rest/img/L1C_T31UES_A021784_20190824T105344/L1C_T31UES_A021784_20190824T105344"	"/usr/src/dmg/L1C_T31UES_A021784_20190824T105344/L1C_T31UES_A021784_20190824T105344"	"test"	120	120	1	false	120	120	"2019-11-05T11:13:47.000Z"	"2019-11-05T11:17:26.000Z"	2
8	"/usr/src/S_product/GRANULE/L1C_T32UPU_A012961_20190830T102552"	"https://platform.candela-h2020.eu/rest/img/L1C_T32UPU_A012961_20190830T102552/L1C_T32UPU_A012961_20190830T102552"	"/usr/src/dmg/L1C_T32UPU_A012961_20190830T102552/L1C_T32UPU_A012961_20190830T102552"	"test"	120	120	1	false	120	120	"2019-11-05T11:13:57.000Z"	"2019-11-05T11:17:37.000Z"	2
10	"/usr/src/S_product/GRANULE/L1C_T31UFU_A021784_20190824T105344"	"https://platform.candela-h2020.eu/rest/img/L1C_T31UFU_A021784_20190824T105344/L1C_T31UFU_A021784_20190824T105344"	"/usr/src/dmg/L1C_T31UFU_A021784_20190824T105344/L1C_T31UFU_A021784_20190824T105344"	"test"	120	120	1	false	120	120	"2019-11-05T11:12:34.000Z"	"2019-11-05T11:17:34.000Z"	2
11	"/usr/src/S1B_IW_GRDH_1SDV_20190718T060632_20190718T060657_017184_020521_F49E"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190718T060632_20190718T060657_017184_020521_F49E"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190718T060632_20190718T060657_017184_020521_F49E"	"test"	120	120	1	false	120	120	"2019-11-05T11:14:29.000Z"	"2019-11-05T11:40:00.000Z"	2
12	"/usr/src/S1B_IW_GRDH_1SDV_20190713T055956_20190713T060021_017111_020314_080A"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190713T055956_20190713T060021_017111_020314_080A"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190713T055956_20190713T060021_017111_020314_080A"	"test"	120	120	1	false	120	120	"2019-11-05T11:14:48.000Z"	"2019-11-05T11:40:33.000Z"	2
13	"/usr/src/S1B_IW_GRDH_1SDV_20190712T051713_20190712T051738_017096_0202A5_C8D8"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190712T051713_20190712T051738_017096_0202A5_C8D8"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190712T051713_20190712T051738_017096_0202A5_C8D8"	"test"	120	120	1	false	120	120	"2019-11-05T11:14:45.000Z"	"2019-11-05T11:40:56.000Z"	2
14	"/usr/src/S1B_IW_GRDH_1SDV_20190710T053414_20190710T053439_017067_0201C6_3EFC"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190710T053414_20190710T053439_017067_0201C6_3EFC"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190710T053414_20190710T053439_017067_0201C6_3EFC"	"test"	120	120	1	false	120	120	"2019-11-05T11:15:39.000Z"	"2019-11-05T11:41:38.000Z"	2
15	"/usr/src/S1A_IW_GRDH_1SDV_20190711T052618_20190711T052643_028065_032B64_C67A"	"https://platform.candela-h2020.eu/rest/img/S1A_IW_GRDH_1SDV_20190711T052618_20190711T052643_028065_032B64_C67A"	"/usr/src/dmg/S1A_IW_GRDH_1SDV_20190711T052618_20190711T052643_028065_032B64_C67A"	"test"	120	120	1	false	120	120	"2019-11-05T11:30:27.000Z"	"2019-11-05T11:58:51.000Z"	2
16	"/usr/src/S1A_IW_GRDH_1SDV_20190718T051950_20190718T052015_028167_032E7E_19D5"	"https://platform.candela-h2020.eu/rest/img/S1A_IW_GRDH_1SDV_20190718T051950_20190718T052015_028167_032E7E_19D5"	"/usr/src/dmg/S1A_IW_GRDH_1SDV_20190718T051950_20190718T052015_028167_032E7E_19D5"	"test"	120	120	1	false	120	120	"2019-11-05T11:12:52.000Z"	"2019-11-05T11:42:50.000Z"	2
17	"/usr/src/S1B_IW_GRDH_1SDV_20190710T164150_20190710T164215_017074_020201_F677"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190710T164150_20190710T164215_017074_020201_F677"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190710T164150_20190710T164215_017074_020201_F677"	"test"	120	120	1	false	120	120	"2019-11-05T11:13:40.000Z"	"2019-11-05T11:43:21.000Z"	2
18	"/usr/src/S1A_IW_GRDH_1SDV_20190710T062615_20190710T062640_028051_032AFB_9FAE"	"https://platform.candela-h2020.eu/rest/img/S1A_IW_GRDH_1SDV_20190710T062615_20190710T062640_028051_032AFB_9FAE"	"/usr/src/dmg/S1A_IW_GRDH_1SDV_20190710T062615_20190710T062640_028051_032AFB_9FAE"	"test"	120	120	1	false	120	120	"2019-11-05T19:43:58.000Z"	"2019-11-05T20:08:57.000Z"	2
19	"/usr/src/S1B_IW_GRDH_1SDV_20190711T172417_20190711T172442_017089_020272_4ABF"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190711T172417_20190711T172442_017089_020272_4ABF"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190711T172417_20190711T172442_017089_020272_4ABF"	"test"	120	120	1	false	120	120	"2019-11-05T19:44:09.000Z"	"2019-11-05T20:09:39.000Z"	2
20	"/usr/src/S_product/GRANULE/L1C_T31UDQ_A021355_20190725T105702"	"https://platform.candela-h2020.eu/rest/img/L1C_T31UDQ_A021355_20190725T105702/L1C_T31UDQ_A021355_20190725T105702"	"/usr/src/dmg/L1C_T31UDQ_A021355_20190725T105702/L1C_T31UDQ_A021355_20190725T105702"	"test"	120	120	1	false	120	120	"2019-11-26T16:16:50.000Z"	"2019-11-26T16:21:29.000Z"	2
21	"/usr/src/S_product/GRANULE/L1C_T31UDQ_A021355_20190725T105702"	"https://platform.candela-h2020.eu/rest/img/L1C_T31UDQ_A021355_20190725T105702/L1C_T31UDQ_A021355_20190725T105702"	"/usr/src/dmg/L1C_T31UDQ_A021355_20190725T105702/L1C_T31UDQ_A021355_20190725T105702"	"test"	120	120	1	false	120	120	"2019-11-27T08:35:36.000Z"	"2019-11-27T08:40:39.000Z"	2
22	"/eodata/Sentinel-1/SAR/GRD/2019/03/08/S1B_IW_GRDH_1SDV_20190308T171625_20190308T171650_015266_01C90B_2269.SAFE"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190308T171625_20190308T171650_015266_01C90B_2269.SAFE"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190308T171625_20190308T171650_015266_01C90B_2269.SAFE"	"test"	120	120	1	false	120	120	"2019-11-27T08:38:43.000Z"	"2019-11-27T09:08:34.000Z"	2
24	"/eodata/Sentinel-2/MSI/L1C/2016/10/15/S2A_MSIL1C_20161015T154222_N0204_R011_T18TWL_20161015T154519.SAFE/GRANULE/L1C_T18TWL_A006872_20161015T154519"	"https://platform.candela-h2020.eu/rest/img/L1C_T18TWL_A006872_20161015T154519/L1C_T18TWL_A006872_20161015T154519"	"/usr/src/dmg/L1C_T18TWL_A006872_20161015T154519/L1C_T18TWL_A006872_20161015T154519"	"test"	120	120	1	false	120	120	"2019-11-27T08:38:43.000Z"	"2019-11-27T09:12:52.000Z"	2
25	"/eodata/Sentinel-1/SAR/GRD/2019/03/08/S1B_IW_GRDH_1SDV_20190308T171625_20190308T171650_015266_01C90B_2269.SAFE"	"https://platform.candela-h2020.eu/rest/img/S1B_IW_GRDH_1SDV_20190308T171625_20190308T171650_015266_01C90B_2269.SAFE"	"/usr/src/dmg/S1B_IW_GRDH_1SDV_20190308T171625_20190308T171650_015266_01C90B_2269.SAFE"	"test"	120	120	1	false	120	120	"2019-11-27T12:20:36.000Z"	"2019-11-27T12:49:31.000Z"	2
26	"/eodata/Sentinel-2/MSI/L1C/2016/10/15/S2A_MSIL1C_20161015T154222_N0204_R011_T18TWL_20161015T154519.SAFE/GRANULE/L1C_T18TWL_A006872_20161015T154519"	"https://platform.candela-h2020.eu/rest/img/L1C_T18TWL_A006872_20161015T154519/L1C_T18TWL_A006872_20161015T154519"	"/usr/src/dmg/L1C_T18TWL_A006872_20161015T154519/L1C_T18TWL_A006872_20161015T154519"	"test"	120	120	1	false	120	120	"2019-11-27T12:20:36.000Z"	"2019-11-27T12:53:22.000Z"	2
27	"/usr/src/S_product/GRANULE/L1C_T30TYQ_A009529_20170419T110601"	"/usr/src/dmg/L1C_T30TYQ_A009529_20170419T110601/L1C_T30TYQ_A009529_20170419T110601"	"/usr/src/dmg/L1C_T30TYQ_A009529_20170419T110601/L1C_T30TYQ_A009529_20170419T110601"	"test"	120	120	1	false	120	120	"2019-12-04T16:37:46.000Z"	"2019-12-04T16:41:23.000Z"	2

CREATE TABLE metadata (
	metadata_id                    INTEGER       NOT NULL,
	mission                        VARCHAR(64)   NOT NULL,
	orbitphase                     INTEGER,
	absorbit                       INTEGER,
	relorbit                       INTEGER,
	orbitcycle                     INTEGER,
	numorbitsincycle               INTEGER,
	orbitdirection                 VARCHAR(64),
	sensor                         VARCHAR(64),
	imagingmode                    VARCHAR(64),
	antennareceiveconfiguration    VARCHAR(64),
	lookdirection                  VARCHAR(64),
	polarisationmode               VARCHAR(64),
	pollayer                       VARCHAR(64),
	projection                     VARCHAR(64), 
	mapprojection       	       VARCHAR(64),
	producttype                    VARCHAR(64),
	productvariant                 VARCHAR(64),
	radiometriccorrection          VARCHAR(64),
	resolutionvariant              VARCHAR(64),
	pixelvalueid                   VARCHAR(64),
	columncontent		       VARCHAR(64),
  	rowcontent		       VARCHAR(64),	
	imagedatadepth                 INTEGER,
	imagedataformat                VARCHAR(64),
	imagedatatype                  VARCHAR(64),	
	imagestorageorder	       VARCHAR(64),		
	numberoflayers                 INTEGER,
	sceneid                        VARCHAR(1024),
	starttimegps			BIGINT,	
	starttimegpsfraction		FLOAT,
	starttimeutc			DATE,
	stoptimegps			BIGINT,
	stoptimegpsfraction		FLOAT,
	stoptimeutc			DATE,
	rangetimefirstpixel		DOUBLE,
	rangetimelastpixel		DOUBLE,
	scenestoptimeutc               VARCHAR(64),
	centerazimuthtimeutc          VARCHAR(64),
	scenerangeextent		DOUBLE,
	sceneazimuthextent		DOUBLE,
	scenecentercoord_refrow        INTEGER,
	scenecentercoord_refcolumn     INTEGER,
	scenecentercoord_lat           FLOAT,
	scenecentercoord_lon      	     	FLOAT,
	scenecentercoord_azimuthtimeutc		DATE,
	scenecentercoord_incidenceangle		DOUBLE,
	scenecentercoord_rangetime		DOUBLE,
	scenecorner_ul_lon             		FLOAT,
	scenecorner_ul_lat             		FLOAT,
	scenecorner_ul_refrow          		INTEGER,
	scenecorner_ul_refcolumn       		INTEGER,
	scenecorner_ul_azimuthtimeutc		DATE,
	scenecorner_ul_incidenceangle		DOUBLE,
	scenecorner_ul_rangetime		DOUBLE,
	scenecorner_ur_lon             		FLOAT,
	scenecorner_ur_lat             		FLOAT,
	scenecorner_ur_refrow          INTEGER,
	scenecorner_ur_refcolumn       INTEGER,
	scenecorner_ur_azimuthtimeutc	DATE,
	scenecorner_ur_incidenceangle	DOUBLE,
	scenecorner_ur_rangetime	DOUBLE,
	scenecorner_ll_lon             FLOAT,
	scenecorner_ll_lat             FLOAT,
	scenecorner_ll_refrow          INTEGER,
	scenecorner_ll_refcolumn       INTEGER,
	scenecorner_ll_azimuthtimeutc	DATE,
	scenecorner_ll_incidenceangle	DOUBLE,
	scenecorner_ll_rangetime	DOUBLE,
	scenecorner_lr_lon             FLOAT,
	scenecorner_lr_lat             FLOAT,
	scenecorner_lr_refrow          INTEGER,
	scenecorner_lr_refcolumn       INTEGER,
	scenecorner_lr_azimuthtimeutc	DATE,
	scenecorner_lr_incidenceangle	DOUBLE,
	scenecorner_lr_rangetime	DOUBLE,
	headingangle			DOUBLE,
	sceneaverageheight		DOUBLE,
	referenceprojection		VARCHAR(64),
	laterror			FLOAT,
	lonerror			FLOAT,
	CONSTRAINT metadata_id_pkey PRIMARY KEY (metadata_id)
);

COPY 22 RECORDS INTO "sys"."metadata" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-05	null	null	2019-11-05	null	null	null	null	null	null	null	null	null	null	null	null	null	0.5367723	43.238262	4790220	300000	2019-11-05	0	0	1.888683	43.25939	4790220	409800	2019-11-05	0	0	0.4959286	44.225964	4900020	300000	2019-11-05	0	0	1.8702372	44.24783	4900020	409800	2019-11-05	0	0	null	null	null	null	null
2	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-05	null	null	2019-11-05	null	null	null	null	null	null	null	null	null	null	null	null	null	13.616763	49.556488	5490240	399960	2019-11-05	0	0	15.134971	49.56468	5490240	509760	2019-11-05	0	0	13.588086	50.54373	5600040	399960	2019-11-05	0	0	15.13777	50.55221	5600040	509760	2019-11-05	0	0	null	null	null	null	null
4	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-05	null	null	2019-11-05	null	null	null	null	null	null	null	null	null	null	null	null	null	7.6875887	46.858177	5190240	399960	2019-11-05	0	0	9.1280575	46.865627	5190240	509760	2019-11-05	0	0	7.662865	47.845913	5300040	399960	2019-11-05	0	0	9.13047	47.853626	5300040	509760	2019-11-05	0	0	null	null	null	null	null
5	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-05	null	null	2019-11-05	null	null	null	null	null	null	null	null	null	null	null	null	null	2.9997182	50.4638	5590200	499980	2019-11-05	0	0	4.5464363	50.45352	5590200	609780	2019-11-05	0	0	2.9997122	51.451183	5700000	499980	2019-11-05	0	0	4.579544	51.44054	5700000	609780	2019-11-05	0	0	null	null	null	null	null
8	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-05	null	null	2019-11-05	null	null	null	null	null	null	null	null	null	null	null	null	null	10.3343315	47.75741	5290200	600000	2019-11-05	0	0	11.798095	47.731045	5290200	709800	2019-11-05	0	0	10.360279	48.744984	5400000	600000	2019-11-05	0	0	11.852439	48.717693	5400000	709800	2019-11-05	0	0	null	null	null	null	null
10	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-05	null	null	2019-11-05	null	null	null	null	null	null	null	null	null	null	null	null	null	4.4649806	52.253456	5790240	600000	2019-11-05	0	0	6.0716786	52.222565	5790240	709800	2019-11-05	0	0	4.4984603	53.240208	5900040	600000	2019-11-05	0	0	6.14177	53.208202	5900040	709800	2019-11-05	0	0	null	null	null	null	null
11	"S1B"	null	17184	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"132385"	null	null	2019-07-18	null	null	2019-07-18	null	null	null	null	null	null	null	null	null	null	null	null	null	2.7267523	48.775703	16697	0	2019-07-18	30.16549312066281	0.005334710189972643	-0.8053875	49.181946	16697	26246	2019-07-18	45.98525781988184	0.006422712698517349	3.1935756	50.270363	0	0	2019-07-18	30.11200614886753	0.005334710189973254	-0.4483819	50.678116	0	26246	2019-07-18	45.9423086928339	0.00642155627024115	null	null	null	null	null
12	"S1B"	null	17111	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"131860"	null	null	2019-07-13	null	null	2019-07-13	null	null	null	null	null	null	null	null	null	null	null	null	null	3.107119	42.90925	16696	0	2019-07-13	30.38301042153092	0.005333937572030251	-0.06557267	43.313885	16696	26211	2019-07-13	46.14452822479293	0.006425014398454577	3.5082238	44.40939	0	0	2019-07-13	30.38158877289202	0.005333937572030348	0.25497672	44.81348	0	26211	2019-07-13	46.14358347694922	0.006424997636296631	null	null	null	null	null
13	"S1B"	null	17096	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"131749"	null	null	2019-07-12	null	null	2019-07-12	null	null	null	null	null	null	null	null	null	null	null	null	null	14.99432	48.55725	16696	0	2019-07-12	30.24077591466897	0.005333244880073161	11.466016	48.964203	16696	26333	2019-07-12	46.09266402078163	0.006427105212553803	15.469602	50.05071	0	0	2019-07-12	30.11073539336138	0.00533324488007541	11.832384	50.45945	0	26333	2019-07-12	45.98794089234568	0.00642426820942879	null	null	null	null	null
14	"S1B"	null	17067	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"131526"	null	null	2019-07-10	null	null	2019-07-10	null	null	null	null	null	null	null	null	null	null	null	null	null	10.215588	46.559456	16675	0	2019-07-10	30.73845477900988	0.005344621013288212	6.8830056	46.95672	16675	25840	2019-07-10	46.2269443335989	0.006425438152421721	10.685976	48.05191	0	0	2019-07-10	30.46679017944434	0.005344621013284478	7.2578797	48.450737	0	25840	2019-07-10	46.00474662756293	0.006419402000794871	null	null	null	null	null
15	"S1A"	null	28065	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"207716"	null	null	2019-07-11	null	null	2019-07-11	null	null	null	null	null	null	null	null	null	null	null	null	null	12.741798	47.974564	16665	0	2019-07-11	30.35941647202238	0.005339666317458033	9.239273	48.381958	16665	26438	2019-07-11	46.22777589922303	0.006440993327488713	13.198593	49.46993	0	0	2019-07-11	30.30272800468076	0.005339666317453142	9.590145	49.87849	0	26438	2019-07-11	46.18225802774877	0.006439756208569548	null	null	null	null	null
16	"S1A"	null	28167	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"208510"	null	null	2019-07-18	null	null	2019-07-18	null	null	null	null	null	null	null	null	null	null	null	null	null	13.065275	41.691032	16668	0	2019-07-18	30.44212337818096	0.005336495919678948	9.917554	42.10022	16668	26513	2019-07-18	46.3377469606664	0.006443312529655186	13.456066	43.192093	0	0	2019-07-18	30.44840380820927	0.005336495919681034	10.231731	43.600372	0	26513	2019-07-18	46.34287274443407	0.006443453185084654	null	null	null	null	null
17	"S1B"	null	17074	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"131585"	null	null	2019-07-10	null	null	2019-07-10	null	null	null	null	null	null	null	null	null	null	null	null	null	15.409759	47.78392	16680	0	2019-07-10	30.44156222409941	0.005341344047512276	18.836113	48.184525	16680	25962	2019-07-10	46.05192037469407	0.006421494925223464	15.837486	46.285755	0	0	2019-07-10	30.42382877199717	0.005341344047508939	19.168846	46.686073	0	25962	2019-07-10	46.03729383126164	0.006421096658491781	null	null	null	null	null
18	"S1A"	null	28051	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"207611"	null	null	2019-07-10	null	null	2019-07-10	null	null	null	null	null	null	null	null	null	null	null	null	null	-3.9587886	39.51886	16690	0	2019-07-10	30.60227642280504	0.005335563449744903	-6.960258	39.924	16690	26130	2019-07-10	46.27518923248228	0.006427206932406182	-3.5800824	41.021023	0	0	2019-07-10	30.58959288491229	0.005335563449743105	-6.649484	41.42493	0	26130	2019-07-10	46.26563993959683	0.006426999861605901	null	null	null	null	null
19	"S1B"	null	17089	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"131698"	null	null	2019-07-11	null	null	2019-07-11	null	null	null	null	null	null	null	null	null	null	null	null	null	3.5885847	52.448494	16704	0	2019-07-11	29.99052939918028	0.005332072632155946	7.401214	52.859425	16704	26200	2019-07-11	45.82676714034871	0.006414150722620937	4.085705	50.95587	0	0	2019-07-11	30.04556010314649	0.005332072632156593	7.774559	51.364334	0	26200	2019-07-11	45.87099206874372	0.006415343285373956	null	null	null	null	null
20	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-26	null	null	2019-11-26	null	null	null	null	null	null	null	null	null	null	null	null	null	1.6415462	48.65702	5390220	399960	2019-11-26	0	0	3.1325514	48.664955	5390220	509760	2019-11-26	0	0	1.6142724	49.64443	5500020	399960	2019-11-26	0	0	3.1352136	49.652645	5500020	509760	2019-11-26	0	0	null	null	null	null	null
21	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-27	null	null	2019-11-27	null	null	null	null	null	null	null	null	null	null	null	null	null	1.6415462	48.65702	5390220	399960	2019-11-27	0	0	3.1325514	48.664955	5390220	509760	2019-11-27	0	0	1.6142724	49.64443	5500020	399960	2019-11-27	0	0	3.1352136	49.652645	5500020	509760	2019-11-27	0	0	null	null	null	null	null
22	"S1+S2"	null	15266	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"117003"	null	null	2019-03-08	null	null	2019-03-08	null	null	null	null	null	null	null	null	null	null	null	null	null	5.08179	54.059944	16698	0	2019-03-08	29.92827260314094	0.005332072632153925	9.09884	54.479412	16698	26581	2019-03-08	45.9687975639977	0.006431049407881228	5.604081	52.56895	0	0	2019-03-08	29.98026326267275	0.005332072632151668	9.4832735	52.985165	0	26581	2019-03-08	46.01045462483671	0.006432194124205971	null	null	null	null	null
24	"S1+S2"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-27	null	null	2019-11-27	null	null	null	null	null	null	null	null	null	null	null	null	null	-75.00024	40.56275	4490220	499980	2019-11-27	0	0	-73.70336	40.555473	4490220	609780	2019-11-27	0	0	-75.00024	41.551846	4600020	499980	2019-11-27	0	0	-73.6838	41.54431	4600020	609780	2019-11-27	0	0	null	null	null	null	null
25	"S1+S2"	null	15266	null	null	null	null	"SAR"	"IW"	null	null	null	"VV"	null	null	"GRD"	null	null	null	null	null	null	null	null	null	null	1	"117003"	null	null	2019-03-08	null	null	2019-03-08	null	null	null	null	null	null	null	null	null	null	null	null	null	5.08179	54.059944	16698	0	2019-03-08	29.92827260314094	0.005332072632153925	9.09884	54.479412	16698	26581	2019-03-08	45.9687975639977	0.006431049407881228	5.604081	52.56895	0	0	2019-03-08	29.98026326267275	0.005332072632151668	9.4832735	52.985165	0	26581	2019-03-08	46.01045462483671	0.006432194124205971	null	null	null	null	null
26	"S1+S2"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-11-27	null	null	2019-11-27	null	null	null	null	null	null	null	null	null	null	null	null	null	-75.00024	40.56275	4490220	499980	2019-11-27	0	0	-73.70336	40.555473	4490220	609780	2019-11-27	0	0	-75.00024	41.551846	4600020	499980	2019-11-27	0	0	-73.6838	41.54431	4600020	609780	2019-11-27	0	0	null	null	null	null	null
27	"S2A"	null	null	null	null	null	null	"MSI"	null	null	null	null	null	null	null	"null"	null	null	null	null	null	null	null	null	null	null	3	null	null	null	2019-12-04	null	null	2019-12-04	null	null	null	null	null	null	null	null	null	null	null	null	null	-0.5001512	44.13801	4890240	699960	2019-12-04	0	0	0.8699868	44.09977	4890240	809760	2019-12-04	0	0	-0.45736864	45.125526	5000040	699960	2019-12-04	0	0	0.93609756	45.085957	5000040	809760	2019-12-04	0	0	null	null	null	null	null

CREATE TABLE "image" (
	image_id     INTEGER       NOT NULL,
	metadata_id  INTEGER       NOT NULL,
	azimuthlooks float,
	rangelooks	float,
	"filename"	VARCHAR(1024)	NOT NULL,
	azimuthresolution	DOUBLE,
	groundrangeresolution	DOUBLE,
	numberofcolumns	INTEGER,
	numberofrows	INTEGER,
	rowspacing	FLOAT,
	columnspacing	FLOAT,
	CONSTRAINT image_id_pkey PRIMARY KEY (image_id),
	CONSTRAINT image_metadata_fkey FOREIGN KEY (metadata_id) REFERENCES metadata (metadata_id) ON DELETE CASCADE
);

COPY 22 RECORDS INTO "sys"."image" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1	1	0	0	"EOLib_L1C_T31TCJ_A012232_20190710T105335.tif"	0	0	10980	10980	0	0
2	2	0	0	"EOLib_L1C_T33UVR_A021326_20190723T101347.tif"	0	0	10980	10980	0	0
4	4	0	0	"EOLib_L1C_T32TMT_A012432_20190724T103030.tif"	0	0	10980	10980	0	0
5	5	0	0	"EOLib_L1C_T31UES_A021784_20190824T105344.tif"	0	0	10980	10980	0	0
8	8	0	0	"EOLib_L1C_T32UPU_A012961_20190830T102552.tif"	0	0	10980	10980	0	0
10	10	0	0	"EOLib_L1C_T31UFU_A021784_20190824T105344.tif"	0	0	10980	10980	0	0
11	11	0	0	"EOLib_s1b-iw-grd-vv-20190718t060632-20190718t060657-017184-020521-001.tif"	0	0	25245	16697	0	0
12	12	0	0	"EOLib_s1b-iw-grd-vv-20190713t055956-20190713t060021-017111-020314-001.tif"	0	0	25204	16696	0	0
13	13	0	0	"EOLib_s1b-iw-grd-vv-20190712t051713-20190712t051738-017096-0202a5-001.tif"	0	0	25209	16696	0	0
14	14	0	0	"EOLib_s1b-iw-grd-vv-20190710t053414-20190710t053439-017067-0201c6-001.tif"	0	0	25177	16675	0	0
15	15	0	0	"EOLib_s1a-iw-grd-vv-20190711t052618-20190711t052643-028065-032b64-001.tif"	0	0	25183	16665	0	0
16	16	0	0	"EOLib_s1a-iw-grd-vv-20190718t051950-20190718t052015-028167-032e7e-001.tif"	0	0	25176	16668	0	0
17	17	0	0	"EOLib_s1b-iw-grd-vv-20190710t164150-20190710t164215-017074-020201-001.tif"	0	0	25184	16680	0	0
18	18	0	0	"EOLib_s1a-iw-grd-vv-20190710t062615-20190710t062640-028051-032afb-001.tif"	0	0	25154	16690	0	0
19	19	0	0	"EOLib_s1b-iw-grd-vv-20190711t172417-20190711t172442-017089-020272-001.tif"	0	0	25213	16704	0	0
20	20	0	0	"EOLib_L1C_T31UDQ_A021355_20190725T105702.tif"	0	0	10980	10980	0	0
21	21	0	0	"EOLib_L1C_T31UDQ_A021355_20190725T105702.tif"	0	0	10980	10980	0	0
22	22	0	0	"EOLib_s1b-iw-grd-vv-20190308t171625-20190308t171650-015266-01c90b-001.tif"	0	0	26582	16699	0	0
24	24	0	0	"EOLib_L1C_T18TWL_A006872_20161015T154519.tif"	0	0	10980	10980	0	0
25	25	0	0	"EOLib_s1b-iw-grd-vv-20190308t171625-20190308t171650-015266-01c90b-001.tif"	0	0	26582	16699	0	0
26	26	0	0	"EOLib_L1C_T18TWL_A006872_20161015T154519.tif"	0	0	10980	10980	0	0
27	27	0	0	"EOLib_L1C_T30TYQ_A009529_20170419T110601.tif"	0	0	10980	10980	0	0

CREATE TABLE eoproduct (
	eoproduct_id	INTEGER		NOT NULL,
	ingestion_id	INTEGER		NOT NULL,
	image_id	INTEGER		NOT NULL,
	CONSTRAINT eoproduct_id_pkey PRIMARY KEY (eoproduct_id),
	CONSTRAINT eoproduct_ingestion_fkey FOREIGN KEY (ingestion_id) REFERENCES ingestion (ingestion_id),
	CONSTRAINT eoproduct_image_fkey FOREIGN KEY (image_id) REFERENCES "image" (image_id)
);

COPY 22 RECORDS INTO "sys"."eoproduct" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1	1	1
2	2	2
4	4	4
5	5	5
8	8	8
10	10	10
11	11	11
12	12	12
13	13	13
14	14	14
15	15	15
16	16	16
17	17	17
18	18	18
19	19	19
20	20	20
21	21	21
22	22	22
24	24	24
25	25	25
26	26	26
27	27	27

Select ' ' as selected, i.image_id, * 
    from metadata m 
    join "image" i on m.metadata_id=i.metadata_id 
    join eoproduct eo on eo.image_id=i.image_id 
    join ingestion ing on ing.ingestion_id=eo.ingestion_id 
    where  m.mission = 'S2A';

ROLLBACK;
