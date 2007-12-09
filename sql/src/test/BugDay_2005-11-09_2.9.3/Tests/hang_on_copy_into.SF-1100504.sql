CREATE TABLE "news" (
	"id" int NOT NULL,
	"content" CHARACTER LARGE OBJECT,
	"user_id" int,
	"date" timestamp,
	CONSTRAINT "news_id_pkey" PRIMARY KEY ("id")
);
COPY 1 RECORDS INTO "news" FROM stdin USING DELIMITERS '\t', '\n', '''';
1	'dwerg.net word volwassen, er is eindelijk content
en het voicemail archief begint al aardig vol te lopen.
De commentaar en rating code voor de voicemails is ook
al vrij aardig aan het vorderen. Dus dat beloofd nog
wat.r
<br /><br />r
Mja, het is dat ik veel te veel
tijd heb en het helemaal verkeerd indeel dat het
allemaal nogal lang duurt, maar in principe is het nog
maar een uurtje of 6 werk om het goed te krijgen. Zoals
je kan zien heb ik bij de voicemails al wat leuke
knopjes gezet, en het aantal voicemails dat je in 1
keer ziet verhoogd naar 15. Poepoe.r
<br /><br
/>r
Het is me allemaal wat...r
<br /><br
/>r
Muziek: Air	1	2002-02-22 00:21:00.000
