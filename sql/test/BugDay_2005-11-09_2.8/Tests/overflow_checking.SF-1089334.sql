CREATE TABLE "rawtriples" (
"id" decimal(10) NOT NULL,
"subjns" decimal(10) NOT NULL,
"subjlname" varchar(255) NOT NULL,
"predns" decimal(10) NOT NULL,
"predlname" varchar(255) NOT NULL,
"objns" decimal(10) NOT NULL,
"objlname" varchar(255) NOT NULL,
"objlabelhash" decimal(10),
"objlang" varchar(16),
"objlabel" varchar(100),
"objisliteral" boolean
);

INSERT INTO rawtriples VALUES(1001, 1,
'100168990', 2, 'glossaryEntry', 0, '',
-53495548, NULL, 'adorning with mosaic', true);
INSERT INTO rawtriples VALUES(1001, 1,
'100168990', 2, 'glossaryEntry', 0, '',
-5342185907394955428, NULL, 'adorning with mosaic', true);

select * from rawtriples;
