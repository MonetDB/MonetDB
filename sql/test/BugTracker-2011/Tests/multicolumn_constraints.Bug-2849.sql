CREATE TABLE "portmapping" (
	  "id" int  NOT NULL AUTO_INCREMENT,
	  "port" int  NOT NULL,
	  "type" int NOT NULL,
	  "appname" varchar(25) NOT NULL,
	  "description" varchar(255) DEFAULT NULL,
	  CONSTRAINT "port" UNIQUE ("port","type"),
	  CONSTRAINT "appname" UNIQUE ("appname","type"),
	  PRIMARY KEY ("id")
) ;

-- duplicate port and appname
INSERT INTO "portmapping" VALUES (2,1,1,'name','test');
INSERT INTO "portmapping" VALUES (3,1,2,'name','test');

-- duplicate type
INSERT INTO "portmapping" VALUES (4,5,1,'x','test');
INSERT INTO "portmapping" VALUES (5,5,1,'y','test');

-- unique records
INSERT INTO "portmapping" VALUES (6,7,1,'z1','test');
INSERT INTO "portmapping" VALUES (7,8,2,'z2','test');

SELECT * FROM "portmapping";

DROP TABLE "portmapping";
