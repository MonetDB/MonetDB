START TRANSACTION;

CREATE TABLE "device" (
	"deviceid"        int,
	"parentdeviceid"  int,
	CONSTRAINT "device_pk" PRIMARY KEY ("deviceid")
);
COPY 2 RECORDS INTO "device" FROM stdin USING DELIMITERS ',','\n','"';
2,NULL
23,2
ALTER TABLE "device" ADD CONSTRAINT "device_device_fk" FOREIGN KEY ("parentdeviceid") REFERENCES "device" ("deviceid");

ROLLBACK;
