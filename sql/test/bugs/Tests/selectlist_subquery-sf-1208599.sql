CREATE TABLE "sys"."pwprijs" (
        "id"              int           NOT NULL,
        "pid"       int           NOT NULL,
        "lid"   int           NOT NULL,
        "prijs"           real          NOT NULL,
        "Time"            bigint        NOT NULL,
        "recordtimestamp" bigint        NOT NULL,
        CONSTRAINT "pwprijs_id_recordtimestamp_pkey"
PRIMARY KEY ("id", "recordtimestamp")
);
-- CREATE INDEX "pwprijs_lid_idx" ON "pwprijs" ("lid");
-- CREATE INDEX "pwprijs_pid_idx" ON "pwprijs" ("pid");
-- CREATE INDEX "pwprijs_recordtimestamp_idx" ON "pwprijs" ("recordtimestamp");

SELECT lid,
        AVG(Prijs) as avg_prijs,
        AVG(CASE WHEN RecordTimestamp =
            (SELECT MAX(i.RecordTimestamp)
            	FROM pwprijs i
               WHERE i.pid = 117097
                 AND i.lid = o.lid)
            THEN Prijs END) as current_prijs
FROM pwprijs o
WHERE pid = 117097
GROUP BY lid
;
