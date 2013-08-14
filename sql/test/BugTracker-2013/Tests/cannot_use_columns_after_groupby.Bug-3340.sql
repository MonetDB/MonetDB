CREATE TABLE "sys"."filer_volumes" (
	     "filer"     VARCHAR(256)  NOT NULL,
	     "volume"    VARCHAR(256)  NOT NULL,
	     "claim_tb"  INTEGER,
	     "used_tb"   INTEGER,
	     "used_perc" SMALLINT,
	     "function"  VARCHAR(24),
	     CONSTRAINT "filer_volumes_filer_volume_pkey" PRIMARY KEY ("filer", "volume")
);
 select "function", (sum(fv.claim_tb) - sum(fv.used_tb)) * 100 / toc.total_overcapacity from sys.filer_volumes as fv, (select sum(claim_tb) - sum(used_tb) as total_overcapacity from sys.filer_volumes) as toc group by fv."function" order by fv."function";
drop table filer_volumes;
