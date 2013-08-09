/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/

#include <monetdb_config.h>
#include "gdal_vault.h"
#include "sql_mvc.h"
#include "sql.h"
#include "sql_scenario.h"
#include "mal_exception.h"
#include "array.h"
#include <gdal.h>

/* FIXME: the use of the 'rs' schema should be reconsidered so that the gdal
 * catalog can be integrated into the SQL catalog.
 * When removing the 'rs' schame, the code of client/mapiclient/dump.c MUST be
 * adapted accordingly.
 */

/* CURRENT_TIMESTAMP() ?*/
#define INSFILE \
	"INSERT INTO rs.files(fileid,location,status,lastmodified) \
	 VALUES(%d, '%s', %d, CURRENT_TIMESTAMP());"
#define INSCAT \
	"INSERT INTO rs.catalog(imageid,fileid,width,length,bps) \
	 VALUES(%d, %d, %d, %d, %d);"
#define CRT_GREYSCALE_IMAGE \
	"CREATE ARRAY %s.%s (x %s DIMENSION[%d], y %s DIMENSION[%d], intensity %s);"

str
GDALtest(int *wid, int *len, str *fname)
{
	GDALDatasetH  hDataset;
	str msg = MAL_SUCCEED;

	GDALAllRegister();
        hDataset = GDALOpen(*fname, GA_ReadOnly);

	if (hDataset==NULL) 
		return createException(MAL, "gdal.test", "Missing GDAL file %s", *fname);

	*len = GDALGetRasterYSize(hDataset);
        *wid = GDALGetRasterXSize(hDataset);

	GDALClose(hDataset);
	return msg;
}

/* attach a single gdal file given its name, fill in gdal catalog tables */
str
GDALattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	sql_table *fls = NULL, *cat = NULL;
	sql_column *col;
	str msg = MAL_SUCCEED;
	str fname = *(str*)getArgReference(stk, pci, 1);
	GDALDatasetH  hDataset;
	GDALRasterBandH hBand;
	char buf[BUFSIZ], *s= buf;
	int  wid = 0, len = 0;
	oid fid, imid, rid = oid_nil;
	sht bps;

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if (msg)
		return msg;

	sch = mvc_bind_schema(m, "rs");
	if ( !sch )
		return createException(MAL, "gdal.attach", "Schema rs missing\n");

	fls = mvc_bind_table(m, sch, "files");
	cat = mvc_bind_table(m, sch, "catalog");
	if (fls == NULL || cat == NULL )
		return createException(MAL, "gdal.attach", "Catalog table missing\n");

	GDALAllRegister();
        hDataset = GDALOpen(fname, GA_ReadOnly);
	if (hDataset==NULL) return createException(MAL, "gdal.attach", "Missing GDAL file %s\n", fname);

	/* check if the file is already attached */
	col = mvc_bind_column(m, fls, "location");
	rid = table_funcs.column_find_row(m->session->tr, col, fname, NULL);
	if (rid != oid_nil) {
		GDALClose(hDataset);
		msg = createException(SQL, "gdal.attach", "File %s already attached\n", fname);
		return msg; /* just return success ?*/
	}

	/* add row in the rs.files catalog table */
	col = mvc_bind_column(m, fls, "fileid");
	fid = store_funcs.count_col(col, 1) + 1;

	snprintf(buf, BUFSIZ, INSFILE, (int)fid, fname, 0);
	if ( ( msg = SQLstatementIntern(cntxt,&s,"gdal.attach",TRUE,FALSE)) != MAL_SUCCEED)
		goto finish;

	/* add row in the rs.catalog catalog table */
	col = mvc_bind_column(m, cat, "imageid");
	imid = store_funcs.count_col(col, 1) + 1;
	len = GDALGetRasterYSize(hDataset);
        wid = GDALGetRasterXSize(hDataset);
	hBand = GDALGetRasterBand(hDataset, 1);
	bps = GDALGetDataTypeSize(GDALGetRasterDataType(hBand));

	snprintf(buf, BUFSIZ, INSCAT, (int)imid, (int)fid, wid, len, bps);
	msg = SQLstatementIntern(cntxt,&s,"gdal.attach",TRUE,FALSE);

finish:
	/* if (msg != MAL_SUCCEED){
	   snprintf(buf, BUFSIZ,"ROLLBACK;");
	   SQLstatementIntern(cntxt,&s,"gdal.attach",TRUE,FALSE));
	   }*/
	GDALClose(hDataset);
	return msg;
}

static str
ARRAYseries(int *bid, bte start, bte step, int stop, int group, int series)
{
	if (stop <= (int) GDK_bte_max && group <= (int) GDK_bte_max && series <= (int) GDK_bte_max) {
		bte sta = (bte) start, ste = (bte) step, sto = (bte) stop;
		return ARRAYseries_bte(bid, &sta, &ste, &sto, &group, &series);
	} else
	if (stop <= (int) GDK_sht_max && group <= (int) GDK_sht_max && series <= (int) GDK_sht_max) {
		sht sta = (sht) start, ste = (sht) step, sto = (sht) stop;
		return ARRAYseries_sht(bid, &sta, &ste, &sto, &group, &series);
	} else {
		int sta = (int) start, ste = (int) step, sto = (int) stop;
		return ARRAYseries_int(bid, &sta, &ste, &sto, &group, &series);
	}
}

str
GDALloadGreyscaleImage(bat *x, bat *y, bat *intensity, str *fname)
{
	GDALDatasetH  hDataset;
	GDALRasterBandH hBand;
	int  bidx = 0, bidy = 0, wid = 0, len = 0;
	BUN pixels = BUN_NONE;
	sht bps;
	int i, j;
	void *linebuf = NULL;
	BAT *resX = NULL, *resY = NULL, *resI = NULL;
	char *errbuf = NULL;

	GDALAllRegister();
        hDataset = GDALOpen(*fname, GA_ReadOnly);
	if (hDataset==NULL)
		return createException(MAL, "gdal.loadimage", "Missing GDAL file %s\n", *fname);

	len = GDALGetRasterYSize(hDataset);
        wid = GDALGetRasterXSize(hDataset);
	hBand = GDALGetRasterBand(hDataset, 1);
        bps = GDALGetDataTypeSize(GDALGetRasterDataType(hBand));

	pixels = (BUN)wid * (BUN)len;

	/* Read the pixel intensities from the GDAL file and fill in the BAT */
	switch (bps/8){ 
	case sizeof(bte):
	{
		sht *data_sht = NULL;

		resI = BATnew(TYPE_void, TYPE_sht, pixels); 
		linebuf = GDKmalloc(wid); /* buffer for one line of image */
		if (resI == NULL || linebuf == NULL) { 
			GDALClose(hDataset); 
			if (resI) BBPunfix(resI->batCacheid);
			if (linebuf) GDKfree(linebuf); 
			return createException(MAL, "gdal.loadimage", MAL_MALLOC_FAIL); 
		}
		data_sht = (sht *)Tloc(resI, BUNfirst(resI));

		for(i = 0; i < len; i++) {
			if (GDALRasterIO(hBand, GF_Read, 0, i, wid, 1, linebuf, wid, 1, GDT_Byte, 0, 0) != CE_Failure) {
                                for (j = 0; j < wid; j++) 
                                        data_sht[i*wid + j] = ((unsigned char*)linebuf)[j];
                        }
                }
		break;
	}
	default:
		GDALClose(hDataset);
		return createException(MAL, "gdal.loadimage", "Unexpected BitsPerSample: %d not in {8}", bps);
	}
	GDALClose(hDataset);
	GDKfree(linebuf);

	BATsetcount(resI, pixels);
	BATseqbase(resI, 0);
	resI->T->nonil = TRUE;
	resI->T->nil = FALSE;
	resI->tsorted = FALSE;
	resI->trevsorted = FALSE;
	BATkey(BATmirror(resI), FALSE);
	BBPkeepref(resI->batCacheid);

	/* Manually compute values for the X-dimension, since we know that its
	 * range is [0:1:wid] and each of its value must be repeated 'len'
	 * times with 1 #repeats */
	errbuf = ARRAYseries(&bidx, 0, 1, wid, 1, len);
	if (errbuf != MAL_SUCCEED) {
		BBPdecref(resI->batCacheid, 1); /* undo the BBPkeepref(resI->batCacheid) above */
		return createException(MAL, "gdal.loadimage", "Failed to create the X-dimension of %s", *fname);
	}
	/* Manually compute values for the Y-dimension, since we know that its
	 * range is [0:1:len] and each of its value must be repeated 1 times
	 * with 'wid' #repeats */
	errbuf = ARRAYseries(&bidy, 0, 1, len, wid, 1);
	if (errbuf != MAL_SUCCEED) {
		BBPdecref(resI->batCacheid, 1); /* undo the BBPkeepref(resI->batCacheid) above */
		BBPdecref(resX->batCacheid, 1); /* undo the BBPkeepref(resX->batCacheid) by ARRAYseries_*() */
		return createException(MAL, "gdal.loadimage", "Failed to create the y-dimension of %s", *fname);
	}

	resX = BATdescriptor(bidx); /* these should not fail... */
	resY = BATdescriptor(bidy);
	if (BATcount(resX) != pixels || BATcount(resY) != pixels) {
		BBPdecref(resX->batCacheid, 1);
		BBPdecref(resY->batCacheid, 1);
		BBPdecref(resI->batCacheid, 1);
		BBPunfix(resX->batCacheid);
		BBPunfix(resY->batCacheid);
		return createException(MAL, "gdal.loadimage", "X or Y dimension has invalid number of pixels. Got " BUNFMT "," BUNFMT " (!= " BUNFMT ")", BATcount(resX), BATcount(resY), pixels);
	}

	*x = resX->batCacheid;
	*y = resY->batCacheid;
	*intensity = resI->batCacheid;

	return MAL_SUCCEED;
}

/* Procedure for loading of image with given imageid into an array with name image<imageid> */

str
GDALimportImage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	sql_table *fls = NULL, *cat = NULL, *img = NULL;
	sql_column *col = NULL;
	oid irid = oid_nil, frid = oid_nil;
	str msg = MAL_SUCCEED, fname = NULL, dimtype = NULL;
	int imageid = *(int*)getArgReference(stk, pci, 1);
	int  wid = 0, len = 0, fid;
	sht bps;
	char aname[20], buf[BUFSIZ], *s = buf;
	bte stat = 0;
	bat x, y, intensity;

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if (msg)
		return msg;

	sch = mvc_bind_schema(m, "rs");
	if (sch == NULL)
		return createException(MAL, "gdal.import", "Schema rs missing\n");

	fls = mvc_bind_table(m, sch, "files");
	cat = mvc_bind_table(m, sch, "catalog");
	if (fls == NULL || cat == NULL)
		return createException(MAL, "gdal.import", "Catalog tables \"files\" or \"catalog\" missing\n");

	/* get fileid */
	col = mvc_bind_column(m, cat, "imageid");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"catalog\".\"imageid\"\n");
	irid = table_funcs.column_find_row(m->session->tr, col, (void *)&imageid, NULL);
	if (irid == oid_nil) 
		return createException(MAL, "gdal.import", "Image %d not in the GDAL \"catalog\"\n", imageid);

	col = mvc_bind_column(m, cat, "fileid");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"catalog\".\"fileid\"\n");
	fid = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	if (fid < 0) 
		return createException(MAL, "gdal.import", "File ID of image %d not in the GDAL \"files\"\n", imageid);
	/* check status of the image file */
	frid = table_funcs.column_find_row(m->session->tr, col, (void *)&fid, NULL);
	col = mvc_bind_column(m, fls, "status");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"files\".\"status\"\n");
	stat = *(bte *)table_funcs.column_find_value(m->session->tr, col, frid);
	if (stat > 0)
		return createException(MAL, "gdal.import", "Image %d in file %d already imported\n", imageid, fid);

	/* TODO: check last modified
	col = mvc_bind_column(m, fls, "lastmodified");
	lmod = (lng *)table_funcs.column_find_value(m->session->tr, col, frid);
	read file timestamp and compare
	*/

	/* create SciQL array */
	col = mvc_bind_column(m, cat, "width");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"catalog\".\"width\"\n");
	wid = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	col = mvc_bind_column(m, cat, "length");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"catalog\".\"length\"\n");
	len = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	col = mvc_bind_column(m, cat, "bps");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"catalog\".\"bps\"\n");
	bps = *(sht*)table_funcs.column_find_value(m->session->tr, col, irid);

	snprintf(aname, 20, "image%d", imageid);
	if (wid <= (int) GDK_bte_max && len <= (int) GDK_bte_max) {
		dimtype = "TINYINT";
	} else
	if (wid <= (int) GDK_sht_max && len <= (int) GDK_sht_max) {
		dimtype = "SMALLINT";
	} else {
		dimtype = "INT";
	}
	snprintf(buf, BUFSIZ, CRT_GREYSCALE_IMAGE, sch->base.name, aname, dimtype, wid, dimtype, len, (bps == 8)? "SMALLINT" : "INT");
	msg = SQLstatementIntern(cntxt,&s,"gdal.import",TRUE,FALSE);
	if (msg != MAL_SUCCEED)
		return msg;

	/* load image data */
	col = mvc_bind_column(m, fls, "location");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"files\".\"location\"\n");
	fname = (char *)table_funcs.column_find_value(m->session->tr, col, frid);
	msg = GDALloadGreyscaleImage(&x, &y, &intensity, &fname);
	if (msg != MAL_SUCCEED)
		return msg;
	
	/* associate columns of the image array with the loaded image data */
	img = mvc_bind_table(m, sch, aname);
	if (img == NULL)
		return createException(MAL, "gdal.import", "Image table \"rs\".\"%s\" missing\n", aname);
	/* the 'x' dimension */
	col = mvc_bind_column(m, img, "x");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"%s\".\"x\"\n", aname);
	store_funcs.append_col(m->session->tr, col, BATdescriptor(x), TYPE_bat);
	/* the 'y' dimension */
	col = mvc_bind_column(m, img, "y");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"%s\".\"y\"\n", aname);
	store_funcs.append_col(m->session->tr, col, BATdescriptor(y), TYPE_bat);
	/* the 'intensity' column */
	col = mvc_bind_column(m, img, "intensity");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"%s\".\"intensity\"\n", aname);
	store_funcs.append_col(m->session->tr, col, BATdescriptor(intensity), TYPE_bat);
	/* update the GDAL catalog to set status to 1 (loaded) */
	stat = 1;
	col = mvc_bind_column(m, fls, "status");
	if (col == NULL)
		return createException(MAL, "gdal.import", "Could not find \"fls\".\"status\"\n");
	table_funcs.column_update_value(m->session->tr, col, frid, (void*) &stat);

	{ /* update SQL catalog to denote this array has been materialised */
		sql_table *systable = NULL, *sysarray = NULL;
		oid rid = 0;
		sql_schema *syss = NULL;
		sqlid tid = 0;
		bit materialised = 1;

		syss = find_sql_schema(m->session->tr, isGlobal(img)?"sys":"tmp");
		systable = find_sql_table(syss, "_tables");
		sysarray = find_sql_table(syss, "_arrays");
		/* find 'id' of this array in _tables */
		rid = table_funcs.column_find_row(m->session->tr, find_sql_column(systable, "name"), aname, NULL);
		tid = *(sqlid*) table_funcs.column_find_value(m->session->tr, find_sql_column(systable, "id"), rid);
		/* update value in _arrays */
		rid = table_funcs.column_find_row(m->session->tr, find_sql_column(sysarray, "table_id"), &tid, NULL);
		table_funcs.column_update_value(m->session->tr, find_sql_column(sysarray, "materialised"), rid, &materialised);	
		img->materialised = 1;
	}

	return MAL_SUCCEED;
}

