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
#include "geotiff.h"
#include "sql_mvc.h"
#include "sql.h"
#include "sql_scenario.h"
#include "mal_exception.h"
#include "array.h"
#include <xtiffio.h>  /* for TIFF */

/* FIXME: the use of the 'rs' schema should be reconsidered so that the geotiff
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
GTIFFtest(int *wid, int *len, str *fname)
{
	TIFF *tif = (TIFF*)0;  /* TIFF-level descriptor */
	str msg = MAL_SUCCEED;

	/* Open TIFF descriptor  */
	tif = XTIFFOpen(*fname, "r");
	if (!tif)
		return createException(MAL, "gtiff.test", "Missing GEOTIFF file %s", *fname);
	TIFFGetField(tif, TIFFTAG_IMAGEWIDTH, wid);
	TIFFGetField(tif,TIFFTAG_IMAGELENGTH, len);
	XTIFFClose(tif);

	return msg;
}

/* attach a single geotiff file given its name, fill in geotiff catalog tables */
str
GTIFFattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	sql_table *fls = NULL, *cat = NULL;
	sql_column *col;
	str msg = MAL_SUCCEED;
	str fname = *(str*)getArgReference(stk, pci, 1);
	TIFF *tif = (TIFF*)0;  /* TIFF-level descriptor */
	char buf[BUFSIZ], *s= buf;
	int  wid = 0, len = 0;
	oid fid, imid, rid = oid_nil;
	sht bps;

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if (msg)
		return msg;

	sch = mvc_bind_schema(m, "rs");
	if ( !sch )
		return createException(MAL, "geotiff.attach", "Schema rs missing\n");

	fls = mvc_bind_table(m, sch, "files");
	cat = mvc_bind_table(m, sch, "catalog");
	if (fls == NULL || cat == NULL )
		return createException(MAL, "geotiff.attach", "Catalog table missing\n");

	tif = XTIFFOpen(fname, "r");
	if (!tif) return createException(MAL, "geotiff.attach", "Missing GEOTIFF file %s\n", fname);

	/* check if the file is already attached */
	col = mvc_bind_column(m, fls, "location");
	rid = table_funcs.column_find_row(m->session->tr, col, fname, NULL);
	if (rid != oid_nil) {
		XTIFFClose(tif);
		msg = createException(SQL, "geotiff.attach", "File %s already attached\n", fname);
		return msg; /* just return success ?*/
	}

	/* add row in the rs.files catalog table */
	col = mvc_bind_column(m, fls, "fileid");
	fid = store_funcs.count_col(col, 1) + 1;

	snprintf(buf, BUFSIZ, INSFILE, (int)fid, fname, 0);
	if ( ( msg = SQLstatementIntern(cntxt,&s,"geotiff.attach",TRUE,FALSE)) != MAL_SUCCEED)
		goto finish;

	/* add row in the rs.catalog catalog table */
	col = mvc_bind_column(m, cat, "imageid");
	imid = store_funcs.count_col(col, 1) + 1;
	TIFFGetField(tif, TIFFTAG_IMAGEWIDTH, &wid);
	TIFFGetField(tif, TIFFTAG_IMAGELENGTH, &len);
	TIFFGetField(tif, TIFFTAG_BITSPERSAMPLE, &bps);

	snprintf(buf, BUFSIZ, INSCAT, (int)imid, (int)fid, wid, len, bps);
	msg = SQLstatementIntern(cntxt,&s,"geotiff.attach",TRUE,FALSE);

finish:
	/* if (msg != MAL_SUCCEED){
	   snprintf(buf, BUFSIZ,"ROLLBACK;");
	   SQLstatementIntern(cntxt,&s,"geotiff.attach",TRUE,FALSE));
	   }*/
	XTIFFClose(tif);
	return msg;
}


#define MALLOC_CHECK(tpe, sz) \
{ \
	resI = BATnew(TYPE_void, TYPE_##tpe, pixels); \
	linebuf = GDKmalloc(sz); /* buffer for one line of image */ \
	if (resI == NULL || linebuf == NULL) { \
		XTIFFClose(tif); \
		if (resI) BBPunfix(resI->batCacheid); \
		if (linebuf) GDKfree(linebuf); \
		return createException(MAL, "geotiff.loadimage", MAL_MALLOC_FAIL); \
	} \
}

static str
ARRAYseries(bat *bid, bte start, bte step, int stop, int group, int series)
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
GTIFFloadGreyscaleImage(bat *x, bat *y, bat *intensity, str *fname)
{
	TIFF *tif = (TIFF*)0;
	bat bidx = 0, bidy = 0, bidi = 0;
	int wid = 0, len = 0;
	BUN pixels = BUN_NONE;
	sht photoint, bps;
	tsize_t i, j;
	void *linebuf = NULL;
	BAT *resX = NULL, *resY = NULL, *resI = NULL;
	char *errbuf = NULL;

	tif = XTIFFOpen(*fname, "r");
	if (!tif)
		return createException(MAL, "geotiff.loadimage", "Missing GEOTIFF file %s\n", *fname);

	TIFFGetField(tif, TIFFTAG_IMAGEWIDTH, &wid);
	TIFFGetField(tif, TIFFTAG_IMAGELENGTH, &len);
	TIFFGetField(tif, TIFFTAG_BITSPERSAMPLE, &bps);
	TIFFGetField(tif, TIFFTAG_PHOTOMETRIC, &photoint);
	if (photoint > 1){
		XTIFFClose(tif);
		return createException(MAL, "geotiff.loadimage", "Currently only support of greyscale images.\n");
	}
	pixels = (BUN)wid * (BUN)len;

	/* Read the pixel intensities from the GeoTIFF file and fill in the BAT */
	switch (bps/8){ /* sacrifice some storage to avoid values become signed */
	case sizeof(bte):
	{
		sht *data_sht = NULL;

		MALLOC_CHECK(sht, wid);
		data_sht = (sht *)Tloc(resI, BUNfirst(resI));
		for( i = 0; i < len; i++){
			if (TIFFReadScanline(tif, linebuf, i, 0) != -1) {
				for (j = 0; j < wid; j++) 
					data_sht[j*len+i] = ((unsigned char*)linebuf)[j];
			}
		}
		break;
	}
	case sizeof(sht):
	{
		int *data_int = NULL;

		MALLOC_CHECK(int, wid*2);
		data_int = (int *)Tloc(resI, BUNfirst(resI));
		for( i = 0; i < len; i++){
			if (TIFFReadScanline(tif, linebuf, i, 0) != -1) {
				for (j = 0; j < wid; j++) 
					data_int[j*len+i] = ((unsigned short*)linebuf)[j];
			}
		}
		break;
	}
	default:
		XTIFFClose(tif);
		return createException(MAL, "geotiff.loadimage", "Unexpected BitsPerSample: %d not in {8,16}", bps);
	}
	XTIFFClose(tif);
	GDKfree(linebuf);

	BATsetcount(resI, pixels);
	BATseqbase(resI, 0);
	resI->T->nonil = TRUE;
	resI->T->nil = FALSE;
	resI->tsorted = FALSE;
	resI->trevsorted = FALSE;
	BATkey(BATmirror(resI), FALSE);
	BBPkeepref(bidi = resI->batCacheid);
	resI = NULL;

	/* Manually compute values for the X-dimension, since we know that its
	 * range is [0:1:wid] and each of its value must be repeated 'len'
	 * times with 1 #repeats */
	errbuf = ARRAYseries(&bidx, 0, 1, wid, len, 1);
	if (errbuf != MAL_SUCCEED) {
		BBPdecref(bidi, 1); /* undo the BBPkeepref(resI->batCacheid) above */
		return createException(MAL, "geotiff.loadimage", "Failed to create the X-dimension of %s", *fname);
	}
	/* Manually compute values for the Y-dimension, since we know that its
	 * range is [0:1:len] and each of its value must be repeated 1 times
	 * with 'wid' #repeats */
	errbuf = ARRAYseries(&bidy, 0, 1, len, 1, wid);
	if (errbuf != MAL_SUCCEED) {
		BBPdecref(bidi, 1); /* undo the BBPkeepref(resI->batCacheid) above */
		BBPdecref(bidy, 1); /* undo the BBPkeepref(resX->batCacheid) by ARRAYseries_*() */
		return createException(MAL, "geotiff.loadimage", "Failed to create the y-dimension of %s", *fname);
	}

	resX = BATdescriptor(bidx); /* these should not fail... */
	resY = BATdescriptor(bidy);
	if (BATcount(resX) != pixels || BATcount(resY) != pixels) {
		BUN cntX = BATcount(resX), cntY = BATcount(resY);
		BBPunfix(bidx);     /* undo physical incref done by BATdescriptor(bidx) */
		BBPunfix(bidi);     /* undo physical incref done by BATdescriptor(bidy) */
		resX = resY = NULL;
		BBPdecref(bidx, 1); /* undo the BBPkeepref(resX->batCacheid) by ARRAYseries_*() */
		BBPdecref(bidy, 1); /* undo the BBPkeepref(resY->batCacheid) by ARRAYseries_*() */
		BBPdecref(bidi, 1); /* undo the BBPkeepref(resI->batCacheid) above */
		return createException(MAL, "geotiff.loadimage", "X or Y dimension has invalid number of pixels. Got " BUNFMT "," BUNFMT " (!= " BUNFMT ")", cntX, cntY, pixels);
	}
	BBPunfix(resX->batCacheid); /* undo physical incref done by BATdescriptor(bidx) */
	BBPunfix(resY->batCacheid); /* undo physical incref done by BATdescriptor(bidy) */
	resX = resY = NULL;

	*x = bidx;
	*y = bidy;
	*intensity = bidi;
	return MAL_SUCCEED;
}

/* Procedure for loading of image with given imageid into an array with name image<imageid> */

str
GTIFFimportImage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
	BAT *bx, *by, *bi;

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if (msg)
		return msg;

	sch = mvc_bind_schema(m, "rs");
	if (sch == NULL)
		return createException(MAL, "geotiff.import", "Schema rs missing\n");

	fls = mvc_bind_table(m, sch, "files");
	cat = mvc_bind_table(m, sch, "catalog");
	if (fls == NULL || cat == NULL)
		return createException(MAL, "geotiff.import", "Catalog tables \"files\" or \"catalog\" missing\n");

	/* get fileid */
	col = mvc_bind_column(m, cat, "imageid");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"catalog\".\"imageid\"\n");
	irid = table_funcs.column_find_row(m->session->tr, col, (void *)&imageid, NULL);
	if (irid == oid_nil) 
		return createException(MAL, "geotiff.import", "Image %d not in the GeoTIFF \"catalog\"\n", imageid);

	col = mvc_bind_column(m, cat, "fileid");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"catalog\".\"fileid\"\n");
	fid = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	if (fid < 0) 
		return createException(MAL, "geotiff.import", "File ID of image %d not in the GeoTIFF \"files\"\n", imageid);
	/* check status of the image file */
	frid = table_funcs.column_find_row(m->session->tr, col, (void *)&fid, NULL);
	col = mvc_bind_column(m, fls, "status");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"files\".\"status\"\n");
	stat = *(bte *)table_funcs.column_find_value(m->session->tr, col, frid);
	if (stat > 0)
		return createException(MAL, "geotiff.import", "Image %d in file %d already imported\n", imageid, fid);

	/* TODO: check last modified
	col = mvc_bind_column(m, fls, "lastmodified");
	lmod = (lng *)table_funcs.column_find_value(m->session->tr, col, frid);
	read file timestamp and compare
	*/

	/* create SciQL array */
	col = mvc_bind_column(m, cat, "width");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"catalog\".\"width\"\n");
	wid = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	col = mvc_bind_column(m, cat, "length");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"catalog\".\"length\"\n");
	len = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	col = mvc_bind_column(m, cat, "bps");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"catalog\".\"bps\"\n");
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
	msg = SQLstatementIntern(cntxt,&s,"geotiff.import",TRUE,FALSE);
	if (msg != MAL_SUCCEED)
		return msg;

	/* load image data */
	col = mvc_bind_column(m, fls, "location");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"files\".\"location\"\n");
	fname = (char *)table_funcs.column_find_value(m->session->tr, col, frid);
	msg = GTIFFloadGreyscaleImage(&x, &y, &intensity, &fname);
	if (msg != MAL_SUCCEED)
		return msg;

	/* associate columns of the image array with the loaded image data */
	img = mvc_bind_table(m, sch, aname);
	if (img == NULL)
		return createException(MAL, "geotiff.import", "Image table \"rs\".\"%s\" missing\n", aname);
	/* the 'x' dimension */
	col = mvc_bind_column(m, img, "x");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"%s\".\"x\"\n", aname);
	bx = BATdescriptor(x);
	store_funcs.append_col(m->session->tr, col, bx, TYPE_bat);
	BBPunfix(x);  /* release physical reference inherited from BATdescriptor()*/
	bx = NULL;
	BBPdecref(x, 1); /* release logical reference inherited from GTIFFloadGreyscaleImage() */
	/* the 'y' dimension */
	col = mvc_bind_column(m, img, "y");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"%s\".\"y\"\n", aname);
	by = BATdescriptor(y);
	store_funcs.append_col(m->session->tr, col, by, TYPE_bat);
	BBPunfix(y); /* release physical reference inherited from BATdescriptor() */
	by = NULL;
	BBPdecref(y, 1); /* release logical reference inherited from GTIFFloadGreyscaleImage() */
	/* the 'intensity' column */
	col = mvc_bind_column(m, img, "intensity");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"%s\".\"intensity\"\n", aname);
	bi = BATdescriptor(intensity);
	store_funcs.append_col(m->session->tr, col, bi, TYPE_bat);
	BBPunfix(intensity); /* release physical reference inherited from BATdescriptor() */
	bi = NULL;
	BBPdecref(intensity, 1); /* release logical reference inherited from GTIFFloadGreyscaleImage() */

	/* update the GeoTIFF catalog to set status to 1 (loaded) */
	stat = 1;
	col = mvc_bind_column(m, fls, "status");
	if (col == NULL)
		return createException(MAL, "geotiff.import", "Could not find \"fls\".\"status\"\n");
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

