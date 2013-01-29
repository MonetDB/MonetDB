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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/

#include <monetdb_config.h>
#include "geotiff.h"
#include "sql_mvc.h"
#include "sql.h"
#include "sql_scenario.h"
#include "mal_exception.h"
#include <xtiffio.h>  /* for TIFF */


/* FIXME: the use of the 'rs' schema should be reconsidered so that the geotiff
 * catalog can be integrated into the SQL catalog.
 * When removing the 'rs' schame, the code of client/mapiclient/dump.c MUST be
 * adapted accordingly.
 */

#define INSFILE "INSERT INTO rs.files(fileid,location,status,lastmodified) \
	 VALUES(%d, '%s', %d, CURRENT_TIMESTAMP());"
#define INSCAT "INSERT INTO rs.catalog(imageid,fileid,width,length,bps) \
	 VALUES(%d, %d, %d, %d, %d);"
#define CRTIMAGE "CREATE ARRAY %s (x int dimension[%d], \
	y int dimension[%d], v %s);"

/* CURRENT_TIMESTAMP() ?*/

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
	if (!tif)
		return createException(MAL, "geotiff.attach", "Missing GEOTIFF file %s\n", fname);

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

str
GTIFFloadImage(bat *result, str *fname)
{
	TIFF *tif = (TIFF*)0;
	int  wid = 0, len = 0;
	BUN pixels = BUN_NONE;
	sht photoint, bps;
	tsize_t i, j;
	void *linebuf = NULL;
	sht *data_sht = NULL;
	int *data_int = NULL;
	BAT *res;


	tif = XTIFFOpen(*fname, "r");
	if (!tif)
		return createException(MAL, "geotiff.loadimage", "Missing GEOTIFF file %s\n", *fname);

	TIFFGetField(tif,TIFFTAG_PHOTOMETRIC, &photoint);
	if ( photoint > 1 ){
		XTIFFClose(tif);
		return createException(MAL, "geotiff.loadimage", "Currently only support of greyscale images.\n");
	}

	TIFFGetField(tif, TIFFTAG_IMAGEWIDTH, &wid);
	TIFFGetField(tif, TIFFTAG_IMAGELENGTH, &len);
	TIFFGetField(tif,TIFFTAG_BITSPERSAMPLE, &bps);

	pixels = (BUN)wid * (BUN)len;

	/* allocate res BAT */
	switch (bps/8){ /* sacrifice some storage to avoid values become signed */
	case sizeof(bte):
		res = BATnew(TYPE_void, TYPE_sht, pixels);
		linebuf = GDKmalloc(wid); /* buffer for one line of image */
		data_sht = (sht *) Tloc(res, BUNfirst(res));
		/* read data */
		for( i = 0; i < len; i++){
			if (TIFFReadScanline(tif, linebuf, i, 0) != -1) {
				for (j = 0; j < wid; j++)
					data_sht[j*len+i] = ((unsigned char*)linebuf)[j];
			}
		}
		break;
	case sizeof(sht):
		res = BATnew(TYPE_void, TYPE_int, pixels);
		linebuf = GDKmalloc(wid * 2); /* buffer for one line of image */
		data_int = (int *) Tloc(res, BUNfirst(res));
		/* read data */
		for( i = 0; i < len; i++){
			if (TIFFReadScanline(tif, linebuf, i, 0) != -1) {
				for (j = 0; j < wid; j++)
					data_int[j*len+i] = ((unsigned short *)linebuf)[j];
			}
		}
		break;
	default:
		XTIFFClose(tif);
		return createException(MAL, "geotiff.loadimage", "Unexpected BitsPerSample: %d not in {8,16}", bps);
	}
	if ( res == NULL ) {
		XTIFFClose(tif);
		return createException(MAL, "geotiff.loadimage", MAL_MALLOC_FAIL);
	}

	/* set result BAT properties */
	BATsetcount(res, pixels);
	BATseqbase(res, 0);
	BATkey(res, TRUE);
	res->T->nonil = TRUE;
	res->T->nil = FALSE;
	res->tsorted = FALSE;

	BBPkeepref(*result = res->batCacheid);
	XTIFFClose(tif);
	GDKfree(linebuf);
	return MAL_SUCCEED;
}

/* Procedure for loading of image with given imageid into an array with name image<imageid> */

str
GTIFFimportImage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	sql_table *fls = NULL, *cat = NULL, *arr = NULL;
	sql_column *col;
	oid irid = oid_nil, frid = oid_nil;
	str msg = MAL_SUCCEED, fname = NULL;
	int imageid = *(int*)getArgReference(stk, pci, 1);
	int  wid = 0, len = 0, fid;
	sht bps;
	char aname[20], buf[BUFSIZ], *s = buf;
	BAT *b;
	bte stat = 0;
	bat res;

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if (msg)
		return msg;

	sch = mvc_bind_schema(m, "rs");
	if ( !sch )
		return createException(MAL, "geotiff.import", "Schema rs missing\n");

	fls = mvc_bind_table(m, sch, "files");
	cat = mvc_bind_table(m, sch, "catalog");
	if (fls == NULL || cat == NULL )
		return createException(MAL, "geotiff.import", "Catalog table missing\n");

	/* get fileid */
	col = mvc_bind_column(m, cat, "imageid");
	irid = table_funcs.column_find_row(m->session->tr, col, (void *)&imageid, NULL);
	if (irid == oid_nil) {
		msg = createException(MAL, "geotiff.import", "Image %d not in the catalog\n", imageid);
		return msg;
	}

	col = mvc_bind_column(m, cat, "fileid");
	fid = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);

	/* check status of the image file */
	col = mvc_bind_column(m, fls, "fileid");
	frid = table_funcs.column_find_row(m->session->tr, col, (void *)&fid, NULL);
	col = mvc_bind_column(m, fls, "status");
	stat = *(bte *)table_funcs.column_find_value(m->session->tr, col, frid);
	if ( stat > 0 )
		return createException(MAL, "geotiff.import", "Image %d in file %d already imported\n", imageid, fid);
	/* TO DO check last modified
	col = mvc_bind_column(m, fls, "lastmodified");
	lmod = (lng *)table_funcs.column_find_value(m->session->tr, col, frid);
	read file timestamp and compare
*/

	/* load image data */
	col = mvc_bind_column(m, fls, "location");
	fname = (char *)table_funcs.column_find_value(m->session->tr, col, frid);
	if (( msg = GTIFFloadImage(&res, &fname)) != MAL_SUCCEED )
		return msg;

	/* create SciQL array */
	col = mvc_bind_column(m, cat, "width");
	wid = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	col = mvc_bind_column(m, cat, "length");
	len = *(int*)table_funcs.column_find_value(m->session->tr, col, irid);
	col = mvc_bind_column(m, cat, "bps");
	bps = *(sht*)table_funcs.column_find_value(m->session->tr, col, irid);

	snprintf(aname, 20,"image%d",imageid);
	snprintf(buf, BUFSIZ, CRTIMAGE, aname, wid, len, (bps == 8)? "SMALLINT" : "INT");
	if (( msg = SQLstatementIntern(cntxt,&s,"geotiff.import",TRUE,FALSE)) != MAL_SUCCEED )
		return msg;

	/* replace column v of the array with the loaded image data */
	arr = mvc_bind_table(m, sch, aname);
	col = mvc_bind_column(m, arr, "v");
	b = BATdescriptor(res);
	store_funcs.append_col(m->session->tr, col, b, TYPE_bat);
	BBPunfix(b->batCacheid);

	/* set status to 1 (loaded) */
	stat = 1;
	col = mvc_bind_column(m, fls, "status");
	table_funcs.column_update_value(m->session->tr, col, frid, (void*) &stat);

	return MAL_SUCCEED;
}


