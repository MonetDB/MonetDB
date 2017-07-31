/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * Authors: M. Ivanova, M. Kersten, N. Nes
 *
 * This module contains primitives for accessing data in FITS file format.
 */

#include "monetdb_config.h"
#include <glob.h>

/* clash with GDK? */
#undef ttype
#include <fitsio.h>
#include <fitsio2.h>
#include <longnam.h>

#include "fits.h"
#include "mutils.h"
#include "sql_mvc.h"
#include "sql_scenario.h"
#include "sql.h"
#include "clients.h"
#include "mal_exception.h"

#define FITS_INS_COL "INSERT INTO sys.fits_columns(id, name, type, units, number, table_id) \
	 VALUES(%d,'%s','%s','%s',%d,%d);"
#define ATTACHDIR "call sys.fitsattach('%s');"

static void
FITSinitCatalog(mvc *m)
{
	sql_schema *sch;
	sql_table *fits_tp, *fits_fl, *fits_tbl, *fits_col;

	sch = mvc_bind_schema(m, "sys");

	fits_fl = mvc_bind_table(m, sch, "fits_files");
	if (fits_fl == NULL) {
		fits_fl = mvc_create_table(m, sch, "fits_files", tt_table, 0, SQL_PERSIST, 0, 2);
		mvc_create_column_(m, fits_fl, "id", "int", 32);
		mvc_create_column_(m, fits_fl, "name", "varchar", 80);
	}

	fits_tbl = mvc_bind_table(m, sch, "fits_tables");
	if (fits_tbl == NULL) {
		fits_tbl = mvc_create_table(m, sch, "fits_tables", tt_table, 0, SQL_PERSIST, 0, 8);
		mvc_create_column_(m, fits_tbl, "id", "int", 32);
		mvc_create_column_(m, fits_tbl, "name", "varchar", 80);
		mvc_create_column_(m, fits_tbl, "columns", "int", 32);
		mvc_create_column_(m, fits_tbl, "file_id", "int", 32);
		mvc_create_column_(m, fits_tbl, "hdu", "int", 32);
		mvc_create_column_(m, fits_tbl, "date", "varchar", 80);
		mvc_create_column_(m, fits_tbl, "origin", "varchar", 80);
		mvc_create_column_(m, fits_tbl, "comment", "varchar", 80);
	}

	fits_col = mvc_bind_table(m, sch, "fits_columns");
	if (fits_col == NULL) {
		fits_col = mvc_create_table(m, sch, "fits_columns", tt_table, 0, SQL_PERSIST, 0, 6);
		mvc_create_column_(m, fits_col, "id", "int", 32);
		mvc_create_column_(m, fits_col, "name", "varchar", 80);
		mvc_create_column_(m, fits_col, "type", "varchar", 80);
		mvc_create_column_(m, fits_col, "units", "varchar", 80);
		mvc_create_column_(m, fits_col, "number", "int", 32);
		mvc_create_column_(m, fits_col, "table_id", "int", 32);
	}

	fits_tp = mvc_bind_table(m, sch, "fits_table_properties");
	if (fits_tp == NULL) {
		fits_tp = mvc_create_table(m, sch, "fits_table_properties", tt_table, 0, SQL_PERSIST, 0, 5);
		mvc_create_column_(m, fits_tp, "table_id", "int", 32);
		mvc_create_column_(m, fits_tp, "xtension", "varchar", 80);
		mvc_create_column_(m, fits_tp, "bitpix", "int", 32);
		mvc_create_column_(m, fits_tp, "stilvers", "varchar", 80);
		mvc_create_column_(m, fits_tp, "stilclas", "varchar", 80);
	}
}

static int
fits2mtype(int t)
{
	switch (t) {
	case TBIT:
	case TLOGICAL:
		return TYPE_bit;
	case TBYTE:
	case TSBYTE:
		return TYPE_bte;
	case TSTRING:
		return TYPE_str;
	case TUSHORT:
	case TSHORT:
		return TYPE_sht;
	case TUINT:
	case TINT:
		return TYPE_int;
	case TLONG:
	case TULONG:
	case TLONGLONG:
		return TYPE_lng;
	case TFLOAT:
		return TYPE_flt;
	case TDOUBLE:
		return TYPE_dbl;
	/* missing */
	case TCOMPLEX:
	case TDBLCOMPLEX:
		return -1;
	}
	return -1;
}

static int
fits2subtype(sql_subtype *tpe, int t, long rep, long wid) /* type long used by fits library */
{
	(void)rep;
	switch (t) {
	case TBIT:
	case TLOGICAL:
		sql_find_subtype(tpe, "boolean", 0, 0);
		break;
	case TBYTE:
	case TSBYTE:
		sql_find_subtype(tpe, "char", 1, 0);
		break;
	case TSTRING:
		sql_find_subtype(tpe, "varchar", (unsigned int)wid, 0);
		break;
	case TUSHORT:
	case TSHORT:
		sql_find_subtype(tpe, "smallint", 16, 0);
		break;
	case TUINT:
	case TINT:
		sql_find_subtype(tpe, "int", 32, 0);
		break;
	case TULONG:
	case TLONG:
	case TLONGLONG:
		sql_find_subtype(tpe, "bigint", 64, 0);
		break;
	case TFLOAT:
		sql_find_subtype(tpe, "real", 32, 0);
		break;
	case TDOUBLE:
		sql_find_subtype(tpe, "double", 51, 0);
		break;
	/* missing */
	case TCOMPLEX:
	case TDBLCOMPLEX:
		return -1;
	}
	return 1;
}

str FITSexportTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	str tname = *getArgReference_str(stk, pci, 1);
	mvc *m = NULL;
	sql_trans *tr;
	sql_schema *sch;
	sql_table *tbl, *column, *tables = NULL;
	sql_column *col;
	oid rid = oid_nil;
	str type, name, *colname, *tform;
	fitsfile *fptr;
	char filename[BUFSIZ];
	size_t nrows = 0;
	long optimal; /* type long used by fits library */
	rids * rs;

	int tm0, texportboolean=0, texportchar=0, texportstring=0, texportshort=0, texportint=0, texportlng=0, texportfloat=0, texportdouble=0;
	size_t numberrow = 0, dimension = 0;
	int cc = 0, status = 0, j = 0, columns, *fid, block = 0;
	int boolcols = 0, charcols = 0, strcols = 0, shortcols = 0, intcols = 0, lngcols = 0, floatcols = 0, doublecols = 0;
	int hdutype;

	char *charvalue, *readcharrows;
	str strvalue; char **readstrrows;
	short *shortvalue, *readshortrows;
	int *intvalue, *readintrows;
	lng *lngvalue, *readlngrows;
	float *realvalue, *readfloatrows;
	double *doublevalue, *readdoublerows;
	_Bool *boolvalue, *readboolrows;
	struct list * set;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	tr = m->session->tr;
	sch = mvc_bind_schema(m, "sys");

	/* First step: look if the table exists in the database. If the table is not in the database, the export function cannot continue */
 
	tbl = mvc_bind_table(m, sch, tname);
	if (tbl == NULL) {
		msg = createException (MAL, "fits.exporttable", SQLSTATE(FI000) "Table %s is missing.\n", tname);
		return msg;
	}

	set = (*tbl).columns.set;

	columns = list_length(set);
	colname = (str *) GDKmalloc(columns * sizeof(str));
	tform = (str *) GDKmalloc(columns * sizeof(str));
	if (colname == NULL || tform == NULL) {
		GDKfree(colname);
		GDKfree(tform);
		throw(MAL, "fits.exporttable", MAL_MALLOC_FAIL);
	}

	/*	fprintf(stderr,"Number of columns: %d\n", columns);*/

	tables = mvc_bind_table(m, sch, "_tables");
	col = mvc_bind_column(m, tables, "name");
	rid = table_funcs.column_find_row(m->session->tr, col, tname, NULL);

	col = mvc_bind_column(m, tables, "id");
	fid = (int*) table_funcs.column_find_value(m->session->tr, col, rid);

	column =  mvc_bind_table(m, sch, "_columns");
	col = mvc_bind_column(m, column, "table_id");

	rs = table_funcs.rids_select(m->session->tr, col, (void *) fid, (void *) fid, NULL);
	GDKfree(fid);

	while ((rid = table_funcs.rids_next(rs)) != oid_nil)
	{
		col = mvc_bind_column(m, column, "name");
		name = (char *) table_funcs.column_find_value(m->session->tr, col, rid);
		colname[j] = toLower(name);
		GDKfree(name);

		col = mvc_bind_column(m, column, "type");
		type = (char *) table_funcs.column_find_value(m->session->tr, col, rid);

		if (strcmp(type,"boolean")==0) tform[j] = "1L";

 		if (strcmp(type,"char")==0) tform[j] = "1S";

		if (strcmp(type,"varchar")==0) tform[j] = "8A";

		if (strcmp(type,"smallint")==0) tform[j] = "1I";

		if (strcmp(type,"int")==0) tform[j] = "1J";

		if (strcmp(type,"bigint")==0) tform[j] = "1K";

		if (strcmp(type,"real")==0) tform[j] = "1E";

		if (strcmp(type,"double")==0) tform[j] = "1D";
		GDKfree(type);

		j++;
	}

	col = mvc_bind_column(m, tbl, colname[0]);

	nrows = store_funcs.count_col(tr, col, 1);
	assert(nrows <= (size_t) GDK_oid_max);

	snprintf(filename,BUFSIZ,"\n%s.fit",tname);
	fprintf(stderr, "Filename: %s\n", filename);

	remove(filename);

	status=0;

	fits_create_file(&fptr, filename, &status);
	fits_create_img(fptr,  USHORT_IMG, 0, NULL, &status);
	fits_close_file(fptr, &status);
	fits_open_file(&fptr, filename, READWRITE, &status);

	fits_movabs_hdu(fptr, 1, &hdutype, &status);
	fits_create_tbl( fptr, BINARY_TBL, 0, columns, colname, tform, NULL, tname, &status);

	for (cc = 0; cc < columns; cc++)
	{
		char * columntype;
		col = mvc_bind_column(m, tbl, colname[cc]);
		columntype = col -> type.type->sqlname;

		if (strcmp(columntype,"boolean")==0)
		{
			boolcols++; dimension = 0; block = 0;
			fits_get_rowsize(fptr,&optimal,&status);
			readboolrows = (_Bool *) GDKmalloc (sizeof(_Bool) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				boolvalue = (_Bool*) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readboolrows[dimension] = *boolvalue;
				GDKfree(boolvalue);
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col(fptr, TLOGICAL, cc+1, (optimal*block)+1, 1, optimal, readboolrows, &status);
					texportboolean += GDKms() - tm0;
					GDKfree(readboolrows);
					readboolrows = (_Bool *) GDKmalloc (sizeof(_Bool) * optimal);
					block++;
				}
			}
			tm0 = GDKms();
			fits_write_col(fptr, TLOGICAL, cc+1, (optimal*block)+1, 1, dimension, readboolrows, &status);
			texportboolean += GDKms() - tm0;
			GDKfree(readboolrows);		
		}

		if (strcmp(columntype,"char")==0)
		{
			charcols++; dimension = 0; block = 0;
			fits_get_rowsize(fptr,&optimal,&status);
			readcharrows = (char *) GDKmalloc (sizeof(char) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				charvalue = (char*) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readcharrows[dimension] = *charvalue;
				GDKfree(charvalue);
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col(fptr, TBYTE, cc+1, (optimal*block)+1, 1, optimal, readcharrows, &status);
					texportchar += GDKms() - tm0;
					GDKfree(readcharrows);
					readcharrows = (char *) GDKmalloc (sizeof(char) * optimal);
					block++;
				}
			}
			tm0 = GDKms();
			fits_write_col(fptr, TBYTE, cc+1, (optimal*block)+1, 1, dimension, readcharrows, &status);
			texportchar += GDKms() - tm0;
			GDKfree(readcharrows);
		}

		if (strcmp(columntype,"varchar")==0)
		{
			strcols++; dimension=0; block=0;
			fits_get_rowsize(fptr,&optimal,&status);
			readstrrows = (char **) GDKmalloc (sizeof(char *) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				strvalue = (char *) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readstrrows[dimension] = strvalue;
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col_str(fptr, cc+1, (optimal*block)+1, 1, optimal, readstrrows, &status);
					texportstring += GDKms() - tm0;
					for (dimension = 0; dimension < (size_t) optimal; dimension++)
						GDKfree(readstrrows[dimension]);
					dimension = 0;
					GDKfree(readstrrows);
					readstrrows = (char **) GDKmalloc(sizeof(char *) * optimal);
					block++;
				}
			}
			tm0 = GDKms();
			fits_write_col_str(fptr, cc+1, (optimal*block)+1, 1, dimension, readstrrows, &status);
			texportstring += GDKms() - tm0;
			for (numberrow = 0; numberrow < dimension; numberrow++)
				GDKfree(readstrrows[numberrow]);
			GDKfree(readstrrows);
		}

		if (strcmp(columntype,"smallint")==0)
		{
			shortcols++; dimension = 0; block = 0;
			fits_get_rowsize(fptr,&optimal,&status);
			readshortrows = (short *) GDKmalloc (sizeof(short) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				shortvalue = (short*) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readshortrows[dimension] = *shortvalue;
				GDKfree(shortvalue);
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col(fptr, TSHORT, cc+1, (optimal*block)+1, 1, optimal, readshortrows, &status);
					texportshort += GDKms() - tm0;
					GDKfree(readshortrows);
					readshortrows = (short *) GDKmalloc (sizeof(short) * optimal);
					block++;
				}
			} 
			tm0 = GDKms();
			fits_write_col(fptr, TSHORT, cc+1, (optimal*block)+1, 1, dimension, readshortrows, &status);
			texportshort += GDKms() - tm0;
			GDKfree(readshortrows);
		}

		if (strcmp(columntype,"int")==0)
		{
			intcols++; dimension = 0; block = 0;
			fits_get_rowsize(fptr,&optimal,&status);
			readintrows = (int *) GDKmalloc (sizeof(int) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				intvalue = (int*) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readintrows[dimension] = *intvalue;
				GDKfree(intvalue);
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col(fptr, TINT, cc+1, (optimal*block)+1, 1, optimal, readintrows, &status);
					texportint += GDKms() - tm0;
					GDKfree(readintrows);
					readintrows = (int *) GDKmalloc (sizeof(int) * optimal);
					block++;
				}
			} 
			tm0 = GDKms();
			fits_write_col(fptr, TINT, cc+1, (optimal*block)+1, 1, dimension, readintrows, &status);
			texportint += GDKms() - tm0;
			GDKfree(readintrows);	
		}

		if (strcmp(columntype,"bigint")==0)
		{
			lngcols++; dimension = 0; block = 0;
			fits_get_rowsize(fptr,&optimal,&status);
			readlngrows = (lng *) GDKmalloc (sizeof(lng) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				lngvalue = (lng*) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readlngrows[dimension] = *lngvalue;
				GDKfree(lngvalue);
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col(fptr, TLONG, cc+1, (optimal*block)+1, 1, optimal, readlngrows, &status);
					texportlng += GDKms() - tm0;
					GDKfree(readlngrows);
					readlngrows = (lng *) GDKmalloc (sizeof(lng) * optimal);
					block++;
				}
			} 
			tm0 = GDKms();
			fits_write_col(fptr, TLONG, cc+1, (optimal*block)+1, 1, dimension, readlngrows, &status);
			texportlng += GDKms() - tm0;
			GDKfree(readlngrows);
		}

		if (strcmp(columntype,"real")==0)
		{
			floatcols++; dimension = 0; block = 0;
			fits_get_rowsize(fptr,&optimal,&status);
			readfloatrows = (float *) GDKmalloc (sizeof(float) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				realvalue = (float*) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readfloatrows[dimension] = *realvalue;
				GDKfree(realvalue);
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col(fptr, TFLOAT, cc+1, (optimal*block)+1, 1, optimal, readfloatrows, &status);
					texportfloat += GDKms() - tm0;
					GDKfree(readfloatrows);
					readfloatrows = (float *) GDKmalloc (sizeof(float) * optimal);
					block++;
				}
			} 
			tm0 = GDKms();
			fits_write_col(fptr, TFLOAT, cc+1, (optimal*block)+1, 1, dimension, readfloatrows, &status);
			texportfloat += GDKms() - tm0;
			GDKfree(readfloatrows);
		}

		if (strcmp(columntype,"double")==0)
		{
			doublecols++; dimension = 0; block = 0;
			fits_get_rowsize(fptr,&optimal,&status);
			readdoublerows = (double *) GDKmalloc (sizeof(double) * optimal);

			for (numberrow = 0; numberrow < nrows ; numberrow++)
			{
				doublevalue = (double*) table_funcs.column_find_value(m->session->tr, col, (oid) numberrow);
				readdoublerows[dimension] = *doublevalue;
				GDKfree(doublevalue);
				dimension++;

				if (dimension == (size_t) optimal)
				{
					dimension = 0;
					tm0 = GDKms();
					fits_write_col(fptr, TDOUBLE, cc+1, (optimal*block)+1, 1, optimal, readdoublerows, &status);
					texportdouble += GDKms() - tm0;
					GDKfree(readdoublerows);
					readdoublerows = (double *) GDKmalloc (sizeof(double) * optimal);
					block++;
				}
			}
			tm0 = GDKms();
			fits_write_col(fptr, TDOUBLE, cc+1, (optimal*block)+1, 1, optimal, readdoublerows, &status);
			texportdouble += GDKms() - tm0;
			GDKfree(readdoublerows); 
		}
	}

	/* print all the times that were needed to export each one of the columns
		
	fprintf(stderr, "\n\n");
	if (texportboolean > 0)		fprintf(stderr, "%d Boolean\tcolumn(s) exported in %d ms\n", boolcols,   texportboolean);
	if (texportchar > 0)		fprintf(stderr, "%d Char\t\tcolumn(s) exported in %d ms\n",    charcols,   texportchar);
	if (texportstring > 0)		fprintf(stderr, "%d String\tcolumn(s) exported in %d ms\n",  strcols,    texportstring);
	if (texportshort > 0)		fprintf(stderr, "%d Short\t\tcolumn(s) exported in %d ms\n",   shortcols,  texportshort);
	if (texportint > 0)		fprintf(stderr, "%d Integer\tcolumn(s) exported in %d ms\n", intcols,    texportint);
	if (texportlng > 0)		fprintf(stderr, "%d Long\t\tcolumn(s) exported in %d ms\n",    lngcols,   texportlng);
	if (texportfloat > 0)		fprintf(stderr, "%d Float\t\tcolumn(s) exported in %d ms\n",   floatcols,  texportfloat);
	if (texportdouble > 0) 		fprintf(stderr, "%d Double\tcolumn(s) exported in %d ms\n",  doublecols, texportdouble);
	*/

	fits_close_file(fptr, &status);
	return msg;
}


str FITSdir(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	str dir = *getArgReference_str(stk, pci, 1);
	DIR *dp;
	struct dirent *ep;
	fitsfile *fptr;
	char *s;
	int status = 0;
	(void)mb;

	dp = opendir(dir);
	if (dp != NULL) {
		char stmt[BUFSIZ];
		char fname[BUFSIZ];

		s = stmt;

		while ((ep = readdir(dp)) != NULL && !msg) {
			char *filename = SQLescapeString(ep->d_name);
			if (!filename) {
				msg = createException(MAL, "fits.listdir", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				break;
			}

			snprintf(fname, BUFSIZ, "%s%s", dir, filename);
			status = 0;
			fits_open_file(&fptr, fname, READONLY, &status);
			if (status == 0) {
				snprintf(stmt, BUFSIZ, ATTACHDIR, fname);
#ifndef NDEBUG
				fprintf(stderr, "Executing:\n%s\n", s);
#endif
				msg = SQLstatementIntern(cntxt, &s, "fits.listofdir", TRUE, FALSE, NULL);
				fits_close_file(fptr, &status);
			}

			GDKfree(filename);
		}
		(void)closedir(dp);
	} else
		msg = createException(MAL, "listdir", SQLSTATE(FI000) "Couldn't open the directory");

	return msg;
}

str FITSdirpat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	str dir = *getArgReference_str(stk, pci, 1);
	str pat = *getArgReference_str(stk, pci, 2);
	char *filename = NULL;
	fitsfile *fptr;
	char *s;
	int status = 0;
	glob_t globbuf;
	char fulldirectory[BUFSIZ];
	size_t j = 0;

	(void)mb;

	globbuf.gl_offs = 0;
	snprintf(fulldirectory, BUFSIZ, "%s%s", dir, pat);
	glob(fulldirectory, GLOB_DOOFFS, NULL, &globbuf);

	/*	fprintf(stderr,"#fulldir: %s \nSize: %lu\n",fulldirectory, globbuf.gl_pathc);*/

	if (globbuf.gl_pathc == 0)
		throw(MAL, "fits.listdirpat", SQLSTATE(FI000) "Couldn't open the directory or there are no files that match the pattern");

	for (j = 0; j < globbuf.gl_pathc; j++) {
		char stmt[BUFSIZ];
		char fname[BUFSIZ];

		s = stmt;
		snprintf(fname, BUFSIZ, "%s", globbuf.gl_pathv[j]);
		filename = SQLescapeString(fname);
		if (!filename) {
			throw(MAL, "fits.listdirpat", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		status = 0;
		fits_open_file(&fptr, filename, READONLY, &status);
		if (status == 0) {
			snprintf(stmt, BUFSIZ, ATTACHDIR, filename);
			GDKfree(filename);
#ifndef NDEBUG
			fprintf(stderr, "Executing:\n%s\n", s);
#endif
			msg = SQLstatementIntern(cntxt, &s, "fits.listofdirpat", TRUE, FALSE, NULL);
			fits_close_file(fptr, &status);

			break;
		}
	}

	return msg;
}


str
FITStest(int *res, str *fname)
{
	fitsfile *fptr;       /* pointer to the FITS file, defined in fitsio.h */
	str msg = MAL_SUCCEED;
	int status = 0, hdutype;

	*res = 0;
	if (fits_open_file(&fptr, *fname, READONLY, &status))
		msg = createException(MAL, "fits.test", SQLSTATE(FI000) "Missing FITS file %s", *fname);
	else {
		fits_movabs_hdu(fptr, 2, &hdutype, &status);
		*res = hdutype;
		fits_close_file(fptr, &status);
	}

	return msg;
}

str FITSattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_trans *tr;
	sql_schema *sch;
	sql_table *fits_tp, *fits_fl, *fits_tbl, *fits_col, *tbl = NULL;
	sql_column *col;
	str msg = MAL_SUCCEED;
	str fname = *getArgReference_str(stk, pci, 1);
	fitsfile *fptr;  /* pointer to the FITS file */
	int status = 0, i, j, hdutype, hdunum = 1, cnum = 0, bitpixnumber = 0;
	oid fid, tid, cid, rid = oid_nil;
	char tname[BUFSIZ], *tname_low = NULL, *s, bname[BUFSIZ], stmt[BUFSIZ];
	long tbcol; /* type long used by fits library */
	char cname[BUFSIZ], tform[BUFSIZ], tunit[BUFSIZ], tnull[BUFSIZ], tdisp[BUFSIZ];
	char *esc_cname, *esc_tform, *esc_tunit;
	double tscal, tzero;
	char xtensionname[BUFSIZ] = "", stilversion[BUFSIZ] = "";
	char stilclass[BUFSIZ] = "", tdate[BUFSIZ] = "", orig[BUFSIZ] = "", comm[BUFSIZ] = "";

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	if (fits_open_file(&fptr, fname, READONLY, &status)) {
		msg = createException(MAL, "fits.attach", SQLSTATE(FI000) "Missing FITS file %s.\n", fname);
		return msg;
	}

	tr = m->session->tr;
	sch = mvc_bind_schema(m, "sys");

	fits_fl = mvc_bind_table(m, sch, "fits_files");
	if (fits_fl == NULL)
		FITSinitCatalog(m);

	fits_fl = mvc_bind_table(m, sch, "fits_files");
	fits_tbl = mvc_bind_table(m, sch, "fits_tables");
	fits_col = mvc_bind_table(m, sch, "fits_columns");
	fits_tp = mvc_bind_table(m, sch, "fits_table_properties");

	/* check if the file is already attached */
	col = mvc_bind_column(m, fits_fl, "name");
	rid = table_funcs.column_find_row(m->session->tr, col, fname, NULL);
	if (rid != oid_nil) {
		fits_close_file(fptr, &status);
		msg = createException(SQL, "fits.attach", SQLSTATE(FI000) "File %s already attached\n", fname);
		return msg;
	}

	/* add row in the fits_files catalog table */
	col = mvc_bind_column(m, fits_fl, "id");
	fid = store_funcs.count_col(tr, col, 1) + 1;
	store_funcs.append_col(m->session->tr,
		mvc_bind_column(m, fits_fl, "id"), &fid, TYPE_int);
	store_funcs.append_col(m->session->tr,
		mvc_bind_column(m, fits_fl, "name"), fname, TYPE_str);

	col = mvc_bind_column(m, fits_tbl, "id");
	tid = store_funcs.count_col(tr, col, 1) + 1;

	if ((s = strrchr(fname, DIR_SEP)) == NULL)
		s = fname;
	else
		s++;
	strcpy(bname, s);
	s = strrchr(bname, '.');
	if (s) *s = 0;

	fits_get_num_hdus(fptr, &hdunum, &status);
	for (i = 1; i <= hdunum; i++) {
		fits_movabs_hdu(fptr, i, &hdutype, &status);
		if (hdutype != ASCII_TBL && hdutype != BINARY_TBL)
			continue;

		/* SQL table name - the name of FITS extention */
		fits_read_key(fptr, TSTRING, "EXTNAME", tname, NULL, &status);
		if (status) {
			snprintf(tname, BUFSIZ, "%s_%d", bname, i);
			tname_low = toLower(tname);
			status = 0;
		}else  { /* check table name for existence in the fits catalog */
			tname_low = toLower(tname);
			col = mvc_bind_column(m, fits_tbl, "name");
			rid = table_funcs.column_find_row(m->session->tr, col, tname_low, NULL);
			/* or as regular SQL table */
			tbl = mvc_bind_table(m, sch, tname_low);
			if (rid != oid_nil || tbl) {
				snprintf(tname, BUFSIZ, "%s_%d", bname, i);
				tname_low = toLower(tname);
			}
		}

		fits_read_key(fptr, TSTRING, "BITPIX", &bitpixnumber, NULL, &status);
		if (status) {
			status = 0;
		}
		fits_read_key(fptr, TSTRING, "DATE-HDU", tdate, NULL, &status);
		if (status) {
			status = 0;
		}
		fits_read_key(fptr, TSTRING, "XTENSION", xtensionname, NULL, &status);
		if (status) {
			status = 0;
		}
		fits_read_key(fptr, TSTRING, "STILVERS", stilversion, NULL, &status);
		if (status) {
			status = 0;
		}
		fits_read_key(fptr, TSTRING, "STILCLAS", stilclass, NULL, &status);
		if (status) {
			status = 0;
		}
		fits_read_key(fptr, TSTRING, "ORIGIN", orig, NULL, &status);
		if (status) {
			status = 0;
		}
		fits_read_key(fptr, TSTRING, "COMMENT", comm, NULL, &status);
		if (status) {
			status = 0;
		}

		fits_get_num_cols(fptr, &cnum, &status);

		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "id"), &tid, TYPE_int);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "name"), tname_low, TYPE_str);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "columns"), &cnum, TYPE_int);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "file_id"), &fid, TYPE_int);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "hdu"), &i, TYPE_int);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "date"), tdate, TYPE_str);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "origin"), orig, TYPE_str);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tbl, "comment"), comm, TYPE_str);

		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tp, "table_id"), &tid, TYPE_int);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tp, "xtension"), xtensionname, TYPE_str);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tp, "bitpix"), &bitpixnumber, TYPE_int);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tp, "stilvers"), stilversion, TYPE_str);
		store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, fits_tp, "stilclas"), stilclass, TYPE_str);

		/* read columns description */
		s = stmt;
		col = mvc_bind_column(m, fits_col, "id");
		cid = store_funcs.count_col(tr, col, 1) + 1;
		for (j = 1; j <= cnum; j++, cid++) {
			fits_get_acolparms(fptr, j, cname, &tbcol, tunit, tform, &tscal, &tzero, tnull, tdisp, &status);
			/* escape the various strings to avoid SQL injection attacks */
			esc_cname = SQLescapeString(cname);
			if (!esc_cname) {
				throw(MAL, "fits.attach", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			esc_tform = SQLescapeString(tform);
			if (!esc_tform) {
				GDKfree(esc_cname);
				throw(MAL, "fits.attach", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			esc_tunit = SQLescapeString(tunit);
			if (!esc_tform) {
				GDKfree(esc_tform);
				GDKfree(esc_cname);
				throw(MAL, "fits.attach", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			snprintf(stmt, BUFSIZ, FITS_INS_COL, (int)cid, esc_cname, esc_tform, esc_tunit, j, (int)tid);
			GDKfree(esc_tunit);
			GDKfree(esc_tform);
			GDKfree(esc_cname);
			msg = SQLstatementIntern(cntxt, &s, "fits.attach", TRUE, FALSE, NULL);
			if (msg != MAL_SUCCEED) {
				fits_close_file(fptr, &status);
				return msg;
			}
		}
		tid++;
	}
	fits_close_file(fptr, &status);

	return MAL_SUCCEED;
}

str FITSloadTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch;
	sql_table *fits_fl, *fits_tbl, *tbl = NULL;
	sql_column *col;
	sql_subtype tpe;
	fitsfile *fptr;
	str tname = *getArgReference_str(stk, pci, 1);
	str fname;
	str msg = MAL_SUCCEED;
	oid rid = oid_nil, frid = oid_nil;
	int status = 0, cnum = 0, *fid, *hdu, hdutype, j, anynull = 0, mtype;
	int *tpcode = NULL;
	long *rep = NULL, *wid = NULL, rows; /* type long used by fits library */
	char keywrd[80], **cname, nm[FLEN_VALUE];
	const void *nilptr;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;
	sch = mvc_bind_schema(m, "sys");

	fits_tbl = mvc_bind_table(m, sch, "fits_tables");
	if (fits_tbl == NULL) {
		msg = createException(MAL, "fits.loadtable", SQLSTATE(FI000) "FITS catalog is missing.\n");
		return msg;
	}

	tbl = mvc_bind_table(m, sch, tname);
	if (tbl) {
		msg = createException(MAL, "fits.loadtable", SQLSTATE(FI000) "Table %s is already created.\n", tname);
		return msg;
	}

	col = mvc_bind_column(m, fits_tbl, "name");
	rid = table_funcs.column_find_row(m->session->tr, col, tname, NULL);
	if (rid == oid_nil) {
		msg = createException(MAL, "fits.loadtable", SQLSTATE(FI000) "Table %s is unknown in FITS catalog. Attach first the containing file\n", tname);
		return msg;
	}

	/* Open FITS file and move to the table HDU */
	col = mvc_bind_column(m, fits_tbl, "file_id");
	fid = (int*)table_funcs.column_find_value(m->session->tr, col, rid);

	fits_fl = mvc_bind_table(m, sch, "fits_files");
	col = mvc_bind_column(m, fits_fl, "id");
	frid = table_funcs.column_find_row(m->session->tr, col, (void *)fid, NULL);
	GDKfree(fid);
	col = mvc_bind_column(m, fits_fl, "name");
	fname = (char *)table_funcs.column_find_value(m->session->tr, col, frid);
	if (fits_open_file(&fptr, fname, READONLY, &status)) {
		msg = createException(MAL, "fits.loadtable", SQLSTATE(FI000) "Missing FITS file %s.\n", fname);
		GDKfree(fname);
		return msg;
	}
	GDKfree(fname);

	col = mvc_bind_column(m, fits_tbl, "hdu");
	hdu = (int*)table_funcs.column_find_value(m->session->tr, col, rid);
	fits_movabs_hdu(fptr, *hdu, &hdutype, &status);
	if (hdutype != ASCII_TBL && hdutype != BINARY_TBL) {
		msg = createException(MAL, "fits.loadtable", SQLSTATE(FI000) "HDU %d is not a table.\n", *hdu);
		GDKfree(hdu);
		fits_close_file(fptr, &status);
		return msg;
	}
	GDKfree(hdu);

	/* create a SQL table to hold the FITS table */
	/*	col = mvc_bind_column(m, fits_tbl, "columns");
	   cnum = *(int*) table_funcs.column_find_value(m->session->tr, col, rid); */
	fits_get_num_cols(fptr, &cnum, &status);
	tbl = mvc_create_table(m, sch, tname, tt_table, 0, SQL_PERSIST, 0, cnum);

	tpcode = (int *)GDKzalloc(sizeof(int) * cnum);
	rep = (long *)GDKzalloc(sizeof(long) * cnum);
	wid = (long *)GDKzalloc(sizeof(long) * cnum);
	cname = (char **)GDKzalloc(sizeof(char *) * cnum);

	for (j = 1; j <= cnum; j++) {
		/*		fits_get_acolparms(fptr, j, cname, &tbcol, tunit, tform, &tscal, &tzero, tnull, tdisp, &status); */
		snprintf(keywrd, 80, "TTYPE%d", j);
		fits_read_key(fptr, TSTRING, keywrd, nm, NULL, &status);
		if (status) {
			snprintf(nm, FLEN_VALUE, "column_%d", j);
			status = 0;
		}
		cname[j - 1] = toLower(nm);
		fits_get_coltype(fptr, j, &tpcode[j - 1], &rep[j - 1], &wid[j - 1], &status);
		fits2subtype(&tpe, tpcode[j - 1], rep[j - 1], wid[j - 1]);

		/*		fprintf(stderr,"#%d %ld %ld - M: %s\n", tpcode[j-1], rep[j-1], wid[j-1], tpe.type->sqlname); */

		mvc_create_column(m, tbl, cname[j - 1], &tpe);
	}

	/* data load */
	fits_get_num_rows(fptr, &rows, &status);
	fprintf(stderr,"#Loading %ld rows in table %s\n", rows, tname);
	for (j = 1; j <= cnum; j++) {
		BAT *tmp = NULL;
		int time0 = GDKms();
		mtype = fits2mtype(tpcode[j - 1]);
		nilptr = ATOMnilptr(mtype);
		col = mvc_bind_column(m, tbl, cname[j - 1]);

		tmp = COLnew(0, mtype, rows, TRANSIENT);
		if ( tmp == NULL){
			GDKfree(tpcode);
			GDKfree(rep);
			GDKfree(wid);
			GDKfree(cname);
			throw(MAL,"fits.load", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		if (mtype != TYPE_str) {
			fits_read_col(fptr, tpcode[j - 1], j, 1, 1, rows, (void *) nilptr, (void *)BUNtloc(bat_iterator(tmp), 0), &anynull, &status);
			BATsetcount(tmp, rows);
			tmp->tsorted = 0;
			tmp->trevsorted = 0;
		} else {
/*			char *v = GDKzalloc(wid[j-1]);*/
			/* type long demanded by "rows", i.e., by fits library */
			long bsize = 50, batch = bsize, k, i;
			int tm0, tloadtm = 0, tattachtm = 0;
			char **v = (char **) GDKzalloc(sizeof(char *) * bsize);
			for(i = 0; i < bsize; i++)
				v[i] = GDKzalloc(wid[j-1]);
			for(i = 0; i < rows; i += batch) {
				batch = rows - i < bsize ? rows - i: bsize;
				tm0 = GDKms();
				fits_read_col(fptr, tpcode[j - 1], j, 1 + i, 1, batch, (void *) nilptr, (void *)v, &anynull, &status);
				tloadtm += GDKms() - tm0;
				tm0 = GDKms();
				for(k = 0; k < batch ; k++)
					if (BUNappend(tmp, v[k], TRUE) != GDK_SUCCEED) {
						BBPreclaim(tmp);
						msg = createException(MAL, "fits.loadtable", SQLSTATE(HY001) MAL_MALLOC_FAIL);
						goto bailout;
					}
				tattachtm += GDKms() - tm0;
			}
			for(i = 0; i < bsize ; i++)
				GDKfree(v[i]);
			GDKfree(v);
			fprintf(stderr,"#String column load %d ms, BUNappend %d ms\n", tloadtm, tattachtm);
		}

		if (status) {
			char buf[FLEN_ERRMSG + 1];
			fits_read_errmsg(buf);
			msg = createException(MAL, "fits.loadtable", SQLSTATE(FI000) "Cannot load column %s of %s table: %s.\n", cname[j - 1], tname, buf);
			break;
		}
		fprintf(stderr,"#Column %s loaded for %d ms\t", cname[j-1], GDKms() - time0);
		if (store_funcs.append_col(m->session->tr, col, tmp, TYPE_bat) != LOG_OK) {
			BBPunfix(tmp->batCacheid);
			msg = createException(MAL, "fits.loadtable", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			break;
		}
		fprintf(stderr,"#Total %d ms\n", GDKms() - time0);
		BBPunfix(tmp->batCacheid);
	}

  bailout:

	GDKfree(tpcode);
	GDKfree(rep);
	GDKfree(wid);
	GDKfree(cname);

	fits_close_file(fptr, &status);
	return msg;
}

