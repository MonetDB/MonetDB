/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * Author: K. Kyzirakos
 *
 * This module contains primitives for accessing data in LiDAR file format.
 */

#include "monetdb_config.h"
#include "glob.h"

/* clash with GDK? */
// #undef ttype

#include <liblas/capi/liblas.h>
#include <liblas/capi/las_version.h>
#include <liblas/capi/las_config.h>

#include "lidar.h"
#include "mutils.h"
#include "sql_mvc.h"
#include "sql_scenario.h"
#include "sql.h"
#include "clients.h"
#include "mal_exception.h"

#define LIDAR_INS_COL "INSERT INTO lidar_columns(id, name, type, units, number, table_id) \
	 VALUES(%d,'%s','%s','%s',%d,%d);"
#define FILE_INS "INSERT INTO lidar_files(id, name) VALUES (%d, '%s');"
#define DEL_TABLE "DELETE FROM lidarfiles;"
#define ATTACHDIR "CALL lidarattach('%s');"

static MT_Lock mt_lidar_lock MT_LOCK_INITIALIZER("mt_lidar_lock");

#ifndef NDEBUG
static
void print_lidar_header(FILE *file, LASHeaderH header, const char* file_name, int bSkipVLR, int bWKT) {

    char *pszSignature = NULL;
    char *pszProjectId = NULL;
    char *pszSystemId = NULL;
    char *pszSoftwareId = NULL;
    char *pszProj4 = NULL;
    char *pszWKT = NULL;
    char *pszVLRUser = NULL;
    char *pszVLRDescription = NULL;
    unsigned short nVLRLength = 0;
    unsigned short nVLRRecordId = 0;
    LASVLRH pVLR = NULL;
    LASSRSH pSRS = NULL;
    unsigned int nVLR = 0;
    int i = 0;

    unsigned char minor, major;

    pszSignature = LASHeader_GetFileSignature(header);
    pszProjectId = LASHeader_GetProjectId(header);
    pszSystemId = LASHeader_GetSystemId(header);
    pszSoftwareId = LASHeader_GetSoftwareId(header);

    pSRS = LASHeader_GetSRS(header);
    pszProj4 = LASSRS_GetProj4(pSRS);
    pszWKT = LASSRS_GetWKT_CompoundOK(pSRS);

    nVLR = LASHeader_GetRecordsCount(header);
    major = LASHeader_GetVersionMajor(header);
    minor = LASHeader_GetVersionMinor(header);

    fprintf(file, "\n---------------------------------------------------------\n");
    fprintf(file, "  Header Summary\n");
    fprintf(file, "---------------------------------------------------------\n");

    fprintf(file, "  File Name: %s\n", file_name);

    if (strcmp(pszSignature,"LASF") !=0) {
        LASError_Print("File signature is not 'LASF'... aborting");
        exit(1);
    }
    fprintf(file, "  Version:                    %hhu.%hhu\n", major, minor);

    fprintf(file, "  Source ID:                  %hu\n", 
                    LASHeader_GetFileSourceId(header) ) ;

    fprintf(file, "  Reserved:                   %hu\n", 
                    LASHeader_GetReserved(header) );

    fprintf(file, "  Project ID/GUID:           '%s'\n", 
                    pszProjectId);

    fprintf(file, "  System Identifier:         '%s'\n", 
                    pszSystemId);

    fprintf(file, "  Generating Software:       '%s'\n", 
                    pszSoftwareId);

    fprintf(file, "  File Creation Day/Year:    %hu/%hu\n", 
                    LASHeader_GetCreationDOY(header), 
                    LASHeader_GetCreationYear(header));

    fprintf(file, "  Header Size                %hu\n", 
                    LASHeader_GetHeaderSize(header));

    fprintf(file, "  Offset to Point Data       %u\n", 
                    LASHeader_GetDataOffset(header));

    fprintf(file, "  Number Var. Length Records %u\n", 
                    LASHeader_GetRecordsCount(header));

    fprintf(file, "  Point Data Format          %hhu\n", 
                    LASHeader_GetDataFormatId(header));

    fprintf(file, "  Point Data Record Length   %hu\n", 
                    LASHeader_GetDataRecordLength(header));

    fprintf(file, "  Number of Point Records    %u\n", 
                    LASHeader_GetPointRecordsCount(header));

    fprintf(file, "  Number of Points by Return %u %u %u %u %u\n", 
                    LASHeader_GetPointRecordsByReturnCount(header, 0), 
                    LASHeader_GetPointRecordsByReturnCount(header, 1), 
                    LASHeader_GetPointRecordsByReturnCount(header, 2), 
                    LASHeader_GetPointRecordsByReturnCount(header, 3), 
                    LASHeader_GetPointRecordsByReturnCount(header, 4));

    fprintf(file, "  Scale Factor X Y Z         %.6g %.6g %.6g\n", 
                    LASHeader_GetScaleX(header), 
                    LASHeader_GetScaleY(header),
                    LASHeader_GetScaleZ(header));

    fprintf(file, "  Offset X Y Z               %.6f %.6f %.6f\n", 
                    LASHeader_GetOffsetX(header), 
                    LASHeader_GetOffsetY(header), 
                    LASHeader_GetOffsetZ(header));

    fprintf(file, "  Min X Y Z                  %.6f %.6f %.6f\n",
                    LASHeader_GetMinX(header), 
                    LASHeader_GetMinY(header), 
                    LASHeader_GetMinZ(header));

    fprintf(file, "  Max X Y Z                  %.6f %.6f %.6f\n", 
                    LASHeader_GetMaxX(header), 
                    LASHeader_GetMaxY(header), 
                    LASHeader_GetMaxZ(header));
    
    fprintf(file, " Spatial Reference           %s\n",
                    pszProj4);
    if (bWKT)
    {
        fprintf(file, "%s", pszWKT);
        fprintf(file, "%s", "\n");
    }
    if (nVLR && !bSkipVLR) {
        
    fprintf(file, "\n---------------------------------------------------------\n");
    fprintf(file, "  VLR Summary\n");
    fprintf(file, "---------------------------------------------------------\n");

        for (i = 0; i < (int)nVLR; i++) {
            pVLR = LASHeader_GetVLR(header, i);

            if (LASError_GetLastErrorNum()) {
                LASError_Print("Unable to fetch VLR");
                exit(1);
            }
            
            pszVLRUser = LASVLR_GetUserId(pVLR);
            pszVLRDescription = LASVLR_GetDescription(pVLR);
            nVLRLength = LASVLR_GetRecordLength(pVLR);
            nVLRRecordId = LASVLR_GetRecordId(pVLR);
            

            fprintf(file, "   User: '%s' - Description: '%s'\n", pszVLRUser, pszVLRDescription);
            fprintf(file, "   ID: %hu Length: %hu\n\n", nVLRRecordId, nVLRLength);
           
            MT_lock_set(&mt_lidar_lock); 
            LASVLR_Destroy(pVLR);
            MT_lock_unset(&mt_lidar_lock);
            pVLR = NULL;
            
            LASString_Free(pszVLRUser);
            LASString_Free(pszVLRDescription);
        }
        
    }
    LASString_Free(pszSignature);
    LASString_Free(pszProjectId);
    LASString_Free(pszSystemId);
    LASString_Free(pszSoftwareId);
    LASString_Free(pszProj4);
    LASString_Free(pszWKT);
    MT_lock_set(&mt_lidar_lock);
    LASSRS_Destroy(pSRS);
    MT_lock_unset(&mt_lidar_lock);
}
#endif

static void
LIDARinitCatalog(mvc *m)
{
	sql_schema *sch;
	sql_table *lidar_fl, *lidar_tbl, *lidar_col;

	sch = mvc_bind_schema(m, "sys");

	lidar_fl = mvc_bind_table(m, sch, "lidar_files");
	if (lidar_fl == NULL) {
		lidar_fl = mvc_create_table(m, sch, "lidar_files", tt_table, 0, SQL_PERSIST, 0, 2, 0);
		mvc_create_column_(m, lidar_fl, "id", "int", 32);
		mvc_create_column_(m, lidar_fl, "name", "varchar", 255);
	}
	
	lidar_tbl = mvc_bind_table(m, sch, "lidar_tables");
	if (lidar_tbl == NULL) {
		lidar_tbl = mvc_create_table(m, sch, "lidar_tables", tt_table, 0, SQL_PERSIST, 0, 21, 0);
		mvc_create_column_(m, lidar_tbl, "id", "int", 32);
		mvc_create_column_(m, lidar_tbl, "file_id", "int", 32);
		mvc_create_column_(m, lidar_tbl, "name", "varchar", 255);
		mvc_create_column_(m, lidar_tbl, "FileSourceId", "int", 32);
		mvc_create_column_(m, lidar_tbl, "VersionMajor", "char", 1);
		mvc_create_column_(m, lidar_tbl, "VersionMinor", "char", 1);
		mvc_create_column_(m, lidar_tbl, "DataFormatId", "char", 1);
		mvc_create_column_(m, lidar_tbl, "CreationDOY", "int", 32);
		mvc_create_column_(m, lidar_tbl, "CreationYear", "int", 32);
		mvc_create_column_(m, lidar_tbl, "RecordsCount", "int", 32);
		mvc_create_column_(m, lidar_tbl, "PointRecordsCount", "int", 32);
		mvc_create_column_(m, lidar_tbl, "DataOffset", "int", 32);
		mvc_create_column_(m, lidar_tbl, "HeaderPadding", "int", 32);
		mvc_create_column_(m, lidar_tbl, "Reserved", "int", 32);
		mvc_create_column_(m, lidar_tbl, "DataRecordLength", "int", 32);
		mvc_create_column_(m, lidar_tbl, "HeaderSize", "int", 32);
		mvc_create_column_(m, lidar_tbl, "ByteSize", "int", 32);
		mvc_create_column_(m, lidar_tbl, "BaseByteSize", "int", 32);
		mvc_create_column_(m, lidar_tbl, "WKT", "varchar", 255);
		mvc_create_column_(m, lidar_tbl, "WKT_CompoundOK", "varchar", 255);
		mvc_create_column_(m, lidar_tbl, "Proj4", "varchar", 255);
	}

	lidar_col = mvc_bind_table(m, sch, "lidar_columns");
	if (lidar_col == NULL) {
		lidar_col = mvc_create_table(m, sch, "lidar_columns", tt_table, 0, SQL_PERSIST, 0, 15, 0);
		mvc_create_column_(m, lidar_col, "id", "int", 32);
		mvc_create_column_(m, lidar_col, "file_id", "int", 32);
		mvc_create_column_(m, lidar_col, "table_id", "int", 32);
		mvc_create_column_(m, lidar_col, "ScaleX", "double", 64);
		mvc_create_column_(m, lidar_col, "ScaleY", "double", 64);
		mvc_create_column_(m, lidar_col, "ScaleZ", "double", 64);
		mvc_create_column_(m, lidar_col, "OffsetX", "double", 64);
		mvc_create_column_(m, lidar_col, "OffsetY", "double", 64);
		mvc_create_column_(m, lidar_col, "OffsetZ", "double", 64);
		mvc_create_column_(m, lidar_col, "MinX", "double", 64);
		mvc_create_column_(m, lidar_col, "MinY", "double", 64);
		mvc_create_column_(m, lidar_col, "MinZ", "double", 64);
		mvc_create_column_(m, lidar_col, "MaxX", "double", 64);
		mvc_create_column_(m, lidar_col, "MaxY", "double", 64);
		mvc_create_column_(m, lidar_col, "MaxZ", "double", 64);
	}
}

#if 0
static int
lidar2mtype(int t)
{
(void) t;
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
#endif

#if 0
static int
lidar2subtype(sql_subtype *tpe, int t, long rep, long wid)
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
#endif

str LIDARexportTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
//	int* res_id = *getArgReference_str(stk, pci, 1);
	str tname = *getArgReference_str(stk, pci, 1);
	str filename = *getArgReference_str(stk, pci, 2);
	str format = toLower(*getArgReference_str(stk, pci, 2));
	mvc *m = NULL;
	sql_trans *tr;
	sql_schema *sch;
	sql_table *tbl;
	BUN nrows = 0;
	BUN i;
	
	sql_column *cols[3];
	dbl *cols_dbl[3];
	BAT *bats_dbl[3];

	LASHeaderH header;
	LASWriterH writer;
	LASPointH point;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	tr = m->session->tr;
	sch = mvc_bind_schema(m, "sys");

	/* First step: look if the table exists in the database. If the table is not in the database, the export function cannot continue */
 
	tbl = mvc_bind_table(m, sch, tname);
	if (tbl == NULL) {
		msg = createException (MAL, "lidar.exporttable", SQLSTATE(LI000) "Table %s is missing.\n", tname);
		return msg;
	}

	/* Bind the necessary columns */
	cols[0] = mvc_bind_column(m, tbl, "x");
	cols[1] = mvc_bind_column(m, tbl, "y");
	cols[2] = mvc_bind_column(m, tbl, "z");
	if (cols[0] == NULL || cols[1] == NULL || cols[2] == NULL) {
		msg = createException(MAL, "lidar.exporttable", SQLSTATE(LI000) "Could not locate a column with name 'x', 'y', or 'z'.");
		return msg;
	} 
	//bats_dbl[0] = mvc_bind(m, *sname, *tname, *cname, *access);
	bats_dbl[0] = store_funcs.bind_col(tr, cols[0], 0);
	bats_dbl[1] = store_funcs.bind_col(tr, cols[1], 0);
	bats_dbl[2] = store_funcs.bind_col(tr, cols[2], 0);

	cols_dbl[0] = (dbl*)Tloc(bats_dbl[0], 0);
	cols_dbl[1] = (dbl*)Tloc(bats_dbl[1], 0);
	cols_dbl[2] = (dbl*)Tloc(bats_dbl[2], 0);

	nrows = store_funcs.count_col(tr, cols[0], 1);

	/* Populate the header */
	header = LASHeader_Create();
	LASHeader_SetCompressed(header, (strcmp(format, "laz") == 0));
/*
	LASHeader_SetCreationDOY
	LASHeader_SetCreationYear
	LASHeader_SetDataFormatId
	LASHeader_SetDataOffset
	LASHeader_SetDataRecordLength
	LASHeader_SetFileSourceId
	LASHeader_SetGUID
	LASHeader_SetHeaderPadding
	LASHeader_SetMax
	LASHeader_SetMin
	LASHeader_SetOffset
	LASHeader_SetPointRecordsByReturnCount
*/
	LASHeader_SetPointRecordsCount(header, nrows);
/*
	LASHeader_SetProjectId
	LASHeader_SetReserved
	LASHeader_SetScale
	LASHeader_SetSchema
*/
	LASHeader_SetSoftwareId(header, "MonetDB B.V.");
/*	LASHeader_SetSRS */
	LASHeader_SetSystemId(header, "MonetDB B.V.");
	LASHeader_SetVersionMajor(header, '1');
	LASHeader_SetVersionMinor(header, '2');

	/* Create the writer*/
	MT_lock_set(&mt_lidar_lock);
	writer = LASWriter_Create(filename, header, LAS_MODE_WRITE);
	// TODO: Failure is always an option
	MT_lock_unset(&mt_lidar_lock);


	/* Iterate over the table */
	for (i = 0; i < nrows; i++) {
		point = LASPoint_Create();
/*
		LASPoint_SetClassification
		LASPoint_SetColor
		LASPoint_SetData
		LASPoint_SetFlightLineEdge
		LASPoint_SetHeader
		LASPoint_SetIntensity
		LASPoint_SetNumberOfReturns
		LASPoint_SetPointSourceId
		LASPoint_SetRawX
		LASPoint_SetRawY
		LASPoint_SetRawZ
		LASPoint_SetReturnNumber
		LASPoint_SetScanAngleRank
		LASPoint_SetScanDirection
		LASPoint_SetScanFlags
		LASPoint_SetTime
		LASPoint_SetUserData
*/
		LASPoint_SetX(point, cols_dbl[0][i]);
		LASPoint_SetY(point, cols_dbl[1][i]);
		LASPoint_SetZ(point, cols_dbl[2][i]);

		LASWriter_WritePoint(writer, point);
		LASPoint_Destroy(point);
	}

	MT_lock_set(&mt_lidar_lock);
	LASHeader_Destroy(header);
	LASWriter_Destroy(writer);
	MT_lock_unset(&mt_lidar_lock);

	return MAL_SUCCEED;
}


str LIDARdir(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
(void) cntxt;
(void) mb;
(void) stk;
(void) pci;
#if 0
	str msg = MAL_SUCCEED;
	str dir = *getArgReference_str(stk, pci, 1);
	DIR *dp;
	struct dirent *ep;
	lidarfile *fptr;
	char *s;
	int status = 0;
	(void)mb;

	dp = opendir(dir);
	if (dp != NULL) {
		char stmt[BUFSIZ];
		char fname[BUFSIZ];

		s = stmt;

		while ((ep = readdir(dp)) != NULL && !msg) {
			snprintf(fname, BUFSIZ, "%s%s", dir, ep->d_name);
			status = 0;
			lidar_open_file(&fptr, fname, READONLY, &status);
			if (status == 0) {
				snprintf(stmt, BUFSIZ, ATTACHDIR, fname);
				msg = SQLstatementIntern(cntxt, &s, "lidar.listofdir", TRUE, FALSE, NULL);
				lidar_close_file(fptr, &status);
			}
		}
		(void)closedir(dp);
	} else
		msg = createException(MAL, "listdir", SQLSTATE(LI000) "Couldn't open the directory");

	return msg;
#endif
return MAL_SUCCEED;
}

str LIDARdirpat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
(void) cntxt;
(void) mb;
(void) stk;
(void) pci;
return MAL_SUCCEED;
#if 0
	str msg = MAL_SUCCEED;
	str dir = *getArgReference_str(stk, pci, 1);
	str pat = *getArgReference_str(stk, pci, 2);
	lidarfile *fptr;
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
		throw(MAL, "listdir", SQLSTATE(LI000) "Couldn't open the directory or there are no files that match the pattern");

	for (j = 0; j < globbuf.gl_pathc; j++) {
		char stmt[BUFSIZ];
		char fname[BUFSIZ];

		s = stmt;
		snprintf(fname, BUFSIZ, "%s", globbuf.gl_pathv[j]);
		status = 0;
		lidar_open_file(&fptr, fname, READONLY, &status);
		if (status == 0) {
			snprintf(stmt, BUFSIZ, ATTACHDIR, fname);
			msg = SQLstatementIntern(cntxt, &s, "lidar.listofdirpat", TRUE, FALSE, NULL);
			lidar_close_file(fptr, &status);
			break;
		}
	}

	return msg;
#endif
return MAL_SUCCEED;
}


str
LIDARtest(int *res, str *fname)
{
	str msg = MAL_SUCCEED;
	LASReaderH reader;
	LASHeaderH header;

	LASError_Reset();
	MT_lock_set(&mt_lidar_lock);
	reader=LASReader_Create(*fname);
	MT_lock_unset(&mt_lidar_lock);

	if (LASError_GetErrorCount() != 0) {
		msg = createException(MAL, "lidar.test", SQLSTATE(LI000) "Error accessing LIDAR file %s (%s)", 
		*fname, LASError_GetLastErrorMsg());
	} else 	{
		header=LASReader_GetHeader(reader);
		*res=LASHeader_GetPointRecordsCount(header);
		MT_lock_set(&mt_lidar_lock);
                if (header != NULL) LASHeader_Destroy(header);
                if (reader != NULL) LASReader_Destroy(reader);
		MT_lock_unset(&mt_lidar_lock);
		if (LASError_GetErrorCount() != 0) {
			msg = createException(MAL, "lidar.test", SQLSTATE(LI000) "Error accessing LIDAR file %s (%s)", 
			*fname, LASError_GetLastErrorMsg());
		}
	}

	return msg;
}

str LIDARattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_trans *tr;
	sql_schema *sch;
	sql_table *lidar_fl, *lidar_tbl, *lidar_col, *tbl = NULL;
	sql_column *col;
	str msg = MAL_SUCCEED;
        LASReaderH reader = NULL;
        LASHeaderH header = NULL;
	str fname = *getArgReference_str(stk, pci, 1);
	oid fid, tid, cid, rid = oid_nil;
	char *tname_low = NULL, *s, bname[BUFSIZ];
	//long rows;
	int cnum;
	/* table */
	int RecordsCount, PointRecordsCount, DataOffset;
	int HeaderPadding, ByteSize, BaseByteSize, CreationDOY;
	int CreationYear, Reserved, DataRecordLength, HeaderSize, FileSourceId;
	char VersionMajor, VersionMinor, DataFormatId;
	str WKT, WKT_CompoundOK, Proj4;
	/* columns */
	double ScaleX, ScaleY, ScaleZ, OffsetX, OffsetY, OffsetZ;
	double MinX, MinY, MinZ, MaxX, MaxY, MaxZ;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	/* check if file exists */
	if (access(fname, F_OK) == -1) {
		msg = createException(MAL, "lidar.test", SQLSTATE(LI000) "File %s not found.", fname);
		return msg;
	}

	/* open the LAS/LAZ file */
	MT_lock_set(&mt_lidar_lock);
        LASError_Reset();
        reader = LASReader_Create(fname);
	MT_lock_unset(&mt_lidar_lock);
	if (LASError_GetErrorCount() != 0) {
		msg = createException(MAL, "lidar.test", SQLSTATE(LI000) "Error accessing LIDAR file %s (%s)", 
		fname, LASError_GetLastErrorMsg());
		return msg;
	}

	/* get the header */
	header = LASReader_GetHeader(reader);
	if (!header) {
		msg = createException(MAL, "lidar.test", SQLSTATE(LI000) "Error accessing LIDAR file %s (%s)", 
		fname, LASError_GetLastErrorMsg());
		return msg;
	}
#ifndef NDEBUG
	print_lidar_header(stderr, header, fname, 0, 0);
#endif
	/* if needed, instantiate the schema and gather all appropriate tables */
	tr = m->session->tr;
	sch = mvc_bind_schema(m, "sys");

	lidar_fl = mvc_bind_table(m, sch, "lidar_files");
	if (lidar_fl == NULL)
		LIDARinitCatalog(m);

	lidar_fl = mvc_bind_table(m, sch, "lidar_files");
	lidar_tbl = mvc_bind_table(m, sch, "lidar_tables");
	lidar_col = mvc_bind_table(m, sch, "lidar_columns");

	/* check if the file is already attached */
	col = mvc_bind_column(m, lidar_fl, "name");
	rid = table_funcs.column_find_row(m->session->tr, col, fname, NULL);
	if (!is_oid_nil(rid)) {
		MT_lock_set(&mt_lidar_lock);
                if (header != NULL) LASHeader_Destroy(header);
                if (reader != NULL) LASReader_Destroy(reader);
		MT_lock_unset(&mt_lidar_lock);
		msg = createException(SQL, "lidar.attach", SQLSTATE(LI000) "File %s already attached\n", fname);
		return msg;
	}

	/* add row in the lidar_files catalog table */
	col = mvc_bind_column(m, lidar_fl, "id");
	fid = store_funcs.count_col(tr, col, 1) + 1;
	store_funcs.append_col(m->session->tr,
		mvc_bind_column(m, lidar_fl, "id"), &fid, TYPE_int);
	store_funcs.append_col(m->session->tr,
		mvc_bind_column(m, lidar_fl, "name"), fname, TYPE_str);
	/* table.id++ */
	col = mvc_bind_column(m, lidar_tbl, "id");
	tid = store_funcs.count_col(tr, col, 1) + 1;

	/* extract the file name from the absolute path */
	if ((s = strrchr(fname, DIR_SEP)) == NULL)
		s = fname;
	else
		s++;
	strcpy(bname, s);
	if (s) *s = 0;

	tname_low = toLower(bname);

	/* check table name for existence in the lidar catalog */
	col = mvc_bind_column(m, lidar_tbl, "name");
	rid = table_funcs.column_find_row(m->session->tr, col, tname_low, NULL);
	/* or as regular SQL table */
	tbl = mvc_bind_table(m, sch, tname_low);
	if (!is_oid_nil(rid) || tbl) {
		MT_lock_set(&mt_lidar_lock);
                if (header != NULL) LASHeader_Destroy(header);
                if (reader != NULL) LASReader_Destroy(reader);
		MT_lock_unset(&mt_lidar_lock);
		msg = createException(SQL, "lidar.attach", SQLSTATE(LI000) "Table %s already exists\n", tname_low);
		return msg;
	}
	
	/* read values from the header */
	FileSourceId = LASHeader_GetFileSourceId(header);
	VersionMajor = LASHeader_GetVersionMajor(header);
	VersionMinor = LASHeader_GetVersionMinor(header);
	DataFormatId = LASHeader_GetDataFormatId(header);
	CreationDOY = LASHeader_GetCreationDOY(header);
	CreationYear = LASHeader_GetCreationYear(header);
	RecordsCount = LASHeader_GetRecordsCount(header);
	PointRecordsCount = LASHeader_GetPointRecordsCount(header);
	DataOffset = LASHeader_GetDataOffset(header);
	HeaderPadding = LASHeader_GetHeaderPadding(header);
	Reserved = LASHeader_GetReserved(header);
	DataRecordLength = LASHeader_GetDataRecordLength(header);
	HeaderSize = LASHeader_GetHeaderSize(header);
	ByteSize     = LASSchema_GetByteSize(LASHeader_GetSchema(header));
	BaseByteSize = LASSchema_GetBaseByteSize(LASHeader_GetSchema(header));
	WKT = LASSRS_GetWKT(LASHeader_GetSRS(header));
	WKT_CompoundOK = LASSRS_GetWKT_CompoundOK(LASHeader_GetSRS(header));
	Proj4 = LASSRS_GetProj4(LASHeader_GetSRS(header));

	/* store data */
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "id"), &tid, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "file_id"), &fid, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "name"), tname_low, TYPE_str);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "FileSourceId"), &FileSourceId, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "VersionMajor"), &VersionMajor, TYPE_str);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "VersionMinor"), &VersionMinor, TYPE_str);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "DataFormatId"), &DataFormatId, TYPE_str);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "CreationDOY"), &CreationDOY, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "CreationYear"), &CreationYear, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "RecordsCount"), &RecordsCount, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "PointRecordsCount"), &PointRecordsCount, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "DataOffset"), &DataOffset, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "HeaderPadding"), &HeaderPadding, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "Reserved"), &Reserved, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "DataRecordLength"), &DataRecordLength, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "HeaderSize"), &HeaderSize, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "ByteSize"), &ByteSize, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "BaseByteSize"), &BaseByteSize, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "WKT"), WKT, TYPE_str);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "WKT_CompoundOK"), WKT_CompoundOK, TYPE_str);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_tbl, "Proj4"), Proj4, TYPE_str);

	/* add a lidar_column tuple */
	col = mvc_bind_column(m, lidar_col, "id");
	cid = store_funcs.count_col(tr, col, 1) + 1;
	/* read data from the header */
	ScaleX = LASHeader_GetScaleX(header);
	ScaleY = LASHeader_GetScaleY(header);
	ScaleZ = LASHeader_GetScaleZ(header);
	OffsetX = LASHeader_GetOffsetX(header);
	OffsetY = LASHeader_GetOffsetY(header);
	OffsetZ = LASHeader_GetOffsetZ(header);
	MinX = LASHeader_GetMinX(header);
	MinY = LASHeader_GetMinY(header);
	MinZ = LASHeader_GetMinZ(header);
	MaxX = LASHeader_GetMaxX(header);
	MaxY = LASHeader_GetMaxY(header);
	MaxZ = LASHeader_GetMaxZ(header);

	/* store */
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "id"), &cid, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "file_id"), &fid, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "table_id"), &tid, TYPE_int);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "ScaleX"), &ScaleX, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "ScaleY"), &ScaleY, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "ScaleZ"), &ScaleZ, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "OffsetX"), &OffsetX, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "OffsetY"), &OffsetY, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "OffsetZ"), &OffsetZ, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "MinX"), &MinX, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "MinY"), &MinY, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "MinZ"), &MinZ, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "MaxX"), &MaxX, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "MaxY"), &MaxY, TYPE_dbl);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, lidar_col, "MaxZ"), &MaxZ, TYPE_dbl);

	/* create an SQL table to hold the LIDAR table */
	cnum = 3;//x, y, z. TODO: Add all available columnt
	tbl = mvc_create_table(m, sch, tname_low, tt_table, 0, SQL_PERSIST, 0, cnum, 0);
	mvc_create_column_(m, tbl, "x", "double", 64);
	mvc_create_column_(m, tbl, "y", "double", 64);
	mvc_create_column_(m, tbl, "z", "double", 64);

	MT_lock_set(&mt_lidar_lock);
	if (header != NULL) LASHeader_Destroy(header);
	if (reader != NULL) LASReader_Destroy(reader);
	MT_lock_unset(&mt_lidar_lock);

	return MAL_SUCCEED;
}

str LIDARloadTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch;
	sql_table *lidar_fl, *lidar_tbl, *lidar_cl, *tbl = NULL;
	sql_column *col, *colx, *coly, *colz;
	str tname = *getArgReference_str(stk, pci, 1);
	str fname;
	str msg = MAL_SUCCEED;
	oid rid = oid_nil, frid = oid_nil, tid = oid_nil;
	int fid, i;
#ifndef NDEBUG
	int time0;
#endif
	int *tpcode = NULL;
	long *rep = NULL, *wid = NULL, rows;
	LASReaderH reader = NULL;
	LASHeaderH header = NULL;
	LASPointH p = NULL;
	dbl *px = NULL, *py = NULL, *pz = NULL;
	BAT *x = NULL, *y = NULL, *z = NULL;
	size_t sz;
	double scalex, scaley, scalez, offsetx, offsety, offsetz;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;
	sch = mvc_bind_schema(m, "sys");

	lidar_tbl = mvc_bind_table(m, sch, "lidar_tables");
	if (lidar_tbl == NULL) {
		msg = createException(MAL, "lidar.loadtable", SQLSTATE(LI000) "LIDAR catalog is missing.\n");
		return msg;
	}

	tbl = mvc_bind_table(m, sch, tname);
	if (tbl == NULL) {
		msg = createException(MAL, "lidar.loadtable", SQLSTATE(LI000) "Could not find table %s.\n", tname);
		return msg;
	}

	col = mvc_bind_column(m, tbl, "x");
	sz = store_funcs.count_col(m->session->tr, col, 1);
	if (sz != 0) {
		msg = createException(MAL, "lidar.loadtable", SQLSTATE(LI000) "Table %s is not empty.\n", tname);
		return msg;
	}

	col = mvc_bind_column(m, lidar_tbl, "name");
	rid = table_funcs.column_find_row(m->session->tr, col, tname, NULL);
	if (is_oid_nil(rid)) {
		msg = createException(MAL, "lidar.loadtable", SQLSTATE(LI000) "Table %s is unknown to the LIDAR catalog. Attach first the containing file\n", tname);
		return msg;
	}

	/* Open LIDAR file */
	col = mvc_bind_column(m, lidar_tbl, "file_id");
	fid = *(int*)table_funcs.column_find_value(m->session->tr, col, rid);

	lidar_fl = mvc_bind_table(m, sch, "lidar_files");
	col = mvc_bind_column(m, lidar_fl, "id");
	frid = table_funcs.column_find_row(m->session->tr, col, (void *)&fid, NULL);
	col = mvc_bind_column(m, lidar_fl, "name");
	fname = (char *)table_funcs.column_find_value(m->session->tr, col, frid);

	lidar_cl = mvc_bind_table(m, sch, "lidar_columns");
	if (lidar_cl == NULL) {
		msg = createException(MAL, "lidar.loadtable", SQLSTATE(LI000) "Could not find table lidar_columns.\n");
		return msg;
	}
	col = mvc_bind_column(m, lidar_cl, "file_id");
	tid = table_funcs.column_find_row(m->session->tr, col, (void *)&fid, NULL);
	col = mvc_bind_column(m, lidar_cl, "OffsetX");
	offsetx = *(double*)table_funcs.column_find_value(m->session->tr, col, tid);
	col = mvc_bind_column(m, lidar_cl, "OffsetY");
	offsety = *(double*)table_funcs.column_find_value(m->session->tr, col, tid);
	col = mvc_bind_column(m, lidar_cl, "OffsetZ");
	offsetz = *(double*)table_funcs.column_find_value(m->session->tr, col, tid);
	col = mvc_bind_column(m, lidar_cl, "ScaleX");
	scalex = *(double*)table_funcs.column_find_value(m->session->tr, col, tid);
	col = mvc_bind_column(m, lidar_cl, "ScaleX");
	scaley = *(double*)table_funcs.column_find_value(m->session->tr, col, tid);
	col = mvc_bind_column(m, lidar_cl, "ScaleX");
	scalez = *(double*)table_funcs.column_find_value(m->session->tr, col, tid);


	/* open the LAS/LAZ file */
	MT_lock_set(&mt_lidar_lock);
	LASError_Reset();
	reader = LASReader_Create(fname);
	MT_lock_unset(&mt_lidar_lock);
	if (LASError_GetErrorCount() != 0) {
		msg = createException(MAL, "lidar.lidarload", SQLSTATE(LI000) "Error accessing LIDAR file %s (%s)", 
		fname, LASError_GetLastErrorMsg());
		return msg;
	}

	/* get the header */
	header = LASReader_GetHeader(reader);
	if (!header) {
		msg = createException(MAL, "lidar.lidarload", SQLSTATE(LI000) "Error accessing LIDAR file %s (%s)", 
		fname, LASError_GetLastErrorMsg());
		return msg;
	}

	/* data load */
	col = mvc_bind_column(m, lidar_tbl, "PointRecordsCount");
	rows = *(int*)table_funcs.column_find_value(m->session->tr, col, rid);
#ifndef NDEBUG
	fprintf(stderr,"#Loading %ld rows in table %s\n", rows, tname);
	time0 = GDKms();
#endif
	colx = mvc_bind_column(m, tbl, "x");
	coly = mvc_bind_column(m, tbl, "y");
	colz = mvc_bind_column(m, tbl, "z");
	x = COLnew(0, TYPE_dbl, rows, PERSISTENT);
	y = COLnew(0, TYPE_dbl, rows, PERSISTENT);
	z = COLnew(0, TYPE_dbl, rows, PERSISTENT);

	if ( x == NULL || y == NULL || z == NULL) {
		GDKfree(tpcode);
		GDKfree(rep);
		GDKfree(wid);
		MT_lock_set(&mt_lidar_lock);
		if (p != NULL) LASPoint_Destroy(p);
		if (header != NULL) LASHeader_Destroy(header);
		if (reader != NULL) LASReader_Destroy(reader);
		MT_lock_unset(&mt_lidar_lock);
		msg = createException(MAL, "lidar.lidarload", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return msg;
	}

	px = (dbl *) Tloc(x, 0);
	py = (dbl *) Tloc(y, 0);
	pz = (dbl *) Tloc(z, 0);

	p = LASReader_GetNextPoint(reader);
	i = 0;
	while (p) {
#ifndef NDEBUG
		/* print the details of a few points when in debug mode */
		if ( i % 1000000 == 0 ) {
			double x = LASPoint_GetX(p);
			double y = LASPoint_GetY(p);
			double z = LASPoint_GetZ(p);
			long rawx = LASPoint_GetRawX(p);
			long rawy = LASPoint_GetRawY(p);
			long rawz = LASPoint_GetRawZ(p);
			unsigned short intensity = LASPoint_GetIntensity (p);
			unsigned short returnno =LASPoint_GetReturnNumber (p);
			unsigned short noofreturns = LASPoint_GetNumberOfReturns (p);
			unsigned short scandir = LASPoint_GetScanDirection (p);
			unsigned short flightline = LASPoint_GetFlightLineEdge (p);
			unsigned char flags = LASPoint_GetScanFlags (p);
			unsigned char class = LASPoint_GetClassification (p);
			double t = LASPoint_GetTime(p);
			char anglerank = LASPoint_GetScanAngleRank (p);
			unsigned short sourceid = LASPoint_GetPointSourceId (p);
			fprintf(stderr,
				"(point # %d)"
				"X (raw)           : %f (%ld)\n"
				"Z (raw)           : %f (%ld)\n"
				"Z (raw)           : %f (%ld)\n"
				"intensity         : %hu\n"
				"return number     : %hu\n"
				"number of returns : %hu\n"
				"scan direction    : %hu\n"
				"flight line edge  : %hu\n"
				"scan flags        : %hhu\n"
				"classification    : %hhu\n"
				"time              : %f\n"
				"scan angle rank   : %hhd\n"
				"point source id   : %hu\n",
			i, x, rawx, y, rawy, z, rawz,
			intensity, returnno, noofreturns, 
			scandir, flightline, flags, class, 
			t, (signed char) anglerank, sourceid);
		}
#endif
		//TODO: Add a flag that indicates whether LiDAR points should be validited up front
		px[i] = LASPoint_GetRawX(p) * scalex + offsetx;
		py[i] = LASPoint_GetRawY(p) * scaley + offsety;
		pz[i] = LASPoint_GetRawZ(p) * scalez + offsetz;

        	p = LASReader_GetNextPoint(reader);
		i++;
	}

	BATsetcount(x, rows);
	BATsetcount(y, rows);
	BATsetcount(z, rows);

	x->tsorted = 0;
	x->trevsorted = 0;
	y->tsorted = 0;
	y->trevsorted = 0;
	z->tsorted = 0;
	z->trevsorted = 0;
#ifndef NDEBUG
	fprintf(stderr,"#File loaded in %d ms\t", GDKms() - time0);
#endif
	BATmode(x, PERSISTENT);
	BATmode(y, PERSISTENT);
	BATmode(z, PERSISTENT);
	store_funcs.append_col(m->session->tr, colx, x, TYPE_bat);
	store_funcs.append_col(m->session->tr, coly, y, TYPE_bat);
	store_funcs.append_col(m->session->tr, colz, z, TYPE_bat);
#ifndef NDEBUG
	fprintf(stderr,"#Total time %d ms\n", GDKms() - time0);
#endif
	BBPrelease(x->batCacheid);
	BBPrelease(y->batCacheid);
	BBPrelease(z->batCacheid);
	BBPunfix(x->batCacheid);
	BBPunfix(y->batCacheid);
	BBPunfix(z->batCacheid);
	GDKfree(tpcode);
	GDKfree(rep);
	GDKfree(wid);

	MT_lock_set(&mt_lidar_lock);
	if (p != NULL) LASPoint_Destroy(p);
	if (header != NULL) LASHeader_Destroy(header);
	if (reader != NULL) LASReader_Destroy(reader);
	MT_lock_unset(&mt_lidar_lock);

	return msg;
}

str
LIDARprelude(void *ret) {
#ifdef NEED_MT_LOCK_INIT
	static int initialized = 0;
	/* since we don't destroy the lock, only initialize it once */
	if (!initialized)
		MT_lock_init(&mt_lidar_lock, "lidar.lock");
	initialized = 1;
#endif
	(void) ret;

	return MAL_SUCCEED;
}
