/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Authors: K. Kyzirakos, D. Savva
 * This module contains primitives for accessing geospatial data
 * stored in ESRI Shapefile documents.
 */

#include "monetdb_config.h"
#include <string.h>
#include "sql_mvc.h"
#include "sql.h"
#ifndef __clang_major__ /* stupid include file gdal/cpl_port.h */
#define __clang_major__ 0
#endif
#include "shp.h"
#include <cpl_conv.h> /* for CPLFree */
#include "sql_execute.h"
#include "mal_exception.h"

#include "libgeom.h"

GDALWConnection *GDALWConnect(char *source)
{
	GDALWConnection *conn = NULL;
	OGRFeatureDefnH featureDefn;
	int fieldCount, i;
	OGRRegisterAll();
	conn = GDKmalloc(sizeof(GDALWConnection));
	if (conn == NULL)
	{
		TRC_ERROR(SHP, "Could not allocate memory\n");
		return NULL;
	}
	conn->handler = OGROpen(source, 0, &(conn->driver));
	if (conn->handler == NULL)
	{
		free(conn);
		return NULL;
	}

	conn->layer = OGR_DS_GetLayer(conn->handler, 0);
	if (conn->layer == NULL)
	{
		OGRReleaseDataSource(conn->handler);
		free(conn);
		return NULL;
	}

	conn->layername = (const char *)OGR_L_GetName(conn->layer);

	featureDefn = OGR_L_GetLayerDefn(conn->layer);
	fieldCount = OGR_FD_GetFieldCount(featureDefn);
	conn->numFieldDefinitions = fieldCount;
	conn->fieldDefinitions = GDKmalloc(fieldCount * sizeof(OGRFieldDefnH));
	if (conn->fieldDefinitions == NULL)
	{
		OGRReleaseDataSource(conn->handler);
		free(conn);
		TRC_ERROR(SHP, "Could not allocate memory\n");
		return NULL;
	}
	for (i = 0; i < fieldCount; i++)
	{
		conn->fieldDefinitions[i] = OGR_FD_GetFieldDefn(featureDefn, i);
	}

	return conn;
}

void GDALWClose(GDALWConnection *conn)
{
	GDKfree(conn->fieldDefinitions);
	OGRReleaseDataSource(conn->handler);
	GDKfree(conn);
}

GDALWSimpleFieldDef *GDALWGetSimpleFieldDefinitions(GDALWConnection conn)
{
	int i;
	GDALWSimpleFieldDef *columns;
	OGRFieldDefnH fieldDefn;
	/*if (conn.layer == NULL || conn.handler == NULL || conn.driver == NULL) {
		printf("Could not extract columns, initialize a connection first.\n");
		exit(-1);
	}*/
	columns = GDKmalloc(conn.numFieldDefinitions * sizeof(GDALWSimpleFieldDef));
	if (columns == NULL)
	{
		TRC_ERROR(SHP, "Could not allocate memory\n");
		return NULL;
	}
	for (i = 0; i < conn.numFieldDefinitions; i++)
	{
		fieldDefn = conn.fieldDefinitions[i];
		columns[i].fieldName = OGR_Fld_GetNameRef(fieldDefn);
		columns[i].fieldType = OGR_GetFieldTypeName(OGR_Fld_GetType(fieldDefn));
	}

	return columns;
}

void GDALWPrintRecords(GDALWConnection conn)
{
	char *wkt;
	int i;
	OGRFeatureH feature;
	OGRGeometryH geometry;
	OGRFeatureDefnH featureDefn;
	featureDefn = OGR_L_GetLayerDefn(conn.layer);
	OGR_L_ResetReading(conn.layer);
	while ((feature = OGR_L_GetNextFeature(conn.layer)) != NULL)
	{
		for (i = 0; i < OGR_FD_GetFieldCount(featureDefn); i++)
		{
			OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn(featureDefn, i);
			if (OGR_Fld_GetType(hFieldDefn) == OFTInteger)
				printf("%d,", OGR_F_GetFieldAsInteger(feature, i));
			else if (OGR_Fld_GetType(hFieldDefn) == OFTReal)
				printf("%.3f,", OGR_F_GetFieldAsDouble(feature, i));
			else
				printf("%s,", OGR_F_GetFieldAsString(feature, i));
		}
		geometry = OGR_F_GetGeometryRef(feature);
		OGR_G_ExportToWkt(geometry, &wkt);
		printf("%s", wkt);
		printf("\n");
		CPLFree(wkt);
		OGR_F_Destroy(feature);
	}
}

GDALWSpatialInfo GDALWGetSpatialInfo(GDALWConnection conn)
{
	GDALWSpatialInfo spatialInfo;
	OGRSpatialReferenceH spatialRef = OGR_L_GetSpatialRef(conn.layer);
	char *proj4, *srsText, *srid;

	OSRExportToProj4(spatialRef, &proj4);
	OSRExportToWkt(spatialRef, &srsText);
	srid = (char *)OSRGetAttrValue(spatialRef, "AUTHORITY", 1);
	if (srid == NULL)
	{
		spatialInfo.epsg = 4326;
	}
	else
	{
		spatialInfo.epsg = atoi(OSRGetAttrValue(spatialRef, "AUTHORITY", 1));
	}
	spatialInfo.authName = OSRGetAttrValue(spatialRef, "AUTHORITY", 0);
	if (spatialInfo.authName == NULL)
	{
		spatialInfo.authName = "EPSG";
	}
	spatialInfo.proj4Text = proj4;
	spatialInfo.srsText = srsText;

	return spatialInfo;
}

//Using SQL query
str createSHPtable(Client cntxt, str schemaname, str tablename, GDALWConnection shp_conn, GDALWSimpleFieldDef *field_definitions)
{
	unsigned int size = BUFSIZ;
	char *buf = GDKmalloc(BUFSIZ * sizeof(char)), *temp_buf = GDKmalloc(BUFSIZ * sizeof(char));
	char *nameToLowerCase = NULL;
	str msg = MAL_SUCCEED;

	if (field_definitions == NULL)
	{
		/* Can't find shapefile field definitions */
		return createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* Create the table that will store the data of the shape file (Allows integers, floats and strings) */
	temp_buf[0] = '\0';
	for (int i = 0; i < shp_conn.numFieldDefinitions; i++)
	{
		/*If the next column definition doesn't fit in the buffer, resize the buffers to double
		Compare current buffer size with current lenght + field name lenght + 11 (lenght of string column definition)*/
		if (size <= (11 + strlen(field_definitions[i].fieldName) + strlen(temp_buf)))
		{
			size = 2 * size;
			buf = GDKrealloc(buf, size);
			temp_buf = GDKrealloc(temp_buf, size);
		}
		nameToLowerCase = toLower(field_definitions[i].fieldName);
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0)
		{
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" INT, ", nameToLowerCase);
		}
		else if (strcmp(field_definitions[i].fieldType, "Real") == 0)
		{
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" FLOAT, ", nameToLowerCase);
		}
		else
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" STRING, ", nameToLowerCase);
		GDKfree(nameToLowerCase);
	}

	//Each shapefile table has one geom column
	sprintf(temp_buf + strlen(temp_buf), "geom GEOMETRY ");

	//Concat the schema name with the table name
	size_t schemaTableSize = strlen(schemaname) + strlen(tablename) + 3;
	char *schemaTable = GDKmalloc(schemaTableSize);
	snprintf(schemaTable, schemaTableSize - 1, "%s.%s", schemaname, tablename);

	//Build the CREATE TABLE command
	snprintf(buf, size, CRTTBL, schemaTable, temp_buf);
	//And execute it
	msg = SQLstatementIntern(cntxt, buf, "shp.load", TRUE, FALSE, NULL);

	GDKfree(buf);
	GDKfree(temp_buf);
	GDKfree(schemaTable);
	return msg;
}

str loadSHPtable(mvc *m, sql_schema *sch, str schemaname, str tablename, GDALWConnection shp_conn, GDALWSimpleFieldDef *field_definitions, GDALWSpatialInfo spatial_info)
{
	sql_table *data_table = NULL;
	sql_column **cols;
	BAT **colsBAT;
	int colsNum = 2; //we will have at least the gid column and a geometry column
	int rowsNum = 0; //the number of rows in the shape file that will be imported
	int gidNum = 0;
	str msg = MAL_SUCCEED;
	char *nameToLowerCase = NULL;
	int i;

	BUN offset;
	BAT *pos = NULL;
	sqlstore *store;

	/* SHP-level descriptor */
	OGRFieldDefnH hFieldDefn;
	OGRFeatureH feature;
	OGRFeatureDefnH featureDefn;

	/* Count the number of lines in the shape file */
	if ((rowsNum = OGR_L_GetFeatureCount(shp_conn.layer, false)) == -1)
	{
		OGR_L_ResetReading(shp_conn.layer);
		rowsNum = 0;
		while ((feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL)
		{
			rowsNum++;
			OGR_F_Destroy(feature);
		}
	}

	/* bind the columns of the data file that was just created
	* and create a BAT for each of the columns */
	if (!(data_table = mvc_bind_table(m, sch, tablename)))
	{
		/* Previously create output table is missing */
		msg = createException(MAL, "shp.load", SQLSTATE(42SO2) "Table '%s.%s' missing", schemaname, tablename);
		return msg;
	}
	colsNum += shp_conn.numFieldDefinitions;
	if (!(cols = (sql_column **)GDKmalloc(sizeof(sql_column *) * colsNum)))
	{
		msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return msg;
	}
	if (!(colsBAT = (BAT **)GDKzalloc(sizeof(BAT *) * colsNum)))
	{
		msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		GDKfree(cols);
		return msg;
	}

	/* Bind shapefile attributes to columns */
	for (i = 0; i < colsNum - 2; i++)
	{
		cols[i] = NULL;
		/* bind the column */
		nameToLowerCase = toLower(field_definitions[i].fieldName);
		cols[i] = mvc_bind_column(m, data_table, nameToLowerCase);
		GDKfree(nameToLowerCase);
		if (cols[i] == NULL)
		{
			msg = createException(MAL, "shp.load", SQLSTATE(42SO2) "Column '%s.%s(%s)' missing", schemaname, tablename, toLower(field_definitions[i].fieldName));
			goto unfree;
		}
		/*create the BAT */
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0)
		{
			if (!(colsBAT[i] = COLnew(0, TYPE_int, rowsNum, PERSISTENT)))
			{
				msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree;
			}
		}
		else if (strcmp(field_definitions[i].fieldType, "Real") == 0)
		{
			if (!(colsBAT[i] = COLnew(0, TYPE_dbl, rowsNum, PERSISTENT)))
			{
				msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree;
			}
		}
		else
		{
			if (!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT)))
			{
				msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree;
			}
		}
	}
	/* Bind GID and GEOM columns */
	if (!(cols[colsNum - 2] = mvc_bind_column(m, data_table, "gid")))
	{
		msg = createException(MAL, "shp.load", SQLSTATE(42SO2) "Column '%s.%s(gid)' missing", schemaname, tablename);
		goto unfree;
	}
	if (!(colsBAT[colsNum - 2] = COLnew(0, TYPE_int, rowsNum, PERSISTENT)))
	{
		msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree;
	}
	if (!(cols[colsNum - 1] = mvc_bind_column(m, data_table, "geom")))
	{
		msg = createException(MAL, "shp.load", SQLSTATE(42SO2) "Column '%s.%s(geom)' missing", schemaname, tablename);
		goto unfree;
	}
	if (!(colsBAT[colsNum - 1] = COLnew(0, ATOMindex("wkb"), rowsNum, PERSISTENT)))
	{
		msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree;
	}

	/* Import the data */
	featureDefn = OGR_L_GetLayerDefn(shp_conn.layer);
	OGR_L_ResetReading(shp_conn.layer);
	/* Import shapefile attributes */
	while ((feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL)
	{
		wkb *geomWKB;
		int len;
		int gidTemp = ++gidNum;
		gdk_return rc;

		OGRGeometryH geometry = OGR_F_GetGeometryRef(feature);

		for (i = 0; i < colsNum - 2; i++)
		{
			hFieldDefn = OGR_FD_GetFieldDefn(featureDefn, i);
			if (OGR_Fld_GetType(hFieldDefn) == OFTInteger)
			{
				int val = OGR_F_GetFieldAsInteger(feature, i);
				rc = BUNappend(colsBAT[i], &val, false);
			}
			else if (OGR_Fld_GetType(hFieldDefn) == OFTReal)
			{
				double val = OGR_F_GetFieldAsDouble(feature, i);
				rc = BUNappend(colsBAT[i], &val, false);
			}
			else
			{
				rc = BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), false);
			}
			if (rc != GDK_SUCCEED)
			{
				/* Append to column failed */
				msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree;
			}
		}

		/* Import GID and GEOM columns */
		if (BUNappend(colsBAT[colsNum - 2], &gidTemp, false) != GDK_SUCCEED)
		{
			msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto unfree;
		}

		len = OGR_G_WkbSize(geometry);
		if (!(geomWKB = GDKmalloc(sizeof(wkb) + len)))
		{
			msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			OGR_F_Destroy(feature);
			goto unfree;
		}
		geomWKB->len = len;
		/* Set SRID */
		geomWKB->srid = spatial_info.epsg;
		OGR_G_ExportToWkb(geometry, wkbNDR, (unsigned char *)geomWKB->data);
		rc = BUNappend(colsBAT[colsNum - 1], geomWKB, false);

		GDKfree(geomWKB);
		OGR_F_Destroy(feature);
		if (rc != GDK_SUCCEED)
			goto unfree;
	}
	/* finalise the BATs */
	store = m->session->tr->store;
	if (store->storage_api.claim_tab(m->session->tr, data_table, BATcount(colsBAT[0]), &offset, &pos) != LOG_OK)
	{
		msg = createException(MAL, "shp.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree;
	}

	for (i = 0; i < colsNum; i++)
	{
		if (store->storage_api.append_col(m->session->tr, cols[i], offset, pos, colsBAT[i], BATcount(colsBAT[i]), TYPE_bat) != LOG_OK)
		{
			bat_destroy(pos);
			msg = createException(MAL, "shp.load", SQLSTATE(38000) "Geos append column failed");
			goto unfree;
		}
	}
	GDKfree(colsBAT);
	GDKfree(cols);
	bat_destroy(pos);
	return msg;
unfree:
	for (i = 0; i < colsNum; i++)
		if (colsBAT[i])
			BBPunfix(colsBAT[i]->batCacheid);
	free(field_definitions);
	GDKfree(cols);
	GDKfree(colsBAT);
	return msg;
}

/* TODO: Use Shapefile table to avoid loading the same file more than once, or allow the user to load as many times as he wants? */
/* Attach and load single shp file given its file name and output table name (uses sys schema) */
str SHPload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	mvc *m = NULL;
	sql_schema *sch = NULL;

	/* Shapefile name (argument 1) */
	str filename = *(str *)getArgReference(stk, pci, 1);
	/* Output schema name (argument 2) */
	str schemaname = *(str *)getArgReference(stk, pci, 2);
	/* Output table name (argument 3) */
	str tablename = *(str *)getArgReference(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	/* SHP-level descriptor */
	GDALWConnection shp_conn;
	GDALWConnection *shp_conn_ptr = NULL;
	GDALWSimpleFieldDef *field_definitions;
	GDALWSpatialInfo spatial_info;

	if (!(sch = mvc_bind_schema(m, schemaname)))
	{
		/* Can't find schema */
		return createException(MAL, "shp.load", SQLSTATE(38000) "Schema %s missing\n", schemaname);
	}

	if ((tablename != NULL) && (tablename[0] == '\0'))
	{
		/* Output table name is NULL */
		return createException(MAL, "shp.load", SQLSTATE(38000) "Missing output table name %s\n", tablename);
	}

	if ((shp_conn_ptr = GDALWConnect((char *)filename)) == NULL)
	{
		/* Can't find shapefile */
		return createException(MAL, "shp.load", SQLSTATE(38000) "Missing shape file %s\n", filename);
	}

	/* Get info about fields and spatial attributes of shapefile*/
	shp_conn = *shp_conn_ptr;
	spatial_info = GDALWGetSpatialInfo(shp_conn);
	field_definitions = GDALWGetSimpleFieldDefinitions(shp_conn);

	/* Convert schema and table name to lower case*/
	schemaname = toLower(schemaname);
	tablename = toLower(tablename);

	/* Create table for outputting shapefile data */
	if ((msg = createSHPtable(cntxt, schemaname, tablename, shp_conn, field_definitions)) != MAL_SUCCEED)
	{
		/* Create table failed */
		GDKfree(schemaname);
		GDKfree(tablename);
		GDKfree(field_definitions);
		GDALWClose(shp_conn_ptr);
		return msg;
	}

	/* Load shapefile data into table */
	msg = loadSHPtable(m, sch, schemaname, tablename, shp_conn, field_definitions, spatial_info);

	/* Frees */
	GDKfree(schemaname);
	GDKfree(tablename);
	GDKfree(field_definitions);
	GDALWClose(shp_conn_ptr);

	return msg;
}

#include "mel.h"
static mel_func shp_init_funcs[] = {
	pattern("shp", "load", SHPload, false, "Import an ESRI Shapefile into a new table", args(1, 4, arg("", void), arg("filename", str), arg("schemaname", str), arg("tablename", str))),
	{.imp = NULL}};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU", read)
#endif
LIB_STARTUP_FUNC(init_shp_mal)
{
	mal_module("shp", NULL, shp_init_funcs);
}

//Old code
#if 0
/* FIXME: the use of the 'rs' schema should be reconsidered so that the geotiff
 * catalog can be integrated into the SQL catalog.
 * When removing the 'rs' schame, the code of client/mapiclient/dump.c MUST be
 * adapted accordingly.
 */

/*#define BUFSIZ 524288*/

/*void getMBB(char * source) {
	SBNSearchHandle handle;
	char * name;
	int i;

	name = malloc((strlen(source) + 1) * sizeof(char));
	for (i=0 ; i<strlen(source) + 1 ; i++) {
		if (i == strlen(source)) {
			name[i] = '\0';
		}
		else if (i == strlen(source) - 1) {
			name[i] = 'n';
		}
		else if (i == strlen(source) - 2) {
			name[i] = 'b';
		}
		else if (i == strlen(source) - 3) {
			name[i] = 's';
		}
		else {
			name[i] = source[i];
		}
	}
	handle = SBNOpenDiskTree(source, NULL);
}*/

/* attach a single shp file given its name, fill in shp catalog tables */
str
SHPattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	sql_table *fls = NULL, *shps = NULL, *shps_dbf = NULL;
	sql_column *col;
	str msg = MAL_SUCCEED;
	str fname = *(str*)getArgReference(stk, pci, 1);
	/* SHP-level descriptor */
	char buf[BUFSIZ], temp_buf[BUFSIZ], *s=buf;
	int  i=0, shpid = 0;
	oid fid, rid = oid_nil;
	GDALWConnection shp_conn;
	GDALWConnection * shp_conn_ptr = NULL;
	GDALWSimpleFieldDef * field_definitions;
	GDALWSpatialInfo spatial_info;

	char *nameToLowerCase = NULL;

	if((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	if(!(sch = mvc_bind_schema(m, "sys")))
		return createException(MAL, "shp.attach", SQLSTATE(38000) "Schema sys missing\n");

	fls = mvc_bind_table(m, sch, "files");
	shps = mvc_bind_table(m, sch, "shapefiles");
	shps_dbf = mvc_bind_table(m, sch, "shapefiles_dbf");
	if (fls == NULL || shps == NULL || shps_dbf == NULL )
		return createException(MAL, "shp.attach", SQLSTATE(38000) "Catalog table missing\n");

	if ((shp_conn_ptr = GDALWConnect((char *) fname)) == NULL) {
		return createException(MAL, "shp.attach", SQLSTATE(38000) "Missing shape file %s\n", fname);
	}
	shp_conn = *shp_conn_ptr;

	/* check if the file is already attached */
	col = mvc_bind_column(m, fls, "path");
	sqlstore *store = m->session->tr->store;
	rid = store->table_api.column_find_row(m->session->tr, col, fname, NULL);
	if (!is_oid_nil(rid)) {
		GDALWClose(shp_conn_ptr);
		return createException(MAL, "shp.attach", SQLSTATE(38000) "File %s already attached\n", fname);
	}

	/* add row in the files(id, path) catalog table */
	col = mvc_bind_column(m, fls, "id");
	fid = store->storage_api.count_col(m->session->tr, col, 1) + 1;
	snprintf(buf, BUFSIZ, INSFILE, (int)fid, fname);
	if ( ( msg = SQLstatementIntern(cntxt, s,"shp.attach",TRUE,FALSE,NULL)) != MAL_SUCCEED)
		goto finish;


	/*if (shp_conn.layer == NULL || shp_conn.source == NULL || shp_conn.handler == NULL || shp_conn.driver == NULL) {
		msg = createException(MAL, "shp.attach", SQLSTATE(38000) "lol-1\n");
									return msg;
	}*/

	/* add row in the shapefiles catalog table (e.g. the name of the table that will store tha data of the shapefile) */
	spatial_info = GDALWGetSpatialInfo(shp_conn);
	col = mvc_bind_column(m, shps, "shapefileid");
	shpid = store->storage_api.count_col(m->session->tr, col, 1) + 1;
	nameToLowerCase = toLower(shp_conn.layername);
	snprintf(buf, BUFSIZ, INSSHP, shpid, (int)fid, spatial_info.epsg, nameToLowerCase);
	GDKfree(nameToLowerCase);
	if ( ( msg = SQLstatementIntern(cntxt, s,"shp.attach",TRUE,FALSE,NULL)) != MAL_SUCCEED)
			goto finish;

	/* add information about the fields of the shape file
 	* one row for each field with info (shapefile_id, field_name, field_type) */
	field_definitions = GDALWGetSimpleFieldDefinitions(shp_conn);
	if (field_definitions == NULL) {
		GDALWClose(&shp_conn);
		return createException(MAL, "shp.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	for (i=0 ; i<shp_conn.numFieldDefinitions ; i++) {
		snprintf(buf, BUFSIZ, INSSHPDBF, shpid, field_definitions[i].fieldName, field_definitions[i].fieldType);
		if ( ( msg = SQLstatementIntern(cntxt, s,"shp.attach",TRUE,FALSE,NULL)) != MAL_SUCCEED)
			goto fin;
	}

	/* create the table that will store the data of the shape file */
	temp_buf[0]='\0';
	for (i=0 ; i<shp_conn.numFieldDefinitions ; i++) {
		nameToLowerCase = toLower(field_definitions[i].fieldName);
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0) {
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" INT, ", nameToLowerCase);
		} else if (strcmp(field_definitions[i].fieldType, "Real") == 0) {
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" FLOAT, ", nameToLowerCase);
#if 0
		} else if (strcmp(field_definitions[i].fieldType, "Date") == 0) {
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" STRING, ", nameToLowerCase);
#endif
        	} else
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" STRING, ", nameToLowerCase);
		GDKfree(nameToLowerCase);
	}

	sprintf(temp_buf + strlen(temp_buf), "geom GEOMETRY ");
	snprintf(buf, BUFSIZ, CRTTBL, shp_conn.layername, temp_buf);

	if ( ( msg = SQLstatementIntern(cntxt, s,"shp.import",TRUE,FALSE,NULL)) != MAL_SUCCEED)
		goto fin;

fin:
	free(field_definitions);
finish:
	/* if (msg != MAL_SUCCEED){
	   snprintf(buf, BUFSIZ,"ROLLBACK;");
	   SQLstatementIntern(cntxt,s,"geotiff.attach",TRUE,FALSE));
	   }*/
	GDALWClose(&shp_conn);
	return msg;
}



static str
SHPimportFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool partial)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	char *sch_name = "sys";

	sql_table *shps_table = NULL, *fls_table = NULL, *data_table = NULL;
	char *shps_table_name = "shapefiles";
	char *fls_table_name = "files";
	char *data_table_name = NULL;

	sql_column *col;

	sql_column **cols;
	BAT **colsBAT, *pos = NULL;
	BUN offset;
	int colsNum = 2; //we will have at least the gid column and a geometry column
	int rowsNum = 0; //the number of rows in the shape file that will be imported
	//GIntBig rowsNum = 0;
	int gidNum = 0;
	char *nameToLowerCase = NULL;

	str msg = MAL_SUCCEED;
	str fname = NULL;
	int vid = *(int*)getArgReference(stk, pci, 1);
	ptr *p;
	wkb *g;
	OGRGeometryH geom;
	OGREnvelope *mbb;
	/* SHP-level descriptor */
	OGRFieldDefnH hFieldDefn;
	int  i=0;
	oid irid;
	GDALWConnection shp_conn;
	GDALWConnection * shp_conn_ptr = NULL;
	GDALWSimpleFieldDef * field_definitions;
	OGRFeatureH feature;
	OGRFeatureDefnH featureDefn;

	/* get table columns from shp and create the table */

	if((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	if(!(sch = mvc_bind_schema(m, sch_name)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Schema '%s' missing", sch_name);

	/* find the name of the shape file corresponding to the given id */
	if(!(fls_table = mvc_bind_table(m, sch, fls_table_name)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Table '%s.%s' missing", sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "id")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(id)' missing", sch_name, fls_table_name);
	sqlstore *store = m->session->tr->store;
	irid = store->table_api.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (is_oid_nil(irid))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Shapefile with id %d not in the %s.%s table\n", vid, sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "path")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(path)' missing", sch_name, fls_table_name);
	fname = (str)store->table_api.column_find_value(m->session->tr, col, irid);

	/* find the name of the table that has been reserved for this shape file */
	if(!(shps_table = mvc_bind_table(m, sch, shps_table_name)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Table '%s.%s' missing", sch_name, shps_table_name);
	if(!(col = mvc_bind_column(m, shps_table, "fileid")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(fileid)' missing", sch_name, shps_table_name);
	irid = store->table_api.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (is_oid_nil(irid))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Shapefile with id %d not in the Shapefile catalog\n", vid);
	if(!(col = mvc_bind_column(m, shps_table, "datatable")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(datatable)' missing", sch_name, shps_table_name);
	data_table_name = (str)store->table_api.column_find_value(m->session->tr, col, irid);


	/* add the data on the file to the table */
	if(!(shp_conn_ptr = GDALWConnect((char *) fname)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Missing shape file %s\n", fname);
	shp_conn = *shp_conn_ptr;

	/*count the number of lines in the shape file */
	if ((rowsNum = OGR_L_GetFeatureCount(shp_conn.layer, false)) == -1) {
		if ((rowsNum = OGR_L_GetFeatureCount(shp_conn.layer, true)) == -1) {
			OGR_L_ResetReading(shp_conn.layer);
			rowsNum = 0;
			while ((feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL ) {
				rowsNum++;
				OGR_F_Destroy(feature);
			}
		}
	}

	/* calculate the mbb of the query geometry */
	if (partial) {
		p = (ptr*)getArgReference(stk, pci, 2);
		g = (wkb*)*p;
		geom = OGR_G_CreateGeometry(wkbPolygon);
		if (OGR_G_ImportFromWkb(geom, (unsigned char*)g->data, g->len) != OGRERR_NONE) {
			msg = createException(MAL, "shp.import", SQLSTATE(38000) "Could not intantiate the query polygon.");
			OGR_F_Destroy(geom);
			goto final;
		}
		if (!(mbb = (OGREnvelope*)GDKmalloc(sizeof(OGREnvelope)))) {
			msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			OGR_F_Destroy(geom);
			goto final;
		}
		OGR_G_GetEnvelope(geom, mbb);

		//FIXME: Take into account the coordinate reference system

		/* apply the spatial filter */
		OGR_L_SetSpatialFilterRect(shp_conn.layer, mbb->MinX, mbb->MinY, mbb->MaxX, mbb->MaxY);

		OGR_F_Destroy(mbb);
		OGR_F_Destroy(geom);
	}

	/* bind the columns of the data file that was just created
	* and create a BAT for each of the columns */
	if(!(data_table = mvc_bind_table(m, sch, data_table_name))) {
		msg = createException(MAL, "shp.import", SQLSTATE(42SO2) "Table '%s.%s' missing", sch_name, data_table_name);
		goto final;
	}
	colsNum += shp_conn.numFieldDefinitions;
	if(!(cols = (sql_column**)GDKmalloc(sizeof(sql_column*)*colsNum))) {
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto final;
	}
	if(!(colsBAT = (BAT**)GDKzalloc(sizeof(BAT*)*colsNum))) {
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree2;
	}
	field_definitions = GDALWGetSimpleFieldDefinitions(shp_conn);
	for(i = 0; i < colsNum - 2; i++) {
		cols[i] = NULL;
		/* bind the column */
		nameToLowerCase = toLower(field_definitions[i].fieldName);
		cols[i] = mvc_bind_column(m, data_table, nameToLowerCase);
		GDKfree(nameToLowerCase);
		if(cols[i] == NULL) {
			msg = createException(MAL, "shp.import", SQLSTATE(42SO2) "Column '%s.%s(%s)' missing", sch_name, data_table_name, field_definitions[i].fieldName);
			goto unfree4;
		}
		/*create the BAT */
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_int, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree4;
			}
		} else if (strcmp(field_definitions[i].fieldType, "Real") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_dbl, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree4;
			}
#if 0
		} else if (strcmp(field_definitions[i].fieldType, "Date") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree4;
			}
#endif
		} else {
			if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree4;
			}
		}
	}
	if(!(cols[colsNum - 2] = mvc_bind_column(m, data_table, "gid"))) {
		msg = createException(MAL, "shp.import", SQLSTATE(42SO2) "Column '%s.%s(gid)' missing", sch_name, data_table_name);
		goto unfree4;
	}
	if(!(colsBAT[colsNum - 2] = COLnew(0, TYPE_int, rowsNum, PERSISTENT))) {
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree4;
	}
	if(!(cols[colsNum - 1] = mvc_bind_column(m, data_table, "geom"))) {
		msg = createException(MAL, "shp.import", SQLSTATE(42SO2) "Column '%s.%s(geom)' missing", sch_name, data_table_name);
		goto unfree4;
	}
	if(!(colsBAT[colsNum - 1] = COLnew(0, ATOMindex("wkb"), rowsNum, PERSISTENT))) {
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree4;
	}

	/* import the data */
	featureDefn = OGR_L_GetLayerDefn(shp_conn.layer);
	OGR_L_ResetReading(shp_conn.layer);
	while((feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL ) {
		wkb *geomWKB;
		int len;
		int gidTemp = ++gidNum;
		gdk_return rc;

		OGRGeometryH geometry = OGR_F_GetGeometryRef(feature);

		for(i = 0; i < colsNum - 2; i++) {
			hFieldDefn = OGR_FD_GetFieldDefn( featureDefn, i );
			if( OGR_Fld_GetType(hFieldDefn) == OFTInteger ) {
				int val = OGR_F_GetFieldAsInteger(feature, i);
				rc = BUNappend(colsBAT[i], &val, false);
			} else if( OGR_Fld_GetType(hFieldDefn) == OFTReal ) {
				double val = OGR_F_GetFieldAsDouble(feature, i);
				rc = BUNappend(colsBAT[i], &val, false);
			} else {
				rc = BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), false);
			}
			if (rc != GDK_SUCCEED) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto unfree4;
			}
		}
		if (BUNappend(colsBAT[colsNum - 2], &gidTemp, false) != GDK_SUCCEED) {
			msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto unfree4;
		}

		len = OGR_G_WkbSize(geometry);
		if (!(geomWKB = GDKmalloc(sizeof(wkb) - 1 + len))) {
			msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			OGR_F_Destroy(feature);
			goto unfree4;
		}
		geomWKB->len = len;
		geomWKB->srid = 0; //FIXME: Add the real srid
		OGR_G_ExportToWkb(geometry, wkbNDR, (unsigned char *)geomWKB->data);
		rc = BUNappend(colsBAT[colsNum - 1], geomWKB, false);
		GDKfree(geomWKB);
		OGR_F_Destroy(feature);
		if (rc != GDK_SUCCEED)
			goto unfree4;
	}

	/* finalise the BATs */
	if (store->storage_api.claim_tab(m->session->tr, data_table, BATcount(colsBAT[0]), &offset, &pos) != LOG_OK) {
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree4;
	}
	if (!isNew(data_table) && sql_trans_add_dependency_change(m->session->tr, data_table->base.id, dml) != LOG_OK) {
		bat_destroy(pos);
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto unfree4;
	}
	for(i = 0; i < colsNum; i++) {
		if (store->storage_api.append_col(m->session->tr, cols[i], offset, pos, colsBAT[i], BATcount(colsBAT[0]), TYPE_bat) != LOG_OK) {
			bat_destroy(pos);
			msg = createException(MAL, "shp.import", SQLSTATE(38000) "Geos append column failed");
			goto unfree4;
		}
	}
	bat_destroy(pos);

	/* free the memory */
unfree4:
	for(i = 0; i < colsNum; i++) {
		if (colsBAT[i])
			BBPunfix(colsBAT[i]->batCacheid);
	}
	free(field_definitions);
	GDKfree(colsBAT);
unfree2:
	GDKfree(cols);
final:
	GDALWClose(shp_conn_ptr);

	return msg;
}

str
SHPpartialimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	size_t pos = 0;
	mvc *m = NULL;
	sql_schema *sch = NULL;
	char *sch_name = "sys";

	sql_table *shps_table = NULL, *fls_table = NULL, *data_table = NULL;
	char *shps_table_name = "shapefiles";
	char *fls_table_name = "files";
	char *data_table_name = NULL;

	sql_column *col;

	sql_column **cols;
	BAT **colsBAT;
	int colsNum = 2; //we will have at least the gid column and a geometry column
	int rowsNum = 0; //the number of rows in the shape file that will be imported
	int gidNum = 0;
	char *nameToLowerCase = NULL;

	str msg = MAL_SUCCEED;
	str fname = NULL;
	int vid = *(int*)getArgReference(stk, pci, 1);
	ptr* p = (ptr*)getArgReference(stk, pci, 2);
	wkb *g = (wkb*)*p;
	OGRGeometryH geom;
	OGREnvelope *mbb;
	/* SHP-level descriptor */
	OGRFieldDefnH hFieldDefn;
	int  i=0;
	oid irid;
	GDALWConnection shp_conn;
	GDALWConnection * shp_conn_ptr = NULL;
	GDALWSimpleFieldDef * field_definitions;
	OGRFeatureH feature;
	OGRFeatureDefnH featureDefn;
	gdk_return rc;

	/* calculate the mbb of the query geometry */
	geom = OGR_G_CreateGeometry(wkbPolygon);
	if (OGR_G_ImportFromWkb(geom, (unsigned char*)g->data, g->len) != OGRERR_NONE)
		return createException(MAL, "shp.import", SQLSTATE(38000) "Could not intantiate the query polygon.");
	if (!(mbb = (OGREnvelope*)GDKmalloc(sizeof(OGREnvelope))))
		return createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	OGR_G_GetEnvelope(geom, mbb);
	//FIXME: Take into account the coordinate reference system

	/* get table columns from shp and create the table */

	if((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	if(!(sch = mvc_bind_schema(m, sch_name)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Schema '%s' missing", sch_name);

	sqlstore *store = m->session->tr->store;
	/* find the name of the shape file corresponding to the given id */
	if(!(fls_table = mvc_bind_table(m, sch, fls_table_name)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Table '%s.%s' missing", sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "id")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(id)' missing", sch_name, fls_table_name);
	irid = store->table_api.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (is_oid_nil(irid))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Shapefile with id %d not in the %s.%s table\n", vid, sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "path")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(path)' missing", sch_name, fls_table_name);
	fname = (str)store->table_api.column_find_value(m->session->tr, col, irid);

	/* find the name of the table that has been reserved for this shape file */
	if(!(shps_table = mvc_bind_table(m, sch, shps_table_name)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Table '%s.%s' missing", sch_name, shps_table_name);
	if(!(col = mvc_bind_column(m, shps_table, "fileid")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(fileid)' missing", sch_name, shps_table_name);
	irid = store->table_api.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (is_oid_nil(irid))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Shapefile with id %d not in the Shapefile catalog\n", vid);
	if(!(col = mvc_bind_column(m, shps_table, "datatable")))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(datatable)' missing", sch_name, shps_table_name);
	data_table_name = (str)store->table_api.column_find_value(m->session->tr, col, irid);


	/* add the data on the file to the table */

	if(!(shp_conn_ptr = GDALWConnect((char *) fname)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Missing shape file %s\n", fname);
	shp_conn = *shp_conn_ptr;

	/*count the number of lines in the shape file */
	/* is there a better way to get this size? */
	OGR_L_ResetReading(shp_conn.layer);
	while( (feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL )
		rowsNum++;

	/* bind the columns of the data file that was just created
 	* and create a BAT for each of the columns */
	if(!(data_table = mvc_bind_table(m, sch, data_table_name)))
		return createException(MAL, "shp.import", SQLSTATE(38000) "Table '%s.%s' missing", sch_name, data_table_name);
	colsNum += shp_conn.numFieldDefinitions;
	if(!(cols = (sql_column**)GDKmalloc(sizeof(sql_column*)*colsNum)))
			return createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if(!(colsBAT = (BAT**)GDKzalloc(sizeof(BAT*)*colsNum)))
			return createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	field_definitions = GDALWGetSimpleFieldDefinitions(shp_conn);
	for(i = 0; i < colsNum - 2; i++) {
		cols[i] = NULL;
		/* bind the column */
		nameToLowerCase = toLower(field_definitions[i].fieldName);
		if(!(cols[i] = mvc_bind_column(m, data_table, nameToLowerCase)))
			return createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(%s)' missing", sch_name, data_table_name, field_definitions[i].fieldName);
		GDKfree(nameToLowerCase);
		/*create the BAT */
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_int, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		} else if (strcmp(field_definitions[i].fieldType, "Real") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_dbl, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
#if 0
		} else if (strcmp(field_definitions[i].fieldType, "Date") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
#endif
		} else {
			if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}
	if(!(cols[colsNum - 2] = mvc_bind_column(m, data_table, "gid"))) {
		msg = createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(gid)' missing", sch_name, data_table_name);
		goto bailout;
	}
	if(!(colsBAT[colsNum - 2] = COLnew(0, TYPE_int, rowsNum, PERSISTENT))) {
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if(!(cols[colsNum - 1] = mvc_bind_column(m, data_table, "geom"))) {
		msg = createException(MAL, "shp.import", SQLSTATE(38000) "Column '%s.%s(geom)' missing", sch_name, data_table_name);
		goto bailout;
	}
	if(!(colsBAT[colsNum - 1] = COLnew(0, ATOMindex("wkb"), rowsNum, PERSISTENT))) {
		msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	/* apply the spatial filter */
	OGR_L_SetSpatialFilterRect(shp_conn.layer, mbb->MinX, mbb->MinY, mbb->MaxX, mbb->MaxY);
	/* import the data */
	featureDefn = OGR_L_GetLayerDefn(shp_conn.layer);
	OGR_L_ResetReading(shp_conn.layer);
	while( (feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL ) {
		wkb *geomWKB;
		int len;
		int gidTemp = ++gidNum;

		OGRGeometryH geometry = OGR_F_GetGeometryRef(feature);

		for(i = 0; i < colsNum - 2; i++) {
			hFieldDefn = OGR_FD_GetFieldDefn( featureDefn, i );
			if( OGR_Fld_GetType(hFieldDefn) == OFTInteger ) {
				int val = OGR_F_GetFieldAsInteger(feature, i);
				rc = BUNappend(colsBAT[i], &val, false);
			} else if( OGR_Fld_GetType(hFieldDefn) == OFTReal ) {
				double val = OGR_F_GetFieldAsDouble(feature, i);
				rc = BUNappend(colsBAT[i], &val, false);
			} else if( OGR_Fld_GetType(hFieldDefn) == OFTString ) {
				rc = BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), false);
			} else {
				rc = BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), false);
			}
			if (rc != GDK_SUCCEED) {
				msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}

		if (BUNappend(colsBAT[colsNum - 2], &gidTemp, false) != HY013) {
			msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}

		len = OGR_G_WkbSize(geometry);
		if (!(geomWKB = GDKmalloc(sizeof(wkb) - 1 + len)))
			return createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		geomWKB->len = len;
		geomWKB->srid = 0; //TODO: Add the real srid
		OGR_G_ExportToWkb(geometry, wkbNDR, (unsigned char *)geomWKB->data);
		if (BUNappend(colsBAT[colsNum - 1], geomWKB, false) != GDK_SUCCEED) {
			msg = createException(MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}

	/* finalise the BATs */
	if (store->storage_api.claim_tab(m->session->tr, data_table, BATcount(colsBAT[0]), &offset, &pos) != LOG_OK) {
		msg = createException(MAL, "shp.import", SQLSTATE(38000) "append_col failed");
		goto bailout;
	}
	if (!isNew(data_table) && sql_trans_add_dependency_change(m->session->tr, data_table->base.id, dml) != LOG_OK) {
		msg = createException((MAL, "shp.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	for(i = 0; i < colsNum; i++) {
		if (store->storage_api.append_col(m->session->tr, cols[i], offset, pos, colsBAT[i], BATcount(colsBAT[0]), TYPE_bat) != LOG_OK) {
			msg = createException(MAL, "shp.import", SQLSTATE(38000) "append_col failed");
			goto bailout;
		}
	}

  bailout:
	for(i = 0; i < colsNum; i++) {
		if (colsBAT[i])
			BBPunfix(colsBAT[i]->batCacheid);
	}

	/* free the memory */
	GDKfree(cols);
	GDKfree(colsBAT);
	OGR_G_DestroyGeometry(geom);

	free(field_definitions);
	GDALWClose(shp_conn_ptr);

	return msg;

}

str SHPimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SHPimportFile(cntxt, mb, stk, pci, false);
}

str SHPpartialimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SHPimportFile(cntxt, mb, stk, pci, true);
}
#endif
