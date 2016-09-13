/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * 
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * Authors: K. Kyzirakos, D. Savva
 * This module contains primitives for accessing geospatial data 
 * stored in ESRI Shapefile documents.
 */

#include <monetdb_config.h>
#include <string.h>
#include "sql_mvc.h"
#include "sql.h"
#include <stdlib.h>
#include "shp.h"
#include "sql_scenario.h"
#include "mal_exception.h"

#include <geom.h>

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

GDALWConnection * GDALWConnect(char * source) {
	GDALWConnection * conn = NULL;
	OGRFeatureDefnH featureDefn;
	int fieldCount, i;
	OGRRegisterAll();
	conn = malloc(sizeof(GDALWConnection));
	if (conn == NULL) {
		fprintf(stderr, "Could not allocate memory\n");
		exit(-1);
	}
	conn->handler = OGROpen(source, 0 , &(conn->driver));
	if (conn->handler == NULL) {
		return NULL;
	}


	conn->layer = OGR_DS_GetLayer(conn->handler, 0);
	if (conn->layer == NULL) {
		return NULL;
	}

	conn->layername = (const char *) OGR_L_GetName(conn->layer);

	featureDefn = OGR_L_GetLayerDefn(conn->layer);
	fieldCount = OGR_FD_GetFieldCount(featureDefn);
	conn->numFieldDefinitions = fieldCount;
	conn->fieldDefinitions = malloc(fieldCount * sizeof(OGRFieldDefnH));
	if (conn->fieldDefinitions == NULL) {
		fprintf(stderr, "Could not allocate memory\n");
		exit(-1);
	}
	for (i=0 ; i<fieldCount ; i++) {
		conn->fieldDefinitions[i] = OGR_FD_GetFieldDefn(featureDefn, i);
	}

	return conn;
}

void GDALWClose(GDALWConnection * conn) {
	free(conn->fieldDefinitions);
	OGRReleaseDataSource(conn->handler);
}

GDALWSimpleFieldDef * GDALWGetSimpleFieldDefinitions(GDALWConnection conn) {
	int i;
	GDALWSimpleFieldDef * columns;
	OGRFieldDefnH fieldDefn;
	/*if (conn.layer == NULL || conn.handler == NULL || conn.driver == NULL) {
		printf("Could not extract columns, initialize a connection first.\n");
		exit(-1);
	}*/
	columns = malloc(conn.numFieldDefinitions * sizeof(GDALWSimpleFieldDef));
	if (columns == NULL) {
		//fprintf(stderr, "Could not allocate memory\n");
		return NULL;
	}
	for (i=0 ; i<conn.numFieldDefinitions ; i++) {
		fieldDefn = conn.fieldDefinitions[i];
		columns[i].fieldName = OGR_Fld_GetNameRef(fieldDefn);
		columns[i].fieldType = OGR_GetFieldTypeName(OGR_Fld_GetType(fieldDefn));
	}

	return columns;
}

void GDALWPrintRecords(GDALWConnection conn) {
	char * wkt;
	int i;
	OGRFeatureH feature;
	OGRGeometryH geometry;
	OGRFeatureDefnH featureDefn;
	featureDefn = OGR_L_GetLayerDefn(conn.layer);
	OGR_L_ResetReading(conn.layer);
	while( (feature = OGR_L_GetNextFeature(conn.layer)) != NULL ) {
		for(i = 0; i < OGR_FD_GetFieldCount(featureDefn); i++ ) {
			OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn( featureDefn, i );
		    if( OGR_Fld_GetType(hFieldDefn) == OFTInteger )
		    	printf( "%d,", OGR_F_GetFieldAsInteger( feature, i ) );
		    else if( OGR_Fld_GetType(hFieldDefn) == OFTReal )
		        printf( "%.3f,", OGR_F_GetFieldAsDouble( feature, i) );
		    else if( OGR_Fld_GetType(hFieldDefn) == OFTString )
		        printf( "%s,", OGR_F_GetFieldAsString( feature, i) );
		    else
		    	printf( "%s,", OGR_F_GetFieldAsString( feature, i) );

		}
		geometry = OGR_F_GetGeometryRef(feature);
		OGR_G_ExportToWkt(geometry, &wkt);
		printf("%s", wkt);
		printf("\n");
		OGRFree(wkt);
		OGR_F_Destroy(feature);
	}
}

GDALWSpatialInfo GDALWGetSpatialInfo(GDALWConnection conn) {
	GDALWSpatialInfo spatialInfo;
	OGRSpatialReferenceH spatialRef = OGR_L_GetSpatialRef(conn.layer);
	char * proj4, * srsText, * srid;

	OSRExportToProj4(spatialRef, &proj4);
	OSRExportToWkt(spatialRef, &srsText);
	srid = (char *) OSRGetAttrValue(spatialRef, "AUTHORITY", 1);
	if (srid == NULL) {
		spatialInfo.epsg = 4326;
	}
	else {
		spatialInfo.epsg = atoi(OSRGetAttrValue(spatialRef, "AUTHORITY", 1));
	}
	spatialInfo.authName = OSRGetAttrValue(spatialRef, "AUTHORITY", 0);
	if (spatialInfo.authName == NULL) {
		spatialInfo.authName = "EPSG";
	}
	spatialInfo.proj4Text = proj4;
	spatialInfo.srsText = srsText;

	return spatialInfo;

}

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
		return createException(MAL, "shp.attach", "Schema sys missing\n");

	fls = mvc_bind_table(m, sch, "files");
	shps = mvc_bind_table(m, sch, "shapefiles");
	shps_dbf = mvc_bind_table(m, sch, "shapefiles_dbf");
	if (fls == NULL || shps == NULL || shps_dbf == NULL )
		return createException(MAL, "shp.attach", "Catalog table missing\n");

	if ((shp_conn_ptr = GDALWConnect((char *) fname)) == NULL) {
		return createException(MAL, "shp.attach", "Missing shp file %s\n", fname);
	}
	shp_conn = *shp_conn_ptr;

	/* check if the file is already attached */
	col = mvc_bind_column(m, fls, "path");
	rid = table_funcs.column_find_row(m->session->tr, col, fname, NULL);
	if (rid != oid_nil) {
		GDALWClose(shp_conn_ptr);
		return createException(MAL, "shp.attach", "File %s already attached\n", fname);
	}

	/* add row in the files(id, path) catalog table */
	col = mvc_bind_column(m, fls, "id");
	fid = store_funcs.count_col(m->session->tr, col, 1) + 1;
	snprintf(buf, BUFSIZ, INSFILE, (int)fid, fname);
	if ( ( msg = SQLstatementIntern(cntxt, &s,"shp.attach",TRUE,FALSE,NULL)) != MAL_SUCCEED)
		goto finish;


	/*if (shp_conn.layer == NULL || shp_conn.source == NULL || shp_conn.handler == NULL || shp_conn.driver == NULL) {
		msg = createException(MAL, "shp.attach", "lol-1\n");
									return msg;
	}*/

	/* add row in the shapefiles catalog table (e.g. the name of the table that will store tha data of the shapefile) */
	spatial_info = GDALWGetSpatialInfo(shp_conn);
	col = mvc_bind_column(m, shps, "shapefileid");
	shpid = store_funcs.count_col(m->session->tr, col, 1) + 1;
	nameToLowerCase = toLower(shp_conn.layername);
	snprintf(buf, BUFSIZ, INSSHP, shpid, (int)fid, spatial_info.epsg, nameToLowerCase);
	GDKfree(nameToLowerCase);
	if ( ( msg = SQLstatementIntern(cntxt, &s,"shp.attach",TRUE,FALSE,NULL)) != MAL_SUCCEED)
			goto finish;

	/* add information about the fields of the shape file 
 	* one row for each field with info (shapefile_id, field_name, field_type) */
	field_definitions = GDALWGetSimpleFieldDefinitions(shp_conn);
	if (field_definitions == NULL) {
		GDALWClose(&shp_conn);
		return createException(MAL, "shp.attach", MAL_MALLOC_FAIL);
	}
	for (i=0 ; i<shp_conn.numFieldDefinitions ; i++) {
		snprintf(buf, BUFSIZ, INSSHPDBF, shpid, field_definitions[i].fieldName, field_definitions[i].fieldType);
		if ( ( msg = SQLstatementIntern(cntxt, &s,"shp.attach",TRUE,FALSE,NULL)) != MAL_SUCCEED)
			goto fin;
	}

	/* create the table that will store the data of the shape file */
	temp_buf[0]='\0';
	for (i=0 ; i<shp_conn.numFieldDefinitions ; i++) {
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0) {
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" INT, ", toLower(field_definitions[i].fieldName));
		} else if (strcmp(field_definitions[i].fieldType, "Real") == 0) {
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" FLOAT, ", toLower(field_definitions[i].fieldName));
		} else if (strcmp(field_definitions[i].fieldType, "Date") == 0) {
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" STRING, ", toLower(field_definitions[i].fieldName));
        	} else 
			sprintf(temp_buf + strlen(temp_buf), "\"%s\" STRING, ", toLower(field_definitions[i].fieldName));
	}

	sprintf(temp_buf + strlen(temp_buf), "geom GEOMETRY ");
	snprintf(buf, BUFSIZ, CRTTBL, shp_conn.layername, temp_buf);

	if ( ( msg = SQLstatementIntern(cntxt, &s,"shp.import",TRUE,FALSE,NULL)) != MAL_SUCCEED)
		goto fin;

fin:
	free(field_definitions);
finish:
	/* if (msg != MAL_SUCCEED){
	   snprintf(buf, BUFSIZ,"ROLLBACK;");
	   SQLstatementIntern(cntxt,&s,"geotiff.attach",TRUE,FALSE));
	   }*/
	GDALWClose(&shp_conn);
	return msg;
}



static str
SHPimportFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool partial) {
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
		return createException(MAL, "shp.import", "Schema '%s' missing", sch_name);

	/* find the name of the shape file corresponding to the given id */
	if(!(fls_table = mvc_bind_table(m, sch, fls_table_name)))
		return createException(MAL, "shp.import", "Table '%s.%s' missing", sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "id")))
		return createException(MAL, "shp.import", "Column '%s.%s(id)' missing", sch_name, fls_table_name);
	irid = table_funcs.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (irid == oid_nil)
		return createException(MAL, "shp.import", "Shapefile with id %d not in the %s.%s table\n", vid, sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "path")))
		return createException(MAL, "shp.import", "Column '%s.%s(path)' missing", sch_name, fls_table_name);
	fname = (str)table_funcs.column_find_value(m->session->tr, col, irid); 

	/* find the name of the table that has been reserved for this shape file */
	if(!(shps_table = mvc_bind_table(m, sch, shps_table_name)))
		return createException(MAL, "shp.import", "Table '%s.%s' missing", sch_name, shps_table_name);
	if(!(col = mvc_bind_column(m, shps_table, "fileid")))
		return createException(MAL, "shp.import", "Column '%s.%s(fileid)' missing", sch_name, shps_table_name);
	irid = table_funcs.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (irid == oid_nil)
		return createException(MAL, "shp.import", "Shapefile with id %d not in the Shapefile catalog\n", vid);
	if(!(col = mvc_bind_column(m, shps_table, "datatable")))
		return createException(MAL, "shp.import", "Column '%s.%s(datatable)' missing", sch_name, shps_table_name);
	data_table_name = (str)table_funcs.column_find_value(m->session->tr, col, irid);


	/* add the data on the file to the table */
	if(!(shp_conn_ptr = GDALWConnect((char *) fname))) 
		return createException(MAL, "shp.import", "Missing shp file %s\n", fname);
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
			msg = createException(MAL, "shp.import", "Could not intantiate the query polygon.");
			OGR_F_Destroy(geom);
			goto final;
		}
		if (!(mbb = (OGREnvelope*)GDKmalloc(sizeof(OGREnvelope)))) {
			msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
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
		msg = createException(MAL, "shp.import", "Table '%s.%s' missing", sch_name, data_table_name);
		goto final;
	}
	colsNum += shp_conn.numFieldDefinitions;
	if(!(cols = (sql_column**)GDKmalloc(sizeof(sql_column*)*colsNum))) {
		msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		goto final;
	}
	if(!(colsBAT = (BAT**)GDKmalloc(sizeof(BAT*)*colsNum))) {
		msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		goto unfree2;
	}
	field_definitions = GDALWGetSimpleFieldDefinitions(shp_conn);
	for(i = 0; i < colsNum - 2; i++) {
		cols[i] = NULL;
		/* bind the column */
		nameToLowerCase = toLower(field_definitions[i].fieldName);
		if(!(cols[i] = mvc_bind_column(m, data_table, nameToLowerCase))) {
			msg = createException(MAL, "shp.import", "Column '%s.%s(%s)' missing", sch_name, data_table_name, field_definitions[i].fieldName);
			goto unfree4;
		}
		GDKfree(nameToLowerCase);
		/*create the BAT */
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_int, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
				goto unfree4;
			}
		} else if (strcmp(field_definitions[i].fieldType, "Real") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_dbl, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
				goto unfree4;
			}
		} else if (strcmp(field_definitions[i].fieldType, "Date") == 0) {
        	if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
				goto unfree4;
			}
		} else {
			if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT))) {
				msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
				goto unfree4;
			}
		}
	}
	if(!(cols[colsNum - 2] = mvc_bind_column(m, data_table, "gid"))) {
		msg = createException(MAL, "shp.import", "Column '%s.%s(gid)' missing", sch_name, data_table_name);
		goto unfree4;
	}
	if(!(colsBAT[colsNum - 2] = COLnew(0, TYPE_int, rowsNum, PERSISTENT))) {
		msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		goto unfree4;
	}
	if(!(cols[colsNum - 1] = mvc_bind_column(m, data_table, "geom"))) {
		msg = createException(MAL, "shp.import", "Column '%s.%s(geom)' missing", sch_name, data_table_name);
		goto unfree4;
	}
	if(!(colsBAT[colsNum - 1] = COLnew(0, ATOMindex("wkb"), rowsNum, PERSISTENT))) {
		msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		goto unfree4;
	}

	/* import the data */
	featureDefn = OGR_L_GetLayerDefn(shp_conn.layer);
	OGR_L_ResetReading(shp_conn.layer);
	while((feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL ) {
		wkb *geomWKB;
		int len;
		int gidTemp = ++gidNum;

		OGRGeometryH geometry = OGR_F_GetGeometryRef(feature);

		for(i = 0; i < colsNum - 2; i++) {
			hFieldDefn = OGR_FD_GetFieldDefn( featureDefn, i );
			if( OGR_Fld_GetType(hFieldDefn) == OFTInteger ) {
				int val = OGR_F_GetFieldAsInteger(feature, i);
				BUNappend(colsBAT[i], &val, TRUE);
			} else if( OGR_Fld_GetType(hFieldDefn) == OFTReal ) {
				double val = OGR_F_GetFieldAsDouble(feature, i);
				BUNappend(colsBAT[i], &val, TRUE);
			} else if( OGR_Fld_GetType(hFieldDefn) == OFTString ) {
				BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), TRUE);
			} else {
				BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), TRUE);
			}
		}
		BUNappend(colsBAT[colsNum - 2], &gidTemp, TRUE);

		len = OGR_G_WkbSize(geometry);
		if (!(geomWKB = GDKmalloc(sizeof(wkb) - 1 + len))) {
			msg = createException(MAL, "shp.import", MAL_MALLOC_FAIL);
			OGR_F_Destroy(feature);
			goto unfree4;
		}
		geomWKB->len = len;
		geomWKB->srid = 0; //FIXME: Add the real srid
		OGR_G_ExportToWkb(geometry, wkbNDR, (unsigned char *)geomWKB->data);
		BUNappend(colsBAT[colsNum - 1], geomWKB, TRUE);
		GDKfree(geomWKB);
		OGR_F_Destroy(feature);
	}

	/* finalise the BATs */
	for(i = 0; i < colsNum; i++) {
		store_funcs.append_col(m->session->tr, cols[i], colsBAT[i], TYPE_bat);
		BBPunfix(colsBAT[i]->batCacheid);
		//BBPdecref(colsBAT[i]->batCacheid, TRUE);
	}

	/* free the memory */
unfree4:
	free(field_definitions);
	GDKfree(colsBAT);
unfree2:
	GDKfree(cols);
final:
	GDALWClose(shp_conn_ptr);

	return msg;
}

#if 0
str
SHPpartialimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
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

	/* calculate the mbb of the query geometry */
	geom = OGR_G_CreateGeometry(wkbPolygon);
	if (OGR_G_ImportFromWkb(geom, (unsigned char*)g->data, g->len) != OGRERR_NONE)
		return createException(MAL, "shp.import", "Could not intantiate the query polygon.");
	if (!(mbb = (OGREnvelope*)GDKmalloc(sizeof(OGREnvelope)))) 
		return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
	OGR_G_GetEnvelope(geom, mbb);
	//FIXME: Take into account the coordinate reference system

	/* get table columns from shp and create the table */

	if((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
		return msg;
	if((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	if(!(sch = mvc_bind_schema(m, sch_name)))
		return createException(MAL, "shp.import", "Schema '%s' missing", sch_name);

	/* find the name of the shape file corresponding to the given id */
	if(!(fls_table = mvc_bind_table(m, sch, fls_table_name)))
		return createException(MAL, "shp.import", "Table '%s.%s' missing", sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "id")))
		return createException(MAL, "shp.import", "Column '%s.%s(id)' missing", sch_name, fls_table_name);
	irid = table_funcs.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (irid == oid_nil)
		return createException(MAL, "shp.import", "Shapefile with id %d not in the %s.%s table\n", vid, sch_name, fls_table_name);
	if(!(col = mvc_bind_column(m, fls_table, "path")))
		return createException(MAL, "shp.import", "Column '%s.%s(path)' missing", sch_name, fls_table_name);
	fname = (str)table_funcs.column_find_value(m->session->tr, col, irid); 

	/* find the name of the table that has been reserved for this shape file */
	if(!(shps_table = mvc_bind_table(m, sch, shps_table_name)))
		return createException(MAL, "shp.import", "Table '%s.%s' missing", sch_name, shps_table_name);
	if(!(col = mvc_bind_column(m, shps_table, "fileid")))
		return createException(MAL, "shp.import", "Column '%s.%s(fileid)' missing", sch_name, shps_table_name);
	irid = table_funcs.column_find_row(m->session->tr, col, (void *)&vid, NULL);
	if (irid == oid_nil)
		return createException(MAL, "shp.import", "Shapefile with id %d not in the Shapefile catalog\n", vid);
	if(!(col = mvc_bind_column(m, shps_table, "datatable")))
		return createException(MAL, "shp.import", "Column '%s.%s(datatable)' missing", sch_name, shps_table_name);
	data_table_name = (str)table_funcs.column_find_value(m->session->tr, col, irid);


	/* add the data on the file to the table */

	if(!(shp_conn_ptr = GDALWConnect((char *) fname)))
		return createException(MAL, "shp.import", "Missing shp file %s\n", fname);
	shp_conn = *shp_conn_ptr;

	/*count the number of lines in the shape file */
	/* is there a better way to get this size? */
	OGR_L_ResetReading(shp_conn.layer);
	while( (feature = OGR_L_GetNextFeature(shp_conn.layer)) != NULL )
		rowsNum++;

	/* bind the columns of the data file that was just created 
 	* and create a BAT for each of the columns */
	if(!(data_table = mvc_bind_table(m, sch, data_table_name)))
		return createException(MAL, "shp.import", "Table '%s.%s' missing", sch_name, data_table_name);
	colsNum += shp_conn.numFieldDefinitions;
	if(!(cols = (sql_column**)GDKmalloc(sizeof(sql_column*)*colsNum)))
			return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
	if(!(colsBAT = (BAT**)GDKmalloc(sizeof(BAT*)*colsNum)))
			return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
	field_definitions = GDALWGetSimpleFieldDefinitions(shp_conn);
	for(i = 0; i < colsNum - 2; i++) {
		cols[i] = NULL;
		/* bind the column */
		nameToLowerCase = toLower(field_definitions[i].fieldName);
		if(!(cols[i] = mvc_bind_column(m, data_table, nameToLowerCase)))
			return createException(MAL, "shp.import", "Column '%s.%s(%s)' missing", sch_name, data_table_name, field_definitions[i].fieldName);
		GDKfree(nameToLowerCase);
		/*create the BAT */
		if (strcmp(field_definitions[i].fieldType, "Integer") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_int, rowsNum, PERSISTENT)))
				return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		} else if (strcmp(field_definitions[i].fieldType, "Real") == 0) {
			if(!(colsBAT[i] = COLnew(0, TYPE_dbl, rowsNum, PERSISTENT)))
				return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		} else if (strcmp(field_definitions[i].fieldType, "Date") == 0) {
        	if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT)))
				return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		} else {
			if(!(colsBAT[i] = COLnew(0, TYPE_str, rowsNum, PERSISTENT)))
				return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		}
	}
	if(!(cols[colsNum - 2] = mvc_bind_column(m, data_table, "gid")))
		return createException(MAL, "shp.import", "Column '%s.%s(gid)' missing", sch_name, data_table_name);
	if(!(colsBAT[colsNum - 2] = COLnew(0, TYPE_int, rowsNum, PERSISTENT)))
		return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
	if(!(cols[colsNum - 1] = mvc_bind_column(m, data_table, "geom")))
		return createException(MAL, "shp.import", "Column '%s.%s(geom)' missing", sch_name, data_table_name);
	if(!(colsBAT[colsNum - 1] = COLnew(0, ATOMindex("wkb"), rowsNum, PERSISTENT)))
		return createException(MAL, "shp.import", MAL_MALLOC_FAIL);

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
				BUNappend(colsBAT[i], &val, TRUE);
			} else if( OGR_Fld_GetType(hFieldDefn) == OFTReal ) {
				double val = OGR_F_GetFieldAsDouble(feature, i);
				BUNappend(colsBAT[i], &val, TRUE);
			} else if( OGR_Fld_GetType(hFieldDefn) == OFTString ) {
				BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), TRUE);
			} else {
				BUNappend(colsBAT[i], OGR_F_GetFieldAsString(feature, i), TRUE);
			}
		}
	
		BUNappend(colsBAT[colsNum - 2], &gidTemp, TRUE);

		len = OGR_G_WkbSize(geometry);
		if (!(geomWKB = GDKmalloc(sizeof(wkb) - 1 + len)))
			return createException(MAL, "shp.import", MAL_MALLOC_FAIL);
		geomWKB->len = len;
		geomWKB->srid = 0; //TODO: Add the real srid
		OGR_G_ExportToWkb(geometry, wkbNDR, (unsigned char *)geomWKB->data);
		BUNappend(colsBAT[colsNum - 1], geomWKB, TRUE);
	}
		
	/* finalise the BATs */
	for(i = 0; i < colsNum; i++) {
		store_funcs.append_col(m->session->tr, cols[i], colsBAT[i], TYPE_bat);
		BBPunfix(colsBAT[i]->batCacheid);
		BBPdecref(colsBAT[i]->batCacheid, TRUE);
	}
		
	/* free the memory */
	GDKfree(cols);
	GDKfree(colsBAT);
	OGR_G_DestroyGeometry(geom);

	free(field_definitions);
	GDALWClose(shp_conn_ptr);

	return msg;

}
#endif

str
SHPimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return SHPimportFile(cntxt, mb, stk, pci, false);
}

str
SHPpartialimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return SHPimportFile(cntxt, mb, stk, pci, true);
}
