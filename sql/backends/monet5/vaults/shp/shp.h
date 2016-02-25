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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _SHP_
#define _SHP_
#include "mal.h"
#include "mal_client.h"
#include <gdal.h>
#include <ogr_api.h>

OGRErr OSRExportToProj4 (OGRSpatialReferenceH, char **);
OGRErr 	OSRExportToWkt (OGRSpatialReferenceH, char **);
const char * OSRGetAttrValue (OGRSpatialReferenceH hSRS, const char *pszName, int iChild);

/* CURRENT_TIMESTAMP() ?*/
#define INSFILE \
	"INSERT INTO files(id,path) \
	 VALUES(%d, '%s');"
#define INSSHP \
	"INSERT INTO shapefiles(shapefileid, fileid, mbb, srid, datatable) \
	 VALUES(%d, %d, NULL, %d, '%s');"
#define INSSHPDBF \
	"INSERT INTO shapefiles_dbf(shapefileid, attr, datatype) \
	 VALUES('%d', '%s', '%s');"
#define CRTTBL \
	"CREATE TABLE %s (gid SERIAL, %s);"
#define INSTD \
	"INSERT INTO %s (%s) VALUES (%s);"

typedef struct {
	const char * fieldName;
	const char * fieldType;
} GDALWSimpleFieldDef;

typedef struct {
	char * source;
	OGRDataSourceH handler;
	const char * layername;
	OGRLayerH layer;
	OGRSFDriverH driver;
	OGRFieldDefnH * fieldDefinitions;
	int numFieldDefinitions;
} GDALWConnection;

typedef struct {
	int epsg;
	const char * authName;
	const char * srsText;
	const char * proj4Text;
}GDALWSpatialInfo;

GDALWConnection * GDALWConnect(char *);
GDALWSimpleFieldDef * GDALWGetSimpleFieldDefinitions(GDALWConnection);
GDALWSpatialInfo GDALWGetSpatialInfo(GDALWConnection);
void GDALWPrintRecords(GDALWConnection);
void GDALWClose(GDALWConnection *);


#ifdef WIN32
#ifndef LIBGTIFF										/* change it! */
#define shp_export extern __declspec(dllimport)
#else
#define shp_export extern __declspec(dllexport)
#endif
#else
#define shp_export extern
#endif

shp_export str SHPattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
shp_export str SHPimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
shp_export str SHPpartialimport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif

