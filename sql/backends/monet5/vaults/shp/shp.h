/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _SHP_
#define _SHP_
#include "mal.h"
#include "mal_client.h"

/* these were previously defined in monetdb_config.h */
#undef HAVE_DLFCN_H
#undef HAVE_FCNTL_H
#undef HAVE_ICONV
#undef HAVE_STRINGS_H
#include <gdal.h>
#include <ogr_api.h>

OGRErr OSRExportToProj4(OGRSpatialReferenceH, char **);
OGRErr OSRExportToWkt(OGRSpatialReferenceH, char **);
const char *OSRGetAttrValue(OGRSpatialReferenceH hSRS, const char *pszName, int iChild);

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

typedef struct
{
	const char *fieldName;
	const char *fieldType;
} GDALWSimpleFieldDef;

typedef struct
{
	char *source;
	OGRDataSourceH handler;
	const char *layername;
	OGRLayerH layer;
	OGRSFDriverH driver;
	OGRFieldDefnH *fieldDefinitions;
	int numFieldDefinitions;
} GDALWConnection;

typedef struct
{
	int epsg;
	const char *authName;
	const char *srsText;
	const char *proj4Text;
} GDALWSpatialInfo;

GDALWConnection *GDALWConnect(char *);
GDALWSimpleFieldDef *GDALWGetSimpleFieldDefinitions(GDALWConnection);
GDALWSpatialInfo GDALWGetSpatialInfo(GDALWConnection);
void GDALWPrintRecords(GDALWConnection);
void GDALWClose(GDALWConnection *);

#ifdef WIN32
#ifndef LIBGTIFF /* change it! */
#define shp_export extern __declspec(dllimport)
#else
#define shp_export extern __declspec(dllexport)
#endif
#else
#define shp_export extern
#endif

str createSHPtable(Client cntxt, str schemaname, str tablename, GDALWConnection shp_conn, GDALWSimpleFieldDef *field_definitions);
str loadSHPtable(mvc *m, sql_schema *sch, str schemaname, str tablename, GDALWConnection shp_conn, GDALWSimpleFieldDef *field_definitions, GDALWSpatialInfo spatial_info);
str SHPloadSchema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str SHPload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif
