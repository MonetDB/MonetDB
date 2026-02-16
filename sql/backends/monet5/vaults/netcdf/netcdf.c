/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "gdk.h"
#include "gdk_system.h"
#include "mal_arguments.h"
#include "monetdb_config.h"
#include "sql_mem.h"
#include "rel_file_loader.h"
#include "rel_exp.h"
#include "sql_catalog.h"
#include "sql_list.h"
#include "sql_mvc.h"
#include "sql.h"
#include "sql_execute.h"
#include "sql_relation.h"
#include "sql_scenario.h"
#include "mal_exception.h"
#include "mal_instruction.h"
#include "mal_builder.h"
#include <netcdf.h>
#include "netcdf_vault.h"
#include "sql_statement.h"
#include "sql_types.h"

/* SQL statements for population of NetCDF catalog */
#define INSFILE \
	"INSERT INTO netcdf_files(file_id,location) VALUES(%d, '%s');"

#define INSDIM \
	"INSERT INTO netcdf_dims(dim_id,file_id,name,length) VALUES(%d, %d, '%s', %d);"

#define INSVAR \
	"INSERT INTO netcdf_vars(var_id,file_id,name,vartype,ndim,coord_dim_id) VALUES(%d, %d, '%s', '%s', %d, %d);"

#define INSVARDIM \
	"INSERT INTO netcdf_vardim (var_id,dim_id,file_id,dimpos) VALUES(%d, %d, %d, %d);"

#define INSATTR \
	"INSERT INTO netcdf_attrs (obj_name,att_name,att_type,value,file_id,gr_name) VALUES('%s', '%s', '%s', '%s', %d, '%s');"

#define LOAD_NCDF_VAR(tpe,ncdftpe) \
	{ \
	tpe *databuf; \
	res = COLnew(0, TYPE_##tpe, sz, TRANSIENT); \
	if ( res == NULL ) \
		return createException(MAL, "netcdf.importvar", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
	databuf = (tpe *)Tloc(res, 0); \
	if ( (retval = nc_get_var_##ncdftpe(ncid, varid, databuf)) ) \
		return createException(MAL, "netcdf.importvar", \
						   SQLSTATE(NC000) "Cannot read variable %d values: %s", \
						   varid, nc_strerror(retval)); \
	}

static void
fix_quote( char *n, int l)
{
	int i;

	for(i=0;i<l;i++)
		if (n[i]=='\'')
			n[i] = ' ';
}


/* simple test for netcdf library */
str
NCDFtest(Client ctx, int *vars, str *fname)
{
	(void) ctx;
	int ncid;   /* dataset id */
	int dims, ngatts, unlimdim;
	int retval;

	str msg = MAL_SUCCEED;

	/* Open NetCDF file  */
	if ((retval = nc_open(*fname, NC_NOWRITE, &ncid)))
		return createException(MAL, "netcdf.test", SQLSTATE(NC000) "Cannot open NetCDF file %s: %s", *fname, nc_strerror(retval));

	if ((retval = nc_inq(ncid, &dims, vars, &ngatts, &unlimdim)))
		return createException(MAL, "netcdf.test", SQLSTATE(NC000) "Cannot read NetCDF header: %s", nc_strerror(retval));

	if ((retval = nc_close(ncid)))
		return createException(MAL, "netcdf.test", SQLSTATE(NC000) "Cannot close file %s: \
%s", *fname, nc_strerror(retval));

	return msg;
}

/* the following function is from ncdump utility: NetCDF type number to name */
static const char *
prim_type_name(nc_type type)
{
	switch (type) {
	case NC_BYTE:
		return "byte";
	case NC_CHAR:
		return "char";
	case NC_SHORT:
		return "short";
	case NC_INT:
		return "int";
	case NC_FLOAT:
		return "float";
	case NC_DOUBLE:
		return "double";
#ifdef USE_NETCDF4
	case NC_UBYTE:
		return "ubyte";
	case NC_USHORT:
		return "ushort";
	case NC_UINT:
		return "uint";
	case NC_INT64:
		return "int64";
	case NC_UINT64:
		return "uint64";
	case NC_STRING:
		return "string";
#endif /* USE_NETCDF4 */
	default:
		return "bad type";
	}
}

/* Mapping NetCDF to SQL data type */

static const char *
NCDF2SQL(nc_type type)
{
	switch (type) {
	case NC_BYTE:
		return "tinyint";
	case NC_CHAR:
		return "char(1)";
	case NC_SHORT:
		return "smallint";
	case NC_INT:
		return "int";
	case NC_FLOAT:
		return "float";
	case NC_DOUBLE:
		return "double";
#ifdef USE_NETCDF4
/* ?? mapping of unsigned types */
	case NC_UBYTE:
		return "ubyte";
	case NC_USHORT:
		return "ushort";
	case NC_UINT:
		return "uint";
	case NC_INT64:
		return "bigint";
	case NC_UINT64:
		return "uint64";
	case NC_STRING:
		return "string";
#endif /* USE_NETCDF4 */
	default:
		return "type not supported";
	}
}

#define array_series(sta, ste, sto, TYPE) {				\
		int s,g;										\
		TYPE i, *o = (TYPE*)Tloc(bn, 0);				\
		TYPE start = sta, step = ste, stop = sto;		\
		if ( start < stop && step > 0) {				\
			for ( s = 0; s < series; s++)				\
				for ( i = start; i < stop; i += step)	\
					for( g = 0; g < group; g++){		\
						*o = i;							\
						o++;							\
					}									\
		} else {										\
			for ( s = 0; s < series; s++)				\
				for ( i = start; i > stop; i += step)	\
					for( g = 0; g < group; g++){		\
						*o = i;							\
						o++;							\
					}									\
		}												\
	}

/* create and populate a dimension bat */
static str
NCDFARRAYseries(bat *bid, bte start, bte step, int stop, int group, int series)
{
	BAT *bn = NULL;
	BUN cnt = 0;

	cnt =  (BUN) ceil(((stop * 1.0 - start) / step)) * group * series ;
	if (stop <= (int) GDK_bte_max ) {
		bte sta = (bte) start, ste = (bte) step, sto = (bte) stop;

		bn = COLnew(0, TYPE_bte, cnt, TRANSIENT);
		if ( bn == NULL)
			throw(MAL, "ntcdf.loadvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		array_series(sta, ste, sto, bte);
	} else if (stop <= (int) GDK_sht_max) {
		sht sta = (sht) start, ste = (sht) step, sto = (sht) stop;

		bn = COLnew(0, TYPE_sht, cnt, TRANSIENT);
		if ( bn == NULL)
			throw(MAL, "netcdf.loadvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		array_series(sta, ste, sto, sht);
	} else {
		int sta = (int) start, ste = (int) step, sto = (int) stop;

		bn = COLnew(0, TYPE_int, cnt, TRANSIENT);
		if ( bn == NULL)
			throw(MAL, "netcdf.loadvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		array_series(sta, ste, sto, int);
	}

	BATsetcount(bn, cnt);
	bn->tsorted = (cnt <= 1 || (series == 1 && step > 0));
	bn->trevsorted = (cnt <= 1 || (series == 1 && step < 0));
	bn->tkey = (cnt <= 1);
	bn->tnonil = true;
	*bid = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

str
NCDFattach(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	sql_table *tfiles = NULL, *tdims = NULL, *tvars = NULL, *tvardim = NULL, *tattrs = NULL;
	sql_column *col;
	str msg = MAL_SUCCEED;
	str fname = *getArgReference_str(stk, pci, 1);
	char buf[BUFSIZ], *s= buf;
	oid fid, rid = oid_nil;
	sql_trans *tr;

	int ncid;   /* dataset id */
	int ndims, nvars, ngatts, unlimdim;
	int didx, vidx, vndims, vnatts, i, aidx, coord_dim_id = -1;
	int vdims[NC_MAX_VAR_DIMS];

	size_t dlen, alen;
	char dname[NC_MAX_NAME+1], vname[NC_MAX_NAME+1], aname[NC_MAX_NAME +1],
		abuf[80], *aval;
	char **dims = NULL;
	nc_type vtype, atype; /* == int */

	int retval, avalint;
	float avalfl;
	double avaldbl;
	str esc_str0, esc_str1;

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if (msg)
		return msg;

	tr = m->session->tr;
	sqlstore *store = tr->store;
	sch = mvc_bind_schema(m, "sys");
	if ( !sch )
		return createException(MAL, "netcdf.attach", SQLSTATE(NC000) "Cannot get schema sys\n");

	tfiles = mvc_bind_table(m, sch, "netcdf_files");
	tdims = mvc_bind_table(m, sch, "netcdf_dims");
	tvars = mvc_bind_table(m, sch, "netcdf_vars");
	tvardim = mvc_bind_table(m, sch, "netcdf_vardim");
	tattrs = mvc_bind_table(m, sch, "netcdf_attrs");

	if (tfiles == NULL || tdims == NULL || tvars == NULL ||
		tvardim == NULL || tattrs == NULL)
		return createException(MAL, "netcdf.attach", SQLSTATE(NC000) "Catalog table missing\n");

	/* check if the file is already attached */
	col = mvc_bind_column(m, tfiles, "location");
	rid = store->table_api.column_find_row(m->session->tr, col, fname, NULL);
	if (!is_oid_nil(rid))
		return createException(SQL, "netcdf.attach", SQLSTATE(NC000) "File %s is already attached\n", fname);

	/* Open NetCDF file  */
	if ((retval = nc_open(fname, NC_NOWRITE, &ncid)))
		return createException(MAL, "netcdf.test", SQLSTATE(NC000) "Cannot open NetCDF \
file %s: %s", fname, nc_strerror(retval));

	if ((retval = nc_inq(ncid, &ndims, &nvars, &ngatts, &unlimdim)))
		return createException(MAL, "netcdf.test", SQLSTATE(NC000) "Cannot read NetCDF \
header: %s", nc_strerror(retval));

	/* Insert row into netcdf_files table */
	col = mvc_bind_column(m, tfiles, "file_id");
	fid = store->storage_api.count_col(tr, col, 1) + 1;

	esc_str0 = SQLescapeString(fname);
	if (!esc_str0) {
		msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto finish;
	}
	snprintf(buf, sizeof(buf), INSFILE, (int)fid, esc_str0);
	GDKfree(esc_str0);
	if ( ( msg = SQLstatementIntern(cntxt, s, "netcdf.attach", TRUE, FALSE, NULL))
		 != MAL_SUCCEED )
		goto finish;

	/* Read dimensions from NetCDF header and insert a row for each one into netcdf_dims table */

	dims = (char **)GDKzalloc(sizeof(char *) * ndims);
	if (!dims) {
		msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto finish;
	}
	for (didx = 0; didx < ndims; didx++){
		if ((retval = nc_inq_dim(ncid, didx, dname, &dlen)) != 0)
			return createException(MAL, "netcdf.attach", SQLSTATE(NC000) "Cannot read dimension %d : %s", didx, nc_strerror(retval));

		esc_str0 = SQLescapeString(dname);
		if (!esc_str0) {
			msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finish;
		}

		snprintf(buf, sizeof(buf), INSDIM, didx, (int)fid, esc_str0, (int)dlen);
		GDKfree(esc_str0);
		if ( ( msg = SQLstatementIntern(cntxt, s, "netcdf.attach", TRUE, FALSE, NULL))
			 != MAL_SUCCEED )
			goto finish;

		dims[didx] = GDKstrdup(dname);
		if (!dims[didx]) {
			msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finish;
		}
	}

	/* Read variables and attributes from the header and insert rows in netcdf_vars, netcdf_vardims, and netcdf_attrs tables */
	for (vidx = 0; vidx < nvars; vidx++){
		if ( (retval = nc_inq_var(ncid, vidx, vname, &vtype, &vndims, vdims, &vnatts)))
			return createException(MAL, "netcdf.attach",
								   SQLSTATE(NC000) "Cannot read variable %d : %s",
								   vidx, nc_strerror(retval));

		/* Check if this is coordinate variable */
		if ( (vndims == 1) && ( strcmp(vname, dims[vdims[0]]) == 0 ))
			coord_dim_id = vdims[0];
		else coord_dim_id = -1;

		esc_str0 = SQLescapeString(vname);
		if (!esc_str0) {
			msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finish;
		}

		snprintf(buf, sizeof(buf), INSVAR, vidx, (int)fid, esc_str0, prim_type_name(vtype), vndims, coord_dim_id);
		GDKfree(esc_str0);
		if ( ( msg = SQLstatementIntern(cntxt, s, "netcdf.attach", TRUE, FALSE, NULL))
			 != MAL_SUCCEED )
			goto finish;

		if ( coord_dim_id < 0 ){
			for (i = 0; i < vndims; i++){
				snprintf(buf, sizeof(buf), INSVARDIM, vidx, vdims[i], (int)fid, i);
				if ( ( msg = SQLstatementIntern(cntxt, s, "netcdf.attach", TRUE, FALSE, NULL))
					 != MAL_SUCCEED )
					goto finish;
			}
		}

		if ( vnatts > 0 ) { /* fill in netcdf_attrs table */

			for (aidx = 0; aidx < vnatts; aidx++){
				if ((retval = nc_inq_attname(ncid,vidx,aidx,aname)))
					return createException(MAL, "netcdf.attach",
										   SQLSTATE(NC000) "Cannot read attribute %d of variable %d: %s",
										   aidx, vidx, nc_strerror(retval));

				if ((retval = nc_inq_att(ncid,vidx,aname,&atype,&alen)))
					return createException(MAL, "netcdf.attach",
										   SQLSTATE(NC000) "Cannot read attribute %s type and length: %s",
										   aname, nc_strerror(retval));

				esc_str0 = SQLescapeString(vname);
				if (!esc_str0) {
					msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto finish;
				}
				esc_str1 = SQLescapeString(aname);
				if (!esc_str1) {
					GDKfree(esc_str0);
					msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto finish;
				}
				switch ( atype ) {
				case NC_CHAR:
					aval = (char *) GDKzalloc(alen + 1);
					if (!aval) {
						GDKfree(esc_str0);
						GDKfree(esc_str1);
						msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto finish;
					}
					if ((retval = nc_get_att_text(ncid,vidx,aname,aval)))
						return createException(MAL, "netcdf.attach",
											   SQLSTATE(NC000) "Cannot read attribute %s value: %s",
											   aname, nc_strerror(retval));
					fix_quote(aval, alen);
					aval[alen] = '\0';
					snprintf(buf, sizeof(buf), INSATTR, esc_str0, esc_str1, "string", aval, (int)fid, "root");
					GDKfree(aval);
					break;

				case NC_INT:
					if ((retval = nc_get_att_int(ncid,vidx,aname,&avalint)))
						return createException(MAL, "netcdf.attach",
											   SQLSTATE(NC000) "Cannot read attribute %s value: %s",
											   aname, nc_strerror(retval));
					snprintf(abuf,sizeof(abuf),"%d",avalint);
					snprintf(buf, sizeof(buf), INSATTR, esc_str0, esc_str1, prim_type_name(atype), abuf, (int)fid, "root");
					break;

				case NC_FLOAT:
					if ((retval = nc_get_att_float(ncid,vidx,aname,&avalfl)))
						return createException(MAL, "netcdf.attach",
											   SQLSTATE(NC000) "Cannot read attribute %s value: %s",
											   aname, nc_strerror(retval));
					snprintf(abuf,sizeof(abuf),"%7.2f",avalfl);
					snprintf(buf, sizeof(buf), INSATTR, esc_str0, esc_str1, prim_type_name(atype), abuf, (int)fid, "root");
					break;

				case NC_DOUBLE:
					if ((retval = nc_get_att_double(ncid,vidx,aname,&avaldbl)))
						return createException(MAL, "netcdf.attach",
											   SQLSTATE(NC000) "Cannot read attribute %s value: %s",
											   aname, nc_strerror(retval));
					snprintf(abuf,sizeof(abuf),"%7.2e",avaldbl);
					snprintf(buf, sizeof(buf), INSATTR, esc_str0, esc_str1, prim_type_name(atype), abuf, (int)fid, "root");
					break;

				default: continue; /* next attribute */
				}
				GDKfree(esc_str1);
				GDKfree(esc_str0);

				printf("statement: '%s'\n", s);
				if ( ( msg = SQLstatementIntern(cntxt, s, "netcdf.attach", TRUE, FALSE, NULL))
					 != MAL_SUCCEED )
					goto finish;

			} /* attr loop */

		}
	} /* var loop */

	/* Extract global attributes */

	for (aidx = 0; aidx < ngatts; aidx++){
		if ((retval = nc_inq_attname(ncid,NC_GLOBAL,aidx,aname)) != 0)
			return createException(MAL, "netcdf.attach",
								   SQLSTATE(NC000) "Cannot read global attribute %d: %s",
								   aidx, nc_strerror(retval));

		if ((retval = nc_inq_att(ncid,NC_GLOBAL,aname,&atype,&alen)) != 0){
			if (dims != NULL ){
				for (didx = 0; didx < ndims; didx++)
					GDKfree(dims[didx]);
				GDKfree(dims);
			}
			return createException(MAL, "netcdf.attach",
								   SQLSTATE(NC000) "Cannot read global attribute %s type and length: %s",
								   aname, nc_strerror(retval));
		}

		esc_str0 = SQLescapeString(aname);
		if (!esc_str0) {
			msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finish;
		}

		switch ( atype ) {
		case NC_CHAR:
			aval = (char *) GDKzalloc(alen + 1);
			if (!aval) {
				GDKfree(esc_str0);
				msg = createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto finish;
			}
			if ((retval = nc_get_att_text(ncid,NC_GLOBAL,aname,aval))) {
				GDKfree(esc_str0);
				if (dims != NULL ){
					for (didx = 0; didx < ndims; didx++)
						GDKfree(dims[didx]);
					GDKfree(dims);
				}
				return createException(MAL, "netcdf.attach",
									   SQLSTATE(NC000) "Cannot read global attribute %s value: %s",
									   aname, nc_strerror(retval));
			}
			fix_quote(aval, alen);
			aval[alen] = '\0';
			snprintf(buf, sizeof(buf), INSATTR, "GLOBAL", esc_str0, "string", aval, (int)fid, "root");
			GDKfree(aval);
			break;

		case NC_INT:
			if ((retval = nc_get_att_int(ncid,NC_GLOBAL,aname,&avalint))){
				GDKfree(esc_str0);
				if (dims != NULL ){
					for (didx = 0; didx < ndims; didx++)
						GDKfree(dims[didx]);
					GDKfree(dims);
				}
				return createException(MAL, "netcdf.attach",
									   SQLSTATE(NC000) "Cannot read global attribute %s of type %s : %s",
									   aname, prim_type_name(atype), nc_strerror(retval));
			}
			snprintf(abuf,sizeof(abuf),"%d",avalint);
			snprintf(buf, sizeof(buf), INSATTR, "GLOBAL", esc_str0, prim_type_name(atype), abuf, (int)fid, "root");
			break;

		case NC_FLOAT:
			if ((retval = nc_get_att_float(ncid,NC_GLOBAL,aname,&avalfl))){
				GDKfree(esc_str0);
				if (dims != NULL ){
					for (didx = 0; didx < ndims; didx++)
						GDKfree(dims[didx]);
					GDKfree(dims);
				}
				return createException(MAL, "netcdf.attach",
									   SQLSTATE(NC000) "Cannot read global attribute %s of type %s: %s",
									   aname, prim_type_name(atype), nc_strerror(retval));
			}
			snprintf(abuf,sizeof(abuf),"%7.2f",avalfl);
			snprintf(buf, sizeof(buf), INSATTR, "GLOBAL", esc_str0, prim_type_name(atype), abuf, (int)fid, "root");
			break;

		case NC_DOUBLE:
			if ((retval = nc_get_att_double(ncid,NC_GLOBAL,aname,&avaldbl))){
				GDKfree(esc_str0);
				if (dims != NULL ){
					for (didx = 0; didx < ndims; didx++)
						GDKfree(dims[didx]);
					GDKfree(dims);
				}
				return createException(MAL, "netcdf.attach",
									   SQLSTATE(NC000) "Cannot read global attribute %s value: %s",
									   aname, nc_strerror(retval));
			}
			snprintf(abuf,sizeof(abuf),"%7.2e",avaldbl);
			snprintf(buf, sizeof(buf), INSATTR, "GLOBAL", esc_str0, prim_type_name(atype), abuf, (int)fid, "root");
			break;

		default: continue; /* next attribute */
		}
		GDKfree(esc_str0);

		printf("global: '%s'\n", s);
		if ( ( msg = SQLstatementIntern(cntxt, s, "netcdf.attach", TRUE, FALSE, NULL))
			 != MAL_SUCCEED )
			goto finish;

	} /* global attr loop */


  finish:
	nc_close(ncid);

	if (dims != NULL ){
		for (didx = 0; didx < ndims; didx++)
			GDKfree(dims[didx]);
		GDKfree(dims);
	}

	return msg;
}


/* Compose create table statement to create table representing NetCDF variable in the
 * database. Used for testing, can be removed from release. */
str
NCDFimportVarStmt(Client ctx, str *sciqlstmt, str *fname, int *varid)
{
	(void) ctx;
	int ncid;   /* dataset id */
	int vndims, vnatts, i, j, retval;
	int vdims[NC_MAX_VAR_DIMS];

	size_t dlen;
	char dname[NC_MAX_NAME+1], vname[NC_MAX_NAME+1];
	nc_type vtype; /* == int */

	char buf[BUFSIZ];
	str msg = MAL_SUCCEED;

	/* Open NetCDF file  */
	if ((retval = nc_open(*fname, NC_NOWRITE, &ncid)))
		return createException(MAL, "netcdf.importvar",
							   SQLSTATE(NC000) "Cannot open NetCDF file %s: %s", *fname, nc_strerror(retval));

	if ( (retval = nc_inq_var(ncid, *varid, vname, &vtype, &vndims, vdims, &vnatts)))
		return createException(MAL, "netcdf.attach",
							   SQLSTATE(NC000) "Cannot read variable %d : %s", *varid, nc_strerror(retval));


	j = snprintf(buf, sizeof(buf),"create table %s( ", vname);

	for (i = 0; i < vndims; i++){
		if ((retval = nc_inq_dim(ncid, vdims[i], dname, &dlen)))
			return createException(MAL, "netcdf.attach",
								   SQLSTATE(NC000) "Cannot read dimension %d : %s", vdims[i], nc_strerror(retval));

		(void)dlen;
		j += snprintf(buf + j, BUFSIZ - j, "%s INTEGER, ", dname);

	}

	j += snprintf(buf + j, BUFSIZ - j, "value %s);", NCDF2SQL(vtype));

	nc_close(ncid);

	*sciqlstmt = GDKstrdup(buf);
	if(*sciqlstmt == NULL)
		return createException(MAL, "netcdf.importvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* Load variable varid from data set ncid into the bat v. Generate dimension
 * bats dim using NCDFARRAYseries */
static str
NCDFloadVar(bat **dim, bat *v, int ncid, int varid, nc_type vtype, int vndims, int *vdims)
{

	BAT *res;
	bat vbid, *dim_bids;
	int retval, i, j;
	char *sermsg = NULL;
	size_t sz = 1;
	size_t *dlen = NULL, *val_rep = NULL, *grp_rep = NULL;

	if ( dim == NULL )
		return createException(MAL, "netcdf.importvar", SQLSTATE(NC000) "array of dimension bat is NULL");
	dim_bids = *dim;

	dlen = (size_t *)GDKzalloc(sizeof(size_t) * vndims);
	if (!dlen)
		return createException(MAL, "netcdf.attach", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < vndims; i++){
		if ((retval = nc_inq_dimlen(ncid, vdims[i], &dlen[i])))
			return createException(MAL, "netcdf.importvar",
								   SQLSTATE(NC000) "Cannot read dimension %d : %s",
								   vdims[i], nc_strerror(retval));
		sz *= dlen[i];
	}

	switch (vtype) {
	case NC_INT:
	{
		LOAD_NCDF_VAR(int,int);
		break;
	}
	case NC_FLOAT:
	case NC_DOUBLE:
	{
		LOAD_NCDF_VAR(dbl,double);
		break;
	}

	default:
		GDKfree(dlen);
		return createException(MAL, "netcdf.importvar",
							   SQLSTATE(NC000) "Type %s not supported yet",
							   prim_type_name(vtype));

	}

	BATsetcount(res, sz);
	res->tnonil = true;
	res->tnil = false;
	res->tsorted = false;
	res->trevsorted = false;
	BATkey(res, false);
	vbid = res->batCacheid;
	BBPkeepref(res);

	res = NULL;

	/* Manually create dimensions with range [0:1:dlen[i]] */
	val_rep = (size_t *)GDKmalloc(sizeof(size_t) * vndims);
	grp_rep = (size_t *)GDKmalloc(sizeof(size_t) * vndims);
	if (val_rep == NULL || grp_rep == NULL) {
		GDKfree(dlen);
		GDKfree(val_rep);
		GDKfree(grp_rep);
		throw(MAL, "netcdf.loadvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* compute the repetition factor inside of the series (val_rep) and of series (grp_rep) */
	for (i = 0; i < vndims; i++) {
		val_rep[i] = grp_rep[i] = 1;
		for (j = 0; j < i; j++)
			grp_rep[i] *= dlen[j];
		for (j = i + 1; j < vndims; j++)
			val_rep[i] *= dlen[j];
	}

	for (i = 0; i < vndims; i++) {
		sermsg = NCDFARRAYseries(&dim_bids[i], 0, 1, dlen[i], val_rep[i], grp_rep[i]);

		if (sermsg != MAL_SUCCEED) {
			BBPrelease(vbid); /* undo the BBPkeepref(res) above */
			for ( j = 0; j < i; j++) /* undo log. ref of previous dimensions */
				BBPrelease(dim_bids[j]);
			GDKfree(dlen);
			GDKfree(val_rep);
			GDKfree(grp_rep);
			return sermsg;
		}
	}
	/* to do : is descriptor check of dim_bids is needed? */

	GDKfree(dlen);
	GDKfree(val_rep);
	GDKfree(grp_rep);

	*v = vbid;

	return MAL_SUCCEED;
}

/* import variable given file id and variable name */
str
NCDFimportVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *sch = NULL;
	sql_table *tfiles = NULL, *arr_table = NULL;
	sql_column *col;

	str msg = MAL_SUCCEED, vname = *getArgReference_str(stk, pci, 2);
	str fname = NULL, dimtype = NULL, aname_sys = NULL;
	int fid = *getArgReference_int(stk, pci, 1);
	int varid, vndims, vnatts, i, j, retval;
	char buf[BUFSIZ], *s= buf, aname[256], **dname;
	oid rid = oid_nil;
	int vdims[NC_MAX_VAR_DIMS];
	nc_type vtype;
	int ncid;   /* dataset id */
	size_t dlen;
	bat vbatid = 0, *dim_bids;
	BAT *vbat = NULL, *dimbat;

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if (msg)
		return msg;

	sqlstore *store = m->session->tr->store;
	sch = mvc_bind_schema(m, "sys");
	if ( !sch )
		return createException(MAL, "netcdf.importvar", SQLSTATE(NC000) "Cannot get schema sys\n");

	tfiles = mvc_bind_table(m, sch, "netcdf_files");
	if (tfiles == NULL)
		return createException(MAL, "netcdf.importvar", SQLSTATE(NC000) "Catalog table missing\n");

	/* get the name of the attached NetCDF file */
	col = mvc_bind_column(m, tfiles, "file_id");
	if (col == NULL)
		return createException(MAL, "netcdf.importvar", SQLSTATE(NC000) "Could not find \"netcdf_files\".\"file_id\"\n");
	rid = store->table_api.column_find_row(m->session->tr, col, (void *)&fid, NULL);
	if (is_oid_nil(rid))
		return createException(MAL, "netcdf.importvar", SQLSTATE(NC000) "File %d not in the NetCDF vault\n", fid);


	col = mvc_bind_column(m, tfiles, "location");
	fname = (str)store->table_api.column_find_value(m->session->tr, col, rid);

	/* Open NetCDF file  */
	if ((retval = nc_open(fname, NC_NOWRITE, &ncid))) {
		char *msg = createException(MAL, "netcdf.importvar", SQLSTATE(NC000) "Cannot open NetCDF file %s: %s",
									fname, nc_strerror(retval));
		GDKfree(fname);
		return msg;
	}
	GDKfree(fname);

	/* Get info for variable vname from NetCDF file */
	if ( (retval = nc_inq_varid(ncid, vname, &varid)) ) {
		nc_close(ncid);
		return createException(MAL, "netcdf.importvar",
							   SQLSTATE(NC000) "Cannot read variable %s: %s",
							   vname, nc_strerror(retval));
	}
	if ( (retval = nc_inq_var(ncid, varid, vname, &vtype, &vndims, vdims, &vnatts))) {
		nc_close(ncid);
		return createException(MAL, "netcdf.importvar",
							   SQLSTATE(NC000) "Cannot read variable %d : %s",
							   varid, nc_strerror(retval));
	}

	/* compose 'create table' statement in the buffer */
	dname = (char **) GDKzalloc( sizeof(char *) * vndims);
	if (dname == NULL) {
		nc_close(ncid);
		throw(MAL, "netcdf.importvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	for (i = 0; i < vndims; i++) {
		dname[i] = (char *) GDKzalloc(NC_MAX_NAME + 1);
		if(!dname[i]) {
			for (j = 0; j < i; j++)
				GDKfree(dname[j]);
			GDKfree(dname);
			nc_close(ncid);
			throw(MAL, "netcdf.importvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}

	snprintf(aname, sizeof(aname), "%s%d", vname, fid);

	j = snprintf(buf, sizeof(buf),"create table %s.%s( ", sch->base.name, aname);

	for (i = 0; i < vndims; i++){
		if ((retval = nc_inq_dim(ncid, vdims[i], dname[i], &dlen))) {
			for (j = 0; j < vndims; j++)
				GDKfree(dname[j]);
			GDKfree(dname);
			nc_close(ncid);
			return createException(MAL, "netcdf.importvar",
								   SQLSTATE(NC000) "Cannot read dimension %d : %s",
								   vdims[i], nc_strerror(retval));
		}

		if ( dlen <= (int) GDK_bte_max )
			dimtype = "TINYINT";
		else if ( dlen <= (int) GDK_sht_max )
			dimtype = "SMALLINT";
		else
			dimtype = "INT";

		(void)dlen;
		j += snprintf(buf + j, BUFSIZ - j, "%s %s, ", dname[i], dimtype);
	}

	j += snprintf(buf + j, BUFSIZ - j, "value %s);", NCDF2SQL(vtype));

/* execute 'create table ' */
	msg = SQLstatementIntern(cntxt, s, "netcdf.importvar", TRUE, FALSE, NULL);
	if (msg != MAL_SUCCEED){
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		nc_close(ncid);
		return msg;
	}

/* load variable data */
	dim_bids = (bat *)GDKmalloc(sizeof(bat) * vndims);
	if (dim_bids == NULL){
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		nc_close(ncid);
		throw(MAL, "netcdf.importvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	msg = NCDFloadVar(&dim_bids, &vbatid, ncid, varid, vtype, vndims, vdims);
	if ( msg != MAL_SUCCEED ) {
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		GDKfree(dim_bids);
		nc_close(ncid);
		return msg;
	}

	/* associate columns in the table with loaded variable data */
	aname_sys = toLower(aname);
	arr_table = mvc_bind_table(m, sch, aname_sys);
	if (arr_table == NULL){
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		GDKfree(dim_bids);
		nc_close(ncid);
		throw(MAL, "netcdf.importvar", SQLSTATE(NC000) "netcdf table %s missing\n", aname_sys);
	}

	col = mvc_bind_column(m, arr_table, "value");
	if (col == NULL){
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		GDKfree(dim_bids);
		nc_close(ncid);
		throw(MAL, "netcdf.importvar", SQLSTATE(NC000) "Cannot find column %s.value\n", aname_sys);
	}

	vbat = BATdescriptor(vbatid);
	if(vbat == NULL) {
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		GDKfree(dim_bids);
		nc_close(ncid);
		throw(MAL, "netcdf.importvar", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	BUN offset;
	BAT *pos = NULL;
	if (store->storage_api.claim_tab(m->session->tr, arr_table, BATcount(vbat), &offset, &pos) != LOG_OK) {
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		GDKfree(dim_bids);
		BBPunfix(vbatid);
		BBPrelease(vbatid);
		nc_close(ncid);
		throw(MAL, "netcdf.importvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (!isNew(arr_table) && sql_trans_add_dependency_change(m->session->tr, arr_table->base.id, dml) != LOG_OK) {
		for (i = 0; i < vndims; i++)
			GDKfree(dname[i]);
		GDKfree(dname);
		GDKfree(dim_bids);
		BBPunfix(vbatid);
		BBPrelease(vbatid);
		bat_destroy(pos);
		nc_close(ncid);
		throw(MAL, "netcdf.importvar", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	store->storage_api.append_col(m->session->tr, col, offset, pos, vbat, BATcount(vbat), true, vbat->ttype);
	BBPunfix(vbatid);
	BBPrelease(vbatid);
	vbat = NULL;

	/* associate dimension bats  */
	for (i = 0; i < vndims; i++){
		col = mvc_bind_column(m, arr_table, dname[i]);
		if (col == NULL){
			msg = createException(MAL, "netcdf.importvar", SQLSTATE(NC000) "Cannot find column %s.%s\n", aname_sys, dname[i]);
			for (i = 0; i < vndims; i++)
				GDKfree(dname[i]);
			GDKfree(dname);
			GDKfree(dim_bids);
			bat_destroy(pos);
			nc_close(ncid);
			return msg;
		}

		dimbat = BATdescriptor(dim_bids[i]);
		if(dimbat == NULL) {
			for (i = 0; i < vndims; i++)
				GDKfree(dname[i]);
			GDKfree(dname);
			GDKfree(dim_bids);
			bat_destroy(pos);
			nc_close(ncid);
			throw(MAL, "netcdf.importvar", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		store->storage_api.append_col(m->session->tr, col, offset, pos, dimbat, BATcount(dimbat), true, dimbat->ttype);
		BBPunfix(dim_bids[i]); /* phys. ref from BATdescriptor */
		BBPrelease(dim_bids[i]); /* log. ref. from loadVar */
		dimbat = NULL;
	}

	for (i = 0; i < vndims; i++)
		GDKfree(dname[i]);
	GDKfree(dname);
	GDKfree(dim_bids);
	bat_destroy(pos);
	nc_close(ncid);
	return msg;
}

static str
HDF5dataset(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) ctx;
	(void) mb;
	char *msg = MAL_SUCCEED;
	int ncid, varid, retval;
    int ndims;
	int dimids[NC_MAX_VAR_DIMS];

	const char* fname = *getArgReference_str(stk, pci, pci->retc);
	const char* dataset = *getArgReference_str(stk, pci, pci->retc + 1);
	sql_subtype *st = *getArgReference_ptr(stk, pci, pci->retc + 2);
	(void) st;
	allocator *ta = MT_thread_getallocator();

    // Open the file
    // NetCDF-4 handles the HDF5 format automatically
    if ((retval = nc_open(fname, NC_NOWRITE, &ncid)))
		throw(MAL, "netcdf.HDF5dataset",
			   	SQLSTATE(NC000) "Cannot open NetCDF file %s: %s", fname, nc_strerror(retval));

    // Find the ID for the dataset
    if ((retval = nc_inq_varid(ncid, dataset, &varid))) {
		nc_close(ncid);
		throw(MAL, "netcdf.HDF5dataset",
			SQLSTATE(NC000) "Cannot find dataset %s: %s",
							   dataset, nc_strerror(retval));
	}

    // Get dimension information to confirm sizes
    size_t rows, cols;
    if ((retval = nc_inq_varndims(ncid, varid, &ndims))) {
		nc_close(ncid);
		throw(MAL, "netcdf.HDF5dataset",
			SQLSTATE(NC000) "Cannot read number of dimmensions %d: %s",
							   varid, nc_strerror(retval));
	}
	assert(ndims == 2);

    if ((retval = nc_inq_vardimid(ncid, varid, dimids))) {
		nc_close(ncid);
		throw(MAL, "netcdf.HDF5dataset",
			SQLSTATE(NC000) "Cannot read dataset %s: %s",
							   dataset, nc_strerror(retval));
	}

    nc_inq_dimlen(ncid, dimids[0], &rows);
    nc_inq_dimlen(ncid, dimids[1], &cols);

	assert(cols == (size_t)pci->retc);
    //printf("Dataset 'train' has dimensions: %zu x %zu\n", rows, cols);

	allocator_state ta_state = ma_open(ta);
    // Allocate memory buffer for the data
    float *buffer = (float *)ma_alloc(ta, rows * cols * sizeof(float));
    if (buffer == NULL) {
		nc_close(ncid);
		ma_close(&ta_state);
		throw(MAL, "netcdf.HDF5dataset", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

    // Read the entire dataset into the buffer
    if ((retval = nc_get_var_float(ncid, varid, buffer))) {
		nc_close(ncid);
		ma_close(&ta_state);
		throw(MAL, "netcdf.HDF5dataset", SQLSTATE(NC000) "Cannot read data");
	}
    nc_close(ncid);
    //printf("First element of train[0][0]: %f\n", buffer[0]);

	BAT **bats = (BAT**)ma_zalloc(ta, sizeof(BAT*) * pci->retc);
	if (!bats) {
		ma_close(&ta_state);
		throw(MAL, "netcdf.HDF5dataset", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for(int i = 0; i < pci->retc; i++) {
		bats[i] = COLnew(0, getBatType(getArgType(mb, pci, i)), 10, TRANSIENT);
		if (!bats[i]) {
			msg = createException(MAL, "netcdf.HDF5dataset", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}
	// loop over load data
	for (size_t i=0; i<rows*cols; i++) {
		double v = buffer[i];
		size_t j = i%cols;
		if ((BUNappend(bats[j], &v, false) != GDK_SUCCEED)) {
			msg = createException(MAL, "netcdf.HDF5dataset", "Error appending value %f", v);
			goto bailout;
		}
	}

	for(int i = 0; i < pci->retc && bats[i]; i++) {
		*getArgReference_bat(stk, pci, i) = bats[i]->batCacheid;
		BBPkeepref(bats[i]);
	}
	ma_close(&ta_state);
	return msg;
bailout:
	for(int i = 0; i < pci->retc; i++)
		if (bats[i])
			BBPreclaim(bats[i]);
	ma_close(&ta_state);
	return msg;
}

static str
hdf5_relation(mvc *sql, sql_subfunc *f, char *fname, list *res_exps, char *tname, lng *est)
{
	#define ERR(e) {printf("Error: %s\n", nc_strerror(e)); exit(1);}
	(void) est;

    int ncid, varid, ndims, type;
    int dimids[NC_MAX_VAR_DIMS];
    size_t dim_lens[NC_MAX_VAR_DIMS];
    //size_t type_size;

    // Open the file (Read-Only)
    int retval = nc_open(fname, NC_NOWRITE, &ncid);
    if (retval)
		throw(MAL, "netcdf.hdf5_relation", SQLSTATE(NC000) "Cannot open HDF5 file %s: %s", fname, nc_strerror(retval));

    // Get the ID for the "train" variable hardcoding for now
	const char *vname = "train";
    if ((retval = nc_inq_varid(ncid, vname, &varid))) {
		nc_close(ncid);
		throw(MAL, "netcdf.hdf5_relation",
							   SQLSTATE(NC000) "Cannot read variable %s: %s",
							   vname, nc_strerror(retval));
	}

    // Get the type and number of dimensions
    if ((retval = nc_inq_var(ncid, varid, NULL, &type, &ndims, dimids, NULL))) {
		nc_close(ncid);
		throw(MAL, "netcdf.hdf5_relation",
				SQLSTATE(NC000) "Cannot read variable %d : %s", varid, nc_strerror(retval));
	}

	// 2 dim vectors only
	assert(ndims==2);
	assert(type == NC_FLOAT || type == NC_DOUBLE);

    // Get the size of each dimension
    //printf("Variable 'train' has %d dimensions:\n", ndims);
    for (int i = 0; i < ndims; i++) {
        if ((retval = nc_inq_dimlen(ncid, dimids[i], &dim_lens[i]))) {
		nc_close(ncid);
		throw(MAL, "netcdf.hdf5_relation",
				SQLSTATE(NC000) "Cannot read dim len %d : %s", dimids[i], nc_strerror(retval));

		}
        //printf("  Dimension %d size: %zu\n", i, dim_lens[i]);
    }
	//const size_t rows = dim_lens[0];
	const size_t cols = dim_lens[1];
    nc_close(ncid);

	list *types = sa_list(sql->sa);
	list *names = sa_list(sql->sa);
	list_append(names, ma_strdup(sql->sa, vname));
	// FIX for actual type in dataset
	sql_subtype *st = SA_ZNEW(sql->sa, sql_subtype);
	*st = *sql_fetch_localtype(TYPE_dbl);
	st->digits = cols;
	st->multiset = MS_VECTOR;
	list_append(types, st);
	sql_alias *atname = a_create(sql->sa, tname);
	sql_exp *e = exp_column(sql->sa, atname, vname, st, CARD_MULTI, 1, 0, 0);
	e->alias.label = -(sql->nid++);
	e->f = sa_list(sql->sa);
	for(size_t i=0; i < cols; i++) {
		char *buf = ma_alloc(sql->sa, sizeof(char)*32);
        snprintf(buf, 32, "%s.%zu", vname, i);
		sql_exp *ne = exp_alias(sql, atname, buf, atname, buf, st, CARD_MULTI, 0, 0, 0);
		list_append(e->f, ne);
	}
	set_basecol(e);
	list_append(res_exps, e);
	f->tname = tname;
	f->res = types;
	f->coltypes = types;
	f->colnames = names;
    return MAL_SUCCEED;
}

static void *
hdf5_load(void *BE, sql_subfunc *f, char *filename, sql_exp *topn)
{
	(void) topn;
	backend *be = BE;
	allocator *sa = be->mvc->sa;
	sql_subtype *st = f->res->h->data;
	size_t ncols = st->digits;
	const char *dataset = "train"; // FIX hardcoded
	const char *cname = f->colnames->h->data;

	InstrPtr q = newStmtArgs(be->mb, "netcdf", "hdf5dataset", ncols + 3);
	setVarType(be->mb, getArg(q, 0), newBatType(st->type->localtype));
	for (size_t i = 1; i<ncols; i++)
		q = pushReturn(be->mb, q, newTmpVariable(be->mb, newBatType(st->type->localtype)));

	q = pushStr(be->mb, q, filename);
	q = pushStr(be->mb, q, dataset);
	q = pushPtr(be->mb, q, st);
	pushInstruction(be->mb, q);

	list *r = sa_list(sa);
	for(size_t i = 0; i < ncols; i++) {
		stmt *br = stmt_blackbox_result(be, q, i, sql_fetch_localtype(TYPE_dbl));
		stmt *s = stmt_alias(be, br, -(be->mvc->nid++), a_create(sa, f->tname), cname);
		append(r, s);
	}
	stmt *s = stmt_list(be, r);

	//stmt* s = stmt_none(be);
	//s->q = q;
	//s->nr = getDestVar(q);
	s->subtype = *st;
	s->nested = true;
	//s->multiset = st->multiset;
	//s = stmt_alias(be, s, 1, a_create(sa, f->tname), dataset);
	list *l = sa_list(be->mvc->sa);
	append(l,s);
	s = stmt_list(be, l);
	return s;
}

static str
NETCDFprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;

	fl_register("hdf5", &hdf5_relation, &hdf5_load);
	return MAL_SUCCEED;
}

static str
NETCDFepilogue(void *ret)
{
	(void)ret;
	fl_unregister("hdf5");
	return MAL_SUCCEED;
}


#include "mel.h"
static mel_func netcdf_init_funcs[] = {
	pattern("netcdf", "prelude", NETCDFprelude, false, "", noargs),
	command("netcdf", "epilogue", NETCDFepilogue, false, "", noargs),
	command("netcdf", "test", NCDFtest, false, "Returns number of variables in a given NetCDF dataset (file)", args(1,2, arg("",int),arg("filename",str))),
	pattern("netcdf", "attach", NCDFattach, true, "Register a NetCDF file in the vault", args(1,2, arg("",void),arg("filename",str))),
	command("netcdf", "importvar", NCDFimportVarStmt, true, "Import variable: compose create array string", args(1,3, arg("",str),arg("filename",str),arg("varid",int))),
	pattern("netcdf", "importvariable", NCDFimportVariable, true, "Import variable: create array and load data from variable varname of file fileid", args(1,3, arg("",void),arg("fileid",int),arg("varname",str))),
	pattern("netcdf", "hdf5dataset", HDF5dataset, true, "Load dataset from hdf5", args(1,4, batvararg("",any),arg("filename",str),arg("dataset",str), arg("type", ptr))),
	{ .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_netcdf_mal)
{ mal_module("netcdf", NULL, netcdf_init_funcs); }
