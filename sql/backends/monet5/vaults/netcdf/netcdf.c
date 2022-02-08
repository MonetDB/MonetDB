/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <netcdf.h>
#include "sql_mvc.h"
#include "sql.h"
#include "sql_execute.h"
#include "sql_scenario.h"
#include "mal_exception.h"
#include "netcdf_vault.h"

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
NCDFtest(int *vars, str *fname)
{
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

#define array_series(sta, ste, sto, TYPE) { 				\
	int s,g;							\
	TYPE i, *o = (TYPE*)Tloc(bn, 0);				\
	TYPE start = sta, step = ste, stop = sto; 			\
	if ( start < stop && step > 0) {				\
		for ( s = 0; s < series; s++)				\
			for ( i = start; i < stop; i += step)		\
				for( g = 0; g < group; g++){		\
					*o = i;				\
					o++;				\
				}					\
	} else {							\
		for ( s = 0; s < series; s++)				\
			for ( i = start; i > stop; i += step)		\
				for( g = 0; g < group; g++){		\
					*o = i;				\
					o++;				\
				}					\
	}								\
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
	bn->tnonil = true;
	BBPkeepref(*bid= bn->batCacheid);
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
	snprintf(buf, BUFSIZ, INSFILE, (int)fid, esc_str0);
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

		snprintf(buf, BUFSIZ, INSDIM, didx, (int)fid, esc_str0, (int)dlen);
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

		snprintf(buf, BUFSIZ, INSVAR, vidx, (int)fid, esc_str0, prim_type_name(vtype), vndims, coord_dim_id);
		GDKfree(esc_str0);
	    if ( ( msg = SQLstatementIntern(cntxt, s, "netcdf.attach", TRUE, FALSE, NULL))
			 != MAL_SUCCEED )
            goto finish;

	    if ( coord_dim_id < 0 ){
	        for (i = 0; i < vndims; i++){
                snprintf(buf, BUFSIZ, INSVARDIM, vidx, vdims[i], (int)fid, i);
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
					snprintf(buf, BUFSIZ, INSATTR, esc_str0, esc_str1, "string", aval, (int)fid, "root");
					GDKfree(aval);
					break;

				case NC_INT:
					if ((retval = nc_get_att_int(ncid,vidx,aname,&avalint)))
						return createException(MAL, "netcdf.attach",
											   SQLSTATE(NC000) "Cannot read attribute %s value: %s",
											   aname, nc_strerror(retval));
					snprintf(abuf,80,"%d",avalint);
					snprintf(buf, BUFSIZ, INSATTR, esc_str0, esc_str1, prim_type_name(atype), abuf, (int)fid, "root");
					break;

				case NC_FLOAT:
					if ((retval = nc_get_att_float(ncid,vidx,aname,&avalfl)))
						return createException(MAL, "netcdf.attach",
											   SQLSTATE(NC000) "Cannot read attribute %s value: %s",
											   aname, nc_strerror(retval));
					snprintf(abuf,80,"%7.2f",avalfl);
					snprintf(buf, BUFSIZ, INSATTR, esc_str0, esc_str1, prim_type_name(atype), abuf, (int)fid, "root");
					break;

				case NC_DOUBLE:
					if ((retval = nc_get_att_double(ncid,vidx,aname,&avaldbl)))
						return createException(MAL, "netcdf.attach",
											   SQLSTATE(NC000) "Cannot read attribute %s value: %s",
											   aname, nc_strerror(retval));
					snprintf(abuf,80,"%7.2e",avaldbl);
					snprintf(buf, BUFSIZ, INSATTR, esc_str0, esc_str1, prim_type_name(atype), abuf, (int)fid, "root");
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
			snprintf(buf, BUFSIZ, INSATTR, "GLOBAL", esc_str0, "string", aval, (int)fid, "root");
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
			snprintf(abuf,80,"%d",avalint);
			snprintf(buf, BUFSIZ, INSATTR, "GLOBAL", esc_str0, prim_type_name(atype), abuf, (int)fid, "root");
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
			snprintf(abuf,80,"%7.2f",avalfl);
			snprintf(buf, BUFSIZ, INSATTR, "GLOBAL", esc_str0, prim_type_name(atype), abuf, (int)fid, "root");
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
			snprintf(abuf,80,"%7.2e",avaldbl);
			snprintf(buf, BUFSIZ, INSATTR, "GLOBAL", esc_str0, prim_type_name(atype), abuf, (int)fid, "root");
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
NCDFimportVarStmt(str *sciqlstmt, str *fname, int *varid)
{
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


	j = snprintf(buf, BUFSIZ,"create table %s( ", vname);

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
	BBPkeepref(vbid = res->batCacheid);

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
			BBPrelease(vbid); /* undo the BBPkeepref(vbid) above */
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

	snprintf(aname, 256, "%s%d", vname, fid);

	j = snprintf(buf, BUFSIZ,"create table %s.%s( ", sch->base.name, aname);

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
	store->storage_api.append_col(m->session->tr, col, offset, pos, vbat, BATcount(vbat), TYPE_bat);
	BBPunfix(vbatid);
	BBPrelease(vbatid);
	vbat = NULL;

	/* associate dimension bats  */
	for (i = 0; i < vndims; i++){
		col = mvc_bind_column(m, arr_table, dname[i]);
		if (col == NULL){
			for (i = 0; i < vndims; i++)
				GDKfree(dname[i]);
			GDKfree(dname);
			GDKfree(dim_bids);
			bat_destroy(pos);
			nc_close(ncid);
			throw(MAL, "netcdf.importvar", SQLSTATE(NC000) "Cannot find column %s.%s\n", aname_sys, dname[i]);
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
		store->storage_api.append_col(m->session->tr, col, offset, pos, dimbat, BATcount(dimbat), TYPE_bat);
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

#include "mel.h"
static mel_func netcdf_init_funcs[] = {
 command("netcdf", "test", NCDFtest, false, "Returns number of variables in a given NetCDF dataset (file)", args(1,2, arg("",int),arg("filename",str))),
 pattern("netcdf", "attach", NCDFattach, true, "Register a NetCDF file in the vault", args(1,2, arg("",void),arg("filename",str))),
 command("netcdf", "importvar", NCDFimportVarStmt, true, "Import variable: compose create array string", args(1,3, arg("",str),arg("filename",str),arg("varid",int))),
 pattern("netcdf", "importvariable", NCDFimportVariable, true, "Import variable: create array and load data from variable varname of file fileid", args(1,3, arg("",void),arg("fileid",int),arg("varname",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_netcdf_mal)
{ mal_module("netcdf", NULL, netcdf_init_funcs); }
