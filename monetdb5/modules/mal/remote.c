/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Fabian Groffen, Martin Kersten
 * Remote querying functionality
 * Communication with other mservers at the MAL level is a delicate task.
 * However, it is indispensable for any distributed functionality.  This
 * module provides an abstract way to store and retrieve objects on a
 * remote site.  Additionally, functions on a remote site can be executed
 * using objects available in the remote session context.  This yields in
 * four primitive functions that form the basis for distribution methods:
 * get, put, register and exec.
 *
 * The get method simply retrieves a copy of a remote object.  Objects can
 * be simple values, strings or Column.  The same holds for the put method,
 * but the other way around.  A local object can be stored on a remote
 * site.  Upon a successful store, the put method returns the remote
 * identifier for the stored object.  With this identifier the object can
 * be addressed, e.g. using the get method to retrieve the object that was
 * stored using put.
 *
 * The get and put methods are symmetric.  Performing a get on an
 * identifier that was returned by put, results in an object with the same
 * value and type as the one that was put.  The result of such an operation is
 * equivalent to making an (expensive) copy of the original object.
 *
 * The register function takes a local MAL function and makes it known at a
 * remote site. It ensures that it does not overload an already known
 * operation remotely, which could create a semantic conflict.
 * Deregistering a function is forbidden, because it would allow for taking
 * over the remote site completely.
 * C-implemented functions, such as io.print() cannot be remotely stored.
 * It would require even more complicated (byte) code shipping and remote
 * compilation to make it work.
 *
 * The choice to let exec only execute functions avoids problems
 * to decide what should be returned to the caller.  With a function it is
 * clear and simple to return that what the function signature prescribes.
 * Any side effect (e.g. io.print calls) may cause havoc in the system,
 * but are currently ignored.
 *
 * This leads to the final contract of this module.  The methods should be
 * used correctly, by obeying their contract.  Failing to do so will result
 * in errors and possibly undefined behaviour.
 *
 * The resolve() function can be used to query Merovingian.  It returns one
 * or more databases discovered in its vicinity matching the given pattern.
 *
 */
#include "monetdb_config.h"
#include "remote.h"

/*
 * Technically, these methods need to be serialised per connection,
 * hence a scheduler that interleaves e.g. multiple get calls, simply
 * violates this constraint.  If parallelism to the same site is
 * desired, a user could create a second connection.  This is not always
 * easy to generate at the proper place, e.g. overloading the dataflow
 * optimizer to patch connections structures is not acceptable.
 *
 * Instead, we maintain a simple lock with each connection, which can be
 * used to issue a safe, but blocking get/put/exec/register request.
 */
#ifdef HAVE_MAPI

static connection conns = NULL;
static unsigned char localtype = 0177;

static inline str RMTquery(MapiHdl *ret, str func, Mapi conn, str query);
static inline str RMTinternalcopyfrom(BAT **ret, char *hdr, stream *in);

/**
 * Returns a BAT with valid redirects for the given pattern.  If
 * merovingian is not running, this function throws an error.
 */
str RMTresolve(bat *ret, str *pat) {
#ifdef WIN32
	throw(MAL, "remote.resolve", "merovingian is not available on "
			"your platform, sorry"); /* please upgrade to Linux, etc. */
#else
	BAT *list;
	char *mero_uri;
	char *p;
	unsigned int port;
	char **redirs;
	char **or;

	if (pat == NULL || *pat == NULL || strcmp(*pat, (str)str_nil) == 0)
		throw(ILLARG, "remote.resolve",
				ILLEGAL_ARGUMENT ": pattern is NULL or nil");

	mero_uri = GDKgetenv("merovingian_uri");
	if (mero_uri == NULL)
		throw(MAL, "remote.resolve", "this function needs the mserver "
				"have been started by merovingian");

	list = COLnew(0, TYPE_str, 0, TRANSIENT);
	if (list == NULL)
		throw(MAL, "remote.resolve", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	/* extract port from mero_uri, let mapi figure out the rest */
	mero_uri+=strlen("mapi:monetdb://");
	if ((p = strchr(mero_uri, ':')) == NULL)
		throw(MAL, "remote.resolve", "illegal merovingian_uri setting: %s",
				GDKgetenv("merovingian_uri"));
	port = (unsigned int)atoi(p + 1);

	or = redirs = mapi_resolve(NULL, port, *pat);

	if (redirs == NULL)
		throw(MAL, "remote.resolve", "unknown failure when resolving pattern");

	while (*redirs != NULL) {
		if (BUNappend(list, (ptr)*redirs, FALSE) != GDK_SUCCEED) {
			BBPreclaim(list);
			do
				free(*redirs);
			while (*++redirs);
			free(or);
			throw(MAL, "remote.resolve", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		free(*redirs);
		redirs++;
	}
	free(or);

	BBPkeepref(*ret = list->batCacheid);
	return(MAL_SUCCEED);
#endif
}


/* for unique connection identifiers */
static size_t connection_id = 0;

/**
 * Returns a connection to the given uri.  It always returns a newly
 * created connection.
 */
str RMTconnectScen(
		str *ret,
		str *ouri,
		str *user,
		str *passwd,
		str *scen)
{
	connection c;
	char conn[BUFSIZ];
	char *s;
	Mapi m;
	MapiHdl hdl;
	str msg;

	/* just make sure the return isn't garbage */
	*ret = 0;

	if (ouri == NULL || *ouri == NULL || strcmp(*ouri, (str)str_nil) == 0)
		throw(ILLARG, "remote.connect", ILLEGAL_ARGUMENT ": database uri "
				"is NULL or nil");
	if (user == NULL || *user == NULL || strcmp(*user, (str)str_nil) == 0)
		throw(ILLARG, "remote.connect", ILLEGAL_ARGUMENT ": username is "
				"NULL or nil");
	if (passwd == NULL || *passwd == NULL || strcmp(*passwd, (str)str_nil) == 0)
		throw(ILLARG, "remote.connect", ILLEGAL_ARGUMENT ": password is "
				"NULL or nil");
	if (scen == NULL || *scen == NULL || strcmp(*scen, (str)str_nil) == 0)
		throw(ILLARG, "remote.connect", ILLEGAL_ARGUMENT ": scenario is "
				"NULL or nil");
	if (strcmp(*scen, "mal") != 0 && strcmp(*scen, "msql") != 0)
		throw(ILLARG, "remote.connect", ILLEGAL_ARGUMENT ": scenario '%s' "
				"is not supported", *scen);

	m = mapi_mapiuri(*ouri, *user, *passwd, *scen);
	if (mapi_error(m)) {
		msg = createException(MAL, "remote.connect",
							  "unable to connect to '%s': %s",
							  *ouri, mapi_error_str(m));
		mapi_destroy(m);
		return msg;
	}

	MT_lock_set(&mal_remoteLock);

	/* generate an unique connection name, they are only known
	 * within one mserver, id is primary key, the rest is super key */
	s = mapi_get_dbname(m);
	snprintf(conn, BUFSIZ, "%s_%s_" SZFMT, s, *user, connection_id++);
	/* make sure we can construct MAL identifiers using conn */
	for (s = conn; *s != '\0'; s++) {
		if (!isalpha((int)*s) && !isdigit((int)*s)) {
			*s = '_';
		}
	}

	if (mapi_reconnect(m) != MOK) {
		MT_lock_unset(&mal_remoteLock);
		msg = createException(IO, "remote.connect",
							  "unable to connect to '%s': %s",
							  *ouri, mapi_error_str(m));
		mapi_destroy(m);
		return msg;
	}

	/* connection established, add to list */
	c = GDKzalloc(sizeof(struct _connection));
	if ( c == NULL || (c->name = GDKstrdup(conn)) == NULL) {
		GDKfree(c);
		mapi_destroy(m);
		MT_lock_unset(&mal_remoteLock);
		throw(MAL,"remote.connect", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	c->mconn = m;
	c->nextid = 0;
	c->next = conns;
	conns = c;

	msg = RMTquery(&hdl, "remote.connect", m, "remote.bintype();");
	if (msg)
		return msg;
	if (hdl != NULL && mapi_fetch_row(hdl)) {
		char *val = mapi_fetch_field(hdl, 0);
		c->type = (unsigned char)atoi(val);
		mapi_close_handle(hdl);
	} else {
		c->type = 0;
	}

	MT_lock_init(&c->lock, "remote connection lock");

#ifdef _DEBUG_MAPI_
	mapi_trace(c->mconn, TRUE);
#endif

	MT_lock_unset(&mal_remoteLock);

	*ret = GDKstrdup(conn);
	return(MAL_SUCCEED);
}

str RMTconnect(
		str *ret,
		str *uri,
		str *user,
		str *passwd)
{
	str scen = "mal";
	return RMTconnectScen(ret, uri, user, passwd, &scen);
}


/**
 * Disconnects a connection.  The connection needs not to exist in the
 * system, it only needs to exist for the client (i.e. it was once
 * created).
 */
str RMTdisconnect(void *ret, str *conn) {
	connection c, t;

	if (conn == NULL || *conn == NULL || strcmp(*conn, (str)str_nil) == 0)
		throw(ILLARG, "remote.disconnect", ILLEGAL_ARGUMENT ": connection "
				"is NULL or nil");


	(void) ret;

	/* we need a lock because the same user can be handled by multiple
	 * threads */
	MT_lock_set(&mal_remoteLock);
	c = conns;
	t = NULL; /* parent */
	/* walk through the list */
	while (c != NULL) {
		if (strcmp(c->name, *conn) == 0) {
			/* ok, delete it... */
			if (t == NULL) {
				conns = c->next;
			} else {
				t->next = c->next;
			}

			MT_lock_set(&c->lock); /* shared connection */
			mapi_disconnect(c->mconn);
			mapi_destroy(c->mconn);
			MT_lock_unset(&c->lock);
			MT_lock_destroy(&c->lock);
			GDKfree(c->name);
			GDKfree(c);
			MT_lock_unset(&mal_remoteLock);
			return MAL_SUCCEED;
		}
		t = c;
		c = c->next;
	}

	MT_lock_unset(&mal_remoteLock);
	throw(MAL, "remote.disconnect", "no such connection: %s", *conn);
}

/**
 * Helper function to return a connection matching a given string, or an
 * error if it does not exist.  Since this function is internal, it
 * doesn't check the argument conn, as it should have been checked
 * already.
 * NOTE: this function acquires the mal_remoteLock before accessing conns
 */
static inline str
RMTfindconn(connection *ret, str conn) {
	connection c;

	/* just make sure the return isn't garbage */
	*ret = NULL;
	MT_lock_set(&mal_remoteLock); /* protect c */
	c = conns;
	while (c != NULL) {
		if (strcmp(c->name, conn) == 0) {
			*ret = c;
			MT_lock_unset(&mal_remoteLock);
			return(MAL_SUCCEED);
		}
		c = c->next;
	}
	MT_lock_unset(&mal_remoteLock);
	throw(MAL, "remote.<findconn>", "no such connection: %s", conn);
}

/**
 * Little helper function that returns a GDKmalloced string containing a
 * valid identifier that is supposed to be unique in the connection's
 * remote context.  The generated string depends on the module and
 * function the caller is in. But also the runtime context is important.
 * The format is rmt<id>_<retvar>_<type>.  Every RMTgetId uses a fresh id,
 * to distinguish amongst different (parallel) execution context.
 * Re-use of this remote identifier should be done with care.
 * The encoding of the type allows for ease of type checking later on.
 */
static inline str
RMTgetId(char *buf, MalBlkPtr mb, InstrPtr p, int arg) {
	InstrPtr f;
	char *mod;
	char *var;
	str rt;
	static int idtag=0;

	assert (p->retc);

	var = getArgName(mb, p, arg);
	f = getInstrPtr(mb, 0); /* top level function */
	mod = getModuleId(f);
	if (mod == NULL)
		mod = "user";
	rt = getTypeIdentifier(getArgType(mb,p,arg));
	if (rt == NULL)
		throw(MAL, "remote.put", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	snprintf(buf, BUFSIZ, "rmt%d_%s_%s", idtag++, var, rt);

	GDKfree(rt);
	return(MAL_SUCCEED);
}

/**
 * Helper function to execute a query over the given connection,
 * returning the result handle.  If communication fails in one way or
 * another, an error is returned.  Since this function is internal, it
 * doesn't check the input arguments func, conn and query, as they
 * should have been checked already.
 * NOTE: this function assumes a lock for conn is set
 */
static inline str
RMTquery(MapiHdl *ret, str func, Mapi conn, str query) {
	MapiHdl mhdl;

	*ret = NULL;
	mhdl = mapi_query(conn, query);
	if (mhdl) {
		if (mapi_result_error(mhdl) != NULL) {
			str err = createException(
					getExceptionType(mapi_result_error(mhdl)),
					func,
					"(mapi:monetdb://%s@%s/%s) %s",
					mapi_get_user(conn),
					mapi_get_host(conn),
					mapi_get_dbname(conn),
					getExceptionMessage(mapi_result_error(mhdl)));
			mapi_close_handle(mhdl);
			return(err);
		}
	} else {
		if (mapi_error(conn) != MOK) {
			throw(IO, func, "an error occurred on connection: %s",
					mapi_error_str(conn));
		} else {
			throw(MAL, func, "remote function invocation didn't return a result");
		}
	}

	*ret = mhdl;
	return(MAL_SUCCEED);
}

str RMTprelude(void *ret) {
	int type = 0;

	(void)ret;
#ifdef WORDS_BIGENDIAN
	type |= RMTT_B_ENDIAN;
#else
	type |= RMTT_L_ENDIAN;
#endif
#if SIZEOF_SIZE_T == SIZEOF_LONG_LONG
	type |= RMTT_64_BITS;
#else
	type |= RMTT_32_BITS;
#endif
#if SIZEOF_SIZE_T == SIZEOF_INT
	type |= RMTT_32_OIDS;
#else
	type |= RMTT_64_OIDS;
#endif
	localtype = (unsigned char)type;

	return(MAL_SUCCEED);
}

str RMTepilogue(void *ret) {
	connection c, t;

	(void)ret;

	MT_lock_set(&mal_remoteLock); /* nobody allowed here */
	/* free connections list */
	c = conns;
	while (c != NULL) {
		t = c;
		c = c->next;
		MT_lock_set(&t->lock);
		mapi_destroy(t->mconn);
		MT_lock_unset(&t->lock);
		MT_lock_destroy(&t->lock);
		GDKfree(t->name);
		GDKfree(t);
	}
	/* not sure, but better be safe than sorry */
	conns = NULL;
	MT_lock_unset(&mal_remoteLock);

	return(MAL_SUCCEED);
}

/**
 * get fetches the object referenced by ident over connection conn.
 * We are only interested in retrieving void-headed BATs, i.e. single columns.
 */
str RMTget(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str conn, ident, tmp, rt;
	connection c;
	char qbuf[BUFSIZ + 1];
	MapiHdl mhdl = NULL;
	int rtype;
	ValPtr v;

	(void)mb;

	conn = *getArgReference_str(stk, pci, 1);
	if (conn == NULL || strcmp(conn, (str)str_nil) == 0)
		throw(ILLARG, "remote.get", ILLEGAL_ARGUMENT ": connection name is NULL or nil");
	ident = *getArgReference_str(stk, pci, 2);
	if (ident == 0 || isIdentifier(ident) < 0)
		throw(ILLARG, "remote.get", ILLEGAL_ARGUMENT ": identifier expected, got '%s'", ident);

	/* lookup conn, set c if valid */
	rethrow("remote.get", tmp, RMTfindconn(&c, conn));

	rtype = getArgType(mb, pci, 0);
	v = &stk->stk[pci->argv[0]];

	if (rtype == TYPE_any || isAnyExpression(rtype)) {
		char *tpe, *msg;
		tpe = getTypeName(rtype);
		msg = createException(MAL, "remote.get", ILLEGAL_ARGUMENT ": unsupported any type: %s",
							  tpe);
		GDKfree(tpe);
		return msg;
	}
	/* check if the remote type complies with what we expect.
	   Since the put() encodes the type as known to the remote site
	   we can simple compare it here */
	rt = getTypeIdentifier(rtype);
	if (rt == NULL)
		throw(MAL, "remote.get", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	if (strcmp(ident + strlen(ident) - strlen(rt), rt)) {
		tmp = createException(MAL, "remote.get", ILLEGAL_ARGUMENT
			": remote object type %s does not match expected type %s",
			rt, ident);
		GDKfree(rt);
		return tmp;
	}
	GDKfree(rt);

	if (isaBatType(rtype) && (localtype == 0177 || localtype != c->type ))
	{
		int t, s;
		ptr r;
		str var;
		BAT *b;

		snprintf(qbuf, BUFSIZ, "io.print(%s);", ident);
#ifdef _DEBUG_REMOTE
		fprintf(stderr, "#remote.get:%s\n", qbuf);
#else
		(void) cntxt;
#endif
		/* this call should be a single transaction over the channel*/
		MT_lock_set(&c->lock);

		if ((tmp = RMTquery(&mhdl, "remote.get", c->mconn, qbuf))
				!= MAL_SUCCEED)
		{
#ifdef _DEBUG_REMOTE
			fprintf(stderr, "#REMOTE GET error: %s\n%s\n",
					qbuf, tmp);
#endif
			MT_lock_unset(&c->lock);
			var = createException(MAL, "remote.get", "%s", tmp);
			GDKfree(tmp);
			return var;
		}
		t = getBatType(rtype);
		b = COLnew(0, t, 0, TRANSIENT);
		if (b == NULL)
			throw(MAL, "remote.get", SQLSTATE(HY001) MAL_MALLOC_FAIL);

		if (ATOMvarsized(t)) {
			while (mapi_fetch_row(mhdl)) {
				var = mapi_fetch_field(mhdl, 1);
				if (BUNappend(b, var == NULL ? str_nil : var, FALSE) != GDK_SUCCEED) {
					BBPreclaim(b);
					throw(MAL, "remote.get", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
			}
		} else
			while (mapi_fetch_row(mhdl)) {
				var = mapi_fetch_field(mhdl, 1); 
				if (var == NULL)
					var = "nil";
				s = 0;
				r = NULL;
				if (ATOMfromstr(t, &r, &s, var) <= 0 ||
					BUNappend(b, r, FALSE) != GDK_SUCCEED) {
					BBPreclaim(b);
					GDKfree(r);
					throw(MAL, "remote.get", GDK_EXCEPTION);
				}
				GDKfree(r);
			}

		v->val.bval = b->batCacheid;
		v->vtype = TYPE_bat;
		BBPkeepref(b->batCacheid);
	} else if (isaBatType(rtype)) {
		/* binary compatible remote host, transfer BAT in binary form */
		stream *sout;
		stream *sin;
		char buf[256];
		ssize_t sz = 0, rd;
		BAT *b = NULL;

		/* this call should be a single transaction over the channel*/
		MT_lock_set(&c->lock);

		/* bypass Mapi from this point to efficiently write all data to
		 * the server */
		sout = mapi_get_to(c->mconn);
		sin = mapi_get_from(c->mconn);
		if (sin == NULL || sout == NULL) {
			MT_lock_unset(&c->lock);
			throw(MAL, "remote.get", "Connection lost");
		}

		/* call our remote helper to do this more efficiently */
		mnstr_printf(sout, "remote.batbincopy(%s);\n", ident);
		mnstr_flush(sout);

		/* read the JSON header */
		while ((rd = mnstr_read(sin, &buf[sz], 1, 1)) == 1 && buf[sz] != '\n') {
			sz += rd;
		}
		if (rd < 0) {
			MT_lock_unset(&c->lock);
			throw(MAL, "remote.get", "could not read BAT JSON header");
		}
		if (buf[0] == '!') {
			MT_lock_unset(&c->lock);
			return(GDKstrdup(buf));
		}

		buf[sz] = '\0';
		if ((tmp = RMTinternalcopyfrom(&b, buf, sin)) != NULL) {
			MT_lock_unset(&c->lock);
			return(tmp);
		}

		v->val.bval = b->batCacheid;
		v->vtype = TYPE_bat;
		BBPkeepref(b->batCacheid);
	} else {
		ptr p = NULL;
		str val;
		int len = 0;

		snprintf(qbuf, BUFSIZ, "io.print(%s);", ident);
#ifdef _DEBUG_REMOTE
		fprintf(stderr, "#remote:%s:%s\n", c->name, qbuf);
#endif
		if ((tmp=RMTquery(&mhdl, "remote.get", c->mconn, qbuf)) != MAL_SUCCEED)
		{
			MT_lock_unset(&c->lock);
			return tmp;
		}
		(void) mapi_fetch_row(mhdl); /* should succeed */
		val = mapi_fetch_field(mhdl, 0);

		if (ATOMvarsized(rtype)) {
			VALset(v, rtype, GDKstrdup(val == NULL ? str_nil : val));
		} else {
			ATOMfromstr(rtype, &p, &len, val == NULL ? "nil" : val);
			if (p != NULL) {
				VALset(v, rtype, p);
				if (ATOMextern(rtype) == 0)
					GDKfree(p);
			} else {
				char tval[BUFSIZ + 1];
				snprintf(tval, BUFSIZ, "%s", val);
				tval[BUFSIZ] = '\0';
				mapi_close_handle(mhdl);
				MT_lock_unset(&c->lock);
				throw(MAL, "remote.get", "unable to parse value: %s", tval);
			}
		}
	}

	if (mhdl != NULL)
		mapi_close_handle(mhdl);
	MT_lock_unset(&c->lock);

	return(MAL_SUCCEED);
}

/**
 * stores the given object on the remote host.  The identifier of the
 * object on the remote host is returned for later use.
 */
str RMTput(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str conn, tmp;
	char ident[BUFSIZ];
	connection c;
	ValPtr v;
	int type;
	ptr value;
	MapiHdl mhdl = NULL;

	(void)cntxt;

	conn = *getArgReference_str(stk, pci, 1);
	if (conn == NULL || strcmp(conn, (str)str_nil) == 0)
		throw(ILLARG, "remote.put", ILLEGAL_ARGUMENT ": connection name is NULL or nil");

	/* lookup conn */
	rethrow("remote.put", tmp, RMTfindconn(&c, conn));

	/* put the thing */
	type = getArgType(mb, pci, 2);
	value = getArgReference(stk, pci, 2);

	/* this call should be a single transaction over the channel*/
	MT_lock_set(&c->lock);

	/* get a free, typed identifier for the remote host */
	tmp = RMTgetId(ident, mb, pci, 2);
	if (tmp != MAL_SUCCEED) {
		MT_lock_unset(&c->lock);
		return tmp;
	}

	/* depending on the input object generate actions to store the
	 * object remotely*/
	if (type == TYPE_any || type == TYPE_bat || isAnyExpression(type)) {
		char *tpe, *msg;
		MT_lock_unset(&c->lock);
		tpe = getTypeName(type);
		msg = createException(MAL, "remote.put", "unsupported type: %s", tpe);
		GDKfree(tpe);
		return msg;
	} else if (isaBatType(type) && *(bat*) value != 0) {
		BATiter bi;
		/* naive approach using bat.new() and bat.insert() calls */
		char *tail;
		char qbuf[BUFSIZ];
		bat bid;
		BAT *b = NULL;
		BUN p, q;
		str tailv;
		stream *sout;

		tail = getTypeIdentifier(getBatType(type));
		if (tail == NULL) {
			MT_lock_unset(&c->lock);
			throw(MAL, "remote.put", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}

		bid = *(bat *)value;
		if (bid != 0) {
			if ((b = BATdescriptor(bid)) == NULL){
				MT_lock_unset(&c->lock);
				GDKfree(tail);
				throw(MAL, "remote.put", RUNTIME_OBJECT_MISSING);
			}
		}

		/* bypass Mapi from this point to efficiently write all data to
		 * the server */
		sout = mapi_get_to(c->mconn);

		/* call our remote helper to do this more efficiently */
		mnstr_printf(sout,
				"%s := remote.batload(:%s, " BUNFMT ");\n",
				ident, tail, (bid == 0 ? 0 : BATcount(b)));
		mnstr_flush(sout);
		GDKfree(tail);

		/* b can be NULL if bid == 0 (only type given, ugh) */
		if (b) {
			bi = bat_iterator(b);
			BATloop(b, p, q) {
				tailv = NULL;
				ATOMformat(getBatType(type), BUNtail(bi, p), &tailv);
				if (getBatType(type) > TYPE_str)
					mnstr_printf(sout, "\"%s\"\n", tailv);
				else
					mnstr_printf(sout, "%s\n", tailv);
				GDKfree(tailv);
			}
			BBPunfix(b->batCacheid);
		}

		/* write the empty line the server is waiting for, handles
		 * all errors at the same time, if any */
		qbuf[0] = '\0';
		if ((tmp = RMTquery(&mhdl, "remote.put", c->mconn, qbuf))
				!= MAL_SUCCEED)
		{
			MT_lock_unset(&c->lock);
			return tmp;
		}
		mapi_close_handle(mhdl);
	} else {
		int l = 0;
		str val = NULL;
		char *tpe;
		char qbuf[512], *nbuf = qbuf;
		if (ATOMvarsized(type)) {
			l = ATOMformat(type, *(str *)value, &val);
		} else {
			l = ATOMformat(type, value, &val);
		}
		if (l < 0) {
			MT_lock_unset(&c->lock);
			throw(MAL, "remote.put", GDK_EXCEPTION);
		}
		tpe = getTypeIdentifier(type);
		if (tpe == NULL) {
			MT_lock_unset(&c->lock);
			GDKfree(val);
			throw(MAL, "remote.put", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		l += (int) (strlen(tpe) + strlen(ident) + 10);
		if (l > (int) sizeof(qbuf) && (nbuf = GDKmalloc(l)) == NULL) {
			MT_lock_unset(&c->lock);
			GDKfree(val);
			GDKfree(tpe);
			throw(MAL, "remote.put", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		if (type <= TYPE_str)
			snprintf(nbuf, l, "%s := %s:%s;\n", ident, val, tpe);
		else
			snprintf(nbuf, l, "%s := \"%s\":%s;\n", ident, val, tpe);
		GDKfree(tpe);
		GDKfree(val);
#ifdef _DEBUG_REMOTE
		fprintf(stderr, "#remote.put:%s:%s\n", c->name, nbuf);
#endif
		tmp = RMTquery(&mhdl, "remote.put", c->mconn, nbuf);
		if (nbuf != qbuf)
			GDKfree(nbuf);
		if (tmp != MAL_SUCCEED) {
			MT_lock_unset(&c->lock);
			return tmp;
		}
		mapi_close_handle(mhdl);
	}
	MT_lock_unset(&c->lock);

	/* return the identifier */
	v = &stk->stk[pci->argv[0]];
	v->vtype = TYPE_str;
	v->val.sval = GDKstrdup(ident);
	return(MAL_SUCCEED);
}

/**
 * stores the given <mod>.<fcn> on the remote host.
 * An error is returned if the function is already known at the remote site.
 * The implementation is based on serialisation of the block into a string
 * followed by remote parsing.
 */
str RMTregisterInternal(Client cntxt, str conn, str mod, str fcn)
{
	str tmp, qry, msg;
	connection c;
	char buf[BUFSIZ];
	MapiHdl mhdl = NULL;
	Symbol sym;

	if (conn == NULL || strcmp(conn, (str)str_nil) == 0)
		throw(ILLARG, "remote.register", ILLEGAL_ARGUMENT ": connection name is NULL or nil");

	/* find local definition */
	sym = findSymbol(cntxt->usermodule, putName(mod), putName(fcn));
	if (sym == NULL)
		throw(MAL, "remote.register", ILLEGAL_ARGUMENT ": no such function: %s.%s", mod, fcn);

	/* lookup conn */
	rethrow("remote.register", tmp, RMTfindconn(&c, conn));

	/* this call should be a single transaction over the channel*/
	MT_lock_set(&c->lock);

	/* check remote definition */
	snprintf(buf, BUFSIZ, "inspect.getSignature(\"%s\",\"%s\");", mod, fcn);
#ifdef _DEBUG_REMOTE
	fprintf(stderr, "#remote.register:%s:%s\n", c->name, buf);
#endif
	msg = RMTquery(&mhdl, "remote.register", c->mconn, buf);
	if (msg == MAL_SUCCEED) {
		MT_lock_unset(&c->lock);
		throw(MAL, "remote.register",
				"function already exists at the remote site: %s.%s",
				mod, fcn);
	} else {
		/* we basically hope/assume this is a "doesn't exist" error */
		freeException(msg);
	}
	if (mhdl)
		mapi_close_handle(mhdl);

	/* make sure the program is error free */
	chkProgram(cntxt->usermodule, sym->def);
	if (sym->def->errors) {
		MT_lock_unset(&c->lock);
		throw(MAL, "remote.register",
				"function '%s.%s' contains syntax or type errors",
				mod, fcn);
	}

	qry = mal2str(sym->def, 0, sym->def->stop);
#ifdef _DEBUG_REMOTE
	fprintf(stderr, "#remote.register:%s:%s\n", c->name, qry);
#endif
	msg = RMTquery(&mhdl, "remote.register", c->mconn, qry);
	GDKfree(qry);
	if (mhdl)
		mapi_close_handle(mhdl);

	MT_lock_unset(&c->lock);
	return msg;
}

str RMTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str conn = *getArgReference_str(stk, pci, 1);
	str mod = *getArgReference_str(stk, pci, 2);
	str fcn = *getArgReference_str(stk, pci, 3);
	(void)mb;
	return RMTregisterInternal(cntxt, conn, mod, fcn);
}

/**
 * exec executes the function with its given arguments on the remote
 * host, returning the function's return value.  exec is purposely kept
 * very spartan.  All arguments need to be handles to previously put()
 * values.  It calls the function with the given arguments at the remote
 * site, and returns the handle which stores the return value of the
 * remotely executed function.  This return value can be retrieved using
 * a get call. It handles multiple return arguments.
 */
str RMTexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str conn, mod, func, tmp;
	int i;
	size_t len, buflen;
	connection c= NULL;
	char *qbuf;
	MapiHdl mhdl;

	(void)cntxt;
	(void)mb;

	for (i = 0; i < pci->retc; i++) {
		tmp = *getArgReference_str(stk, pci, i);
		if (tmp == NULL || strcmp(tmp, (str)str_nil) == 0)
			throw(ILLARG, "remote.exec", ILLEGAL_ARGUMENT
					": return value %d is NULL or nil", i);
	}
	conn = *getArgReference_str(stk, pci, i++);
	if (conn == NULL || strcmp(conn, (str)str_nil) == 0)
		throw(ILLARG, "remote.exec", ILLEGAL_ARGUMENT ": connection name is NULL or nil");
	mod = *getArgReference_str(stk, pci, i++);
	if (mod == NULL || strcmp(mod, (str)str_nil) == 0)
		throw(ILLARG, "remote.exec", ILLEGAL_ARGUMENT ": module name is NULL or nil");
	func = *getArgReference_str(stk, pci, i++);
	if (func == NULL || strcmp(func, (str)str_nil) == 0)
		throw(ILLARG, "remote.exec", ILLEGAL_ARGUMENT ": function name is NULL or nil");

	/* lookup conn */
	rethrow("remote.exec", tmp, RMTfindconn(&c, conn));

	/* this call should be a single transaction over the channel*/
	MT_lock_set(&c->lock);

	assert(pci->argc - pci->retc >= 3); /* conn, mod, func, ... */

	len = 0;
	/* count how big a buffer we need */
	len += 2 * (pci->retc > 1);
	for (i = 0; i < pci->retc; i++) {
		len += 2 * (i > 0);
		len += strlen(*getArgReference_str(stk, pci, i));
	}
	len += strlen(mod) + strlen(func) + 6;
	for (i = 3; i < pci->argc - pci->retc; i++) {
		len += 2 * (i > 3);
		len += strlen(*getArgReference_str(stk, pci, pci->retc + i));
	}
	len += 2;
	buflen = len + 1;
	if ((qbuf = GDKmalloc(buflen)) == NULL)
		throw(MAL, "remote.exec", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	len = 0;

	if (pci->retc > 1)
		qbuf[len++] = '(';
	for (i = 0; i < pci->retc; i++)
		len += snprintf(&qbuf[len], buflen - len, "%s%s",
				(i > 0 ? ", " : ""), *getArgReference_str(stk, pci, i));

	if (pci->retc > 1)
		qbuf[len++] = ')';

	/* build the function invocation string in qbuf */
	len += snprintf(&qbuf[len], buflen - len, " := %s.%s(", mod, func);

	/* handle the arguments to the function */

	/* put the arguments one by one, and dynamically build the
	 * invocation string */
	for (i = 3; i < pci->argc - pci->retc; i++) {
		len += snprintf(&qbuf[len], buflen - len, "%s%s",
				(i > 3 ? ", " : ""),
				*(getArgReference_str(stk, pci, pci->retc + i)));
	}

	/* finish end execute the invocation string */
	len += snprintf(&qbuf[len], buflen - len, ");");
#ifdef _DEBUG_REMOTE
	fprintf(stderr,"#remote.exec:%s:%s\n",c->name,qbuf);
#endif
	tmp = RMTquery(&mhdl, "remote.exec", c->mconn, qbuf);
	GDKfree(qbuf);
	if (mhdl)
		mapi_close_handle(mhdl);
	MT_lock_unset(&c->lock);
	return tmp;
}

/**
 * batload is a helper function to make transferring a BAT with RMTput
 * more efficient.  It works by creating a BAT, and loading it with the
 * data as comma separated values from the input stream, until an empty
 * line is read.  The given size argument is taken as a hint only, and
 * is not enforced to match the number of rows read.
 */
str RMTbatload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	ValPtr v;
	int t;
	int size;
	ptr  r;
	int s;
	BAT *b;
	size_t len;
	char *var;
	bstream *fdin = cntxt->fdin;

	v = &stk->stk[pci->argv[0]]; /* return */
	t = getArgType(mb, pci, 1); /* tail type */
	size = *getArgReference_int(stk, pci, 2); /* size */

	b = COLnew(0, t, size, TRANSIENT);
	if (b == NULL)
		throw(MAL, "remote.load", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	/* grab the input stream and start reading */
	fdin->eof = 0;
	len = fdin->pos;
	while (len < fdin->len || bstream_next(fdin) > 0) {
		/* newline hunting (how spartan) */
		for (len = fdin->pos; len < fdin->len && fdin->buf[len] != '\n'; len++)
			;
		/* unterminated line, request more */
		if (fdin->buf[len] != '\n')
			continue;
		/* empty line, end of input */
		if (fdin->pos == len) {
			if (isa_block_stream(fdin->s)) {
				ssize_t n = bstream_next(fdin);
				assert(n == 0);
				(void) n;
			}
			break;
		}
		fdin->buf[len] = '\0'; /* kill \n */
		var = &fdin->buf[fdin->pos];
		/* skip over this line */
		fdin->pos = ++len;

		s = 0;
		r = NULL;
		if (ATOMfromstr(t, &r, &s, var) <= 0 ||
			BUNappend(b, r, FALSE) != GDK_SUCCEED) {
			BBPreclaim(b);
			GDKfree(r);
			throw(MAL, "remote.get", GDK_EXCEPTION);
		}
		GDKfree(r);
	}

	v->val.bval = b->batCacheid;
	v->vtype = TYPE_bat;
	BBPkeepref(b->batCacheid);

	return(MAL_SUCCEED);
}

/**
 * dump given BAT to stream
 */
str RMTbincopyto(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid = *getArgReference_bat(stk, pci, 1);
	BAT *b = BBPquickdesc(bid, FALSE);
	char sendtheap = 0;

	(void)mb;
	(void)stk;
	(void)pci;

	if (b == NULL)
		throw(MAL, "remote.bincopyto", RUNTIME_OBJECT_UNDEFINED);

	BBPfix(bid);

	sendtheap = b->ttype != TYPE_void && b->tvarsized;

	mnstr_printf(cntxt->fdout, /*JSON*/"{"
			"\"version\":1,"
			"\"ttype\":%d,"
			"\"hseqbase\":" OIDFMT ","
			"\"tseqbase\":" OIDFMT ","
			"\"tsorted\":%d,"
			"\"trevsorted\":%d,"
			"\"tkey\":%d,"
			"\"tnonil\":%d,"
			"\"tdense\":%d,"
			"\"size\":" BUNFMT ","
			"\"tailsize\":" SZFMT ","
			"\"theapsize\":" SZFMT
			"}\n",
			b->ttype,
			b->hseqbase, b->tseqbase,
			b->tsorted, b->trevsorted,
			b->tkey,
			b->tnonil,
			b->tdense,
			b->batCount,
			(size_t)b->batCount * Tsize(b),
			sendtheap && b->batCount > 0 ? b->tvheap->free : 0
			);

	if (b->batCount > 0) {
		mnstr_write(cntxt->fdout, /* tail */
		Tloc(b, 0), b->batCount * Tsize(b), 1);
		if (sendtheap)
			mnstr_write(cntxt->fdout, /* theap */
					Tbase(b), b->tvheap->free, 1);
	}
	/* flush is done by the calling environment (MAL) */

	BBPunfix(bid);

	return(MAL_SUCCEED);
}

typedef struct _binbat_v1 {
	int Htype;
	int Ttype;
	oid Hseqbase;
	oid Tseqbase;
	bit Hsorted;
	bit Hrevsorted;
	bit Tsorted;
	bit Trevsorted;
	unsigned int
		Hkey:2,
		Tkey:2,
		Hnonil:1,
		Tnonil:1,
		Tdense:1;
	BUN size;
	size_t headsize;
	size_t tailsize;
	size_t theapsize;
} binbat;

static inline str
RMTinternalcopyfrom(BAT **ret, char *hdr, stream *in)
{
	binbat bb = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	char *nme = NULL;
	char *val = NULL;
	char tmp;
	int len;
	lng lv, *lvp;

	BAT *b;

	/* hdr is a JSON structure that looks like
	 * {"version":1,"ttype":6,"tseqbase":0,"tailsize":4,"theapsize":0}
	 * we take the binary data directly from the stream */

	/* could skip whitespace, but we just don't allow that */
	if (*hdr++ != '{')
		throw(MAL, "remote.bincopyfrom", "illegal input, not a JSON header (got '%s')", hdr - 1);
	while (*hdr != '\0') {
		switch (*hdr) {
			case '"':
				/* we assume only numeric values, so all strings are
				 * elems */
				if (nme != NULL) {
					*hdr = '\0';
				} else {
					nme = hdr + 1;
				}
				break;
			case ':':
				val = hdr + 1;
				break;
			case ',':
			case '}':
				if (val == NULL)
					throw(MAL, "remote.bincopyfrom",
							"illegal input, JSON value missing");
				*hdr = '\0';

				lvp = &lv;
				len = (int) sizeof(lv);
				/* tseqbase can be 1<<31/1<<63 which causes overflow
				 * in lngFromStr, so we check separately */
				if (strcmp(val,
#if SIZEOF_OID == 8
						   "9223372036854775808"
#else
						   "2147483648"
#endif
						) == 0 &&
					strcmp(nme, "tseqbase") == 0) {
					bb.Tseqbase = oid_nil;
				} else {
					/* all values should be non-negative, so we check that
					 * here as well */
					if (lngFromStr(val, &len, &lvp) == 0 ||
						lv < 0 /* includes lng_nil */)
						throw(MAL, "remote.bincopyfrom",
							  "bad %s value: %s", nme, val);

					/* deal with nme and val */
					if (strcmp(nme, "version") == 0) {
						if (lv != 1)
							throw(MAL, "remote.bincopyfrom",
								  "unsupported version: %s", val);
					} else if (strcmp(nme, "hseqbase") == 0) {
#if SIZEOF_OID < SIZEOF_LNG
						if (lv > GDK_oid_max)
							throw(MAL, "remote.bincopyfrom",
									  "bad %s value: %s", nme, val);
#endif
						bb.Hseqbase = (oid)lv;
					} else if (strcmp(nme, "ttype") == 0) {
						if (lv >= GDKatomcnt)
							throw(MAL, "remote.bincopyfrom",
								  "bad %s value: %s", nme, val);
						bb.Ttype = (int) lv;
					} else if (strcmp(nme, "tseqbase") == 0) {
#if SIZEOF_OID < SIZEOF_LNG
						if (lv > GDK_oid_max)
							throw(MAL, "remote.bincopyfrom",
								  "bad %s value: %s", nme, val);
#endif
						bb.Tseqbase = (oid) lv;
					} else if (strcmp(nme, "tsorted") == 0) {
						bb.Tsorted = lv != 0;
					} else if (strcmp(nme, "trevsorted") == 0) {
						bb.Trevsorted = lv != 0;
					} else if (strcmp(nme, "hkey") == 0) {
						bb.Hkey = lv != 0;
					} else if (strcmp(nme, "tkey") == 0) {
						bb.Tkey = lv != 0;
					} else if (strcmp(nme, "tnonil") == 0) {
						bb.Tnonil = lv != 0;
					} else if (strcmp(nme, "tdense") == 0) {
						bb.Tdense = lv != 0;
					} else if (strcmp(nme, "size") == 0) {
						if (lv > (lng) BUN_MAX)
							throw(MAL, "remote.bincopyfrom",
								  "bad %s value: %s", nme, val);
						bb.size = (BUN) lv;
					} else if (strcmp(nme, "tailsize") == 0) {
						bb.tailsize = (size_t) lv;
					} else if (strcmp(nme, "theapsize") == 0) {
						bb.theapsize = (size_t) lv;
					} else {
						throw(MAL, "remote.bincopyfrom",
							  "unknown element: %s", nme);
					}
				}
				nme = val = NULL;
				break;
		}
		hdr++;
	}

	b = COLnew(0, bb.Ttype, bb.size, TRANSIENT);
	if (b == NULL)
		throw(MAL, "remote.get", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	/* for strings, the width may not match, fix it to match what we
	 * retrieved */
	if (bb.Ttype == TYPE_str && bb.size) {
		b->twidth = (unsigned short) (bb.tailsize / bb.size);
		b->tshift = ATOMelmshift(Tsize(b));
	}

	if (bb.tailsize > 0) {
		if (HEAPextend(&b->theap, bb.tailsize, TRUE) != GDK_SUCCEED ||
			mnstr_read(in, b->theap.base, bb.tailsize, 1) < 0)
			goto bailout;
		b->theap.dirty = TRUE;
	}
	if (bb.theapsize > 0) {
		if (HEAPextend(b->tvheap, bb.theapsize, TRUE) != GDK_SUCCEED ||
			mnstr_read(in, b->tvheap->base, bb.theapsize, 1) < 0)
			goto bailout;
		b->tvheap->free = bb.theapsize;
		b->tvheap->dirty = TRUE;
	}

	/* set properties */
	b->tseqbase = bb.Tseqbase;
	b->tsorted = bb.Tsorted;
	b->trevsorted = bb.Trevsorted;
	b->tkey = bb.Tkey;
	b->tnonil = bb.Tnonil;
	b->tdense = bb.Tdense;
	if (bb.Ttype == TYPE_str && bb.size)
		BATsetcapacity(b, (BUN) (bb.tailsize >> b->tshift));
	BATsetcount(b, bb.size);
	b->batDirty = TRUE;

	/* read blockmode flush */
	while (mnstr_read(in, &tmp, 1, 1) > 0) {
		mnstr_printf(GDKout, "!MALexception:remote.bincopyfrom: expected flush, got: %c\n", tmp);
	}

	BATsettrivprop(b);

	*ret = b;
	return(MAL_SUCCEED);

  bailout:
	BBPreclaim(b);
	throw(MAL, "remote.bincopyfrom", "reading failed");
}

/**
 * read from the input stream and give the BAT handle back to the caller
 */
str RMTbincopyfrom(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	BAT *b = NULL;
	ValPtr v;
	str err;

	(void)mb;

	/* We receive a normal line, which contains the JSON header, the
	 * rest is binary data directly on the stream.  We get the first
	 * line from the buffered stream we have here, and pass it on
	 * together with the raw stream we have. */
	cntxt->fdin->eof = 0; /* in case it was before */
	if (bstream_next(cntxt->fdin) <= 0)
		throw(MAL, "remote.bincopyfrom", "expected JSON header");

	cntxt->fdin->buf[cntxt->fdin->len] = '\0';
	err = RMTinternalcopyfrom(&b,
			&cntxt->fdin->buf[cntxt->fdin->pos], cntxt->fdin->s);
	/* skip the JSON line */
	cntxt->fdin->pos = ++cntxt->fdin->len;
	if (err != MAL_SUCCEED)
		return(err);

	v = &stk->stk[pci->argv[0]];
	v->val.bval = b->batCacheid;
	v->vtype = TYPE_bat;
	BBPkeepref(b->batCacheid);

	return(MAL_SUCCEED);
}

/**
 * bintype identifies the system on its binary profile.  This is mainly
 * used to determine if BATs can be sent binary across.
 */
str RMTbintype(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	int type = 0;
	(void)mb;
	(void)stk;
	(void)pci;

#ifdef WORDS_BIGENDIAN
	type |= RMTT_B_ENDIAN;
#else
	type |= RMTT_L_ENDIAN;
#endif
#if SIZEOF_SIZE_T == SIZEOF_LONG_LONG
	type |= RMTT_64_BITS;
#else
	type |= RMTT_32_BITS;
#endif
#if SIZEOF_SIZE_T == SIZEOF_INT
	type |= RMTT_32_OIDS;
#else
	type |= RMTT_64_OIDS;
#endif

	mnstr_printf(cntxt->fdout, "[ %d ]\n", type);

	return(MAL_SUCCEED);
}

/**
 * Returns whether the underlying connection is still connected or not.
 * Best effort implementation on top of mapi using a ping.
 */
str
RMTisalive(int *ret, str *conn)
{
	str tmp;
	connection c;

	if (*conn == NULL || strcmp(*conn, (str)str_nil) == 0)
		throw(ILLARG, "remote.get", ILLEGAL_ARGUMENT ": connection name is NULL or nil");

	/* lookup conn, set c if valid */
	rethrow("remote.get", tmp, RMTfindconn(&c, *conn));

	*ret = 0;
	if (mapi_is_connected(c->mconn) != 0 && mapi_ping(c->mconn) == MOK)
		*ret = 1;

	return MAL_SUCCEED;
}

#endif // HAVE_MAPI
