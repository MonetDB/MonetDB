/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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


#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_function.h" /* for printFunction */
#include "mal_listing.h"
#include "mal_instruction.h" /* for getmodule/func macros */
#include "mal_authorize.h"
#include "mapi.h"
#include "mutils.h"

#define RMTT_L_ENDIAN   (0<<1)
#define RMTT_B_ENDIAN   (1<<1)
#define RMTT_32_BITS    (0<<2)
#define RMTT_64_BITS    (1<<2)
#define RMTT_32_OIDS    (0<<3)
#define RMTT_64_OIDS    (1<<3)

typedef struct _connection {
	MT_Lock            lock;      /* lock to avoid interference */
	str                name;      /* the handle for this connection */
	Mapi               mconn;     /* the Mapi handle for the connection */
	unsigned char      type;      /* binary profile of the connection target */
	size_t             nextid;    /* id counter */
	struct _connection *next;     /* the next connection in the list */
} *connection;

#ifndef WIN32
#include <sys/socket.h> /* socket */
#include <sys/un.h> /* sockaddr_un */
#endif
#include <unistd.h> /* gethostname */

static connection conns = NULL;
static unsigned char localtype = 0177;

static inline str RMTquery(MapiHdl *ret, const char *func, Mapi conn, const char *query);

/**
 * Returns a BAT with valid redirects for the given pattern.  If
 * merovingian is not running, this function throws an error.
 */
static str RMTresolve(bat *ret, str *pat) {
#ifdef NATIVE_WIN32
	(void) ret;
	(void) pat;
	throw(MAL, "remote.resolve", "merovingian is not available on "
			"your platform, sorry"); /* please upgrade to Linux, etc. */
#else
	BAT *list;
	const char *mero_uri;
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
		throw(MAL, "remote.resolve", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* extract port from mero_uri, let mapi figure out the rest */
	mero_uri+=strlen("mapi:monetdb://");
	if (*mero_uri == '[') {
		if ((mero_uri = strchr(mero_uri, ']')) == NULL)
			throw(MAL, "remote.resolve", "illegal IPv6 address on merovingian_uri: %s",
				  GDKgetenv("merovingian_uri"));
	}
	if ((p = strchr(mero_uri, ':')) == NULL)
		throw(MAL, "remote.resolve", "illegal merovingian_uri setting: %s",
				GDKgetenv("merovingian_uri"));
	port = (unsigned int)atoi(p + 1);

	or = redirs = mapi_resolve(NULL, port, *pat);

	if (redirs == NULL)
		throw(MAL, "remote.resolve", "unknown failure when resolving pattern");

	while (*redirs != NULL) {
		if (BUNappend(list, (ptr)*redirs, false) != GDK_SUCCEED) {
			BBPreclaim(list);
			do
				free(*redirs);
			while (*++redirs);
			free(or);
			throw(MAL, "remote.resolve", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
static str RMTconnectScen(
		str *ret,
		str *ouri,
		str *user,
		str *passwd,
		str *scen,
		bit *columnar)
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
	snprintf(conn, BUFSIZ, "%s_%s_%zu", mapi_get_dbname(m), *user, connection_id++);
	/* make sure we can construct MAL identifiers using conn */
	for (s = conn; *s != '\0'; s++) {
		if (!isalnum((unsigned char)*s)) {
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

	if (columnar && *columnar) {
		char set_protocol_query_buf[50];
		snprintf(set_protocol_query_buf, 50, "sql.set_protocol(%d:int);", PROTOCOL_COLUMNAR);
		if ((msg = RMTquery(&hdl, "remote.connect", m, set_protocol_query_buf)))
			return msg;
	}

	/* connection established, add to list */
	c = GDKzalloc(sizeof(struct _connection));
	if ( c == NULL || (c->name = GDKstrdup(conn)) == NULL) {
		GDKfree(c);
		mapi_destroy(m);
		MT_lock_unset(&mal_remoteLock);
		throw(MAL,"remote.connect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	c->mconn = m;
	c->nextid = 0;
	MT_lock_init(&c->lock, c->name);
	c->next = conns;
	conns = c;

	msg = RMTquery(&hdl, "remote.connect", m, "remote.bintype();");
	if (msg) {
		MT_lock_unset(&mal_remoteLock);
		return msg;
	}
	if (hdl != NULL && mapi_fetch_row(hdl)) {
		char *val = mapi_fetch_field(hdl, 0);
		c->type = (unsigned char)atoi(val);
		mapi_close_handle(hdl);
	} else {
		c->type = 0;
	}

#ifdef _DEBUG_MAPI_
	mapi_trace(c->mconn, true);
#endif

	MT_lock_unset(&mal_remoteLock);

	*ret = GDKstrdup(conn);
	if(*ret == NULL)
		throw(MAL,"remote.connect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return(MAL_SUCCEED);
}

static str
RMTconnect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	(void) cntxt;
	(void) mb;
		str* ret	= getArgReference_str(stk, pci, 0);
		str* uri	= getArgReference_str(stk, pci, 1);
		str* user	= getArgReference_str(stk, pci, 2);
		str* passwd	= getArgReference_str(stk, pci, 3);

		str scen = "msql";

		if (pci->argc >= 5)
			scen = *getArgReference_str(stk, pci, 4);

		return RMTconnectScen(ret, uri, user, passwd, &scen, NULL);
}

static str
RMTconnectTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char *local_table;
	char *remoteuser;
	char *passwd;
	char *uri;
	char *tmp;
	char *ret;
	str scen;
	str msg;
	ValPtr v;

	(void)mb;
	(void)cntxt;

	local_table = *getArgReference_str(stk, pci, 1);
	scen = *getArgReference_str(stk, pci, 2);
	if (local_table == NULL || strcmp(local_table, (str)str_nil) == 0) {
		throw(ILLARG, "remote.connect", ILLEGAL_ARGUMENT ": local table is NULL or nil");
	}

	rethrow("remote.connect", tmp, AUTHgetRemoteTableCredentials(local_table, &uri, &remoteuser, &passwd));

	/* The password we just got is hashed. Add the byte \1 in front to
	 * signal this fact to the mapi. */
	size_t pwlen = strlen(passwd);
	char *pwhash = (char*)GDKmalloc(pwlen + 2);
	if (pwhash == NULL) {
		GDKfree(remoteuser);
		GDKfree(passwd);
		throw(MAL, "remote.connect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(pwhash, pwlen + 2, "\1%s", passwd);

	msg = RMTconnectScen(&ret, &uri, &remoteuser, &pwhash, &scen, NULL);

	GDKfree(passwd);
	GDKfree(pwhash);

	if (msg == MAL_SUCCEED) {
		v = &stk->stk[pci->argv[0]];
		v->vtype = TYPE_str;
		if((v->val.sval = GDKstrdup(ret)) == NULL) {
			GDKfree(ret);
			throw(MAL, "remote.connect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}

	GDKfree(ret);
	return msg;
}


/**
 * Disconnects a connection.  The connection needs not to exist in the
 * system, it only needs to exist for the client (i.e. it was once
 * created).
 */
str
RMTdisconnect(void *ret, str *conn) {
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
RMTfindconn(connection *ret, const char *conn) {
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
	const char *mod;
	char *var;
	str rt;
	static ATOMIC_TYPE idtag = ATOMIC_VAR_INIT(0);

	if( p->retc == 0)
		throw(MAL, "remote.getId", ILLEGAL_ARGUMENT "MAL instruction misses retc");

	var = getArgName(mb, p, arg);
	f = getInstrPtr(mb, 0); /* top level function */
	mod = getModuleId(f);
	if (mod == NULL)
		mod = "user";
	rt = getTypeIdentifier(getArgType(mb,p,arg));
	if (rt == NULL)
		throw(MAL, "remote.put", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	snprintf(buf, BUFSIZ, "rmt%u_%s_%s", (unsigned) ATOMIC_ADD(&idtag, 1), var, rt);

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
RMTquery(MapiHdl *ret, const char *func, Mapi conn, const char *query) {
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

static str RMTprelude(void *ret) {
	unsigned int type = 0;

	(void)ret;
#ifdef WORDS_BIGENDIAN
	type |= RMTT_B_ENDIAN;
#else
	type |= RMTT_L_ENDIAN;
#endif
#if SIZEOF_SIZE_T == SIZEOF_LNG
	type |= RMTT_64_BITS;
#else
	type |= RMTT_32_BITS;
#endif
#if SIZEOF_OID == SIZEOF_LNG
	type |= RMTT_64_OIDS;
#else
	type |= RMTT_32_OIDS;
#endif
	localtype = (unsigned char)type;

	return(MAL_SUCCEED);
}

static str RMTepilogue(void *ret) {
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
static str
RMTreadbatheader(stream* sin, char* buf) {
		ssize_t sz = 0, rd;

		/* read the JSON header */
		while ((rd = mnstr_read(sin, &buf[sz], 1, 1)) == 1 && buf[sz] != '\n') {
			sz += rd;
		}
		if (rd < 0) {
			throw(MAL, "remote.get", "could not read BAT JSON header");
		}
		if (buf[0] == '!') {
			char *result;
			if((result = GDKstrdup(buf)) == NULL)
				throw(MAL, "remote.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return result;
		}

		buf[sz] = '\0';

		return MAL_SUCCEED;
}

typedef struct _binbat_v1 {
	int Ttype;
	oid Hseqbase;
	oid Tseqbase;
	bool
		Tsorted:1,
		Trevsorted:1,
		Tkey:1,
		Tnonil:1,
		Tdense:1;
	BUN size;
	size_t headsize;
	size_t tailsize;
	size_t theapsize;
} binbat;

static str
RMTinternalcopyfrom(BAT **ret, char *hdr, stream *in, bool must_flush)
{
	binbat bb = { 0, 0, 0, false, false, false, false, false, 0, 0, 0, 0 };
	char *nme = NULL;
	char *val = NULL;
	char tmp;
	size_t len;
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
				len = sizeof(lv);
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
					if (lngFromStr(val, &len, &lvp, true) < 0 ||
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
		throw(MAL, "remote.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* for strings, the width may not match, fix it to match what we
	 * retrieved */
	if (bb.Ttype == TYPE_str && bb.size) {
		b->twidth = (unsigned short) (bb.tailsize / bb.size);
		b->tshift = ATOMelmshift(Tsize(b));
	}

	if (bb.tailsize > 0) {
		if (HEAPextend(b->theap, bb.tailsize, true) != GDK_SUCCEED ||
			mnstr_read(in, b->theap->base, bb.tailsize, 1) < 0)
			goto bailout;
		b->theap->dirty = true;
	}
	if (bb.theapsize > 0) {
		if (HEAPextend(b->tvheap, bb.theapsize, true) != GDK_SUCCEED ||
			mnstr_read(in, b->tvheap->base, bb.theapsize, 1) < 0)
			goto bailout;
		b->tvheap->free = bb.theapsize;
		b->tvheap->dirty = true;
	}

	/* set properties */
	b->tseqbase = bb.Tdense ? bb.Tseqbase : oid_nil;
	b->tsorted = bb.Tsorted;
	b->trevsorted = bb.Trevsorted;
	b->tkey = bb.Tkey;
	b->tnonil = bb.Tnonil;
	if (bb.Ttype == TYPE_str && bb.size)
		BATsetcapacity(b, (BUN) (bb.tailsize >> b->tshift));
	BATsetcount(b, bb.size);
	b->batDirtydesc = true;

	// read blockmode flush
	while (must_flush && mnstr_read(in, &tmp, 1, 1) > 0) {
		TRC_ERROR(MAL_REMOTE, "Expected flush, got: %c\n", tmp);
	}

	BATsettrivprop(b);

	*ret = b;
	return(MAL_SUCCEED);

  bailout:
	BBPreclaim(b);
	throw(MAL, "remote.bincopyfrom", "reading failed");
}

/**
 * get fetches the object referenced by ident over connection conn.
 * We are only interested in retrieving void-headed BATs, i.e. single columns.
 */
static str RMTget(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str conn, ident, tmp, rt;
	connection c;
	char qbuf[BUFSIZ + 1];
	MapiHdl mhdl = NULL;
	int rtype;
	ValPtr v;

	(void)mb;
	(void) cntxt;

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
		throw(MAL, "remote.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		int t;
		size_t s;
		ptr r;
		str var;
		BAT *b;

		snprintf(qbuf, BUFSIZ, "io.print(%s);", ident);

		TRC_DEBUG(MAL_REMOTE, "Remote get: %s\n", qbuf);

		/* this call should be a single transaction over the channel*/
		MT_lock_set(&c->lock);

		if ((tmp = RMTquery(&mhdl, "remote.get", c->mconn, qbuf))
				!= MAL_SUCCEED)
		{
			TRC_ERROR(MAL_REMOTE, "Remote get: %s\n%s\n", qbuf, tmp);
			MT_lock_unset(&c->lock);
			var = createException(MAL, "remote.get", "%s", tmp);
			freeException(tmp);
			return var;
		}
		t = getBatType(rtype);
		b = COLnew(0, t, 0, TRANSIENT);
		if (b == NULL) {
			mapi_close_handle(mhdl);
			MT_lock_unset(&c->lock);
			throw(MAL, "remote.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		if (ATOMvarsized(t)) {
			while (mapi_fetch_row(mhdl)) {
				var = mapi_fetch_field(mhdl, 1);
				if (BUNappend(b, var == NULL ? str_nil : var, false) != GDK_SUCCEED) {
					BBPreclaim(b);
					mapi_close_handle(mhdl);
					MT_lock_unset(&c->lock);
					throw(MAL, "remote.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		} else
			while (mapi_fetch_row(mhdl)) {
				var = mapi_fetch_field(mhdl, 1);
				if (var == NULL)
					var = "nil";
				s = 0;
				r = NULL;
				if (ATOMfromstr(t, &r, &s, var, true) < 0 ||
					BUNappend(b, r, false) != GDK_SUCCEED) {
					BBPreclaim(b);
					GDKfree(r);
					mapi_close_handle(mhdl);
					MT_lock_unset(&c->lock);
					throw(MAL, "remote.get", GDK_EXCEPTION);
				}
				GDKfree(r);
			}

		v->val.bval = b->batCacheid;
		v->vtype = TYPE_bat;
		BBPkeepref(b->batCacheid);

		mapi_close_handle(mhdl);
		MT_lock_unset(&c->lock);
	} else if (isaBatType(rtype)) {
		/* binary compatible remote host, transfer BAT in binary form */
		stream *sout;
		stream *sin;
		char buf[256];
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
		mnstr_flush(sout, MNSTR_FLUSH_DATA);

		if ( (tmp = RMTreadbatheader(sin, buf)) != MAL_SUCCEED) {
			MT_lock_unset(&c->lock);
			return tmp;
		}

		if ((tmp = RMTinternalcopyfrom(&b, buf, sin, true)) != NULL) {
			MT_lock_unset(&c->lock);
			return(tmp);
		}

		v->val.bval = b->batCacheid;
		v->vtype = TYPE_bat;
		BBPkeepref(b->batCacheid);

		MT_lock_unset(&c->lock);
	} else {
		ptr p = NULL;
		str val;
		size_t len = 0;

		snprintf(qbuf, BUFSIZ, "io.print(%s);", ident);
		TRC_DEBUG(MAL_REMOTE, "Remote get: %s - %s\n", c->name, qbuf);
		if ((tmp=RMTquery(&mhdl, "remote.get", c->mconn, qbuf)) != MAL_SUCCEED)
		{
			return tmp;
		}
		(void) mapi_fetch_row(mhdl); /* should succeed */
		val = mapi_fetch_field(mhdl, 0);

		if (ATOMvarsized(rtype)) {
			p = GDKstrdup(val == NULL ? str_nil : val);
			if (p == NULL) {
				mapi_close_handle(mhdl);
				throw(MAL, "remote.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			VALset(v, rtype, p);
		} else if (ATOMfromstr(rtype, &p, &len, val == NULL ? "nil" : val, true) < 0) {
			char *msg;
			msg = createException(MAL, "remote.get",
								  "unable to parse value: %s",
								  val == NULL ? "nil" : val);
			mapi_close_handle(mhdl);
			GDKfree(p);
			return msg;
		} else {
			VALset(v, rtype, p);
			if (ATOMextern(rtype) == 0)
				GDKfree(p);
		}

		mapi_close_handle(mhdl);
	}

	return(MAL_SUCCEED);
}

/**
 * stores the given object on the remote host.  The identifier of the
 * object on the remote host is returned for later use.
 */
static str RMTput(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
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
	} else if (isaBatType(type) && !is_bat_nil(*(bat*) value)) {
		BATiter bi;
		/* naive approach using bat.new() and bat.insert() calls */
		char *tail;
		bat bid;
		BAT *b = NULL;
		BUN p, q;
		str tailv;
		stream *sout;

		tail = getTypeIdentifier(getBatType(type));
		if (tail == NULL) {
			MT_lock_unset(&c->lock);
			throw(MAL, "remote.put", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		bid = *(bat *)value;
		if (bid != 0) {
			if ((b = BATdescriptor(bid)) == NULL){
				MT_lock_unset(&c->lock);
				GDKfree(tail);
				throw(MAL, "remote.put", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
		}

		/* bypass Mapi from this point to efficiently write all data to
		 * the server */
		sout = mapi_get_to(c->mconn);

		/* call our remote helper to do this more efficiently */
		mnstr_printf(sout,
				"%s := remote.batload(nil:%s, " BUNFMT ");\n",
				ident, tail, (bid == 0 ? 0 : BATcount(b)));
		mnstr_flush(sout, MNSTR_FLUSH_DATA);
		GDKfree(tail);

		/* b can be NULL if bid == 0 (only type given, ugh) */
		if (b) {
			bi = bat_iterator(b);
			BATloop(b, p, q) {
				tailv = ATOMformat(getBatType(type), BUNtail(bi, p));
				if (tailv == NULL) {
					BBPunfix(b->batCacheid);
					MT_lock_unset(&c->lock);
					throw(MAL, "remote.put", GDK_EXCEPTION);
				}
				if (getBatType(type) >= TYPE_date && getBatType(type) != TYPE_str)
					mnstr_printf(sout, "\"%s\"\n", tailv);
				else
					mnstr_printf(sout, "%s\n", tailv);
				GDKfree(tailv);
			}
			BBPunfix(b->batCacheid);
		}

		/* write the empty line the server is waiting for, handles
		 * all errors at the same time, if any */
		if ((tmp = RMTquery(&mhdl, "remote.put", c->mconn, ""))
				!= MAL_SUCCEED)
		{
			MT_lock_unset(&c->lock);
			return tmp;
		}
		mapi_close_handle(mhdl);
	} else if (isaBatType(type) && is_bat_nil(*(bat*) value)) {
		stream *sout;
		str typename = getTypeName(type);
		sout = mapi_get_to(c->mconn);
		mnstr_printf(sout,
				"%s := nil:%s;\n", ident, typename);
		mnstr_flush(sout, MNSTR_FLUSH_DATA);
		GDKfree(typename);
	} else {
		size_t l;
		str val;
		char *tpe;
		char qbuf[512], *nbuf = qbuf;
		if (ATOMvarsized(type)) {
			val = ATOMformat(type, *(str *)value);
		} else {
			val = ATOMformat(type, value);
		}
		if (val == NULL) {
			MT_lock_unset(&c->lock);
			throw(MAL, "remote.put", GDK_EXCEPTION);
		}
		tpe = getTypeIdentifier(type);
		if (tpe == NULL) {
			MT_lock_unset(&c->lock);
			GDKfree(val);
			throw(MAL, "remote.put", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		l = strlen(val) + strlen(tpe) + strlen(ident) + 10;
		if (l > (ssize_t) sizeof(qbuf) && (nbuf = GDKmalloc(l)) == NULL) {
			MT_lock_unset(&c->lock);
			GDKfree(val);
			GDKfree(tpe);
			throw(MAL, "remote.put", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (type < TYPE_date || type == TYPE_str)
			snprintf(nbuf, l, "%s := %s:%s;\n", ident, val, tpe);
		else
			snprintf(nbuf, l, "%s := \"%s\":%s;\n", ident, val, tpe);
		GDKfree(tpe);
		GDKfree(val);
		TRC_DEBUG(MAL_REMOTE, "Remote put: %s - %s\n", c->name, nbuf);
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
	if((v->val.sval = GDKstrdup(ident)) == NULL)
		throw(MAL, "remote.put", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return(MAL_SUCCEED);
}

/**
 * stores the given <mod>.<fcn> on the remote host.
 * An error is returned if the function is already known at the remote site.
 * The implementation is based on serialisation of the block into a string
 * followed by remote parsing.
 */
static str RMTregisterInternal(Client cntxt, char** fcn_id, const char *conn, const char *mod, const char *fcn)
{
	str tmp, qry, msg;
	connection c;
	char buf[BUFSIZ];
	MapiHdl mhdl = NULL;
	Symbol sym;

	if (strNil(conn))
		throw(ILLARG, "remote.register", ILLEGAL_ARGUMENT ": connection name is NULL or nil");

	/* find local definition */
	sym = findSymbol(cntxt->usermodule, putName(mod), putName(fcn));
	if (sym == NULL)
		throw(MAL, "remote.register", ILLEGAL_ARGUMENT ": no such function: %s.%s", mod, fcn);

	/* lookup conn */
	rethrow("remote.register", tmp, RMTfindconn(&c, conn));

	/* this call should be a single transaction over the channel*/
	MT_lock_set(&c->lock);

	/* get a free, typed identifier for the remote host */
	char ident[BUFSIZ];
	tmp = RMTgetId(ident, sym->def, getInstrPtr(sym->def, 0), 0);
	if (tmp != MAL_SUCCEED) {
		MT_lock_unset(&c->lock);
		return tmp;
	}

	/* check remote definition */
	snprintf(buf, BUFSIZ, "b:bit:=inspect.getExistence(\"%s\",\"%s\");\nio.print(b);", mod, ident);
	TRC_DEBUG(MAL_REMOTE, "Remote register: %s - %s\n", c->name, buf);
	if ((msg = RMTquery(&mhdl, "remote.register", c->mconn, buf)) != MAL_SUCCEED){
		MT_lock_unset(&c->lock);
		return msg;
	}

	char* result;
	if ( mapi_get_field_count(mhdl) && mapi_fetch_row(mhdl) && (result = mapi_fetch_field(mhdl, 0))) {
		if (strcmp(result, "false") != 0)
			msg = createException(MAL, "remote.register",
					"function already exists at the remote site: %s.%s",
					mod, fcn);
	}
	else
		msg = createException(MAL, "remote.register", OPERATION_FAILED);

	mapi_close_handle(mhdl);

	if (msg) {
		MT_lock_unset(&c->lock);
		return msg;
	}

	*fcn_id = GDKstrdup(ident);
	if (*fcn_id == NULL) {
		MT_lock_unset(&c->lock);
		throw(MAL, "Remote register", MAL_MALLOC_FAIL);
	}

	Symbol prg;
	if ((prg = newFunction(putName(mod), putName(*fcn_id), FUNCTIONsymbol)) == NULL) {
		MT_lock_unset(&c->lock);
		throw(MAL, "Remote register", MAL_MALLOC_FAIL);
	}

	// We only need the Symbol not the inner program stub. So we clear it.
	freeMalBlk(prg->def);
	prg->def = NULL;

	if ((prg->def = copyMalBlk(sym->def)) == NULL) {
		MT_lock_unset(&c->lock);
		freeSymbol(prg);
		throw(MAL, "Remote register", MAL_MALLOC_FAIL);
	}
	setFunctionId(getInstrPtr(prg->def, 0), putName(*fcn_id));

	/* make sure the program is error free */
	msg = chkProgram(cntxt->usermodule, prg->def);
	if ( msg != MAL_SUCCEED || prg->def->errors) {
		MT_lock_unset(&c->lock);
		if (msg)
			return msg;
		throw(MAL, "remote.register",
				"function '%s.%s' contains syntax or type errors",
				mod, *fcn_id);
	}

	qry = mal2str(prg->def, 0, prg->def->stop);
	TRC_DEBUG(MAL_REMOTE, "Remote register: %s - %s\n", c->name, qry);
	msg = RMTquery(&mhdl, "remote.register", c->mconn, qry);
	GDKfree(qry);
	if (mhdl)
		mapi_close_handle(mhdl);

	freeSymbol(prg);

	MT_lock_unset(&c->lock);
	return msg;
}

static str RMTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	char **fcn_id = getArgReference_str(stk, pci, 0);
	const char *conn = *getArgReference_str(stk, pci, 1);
	const char *mod = *getArgReference_str(stk, pci, 2);
	const char *fcn = *getArgReference_str(stk, pci, 3);
	(void)mb;
	return RMTregisterInternal(cntxt, fcn_id, conn, mod, fcn);
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
static str RMTexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str conn, mod, func, tmp;
	int i;
	size_t len, buflen;
	connection c= NULL;
	char *qbuf;
	MapiHdl mhdl;

	(void)cntxt;
	(void)mb;
	bool no_return_arguments = 0;

	columnar_result_callback* rcb = NULL;
	ValRecord *v = &(stk)->stk[(pci)->argv[4]];
	if (pci->retc == 1 && (pci->argc >= 4) && (v->vtype == TYPE_ptr) ) {
		rcb = (columnar_result_callback*) v->val.pval;
	}

	for (i = 0; i < pci->retc; i++) {
		if (stk->stk[pci->argv[i]].vtype == TYPE_str) {
			tmp = *getArgReference_str(stk, pci, i);
			if (tmp == NULL || strcmp(tmp, (str)str_nil) == 0)
				throw(ILLARG, "remote.exec", ILLEGAL_ARGUMENT
						": return value %d is NULL or nil", i);
		}
		else
			no_return_arguments = 1;
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

	if(!no_return_arguments && pci->argc - pci->retc < 3) { /* conn, mod, func, ... */
		MT_lock_unset(&c->lock);
		throw(MAL, "remote.exec", ILLEGAL_ARGUMENT  " MAL instruction misses arguments");
	}

	len = 0;
	/* count how big a buffer we need */
	len += 2 * (pci->retc > 1);
	if (!no_return_arguments)
		for (i = 0; i < pci->retc; i++) {
			len += 2 * (i > 0);
			len += strlen(*getArgReference_str(stk, pci, i));
		}

	const int arg_index = rcb ? 4 : 3;

	len += strlen(mod) + strlen(func) + 6;
	for (i = arg_index; i < pci->argc - pci->retc; i++) {
		len += 2 * (i > arg_index);
		len += strlen(*getArgReference_str(stk, pci, pci->retc + i));
	}
	len += 2;
	buflen = len + 1;
	if ((qbuf = GDKmalloc(buflen)) == NULL) {
		MT_lock_unset(&c->lock);
		throw(MAL, "remote.exec", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	len = 0;

	if (pci->retc > 1)
		qbuf[len++] = '(';
	if (!no_return_arguments)
		for (i = 0; i < pci->retc; i++)
			len += snprintf(&qbuf[len], buflen - len, "%s%s",
					(i > 0 ? ", " : ""), *getArgReference_str(stk, pci, i));

	if (pci->retc > 1)
		qbuf[len++] = ')';

	/* build the function invocation string in qbuf */
	if (!no_return_arguments && pci->retc > 0) {
		len += snprintf(&qbuf[len], buflen - len, " := %s.%s(", mod, func);
	}
	else {
		len += snprintf(&qbuf[len], buflen - len, " %s.%s(", mod, func);
	}

	/* handle the arguments to the function */

	/* put the arguments one by one, and dynamically build the
	 * invocation string */
	for (i = arg_index; i < pci->argc - pci->retc; i++) {
		len += snprintf(&qbuf[len], buflen - len, "%s%s",
				(i > arg_index ? ", " : ""),
				*(getArgReference_str(stk, pci, pci->retc + i)));
	}

	/* finish end execute the invocation string */
	len += snprintf(&qbuf[len], buflen - len, ");");
	TRC_DEBUG(MAL_REMOTE, "Remote exec: %s - %s\n", c->name, qbuf);
	tmp = RMTquery(&mhdl, "remote.exec", c->mconn, qbuf);
	GDKfree(qbuf);

	/* Temporary hack:
	 * use a callback to immediately handle columnar results before hdl is destroyed. */
	if(tmp == MAL_SUCCEED && rcb && mhdl && (mapi_get_querytype(mhdl) == Q_TABLE || mapi_get_querytype(mhdl) == Q_PREPARE)) {

		int fields = mapi_get_field_count(mhdl);

		columnar_result* results = GDKzalloc(sizeof(columnar_result) * fields);

		char buf[256] = {0};

		stream* sin = mapi_get_from(c->mconn);

		char* tblname = mapi_get_table(mhdl, 0);

		int i;
		for (i = 0; i < fields; i++) {
			BAT *b = NULL;

			RMTreadbatheader(sin, buf);
			RMTinternalcopyfrom(&b, buf, sin, i == fields - 1);

			if ( b == NULL) {
				tmp= createException(MAL,"sql.resultset",SQLSTATE(HY005) "Cannot access column descriptor ");
				break;
			}

			results[i].id = b->batCacheid;
			results[i].colname = mapi_get_name(mhdl, i);
			results[i].tpename = mapi_get_type(mhdl, i);
			results[i].digits = mapi_get_digits(mhdl, i);
			results[i].scale = mapi_get_scale(mhdl, i);
			BBPkeepref(results[i].id);
		}

		if (tmp != MAL_SUCCEED) {
			for (int j = 0; j < i; j++) {
				BBPrelease(results[j].id);
			}
		}
		else {
			assert(rcb->context);
			tmp = rcb->call(rcb->context, tblname, results, fields);
		}
		GDKfree(results);
	}

	if (rcb) {
		GDKfree(rcb->context);
		rcb->context = NULL;
		GDKfree(rcb);
	}

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
static str RMTbatload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	ValPtr v;
	int t;
	int size;
	ptr  r;
	size_t s;
	BAT *b;
	size_t len;
	char *var;
	str msg = MAL_SUCCEED;
	bstream *fdin = cntxt->fdin;

	v = &stk->stk[pci->argv[0]]; /* return */
	t = getArgType(mb, pci, 1); /* tail type */
	size = *getArgReference_int(stk, pci, 2); /* size */

	b = COLnew(0, t, size, TRANSIENT);
	if (b == NULL)
		throw(MAL, "remote.load", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* grab the input stream and start reading */
	fdin->eof = false;
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
				if( n )
					msg = createException(MAL, "remote.load", SQLSTATE(HY013) "Unexpected return from remote");
			}
			break;
		}
		fdin->buf[len] = '\0'; /* kill \n */
		var = &fdin->buf[fdin->pos];
		/* skip over this line */
		fdin->pos = ++len;

		s = 0;
		r = NULL;
		if (ATOMfromstr(t, &r, &s, var, true) < 0 ||
			BUNappend(b, r, false) != GDK_SUCCEED) {
			BBPreclaim(b);
			GDKfree(r);
			throw(MAL, "remote.get", GDK_EXCEPTION);
		}
		GDKfree(r);
	}

	v->val.bval = b->batCacheid;
	v->vtype = TYPE_bat;
	BBPkeepref(b->batCacheid);

	return msg;
}

/**
 * dump given BAT to stream
 */
static str RMTbincopyto(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid = *getArgReference_bat(stk, pci, 1);
	BAT *b = BBPquickdesc(bid, false), *v = b;
	char sendtheap = 0;

	(void)mb;
	(void)stk;
	(void)pci;

	if (b == NULL)
		throw(MAL, "remote.bincopyto", RUNTIME_OBJECT_UNDEFINED);

	if (BBPfix(bid) <= 0)
		throw(MAL, "remote.bincopyto", MAL_MALLOC_FAIL);

	sendtheap = b->ttype != TYPE_void && b->tvarsized;
	if (isVIEW(b) && sendtheap && VIEWvtparent(b) && BATcount(b) < BATcount(BBPquickdesc(VIEWvtparent(b), false)))
		v = COLcopy(b, b->ttype, true, TRANSIENT);

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
			"\"tailsize\":%zu,"
			"\"theapsize\":%zu"
			"}\n",
			v->ttype,
			v->hseqbase, v->tseqbase,
			v->tsorted, v->trevsorted,
			v->tkey,
			v->tnonil,
			BATtdense(v),
			v->batCount,
			(size_t)v->batCount * Tsize(v),
			sendtheap && v->batCount > 0 ? v->tvheap->free : 0
			);

	if (v->batCount > 0) {
		mnstr_write(cntxt->fdout, /* tail */
		Tloc(v, 0), v->batCount * Tsize(v), 1);
		if (sendtheap)
			mnstr_write(cntxt->fdout, /* theap */
					Tbase(v), v->tvheap->free, 1);
	}
	/* flush is done by the calling environment (MAL) */

	if (b != v)
		BBPreclaim(v);

	BBPunfix(bid);

	return(MAL_SUCCEED);
}

/**
 * read from the input stream and give the BAT handle back to the caller
 */
static str RMTbincopyfrom(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	BAT *b = NULL;
	ValPtr v;
	str err;

	(void)mb;

	/* We receive a normal line, which contains the JSON header, the
	 * rest is binary data directly on the stream.  We get the first
	 * line from the buffered stream we have here, and pass it on
	 * together with the raw stream we have. */
	cntxt->fdin->eof = false; /* in case it was before */
	if (bstream_next(cntxt->fdin) <= 0)
		throw(MAL, "remote.bincopyfrom", "expected JSON header");

	cntxt->fdin->buf[cntxt->fdin->len] = '\0';
	err = RMTinternalcopyfrom(&b,
			&cntxt->fdin->buf[cntxt->fdin->pos], cntxt->fdin->s, true);
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
static str RMTbintype(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	int type = 0;
	(void)mb;
	(void)stk;
	(void)pci;

#ifdef WORDS_BIGENDIAN
	type |= RMTT_B_ENDIAN;
#else
	type |= RMTT_L_ENDIAN;
#endif
#if SIZEOF_SIZE_T == SIZEOF_LNG
	type |= RMTT_64_BITS;
#else
	type |= RMTT_32_BITS;
#endif
#if SIZEOF_OID == SIZEOF_LNG
	type |= RMTT_64_OIDS;
#else
	type |= RMTT_32_OIDS;
#endif

	mnstr_printf(cntxt->fdout, "[ %d ]\n", type);

	return(MAL_SUCCEED);
}

/**
 * Returns whether the underlying connection is still connected or not.
 * Best effort implementation on top of mapi using a ping.
 */
static str
RMTisalive(int *ret, str *conn)
{
	str tmp;
	connection c;

	if (*conn == NULL || strcmp(*conn, (str)str_nil) == 0)
		throw(ILLARG, "remote.get", ILLEGAL_ARGUMENT ": connection name is NULL or nil");

	/* lookup conn, set c if valid */
	rethrow("remote.get", tmp, RMTfindconn(&c, *conn));

	*ret = 0;
	if (mapi_is_connected(c->mconn) && mapi_ping(c->mconn) == MOK)
		*ret = 1;

	return MAL_SUCCEED;
}

// This is basically a no op
static str
RMTregisterSupervisor(int *ret, str *sup_uuid, str *query_uuid) {
	(void)sup_uuid;
	(void)query_uuid;

	*ret = 0;
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func remote_init_funcs[] = {
 command("remote", "prelude", RMTprelude, false, "initialise the remote module", args(1,1, arg("",void))),
 command("remote", "epilogue", RMTepilogue, false, "release the resources held by the remote module", args(1,1, arg("",void))),
 command("remote", "resolve", RMTresolve, false, "resolve a pattern against Merovingian and return the URIs", args(1,2, batarg("",str),arg("pattern",str))),
 pattern("remote", "connect", RMTconnect, false, "returns a newly created connection for uri, using user name and password", args(1,5, arg("",str),arg("uri",str),arg("user",str),arg("passwd",str),arg("scen",str))),
 command("remote", "connect", RMTconnectScen, false, "returns a newly created connection for uri, using user name, password and scenario", args(1,6, arg("",str),arg("uri",str),arg("user",str),arg("passwd",str),arg("scen",str),arg("columnar",bit))),
 pattern("remote", "connect", RMTconnectTable, false, "return a newly created connection for a table. username and password should be in the vault", args(1,3, arg("",str),arg("table",str),arg("schen",str))),
 command("remote", "disconnect", RMTdisconnect, false, "disconnects the connection pointed to by handle (received from a call to connect()", args(1,2, arg("",void),arg("conn",str))),
 pattern("remote", "get", RMTget, false, "retrieves a copy of remote object ident", args(1,3, argany("",0),arg("conn",str),arg("ident",str))),
 pattern("remote", "put", RMTput, false, "copies object to the remote site and returns its identifier", args(1,3, arg("",str),arg("conn",str),argany("object",0))),
 pattern("remote", "register", RMTregister, false, "register <mod>.<fcn> at the remote site", args(1,4, arg("",str),arg("conn",str),arg("mod",str),arg("fcn",str))),
 pattern("remote", "exec", RMTexec, false, "remotely executes <mod>.<func> and returns the handle to its result", args(1,4, arg("",str),arg("conn",str),arg("mod",str),arg("func",str))),
 pattern("remote", "exec", RMTexec, false, "remotely executes <mod>.<func> and returns the handle to its result", args(1,4, vararg("",str),arg("conn",str),arg("mod",str),arg("func",str))),
 pattern("remote", "exec", RMTexec, false, "remotely executes <mod>.<func> using the argument list of remote objects and returns the handle to its result", args(1,5, arg("",str),arg("conn",str),arg("mod",str),arg("func",str),vararg("",str))),
 pattern("remote", "exec", RMTexec, false, "remotely executes <mod>.<func> using the argument list of remote objects and returns the handle to its result", args(1,5, vararg("",str),arg("conn",str),arg("mod",str),arg("func",str),vararg("",str))),
 pattern("remote", "exec", RMTexec, false, "remotely executes <mod>.<func> using the argument list of remote objects and applying function pointer rcb as callback to handle any results.", args(0,5, arg("conn",str),arg("mod",str),arg("func",str),arg("rcb",ptr), vararg("",str))),
 pattern("remote", "exec", RMTexec, false, "remotely executes <mod>.<func> using the argument list of remote objects and ignoring results.", args(0,4, arg("conn",str),arg("mod",str),arg("func",str), vararg("",str))),
 command("remote", "isalive", RMTisalive, false, "check if conn is still valid and connected", args(1,2, arg("",int),arg("conn",str))),
 pattern("remote", "batload", RMTbatload, false, "create a BAT of the given type and size, and load values from the input stream", args(1,3, batargany("",1),argany("tt",1),arg("size",int))),
 pattern("remote", "batbincopy", RMTbincopyto, false, "dump BAT b in binary form to the stream", args(1,2, arg("",void),batargany("b",0))),
 pattern("remote", "batbincopy", RMTbincopyfrom, false, "store the binary BAT data in the BBP and return as BAT", args(1,1, batargany("",0))),
 pattern("remote", "bintype", RMTbintype, false, "print the binary type of this mserver5", args(1,1, arg("",void))),
 command("remote", "register_supervisor", RMTregisterSupervisor, false, "Register the supervisor uuid at a remote site", args(1,3, arg("",int),arg("sup_uuid",str),arg("query_uuid",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_remote_mal)
{ mal_module("remote", NULL, remote_init_funcs); }

#endif
