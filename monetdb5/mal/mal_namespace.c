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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (co) M.L. Kersten
 * Name Space Management.
 * Significant speed improvement at type resolution and during the
 * optimization phases can be gained when each module or function identifier is
 * replaced by a fixed length internal identifier. This translation is
 * done once during parsing.
 * Variables are always stored local to the MAL block in which they
 * are used.
 *
 * The number of module and function names is expected to be limited.
 * Therefore, the namespace manager is organized as a shared table. The alternative
 * is a namespace per client. However, this would force
 * passing around the client identity or an expensive operation to deduce
 * this from the process id. The price paid is that updates to the namespace
 * should be protected against concurrent access.
 * The current version is protected with locks, which by itself may cause quite
 * some overhead.
 *
 * The space can, however, also become polluted with identifiers generated on the fly.
 * Compilers are adviced to be conservative in their naming, or explicitly manage
 * the name space by deletion of non-used names once in a while.
 *
 * Code bodies
 * The Namespace block is organized using a simple hashstructure over the first
 * two characters. Better structures can be introduced when searching becomes
 * too expensive. An alternative would be to use a BAT to handle the collection.
 */
#include "monetdb_config.h"
#include "mal_type.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#define MAXIDENTIFIERS 4096

#define HASHMASK  4095
#define NMEHASH(X,L)  (L > 1 ?( ((*X) ^ ((*(X+1)) << 7)) & HASHMASK): (*X))
//#define NMEHASH(X,L)  (*X)

typedef struct NAMESPACE{
	int  size;  /* amount of space available */
	int  nmetop;
	str  *nme;
	int  *link;
	size_t	 *length;
} Namespace;

static Namespace namespace;

static void expandNamespace(void){
	str *nme;
	size_t *length;
	int *link, newsize, incr = 2048;

	newsize= namespace.size + incr;
	nme= (str *) GDKmalloc(sizeof(str *) * newsize);
	assert(nme != NULL); /* we cannot continue */
	link= (int *) GDKmalloc(sizeof(int) * newsize);
	assert(link != NULL); /* we cannot continue */
	length = (size_t *) GDKmalloc(sizeof(size_t) * newsize);
	assert(length != NULL); /* we cannot continue */

	memcpy(nme, namespace.nme, sizeof(str *) * namespace.nmetop);
	memcpy(link, namespace.link, sizeof(int) * namespace.nmetop);
	memcpy(length, namespace.length, sizeof(size_t) * namespace.nmetop);

	namespace.size += incr;
	GDKfree(namespace.nme); namespace.nme= nme;
	GDKfree(namespace.link); namespace.link= link;
	GDKfree(namespace.length); namespace.length= length;
}

void initNamespace(void) {
	assert(namespace.nme == NULL);
	assert(namespace.link == NULL);
	assert(namespace.length == NULL);
	namespace.nme= (str *) GDKzalloc(sizeof(str *) * MAXIDENTIFIERS);
	namespace.link= (int *) GDKzalloc(sizeof(int) * MAXIDENTIFIERS);
	namespace.length= (size_t *) GDKzalloc(sizeof(size_t) * MAXIDENTIFIERS);
	if ( namespace.nme == NULL ||
		 namespace.link == NULL ||
		 namespace.length == NULL) {
		/* absolute an error we can not recover from */
		showException(GDKout, MAL,"initNamespace",MAL_MALLOC_FAIL);
		mal_exit();
	}
	namespace.size = MAXIDENTIFIERS;
	namespace.nmetop= HASHMASK; /* hash overflow */
}
void finishNamespace(void) {
	int i;

	MT_lock_set(&mal_contextLock, "putName");
	for(i=0;i<namespace.nmetop; i++) {
		if( namespace.nme[i])
			GDKfree(namespace.nme[i]);
		namespace.nme[i]= 0;
	}
	GDKfree(namespace.nme); namespace.nme= 0;
	GDKfree(namespace.link); namespace.link= 0;
	GDKfree(namespace.length); namespace.length= 0;
	MT_lock_unset(&mal_contextLock, "putName");
}

/*
 * Before a name is being stored we should check for its occurrence first.
 * The administration is initialized incrementally.
 * Beware, the routine getName is not thread safe under updates
 * of the namespace itself.
 */
str getName(str nme, size_t len)
{
	size_t l;
	if(len == 0 || nme== NULL || *nme==0) return 0;

	MT_lock_set(&mal_contextLock, "putName");
	for(l= NMEHASH(nme,len); l && namespace.nme[l]; l= namespace.link[l]){
		if (namespace.length[l] == len  &&
			strncmp(nme,namespace.nme[l],len)==0) {
			MT_lock_unset(&mal_contextLock, "putName");
			return namespace.nme[l];
	    }
	}
	MT_lock_unset(&mal_contextLock, "putName");
	return 0;
}
/*
 * Name deletion from the namespace is tricky, because there may
 * be multiple threads active on the structure. Moreover, the
 * symbol may be picked up by a concurrent thread and stored
 * somewhere.
 * To avoid all these problems, the namespace should become
 * private to each Client, but this would mean expensive look ups
 * deep into the kernel to access the context.
 */
void delName(str nme, size_t len){
	str n;
	n= getName(nme,len);
	if( nme[0]==0 || n == 0) return ;

	/*Namespace garbage collection not available yet 
	MT_lock_set(&mal_contextLock, "putName");
	MT_lock_unset(&mal_contextLock, "putName");
	*/
}
str putName(str nme, size_t len)
{
	size_t l,top;
	char buf[MAXIDENTLEN];

	if( nme == NULL || len == 0)
		return NULL;
	/* protect this, as it will be updated by multiple threads */
	MT_lock_set(&mal_contextLock, "putName");
	for(l= NMEHASH(nme,len); l && namespace.nme[l]; l= namespace.link[l]){
	    if( namespace.length[l] == len  &&
			strncmp(nme,namespace.nme[l],len) == 0 ) {
			MT_lock_unset(&mal_contextLock, "putName");
			return namespace.nme[l];
	    }
	}

	if(len>=MAXIDENTLEN)
		len = MAXIDENTLEN - 1;
	memcpy(buf, nme, len);
	buf[len]=0;

	if( namespace.nmetop+1== namespace.size)
	    expandNamespace();
	l= NMEHASH(nme,len);
	top= namespace.nme[l]== 0? (int)l: namespace.nmetop;
	namespace.nme[top]= GDKstrdup(buf);
	namespace.link[top]= namespace.link[l];
	if ((int)top == namespace.nmetop)
		namespace.link[l] = (int)top;
	namespace.length[top]= len;
	namespace.nmetop++;
	MT_lock_unset(&mal_contextLock, "putName");
	return putName(nme, len);	/* just to be sure */
}

/* dummy function to maintain ABI compatibility */
void dumpNamespaceStatistics(stream *f, int details)
{
	(void) f;
	(void) details;
}
