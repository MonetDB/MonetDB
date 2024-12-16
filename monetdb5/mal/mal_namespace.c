/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * (author) M.L. Kersten
 */
#include "monetdb_config.h"
#include "mal_type.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_private.h"

#define MAXIDENTIFIERS 4096
#define HASHMASK  4095

MT_Lock mal_namespaceLock = MT_LOCK_INITIALIZER(mal_namespaceLock);

/* taken from gdk_atoms */
__attribute__((__pure__))
static inline size_t
nme_hash(const char *key, size_t len)
{
	size_t y = 0;

	for (size_t i = 0; i < len && key[i]; i++) {
		y += key[i];
		y += (y << 10);
		y ^= (y >> 6);
	}
	y += (y << 3);
	y ^= (y >> 11);
	y += (y << 15);
	return y & HASHMASK;
}

typedef struct NAME {
	struct NAME *next;
	char nme[IDLENGTH + 1];
} *NamePtr;

static NamePtr hash[MAXIDENTIFIERS];

static struct namespace {
	struct namespace *next;
	int count;
	struct NAME data[4096];
} namespace1, *namespace = &namespace1;

struct fixname {
	struct fixname *next;
	const char *name;
};
static struct fixnamespace {
	int count;
	struct fixname data[1024];
} fixnamespace;
static struct fixname *fixhash[4096];

static void
fixName(const char *name)
{
	size_t key = nme_hash(name, 1024 /* something large */);
	MT_lock_set(&mal_namespaceLock);
	struct fixname **n;
	for (n = &fixhash[key]; *n; n = &(*n)->next) {
		if ((*n)->name == name || strcmp((*n)->name, name) == 0) {
			/* name is already there; this can happen when
			 * reinitializing */
			MT_lock_unset(&mal_namespaceLock);
			return;
		}
	}
	assert(fixnamespace.count < 1024);
	struct fixname *new = &fixnamespace.data[fixnamespace.count++];
	*new = (struct fixname) {
		.name = name,
	};
	*n = new;
	MT_lock_unset(&mal_namespaceLock);
}

/* ! please keep this list sorted for easier maintenance ! */
const char affectedRowsRef[] = "affectedRows";
const char aggrRef[] = "aggr";
const char alarmRef[] = "alarm";
const char algebraRef[] = "algebra";
const char alter_add_range_partitionRef[] = "alter_add_range_partition";
const char alter_add_tableRef[] = "alter_add_table";
const char alter_add_value_partitionRef[] = "alter_add_value_partition";
const char alter_del_tableRef[] = "alter_del_table";
const char alter_seqRef[] = "alter_seq";
const char alter_set_tableRef[] = "alter_set_table";
const char alter_tableRef[] = "alter_table";
const char alter_userRef[] = "alter_user";
const char appendBulkRef[] = "appendBulk";
const char appendRef[] = "append";
const char assertRef[] = "assert";
const char avgRef[] = "avg";
const char bandjoinRef[] = "bandjoin";
const char batalgebraRef[] = "batalgebra";
const char batcalcRef[] = "batcalc";
const char batcapiRef[] = "batcapi";
const char batmalRef[] = "batmal";
const char batmkeyRef[] = "batmkey";
const char batmmathRef[] = "batmmath";
const char batmtimeRef[] = "batmtime";
const char batpyapi3Ref[] = "batpyapi3";
const char batrapiRef[] = "batrapi";
const char batRef[] = "bat";
const char batsqlRef[] = "batsql";
const char batstrRef[] = "batstr";
const char bbpRef[] = "bbp";
const char betweenRef[] = "between";
const char binddbatRef[] = "bind_dbat";
const char bindidxRef[] = "bind_idxbat";
const char bindRef[] = "bind";
const char blockRef[] = "block";
const char bstreamRef[] = "bstream";
const char calcRef[] = "calc";
const char capiRef[] = "capi";
const char claimRef[] = "claim";
const char clear_tableRef[] = "clear_table";
const char columnBindRef[] = "columnBind";
const char comment_onRef[] = "comment_on";
const char compressRef[] = "compress";
const char connectRef[] = "connect";
const char containsRef[] = "contains";
const char copy_fromRef[] = "copy_from";
const char corrRef[] = "corr";
const char count_no_nilRef[] = "count_no_nil";
const char countRef[] = "count";
const char create_functionRef[] = "create_function";
const char create_roleRef[] = "create_role";
const char create_schemaRef[] = "create_schema";
const char create_seqRef[] = "create_seq";
const char create_tableRef[] = "create_table";
const char create_triggerRef[] = "create_trigger";
const char create_typeRef[] = "create_type";
const char create_userRef[] = "create_user";
const char create_viewRef[] = "create_view";
const char crossRef[] = "crossproduct";
const char cume_distRef[] = "cume_dist";
const char dataflowRef[] = "dataflow";
const char dblRef[] = "dbl";
const char decompressRef[] = "decompress";
const char defineRef[] = "define";
const char deleteRef[] = "delete";
const char deltaRef[] = "delta";
const char dense_rankRef[] = "dense_rank";
const char dependRef[] = "depend";
const char deregisterRef[] = "deregister";
const char dictRef[] = "dict";
const char diffcandRef[] = "diffcand";
const char differenceRef[] = "difference";
const char disconnectRef[] = "disconnect";
const char divRef[] = "/";
const char drop_constraintRef[] = "drop_constraint";
const char drop_functionRef[] = "drop_function";
const char drop_indexRef[] = "drop_index";
const char drop_roleRef[] = "drop_role";
const char drop_schemaRef[] = "drop_schema";
const char drop_seqRef[] = "drop_seq";
const char drop_tableRef[] = "drop_table";
const char drop_triggerRef[] = "drop_trigger";
const char drop_typeRef[] = "drop_type";
const char drop_userRef[] = "drop_user";
const char drop_viewRef[] = "drop_view";
const char emptybindidxRef[] = "emptybindidx";
const char emptybindRef[] = "emptybind";
const char endswithjoinRef[] = "endswithjoin";
const char eqRef[] = "==";
const char evalRef[] = "eval";
const char execRef[] = "exec";
const char export_bin_columnRef[] = "export_bin_column";
const char exportOperationRef[] = "exportOperation";
const char export_tableRef[] = "export_table";
const char fetchRef[] = "fetch";
const char findRef[] = "find";
const char firstnRef[] = "firstn";
const char first_valueRef[] = "first_value";
const char forRef[] = "for";
const char generatorRef[] = "generator";
const char getRef[] = "get";
const char getTraceRef[] = "getTrace";
const char getVariableRef[] = "getVariable";
const char grant_functionRef[] = "grant_function";
const char grantRef[] = "grant";
const char grant_rolesRef[] = "grant_roles";
const char groupbyRef[] = "groupby";
const char groupdoneRef[] = "groupdone";
const char groupRef[] = "group";
const char growRef[] = "grow";
const char hgeRef[] = "hge";
const char identityRef[] = "identity";
const char ifthenelseRef[] = "ifthenelse";
const char importColumnRef[] = "importColumn";
const char intersectcandRef[] = "intersectcand";
const char intersectRef[] = "intersect";
const char intRef[] = "int";
const char ioRef[] = "io";
const char iteratorRef[] = "iterator";
const char joinRef[] = "join";
const char jsonRef[] = "json";
const char lagRef[] = "lag";
const char languageRef[] = "language";
const char last_valueRef[] = "last_value";
const char leadRef[] = "lead";
const char leftjoinRef[] = "leftjoin";
const char likejoinRef[] = "likejoin";
const char likeRef[] = "like";
const char likeselectRef[] = "likeselect";
const char lngRef[] = "lng";
const char lockRef[] = "lock";
const char lookupRef[] = "lookup";
const char mainRef[] = "main";
const char malRef[] = "mal";
const char manifoldRef[] = "manifold";
const char mapiRef[] = "mapi";
const char markjoinRef[] = "markjoin";
const char markselectRef[] = "markselect";
const char maskRef[] = "mask";
const char matRef[] = "mat";
const char maxlevenshteinRef[] = "maxlevenshtein";
const char maxRef[] = "max";
const char mdbRef[] = "mdb";
const char mergecandRef[] = "mergecand";
const char mergepackRef[] = "mergepack";
const char mergetableRef[] = "mergetable";
const char minjarowinklerRef[] = "minjarowinkler";
const char minRef[] = "min";
const char minusRef[] = "-";
const char mirrorRef[] = "mirror";
const char mitosisRef[] = "mitosis";
const char mmathRef[] = "mmath";
const char modRef[] = "%";
const char mtimeRef[] = "mtime";
const char mulRef[] = "*";
const char multiplexRef[] = "multiplex";
const char mvcRef[] = "mvc";
const char newRef[] = "new";
const char nextRef[] = "next";
const char not_likeRef[] = "not_like";
const char notRef[] = "not";
const char not_uniqueRef[] = "not_unique";
const char nth_valueRef[] = "nth_value";
const char ntileRef[] = "ntile";
const char optimizerRef[] = "optimizer";
const char outercrossRef[] = "outercrossproduct";
const char outerjoinRef[] = "outerjoin";
const char outerselectRef[] = "outerselect";
const char packIncrementRef[] = "packIncrement";
const char packRef[] = "pack";
const char parametersRef[] = "parameters";
const char passRef[] = "pass";
const char percent_rankRef[] = "percent_rank";
const char plusRef[] = "+";
const char predicateRef[] = "predicate";
const char printRef[] = "print";
const char prodRef[] = "prod";
const char profilerRef[] = "profiler";
const char projectdeltaRef[] = "projectdelta";
const char projectionpathRef[] = "projectionpath";
const char projectionRef[] = "projection";
const char projectRef[] = "project";
const char putRef[] = "put";
const char pyapi3Ref[] = "pyapi3";
const char querylogRef[] = "querylog";
const char raiseRef[] = "raise";
const char rangejoinRef[] = "rangejoin";
const char rankRef[] = "rank";
const char rapiRef[] = "rapi";
const char reconnectRef[] = "reconnect";
const char registerRef[] = "register";
const char register_supervisorRef[] = "register_supervisor";
const char remapRef[] = "remap";
const char remoteRef[] = "remote";
const char rename_columnRef[] = "rename_column";
const char rename_schemaRef[] = "rename_schema";
const char rename_tableRef[] = "rename_table";
const char rename_userRef[] = "rename_user";
const char renumberRef[] = "renumber";
const char replaceRef[] = "replace";
const char resultSetRef[] = "resultSet";
const char revoke_functionRef[] = "revoke_function";
const char revokeRef[] = "revoke";
const char revoke_rolesRef[] = "revoke_roles";
const char row_numberRef[] = "row_number";
const char rpcRef[] = "rpc";
const char rsColumnRef[] = "rsColumn";
const char rtreeRef[] = "rtree";
const char sampleRef[] = "sample";
const char selectNotNilRef[] = "selectNotNil";
const char selectRef[] = "select";
const char semaRef[] = "sema";
const char semijoinRef[] = "semijoin";
const char seriesRef[] = "series";
const char setAccessRef[] = "setAccess";
const char set_protocolRef[] = "set_protocol";
const char setVariableRef[] = "setVariable";
const char singleRef[] = "single";
const char sliceRef[] = "slice";
const char sortRef[] = "sort";
const char sqlcatalogRef[] = "sqlcatalog";
const char sqlRef[] = "sql";
const char startswithjoinRef[] = "startswithjoin";
const char stoptraceRef[] = "stoptrace";
const char streamsRef[] = "streams";
const char strimpsRef[] = "strimps";
const char strRef[] = "str";
const char subavgRef[] = "subavg";
const char subcountRef[] = "subcount";
const char subdeltaRef[] = "subdelta";
const char subeval_aggrRef[] = "subeval_aggr";
const char subgroupdoneRef[] = "subgroupdone";
const char subgroupRef[] = "subgroup";
const char submaxRef[] = "submax";
const char subminRef[] = "submin";
const char subprodRef[] = "subprod";
const char subsliceRef[] = "subslice";
const char subsumRef[] = "subsum";
const char subuniformRef[] = "subuniform";
const char sumRef[] = "sum";
const char takeRef[] = "take";
const char thetajoinRef[] = "thetajoin";
const char thetaselectRef[] = "thetaselect";
const char tidRef[] = "tid";
const char totalRef[] = "total";
const char transaction_abortRef[] = "transaction_abort";
const char transaction_beginRef[] = "transaction_begin";
const char transaction_commitRef[] = "transaction_commit";
const char transactionRef[] = "transaction";
const char transaction_releaseRef[] = "transaction_release";
const char transaction_rollbackRef[] = "transaction_rollback";
const char umaskRef[] = "umask";
const char unionfuncRef[] = "unionfunc";
const char uniqueRef[] = "unique";
const char unlockRef[] = "unlock";
const char updateRef[] = "update";
const char userRef[] = "user";
const char window_boundRef[] = "window_bound";
const char zero_or_oneRef[] = "zero_or_one";
/* ! please keep this list sorted for easier maintenance ! */

void
initNamespace(void)
{
/* ! please keep this list sorted for easier maintenance ! */
	fixName(affectedRowsRef);
	fixName(aggrRef);
	fixName(alarmRef);
	fixName(algebraRef);
	fixName(alter_add_range_partitionRef);
	fixName(alter_add_tableRef);
	fixName(alter_add_value_partitionRef);
	fixName(alter_del_tableRef);
	fixName(alter_seqRef);
	fixName(alter_set_tableRef);
	fixName(alter_tableRef);
	fixName(alter_userRef);
	fixName(appendBulkRef);
	fixName(appendRef);
	fixName(assertRef);
	fixName(avgRef);
	fixName(bandjoinRef);
	fixName(batalgebraRef);
	fixName(batcalcRef);
	fixName(batcapiRef);
	fixName(batmalRef);
	fixName(batmkeyRef);
	fixName(batmmathRef);
	fixName(batmtimeRef);
	fixName(batpyapi3Ref);
	fixName(batrapiRef);
	fixName(batRef);
	fixName(batsqlRef);
	fixName(batstrRef);
	fixName(bbpRef);
	fixName(betweenRef);
	fixName(binddbatRef);
	fixName(bindidxRef);
	fixName(bindRef);
	fixName(blockRef);
	fixName(bstreamRef);
	fixName(calcRef);
	fixName(capiRef);
	fixName(claimRef);
	fixName(clear_tableRef);
	fixName(columnBindRef);
	fixName(comment_onRef);
	fixName(compressRef);
	fixName(connectRef);
	fixName(containsRef);
	fixName(copy_fromRef);
	fixName(corrRef);
	fixName(count_no_nilRef);
	fixName(countRef);
	fixName(create_functionRef);
	fixName(create_roleRef);
	fixName(create_schemaRef);
	fixName(create_seqRef);
	fixName(create_tableRef);
	fixName(create_triggerRef);
	fixName(create_typeRef);
	fixName(create_userRef);
	fixName(create_viewRef);
	fixName(crossRef);
	fixName(cume_distRef);
	fixName(dataflowRef);
	fixName(dblRef);
	fixName(decompressRef);
	fixName(defineRef);
	fixName(deleteRef);
	fixName(deltaRef);
	fixName(dense_rankRef);
	fixName(dependRef);
	fixName(deregisterRef);
	fixName(dictRef);
	fixName(diffcandRef);
	fixName(differenceRef);
	fixName(disconnectRef);
	fixName(divRef);
	fixName(drop_constraintRef);
	fixName(drop_functionRef);
	fixName(drop_indexRef);
	fixName(drop_roleRef);
	fixName(drop_schemaRef);
	fixName(drop_seqRef);
	fixName(drop_tableRef);
	fixName(drop_triggerRef);
	fixName(drop_typeRef);
	fixName(drop_userRef);
	fixName(drop_viewRef);
	fixName(emptybindidxRef);
	fixName(emptybindRef);
	fixName(endswithjoinRef);
	fixName(eqRef);
	fixName(evalRef);
	fixName(execRef);
	fixName(export_bin_columnRef);
	fixName(exportOperationRef);
	fixName(export_tableRef);
	fixName(fetchRef);
	fixName(findRef);
	fixName(firstnRef);
	fixName(first_valueRef);
	fixName(forRef);
	fixName(generatorRef);
	fixName(getRef);
	fixName(getTraceRef);
	fixName(getVariableRef);
	fixName(grant_functionRef);
	fixName(grantRef);
	fixName(grant_rolesRef);
	fixName(groupbyRef);
	fixName(groupdoneRef);
	fixName(groupRef);
	fixName(growRef);
	fixName(hgeRef);
	fixName(identityRef);
	fixName(ifthenelseRef);
	fixName(importColumnRef);
	fixName(intersectcandRef);
	fixName(intersectRef);
	fixName(intRef);
	fixName(ioRef);
	fixName(iteratorRef);
	fixName(joinRef);
	fixName(jsonRef);
	fixName(lagRef);
	fixName(languageRef);
	fixName(last_valueRef);
	fixName(leadRef);
	fixName(leftjoinRef);
	fixName(likejoinRef);
	fixName(likeRef);
	fixName(likeselectRef);
	fixName(lngRef);
	fixName(lockRef);
	fixName(lookupRef);
	fixName(mainRef);
	fixName(malRef);
	fixName(manifoldRef);
	fixName(mapiRef);
	fixName(markjoinRef);
	fixName(markselectRef);
	fixName(maskRef);
	fixName(matRef);
	fixName(maxlevenshteinRef);
	fixName(maxRef);
	fixName(mdbRef);
	fixName(mergecandRef);
	fixName(mergepackRef);
	fixName(mergetableRef);
	fixName(minjarowinklerRef);
	fixName(minRef);
	fixName(minusRef);
	fixName(mirrorRef);
	fixName(mitosisRef);
	fixName(mmathRef);
	fixName(modRef);
	fixName(mtimeRef);
	fixName(mulRef);
	fixName(multiplexRef);
	fixName(mvcRef);
	fixName(newRef);
	fixName(nextRef);
	fixName(not_likeRef);
	fixName(notRef);
	fixName(not_uniqueRef);
	fixName(nth_valueRef);
	fixName(ntileRef);
	fixName(optimizerRef);
	fixName(outercrossRef);
	fixName(outerjoinRef);
	fixName(outerselectRef);
	fixName(packIncrementRef);
	fixName(packRef);
	fixName(parametersRef);
	fixName(passRef);
	fixName(percent_rankRef);
	fixName(plusRef);
	fixName(predicateRef);
	fixName(printRef);
	fixName(prodRef);
	fixName(profilerRef);
	fixName(projectdeltaRef);
	fixName(projectionpathRef);
	fixName(projectionRef);
	fixName(projectRef);
	fixName(putRef);
	fixName(pyapi3Ref);
	fixName(querylogRef);
	fixName(raiseRef);
	fixName(rangejoinRef);
	fixName(rankRef);
	fixName(rapiRef);
	fixName(reconnectRef);
	fixName(registerRef);
	fixName(register_supervisorRef);
	fixName(remapRef);
	fixName(remoteRef);
	fixName(rename_columnRef);
	fixName(rename_schemaRef);
	fixName(rename_tableRef);
	fixName(rename_userRef);
	fixName(renumberRef);
	fixName(replaceRef);
	fixName(resultSetRef);
	fixName(revoke_functionRef);
	fixName(revokeRef);
	fixName(revoke_rolesRef);
	fixName(row_numberRef);
	fixName(rpcRef);
	fixName(rsColumnRef);
	fixName(rtreeRef);
	fixName(sampleRef);
	fixName(selectNotNilRef);
	fixName(selectRef);
	fixName(semaRef);
	fixName(semijoinRef);
	fixName(seriesRef);
	fixName(setAccessRef);
	fixName(set_protocolRef);
	fixName(setVariableRef);
	fixName(singleRef);
	fixName(sliceRef);
	fixName(sortRef);
	fixName(sqlcatalogRef);
	fixName(sqlRef);
	fixName(startswithjoinRef);
	fixName(stoptraceRef);
	fixName(streamsRef);
	fixName(strimpsRef);
	fixName(strRef);
	fixName(subavgRef);
	fixName(subcountRef);
	fixName(subdeltaRef);
	fixName(subeval_aggrRef);
	fixName(subgroupdoneRef);
	fixName(subgroupRef);
	fixName(submaxRef);
	fixName(subminRef);
	fixName(subprodRef);
	fixName(subsliceRef);
	fixName(subsumRef);
	fixName(subuniformRef);
	fixName(sumRef);
	fixName(takeRef);
	fixName(thetajoinRef);
	fixName(thetaselectRef);
	fixName(tidRef);
	fixName(totalRef);
	fixName(transaction_abortRef);
	fixName(transaction_beginRef);
	fixName(transaction_commitRef);
	fixName(transactionRef);
	fixName(transaction_releaseRef);
	fixName(transaction_rollbackRef);
	fixName(umaskRef);
	fixName(unionfuncRef);
	fixName(uniqueRef);
	fixName(unlockRef);
	fixName(updateRef);
	fixName(userRef);
	fixName(window_boundRef);
	fixName(zero_or_oneRef);
/* ! please keep this list sorted for easier maintenance ! */
}

void
mal_namespace_reset(void)
{
	struct namespace *ns;

	/* assume we are at the end of the server session */
	MT_lock_set(&mal_namespaceLock);
	memset(hash, 0, sizeof(hash));
	while (namespace) {
		ns = namespace->next;
		if (namespace != &namespace1)
			GDKfree(namespace);
		namespace = ns;
	}
	namespace1 = (struct namespace) {
		.count = 0,
	};
	namespace = &namespace1;
	MT_lock_unset(&mal_namespaceLock);
}

static const char *
findName(const char *nme, size_t len, bool allocate)
{
	NamePtr *n, m;
	size_t key;

	assert(len == 0 || nme != NULL);
	if (len == 0 || nme == NULL)
		return NULL;
	if (len > IDLENGTH) {
		len = IDLENGTH;
	}
	key = nme_hash(nme, len);
	MT_lock_set(&mal_namespaceLock);
	for (struct fixname *p = fixhash[key]; p; p = p->next) {
		if (p->name == nme || (strncmp(p->name, nme, len) == 0 && p->name[len] == 0)) {
			MT_lock_unset(&mal_namespaceLock);
			return p->name;
		}
	}
	for (n = &hash[key]; *n; n = &(*n)->next) {
		if (strncmp(nme, (*n)->nme, len) == 0 && (*n)->nme[len] == 0) {
			MT_lock_unset(&mal_namespaceLock);
			return (*n)->nme;
		}
	}
	/* item not found */
	if (!allocate) {
		MT_lock_unset(&mal_namespaceLock);
		return NULL;
	}
	if (namespace == NULL || namespace->count == 4096) {
		struct namespace *ns = GDKmalloc(sizeof(struct namespace));
		if (ns == NULL) {
			MT_lock_unset(&mal_namespaceLock);
			return NULL;
		}
		ns->next = namespace;
		ns->count = 0;
		namespace = ns;
	}
	m = &namespace->data[namespace->count++];
	assert(m->nme != nme);
	strncpy(m->nme, nme, len);
	m->nme[len] = 0;
	m->next = *n;
	*n = m;
	MT_lock_unset(&mal_namespaceLock);
	return m->nme;
}

const char *
getName(const char *nme)
{
	if (nme != NULL)
		nme = findName(nme, strlen(nme), false);
	return nme;
}

const char *
getNameLen(const char *nme, size_t len)
{
	return findName(nme, len, false);
}

const char *
putName(const char *nme)
{
	if (nme != NULL)
		nme = findName(nme, strlen(nme), true);
	return nme;
}

const char *
putNameLen(const char *nme, size_t len)
{
	return findName(nme, len, true);
}
