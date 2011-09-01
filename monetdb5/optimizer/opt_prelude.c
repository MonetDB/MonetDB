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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f opt_prelude
 * @a M. Kersten
 * These definitions are handy to have around in the optimizer
 */
#include "monetdb_config.h"
#include "opt_prelude.h"

str affectedRowsRef;
str aggrRef;
str alarmRef;
str algebraRef;
str appendidxRef;
str appendRef;
str assertRef;
str attachRef;
str avgRef;
str basketRef;
str batcalcRef;
str batRef;
str boxRef;
str batstrRef;
str batmtimeRef;
str batmmathRef;
str bbpRef;
str binddbatRef;
str bindidxRef;
str bindRef;
str bpmRef;
str bstreamRef;
str calcRef;
str catalogRef;
str centipedeRef;
str clear_tableRef;
str closeRef;
str compressRef;
str columnRef;
str columnBindRef;
str commitRef;
str connectRef;
str constraintsRef;
str countRef;
str copyRef;
str copy_fromRef;
str count_no_nilRef;
str crossRef;
str createRef;
str datacellRef;
str dataflowRef;
str datacyclotronRef;
str dblRef;
str decompressRef;
str deleteRef;
str depositRef;
str deriveRef;
str derivePathRef;
str differenceRef;
str divRef;
str disconnectRef;
str doneRef;
str evalRef;
str execRef;
str expandRef;
str exportOperationRef;
str finishRef;
str getRef;
str grabRef;
str groupRef;
str groupbyRef;
str hashRef;
str histogramRef;
str identityRef;
str ifthenelseRef;
str inplaceRef;
str insertRef;
str intRef;
str ioRef;
str joinRef;
str joinPathRef;
str bandjoinRef;
str thetajoinRef;
str thetauselectRef;
str thetaselectRef;
str kdifferenceRef;
str kunionRef;
str kuniqueRef;
str languageRef;
str leftjoinRef;
str leftjoinPathRef;
str likeselectRef;
str ilikeselectRef;
str likeuselectRef;
str ilikeuselectRef;
str likeRef;
str listRef;
str lockRef;
str lookupRef;
str malRef;
str mapiRef;
str markHRef;
str markTRef;
str mark_grpRef;
str materializeRef;
str mtimeRef;
str dense_rank_grpRef;
str materializeRef;
str matRef;
str max_no_nilRef;
str maxRef;
str mdbRef;
str min_no_nilRef;
str minRef;
str mirrorRef;
str mkeyRef;
str mmathRef;
str multiplexRef;
str mvcRef;
str newRef;
str notRef;
str oidRef;
str octopusRef;
str openRef;
str optimizerRef;
str packRef;
str pack2Ref;
str partitionRef;
str pcreRef;
str pinRef;
str plusRef;
str sqlplusRef;
str printRef;
str preludeRef;
str prodRef;
str postludeRef;
str pqueueRef;
str profilerRef;
str projectRef;
str putRef;
str queryRef;
str rank_grpRef;
str reconnectRef;
str recycleRef;
str refineRef;
str refine_reverseRef;
str registerRef;
str remapRef;
str remoteRef;
str replaceRef;
str replicatorRef;
str resultSetRef;
str reuseRef;
str reverseRef;
str rpcRef;
str rsColumnRef;
str schedulerRef;
str selectNotNilRef;
str selectRef;
str semaRef;
str semijoinRef;
str setAccessRef;
str setWriteModeRef;
str sliceRef;
str singleRef;
str sortHRef;
str sortHTRef;
str sortRef;
str sortReverseTailRef;
str sortTailRef;
str sortTHRef;
str sqlRef;
str srvpoolRef;
str streamsRef;
str startRef;
str stopRef;
str strRef;
str sumRef;
str sunionRef;
str takeRef;
str topn_minRef;
str topn_maxRef;
str utopn_minRef;
str utopn_maxRef;
str tuniqueRef;
str not_uniqueRef;
str unionRef;
str unlockRef;
str unpackRef;
str unpinRef;
str updateRef;
str uselectRef;
str userRef;
str vectorRef;
str antiuselectRef;
str antijoinRef;
str zero_or_oneRef;

int canBeCrackedProp;
int canBeJoinselectProp;
int sidewaysSelectProp;
int headProp;
int pivotProp;
int pivotDisjunctiveProp;
int removeProp;
int tableProp;
int sqlfunctionProp;

int inlineProp;
int keepProp;
int notnilProp;
int rowsProp;
int fileProp;
int runonceProp;
int singletonProp;
int unsafeProp;

int stableProp;
int insertionsProp;
int updatesProp;
int deletesProp;

int hlbProp;
int hubProp;
int tlbProp;
int tubProp;
int horiginProp;		/* original oid source */
int toriginProp;		/* original oid source */

void optimizerInit(void){
	if(batRef == NULL){
		affectedRowsRef = putName("affectedRows",12);
		aggrRef = putName("aggr",4);
		alarmRef = putName("alarm",5);
		algebraRef = putName("algebra",7);
		appendidxRef = putName("append_idxbat",13);
		appendRef = putName("append",6);
		assertRef = putName("assert",6);
		attachRef = putName("attach",6);
		avgRef = putName("avg",3);
		batcalcRef = putName("batcalc",7);
		basketRef = putName("basket",6);
		batRef = putName("bat",3);
		boxRef = putName("box",3);
		batstrRef = putName("batstr",6);
		batmtimeRef = putName("batmtime",8);
		batmmathRef = putName("batmmath",8);
		bbpRef = putName("bbp",3);
		binddbatRef = putName("bind_dbat",9);
		bindidxRef = putName("bind_idxbat",11);
		bindRef = putName("bind",4);
		bpmRef = putName("bpm",3);
		bstreamRef = putName("bstream",7);
		calcRef = putName("calc",4);
		catalogRef = putName("catalog",7);
		centipedeRef = putName("centipede",9);
		clear_tableRef = putName("clear_table",11);
		closeRef = putName("close",5);
		compressRef = putName("compress",8);
		columnRef = putName("column",6);
		columnBindRef = putName("columnBind",10);
		commitRef = putName("commit",6);
		connectRef = putName("connect",7);
		constraintsRef = putName("constraints",11);
		countRef = putName("count",5);
		copyRef = putName("copy",4);
		copy_fromRef = putName("copy_from",9);
		count_no_nilRef = putName("count_no_nil",12);
		crossRef = putName("cross",5);
		createRef = putName("create",6);
		datacellRef = putName("datacell",8);
		dataflowRef = putName("dataflow",8);
		datacyclotronRef = putName("datacyclotron",13);
		dblRef = putName("dbl",3);
		decompressRef = putName("decompress",10);
		deleteRef = putName("delete",6);
		depositRef = putName("deposit",7);
		deriveRef = putName("derive",6);
		derivePathRef = putName("derivePath",10);
		differenceRef= putName("difference",10);
		divRef = putName("/",1);
		disconnectRef= putName("disconnect",10);
		doneRef= putName("done",4);
		evalRef = putName("eval",4);
		execRef = putName("exec",4);
		expandRef = putName("expand",6);
		exportOperationRef = putName("exportOperation",15);
		finishRef = putName("finish",6);
		getRef = putName("get",3);
		grabRef = putName("grab",4);
		groupRef = putName("group",5);
		groupbyRef = putName("groupby",7);
		hashRef = putName("hash",4);
		histogramRef = putName("histogram",9);
		identityRef = putName("identity",8);
		ifthenelseRef = putName("ifthenelse",10);
		inplaceRef = putName("inplace",7);
		insertRef = putName("insert",6);
		intRef = putName("int",3);
		ioRef = putName("io",2);
		joinRef = putName("join",4);
		joinPathRef = putName("joinPath",8);
		bandjoinRef = putName("bandjoin",8);
		thetajoinRef = putName("thetajoin",9);
		thetauselectRef = putName("thetauselect",12);
		thetaselectRef = putName("thetaselect",11);
		kdifferenceRef= putName("kdifference",11);
		kunionRef= putName("kunion",6);
		kuniqueRef= putName("kunique",7);
		languageRef= putName("language",8);
		leftjoinRef = putName("leftjoin",8);
		leftjoinPathRef = putName("leftjoinPath",12);
		likeselectRef = putName("like_select",11);
		ilikeselectRef = putName("ilike_select",12);
		likeuselectRef = putName("like_uselect",12);
		ilikeuselectRef = putName("ilike_uselect",13);
		listRef = putName("list",4);
		likeRef = putName("like",4);
		lockRef = putName("lock",4);
		lookupRef = putName("lookup",6);
		malRef = putName("mal", 3);
		mapiRef = putName("mapi", 4);
		markHRef = putName("markH", 5);
		markTRef = putName("markT", 5);
		mark_grpRef = putName("mark_grp", 8);
		materializeRef = putName("materialize", 11);
		mtimeRef = putName("mtime", 5);
		dense_rank_grpRef = putName("dense_rank_grp", 14);
		materializeRef = putName("materialize", 11);
		matRef = putName("mat", 3);
		max_no_nilRef = putName("max_no_nil", 10);
		maxRef = putName("max", 3);
		mdbRef = putName("mdb", 3);
		min_no_nilRef = putName("min_no_nil", 10);
		minRef = putName("min", 3);
		mirrorRef = putName("mirror", 6);
		mkeyRef = putName("mkey", 4);
		mmathRef = putName("mmath", 5);
		multiplexRef = putName("multiplex", 9);
		mvcRef = putName("mvc", 3);
		newRef = putName("new",3);
		notRef = putName("not",3);
		oidRef = putName("oid",3);
		octopusRef = putName("octopus",7);
		optimizerRef = putName("optimizer",9);
		openRef = putName("open",4);
		packRef = putName("pack",4);
		pack2Ref = putName("pack2",5);
		partitionRef = putName("partition",9);
		pcreRef = putName("pcre",4);
		pinRef = putName("pin",3);
		plusRef = putName("+",1);
		sqlplusRef = putName("sqladd",6);
		printRef = putName("print",5);
		preludeRef = putName("prelude",7);
		prodRef = putName("product",7);
		profilerRef = putName("profiler",8);
		postludeRef = putName("postlude",8);
		pqueueRef = putName("pqueue",6);
		projectRef = putName("project",7);
		putRef = putName("put",3);
		queryRef = putName("query",5);
		rank_grpRef = putName("rank_grp", 8);
		reconnectRef = putName("reconnect",9);
		recycleRef = putName("recycle",7);
		refineRef = putName("refine",6);
		refine_reverseRef = putName("refine_reverse",14);
		registerRef = putName("register",8);
		remapRef = putName("remap",5);
		remoteRef = putName("remote",6);
		replaceRef = putName("replace",7);
		replicatorRef = putName("replicator",10);
		resultSetRef = putName("resultSet",9);
		reuseRef = putName("reuse",5);
		reverseRef = putName("reverse",7);
		rpcRef = putName("rpc",3);
		rsColumnRef = putName("rsColumn",8);
		schedulerRef = putName("scheduler",9);
		selectNotNilRef = putName("selectNotNil",12);
		selectRef = putName("select",6);
		semaRef = putName("sema",4);
		semijoinRef = putName("semijoin",8);
		setAccessRef = putName("setAccess",9);
		setWriteModeRef= putName("setWriteMode",12);
		sliceRef = putName("slice",5);
		singleRef = putName("single",6);
		sortHRef = putName("sortH",5);
		sortHTRef = putName("sortHT",6);
		sortRef = putName("sort",4);
		sortReverseTailRef = putName("sortReverseTail",15);
		sortTailRef = putName("sortTail",8);
		sortTHRef = putName("sortTH",6);
		sqlRef = putName("sql",3);
		srvpoolRef = putName("srvpool",7);
		streamsRef = putName("streams",7);
		startRef = putName("start",5);
		stopRef = putName("stop",4);
		strRef = putName("str",3);
		sumRef = putName("sum",3);
		sunionRef= putName("sunion",6);
		takeRef= putName("take",5);
		topn_minRef= putName("topn_min",8);
		topn_maxRef= putName("topn_max",8);
		utopn_minRef= putName("utopn_min",9);
		utopn_maxRef= putName("utopn_max",9);
		tuniqueRef = putName("tunique",7);
		not_uniqueRef= putName("not_unique",10);
		unionRef= putName("union",5);
		unlockRef= putName("unlock",6);
		unpackRef = putName("unpack",6);
		unpinRef = putName("unpin",5);
		updateRef = putName("update",6);
		uselectRef = putName("uselect",7);
		vectorRef = putName("vector",6);
		antiuselectRef = putName("antiuselect",11);
		antijoinRef = putName("antijoin",8);
		zero_or_oneRef = putName("zero_or_one",11);
		userRef = putName("user",4);

		canBeCrackedProp = PropertyIndex("canBeCracked");
		canBeJoinselectProp = PropertyIndex("canBeJoinselect");
		sidewaysSelectProp = PropertyIndex("sidewaysSelect");
		headProp = PropertyIndex("head");
		pivotProp = PropertyIndex("pivot");
		pivotDisjunctiveProp = PropertyIndex("pivotDisjunctive");
		removeProp = PropertyIndex("remove");
		tableProp = PropertyIndex("table");

		fileProp = PropertyIndex("file");
		inlineProp = PropertyIndex("inline");
		keepProp = PropertyIndex("keep");
		notnilProp = PropertyIndex("notnil");
		rowsProp = PropertyIndex("rows");
		runonceProp = PropertyIndex("runonce");
		singletonProp = PropertyIndex("singleton");
		unsafeProp = PropertyIndex("unsafe");
		sqlfunctionProp = PropertyIndex("sqlfunction");

		stableProp = PropertyIndex("stableProp");
		insertionsProp = PropertyIndex("insertionsProp");
		updatesProp = PropertyIndex("updatesProp");
		deletesProp = PropertyIndex("deletesProp");

		hlbProp = PropertyIndex("hlb");
		hubProp = PropertyIndex("hub");
		tlbProp = PropertyIndex("tlb");
		tubProp = PropertyIndex("tub");

		horiginProp = PropertyIndex("horigin");
		toriginProp = PropertyIndex("torigin");
		/*
		 * @-
		 * Set the optimizer debugging flag
		 */
		{
			int ret;
			str ref= GDKgetenv("opt_debug");
			if ( ref)
				OPTsetDebugStr(&ret,&ref);
		}
	}
}
