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

/*****************************************************************
 *  Title: tpcd.h for TPC D
 *  Sccsid: @(#)tpcd.h	2.1.8.1 
 *  Description:
 *  X
 *
 *****************************************************************
 */
#define DFLT            0x0001
#define OUTPUT          0x0002
#define EXPLAIN         0x0004
#define DBASE           0x0008
#define VERBOSE         0x0010
#define TIMING          0x0020
#define LOG             0x0040
#define QUERY           0x0080
#define REFRESH         0x0100
#define ANSI            0x0200
#define SEED            0x0400
#define COMMENT         0x0800
#define INIT            0x1000
#define TERMINATE       0x2000
#define DFLT_NUM        0x4000

/*
 * general defines
 */
#define VTAG            ':'          /* flags a variable substitution */
#define ofp             stdout       /* make the routine a filter */
#define QDIR_TAG        "DSS_QUERY"  /* variable to point to queries */
#define QDIR_DFLT       "."          /* and its default */

/*
 * database portability defines
 */
#ifdef DB2
#define GEN_QUERY_PLAN  "SET CURRENT EXPLAIN SNAPSHOT ON;"
#define START_TRAN      ""
#define END_TRAN        "COMMIT WORK;"
#define SET_OUTPUT      ""
#define SET_ROWCOUNT    "--#SET ROWS_FETCH %d\n"
#define SET_DBASE       "CONNECT TO %s ;\n"
#endif

#ifdef INFORMIX
#define GEN_QUERY_PLAN  "SET EXPLAIN ON;"
#define START_TRAN      "BEGIN WORK;"
#define END_TRAN        "COMMIT WORK;"
#define SET_OUTPUT      "OUTPUT TO "
#define SET_ROWCOUNT    "FIRST %d"
#define SET_DBASE       "database %s ;\n"
#endif

#ifdef 	SQLSERVER
#define GEN_QUERY_PLAN  "set showplan on\nset noexec on\ngo\n"
#define START_TRAN      "begin transaction\ngo\n"
#define END_TRAN        "commit transaction\ngo\n"
#define SET_OUTPUT      ""
#define SET_ROWCOUNT    "set rowcount %d\ngo\n\n"
#define SET_DBASE       "use %s\ngo\n"
#endif

#ifdef 	SYBASE
#define GEN_QUERY_PLAN  "set showplan on\nset noexec on\ngo\n"
#define START_TRAN      "begin transaction\ngo\n"
#define END_TRAN        "commit transaction\ngo\n"
#define SET_OUTPUT      ""
#define SET_ROWCOUNT    "set rowcount %d\ngo\n\n"
#define SET_DBASE       "use %s\ngo\n"
#endif

#ifdef 	MONET
#define GEN_QUERY_PLAN  ""
#define START_TRAN      ""
#define END_TRAN        ""
#define SET_OUTPUT      ""
#define SET_ROWCOUNT    ""
#define SET_DBASE       ""
#endif

#ifdef TDAT
#define GEN_QUERY_PLAN  "EXPLAIN"
#define START_TRAN      "BEGIN TRANSACTION"
#define END_TRAN        "END TRANSACTION"
#define SET_OUTPUT      ".SET FORMAT OFF\n.EXPORT REPORT file="
#define SET_ROWCOUNT    ".SET RETCANCEL ON\n.SET RETLIMIT %d\n"
#define SET_DBASE       ".LOGON %s\n"
#endif

#define MAX_VARS      8 /* max number of host vars in any query */
#define QLEN_MAX   2048 /* max length of any query */
#define QUERIES_PER_SET 22
#define MAX_PIDS 50

EXTERN int flags;
EXTERN int s_cnt;
EXTERN char *osuff;
EXTERN int stream;
EXTERN char *lfile;
EXTERN char *ifile;
EXTERN char *tfile;

#define MAX_PERMUTE     41
#ifdef DECLARER
int rowcnt_dflt[QUERIES_PER_SET + 1] = 
    {-1,-1,100,10,-1,-1,-1,-1,-1,-1,20,-1,-1,-1,-1,-1,-1,-1,100,-1,-1,100,-1};
int rowcnt;
#define SEQUENCE(stream, query) permutation[stream % MAX_PERMUTE][query - 1]
#else
extern int rowcnt_dflt[];
extern int rowcnt;
#endif
