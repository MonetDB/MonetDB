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
 * @f tablet
 *
 */
/*
 * @a Niels Nes, Martin Kersten
 * @d 29/07/2003
 * @+ The table interface
 *
 * A database cannot live without ASCII tabular print/dump/load operations.
 * It is needed to produce reasonable listings, to exchange answers
 * with a client, and to keep a database version for backup.
 * This is precisely where the tablet module comes in handy.
 * [This module should replace all other table dump/load functions]
 *
 * We start with a simple example to illustrate the plain ASCII
 * representation and the features provided. Consider the
 * relational table answer(name:str, age:int, sex:chr, address:str, dob:date)
 * obtained by calling the routine tablet.page(B1,...,Bn) where the Bi represent
 * BATS.
 * @verbatim
 * [ "John Doe",		25,	'M',	"Parklane 5",	"25-12-1978" ]
 * [ "Maril Streep",	23,	'F',	"Church 5",	"12-07-1980" ]
 * [ "Mr. Smith",		53,	'M',	"Church 1",	"03-01-1950" ]
 * @end verbatim
 * @-
 * The lines contain the representation of a list in Monet tuple format.
 * This format has been chosen to ease parsing by any front-end. The scalar values
 * are represented according to their type. For visual display, the columns
 * are aligned by placing enough tabs between columns based on sampling the
 * underlying bat to determine a maximal column width.
 * (Note,actual commas are superfluous).
 *
 * The arguments to the command can be any sequence of BATs, but which are
 * assumed to be aligned. That is, they all should have the same number of
 * tuples and the j-th tuple tail of Bi is printed along-side the j-th tuple
 * tail of Bi+1.
 *
 * Printing both columns of a single bat is handled by tablet as a
 * print of two columns. This slight inconvenience is catch-ed by
 * the io.print(b) command, which resolves most back-ward compatibility issues.
 * @-
 * In many cases, this output would suffice for communication with a front-end.
 * However, for visual inspection the user should be provided also some meta
 * information derived from the database schema. Likewise, when reading a
 * table this information is needed to prepare a first approximation of
 * the schema namings. This information is produced by the command
 * tablet.header(B1,...,Bn), which lists the column role name.
 * If no role name is give, a default is generated based on the
 * BAT name, e.g. B1_tail.
 *
 * @verbatim
 * #------------------------------------------------------#
 * # name,           age, sex, address,       dob         #
 * #------------------------------------------------------#
 * [ "John Doe",      25, 'M', "Parklane 5", "25-12-1978" ]
 * [ "Maril Streep",  23, 'F', "Church 5",   "12-07-1980" ]
 * [ "Mr. Smith",     53, 'M', "Church 1",   "03-01-1950" ]
 * @end verbatim
 * @-
 *
 * The command tablet.display(B1,...,Bn) is a contraction of tablet.header();
 * tablet.page().
 *
 * In many cases, the @code{tablet} produced may be too long to consume completely
 * by the front end. In that case, the user needs page size control, much
 * like the more/less utilities under Linux. However, no guarantee
 * is given for arbitrarily going back and forth.
 * [but works as long as we materialize results first ].
 * A portion of the tablet can be printed by identifying the rows of interest as
 * the first parameter(s) in the page command, e.g.
 *
 *
 * @verbatim
 * tablet.page(nil,10,B1,...,Bn);	#prints first 10 rows
 * tablet.page(10,20,B1,...,Bn);	#prints next 10 rows
 * tablet.page(100,nil,B1,...,Bn);	#starts printing at tuple 100 until end
 * @end verbatim
 *
 * A paging system also provides the commands tablet.firstPage(),
 * tablet.nextPage(), tablet.prevPage(), and tablet.lastPage() using
 * a user controlled tablet size tablet.setPagesize(L).
 *
 * The tablet display operations use a client (thread) specific formatting
 * structure. This structure is initialized using either
 * tablet.setFormat(B1,...,Bn) or tablet.setFormat(S1,...,Sn) (Bi is a BAT, Si a scalar).
 * Subsequently, some additional properties can be set/modified,
 * column width and brackets.
 * After printing/paging the BAT resources should be freed using
 * the command tablet.finish().
 *
 * Any access outside the page-range leads to removal of the report structure.
 * Subsequent access will generate an error.
 * To illustrate, the following code fragment would be generated by
 * the SQL compiler
 *
 * @verbatim
 * 	tablet.setFormat(B1,B2);
 * 	tablet.setDelimiters("|","\t","|\n");
 * 	tablet.setName(0, "Name");
 * 	tablet.setNull(0, "?");
 * 	tablet.setWidth(0, 15);
 * 	tablet.setBracket(0, " ", ",");
 * 	tablet.setName(1, "Age");
 * 	tablet.setNull(1, "-");
 * 	tablet.setDecimal(1, 9,2);
 * 	tablet.SQLtitle("Query: select * from tables");
 * 	tablet.page();
 * 	tablet.SQLfooter(count(B1),cpuTicks);
 * @end verbatim
 *
 * @-
 * This table is printed with tab separator(s) between elements
 * and the bar (|) to mark begin and end of the string.
 * The column parameters give a new title,
 * a null replacement value, and the preferred column width.
 * Each column value is optionally surrounded by brackets.
 * Note, scale and precision can be applied to integer values only.
 * A negative scale leads to a right adjusted value.
 *
 * The title and footer operations are SQL specific routines to
 * decorate the output.
 *
 * Another example involves printing a two column table in XML format.
 * [Alternative, tablet.XMLformat(B1,B2) is a shorthand for the following:]
 *
 * @verbatim
 * 	tablet.setFormat(B1,B2);
 * 	tablet.setTableBracket("<rowset>","</rowset>");
 * 	tablet.setRowBracket("<row>","</row>");
 * 	tablet.setBracket(0, "<name>", "</name>");
 * 	tablet.setBracket(1, "<age>", "</age>");
 * 	tablet.page();
 * @end verbatim
 * @- Tablet properties
 * More detailed header information can be obtained with the command
 * tablet.setProperties(S), where S
 * is a comma separated list of properties of interest,
 * followed by the tablet.header().
 * The properties to choose from are: bat, name, type, width,
 * sorted, dense, key, base, min, max, card,....
 *
 * @verbatim
 * #--------------------------------------#
 * # B1,   B2,     B3,     B4,     B5     # BAT
 * # str,  int,    chr,    str,    date   # type
 * # true, false,  false,  false,  false  # sorted
 * # true, true,   false,  false,  false  # key
 * # ,     23,     'F',    ,              # min
 * # ,     53,     'M',	,              # max
 * # 4,     4,     4,      4,      4      # count
 * # 4,i    3,     2,      2,      3      # card
 * # name,	age,    sex,   address, dob    # name
 * #--------------------------------------#
 * @end verbatim
 *
 * @- Scalar tablets
 * In line with the 10-year experience of Monet, printing scalar values
 * follow the tuple layout structure. This means that the header() command
 * is also applicable.
 * For example, the sequence "i:=0.2;v:=sin(i); tablet.display(i,v);"
 * produces the answer:
 * @verbatim
 * #----------------#
 * # i,	v	 #
 * #----------------#
 * [ 0.2,	0.198669 ]
 * #----------------#
 * @end verbatim
 * @-
 *
 * All other formatted printing should be done with the printf() operations
 * contained in the module @sc{io}.
 *
 * @- Tablet dump/restore
 *
 * Dump and restore operations are abstractions over sequence of tablet commands.
 * The command tablet.dump(stream,B1,...,Bn) is a contraction of the sequence
 * tablet.setStream(stream);
 * tablet.setProperties("name,type,dense,sorted,key,min,max");
 * tablet.header(B1,..,Bn); tablet.page(B1,..,Bn).
 * The result can be read by tablet.load(stream,B1,..,Bn) command.
 * If loading is successful, e.g. no parsing
 * errors occurred, the tuples are appended to the corresponding BATs.
 *
 * @- Front-end extension
 * A general bulk loading of foreign tables, e.g. CSV-files and fixed position
 * records, is not provided. Instead, we extend the list upon need.
 * Currently, the routines tablet.SQLload(stream,delim1,delim2, B1,..,Bn)
 * reads the files using the Oracle(?) storage. The counterpart for
 * dumping is tablet.SQLdump(stream,delim1,delim2);
 *
 * @- The commands
 *
 * The load operation is for bulk loading a table, each column will be loaded
 * into its own bat. The arguments are void-aligned bats describing the
 * input, ie the name of the column, the tuple separator and the type.
 * The nr argument can be -1 (The input (datafile) is read until the end)
 * or a maximum.
 *
 * The dump operation is for dumping a set of bats, which are aligned.
 * Again with void-aligned arguments, with name (currently not used),
 * tuple separator (the last is the record separator) and bat to be dumped.
 * With the nr argument the dump can be limited (-1 for unlimited).
 *
 * The output operation is for ordered output. A bat (possibly form the collection)
 * gives the order. For each element in the order bat the values in the bats are
 * searched, if all are found they are output in the datafile, with the given
 * separators.
 *
 * The scripts from the tablet.mil file are all there too for backward
 * compatibility with the old Mload format files.
 *
 * The load_format loads the format file, since the old format file was
 * in a table format it can be loaded with the load command.
 *
 * The result from load_format can be used with load_data to load the data
 * into a set of new bats.
 *
 * These bats can be made persistent with the make_persistent script or
 * merge with existing bats with the merge_data script.
 *
 * The dump_format scripts dump a format file for a given set of
 * to be dumped bats. These bats can be dumped with dump_data.
 */
#include "monetdb_config.h"
#include "tablet.h"
#include "algebra.h"

#include <string.h>
#include <ctype.h>

#define CLEAR(X) if(X) {GDKfree(X);X = NULL;}
#define isScalar(C)  C->adt != TYPE_bat

#ifdef _MSC_VER
#define getcwd _getcwd
#endif

tablet_export str TABsetFormat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABheader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABdisplayTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABdisplayRow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABpage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetProperties(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABfinishReport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetPivot(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetComplaints(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetDelimiter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetColumn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetColumnName(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetTableBracket(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetRowBracket(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetColumnBracket(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetColumnNull(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetColumnWidth(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetColumnPosition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

tablet_export str TABsetColumnDecimal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetColumnDecimal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABsetTryAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
tablet_export str TABfirstPage(int *ret);
tablet_export str TABlastPage(int *ret);
tablet_export str TABnextPage(int *ret);
tablet_export str TABprevPage(int *ret);
tablet_export str TABgetPage(int *ret, int *pnr);
tablet_export str TABgetPageCnt(int *ret);
tablet_export str CMDtablet_load(int *ret, int *nameid, int *sepid, int *typeid, str *filename, int *nr);
tablet_export str CMDtablet_dump(int *ret, int *nameid, int *sepid, int *bids, str *filename, int *nr);
tablet_export str CMDtablet_input(int *ret, int *nameid, int *sepid, int *typeid, stream *s, int *nr);
tablet_export str CMDtablet_output(int *ret, int *nameid, int *sepid, int *bids, void **s);
tablet_export void TABshowHeader(Tablet *t);
tablet_export void TABshowRow(Tablet *t);
tablet_export void TABshowRange(Tablet *t, lng first, lng last);

static void makeTableSpace(int rnr, unsigned int acnt);
static str bindVariable(Tablet *t, unsigned int anr, str nme, int tpe, ptr val, int *k);
static void clearTable(Tablet *t);
static int isScalarVector(Tablet *t);
static int isBATVector(Tablet *t);

static void TABshowPage(Tablet *t);
static int setTabwidth(Column *c);

#define LINE(s, X)								\
	do {										\
		int n=(int)(X)-1;						\
		mnstr_write(s, "#", 1, 1);				\
		while(--n>0)							\
			mnstr_write(s, "-", 1, 1);			\
		mnstr_printf(s, "#\n");					\
	} while (0)
#define TABS(s, X)								\
	do {										\
		int n=(int)(X);							\
		while(n-->0)							\
			mnstr_printf(s, "\t");				\
	} while (0)
/*
 * @+
 * The table formatting information is stored in a system wide table.
 * Access is granted to a single client thread only.
 * The table structure depends on the columns to be printed,
 * it will be dynamically extended to accommodate the space.
 */
static Tablet *tableReports[MAL_MAXCLIENTS];


static void
TABformatPrepare(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int rnr = (int) (cntxt - mal_clients);
	Tablet *t;
	int anr = 0, i, tpe, k = 0;
	ptr val;

	makeTableSpace(rnr, pci->argc - pci->retc);
	t = tableReports[rnr];
	if (t->rlbrk == 0)
		t->rlbrk = GDKstrdup("[ ");
	if (t->rrbrk == 0)
		t->rrbrk = GDKstrdup("]");
	if (t->sep == 0)
		t->sep = GDKstrdup("\t");
	t->rowwidth = (int) (strlen(t->rlbrk) + strlen(t->rrbrk) - 2);

	for (i = pci->retc; i < pci->argc; anr++, i++) {
		char *name = getArgName(mb, pci, i);
		/* The type should be taken from the stack ! */
		tpe = stk->stk[pci->argv[i]].vtype;
		val = (ptr) getArgReference(stk, pci, i);
		bindVariable(t, anr, name, tpe, val, &k);
	}
	t->nr_attrs = anr;
	t->fd = cntxt->fdout;
	t->pivot = 0;
}

str
TABsetFormat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	TABformatPrepare(cntxt, mb, stk, pci);
	return MAL_SUCCEED;
}

str
TABheader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int rnr = (int) (cntxt - mal_clients);

	TABformatPrepare(cntxt, mb, stk, pci);
	TABshowHeader(tableReports[rnr]);
	return MAL_SUCCEED;
}

str
TABdisplayTable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int rnr = (int) (cntxt - mal_clients);
	Tablet *t;

	TABheader(cntxt, mb, stk, pci);
	t = tableReports[rnr];
	if (!isBATVector(t))
		throw(MAL, "tablet.print", ILLEGAL_ARGUMENT " Only aligned BATs expected");
	else {
		t->pageLimit = 20;
		t->firstrow = t->lastrow = 0;
		TABshowPage(t);
	}
	return MAL_SUCCEED;
}

str
TABdisplayRow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int rnr = (int) (cntxt - mal_clients);
	Tablet *t;

	TABheader(cntxt, mb, stk, pci);
	t = tableReports[rnr];
	if (!isScalarVector(t))
		throw(MAL, "tablet.print", ILLEGAL_ARGUMENT " Only scalars expected");
	else
		TABshowRow(t);
	if (t->tbotbrk == 0) {
		LINE(t->fd, t->rowwidth);
	} else
		mnstr_write(t->fd, t->tbotbrk, 1, strlen(t->tbotbrk));
	return MAL_SUCCEED;
}

str
TABpage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int rnr = (int) (cntxt - mal_clients);
	Tablet *t;

	TABformatPrepare(cntxt, mb, stk, pci);
	t = tableReports[rnr];
	if (t->ttopbrk == 0) {
		LINE(t->fd, t->rowwidth);
	} else
		mnstr_write(t->fd, t->ttopbrk, 1, strlen(t->ttopbrk));
	if (!isBATVector(t))
		throw(MAL, "tablet.print", ILLEGAL_ARGUMENT " Only aligned BATs expected");
	else {
		t->pageLimit = 20;
		t->firstrow = t->lastrow = 0;
		TABshowPage(t);
	}
	return MAL_SUCCEED;
}

str
TABsetProperties(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *prop = (str *) getArgReference(stk, pci, 1);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	if (tableReports[rnr] == 0)
		throw(MAL, "tablet.properties", ILLEGAL_ARGUMENT " Format definition missing");
	CLEAR(tableReports[rnr]->properties);
	tableReports[rnr]->properties = !strNil(*prop) ? GDKstrdup(*prop) : 0;
	return MAL_SUCCEED;
}

str
TABdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;					/* fool compiler */
	throw(MAL, "tablet.report", PROGRAM_NYI);
}

str
TABfinishReport(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	(void) stk;
	(void) pci;
	if (tableReports[rnr] == 0)
		throw(MAL, "tablet.finish", ILLEGAL_ARGUMENT " Header information missing");
	clearTable(tableReports[rnr]);
	GDKfree(tableReports[rnr]);
	tableReports[rnr] = 0;
	return MAL_SUCCEED;
}


str
TABsetStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	if (tableReports[rnr] == 0)
		throw(MAL, "tablet.setStream", ILLEGAL_ARGUMENT " Header information missing");
	tableReports[rnr]->fd = *s;
	return MAL_SUCCEED;
}

str
TABsetPivot(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *bid = (int *) getArgReference(stk, pci, 1);
	int rnr = (int) (cntxt - mal_clients);
	BAT *b;

	(void) mb;					/* fool compiler */
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "tablet.setPivot", RUNTIME_OBJECT_MISSING "Pivot BAT missing.");
	}

	tableReports[rnr]->pivot = b;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
TABsetComplaints(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *bid = (int *) getArgReference(stk, pci, 1);
	int rnr = (int) (cntxt - mal_clients);
	BAT *b;

	(void) mb;					/* fool compiler */
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "tablet.setComplaints", RUNTIME_OBJECT_MISSING "Complaints BAT missing.");
	}

	tableReports[rnr]->complaints = b;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
TABsetDelimiter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *sep = (str *) getArgReference(stk, pci, 1);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	if (tableReports[rnr] == 0)
		throw(MAL, "tablet.setDelimiters", RUNTIME_OBJECT_MISSING "Header information missing");
	CLEAR(tableReports[rnr]->sep);
	tableReports[rnr]->sep = !strNil(*sep) ? GDKstrdup(*sep) : 0;
	return MAL_SUCCEED;
}

str
TABsetColumn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;					/* fool compiler */
	throw(MAL, "tablet.setColumn", PROGRAM_NYI);
}

str
TABsetColumnName(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *idx = (int *) getArgReference(stk, pci, 1);
	str *s = (str *) getArgReference(stk, pci, 2);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	makeTableSpace(rnr, (*idx >= MAXARG ? *idx : MAXARG));
	CLEAR(tableReports[rnr]->columns[*idx].name);
	tableReports[rnr]->columns[*idx].name = !strNil(*s) ? GDKstrdup(*s) : 0;
	return MAL_SUCCEED;
}

str
TABsetTableBracket(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *lbrk = (str *) getArgReference(stk, pci, 1);
	str *rbrk = (str *) getArgReference(stk, pci, 2);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	makeTableSpace(rnr, MAXARG);
	CLEAR(tableReports[rnr]->ttopbrk);
	CLEAR(tableReports[rnr]->tbotbrk);
	tableReports[rnr]->ttopbrk = !strNil(*lbrk) ? GDKstrdup(*lbrk) : 0;
	tableReports[rnr]->tbotbrk = !strNil(*rbrk) ? GDKstrdup(*rbrk) : 0;
	return MAL_SUCCEED;
}

str
TABsetRowBracket(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *lbrk = (str *) getArgReference(stk, pci, 1);
	str *rbrk = (str *) getArgReference(stk, pci, 2);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	makeTableSpace(rnr, MAXARG);
	CLEAR(tableReports[rnr]->rlbrk);
	CLEAR(tableReports[rnr]->rrbrk);
	tableReports[rnr]->rlbrk = !strNil(*lbrk) ? GDKstrdup(*lbrk) : 0;
	tableReports[rnr]->rrbrk = !strNil(*rbrk) ? GDKstrdup(*rbrk) : 0;
	return MAL_SUCCEED;
}

str
TABsetColumnBracket(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *idx = (int *) getArgReference(stk, pci, 1);
	str *lbrk = (str *) getArgReference(stk, pci, 2);
	str *rbrk = (str *) getArgReference(stk, pci, 3);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	makeTableSpace(rnr, (*idx >= MAXARG ? *idx : MAXARG));
	CLEAR(tableReports[rnr]->columns[*idx].lbrk);
	CLEAR(tableReports[rnr]->columns[*idx].rbrk);
	tableReports[rnr]->columns[*idx].lbrk = !strNil(*lbrk) ? GDKstrdup(*lbrk) : 0;
	tableReports[rnr]->columns[*idx].rbrk = !strNil(*rbrk) ? GDKstrdup(*rbrk) : 0;
	return MAL_SUCCEED;
}

str
TABsetColumnNull(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *idx = (int *) getArgReference(stk, pci, 1);
	str *nullstr = (str *) getArgReference(stk, pci, 2);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	makeTableSpace(rnr, (*idx >= MAXARG ? *idx : MAXARG));
	CLEAR(tableReports[rnr]->columns[*idx].nullstr);
	tableReports[rnr]->columns[*idx].nullstr = !strNil(*nullstr) ? GDKstrdup(*nullstr) : 0;
	return MAL_SUCCEED;
}

str
TABsetColumnWidth(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *idx = (int *) getArgReference(stk, pci, 1);
	int *width = (int *) getArgReference(stk, pci, 2);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	makeTableSpace(rnr, (*idx >= MAXARG ? *idx : MAXARG));
	tableReports[rnr]->columns[*idx].maxwidth = *width;
	return MAL_SUCCEED;
}

str
TABsetColumnPosition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *idx = (int *) getArgReference(stk, pci, 1);
	int *first = (int *) getArgReference(stk, pci, 2);
	int *width = (int *) getArgReference(stk, pci, 3);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;
	(void) first;				/* fool compiler */
	tableReports[rnr]->columns[*idx].fieldwidth = *width;
	tableReports[rnr]->columns[*idx].fieldstart = *width;
	makeTableSpace(rnr, (*idx >= MAXARG ? *idx : MAXARG));
	return MAL_SUCCEED;
}

str
TABsetColumnDecimal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *idx = (int *) getArgReference(stk, pci, 1);
	int *scale = (int *) getArgReference(stk, pci, 2);
	int *prec = (int *) getArgReference(stk, pci, 3);
	int rnr = (int) (cntxt - mal_clients);

	(void) mb;					/* fool compiler */
	makeTableSpace(rnr, (*idx >= MAXARG ? *idx : MAXARG));
	if (*prec > *scale)
		throw(MAL, "tablet.setColumnDecimal", ILLEGAL_ARGUMENT " Illegal range");
	tableReports[rnr]->columns[*idx].precision = *prec;
	tableReports[rnr]->columns[*idx].scale = *scale;
	return MAL_SUCCEED;
}

str
TABsetTryAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ptrdiff_t rnr = cntxt - mal_clients;
	int flg = *(int *) getArgReference(stk, pci, 1);

	(void) mb;
	if (tableReports[rnr] == 0)
		throw(MAL, "tablet.setTryAll", RUNTIME_OBJECT_MISSING);
	tableReports[rnr]->tryall = flg;
	return MAL_SUCCEED;
}

str
TABfirstPage(int *ret)
{
	(void) ret;					/* fool compiler */
	throw(MAL, "tablet.firstPage", PROGRAM_NYI);
}

str
TABlastPage(int *ret)
{
	(void) ret;					/* fool compiler */
	throw(MAL, "tablet.lastPage", PROGRAM_NYI);
}

str
TABnextPage(int *ret)
{
	(void) ret;					/* fool compiler */
	throw(MAL, "tablet.nextPage", PROGRAM_NYI);
}

str
TABprevPage(int *ret)
{
	(void) ret;					/* fool compiler */
	throw(MAL, "tablet.prevPage", PROGRAM_NYI);
}

str
TABgetPage(int *ret, int *pnr)
{
	(void) ret;
	(void) pnr;					/* fool compiler */
	throw(MAL, "tablet.getPage", PROGRAM_NYI);
}

str
TABgetPageCnt(int *ret)
{
	(void) ret;					/* fool compiler */
	throw(MAL, "tablet.getPageCnt", PROGRAM_NYI);
}

static ptr
bun_tail(BAT *b, BUN nr)
{
	BATiter bi = bat_iterator(b);
	register BUN _i = BUNfirst(b);

	return (ptr) BUNtail(bi, _i + nr);
}


static BAT *
void_bat_create(int adt, BUN nr)
{
	BAT *b = BATnew(TYPE_void, adt, BATTINY);

	/* check for correct structures */
	if (b == NULL)
		return b;
	if (BATmirror(b))
		BATseqbase(b, 0);
	BATsetaccess(b, BAT_APPEND);
	if (nr > (BUN) REMAP_PAGE_MAXSIZE)
		BATmmap(b, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 0);
	if (nr > BATTINY && adt)
		b = BATextend(b, nr);
	if (b == NULL)
		return b;

	/* disable all properties here */
	b->tsorted = FALSE;
	b->T->nosorted = 0;
	b->tdense = FALSE;
	b->T->nodense = 0;
	b->tkey = FALSE;
	b->T->nokey[0] = 0;
	b->T->nokey[1] = 1;
	return b;
}

static char *
sep_dup(char *sep)
{
	size_t len = strlen(sep);
	char *res = GDKmalloc(len * 2 + 1), *result = res;
	char *end = sep + len;

	if (res == NULL)
		return NULL;
	while (sep < end) {
		if (*sep == '\\') {
			++sep;
			switch (*sep++) {
			case 'r':
				*res++ = '\r';
				break;
			case 'n':
				*res++ = '\n';
				break;
			case 't':
				*res++ = '\t';
				break;
			}
		} else {
			*res++ = *sep++;
		}
	}
	*res = '\0';
	return result;
}


ptr *
TABLETstrFrStr(Column *c, char *s, char *e)
{
	int len = (int) (e - s + 1);	/* 64bit: should check for overflow */

	if (c->len < len) {
		c->len = len;
		c->data = GDKrealloc(c->data, len);
	}

	if (s == e) {
		*(char *) c->data = 0;
	} else if (GDKstrFromStr(c->data, (unsigned char *) s, (ssize_t) (e - s)) < 0) {
		return NULL;
	}
	return c->data;
}

ptr *
TABLETadt_frStr(Column *c, int type, char *s, char *e, char quote)
{
	if (s == NULL || (!quote && strcmp(s, "nil") == 0)) {
		memcpy(c->data, ATOMnilptr(type), c->nillen);
	} else if (type == TYPE_str) {
		return TABLETstrFrStr(c, s, e);
	} else {
		(void) (*BATatoms[type].atomFromStr) (s, &c->len, (ptr) &c->data);
	}
	return c->data;
}

int
TABLETadt_toStr(void *extra, char **buf, int *len, int type, ptr a)
{
	(void) extra;				/* fool compiler */
	if (type == TYPE_str) {
		char *dst, *src = a;
		int l;

		if (GDK_STRNIL(src)) {
			src = "nil";
		}
		l = (int) strlen(src);
		if (l + 3 > *len) {
			GDKfree(buf);
			*len = 2 * l + 3;
			*buf = GDKzalloc(*len);
		}
		dst = *buf;
		dst[0] = '"';
		strncpy(dst + 1, src, l);
		dst[l + 1] = '"';
		dst[l + 2] = 0;
		return l + 2;
	} else {
		return (*BATatoms[type].atomToStr) (buf, len, a);
	}
}

#define myisspace(s)  ( s<=' ' && (s == ' ' || s == '\t'))

int
has_whitespace(char *sep)
{
	char *s = sep;

	if (myisspace(*s))
		return 1;
	while (*s)
		s++;
	s--;
	if (myisspace(*s))
		return 1;
	return 0;
}

static BUN
create_loadformat(Tablet *as, BAT *names, BAT *seps, BAT *types)
{
	BUN p;
	BUN nr_attrs = BATcount(names);
	Column *fmt = as->format = (Column *) GDKmalloc(sizeof(Column) * (nr_attrs + 1));

	if (fmt == NULL)
		return 0;
	as->offset = 0;
	as->nr_attrs = nr_attrs;
	as->tryall = 0;
	as->complaints = 0;
	as->error = NULL;
	as->input = NULL;
	as->output = NULL;
	/* assert(as->nr_attrs == nr_attrs); *//* i.e. it fits */
	for (p = 0; p < nr_attrs; p++) {
		fmt[p].name = (char *) bun_tail(names, p);
		fmt[p].sep = sep_dup((char *) bun_tail(seps, p));
		fmt[p].seplen = (int) strlen(fmt[p].sep);
		fmt[p].type = GDKstrdup((char *) bun_tail(types, p));
		fmt[p].adt = ATOMindex(fmt[p].type);
		if (fmt[p].adt <= 0) {
			GDKerror("create_loadformat: %s has unknown type %s (using str instead).\n", fmt[p].name, fmt[p].name);
			fmt[p].adt = TYPE_str;
		}
		fmt[p].tostr = &TABLETadt_toStr;
		fmt[p].frstr = &TABLETadt_frStr;
		fmt[p].extra = NULL;
		fmt[p].len = fmt[p].nillen = ATOMlen(fmt[p].adt, ATOMnilptr(fmt[p].adt));
		fmt[p].ws = !(has_whitespace(fmt[p].sep));
		fmt[p].quote = '"';
		fmt[p].data = GDKmalloc(fmt[p].len);
		fmt[p].nildata = GDKmalloc(fmt[p].nillen);
		memcpy(fmt[p].nildata, ATOMnilptr(fmt[p].adt), fmt[p].nillen);
		fmt[p].nullstr = GDKstrdup("nil");
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout, "#%s\n", fmt[p].name);
#endif
		fmt[p].batfile = NULL;
		fmt[p].rawfile = NULL;
		fmt[p].raw = NULL;
	}
	return as->nr_attrs;
}

static BUN
create_dumpformat(Tablet *as, BAT *names, BAT *seps, BAT *bats)
{
	BUN p;
	BUN nr_attrs = BATcount(bats);
	Column *fmt = as->format = (Column *) GDKmalloc(sizeof(Column) * (nr_attrs + 1));

	if (fmt == NULL)
		return 0;
	as->offset = 0;
	as->nr_attrs = nr_attrs;
	as->tryall = 0;
	as->complaints = 0;
	as->error = NULL;
	as->input = NULL;
	as->output = NULL;
	/* assert(as->nr_attrs == nr_attrs); *//* i.e. it fits */
	for (p = 0; p < nr_attrs; p++) {
		BAT *b = (BAT *) BATdescriptor(*(bat *) bun_tail(bats, p));

		if (!b)
			return BUN_NONE;
		fmt[p].name = NULL;
		if (names)
			fmt[p].name = (char *) bun_tail(names, p);
		fmt[p].sep = sep_dup((char *) bun_tail(seps, p));
		fmt[p].seplen = (int) strlen(fmt[p].sep);
		fmt[p].type = GDKstrdup(ATOMname(b->ttype));
		fmt[p].adt = (b)->ttype;
		fmt[p].tostr = &TABLETadt_toStr;
		fmt[p].frstr = &TABLETadt_frStr;
		fmt[p].extra = NULL;
		fmt[p].data = NULL;
		fmt[p].len = 0;
		fmt[p].nillen = 0;
		fmt[p].ws = 0;
		fmt[p].nullstr = GDKstrdup("nil");
		BBPunfix(b->batCacheid);
	}
	return as->nr_attrs;
}

void
TABLETdestroy_format(Tablet *as)
{
	BUN p;
	Column *fmt = as->format;

	for (p = 0; p < as->nr_attrs; p++) {
		if (fmt[p].c[0])
			BBPunfix(fmt[p].c[0]->batCacheid);
		GDKfree(fmt[p].sep);
		if (fmt[p].nullstr)
			GDKfree(fmt[p].nullstr);
		if (fmt[p].data)
			GDKfree(fmt[p].data);
		if (fmt[p].nildata)
			GDKfree(fmt[p].nildata);
		if (fmt[p].batfile)
			GDKfree(fmt[p].batfile);
		if (fmt[p].rawfile)
			GDKfree(fmt[p].rawfile);
		if (fmt[p].type)
			GDKfree(fmt[p].type);
	}
	GDKfree(fmt);
}

BUN
TABLETassign_BATs(Tablet *as, BAT *bats)
{
	Column *fmt = as->format;
	BUN res = as->nr;
	BUN i;

	for (i = 0; i < as->nr_attrs; i++) {
		BAT *b = (BAT *) BATdescriptor(*(bat *) bun_tail(bats, i));

		fmt[i].c[0] = (b);
		fmt[i].ci[0] = bat_iterator(b);
		if (res == BUN_NONE || BATcount(fmt[i].c[0]) < res)
			res = BATcount(fmt[i].c[0]);
	}
	as->nr = res;
	return res;
}

static oid
check_BATs(Tablet *as)
{
	Column *fmt = as->format;
	BUN i = 0;
	BUN cnt;
	oid base;

	if (fmt[i].c[0] == NULL)
		i++;
	cnt = BATcount(fmt[i].c[0]);
	base = fmt[i].c[0]->hseqbase;

	if (!BAThdense(fmt[i].c[0]) || as->nr != cnt)
		return oid_nil;

	for (i = 0; i < as->nr_attrs; i++) {
		BAT *b;
		BUN offset;

		b = fmt[i].c[0];
		if (b == NULL)
			continue;
		offset = BUNfirst(b) + as->offset;

		if (BATcount(b) != cnt || !BAThdense(b) || b->hseqbase != base)
			return oid_nil;

		fmt[i].p = offset;
	}
	return base;
}

int
TABLETcreate_bats(Tablet *as, BUN est)
{
	Column *fmt = as->format;
	BUN i;
	char nme[BUFSIZ];

	if (getcwd(nme, BUFSIZ) == NULL) {
		GDKerror("TABLETcreate_bats: Failed to locate directory\n");
		return -1;
	}

	assert(strlen(nme) < BUFSIZ - 50);

	for (i = 0; i < as->nr_attrs; i++) {
		fmt[i].c[0] = void_bat_create(fmt[i].adt, est);
		fmt[i].ci[0] = bat_iterator(fmt[i].c[0]);
		if (!fmt[i].c[0]) {
			GDKerror("TABLETcreate_bats: Failed to create bat of size " BUNFMT "\n", as->nr);
			return -1;
		}
	}
	return 0;
}

BAT *
TABLETcollect_bats(Tablet *as)
{
	BAT *bats = BATnew(TYPE_str, TYPE_bat, as->nr_attrs);
	Column *fmt = as->format;
	BUN i;
	BUN cnt = BATcount(fmt[0].c[0]);

	if (bats == NULL)
		return NULL;
	for (i = 0; i < as->nr_attrs; i++) {
		BUNins(bats, (ptr) fmt[i].name, (ptr) &fmt[i].c[0]->batCacheid, FALSE);
		BATsetaccess(fmt[i].c[0], BAT_READ);
		BATaccessBegin(fmt[i].c[0], USE_ALL, MMAP_WILLNEED);
		BATpropcheck(fmt[i].c[0], BATPROPS_ALL);
		/* drop the hashes, we don't need them now  and they consume space */
		HASHremove(fmt[i].c[0]);

		BATpropcheck(BATmirror(fmt[i].c[0]), BATPROPS_ALL);
		/* drop the hashes, we don't need them now  and they consume space */
		HASHremove(BATmirror(fmt[i].c[0]));
		BATaccessEnd(fmt[i].c[0], USE_ALL, MMAP_WILLNEED);

		if (cnt != BATcount(fmt[i].c[0])) {
			if (as->error == 0)	/* a new error */
				GDKerror("Error: column " BUNFMT "  count " BUNFMT " differs from " BUNFMT "\n", i, BATcount(fmt[i].c[0]), cnt);
			BBPunfix(bats->batCacheid);
			return NULL;
		}
	}
	return bats;
}

BAT *
TABLETcollect_parts(Tablet *as, BUN offset)
{
	BAT *bats = BATnew(TYPE_str, TYPE_bat, as->nr_attrs);
	Column *fmt = as->format;
	BUN i;
	BUN cnt = BATcount(fmt[0].c[0]);

	if (bats == NULL)
		return NULL;
	for (i = 0; i < as->nr_attrs; i++) {
		int GDKdebug_bak = GDKdebug;
		BAT *b = fmt[i].c[0];
		BAT *bv = NULL;

		BATsetaccess(b, BAT_READ);
		bv = BATslice(b, offset, BATcount(b));
		BUNins(bats, (ptr) fmt[i].name, (ptr) &bv->batCacheid, FALSE);
		/* we "mis"use BATpropcheck to set rather than verify properties on
		 * the newly loaded slice; hence, we locally disable property errors */
		GDKdebug &= ~PROPMASK;
		BATaccessBegin(bv, USE_ALL, MMAP_WILLNEED);
		BATpropcheck(bv, BATPROPS_ALL);
		/* drop the hashes, we don't need them now  and they consume space */
		HASHremove(b);

		BATpropcheck(BATmirror(bv), BATPROPS_ALL);
		/* drop the hashes, we don't need them now  and they consume space */
		HASHremove(BATmirror(b));
		BATaccessEnd(bv, USE_ALL, MMAP_WILLNEED);
		GDKdebug = GDKdebug_bak;

		b->hkey &= bv->hkey;
		b->tkey &= bv->tkey;
		b->H->nonil &= bv->H->nonil;
		b->T->nonil &= bv->T->nonil;
		b->hdense &= bv->hdense;
		b->tdense &= bv->tdense;
		if (b->hsorted != bv->hsorted)
			b->hsorted = 0;
		if (b->tsorted != bv->tsorted)
			b->tsorted = 0;
		b->batDirty = TRUE;

		if (cnt != BATcount(b)) {
			if (as->error == 0)	/* a new error */
				GDKerror("Error: column " BUNFMT "  count " BUNFMT " differs from " BUNFMT "\n", i, BATcount(b), cnt);
			BBPunfix(bats->batCacheid);
			return NULL;
		}
		BBPunfix(bv->batCacheid);
	}
	return bats;
}

static void
sync_bats(Tablet *as)
{
	unsigned int i;
	Column *fmt = as->format;

	for (i = 0; i < as->nr_attrs; i++)
		if (fmt[i].c[0]->T->heap.storage == STORE_MMAP) {
			BATsave(fmt[i].c[0]);
			fmt[i].c[0]->batDirty = TRUE;
		}
}

static inline char *
rstrip(char *s, char *e)
{
	e--;
	while (myisspace((int) *e) && e >= s)
		e--;
	e++;
	*e = 0;
	return e;
}

static inline char *
find_quote(char *s, char quote)
{
	while (*s != quote)
		s++;
	return s;
}

static inline char *
rfind_quote(char *s, char *e, char quote)
{
	while (*e != quote && e > s)
		e--;
	return e;
}

static inline int
insert_val(Column *fmt, char *s, char *e, char quote, ptr key, str *err, int c)
{
	char bak = 0;
	ptr *adt;
	char buf[BUFSIZ];

	if (quote) {
		/* string needs the quotes included */
		s = find_quote(s, quote);
		if (!s) {
			snprintf(buf, BUFSIZ, "quote '%c' expected but not found in \"%s\" from line " BUNFMT "\n", quote, s, BATcount(fmt->c[0]));
			*err = GDKstrdup(buf);
			return -1;
		}
		s++;
		e = rfind_quote(s, e, quote);
		if (s != e) {
			bak = *e;
			*e = 0;
		}
		if ((s == e && fmt->nullstr[0] == 0) ||
			(quote == fmt->nullstr[0] && e > s &&
			 strncasecmp(s, fmt->nullstr + 1, fmt->nillen) == 0 &&
			 quote == fmt->nullstr[fmt->nillen - 1])) {
			adt = fmt->nildata;
			fmt->c[0]->T->nonil = 0;
		} else
			adt = fmt->frstr(fmt, fmt->adt, s, e, quote);
		if (bak)
			*e = bak;
	} else {
		if (s != e) {
			bak = *e;
			*e = 0;
		}

		if ((s == e && fmt->nullstr[0] == 0) ||
			(e > s && strcasecmp(s, fmt->nullstr) == 0)) {
			adt = fmt->nildata;
			fmt->c[0]->T->nonil = 0;
		} else
			adt = fmt->frstr(fmt, fmt->adt, s, e, quote);
		if (bak)
			*e = bak;
	}

	if (!adt) {
		char *val;
		bak = *e;
		*e = 0;
		val = (s != e) ? GDKstrdup(s) : GDKstrdup("");
		*e = bak;

		snprintf(buf, BUFSIZ, "value '%s' while parsing '%s' from line " BUNFMT " field %d not inserted, expecting type %s\n", val, s, BATcount(fmt->c[0]), c, fmt->type);
		*err = GDKstrdup(buf);
		GDKfree(val);
		return -1;
	}
	/* key may be NULL but that's not a problem, as long as we have void */
	if (fmt->raw) {
		mnstr_write(fmt->raw, adt, ATOMsize(fmt->adt), 1);
	} else {
		bunfastins(fmt->c[0], key, adt);
	}
	return 0;
  bunins_failed:
	snprintf(buf, BUFSIZ, "while parsing '%s' from line " BUNFMT " field %d not inserted\n", s, BATcount(fmt->c[0]), c);
	*err = GDKstrdup(buf);
	return -1;
}

char *
tablet_skip_string(char *s, char quote)
{
	while (*s) {
		if (*s == '\\' && s[1] != '\0')
			s++;
		else if (*s == quote) {
			if (s[1] == quote)
				*s++ = '\\';	/* sneakily replace "" with \" */
			else
				break;
		}
		s++;
	}
	assert(*s == quote || *s == '\0');
	if (*s)
		s++;
	else
		return NULL;
	return s;
}

inline int
insert_line(Tablet *as, char *line, ptr key, BUN col1, BUN col2)
{
	Column *fmt = as->format;
	char *s, *e = 0, quote = 0, seperator = 0;
	BUN i;
	char errmsg[BUFSIZ];

	for (i = 0; i < as->nr_attrs; i++) {
		e = 0;

		/* skip leading spaces */
		if (fmt[i].ws)
			while (myisspace((int) (*line)))
				line++;
		s = line;

		/* recognize fields starting with a quote */
		if (*line && *line == fmt[i].quote && (line == s || *(line - 1) != '\\')) {
			quote = *line;
			line++;
			line = tablet_skip_string(line, quote);
			if (!line) {
				snprintf(errmsg, BUFSIZ, "End of string (%c) missing " "in %s at line " BUNFMT "\n", quote, s, BATcount(fmt->c[0]));
				as->error = GDKstrdup(errmsg);
				if (!as->tryall)
					return -1;
				BUNins(as->complaints, NULL, as->error, TRUE);
			}
		}

		/* skip until separator */
		seperator = fmt[i].sep[0];
		if (fmt[i].sep[1] == 0) {
			while (*line) {
				if (*line == seperator) {
					e = line;
					break;
				}
				line++;
			}
		} else {
			while (*line) {
				if (*line == seperator &&
					strncmp(fmt[i].sep, line, fmt[i].seplen) == 0) {
					e = line;
					break;
				}
				line++;
			}
		}
		if (!e && i == (as->nr_attrs - 1))
			e = line;
		if (e) {
			if (i >= col1 && i < col2)
				(void) insert_val(&fmt[i], s, e, quote, key, &as->error, (int) i);
			quote = 0;
			line = e + fmt[i].seplen;
			if (as->error) {
				if (!as->tryall)
					return -1;
				BUNins(as->complaints, NULL, as->error, TRUE);
			}
		} else {
			snprintf(errmsg, BUFSIZ, "missing separator '%s' line " BUNFMT " field " BUNFMT "\n", fmt->sep, BATcount(fmt->c[0]), i);
			as->error = GDKstrdup(errmsg);
			if (!as->tryall)
				return -1;
			BUNins(as->complaints, NULL, as->error, TRUE);
		}
	}
	return 0;
}

static int
TABLET_error(stream *s)
{
	if (!mnstr_errnr(GDKerr)) {
		char *err = mnstr_error(s);

		mnstr_printf(GDKout, "#Stream error %s\n", err);
		/* use free as stream allocates out side GDK */
		if (err)
			free(err);
	}
	return -1;
}

static inline int
dump_line(char **buf, int *len, Column *fmt, stream *fd, BUN nr_attrs, BUN id)
{
	BUN i;

	for (i = 0; i < nr_attrs; i++) {
		Column *f;
		char *p;
		int l;

		f = fmt + i;
		if (f->c[0]) {
			p = (char *) bun_tail(f->c[0], id);

			if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
				l = (int) strlen(f->nullstr);
				if (mnstr_write(fd, f->nullstr, 1, l) != l)
					return TABLET_error(fd);
			} else {
				l = f->tostr(f->extra, buf, len, f->adt, p);
				if (mnstr_write(fd, *buf, 1, l) != l)
					return TABLET_error(fd);
			}
		}
		if (mnstr_write(fd, f->sep, 1, f->seplen) != f->seplen)
			return TABLET_error(fd);
	}
	return 0;
}

static inline int
output_line(char **buf, int *len, Column *fmt, stream *fd, BUN nr_attrs, ptr id)
{
	BUN i;

	for (i = 0; i < nr_attrs; i++) {
		if (fmt[i].c[0] == NULL)
			continue;
		fmt[i].p = BUNfnd(fmt[i].c[0], id);

		if (fmt[i].p == BUN_NONE)
			break;
	}
	if (i == nr_attrs) {
		for (i = 0; i < nr_attrs; i++) {
			Column *f;
			char *p;
			int l;

			f = fmt + i;
			if (f->c[0]) {
				p = BUNtail(f->ci[0], f->p);

				if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
					l = (int) strlen(f->nullstr);
					if (mnstr_write(fd, f->nullstr, 1, l) != l)
						return TABLET_error(fd);
				} else {
					l = f->tostr(f->extra, buf, len, f->adt, p);
					if (mnstr_write(fd, *buf, 1, l) != l)
						return TABLET_error(fd);
				}
			}
			if (mnstr_write(fd, f->sep, 1, f->seplen) != f->seplen)
				return TABLET_error(fd);
		}
	}
	return 0;
}

static inline int
output_line_dense(char **buf, int *len, Column *fmt, stream *fd, BUN nr_attrs)
{
	BUN i;

	for (i = 0; i < nr_attrs; i++) {
		Column *f = fmt + i;

		if (f->c[0]) {
			char *p = BUNtail(f->ci[0], f->p);

			if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
				int l = (int) strlen(f->nullstr);
				if (mnstr_write(fd, f->nullstr, 1, l) != l)
					return TABLET_error(fd);
			} else {
				int l = f->tostr(f->extra, buf, len, f->adt, p);
				if (mnstr_write(fd, *buf, 1, l) != l)
					return TABLET_error(fd);
			}
			f->p++;
		}
		if (mnstr_write(fd, f->sep, 1, f->seplen) != f->seplen)
			return TABLET_error(fd);
	}
	return 0;
}

static inline int
output_line_lookup(char **buf, int *len, Column *fmt, stream *fd, BUN nr_attrs, BUN id)
{
	BUN i;

	for (i = 0; i < nr_attrs; i++) {
		Column *f = fmt + i;

		if (f->c[0]) {
			char *p = BUNtail(f->ci[0], id +BUNfirst(f->c[0]));

			if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
				size_t l = strlen(f->nullstr);
				if (mnstr_write(fd, f->nullstr, 1, l) != (ssize_t) l)
					return TABLET_error(fd);
			} else {
				int l = f->tostr(f->extra, buf, len, f->adt, p);

				if (mnstr_write(fd, *buf, 1, l) != l)
					return TABLET_error(fd);
			}
		}
		if (mnstr_write(fd, f->sep, 1, f->seplen) != f->seplen)
			return TABLET_error(fd);
	}
	return 0;
}

int
tablet_read_more(bstream *in, stream *out, size_t n)
{
	if (out) {
		do {
			/* query is not finished ask for more */
			/* we need more query text */
			if (bstream_next(in) < 0)
				return EOF;
			if (in->eof) {
				if (out && mnstr_write(out, PROMPT2, sizeof(PROMPT2) - 1, 1) == 1)
					mnstr_flush(out);
				in->eof = 0;
				/* we need more query text */
				if (bstream_next(in) <= 0)
					return EOF;
			}
		} while (in->len <= in->pos);
	} else if (bstream_read(in, n) <= 0) {
		return EOF;
	}
	return 1;
}

/*
 * @-
 * @-Fast Load
 * To speedup the CPU intensive loading of files we have to break
 * the file into pieces and perform parallel analysis. Experimentation
 * against lineitem SF1 showed that half of the time goes into very
 * basis atom analysis (41 out of 102 B instructions).
 * Furthermore, the actual insertion into the BATs takes only
 * about 10% of the total. With multi-core processors around
 * it seems we can gain here significantly.
 *
 * The approach taken is to fork a parallel scan over the text file.
 * We assume that the blocked stream is already
 * positioned correctly at the reading position. The start and limit
 * indicates the byte range to search for tuples.
 * If start> 0 then we first skip to the next record separator.
 * If necessary we read more then 'limit' bytes to ensure parsing a complete
 * record and stop at the record boundary.
 * Beware, we should allocate Tablet descriptors for each file segment,
 * otherwise we end up with a gross concurrency control problem.
 * The resulting BATs should be glued at the final phase.
 *
 * @- Raw Load
 * Front-ends can bypass most of the overhead in loading the BATs
 * by preparing the corresponding files directly and replace those
 * created by e.g. the SQL frontend.
 * This strategy is only advisable for cases where we have very
 * large files >200GB and/or are created by a well debugged code.
 *
 * To experiment with this approach, the code base responds
 * on negative number of cores by dumping the data directly in BAT
 * storage format into a collections of files on disk.
 * It reports on the actions to be taken to replace BATs.
 * This technique is initially only supported for fixed-sized columns.
 * The rawmode() indicator acts as the internal switch.
 */

static BUN
TABLETload_bulk(Tablet *as, bstream *b, stream *out, BUN col1, BUN col2, int start, lng limit)
{
	int res = 0, done = 0;
	BUN i = 0;
	char *sep = as->format[as->nr_attrs - 1].sep;
	int seplen = as->format[as->nr_attrs - 1].seplen;
	BUN offset = as->offset;
	BUN nr = offset + as->nr;
	int started = !start;

#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "load_bulk %d\n", start);
#endif
	while ((b->pos < b->len || !b->eof) && res == 0 && limit >= 0 && (nr == BUN_NONE || i < nr)) {
		char *s, *e, *end;


		if (b->pos >= b->len && tablet_read_more(b, out, b->size - (b->len - b->pos)) == EOF) {
			if (nr != BUN_NONE && i < nr) {
				res = 1;
				if (b->len > b->pos) {
					GDKerror("TABLETload_bulk: read error (after loading " BUNFMT " records)\n", BATcount(as->format[0].c[0]));
					res = -1;
				}
			}
			break;
		}
		end = b->buf + b->len;
		s = b->buf + b->pos;
		*end = '\0';
		done = 0;
		/* We use `e' to indicate from where we search for the next
		 * separator.  When records are large, we don't want to scan
		 * data twice, so `e' points to where we left off the last
		 * time (minus the separator length). */
		e = s;
		while (s < end && limit >= 0) {
			e = strstr(e, sep);

			if (e) {
				limit -= (lng) (e - s) + seplen;
				*e = '\0';
				if (started && i >= offset && insert_line(as, s, NULL, col1, col2) < 0) {
					s = e + seplen;
					b->pos = (s - b->buf);
					res = -1;
					break;
				}
				s = e + seplen;
				e = s;
				done = 1;
			} else {
				if (!done) {	/* nothing found in current buf
								 * ie. need to enlarge
								 */
					size_t size = b->size;
					size_t len = b->len - b->pos;

					if (b->pos == 0 || (b->len - b->pos > b->size >> 1))
						size <<= 4;
					if (tablet_read_more(b, out, size) == EOF) {
						/* some data left? */
						res = 1;
						if (b->len > b->pos &&
							i >= offset &&
							insert_line(as, s, NULL, col1, col2) < 0 &&
							!as->tryall) {
							GDKerror("%s", as->error);
							as->error = 0;
							GDKerror("TABLETload_bulk: read error " "(after loading " BUNFMT " records)\n", BATcount(as->format[0].c[0]));
							res = -1;
						}
						limit = -1;
						break;
					}
					end = b->buf + b->len;
					s = b->buf + b->pos;
					*end = '\0';
					/* from where to search for separator */
					if (len < (size_t) seplen)
						e = s;
					else
						e = s + len - seplen;
					continue;
				}
				break;
			}
			b->pos = (s - b->buf);
			i += started;
			/* ignored partial record from previous block */
			started = 1;
#ifdef _DEBUG_TABLET_
			if ((i % 100000) == 0)
				mnstr_printf(GDKout, "#inserted " BUNFMT "\n", i);
#endif
			if (i && (i % 100000) == 0)
				sync_bats(as);
			if (nr != BUN_NONE && i >= nr)
				break;
		}
	}
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "#limit " LLFMT ", nr " BUNFMT " res %d offset " BUNFMT "\n", limit, i - offset, res, offset);
	mnstr_printf(GDKout, "stream eof %d len " SZFMT " pos " SZFMT "\n", b->eof, b->len, b->pos);
#endif
	as->nr = i - offset;
	sync_bats(as);
	if (res < 0)
		return BUN_NONE;
	return as->nr;
}

/*
 * @-
 * To speed up loading ascii files we have to determine the number of blocks.
 * This depends on the number of cores available.
 * For the time being we hardwire this decision based on our own
 * platforms.
 * Furthermore, we only consider parallel load for file-based requests.
 *
 * To simplify our world, we assume a single loader process.
 */

BUN
TABLETload_file(Tablet *as, bstream *b, stream *out)
{
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "#starting file based load\n");
#endif

	return TABLETload_bulk(as, b, out, 0, as->nr_attrs, 0, LLONG_MAX);
}

static int
dump_file(Tablet *as, stream *fd)
{
	BUN i = 0;
	int len = BUFSIZ;
	char *buf = GDKmalloc(len);

	if (buf == NULL)
		return -1;
	for (i = 0; i < as->nr; i++) {
		if (dump_line(&buf, &len, as->format, fd, as->nr_attrs, i) < 0) {
			GDKfree(buf);
			return -1;
		}
#ifdef _DEBUG_TABLET_
		if ((i % 1000000) == 0)
			mnstr_printf(GDKout, "#dumped " BUNFMT " lines\n", i);
#endif
	}
	GDKfree(buf);
	return 0;
}

static int
output_file_default(Tablet *as, BAT *order, stream *fd)
{
	int len = BUFSIZ, res = 0;
	char *buf = GDKmalloc(len);
	BUN p, q;
	BUN i = 0;
	BUN offset = BUNfirst(order) + as->offset;
	BATiter orderi = bat_iterator(order);

	if (buf == NULL)
		return -1;
	for (q = offset + as->nr, p = offset; p < q; p++) {
		ptr h = BUNhead(orderi, p);

		if ((res = output_line(&buf, &len, as->format, fd, as->nr_attrs, h)) < 0) {
			GDKfree(buf);
			return res;
		}
		i++;
#ifdef _DEBUG_TABLET_
		if ((i % 1000000) == 0)
			mnstr_printf(GDKout, "#dumped " BUNFMT " lines\n", i);
#endif
	}
	GDKfree(buf);
	return res;
}

int
output_file_dense(Tablet *as, stream *fd)
{
	int len = BUFSIZ, res = 0;
	char *buf = GDKmalloc(len);
	BUN i = 0;

	if (buf == NULL)
		return -1;
	for (i = 0; i < as->nr; i++) {
		if ((res = output_line_dense(&buf, &len, as->format, fd, as->nr_attrs)) < 0) {
			GDKfree(buf);
			return res;
		}
#ifdef _DEBUG_TABLET_
		if ((i % 1000000) == 0)
			mnstr_printf(GDKout, "#dumped " BUNFMT " lines\n", i);
#endif
	}
	GDKfree(buf);
	return res;
}

static int
output_file_ordered(Tablet *as, BAT *order, stream *fd, oid base)
{
	int len = BUFSIZ, res = 0;
	char *buf = GDKmalloc(len);
	BUN p, q;
	BUN i = 0;
	BUN offset = BUNfirst(order) + as->offset;
	BATiter orderi = bat_iterator(order);

	if (buf == NULL)
		return -1;
	for (q = offset + as->nr, p = offset; p < q; p++, i++) {
		BUN h = (BUN) (*(oid *) BUNhead(orderi, p) - base);

		if ((res = output_line_lookup(&buf, &len, as->format, fd, as->nr_attrs, h)) < 0) {
			GDKfree(buf);
			return res;
		}
#ifdef _DEBUG_TABLET_
		if ((i % 1000000) == 0)
			mnstr_printf(GDKout, "#dumped " BUNFMT " lines\n", i);
#endif
	}
	GDKfree(buf);
	return res;
}

/*
 * @-
 * Estimate the size of a BAT to avoid multiple extends.
 */
static BUN
estimator(char *datafile)
{
	long size;
	char buf[BUFSIZ + 1], *s = buf;
	size_t nr = 0;
	FILE *f;
	f = fopen(datafile, "r");
	if (f == NULL)
		return 0;

	buf[BUFSIZ] = 0;
	if ((nr = fread(buf, 0, BUFSIZ, f)) > 0) {
		buf[nr] = 0;
		nr = 0;
		for (s = buf; *s; s++)
			if (*s == '\n')
				nr++;
	} else
		nr = 0;
	fseek(f, 0L, SEEK_END);
	size = ftell(f);
	fclose(f);
	if (nr == 0)
		return (BUN) (size / 40);	/* some handhaving for stream input */
	return (BUN) (((size / BUFSIZ + 1) / nr) * 1.3);	/* take some slack and take reduction */
}

BAT *
TABLETload(Tablet *as, char *datafile)
{
	BAT *res = NULL;
	stream *s = open_rastream(datafile);
	bstream *b = NULL;
	BUN est = as->nr + BATTINY;	/* take some reserve */

	if (s == NULL) {
		GDKerror("could not open file %s\n", datafile);
		return NULL;
	}
	if (mnstr_errnr(s)) {
		GDKerror("could not open file %s\n", datafile);
		mnstr_destroy(s);
		return NULL;
	}
	if (as->nr == BUN_NONE)
		est = estimator(datafile);

	b = bstream_create(s, SIZE);
	if (b) {
		if (TABLETcreate_bats(as, est) == 0 && TABLETload_file(as, b, NULL) != BUN_NONE)
			res = TABLETcollect_bats(as);
		bstream_destroy(b);
	}
	TABLETdestroy_format(as);
	return res;
}

void
TABLETdump(BAT *names, BAT *seps, BAT *bats, char *datafile, BUN nr)
{
	Tablet as;

	as.nr_attrs = 0;
	as.nr = nr;
	if (create_dumpformat(&as, names, seps, bats) != BUN_NONE && TABLETassign_BATs(&as, bats) != BUN_NONE) {
		stream *s = open_wastream(datafile);

		if (s != NULL && !mnstr_errnr(s) && dump_file(&as, s) >= 0) {
			mnstr_printf(GDKout, "#saved in %s\n", datafile);
		}
		if (s == NULL || mnstr_errnr(s)) {
			GDKerror("could not open file %s\n", datafile);
		} else {
			mnstr_close(s);
		}
		mnstr_destroy(s);
	}
	TABLETdestroy_format(&as);
}

int
TABLEToutput_file(Tablet *as, BAT *order, stream *s)
{
	oid base = oid_nil;
	BUN maxnr = BATcount(order);
	int ret = 0;

	/* only set nr if it is zero or lower (bogus) to the maximum value
	 * possible (BATcount), if already set within BATcount range,
	 * preserve value such that for instance SQL's reply_size still
	 * works
	 */
	if (as->nr == BUN_NONE || as->nr > maxnr)
		as->nr = maxnr;

	if ((base = check_BATs(as)) != oid_nil) {
		if (BAThdense(order) && order->hseqbase == base)
			ret = output_file_dense(as, s);
		else
			ret = output_file_ordered(as, order, s, base);
	} else {
		ret = output_file_default(as, order, s);
	}
	return ret;
}

BUN
TABLEToutput(BAT *order, BAT *seps, BAT *bats, stream *s)
{
	int res = 0;
	Tablet as;

	as.nr_attrs = 0;
	as.nr = BUN_NONE;
	if (create_dumpformat(&as, NULL, seps, bats) != BUN_NONE && TABLETassign_BATs(&as, bats) != BUN_NONE) {
		res = TABLEToutput_file(&as, order, s);
	}
	TABLETdestroy_format(&as);
	if (res >= 0)
		return as.nr;
	return BUN_NONE;
}

static void
tablet_load(BAT **bats, BAT *names, BAT *seps, BAT *types, str datafile, int *N)
{
	BUN nr = BUN_NONE;
	Tablet as;

	if (*N >= 0)
		nr = *N;
	as.nr_attrs = 0;
	as.nr = nr;
	as.tryall = 0;
	as.complaints = NULL;
	as.input = NULL;
	as.output = NULL;

	if (create_loadformat(&as, names, seps, types) != BUN_NONE)
		*bats = TABLETload(&as, datafile);
}

static void
tablet_dump(BAT *names, BAT *seps, BAT *bats, str datafile, int *nr)
{
	TABLETdump(names, seps, bats, datafile, *nr);
}

static void
tablet_output(BAT *order, BAT *seps, BAT *bats, void **s)
{
	(void) TABLEToutput(order, seps, bats, *(stream **) s);
}

/*
 * @+ MAL interface
 */
str
CMDtablet_load(int *ret, int *nameid, int *sepid, int *typeid, str *filename, int *nr)
{
	BAT *names, *seps, *types, *bn = NULL;

	if ((names = BATdescriptor(*nameid)) == NULL) {
		throw(MAL, "tablet.load", RUNTIME_OBJECT_MISSING);
	}
	if ((seps = BATdescriptor(*sepid)) == NULL) {
		BBPunfix(names->batCacheid);
		throw(MAL, "tablet.load", RUNTIME_OBJECT_MISSING);
	}
	if ((types = BATdescriptor(*typeid)) == NULL) {
		BBPunfix(names->batCacheid);
		BBPunfix(seps->batCacheid);
		throw(MAL, "tablet.load", RUNTIME_OBJECT_MISSING);
	}

	tablet_load(&bn, names, seps, types, *filename, nr);
	if (bn == NULL)
		throw(MAL, "tablet.load", MAL_MALLOC_FAIL);
	*ret = bn->batCacheid;
	BBPincref(*ret, TRUE);
	BBPunfix(*ret);
	BBPunfix(names->batCacheid);
	BBPunfix(seps->batCacheid);
	BBPunfix(types->batCacheid);
	return MAL_SUCCEED;
}

str
CMDtablet_dump(int *ret, int *nameid, int *sepid, int *bids, str *filename, int *nr)
{
	BAT *names, *seps, *bats;

	(void) ret;

	if ((names = BATdescriptor(*nameid)) == NULL) {
		throw(MAL, "tablet.dump", RUNTIME_OBJECT_MISSING);
	}
	if ((seps = BATdescriptor(*sepid)) == NULL) {
		BBPunfix(names->batCacheid);
		throw(MAL, "tablet.dump", RUNTIME_OBJECT_MISSING);
	}
	if ((bats = BATdescriptor(*bids)) == NULL) {
		BBPunfix(names->batCacheid);
		BBPunfix(seps->batCacheid);
		throw(MAL, "tablet.dump", RUNTIME_OBJECT_MISSING);
	}

	tablet_dump(names, seps, bats, *filename, nr);
	BBPunfix(names->batCacheid);
	BBPunfix(seps->batCacheid);
	BBPunfix(bats->batCacheid);
	return MAL_SUCCEED;
}

str
CMDtablet_input(int *ret, int *nameid, int *sepid, int *typeid, stream *s, int *nr)
{
	BAT *names, *seps, *types, *bn = NULL;
	bstream *bs = NULL;
	Tablet as;

	if ((names = BATdescriptor(*nameid)) == NULL) {
		throw(MAL, "tablet.load", RUNTIME_OBJECT_MISSING);
	}
	if ((seps = BATdescriptor(*sepid)) == NULL) {
		BBPunfix(names->batCacheid);
		throw(MAL, "tablet.load", RUNTIME_OBJECT_MISSING);
	}
	if ((types = BATdescriptor(*typeid)) == NULL) {
		BBPunfix(names->batCacheid);
		BBPunfix(seps->batCacheid);
		throw(MAL, "tablet.load", RUNTIME_OBJECT_MISSING);
	}

	as.nr_attrs = 0;
	as.nr = *nr;
	as.tryall = 0;
	as.complaints = NULL;
	as.input = NULL;
	as.output = NULL;

	bs = bstream_create(*(stream **) s, SIZE);
	if (bs) {
		if (create_loadformat(&as, names, seps, types) != BUN_NONE &&
			TABLETcreate_bats(&as, (BUN) 0) == 0 &&
			TABLETload_file(&as, bs, NULL) != BUN_NONE)
			 bn = TABLETcollect_bats(&as);
		bstream_destroy(bs);
	}
	TABLETdestroy_format(&as);

	if (bn == NULL) {
		BBPunfix(names->batCacheid);
		BBPunfix(seps->batCacheid);
		BBPunfix(types->batCacheid);
		throw(MAL, "tablet.load", OPERATION_FAILED);
	}
	*ret = bn->batCacheid;
	BBPincref(*ret, TRUE);
	BBPunfix(*ret);
	BBPunfix(names->batCacheid);
	BBPunfix(seps->batCacheid);
	BBPunfix(types->batCacheid);
	return MAL_SUCCEED;
}

str
CMDtablet_output(int *ret, int *nameid, int *sepid, int *bids, void **s)
{
	BAT *names, *seps, *bats;
	(void) ret;

	if ((names = BATdescriptor(*nameid)) == NULL) {
		throw(MAL, "tablet.output", RUNTIME_OBJECT_MISSING);
	}
	if ((seps = BATdescriptor(*sepid)) == NULL) {
		BBPunfix(names->batCacheid);
		throw(MAL, "tablet.output", RUNTIME_OBJECT_MISSING);
	}
	if ((bats = BATdescriptor(*bids)) == NULL) {
		BBPunfix(names->batCacheid);
		BBPunfix(seps->batCacheid);
		throw(MAL, "tablet.output", RUNTIME_OBJECT_MISSING);
	}
	tablet_output(names, seps, bats, s);
	BBPunfix(names->batCacheid);
	BBPunfix(seps->batCacheid);
	BBPunfix(bats->batCacheid);
	return MAL_SUCCEED;
}


/*
 * @+ Tablet report
 * The routines to manage the table descriptor
 */
static void
clearColumn(Column *c)
{
	int i;
	CLEAR(c->batname);
	CLEAR(c->name);
	CLEAR(c->type);
	CLEAR(c->sep);
	c->width = 0;
	c->tabs = 0;
	for (i = 0; i < SLICES; i++)
		c->c[i] = NULL;
	for (i = 0; i < BINS; i++)
		c->bin[i] = NULL;
	c->p = 0;
	/* keep nullstr and brackets */
}

static void
clearTable(Tablet *t)
{
	unsigned int i;

	for (i = 0; i < t->nr_attrs; i++) {
		clearColumn(t->columns + i);
		CLEAR(t->columns[i].lbrk);
		CLEAR(t->columns[i].rbrk);
		CLEAR(t->columns[i].nullstr);
	}
	CLEAR(t->ttopbrk);
	CLEAR(t->tbotbrk);
	CLEAR(t->rlbrk);
	CLEAR(t->rrbrk);
	CLEAR(t->properties);
	CLEAR(t->title);
	CLEAR(t->footer);
	CLEAR(t->sep);
	t->rowwidth = 0;
	t->nr_attrs = 0;
	t->firstrow = t->lastrow = 0;
	/* keep brackets and stream */
}

/*
 * @-
 * Expansion of a table report descriptor should not
 * mean loosing its content.
 */
static void
makeTableSpace(int rnr, unsigned int acnt)
{
	Tablet *t = 0;

	assert(rnr >= 0 && rnr < MAL_MAXCLIENTS);
	t = tableReports[rnr];
	if (t == 0) {
		int len = sizeof(Tablet) + acnt * sizeof(Column);

		t = tableReports[rnr] = (Tablet *) GDKzalloc(len);
		t->max_attrs = acnt;
	}
	if (t && t->max_attrs < acnt) {
		Tablet *tn;
		int len = sizeof(Tablet) + acnt * sizeof(Column);

		tn = tableReports[rnr] = (Tablet *) GDKzalloc(len);
		memcpy((char *) tn, (char *) t, sizeof(Tablet) + t->max_attrs * sizeof(Column));
		GDKfree(t);
		tn->max_attrs = acnt;
	}
}

/*
 * @-
 * Binding variables also involves setting the default
 * formatting scheme. These are based on the storage type only
 * at this point. The variables should be either all BATs
 * or all scalars. But this is to be checked just before
 * printing.
 */
static int
isScalarVector(Tablet *t)
{
	unsigned int i;

	for (i = 0; i < t->nr_attrs; i++)
		if (t->columns[i].c[0])
			return 0;
	return 1;
}

static int
isBATVector(Tablet *t)
{
	unsigned int i;
	BUN cnt;

	cnt = BATcount(t->columns[0].c[0]);
	for (i = 0; i < t->nr_attrs; i++)
		if (t->columns[i].c[0] == 0)
			return 0;
		else if (BATcount(t->columns[i].c[0]) != cnt)
			return 0;
	return 1;
}

static str
bindVariable(Tablet *t, unsigned int anr, str nme, int tpe, ptr val, int *k)
{
	Column *c;
	char *buf;
	int tpeStore;

	c = t->columns + anr;
	tpeStore = ATOMstorage(tpe);
	c->type = GDKstrdup(ATOMname(tpeStore));
	c->adt = tpe;
	c->name = GDKstrdup(nme);
	if (c->rbrk == 0)
		c->rbrk = GDKstrdup(",");
	c->width = (int) strlen(nme);	/* plus default bracket(s) */

	if (anr >= t->nr_attrs)
		t->nr_attrs = anr + 1;
	buf = (char *) GDKzalloc(BUFSIZ);

	if (tpe == TYPE_bat) {
		BAT *b;
		int bid = *(int *) val;

		if ((b = BATdescriptor(bid)) == NULL) {
			throw(MAL, "tablet.bindVariable", RUNTIME_OBJECT_MISSING);
		}

		c->c[0] = b;
		c->ci[0] = bat_iterator(b);
		/* the first column should take care of leader text size */
		if (c->c[0])
			setTabwidth(c);
	} else if (val) {
		if (ATOMstorage(tpe) == TYPE_str || ATOMstorage(tpe) > TYPE_str)
			val = *(str *) val;	/* V5 */
		(*BATatoms[tpe].atomToStr) (&buf, k, val);
		c->width = MAX(c->width, (unsigned int) strlen(buf));
		if (c->lbrk)
			c->width += (int) strlen(c->lbrk);
		if (c->rbrk)
			c->width += (int) strlen(c->rbrk);
	}
	GDKfree(buf);

	c->data = val;
	if (c->scale)
		c->width = c->scale + 2;	/* decimal point  and '-' */
	c->width += (unsigned int) ((c->rbrk ? strlen(c->rbrk) : 0) + (c->lbrk ? strlen(c->lbrk) : 0));
	if (c->maxwidth && c->maxwidth < c->width)
		c->width = c->maxwidth;
	if (t->columns == c)
		c->width += t->rlbrk ? (int) strlen(t->rlbrk) : 0;
	c->tabs = 1 + (c->width) / 8;
	t->rowwidth += 8 * c->tabs;
	*k = c->width;
	if (c->c[0])
		BBPunfix(c->c[0]->batCacheid);
	return NULL;
}

/*
 * @+ Actual printing
 */


void
TABshowHeader(Tablet *t)
{
	unsigned int i;
	char *prop = "name", *p, *q;

	if (t->title)
		mnstr_write(t->fd, t->title, 1, strlen(t->title));
	else {
		LINE(t->fd, t->rowwidth);
	}

	p = t->properties ? t->properties : prop;
	while (p) {
		q = strchr(p, ',');
		if (q)
			*q = 0;
		mnstr_write(t->fd, "% ", 1, 2);
		for (i = 0; i < t->nr_attrs; i++) {
			Column *c = t->columns + i;
			unsigned int len;
			str prop = 0;
			int u = 0, v = 0;
			char buf[32];

			if (strcmp(p, "name") == 0)
				prop = c->name;
			else if (strcmp(p, "type") == 0)
				prop = c->type;
			else if (!isScalar(c) && c->c[0]) {
				if (strcmp(p, "bat") == 0) {
					prop = BBPname(c->c[0]->batCacheid);
				}
				if (strcmp(p, "name") == 0) {
					prop = GDKstrdup(c->c[0]->tident);
				}
				if (strcmp(p, "base") == 0) {
					snprintf(buf, sizeof(buf), OIDFMT, c->c[0]->hseqbase);
					prop = GDKstrdup(buf);
				}
				if (strcmp(p, "sorted") == 0) {
					if (BATtordered(c->c[0]) & 1)
						prop = GDKstrdup("true");
					else
						prop = GDKstrdup("false");
				}
				if (strcmp(p, "dense") == 0) {
					if (BATtdense(c->c[0]))
						prop = GDKstrdup("true");
					else
						prop = GDKstrdup("false");
				}
				if (strcmp(p, "key") == 0) {
					if (c->c[0]->tkey)
						prop = GDKstrdup("true");
					else
						prop = GDKstrdup("false");
				}
				if (strcmp(p, "min") == 0) {
					switch (c->adt) {
					case TYPE_int:{
						int m;
						BATmin(c->c[0], &m);
						snprintf(buf, sizeof(buf), "%d", m);
						prop = GDKstrdup(buf);
						break;
					}
					case TYPE_lng:{
						lng m;
						BATmin(c->c[0], &m);
						snprintf(buf, sizeof(buf), LLFMT, m);
						prop = GDKstrdup(buf);
						break;
					}
					case TYPE_sht:{
						sht m;
						BATmin(c->c[0], &m);
						snprintf(buf, sizeof(buf), "%d", m);
						prop = GDKstrdup(buf);
						break;
					}
					case TYPE_dbl:{
						dbl m;
						BATmin(c->c[0], &m);
						snprintf(buf, sizeof(buf), "%f", m);
						prop = GDKstrdup(buf);
						break;
					}
					default:
						prop = GDKstrdup("");
					}
				}
				if (strcmp(p, "max") == 0) {
					switch (c->adt) {
					case TYPE_int:{
						int m;
						BATmax(c->c[0], &m);
						snprintf(buf, sizeof(buf), "%d", m);
						prop = GDKstrdup(buf);
						break;
					}
					case TYPE_lng:{
						lng m;
						BATmax(c->c[0], &m);
						snprintf(buf, sizeof(buf), LLFMT, m);
						prop = GDKstrdup(buf);
						break;
					}
					case TYPE_sht:{
						sht m;
						BATmax(c->c[0], &m);
						snprintf(buf, sizeof(buf), "%d", m);
						prop = GDKstrdup(buf);
						break;
					}
					case TYPE_dbl:{
						dbl m;
						BATmax(c->c[0], &m);
						snprintf(buf, sizeof(buf), "%f", m);
						prop = GDKstrdup(buf);
						break;
					}
					default:
						prop = GDKstrdup("");
					}
				}
			}
			len = prop ? (int) strlen(prop) : 0;
			if (c->maxwidth && len > c->maxwidth)
				len = c->maxwidth;
			if (c->lbrk)
				mnstr_write(t->fd, c->lbrk, 1, u = (int) strlen(c->lbrk));
			mnstr_write(t->fd, prop, len, 1);
			if (c->rbrk && i + 1 < t->nr_attrs)
				mnstr_write(t->fd, c->rbrk, 1, v = (int) strlen(c->rbrk));
			if (c == t->columns)
				len += t->rlbrk ? (int) strlen(t->rlbrk) : 0;
			TABS(t->fd, c->tabs - ((len + u + v) / 8));
			if (prop) {
				GDKfree(prop);
				prop = 0;
			}

		}
		mnstr_write(t->fd, "# ", 1, 2);
		mnstr_write(t->fd, p, 1, (int) strlen(p));
		mnstr_write(t->fd, "\n", 1, 1);
		if (q) {
			*q = ',';
			p = q + 1;
		} else
			p = 0;
	}

	if (t->tbotbrk == 0) {
		LINE(t->fd, t->rowwidth);
	} else
		mnstr_write(t->fd, t->tbotbrk, 1, (int) strlen(t->tbotbrk));
}

void
TABshowRow(Tablet *t)
{
	unsigned int i = 0;
	unsigned int m = 0;
	int zero = 0;
	char *buf = 0;
	Column *c = t->columns + i;
	unsigned int len;
	int u = 0, v = 0;

	buf = (char *) GDKmalloc(m = t->rowwidth);

	if (buf == NULL)
		return;
	if (t->rlbrk)
		mnstr_printf(t->fd, "%s", t->rlbrk);
	for (i = 0; i < t->nr_attrs; i++) {
		c = t->columns + i;
		u = 0;
		v = 0;
		if (c->data)
			(*BATatoms[c->adt].atomToStr) (&buf, &zero, c->data);
		m = (unsigned int) zero;
		if (strcmp(buf, "nil") == 0 && c->nullstr && strlen(c->nullstr) < m)
			strcpy(buf, c->nullstr);
		if (c->precision) {
			if (strcmp(buf, "nil") == 0) {
				snprintf(buf, m, "%*s", c->scale + (c->precision ? 1 : 0), "nil");
			} else if (c->adt == TYPE_int) {
				int vi = *(int *) c->data, vj = vi, m = 1;
				int k;

				for (k = c->precision; k > 0; k--) {
					vi /= 10;
					m *= 10;
				}
				snprintf(buf, m, "%*d.%d", c->scale - c->precision, vi, vj % m);
			}
		}
		len = (int) strlen(buf);
		if (c->maxwidth && len > c->maxwidth)
			len = c->maxwidth;
		if (c->lbrk)
			mnstr_write(t->fd, c->lbrk, 1, u = (int) strlen(c->lbrk));
		mnstr_write(t->fd, buf, 1, len);
		if (c->rbrk) {
			v = (int) strlen(c->rbrk);
			if (i + 1 < t->nr_attrs) {
				mnstr_write(t->fd, c->rbrk, 1, v);
			} else if (*c->rbrk != ',')
				mnstr_write(t->fd, c->rbrk, 1, v);
		}

		if (c == t->columns)
			len += t->rlbrk ? (int) strlen(t->rlbrk) : 0;
		TABS(t->fd, c->tabs - ((len + u + v - 1) / 8));
	}
	if (t->rrbrk)
		mnstr_printf(t->fd, "%s\n", t->rrbrk);
	GDKfree(buf);
}

void
TABshowRange(Tablet *t, lng first, lng last)
{
	BUN i, j;
	oid k;
	BATiter pi;

	assert(first <= (lng) BUN_MAX);
	assert(last <= (lng) BUN_MAX);

	i = BATcount(t->columns[0].c[0]);
	if (last < 0 || last > (lng) i)
		last = (lng) i;
	if (first < 0)
		first = 0;

	pi = bat_iterator(t->pivot);
	for (i = (BUN) first; i < (BUN) last; i++) {
		if (t->pivot) {
			k = *(oid *) BUNtail(pi, i);
		} else
			k = (oid) i;
		for (j = 0; j < t->nr_attrs; j++) {
			t->columns[j].data = BUNtail(t->columns[j].ci[0], k);
		}
		TABshowRow(t);
	}
}

static void
TABshowPage(Tablet *t)
{
	/* if( t->ttopbrk==0) { LINE(t->fd,t->rowwidth); }
	   else mnstr_printf(t->fd, "%s\n", t->ttopbrk); */
	TABshowRange(t, 0, -1);
	if (t->tbotbrk == 0) {
		LINE(t->fd, t->rowwidth);
	} else
		mnstr_printf(t->fd, "%s\n", t->tbotbrk);
}

/*
 * @+ V4 stuff
 * The remainder is a patched copy of material from gdk_storage.
 */
typedef int (*strFcn) (str *s, int *len, ptr val);

#define printfcn(b)	((b->ttype==TYPE_void && b->tseqbase==oid_nil)?	\
			          print_nil:BATatoms[b->ttype].atomToStr)
static int
print_nil(char **dst, int *len, ptr dummy)
{
	(void) dummy;				/* fool compiler */
	if (*len < 3) {
		if (*dst)
			GDKfree(*dst);
		*dst = (char *) GDKmalloc(*len = 40);
	}
	if (*dst)
		strcpy(*dst, "nil");
	return 3;
}

static int
setTabwidth(Column *c)
{
	strFcn tostr = printfcn(c->c[0]);
	BUN cnt = BATcount(c->c[0]);
	int ret = 0;
	unsigned int max;
	int t = BATttype(c->c[0]);
	char *buf = 0;
	char *title = c->c[0]->tident;

	if (strcmp(c->c[0]->tident, "t") == 0) {
		title = BATgetId(c->c[0]);
	}
	CLEAR(c->type);
	c->type = GDKstrdup(ATOMname(c->c[0]->ttype));
	c->adt = c->c[0]->ttype;
	max = MAX((int) strlen(c->type), ret);
	if (c->nullstr)
		max = MAX(max, (unsigned int) strlen(c->nullstr));
	if (c->lbrk)
		max += (int) strlen(c->lbrk);
	if (c->rbrk)
		max += (int) strlen(c->rbrk);

	if (t >= 0 && t < GDKatomcnt && tostr) {
		BUN off = BUNfirst(c->c[0]);
		BUN j, i, probe = MIN(10000, MAX(200, cnt / 100));

		for (i = 0; i < probe; i++) {
			if (i >= cnt)
				break;
			j = off + ((cnt < probe) ? i : ((BUN) rand() % cnt));
			(*tostr) (&buf, &ret, BUNtail(c->ci[0], j));
			max = MAX(max, (unsigned int) strlen(buf));
			GDKfree(buf);
			buf = 0;
			ret = 0;
		}
	}
	c->width = max;
	CLEAR(c->name);
	c->name = GDKstrdup(title);
	return c->width;
}
