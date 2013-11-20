#include "monetdb_config.h"
#include "sql.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include <sql_storage.h>
#include <sql_scenario.h>
#include <store_sequence.h>
#include <sql_optimizer.h>
#include <sql_datetime.h>
#include <rel_optimizer.h>
#include <rel_distribute.h>
#include <rel_select.h>
#include <rel_exp.h>
#include <rel_dump.h>
#include <rel_bin.h>
#include <bbp.h>
#include <cluster.h>
#include <opt_dictionary.h>
#include <opt_pipes.h>
#include "clients.h"
#ifdef HAVE_RAPTOR
# include <rdf.h>
#endif
#include "mal_instruction.h"

/*
 * Shredding RDF documents through SQL
 * Wrapper around the RDF shredder of the rdf module of M5.
 *
 * An rdf file can be now shredded with SQL command:
 * CALL rdf_shred('/path/to/location','graph name');
 *
 * The table rdf.graph will be updated with an entry of the form:
 * [graph name, graph id] -> [gname,gid].
 *
 * In addition all permutation of SPO for the specific rdf document will be
 * created. The name of the triple tables are rdf.pso$gid$, rdf.spo$gid$ etc.
 * For example if gid = 3 then rdf.spo3 is the triple table ordered on subject,
 * property, object. Finally, there is one more table called rdf.map$gid$ that
 * maps oids to strings (i.e., the lexical representation).
 */
str
SQLrdfShred(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
#ifdef HAVE_RAPTOR
	BAT *b[128];
	BAT *p, *s, *o;
	sql_schema *sch;
	sql_table *g_tbl;
	sql_column *gname, *gid;
#if STORE == TRIPLE_STORE
	sql_table *spo_tbl, *sop_tbl, *pso_tbl, *pos_tbl, *osp_tbl, *ops_tbl;
#elif STORE == MLA_STORE
	sql_table *spo_tbl;
#endif /* STORE */
	sql_table *map_tbl;
	sql_subtype tpe;
	str *location = (str *) getArgReference(stk,pci,1);
	str *name = (str *) getArgReference(stk,pci,2);
	str *schema = (str *) getArgReference(stk,pci,3);
	char buff[24];
	mvc *m = NULL;
	int id = 0;
	oid rid = oid_nil;
	str msg = getSQLContext(cntxt, mb, &m, NULL);

	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((sch = mvc_bind_schema(m, *schema)) == NULL)
		throw(SQL, "sql.rdfShred", "3F000!schema missing");

	g_tbl = mvc_bind_table(m, sch, "graph");
	gname = mvc_bind_column(m, g_tbl, "gname");
	gid = mvc_bind_column(m, g_tbl, "gid");

	rid = table_funcs.column_find_row(m->session->tr, gname, *name, NULL);
	if (rid != oid_nil)
		throw(SQL, "sql.rdfShred", "graph name already exists in rdf.graph");

	id = (int) store_funcs.count_col(m->session->tr, gname, 1);
	store_funcs.append_col(m->session->tr, gname, *name, TYPE_str);
	store_funcs.append_col(m->session->tr, gid, &id, TYPE_int);

	rethrow("sql.rdfShred", msg, RDFParser(b, location, name, schema));

	if (sizeof(oid) == 8) {
		sql_find_subtype(&tpe, "oid", 31, 0);
		/* todo for niels: if use int/bigint the @0 is serialized */
		/* sql_find_subtype(&tpe, "bigint", 64, 0); */
	} else {
		sql_find_subtype(&tpe, "oid", 31, 0);
		/* sql_find_subtype(&tpe, "int", 32, 0); */
	}
#if STORE == TRIPLE_STORE
	sprintf(buff, "spo%d", id);
	spo_tbl = mvc_create_table(m, sch, buff, tt_table, 0,
				   SQL_PERSIST, 0, 3);
	mvc_create_column(m, spo_tbl, "subject", &tpe);
	mvc_create_column(m, spo_tbl, "property", &tpe);
	mvc_create_column(m, spo_tbl, "object", &tpe);

	sprintf(buff, "sop%d", id);
	sop_tbl = mvc_create_table(m, sch, buff, tt_table, 0,
				   SQL_PERSIST, 0, 3);
	mvc_create_column(m, sop_tbl, "subject", &tpe);
	mvc_create_column(m, sop_tbl, "object", &tpe);
	mvc_create_column(m, sop_tbl, "property", &tpe);

	sprintf(buff, "pso%d", id);
	pso_tbl = mvc_create_table(m, sch, buff, tt_table, 0,
				   SQL_PERSIST, 0, 3);
	mvc_create_column(m, pso_tbl, "property", &tpe);
	mvc_create_column(m, pso_tbl, "subject", &tpe);
	mvc_create_column(m, pso_tbl, "object", &tpe);

	sprintf(buff, "pos%d", id);
	pos_tbl = mvc_create_table(m, sch, buff, tt_table, 0,
				   SQL_PERSIST, 0, 3);
	mvc_create_column(m, pos_tbl, "property", &tpe);
	mvc_create_column(m, pos_tbl, "object", &tpe);
	mvc_create_column(m, pos_tbl, "subject", &tpe);

	sprintf(buff, "osp%d", id);
	osp_tbl = mvc_create_table(m, sch, buff, tt_table, 0,
				   SQL_PERSIST, 0, 3);
	mvc_create_column(m, osp_tbl, "object", &tpe);
	mvc_create_column(m, osp_tbl, "subject", &tpe);
	mvc_create_column(m, osp_tbl, "property", &tpe);

	sprintf(buff, "ops%d", id);
	ops_tbl = mvc_create_table(m, sch, buff, tt_table, 0,
				   SQL_PERSIST, 0, 3);
	mvc_create_column(m, ops_tbl, "object", &tpe);
	mvc_create_column(m, ops_tbl, "property", &tpe);
	mvc_create_column(m, ops_tbl, "subject", &tpe);

#elif STORE == MLA_STORE
	sprintf(buff, "spo%d", id);
	spo_tbl = mvc_create_table(m, sch, buff, tt_table,
				   0, SQL_PERSIST, 0, 3);
	mvc_create_column(m, spo_tbl, "subject", &tpe);
	mvc_create_column(m, spo_tbl, "property", &tpe);
	mvc_create_column(m, spo_tbl, "object", &tpe);
#endif /* STORE */

	sprintf(buff, "map%d", id);
	map_tbl = mvc_create_table(m, sch, buff, tt_table, 0, SQL_PERSIST, 0, 2);
	mvc_create_column(m, map_tbl, "sid", &tpe);
	sql_find_subtype(&tpe, "varchar", 1024, 0);
	mvc_create_column(m, map_tbl, "lexical", &tpe);

	s = b[MAP_LEX];
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, map_tbl, "lexical"),
			BATmirror(BATmark(BATmirror(s),0)), TYPE_bat);
	store_funcs.append_col(m->session->tr,
			mvc_bind_column(m, map_tbl, "sid"),
			BATmirror(BATmark(s, 0)),
			TYPE_bat);
	BBPunfix(s->batCacheid);

#if STORE == TRIPLE_STORE
	s = b[S_sort];
	p = b[P_PO];
	o = b[O_PO];
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, spo_tbl, "subject"),
			       s, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, spo_tbl, "property"),
			       p, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, spo_tbl, "object"),
			       o, TYPE_bat);
	s = b[S_sort];
	p = b[P_OP];
	o = b[O_OP];
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, sop_tbl, "subject"),
			       s, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, sop_tbl, "property"),
			       p, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, sop_tbl, "object"),
			       o, TYPE_bat);
	s = b[S_SO];
	p = b[P_sort];
	o = b[O_SO];
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, pso_tbl, "subject"),
			       s, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, pso_tbl, "property"),
			       p, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, pso_tbl, "object"),
			       o, TYPE_bat);
	s = b[S_OS];
	p = b[P_sort];
	o = b[O_OS];
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, pos_tbl, "subject"),
			       s, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, pos_tbl, "property"),
			       p, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, pos_tbl, "object"),
			       o, TYPE_bat);
	s = b[S_SP];
	p = b[P_SP];
	o = b[O_sort];
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, osp_tbl, "subject"),
			       s, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, osp_tbl, "property"),
			       p, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, osp_tbl, "object"),
			       o, TYPE_bat);
	s = b[S_PS];
	p = b[P_PS];
	o = b[O_sort];
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, ops_tbl, "subject"),
			       s, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, ops_tbl, "property"),
			       p, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, ops_tbl, "object"),
			       o, TYPE_bat);
#elif STORE == MLA_STORE
	s = b[S_sort];
	p = b[P_sort];
	o = b[O_sort];
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, spo_tbl, "subject"),
			       s, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, spo_tbl, "property"),
			       p, TYPE_bat);
	store_funcs.append_col(m->session->tr,
			       mvc_bind_column(m, spo_tbl, "object"),
			       o, TYPE_bat);
#endif /* STORE */

	/* unfix graph */
	for(id=0; b[id]; id++) {
		BBPunfix(b[id]->batCacheid);
	}
	return MAL_SUCCEED;
#else
	(void) cntxt; (void) mb; (void) stk; (void) pci;
	throw(SQL, "sql.rdfShred", "RDF support is missing from MonetDB5");
#endif /* RDF */
}
