
#include "optimize.h"
#include "statement.h"
#include <stdio.h>

#define create_stmt_list() list_create((fdestroy)&stmt_destroy)

/* key is a join output */
static int cmp_sel_head( stmt *sel, stmt *key )
{
	if (key->h && key->h == sel->h){
		return 0;
	}
	return -1;
}

static int cmp_sel_tail( stmt *sel, stmt *key )
{
	if (key->t && key->t == sel->h){
		return 0;
	}
	return -1;
}

static int stmt_cmp_head_tail( stmt *h, stmt *key )
{
	if (h->nrcols != key->nrcols ) {
		return -1;
	}
	if (h->nrcols == 1 && h->h == key->h){
		return 0;
	}
	if (h->nrcols == 2 && (
		(h->h == key->h && h->t == key->t) ||
		(h->t == key->h && h->h == key->t) )){
		return 0;
	}
	return -1;
}

static int select_count( stmt *s )
{
	switch(s->type){
		case st_intersect: return select_count( s->op1.stval );
		case st_join: return select_count(s->op1.stval) +
					select_count(s->op2.stval);
		case st_reverse: return select_count(s->op1.stval);
				 /*
		case st_semijoin: return 1;
		*/
		case st_semijoin: return select_count(s->op1.stval) +
					select_count(s->op2.stval);
		case st_select: return 1;
		default: return 0;
	}
}

static stmt *stmt_smallest( stmt *h, stmt *key )
{
	int h1 = select_count(h);
	int h2 = select_count(key);

	if (h1 > h2){
		stmt_destroy(key);
		return h;
	} else {
		stmt_destroy(h);
		return key;
	}
}

static int pivot_cmp_tail( stmt *piv, stmt *key )
{
	if (key->t && key->t == piv->t){
		return 0;
	}
	return -1;
}


static int data_cmp( void *d, void *k )
{
	if (d == k) 
		return 0;
	return -1;
}

/*
 * The push_selects_down function converts a conjunction set into a
 * reduced conjuntion tree where the selects are grouped together and
 * pushed under the join. Also joins are grouped together.
 * */
static list *push_selects_down(list * con){
	int one = 1;
	int two = 2;

	list *rsel = create_stmt_list();
	list *res = create_stmt_list();
	list *sels, *dsels, *joins, *djoins;
	node *n = NULL;
       
	sels = list_select(con, (void*)&one, (fcmp)&stmt_cmp_nrcols, (fdup)&stmt_dup );
	dsels = list_distinct( sels, (fcmp)&stmt_cmp_head_tail, (fdup)&stmt_dup );

	joins = list_select(con, (void*)&two, (fcmp)&stmt_cmp_nrcols, (fdup)&stmt_dup );
	djoins = list_distinct( joins, (fcmp)&stmt_cmp_head_tail, (fdup)&stmt_dup );
	for( n = dsels->h; n; n = n->next){
		list *esels = list_select(sels, n->data, (fcmp)&stmt_cmp_head_tail, (fdup)&stmt_dup );
		stmt *sel = (stmt*)list_reduce(esels, (freduce)&stmt_semijoin, (fdup)&stmt_dup );
		list_destroy(esels);
		/* todo check for foreign key selects */
		list_append(rsel, sel);
	}
	list_destroy(sels);
	list_destroy(dsels);

	/* todo join order rewrites */
	for( n = djoins->h; n; n = n->next){
		list *ejoins = list_select(joins, n->data, (fcmp)&stmt_cmp_head_tail, (fdup)&stmt_dup );
		/* TODO change to [].().select(TRUE) */
		stmt *join = (stmt*)list_reduce(ejoins, (freduce)&stmt_intersect, (fdup)&stmt_dup );

		/* todo check for foreign key joins */
		node *hsel = list_find(rsel, (void*)join, (fcmp)&cmp_sel_head);
		node *tsel = list_find(rsel, (void*)join, (fcmp)&cmp_sel_tail);

		list_destroy(ejoins);

		if (hsel){
			join = stmt_push_down_head(join, stmt_dup(hsel->data));
		}
		if (tsel){
			join = stmt_push_down_tail(join, stmt_dup(tsel->data));
		}
		list_append(res, join);
	}
	/* find least expensive join (with head/tail selects) */
	/* continue finding usages of this until done */

	list_destroy(joins);
	list_destroy(djoins);
	if (!list_length(res)){
		if (list_length(rsel) == 1){
			list_destroy(res);
			return rsel;
		} else {
			printf("error: more then one selected but no joins\n");
		}
	}
	list_destroy(rsel);
	return res;
}

list *pivot( list *pivots, stmt *st, stmt *p, int markid ){
	list *npivots;
	stmt *j, *pnl, *pnr;
	node *pn;

	node *tp = list_find(pivots, (void*)st, (fcmp)&pivot_cmp_tail);

	j = stmt_push_join_head(st, p);
	if (tp){
		/* convert to a select [op](l,r).select(TRUE); */
		j = stmt_push_join_tail(j, stmt_reverse(stmt_dup(tp->data)));
		j = stmt_join2select(j);
	}

	/*
	pnl = stmt_mark(j, markid);
	*/
	pnr = stmt_mark(stmt_reverse(j), markid);

	/*
	if (tp)
		pnr = stmt_intersect(pnr,pnl);
		*/

	npivots = create_stmt_list();
	if (!tp) {
		pnl = stmt_mark(stmt_dup(j), markid);
		list_append (npivots, pnl );
	}
	for (pn = pivots->h; pn; pn = pn->next){
		list_append (npivots, stmt_join (stmt_dup(pnr), stmt_dup(pn->data), cmp_equal));
	}
	stmt_destroy(pnr);
	return npivots;
}

/* merge_pivot_sets tries to merge pivot sets using the joins in the join set
 * returns a set of pivot sets.
 * */
/*
static list *merge_pivot_sets(list *pivotsets, list *joins)
{
	stmt *st;
	if (list_length(joins) == 0)
		return NULL;
	if (!pivotsets){
		list *pivots = create_stmt_list();
		node *n = joins->h;
		pivotsets = list_create((fdestroy)&list_destroy);
		st = n->data;

		if (st->nrcols == 1) {
			list_append(pivots, 
					stmt_mark(stmt_reverse(st), markid++));
		}
		if (st->nrcols == 2) {
			list_append(pivots, 
					stmt_mark (stmt_reverse(st), markid));
			list_append(pivots, 
					stmt_mark(st, markid++));
		}
		list_append(pivotsets,pivots);
	}
}

static list *pivot_sets(list *joins, int *Markid)
{
	int markid = *Markid;
	list *psets = list_create((fdestroy)&list_destroy);
	node *n;

	for( n = joins->h; n; n = n->next ){
		list *pivots = create_stmt_list();
		stmt *st = n->data;

		if (st->nrcols == 1) {
			list_append(pivots, stmt_mark(stmt_reverse(stmt_dup(st)), markid++));
		}
		if (st->nrcols == 2) {
			list_append(pivots, stmt_mark (stmt_reverse(stmt_dup(st)), markid));
			list_append(pivots, stmt_mark(stmt_dup(st), markid++));
		}
		list_append(psets,pivots);
	}
	*Markid = markid;
	return psets;
}
*/

static stmt *set2pivot(list * l)
{
	list *pivots = create_stmt_list();
	stmt *join, *st;
	node *n;
	int len = 0;
	int markid = 0;

	l = push_selects_down(l);
	n = l->h;

	join = (stmt*)list_reduce(l, (freduce)&stmt_smallest, (fdup)&stmt_dup );
	n = list_find(l, (void*)join, data_cmp);
	stmt_destroy(join);
	if (!n) {
		list_destroy(l);
		list_destroy(pivots);
		return NULL;
	}
	st = n->data;
	if (st->nrcols == 1) {
		list_append(pivots, stmt_mark(stmt_reverse(stmt_dup(st)), markid++));
	}
	if (st->nrcols == 2) {
		list_append(pivots, stmt_mark (stmt_reverse(stmt_dup(st)), markid));
		list_append(pivots, stmt_mark( stmt_dup(st), markid++));
	}
	n = list_remove_node(l, n);
	len = list_length(l) + 1;
	while (list_length(l) > 0 && len > 0) {
		len--;
		n = l->h;
		while (n) {
			stmt *st = n->data;
			node *p = NULL;
			for ( p = pivots->h; p; p = p->next) {
				list *nps = NULL;
				stmt *pv = p->data;
				if (pv->t == st->h) {
					nps = pivot(pivots, stmt_dup(st), stmt_dup(pv), markid++);
					list_destroy(pivots);
					pivots = nps;
					n = list_remove_node(l, n);
					break;
				} else if (pv->t == st->t) {
					nps = pivot(pivots, stmt_reverse(stmt_dup(st)), stmt_dup(pv), markid++);
					list_destroy(pivots);
					pivots = nps;
					n = list_remove_node(l, n);
					break;
				}
			}
			if (n)
				n = n->next;
		}
	}
	if (!len) {
		assert(0);
		return NULL;
	}
	list_destroy(l);
	return stmt_list(pivots);
}

/* The group code is needed for double elimination.
 * if a value is selected twice once on the left hand of the
 * 'OR' and once on the right hand of the 'OR' it will be in the
 * result twice.
 *
 * current version is broken, unique also remove normal doubles.
 */
static stmt *sets2pivot(list * ll)
{
	node *n = ll->h;
	if (n) {
		stmt *pivots = set2pivot(n->data);
		n = n->next;
		while (n) {
			stmt *npivots = set2pivot(n->data);
			list *l = npivots->op1.lval;
			list *inserts = create_stmt_list();

			node *m, *c;

			/* we use a special bat insert to garantee unique head 
			 * oids 
			 */
			for (m = l->h ; m; m = m->next) {
				for (c = pivots->op1.lval->h; c; c = c->next){
					stmt *cd = c->data;
					stmt *md = m->data;
					if (cd->t == md->t) {
						list_append (inserts, stmt_append (stmt_dup(cd), stmt_dup(md)));
						break;
					}
				}
			}
			stmt_destroy(pivots);
			stmt_destroy(npivots);
			pivots = stmt_list(inserts);
			n = n->next;
		}
		{
		   group *g = NULL;
		   stmt *u;
		   node *m;
		   list *inserts = create_stmt_list();

		   for( m = pivots->op1.lval->h; m; m = m->next){
		   	g = grp_create(stmt_dup(m->data), g);
		   }
		   u = stmt_reverse( stmt_unique( stmt_reverse( stmt_dup(g->ext) ), NULL));
		   grp_destroy(g);

		   for( m = pivots->op1.lval->h; m; m = m->next){
		   	list_append( inserts, 
				stmt_semijoin( stmt_dup(m->data), stmt_dup(u)));
		   }
		   stmt_destroy(u);
		   stmt_destroy(pivots);
		   return stmt_list(inserts);
		}
		/* no double elimination jet 
		return pivots;
		 */
	}
	return NULL;
}

static stmt *stmt2pivot(stmt * s)
{
	if (s->type != st_set && s->type != st_sets) {
		s = stmt_set(s);
	}
	if (s->type == st_sets) {
		stmt *ns = sets2pivot(s->op1.lval);
		stmt_destroy(s);
		s = ns;
	} else {
		stmt *ns = set2pivot(s->op1.lval);
		stmt_destroy(s);
		s = ns;
	}
	return s;
}

static stmt *find_pivot(stmt * subset, stmt * t)
{
	if (t){
		node *n;
		for (n = subset->op1.lval->h; n; n = n->next) {
			stmt *s = n->data;
			if (s->t == t) 
				return stmt_dup(s);
		}
	}
	assert(0);
	return NULL;
}


stmt *optimize( context *c, stmt *s ){
	stmt *res = s;

	switch(s->type){
	/* first just return those statements which we cannot optimize,
	 * such as schema manipulation, transaction managment, 
	 * and user authentication.
	 */
	case st_none:
	case st_release: case st_commit: case st_rollback: 
	case st_schema: case st_table: case st_column: case st_key: 
	case st_create_schema: case st_drop_schema: 
	case st_create_table: case st_drop_table: 
	case st_create_column: case st_null: case st_default: 
	case st_create_key: 
	case st_create_role: case st_drop_role: 
	case st_grant_role: case st_revoke_role: 
	case st_grant: case st_revoke:

	case st_dbat: case st_obat: case st_basetable: case st_kbat:

	case st_atom: 
	case st_copyfrom: 

		s->optimized = 1;
		return stmt_dup(s);

	case st_temp:
	case st_select: case st_select2: case st_like: case st_semijoin: 
	case st_diff: case st_intersect: case st_union: case st_outerjoin:
	case st_join: 
	case st_reverse: case st_const: case st_mark: 
	case st_group: case st_group_ext: case st_derive: case st_unique: 
	case st_limit: case st_order: case st_reorder: case st_ordered: 

	case st_alias: case st_column_alias: 
	case st_ibat: 
	case st_output: 
	case st_append: case st_insert: case st_replace: 
	case st_exception:

	case st_count: case st_aggr: 
	case st_op: case st_unop: case st_binop: case st_triop: 

		if (s->optimized) 
			return stmt_dup(s);

		if (s->op1.stval){
			stmt *os = s->op1.stval;
		       	stmt *ns = optimize(c, os);
			s->op1.stval = ns;
			stmt_destroy(os);
		}
		if (s->op2.stval){
			stmt *os = s->op2.stval;
		       	stmt *ns = optimize(c, os);
			s->op2.stval = ns;
			stmt_destroy(os);
		}
		if (s->op3.stval){
			stmt *os = s->op3.stval;
		       	stmt *ns = optimize(c, os);
			s->op3.stval = ns;
			stmt_destroy(os);
		}
		s->optimized = 1;
		return stmt_dup(s);

	case st_list: {
		stmt *res = NULL;
		node *n;
		list *l = s->op1.lval;
		list *nl = NULL;

		if (s->optimized) 
			return stmt_dup(s);

		nl = create_stmt_list();
		for(n = l->h; n; n = n->next ){
			stmt *ns = optimize(c, n->data);
			list_append(nl, ns);
		}
		res = stmt_list(nl);
		res->optimized = 1;
		return res;
	} 

	case st_bat: case st_ubat: 
		if (s->optimized) 
			return stmt_dup(s);
		if (s->op1.cval->table->type == tt_view){
			return optimize(c, s->op1.cval->s);
		} else {
			s->optimized = 1;
			return stmt_dup(s);
		}

	case st_pivot: {
		stmt *ns = optimize(c, s->op2.stval); /* optimize ptable */
		stmt *np = find_pivot(ns, s->op1.stval->h);
		stmt_destroy(ns);
		return np;
	}
	case st_ptable:
		if (!s->op3.stval){ /* use op3 to store the new ptable */
			stmt *pivots = stmt2pivot(stmt_dup(s->op2.stval));
			/* also optimize the pivots */
			s->op3.stval = optimize(c, pivots);
			stmt_destroy(pivots);
		}
		return stmt_dup(s->op3.stval);
	case st_set: case st_sets: 
		assert(0); 	/* these should have been rewriten by now */
	}
	return res;
}
