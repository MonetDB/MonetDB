
#include <gdk.h>
#include "catalog.h"
#include "context.h"
#include "mem.h"
#include <sqlexecute.h>
#include <mvc.h>

static int key_cmp( key *k, long *id )
{
	if (k && id && k->id == *id)
		return 0;
	return 1;
}

static void getschemas( mvc *myc, context *lc, catalog *c, char *schema, char *user ){
	node *n,*l;
	list *keys = list_create(NULL);

	if (c->schemas) list_destroy( c->schemas );
	c->schemas = list_create((fdestroy)&cat_drop_schema);
	
	for(l = myc->trans->schemas->h; l; l = l->next){
	    sql_schema *ns = l->data;
	    c->cur_schema = cat_create_schema(c, 0, ns->name, user );
	    list_append( c->schemas, c->cur_schema );
	    for(n = ns->tables->h; n; n = n->next){
		sql_table *nt = n->data;

		if (nt->type != tt_view ){
			node *m;
	      		table *t = cat_create_table( c, nt->id, c->cur_schema, nt->name, nt->type, NULL );
			for (m = nt->columns->h; m; m = m->next ){
				sql_column *col = m->data;
		  		column *cx = cat_create_column( c, col->id, t, col->name, 
					sql_dup_subtype(col->type), col->def, col->null );
			}	
			if (nt->keys) for (m = nt->keys->h; m; m = m->next ){
				node *p, *o = NULL;
				key *nk = NULL;
				sql_key *k = m->data;

				if (k->type == fkey){
					sql_fkey *fk = (sql_fkey*)k;
					o = list_find(keys, &fk->rkey->k.id, 
							(fcmp)&key_cmp);
				}

				if (o){
	    				nk = cat_table_add_key( t, k->type, 
							k->name, o->data);
	  				list_remove_node( keys, o); 
				} else {
	    				nk = cat_table_add_key( t, k->type, 
							k->name, NULL);
					list_append( keys, nk);
				}
				nk->id = k->id;
				for (o = k->columns->h; o; o = o->next){
					sql_kc *kc = o->data;
					column *nc = NULL;
					nc = cat_bind_column(c, t, kc->c->name);
					assert(nc);
					cat_key_add_column( nk, nc, kc->trunc);
				}
			}
		} else {
	      		stmt *s = sqlexecute( lc, nt->query );
			stmt_destroy(s);
		}
	    }
	}
	c->cur_schema = cat_bind_schema( c, schema );
}

catalog *mvc_catalog_create( mvc *myc, context *lc )
{
	catalog *c = lc->cat;
	getschemas( myc, lc, c, myc->trans->schema->name, myc->user );
	return c;
}
