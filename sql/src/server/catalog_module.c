
#include <gdk.h>
#include "catalog.h"
#include "context.h"
#include "mem.h"
#include <sqlexecute.h>
#include <mvc.h>


typedef struct cc {
	context *lc;
	mvc 	*myc;
} cc;

static int key_cmp( key *k, long *id )
{
	if (k && id && k->id == *id)
		return 0;
	return 1;
}

static void getschema( catalog *c, char *schema, char *user ){
	node *n;
	context *lc = ((cc*)c->cc)->lc;
	mvc *myc = ((cc*)c->cc)->myc;
	list *keys = list_create(NULL);

	if (c->schemas) list_destroy( c->schemas );
	c->schemas = list_create((fdestroy)&cat_drop_schema);
	c->cur_schema = cat_create_schema( c, 0, schema, user );
	list_append( c->schemas, c->cur_schema );
	
	for(n = myc->trans->schema->tables->h; n; n = n->next){
		sql_table *nt = n->data;

		if (!nt->view){
			node *m;
	      		table *t = cat_create_table( c, nt->id, c->cur_schema, nt->name, nt->temp, NULL );
			for (m = nt->columns->h; m; m = m->next ){
				sql_column *col = m->data;
				if (col->colnr >= 0)
		  			cat_create_column( c, col->id, t, col->name, 
						col->type, col->def, col->null );
			}	
			if (nt->keys) for (m = nt->keys->h; m; m = m->next ){
				node *p, *o = NULL;
				key *nk = NULL;
				key *k = m->data;

				if (k->rkey)
					o = list_find(keys, &k->rkey->id, 
							(fcmp)&key_cmp);

				if (o){
	    				nk = cat_table_add_key( t, k->type, 
							o->data);
	  				list_remove_node( keys, o); 
				} else {
	    				nk = cat_table_add_key( t, k->type, 
							NULL);
					list_append( keys, nk);
				}
				nk->id = k->id;
				for (o = k->columns->h; o; o = o->next){
					column *col = o->data;
					column *ncol = NULL;
					ncol =cat_bind_column(c, t, col->name);
					assert(ncol);
					cat_key_add_column( nk, ncol);
				}
			}
		} else {
	      		sqlexecute( lc, nt->query );
		}
	}
}

static void cc_destroy( catalog *c ){
	_DELETE(c->cc);
	c->cc = NULL;
}

catalog *catalog_create( context *lc, mvc *myc ){
	cc *CC = NEW(cc);
	catalog *c = lc->cat;
	
	CC->lc = lc;
	CC->myc = myc;
	c->cc = (char*)CC;
	c->cc_getschema = &getschema;
	c->cc_destroy = &cc_destroy;
	return c;
}
