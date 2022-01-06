/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"		/* for GDKmalloc() & GDKfree() */
#include "sql_list.h"

static node *
node_create(sql_allocator *sa, void *data)
{
	node *n = (sa)?SA_NEW(sa, node):MNEW(node);

	if (n == NULL)
		return NULL;
	*n = (node) {
		.data = data,
	};
	return n;
}

static list *
list_init(list *l, sql_allocator *sa, fdestroy destroy)
{
	if (l) {
		*l = (list) {
			.sa = sa,
			.destroy = destroy,
		};
		MT_lock_init(&l->ht_lock, "sa_ht_lock");
	}
	return l;
}

list *
list_create(fdestroy destroy)
{
	return list_init(MNEW(list), NULL, destroy);
}

list *
sa_list(sql_allocator *sa)
{
	list *l = (sa)?SA_NEW(sa, list):MNEW(list);
	return list_init(l, sa, NULL);
}

/*
static void
_free(void *dummy, void *data)
{
	(void)dummy;
	GDKfree(data);
}
*/

list *
sa_list_append(sql_allocator *sa, list *l, void *data)
{
	if (!l)
		l = SA_LIST(sa, NULL);
	return list_append(l, data);
}

list *
list_new(sql_allocator *sa, fdestroy destroy)
{
	list *l = (sa)?SA_NEW(sa, list):MNEW(list);
	return list_init(l, sa, destroy);
}

static list *
list_new_(list *l)
{
	list *res = NULL;
	if (l->sa)
		res = list_new(l->sa, l->destroy);
	else
		res = list_create(l->destroy);
	return res;
}

int
list_empty(list *l)
{
	if (l)
		return list_length(l) == 0;
	return 1;
}

static void
node_destroy(list *l, void *data, node *n)
{
	if (n->data && l->destroy) {
		l->destroy(data, n->data);
		n->data = NULL;
	}
	if (!l->sa)
		_DELETE(n);
}

void
list_destroy2(list *l, void *data)
{
	if (l) {
		node *n = l->h;

		MT_lock_destroy(&l->ht_lock);
		l->h = NULL;
		if (l->destroy || l->sa == NULL) {
			while (n) {
				node *t = n;

				n = t->next;
				node_destroy(l, data, t);
			}
		}

		if (l->ht && !l->ht->sa)
			hash_destroy(l->ht);

		if (!l->sa)
			_DELETE(l);
	}
}

void
list_destroy(list *l)
{
	list_destroy2(l, NULL);
}

int
list_length(list *l)
{
	if (l)
		return l->cnt;
	return 0;
}

static list *
list_append_node(list *l, node *n)
{
	if (l->cnt) {
		l->t->next = n;
	} else {
		l->h = n;
	}
	l->t = n;
	l->cnt++;
	if (n->data) {
		MT_lock_set(&l->ht_lock);
		if (l->ht) {
			int key = l->ht->key(n->data);

			if (hash_add(l->ht, key, n->data) == NULL) {
				MT_lock_unset(&l->ht_lock);
				return NULL;
			}
		}
		MT_lock_unset(&l->ht_lock);
	}
	return l;
}

list *
list_append(list *l, void *data)
{
	node *n = node_create(l->sa, data);

	if (n == NULL)
		return NULL;
	return list_append_node(l, n);
}

void *
list_append_with_validate(list *l, void *data, void *extra, fvalidate cmp)
{
	node *n = node_create(l->sa, data), *m;
	void* err = NULL;

	if (n == NULL)
		return NULL;
	if (l->cnt) {
		for (m = l->h; m; m = m->next) {
			err = cmp(m->data, data, extra);
			if(err) {
				n->data = NULL;
				node_destroy(l, NULL, n);
				return err;
			}
		}
		l->t->next = n;
	} else {
		l->h = n;
	}
	l->t = n;
	l->cnt++;
	MT_lock_set(&l->ht_lock);
	if (l->ht) {
		int key = l->ht->key(data);

		if (hash_add(l->ht, key, data) == NULL) {
			MT_lock_unset(&l->ht_lock);
			return NULL;
		}
	}
	MT_lock_unset(&l->ht_lock);
	return NULL;
}

void *
list_append_sorted(list *l, void *data, void *extra, fcmpvalidate cmp)
{
	node *n = node_create(l->sa, data), *m, *prev = NULL;
	int first = 1, comp = 0;
	void* err = NULL;

	if (n == NULL)
		return NULL;
	if (l->cnt == 0) {
		l->h = n;
		l->t = n;
	} else {
		for (m = l->h; m; m = m->next) {
			err = cmp(m->data, data, extra, &comp);
			if(err) {
				n->data = NULL;
				node_destroy(l, NULL, n);
				return err;
			}
			if(comp < 0)
				break;
			first = 0;
			prev = m;
		}
		if(first) {
			n->next = l->h;
			l->h = n;
		} else if(!m) {
			l->t->next = n;
			l->t = n;
		} else {
			assert(prev);
			n->next = m;
			prev->next = n;
		}
	}
	l->cnt++;
	MT_lock_set(&l->ht_lock);
	if (l->ht) {
		int key = l->ht->key(data);

		if (hash_add(l->ht, key, data) == NULL) {
			MT_lock_unset(&l->ht_lock);
			return NULL;
		}
	}
	MT_lock_unset(&l->ht_lock);
	return NULL;
}

list *
list_append_before(list *l, node *m, void *data)
{
	node *p = l->h;
	node *n = node_create(l->sa, data);

	if (n == NULL)
		return NULL;
	n->next = m;
	if (p == m){
		l->h = n;
	} else {
		while (p->next && p->next != m)
			p = p->next;
		p->next = n;
	}
	l->cnt++;
	MT_lock_set(&l->ht_lock);
	if (l->ht) {
		int key = l->ht->key(data);

		if (hash_add(l->ht, key, data) == NULL) {
			MT_lock_unset(&l->ht_lock);
			return NULL;
		}
	}
	MT_lock_unset(&l->ht_lock);
	return l;
}

list *
list_prepend(list *l, void *data)
{
	node *n = node_create(l->sa, data);

	if (n == NULL)
		return NULL;
	if (!l->cnt) {
		l->t = n;
	}
	n->next = l->h;
	l->h = n;
	l->cnt++;
	MT_lock_set(&l->ht_lock);
	if (l->ht) {
		int key = l->ht->key(data);

		if (hash_add(l->ht, key, data) == NULL) {
			MT_lock_unset(&l->ht_lock);
			return NULL;
		}
	}
	MT_lock_unset(&l->ht_lock);
	return l;
}

static void
hash_delete(sql_hash *h, void *data)
{
	int key = h->key(data);
	sql_hash_e *e, *p = h->buckets[key&(h->size-1)];

	e = p;
	for (;  p && p->value != data ; p = p->chain)
		e = p;
	if (p && p->value == data) {
		if (p == e)
			h->buckets[key&(h->size-1)] = p->chain;
		else
			e->chain = p->chain;
	}
	h->entries--;
}

static node *
list_remove_node_(list *l, node *n)
{
	void *data = n->data;
	node *p = l->h;

	if (p != n)
		while (p && p->next != n)
			p = p->next;
	assert(p==n||(p && p->next == n));
	if (p == n) {
		l->h = n->next;
		p = NULL;
	} else if ( p != NULL)  {
		p->next = n->next;
	}
	if (n == l->t)
		l->t = p;
	if (data) {
		MT_lock_set(&l->ht_lock);
		if (l->ht && data)
			hash_delete(l->ht, data);
		MT_lock_unset(&l->ht_lock);
	}
	l->cnt--;
	assert(l->cnt > 0 || l->h == NULL);
	return p;
}

node *
list_remove_node(list *l, void *gdata, node *n)
{
	node *p = list_remove_node_(l, n);

	node_destroy(l, gdata, n);
	return p;
}

void
list_remove_data(list *s, void *gdata, void *data)
{
	node *n;

	/* maybe use compare func */
	if (s == NULL)
		return;
	for (n = s->h; n; n = n->next) {
		if (n->data == data) {
			MT_lock_set(&s->ht_lock);
			if (s->ht && n->data)
				hash_delete(s->ht, n->data);
			MT_lock_unset(&s->ht_lock);
			n->data = NULL;
			list_remove_node(s, gdata, n);
			break;
		}
	}
}

void
list_remove_list(list *l, void *gdata, list *data)
{
	node *n;

	for (n=data->h; n; n = n->next)
		list_remove_data(l, gdata, n->data);
}

void
list_move_data(list *s, list *d, void *data)
{
	node *n = NULL;

	for (n = s->h; n; n = n->next) {
		if (n->data == data) {
			MT_lock_set(&s->ht_lock);
			if (s->ht && n->data)
				hash_delete(s->ht, n->data);
			MT_lock_unset(&s->ht_lock);
			n->data = NULL;	/* make sure data isn't destroyed */
			(void)list_remove_node_(s, n);
			n->data = data;
			break;
		}
	}
	if (!n) {
		list_append(d, data);
	} else {
		n->next = NULL;
		list_append_node(d, n);
	}
}

int
list_check_prop_all(list *l, prop_check_func f)
{
	int res = 1;
	if (l)
		for (node *n = l->h; n && res; n = n->next)
			res &= f(n->data);
	return res;
}

int
list_traverse(list *l, traverse_func f, void *clientdata)
{
	int res = 0, seqnr = 0;
	node *n = l->h;

	while (n && !res) {
		res = f(clientdata, seqnr++, n->data);
		n = n->next;
	}
	return res;
}

void *
list_transverse_with_validate(list *l, void *data, void *extra, fvalidate cmp)
{
	void* err = NULL;

	for (node *n = l->h; n; n = n->next) {
		err = cmp(n->data, data, extra);
		if(err)
			break;
	}
	return err;
}

node *
list_find(list *l, void *key, fcmp cmp)
{
	node *n = NULL;

	if (key) {
		if (cmp) {
			for (n = l->h; n; n = n->next) {
				if (cmp(n->data, key) == 0) {
					return n;
				}
			}
		} else {
			for (n = l->h; n; n = n->next) {
				if (n->data == key)
					return n;
			}
		}
	}
	return NULL;
}

int
list_cmp(list *l1, list *l2, fcmp cmp)
{
	node *n, *m;
	int res = 0;

	if (l1 == l2)
		return 0;
	if (!l1 && l2 && list_empty(l2))
		return 0;
	if (!l2 && l1 && list_empty(l1))
		return 0;
	if (!l1 || !l2 || (list_length(l1) != list_length(l2)))
		return -1;

	for (n = l1->h, m = l2->h; res == 0 && n; n = n->next, m = m->next) {
		res = cmp(n->data, m->data);
	}
	return res;
}

int
list_match(list *l1, list *l2, fcmp cmp)
{
	node *n, *m;
	ulng chk = 0;

	if (l1 == l2)
		return 0;

	if (!l1 || !l2 || (list_length(l1) != list_length(l2)))
		return -1;

	for (n = l1->h; n; n = n->next) {
		int pos = 0, fnd = 0;
		for (m = l2->h; m; m = m->next, pos++) {
			if (!(chk & ((ulng) 1 << pos)) &&
			    cmp(n->data, m->data) == 0) {
				chk |= (ulng) 1 << pos;
				fnd = 1;
			}
		}
		if (!fnd)
			return -1;
	}
	return 0;
}

list *
list_keysort(list *l, int *keys, fdup dup)
{
	list *res;
	node *n = NULL;
	int i, cnt = list_length(l);
	void **data;

	data = GDKmalloc(cnt*sizeof(void *));
	if (data == NULL) {
		return NULL;
	}
	res = list_new_(l);
	if (res == NULL) {
		GDKfree(data);
		return NULL;
	}
	for (n = l->h, i = 0; n; n = n->next, i++) {
		data[i] = n->data;
	}
	/* sort descending */
	GDKqsort(keys, data, NULL, cnt, sizeof(int), sizeof(void *), TYPE_int, true, true);
	for(i=0; i<cnt; i++) {
		list_append(res, dup?dup(data[i]):data[i]);
	}
	GDKfree(data);
	return res;
}

list *
list_sort(list *l, fkeyvalue key, fdup dup)
{
	list *res;
	node *n = NULL;
	int i, *keys, cnt = list_length(l);
	void **data;

	keys = GDKmalloc(cnt*sizeof(int));
	data = GDKmalloc(cnt*sizeof(void *));
	if (keys == NULL || data == NULL) {
		GDKfree(keys);
		GDKfree(data);
		return NULL;
	}
	res = list_new_(l);
	if (res == NULL) {
		GDKfree(keys);
		GDKfree(data);
		return NULL;
	}
	for (n = l->h, i = 0; n; n = n->next, i++) {
		keys[i] = key(n->data);
		data[i] = n->data;
	}
	/* sort descending */
	GDKqsort(keys, data, NULL, cnt, sizeof(int), sizeof(void *), TYPE_int, true, true);
	for(i=0; i<cnt; i++) {
		list_append(res, dup?dup(data[i]):data[i]);
	}
	GDKfree(keys);
	GDKfree(data);
	return res;
}

list *
list_select(list *l, void *key, fcmp cmp, fdup dup)
{
	list *res = NULL;
	node *n = NULL;

	if (key && l) {
		res = list_new_(l);
		if(res) {
			for (n = l->h; n; n = n->next)
				if (cmp(n->data, key) == 0)
					list_append(res, dup?dup(n->data):n->data);
		}
	}
	return res;
}

/* order the list based on the compare function cmp */
list *
list_order(list *l, fcmp cmp, fdup dup)
{
	list *res = list_new_(l);
	node *m, *n = NULL;

	/* use simple insert sort */
	if(res) {
		for (n = l->h; n; n = n->next) {
			int append = 1;
			for (m = res->h; m && append; m = m->next) {
				if (cmp(n->data, m->data) > 0) {
					list_append_before(res, m, dup ? dup(n->data) : n->data);
					append = 0;
				}
			}
			if (append)
				list_append(res, dup ? dup(n->data) : n->data);
		}
	}
	return res;
}

list *
list_distinct(list *l, fcmp cmp, fdup dup)
{
	list *res = list_new_(l);
	node *n = NULL;

	if(res) {
		for (n = l->h; n; n = n->next) {
			if (!list_find(res, n->data, cmp)) {
				list_append(res, dup ? dup(n->data) : n->data);
			}
		}
	}
	return res;
}

int
list_position(list *l, void *val)
{
	node *n = NULL;
	int i;

	for (n = l->h, i=0; n && val != n->data; n = n->next, i++)
		;
	if (n && n->data == val)
		return i;
	return -1;
}

void *
list_fetch(list *l, int pos)
{
	node *n = NULL;
	int i;

	for (n = l->h, i=0; n && i<pos; n = n->next, i++)
		;
	if (n)
		return n->data;
	return NULL;
}

void *
list_reduce(list *l, freduce red, fdup dup)
{
	void *res = NULL;
	node *n = l->h;

	if (n) {
		res = dup?dup(n->data):n->data;
		for (n = n->next; n; n = n->next) {
			res = red(res, dup?dup(n->data):n->data);
		}
	}
	return res;
}

void *
list_reduce2(list *l, freduce2 red, sql_allocator *sa)
{
	void *res = NULL;
	node *n = l->h;

	if (n) {
		res = n->data;
		for (n = n->next; n; n = n->next)
			res = red(sa, res, n->data);
	}
	return res;
}


list *
list_map(list *l, void *data, fmap map)
{
	list *res = list_new_(l);

	node *n = l->h;

	if(res) {
		while (n) {
			void *v = map(n->data, data);

			if (v)
				list_append(res, v);
			n = n->next;
		}
	}
	return res;
}

list *
list_merge(list *l, list *data, fdup dup)
{
	if (data) {
		node *n = data->h;

		while (n) {
			if (dup && n->data)
				list_append(l, dup(n->data));
			else
				list_append(l, n->data);
			n = n->next;
		}
	}
	return l;
}

list *
list_merge_destroy(list *l, list *data, fdup dup)
{
	if (data) {
		node *n = data->h;

		while (n) {
			if (dup)
				list_append(l, dup(n->data));
			else
				list_append(l, n->data);
			n = n->next;
		}
	}

	list_destroy(data);

	return l;
}

list *
list_dup(list *l, fdup dup)
{
	list *res = list_new_(l);
	return res ? list_merge(res, l, dup) : NULL;
}

list *
list_flaten(list *l)
{
	list *res = list_new_(l);
	for (node *n = l->h ; n ; n = n->next) {
		list *ll = (list*) n->data;
		for (node *m = ll->h ; m ; m = m->next)
			list_append(res, m->data);
	}
	return res;
}

void
list_hash_delete(list *l, void *data, fcmp cmp)
{
	if (l && data) {
		node *n = list_find(l, data, cmp);
		if(n) {
			MT_lock_set(&l->ht_lock);
			if (l->ht && n->data)
				hash_delete(l->ht, data);
			MT_lock_unset(&l->ht_lock);
		}
	}
}

void*
list_hash_add(list *l, void *data, fcmp cmp)
{
	if (l && data) {
		node *n = list_find(l, data, cmp);
		if(n) {
			MT_lock_set(&l->ht_lock);
			if (l->ht && n->data) {
				int nkey = l->ht->key(data);
				if (hash_add(l->ht, nkey, data) == NULL) {
					MT_lock_unset(&l->ht_lock);
					return NULL;
				}
			}
			MT_lock_unset(&l->ht_lock);
		}
	}
	return data;
}

void
list_hash_clear(list *l)
{
	if (l->ht) {
		MT_lock_set(&l->ht_lock);
		l->ht = NULL;
		MT_lock_unset(&l->ht_lock);
	}
}
