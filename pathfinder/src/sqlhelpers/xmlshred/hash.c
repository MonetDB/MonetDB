/**
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pf_config.h"
#include "hash.h"
#include "shred_helper.h"
#include <string.h>
#include <unistd.h>
#include <assert.h>

#define MIN(a,b) ((a) < (b) ? (a) : (b))

#define NAME_ID 0

/**
 * Create a new Hashtable.
 */
hashtable_t
new_hashtable (void)
{
    return (hashtable_t)
	    malloc (HASHTABLE_SIZE * sizeof (bucket_t));
}

/**
 * Hashfunction
 * You should use the macro #HASHFUNCTON to apply the the function only to a
 * fragment of the string.
 */
int
hashfunc(char *str)
{
    assert (str);
    /* applying horners rule */
    int x;
    int k = strlen(str);
    k--; 
    x = (int)str[k]-'a';
	str[k] = '\0';
	assert (k >= 0);
    if(k <= 0) {
        return x % PRIME;
    }
    return (x + POLY_A * hashfunc(str)) % PRIME; 
}

/* find element in bucket */
static int
find_bucket(bucket_t *bucket, char *key)
{
    assert (key);

    bucket_t *actbucket = bucket;
    while (actbucket)
    {
        if (strcmp(actbucket->key, key)==0)
	       return actbucket->id;
	    else
	        actbucket = actbucket->next;
    }
    return NO_KEY;
}

/* add id and key to the bucket list */
static bucket_t *
bucket_insert(bucket_t *bucket, char *key, int id)
{
    int ident = find_bucket(bucket, key);
    bucket_t *actbucket = NULL;
    /* no key found */
    if( ident == -1) {
    	actbucket = (bucket_t*) malloc(sizeof(bucket_t));

	    actbucket->id = id;
	    actbucket->key = strndup(key,strlen(key));

	    /* add actbucket to the front of list */
	    actbucket->next = bucket;
	    return actbucket;
    }
    else {
    	return bucket;
    }
    /* satisfy picky compilers */
    return NULL;
}

/**
 * Insert key and id into hashtable.
 */
void
hashtable_insert(hashtable_t hash_table, char *key, int id)
{
     assert (hash_table != NULL);
     int hashkey = HASHFUNCTION(key);
     hash_table[hashkey] = 
	         bucket_insert(hash_table[hashkey], key, id); 
     return;
}

/**
 * Find element in hashtable. 
 */
int
find_element(hashtable_t hash_table, char *key)
{
    assert (key);
    return find_bucket(hash_table[HASHFUNCTION(key)],key);
}

/**
 * Free memory assigned to hash_table.
 */
void
free_hash(hashtable_t hash_table)
{
   assert (hash_table != NULL);
   int i = 0;
   if(!hash_table) return;

   for(i = 0; i < HASHTABLE_SIZE; i++)
        free(hash_table[i]);
}
