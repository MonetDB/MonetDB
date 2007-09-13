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

/* We use a seperate chaining strategy to
 * mantain our hash_table,
 * So each bucket is a chained list itself,
 * to handle possible collisions.
 */
struct bucket_t {
    char *key;      /**< key as string */
    int id;         /**< name_id */
    bucket_t* next; /**< next bucket in our list */
};

/* size of the hashtable */
#define PRIME 113

/**
 * Lookup an id in a given bucket using it associated key.
 */
static int
find_id (bucket_t *bucket, char *key)
{
    bucket_t *cur_bucket = bucket;
    
    assert (key);

    while (cur_bucket)
        if (strcmp (cur_bucket->key, key) == 0)
            return cur_bucket->id;
        else
            cur_bucket = cur_bucket->next;

    return NO_KEY;
}

/**
 * Attach an (id, key) pair to a given bucket list.
 */
static bucket_t *
bucket_insert(bucket_t *bucket, char *key, int id)
{
    int ident = find_id (bucket, key);
    
    /* no key found */
    if (NOKEY (ident)) {
        bucket_t *newbucket = (bucket_t*) malloc (sizeof (bucket_t));

        newbucket->id = id;
        newbucket->key = strndup (key, strlen(key));

        /* add new bucket to the front of list */
        newbucket->next = bucket;
        return newbucket;
    }
    else
        return bucket;

    /* satisfy picky compilers */
    return NULL;
}

/**
 * Create the hash value for a given key.
 */
static int
find_hash_bucket (char *key)
{
    size_t len = strlen (key);
    /* keys have at least length 1 */
    assert (len > 0);
    /* build a hash out of the first and the last character
       and the length of the key */
    return (key[0] * key[len-1] * len) % PRIME;
}

/**
 * Create a new Hashtable.
 */
hashtable_t
new_hashtable (void)
{
    return (hashtable_t) malloc (PRIME * sizeof (bucket_t));
}

/**
 * Insert key and id into hashtable.
 */
void
hashtable_insert(hashtable_t hash_table, char *key, int id)
{
    int hashkey;
    
    assert (hash_table && key);
    
    hashkey = find_hash_bucket (key);
    hash_table[hashkey] = bucket_insert (hash_table[hashkey], key, id);
    return;
}

/**
 * Find element in hashtable. 
 */
int
find_element(hashtable_t hash_table, char *key)
{
    assert (key);
    return find_id (hash_table[find_hash_bucket (key)], key);
}

/**
 * Free memory assigned to hash_table.
 */
void
free_hash(hashtable_t hash_table)
{
    bucket_t *bucket, *free_bucket;
    
    assert (hash_table);
    if (!hash_table) return;

    for (int i = 0; i < PRIME; i++) {
        bucket = hash_table[i];
        while (bucket) {
            free_bucket = bucket;
            bucket = bucket->next;
            free (free_bucket);
        }
   }
   free(hash_table);
}
