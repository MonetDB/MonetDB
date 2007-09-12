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

#ifndef HASH_H__
#define HASH_H__

/* code for no key */
#define NO_KEY -1 

/* returns true if no such key is found */
#define NOKEY(k) (k == NO_KEY)

/* size of the hashtable */
#define HASHTABLE_SIZE 2000 

/* prime number due to bertrands theorem:
 * there exists a prime number p that satisfy,
 * the following condition
 *     HASHTABLE_SIZE < p <= 2 HASHTABLE_SIZE
 */
#define PRIME 2011

/**
 * Compression function
 * We use a universal hash function
 */
#define MAD(key) (((123 * key + 593) % PRIME) % HASHTABLE_SIZE)

/* 33 has proved  to be a good choice
 * for polynomial hash functions
 */
#define POLY_A 33

/* HASHFUNCTION appylied to the first ten characters of a string */
#define HASHFUNCTION(str) MAD(hashfunc(strndup(str, MIN(strlen(str), 10))))

#define MIN(a,b) ((a) < (b) ? (a) : (b))

/* We use a seperate chaining strategy to
 * mantain our hash_table,
 * So each bucket is a chained list itself,
 * to handle possible collisions.
 */
typedef struct bucket_t bucket_t;
struct bucket_t
{
    char *key;      /**< key as string */
    int id;         /**< name_id */
    bucket_t* next; /**< next bucket in our list */
};

/* definition for hashtable_t */
typedef struct bucket_t** hashtable_t;

/**
 * Create a new Hashtable.
 */
hashtable_t new_hashtable (void);

/**
 * Hashfunction
 */
int hashfunc (char *str); 

/**
 * Find element in hashtable.
 */
int find_element (bucket_t **hash_table, char *key);

/**
 * Insert key and id to hashtable.
 */
void hashtable_insert (bucket_t **hash_table, char *key, int id);

/**
 * Free memory assigned to hash_table.
 */
void free_hash(bucket_t **hash_table);

#endif /* HASH_H__ */
