/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "bincopybackref.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


#define HASH_SLOTS (65536)
#define HASH_BUFFER_SIZE (1<<20)
#define MAX_ITEM_LEN (HASH_BUFFER_SIZE / 2)
#define HASHES_PER_MEM (2)

#define INVALID_OFFSET ((uint32_t)-1)


// A string, its hash and its length including the NUL.
typedef struct input_string {
	const char *s;
	size_t size;
	uint32_t hash;
	item_index idx;
} input_string;

typedef struct my_hash_table {
	uint32_t offsets[HASH_SLOTS];   // maps hash to to potential matching string
	item_index indices[HASH_SLOTS]; // last time that string was seen
	size_t len;                     // portion of .buffer used so far
	char buffer[HASH_BUFFER_SIZE];
} my_hash_table;

struct backref_memory {
	my_hash_table hashtabs[HASHES_PER_MEM];
	int active;
	item_index last_nil;
	uint8_t encoded[20];
};

static void
reset_hash(my_hash_table *hash)
{
	hash->len = 0;
	for (int i = 0; i < HASH_SLOTS; i++)
		hash->offsets[i] = INVALID_OFFSET;
}

static void
prepare_string(input_string *hs, const char *str, item_index cur_index)
{
	hs->s = str;
	hs->idx = cur_index;

	uint32_t h = 0;
	const char *s = str;
	while (*s) {
		h += *s++;
		h += (h << 10);
		h ^= (h >> 6);
	}
	h += (h << 3);
	h ^= (h >> 11);
	h += (h << 15);

	hs->size = s - str + 1;
	hs->hash = h;
}

backref_memory*
backref_create(void)
{
	assert((uint64_t)HASH_BUFFER_SIZE < (uint64_t)UINT32_MAX);
	backref_memory *mem = malloc(sizeof(*mem));
	if (mem == NULL)
		return NULL;
	mem->active = 0;
	mem->last_nil = -1000;
	for (int i = 0; i < HASHES_PER_MEM; i++)
		reset_hash(&mem->hashtabs[i]);
	return mem;
}

void
backref_destroy(backref_memory *mem)
{
	free(mem);
}

static bool
valid_slot(my_hash_table *htab, size_t slot, input_string *hs)
{
	if (htab->offsets[slot] == INVALID_OFFSET)
		return false;
	size_t offset = htab->offsets[slot];
	if (hs->size > HASH_BUFFER_SIZE - htab->len) {
		// can't be a match, it would never fit
		return false;
	}
	return memcmp(htab->buffer + offset, hs->s, hs->size) == 0;
}

static bool
probe_hash(my_hash_table *htab, input_string *hs, size_t *slotp)
{
	// For now we only check the one bucket
	size_t slot = hs->hash % HASH_SLOTS;
	bool valid = valid_slot(htab, slot, hs);
	*slotp = slot;
	return valid;
}

static void
insert_hash(my_hash_table *htab, size_t slot, input_string *istr)
{
	assert(istr->size <= HASH_BUFFER_SIZE - htab->len);
	memcpy(htab->buffer + htab->len, istr->s, istr->size);
	htab->offsets[slot] = (uint32_t)htab->len;
	htab->indices[slot] = istr->idx;
	htab->len += istr->size;
}

static item_index
backref_apply(backref_memory *mem, input_string *istr)
{
	my_hash_table *hashtab;
	if (istr->size > MAX_ITEM_LEN) {
		return -1;
	}

	// First try to find the string in the active hashmap
	hashtab = &mem->hashtabs[mem->active];
	size_t slot;
	bool found = probe_hash(hashtab, istr, &slot);
	if (found) {
		item_index ret = hashtab->indices[slot];
		hashtab->indices[slot] = istr->idx;
		return ret;
	}

	// Ok, insert it.
	if (HASH_BUFFER_SIZE - hashtab->len < istr->size) {
		// rotate
		mem->active = (mem->active + 1) % HASHES_PER_MEM;
		hashtab = &mem->hashtabs[mem->active];
		reset_hash(hashtab);
	}
	insert_hash(hashtab, slot, istr);

	// Before we return the string as-is, check if one of the older
	// hash tables has it.
	item_index best = -1;
	for (int i = 1; i < HASHES_PER_MEM; i++) {
		int ii = (mem->active + i) % HASHES_PER_MEM;
		my_hash_table *htab = &mem->hashtabs[ii];
		if (probe_hash(htab, istr, &slot)) {
			item_index candidate = htab->indices[slot];
			if (candidate > best)
				best = candidate;
		}
	}

	return best; // either a proper index or still -1
}


const char *
backref_encode(backref_memory *mem, const char *input, item_index cur_index, size_t *output_len)
{
	if (input[0] == '\0') {
		*output_len = 1;
		return input;
	} else if (input[0] == '\x80' && input[1] == 0) {
		// nil
		item_index delta = cur_index - mem->last_nil;
		assert(delta > 0);
		mem->last_nil = cur_index;
		if (delta < 64) {
			// we've recently seen a nil, referring to it saves one byte
			mem->encoded[0] = 0x80 + (uint8_t) delta;
			*output_len = 1;
			return (char*) mem->encoded;
		} else {
			// no space advantage to using a backref
			*output_len = 2;
			return input;
		}
	}

	// Look it up
	input_string hs;
	prepare_string(&hs, input, cur_index);
	item_index idx = backref_apply(mem, &hs);
	if (idx == -1) {
		*output_len = hs.size;
		return input;
	}

	// We found it at item_index idx.
	item_index delta = cur_index - idx;
	assert(delta > 0);

	if (delta < 64) {
		// short encoding
		mem->encoded[0] = 0x80 + (uint8_t) delta;
		*output_len = 1;
		return (char*) mem->encoded;
	}

	// long encoding
	unsigned char *p = mem->encoded;
	*p++ = 0x80;
	while (delta > 0) {
		unsigned char chunk = delta % 128;
		if (chunk < delta) {
			// not the last one
			chunk += 128;
		}
		*p++ = chunk;
		delta /= 128;
	}
	*output_len = p - mem->encoded;
	return (char*) mem->encoded;
}
