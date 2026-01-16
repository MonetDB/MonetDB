/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include <stddef.h>
#include <stdint.h>

typedef int64_t item_index;
typedef struct backref_memory backref_memory;


backref_memory *backref_create(void);
void backref_destroy(backref_memory *mem);
const char *backref_encode(backref_memory *mem, const char *input, item_index cur_index, size_t *output_len);


