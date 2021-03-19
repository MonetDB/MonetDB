/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef COPYBINARY_H
#define COPYBINARY_H

#include "monetdb_config.h"

typedef struct {
	uint8_t day;
	uint8_t month;
	int16_t year;
} copy_binary_date; // natural size: 32, natural alignment: 16

typedef struct {
	uint32_t ms;
	uint8_t seconds;
	uint8_t minutes;
	uint8_t hours;
	uint8_t padding; // implied in C, explicit elsewhere
} copy_binary_time;		 // natural size: 64, natural alignment: 32

typedef struct {
	copy_binary_time time;
	copy_binary_date date;
} copy_binary_timestamp; // natural size: 96, natural alignment: 32

#endif
