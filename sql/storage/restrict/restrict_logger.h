/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef RESTRICT_LOGGER_H
#define RESTRICT_LOGGER_H

#include "sql_storage.h"
#include <gdk_logger.h>

extern logger *restrict_logger;

extern int su_logger_init( logger_functions *lf );
extern int ro_logger_init( logger_functions *lf );
extern int suro_logger_init( logger_functions *lf );

#endif /*RESTRICT_LOGGER_H */
