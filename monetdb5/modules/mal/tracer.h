/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#ifndef _TRACER_H
#define _TRACER_H

#include "mal.h"
#include "mal_interpreter.h"

/*
 * All the functions correspond one by one to the API calls in gdk_tracer.h
 */
mal_export str TRACERflush_buffer(void);
mal_export str TRACERset_component_level(void *ret, int *comp, int *lvl);
mal_export str TRACERreset_component_level(int *comp);
mal_export str TRACERset_layer_level(void *ret, int *layer, int *lvl);
mal_export str TRACERreset_layer_level(int *layer);
mal_export str TRACERset_flush_level(void *ret, int *lvl);
mal_export str TRACERreset_flush_level(void);
mal_export str TRACERset_adapter(void *ret, int *adapter);
mal_export str TRACERreset_adapter(void);

#endif /* _TRACER_H */
