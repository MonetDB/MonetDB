/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef SPHINX_H
#define SPHINX_H

#include <gdk.h>
#include <ctype.h>
#include <sphinxclient.h>
#include "mal_client.h"
#include "mal_interpreter.h"

#ifdef WIN32
#ifndef LIBSPHINX
#define sphinx_export extern __declspec(dllimport)
#else
#define sphinx_export extern __declspec(dllexport)
#endif
#else
#define sphinx_export extern
#endif

sphinx_export str SPHINXsearchIndexLimit(bat *ret, const str *query, const str *index, const int *limit);

#endif /* SPHINX_H */

