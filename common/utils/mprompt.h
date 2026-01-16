/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef mutils_export
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#ifndef LIBMUTILS
#define mutils_export extern __declspec(dllimport)
#else
#define mutils_export extern __declspec(dllexport)
#endif
#else
#define mutils_export extern
#endif
#endif

mutils_export char *prompt_getlogin(void);
mutils_export char *simple_prompt(const char *prompt, int maxlen, int echo, const char *def);
