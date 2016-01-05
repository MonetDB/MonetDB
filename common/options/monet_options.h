/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MO_H_
#define _MO_H_

typedef enum opt_kind {
	opt_builtin = 0,
	opt_config = 1,
	opt_cmdline = 2
} opt_kind;

typedef struct opt {
	opt_kind kind;
	char *name;
	char *value;
} opt;

#ifdef __cplusplus
extern "C" {
#endif

#ifndef moptions_export
/* avoid using "#ifdef WIN32" so that this file does not need our config.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#if !defined(LIBMOPTIONS) && !defined(LIBGDK) && !defined(LIBMAPI)
#define moptions_export extern __declspec(dllimport)
#else
#define moptions_export extern __declspec(dllexport)
#endif
#else
#define moptions_export extern
#endif
#endif

/* mo_print_options will print the option set on stderr */
moptions_export void mo_print_options(opt *set, int setlen);

/* mo_find_option, finds the option with the given name in the option set
   (set,setlen). */
moptions_export char *mo_find_option(opt *set, int setlen, const char *name);

/* mo_system_config will add the options from the system config file
   (returns the new setlen) */
moptions_export int mo_system_config(opt **Set, int setlen);

/* mo_builtin_settings, will place the builtin settings into a new
   option set (returns the length of this set). */
moptions_export int mo_builtin_settings(opt **Set);

/* mo_add_option will add a single option to the option set
   (returns new length) */
moptions_export int mo_add_option(opt **Set, int setlen, opt_kind kind, const char *name, const char *value);

/* mo_free_options will free the resouces take by the options set */
moptions_export void mo_free_options(opt *set, int setlen);

#ifdef __cplusplus
}
#endif
#endif
