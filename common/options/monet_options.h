/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
