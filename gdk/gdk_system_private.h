/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBGDK
#error this file should not be included outside its source directory
#endif

void dump_threads(void)
	__attribute__((__visibility__("hidden")));
void join_detached_threads(void)
	__attribute__((__visibility__("hidden")));
int MT_kill_thread(MT_Id t)
	__attribute__((__visibility__("hidden")));
bool MT_thread_override_limits(void)
	__attribute__((__visibility__("hidden")));
#ifdef NATIVE_WIN32
#define GDKwinerror(format, ...)					\
	do {								\
		char _osmsgbuf[128];					\
		FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL,		\
			      GetLastError(),				\
			      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), \
			      (LPTSTR) _osmsgbuf, sizeof(_osmsgbuf),	\
			      NULL);					\
		GDKtracer_log(__FILE__, __func__, __LINE__, M_CRITICAL,	\
			      GDK, _osmsgbuf, format, ##__VA_ARGS__);	\
		SetLastError(0);					\
	} while (0)
#endif
