/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

#include "sysdefs.h"
@TOP@

/* Define this if the compiler has bool type defined */
#undef HAVE_BOOL

/* Define this if you have the pthread library */
#undef HAVE_LIBPTHREAD

/* Define this if you have the pthread_sigmask function */
#undef HAVE_PTHREAD_SIGMASK

/* Define this if you have the readline library */
#undef HAVE_LIBREADLINE
#undef HAVE_LIBZ
#undef HAVE_LIBBZ2

/* Define these if you have the respective hardware-counters library */
#undef HAVE_LIBPPERF	/* Linux <= 2.2 */
#undef HAVE_LIBPERFCTR	/* Linux >= 2.4 */
#undef HAVE_LIBPERFMON	/* Solaris <= 7 */
#undef HAVE_LIBCPC	/* Solaris >= 8 */

/* Define this if you have ctime_r(time_t*,char *buf,size_t s) */
#undef HAVE_CTIME_R3

/* Defines HOST */
#undef HOST

/* LONGLONG */
#undef HAVE_LONGLONG
#undef HAVE__INT64
