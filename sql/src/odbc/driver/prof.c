/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
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
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

#include "prof.h"
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <link.h>
#include <NDK.h>
#include <sys/gmon.h>

#define M_MAPFILE "gmon.map"

typedef struct _map_entry {
  u_long nameLen;
  u_long address;		/* base address */
} map_entry;

static int gprof = 0;
static void startGprof()
{
    u_long s = ~0;
    u_long e = 0;
    struct link_map* map = _r_debug.r_map;

    if (gprof) return;
    gprof++;

    while (NULL != map) {
      if (0 != map->l_addr) {
	    if (s > (u_long)map->l_addr) s = (u_long)map->l_addr;
	    if (e < (u_long)map->l_addr) e = (u_long)map->l_addr;
      }
      map = map->l_next;
    }

    printf("low %lx high %lx\n", s, e );

    monstartup(s,e);
}

static void endGprof()
{
  int mfd;

  if (!gprof) return;
  mfd = open(M_MAPFILE, O_CREAT|O_WRONLY|O_TRUNC, 0666);
  if (mfd >= 0) {
    FILE *fd = fdopen(mfd, "w");
    map_entry mme;
    struct link_map* map = _r_debug.r_map;
    while (NULL != map) {
      if (0 != map->l_addr) {
	mme.nameLen = strlen(map->l_name);
	mme.address = map->l_addr;
	/*
	write(mfd, &mme, sizeof(mme));
	write(mfd, map->l_name, mme.nameLen);
	*/
	fprintf(fd, "name %s addr %lx\n", map->l_name, (u_long)map->l_addr );
      }
      map = map->l_next;
    }
    fclose(fd);
    close(mfd);
  }

  _mcleanup();
}

void start_profiling()
{
    startGprof();
    atexit(endGprof);
}

void end_profiling()
{
    endGprof();
}
