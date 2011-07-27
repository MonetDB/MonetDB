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

#ifndef BPMDISTRIBUTION_H
#define BPMDISTRIBUTION_H

typedef struct BPMHOST {
	int id;
	char *host;
	int port;
	char *dbname;
	char *user;
	char *passwd;
	struct BPMHOST *next;
} bpmHostRec, *bpmHost;

extern bpmHost bpm_host_create(char *host, int port, char *dbname, char *user, char *password);
extern void bpm_host_destroy(bpmHost bpmH);
extern bpmHost bpm_host_find(char *host, int port, char *dbname );
extern void bpm_init_host(void);
extern void bpm_insert_host(bpmHost bpmH);
extern void bpm_delete_host(bpmHost bpmH);
extern bpmHost bpm_host_get(int id);
extern bpmHost bpm_set_default_host(void);

#endif /*BPMDISTRIBUTION_H */

