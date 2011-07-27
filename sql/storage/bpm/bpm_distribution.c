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

#include "monetdb_config.h"
#include "bpm_distribution.h"
#include "bat/bat_logger.h"
#include "bat/bat_utils.h"
#include <sql_string.h>

static MT_Lock host_lock;
static BAT *dist_id = NULL;
static BAT *dist_host = NULL;
static BAT *dist_port = NULL;
static BAT *dist_dbname = NULL;
static BAT *dist_user = NULL;
static BAT *dist_passwd = NULL;

static bpmHost hostAnchor = NULL;
static BAT* bpm_dist_id(void);
bpmHost default_host = NULL;


int host = 1; 

static int
HostID()
{
        int p;

        /* needs locks */
        MT_lock_set(&host_lock, "HostID");
        p = host++;
        MT_lock_unset(&host_lock, "HostID");
        return p;
}

bpmHost
bpm_set_default_host(void)
{
	if (!hostAnchor) {
		default_host = bpm_host_create("localhost", 50000, "demo", "monetdb", "monetdb");
	}
	return default_host;
}

static bpmHost
bpm_host_add(int id, char *host, int port, char *dbname, char *user, char *password)
{
	bpmHost bpmH = NULL;
	bpmH = ZNEW(bpmHostRec);
	bpmH->id = id;
	bpmH->host = _strdup(host);
	bpmH->port = port;
	bpmH->dbname = _strdup(dbname);
	bpmH->user = _strdup(user);
	bpmH->passwd = _strdup(password);
	bpmH->next = hostAnchor;
	hostAnchor = bpmH;

	return bpmH;
}

bpmHost
bpm_host_create(char *host, int port, char *dbname, char *user, char *password)
{
	int id;
	bpmHost bpmH = NULL;
	if (!(bpmH = bpm_host_find(host, port, dbname))) {
 		id = HostID();
		bpmH = bpm_host_add(id, host, port, dbname, user, password);
	}
	return bpmH;
}


void
bpm_host_destroy(bpmHost bpmH)
{
	bpmHost n = NULL, previous = NULL;
	
	for (previous = hostAnchor, n = hostAnchor; n; n = n->next) {
		if (n->id == bpmH->id) {
			previous->next = n->next; 
			break;
		}
		previous = n;
	}

	_DELETE(n->host);
	_DELETE(n->dbname);
	_DELETE(n->user);
	_DELETE(n->passwd);
	_DELETE(n);
}

bpmHost
bpm_host_find(char *host, int port, char *dbname )
{
	bpmHost bpmH = NULL;

	if (hostAnchor) {
		for (bpmH = hostAnchor; bpmH; bpmH = bpmH->next) 
			if ((strcmp(host, bpmH->host) == 0) && (port == bpmH->port) && (strcmp(dbname, bpmH->dbname) == 0))
				return bpmH;
	}

	return bpmH;
}

bpmHost
bpm_host_get(int id)
{
	bpmHost bpmH = NULL;

	if (!hostAnchor) 
		bpm_init_host();
	for (bpmH = hostAnchor; bpmH; bpmH = bpmH->next) 
		if (id == bpmH->id)
			break;

	return bpmH;
}

void
bpm_load_host(void){
        BUN p, q, r;
        BATiter dist_idi = bat_iterator(dist_id);
	BAT *dist_id = bpm_dist_id();
	int host_id = host;
	r = BUNfnd(BATmirror(dist_id), &host_id);
	bpm_set_default_host();

        if (r == BUN_NONE)
                assert(0);

        BATloop(dist_id, p, q) {
                ptr id = BUNhead(dist_idi,p);
                BATiter dist_hosti = bat_iterator(dist_host);
                BATiter dist_porti = bat_iterator(dist_port);
                BATiter dist_dbnamei = bat_iterator(dist_dbname);
                BATiter dist_useri = bat_iterator(dist_user);
                BATiter dist_passwdi = bat_iterator(dist_passwd);

		int hid = *(int*) BUNtail(dist_idi, BUNfnd(dist_id, id));
		char *host = (char*) BUNtail(dist_hosti, BUNfnd(dist_host, id));
		int port = *(int*) BUNtail(dist_porti, BUNfnd(dist_port, id));
		char *dbname = (char*) BUNtail(dist_dbnamei, BUNfnd(dist_dbname, id));
		char *user = (char*) BUNtail(dist_useri, BUNfnd(dist_user, id));
		char *passwd = (char*) BUNtail(dist_passwdi, BUNfnd(dist_passwd, id));
		
		bpm_host_add(hid, host, port, dbname, user, passwd);
        }
}


void
bpm_init_host(void)
{
	int p;

        if (!dist_id && (p = logger_find_bat(bat_logger, "dist_id"))) {
                #define get_bat(nme) \
                        temp_descriptor(logger_find_bat(bat_logger, nme))
                dist_id = temp_descriptor(p);
                dist_host = get_bat("dist_host");
                dist_port = get_bat("dist_port");
                dist_dbname = get_bat("dist_dbname");
                dist_user = get_bat("dist_user");
                dist_passwd = get_bat("dist_passwd");
        }
	if (!dist_id) {
		/* host bats */
		dist_id = bat_new(TYPE_oid, TYPE_int, 0);
		dist_host = bat_new(TYPE_oid, TYPE_str, 0);
		dist_port = bat_new(TYPE_oid, TYPE_int, 0);
		dist_dbname = bat_new(TYPE_oid, TYPE_str, 0);
		dist_user = bat_new(TYPE_oid, TYPE_str, 0);
		dist_passwd = bat_new(TYPE_oid, TYPE_str, 0);

#define log_P(l, b, n) logger_add_bat(l, b, n); log_bat_persists(l, b, n)
                log_P(bat_logger, dist_id, "dist_id");
                log_P(bat_logger, dist_host, "dist_host");
                log_P(bat_logger, dist_port, "dist_port");
                log_P(bat_logger, dist_dbname, "dist_dbname");
                log_P(bat_logger, dist_user, "dist_user");
                log_P(bat_logger, dist_passwd, "dist_passwd");
        }
}

BAT*
bpm_dist_id(void)
{
	if (!dist_id)
		bpm_init_host();
	return dist_id;
}

void
bpm_insert_host(bpmHost bpmH )
{
	if (!dist_host)
		bpm_init_host();
	
	BUNappend(dist_id, &bpmH->id, TRUE);
	BUNappend(dist_host, bpmH->host, TRUE);
	BUNappend(dist_port, &bpmH->port, TRUE);
	BUNappend(dist_dbname, bpmH->dbname, TRUE);
	BUNappend(dist_user, bpmH->user, TRUE);
	BUNappend(dist_passwd, bpmH->passwd, TRUE);
}

void
bpm_delete_host(bpmHost bpmH )
{
	BAT *r = NULL;
	oid rid = oid_nil;
	BUN p;

	if (!dist_id)
		return ;

	r = BATmirror(dist_id);
	p = BUNfnd(r, &bpmH->id);
	if (p != BUN_NONE) {
                BATiter ri = bat_iterator(r);
                rid = *(oid *) BUNtail(ri, p);
        }

	BUNins(dist_id, &rid, NULL, TRUE);
	BUNins(dist_host, &rid, NULL, TRUE);
	BUNins(dist_port, &rid, NULL, TRUE);
	BUNins(dist_dbname, &rid, NULL, TRUE);
	BUNins(dist_user, &rid, NULL, TRUE);
	BUNins(dist_port, &rid, NULL, TRUE);
}







