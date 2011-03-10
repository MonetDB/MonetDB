/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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

#ifndef _SQL_PRIV_H_
#define _SQL_PRIV_H_

/* privileges */
#include <sql_mvc.h>
#include <sql_catalog.h>

extern char * sql_grant_table_privs( mvc *sql, char *grantee, int privs, char *tname, char *cname, int grant, int grantor);
extern char * sql_revoke_table_privs( mvc *sql, char *grantee, int privs, char *tname, char *cname, int grant, int grantor);

extern int mvc_set_role(mvc *m, char *role);
extern int mvc_set_schema(mvc *m, char *schema);

extern int schema_privs(int grantor, sql_schema *t);
extern int table_privs(mvc *m, sql_table *t, int privs);

extern int sql_privilege(mvc *m, int auth_id, int obj_id, int privs, int sub);
extern int sql_grantable(mvc *m, int grantorid, int obj_id, int privs, int sub);
extern int sql_find_auth(mvc *m, str auth);
extern int sql_find_schema(mvc *m, str schema);

extern char *sql_create_role(mvc *m, str auth, int grantor);
extern char *sql_drop_role(mvc *m, str auth);
extern char *sql_grant_role(mvc *m, str grantee, str auth);
extern char *sql_revoke_role(mvc *m, str grantee, str auth);
extern int sql_alter_user(mvc *m, str user, str passwd, char enc, sqlid schema_id, str oldpasswd);
extern int sql_rename_user(mvc *m, str olduser, str newuser);
extern str sql_drop_user(mvc *m, str user);
extern int sql_create_privileges(mvc *m, sql_schema *s);
extern int sql_schema_has_user(mvc *m, sql_schema *s);

#endif /*_SQL_PRIV_H_ */
