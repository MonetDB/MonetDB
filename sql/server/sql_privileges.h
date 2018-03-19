/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_PRIV_H_
#define _SQL_PRIV_H_

/* privileges */
#include "sql_mvc.h"
#include "sql_catalog.h"

extern char * sql_grant_global_privs( mvc *sql, char *grantee, int privs, int grant, int grantor);
extern char * sql_revoke_global_privs( mvc *sql, char *grantee, int privs, int grant, int grantor);
extern char * sql_grant_table_privs( mvc *sql, char *grantee, int privs, char *sname, char *tname, char *cname, int grant, int grantor);
extern char * sql_revoke_table_privs( mvc *sql, char *grantee, int privs, char *sname, char *tname, char *cname, int grant, int grantor);
extern char * sql_grant_func_privs( mvc *sql, char *grantee, int privs, char *sname, int func_id, int grant, int grantor);
extern char * sql_revoke_func_privs( mvc *sql, char *grantee, int privs, char *sname, int func_id, int grant, int grantor);

extern int mvc_set_role(mvc *m, char *role);
extern int mvc_set_schema(mvc *m, char *schema);

extern int global_privs(mvc *m, int privs);
extern int mvc_schema_privs(mvc *m, sql_schema *t);
extern int table_privs(mvc *m, sql_table *t, int privs);

extern int execute_priv(mvc *m, sql_func *f);

extern int sql_privilege(mvc *m, int auth_id, int obj_id, int privs, int sub);
extern int sql_grantable(mvc *m, int grantorid, int obj_id, int privs, int sub);
extern int sql_find_auth(mvc *m, str auth);
extern int sql_find_schema(mvc *m, str schema);

extern char *sql_create_role(mvc *m, str auth, int grantor);
extern char *sql_drop_role(mvc *m, str auth);
extern char *sql_grant_role(mvc *m, str grantee, str auth, int grantor, int admin);
extern char *sql_revoke_role(mvc *m, str grantee, str auth, int grantor, int admin);
extern int sql_create_privileges(mvc *m, sql_schema *s);
extern int sql_schema_has_user(mvc *m, sql_schema *s);


extern char * sql_create_user(mvc *sql, char *user, char *passwd, char enc, char *fullname, char *schema);
extern char * sql_drop_user(mvc *sql, char *user);
extern char * sql_alter_user(mvc *sql, char *user, char *passwd, char enc, char *schema, char *oldpasswd);
extern char * sql_rename_user(mvc *sql, char *olduser, char *newuser);
#endif /*_SQL_PRIV_H_ */
