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
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

#include "embeddedclient.h"

int
main(int argc, char **argv)
{
	Mapi mid;
	MapiHdl hdl;
	char *line;

	(void) argc;
	mid = embedded_sql();
	while (*++argv) {
		hdl = mapi_query(mid, *argv);
		do {
			if (mapi_result_error(hdl) != NULL)
				mapi_explain_result(hdl, stderr);
			while ((line = mapi_fetch_line(hdl)) != NULL)
				printf("%s\n", line);
		} while (mapi_next_result(hdl) == 1);
		mapi_close_handle(hdl);
	}
	return 0;
}
