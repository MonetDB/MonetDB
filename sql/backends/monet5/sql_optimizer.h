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

#ifndef _SQL_OPTIMIZER_H_
#define _SQL_OPTIMIZER_H_
#include "sql.h"

//#define _SQL_OPTIMIZER_DEBUG

sql5_export void addQueryToCache(Client c);
sql5_export str SQLoptimizer(Client c);
sql5_export void SQLsetAccessMode(Client c);
sql5_export str SQLvalidatePipeline(void);
sql5_export str setOptimizers(str optimizer);

sql5_export str optimizerpipe;		/* the active pipeline */
sql5_export void addOptimizers(Client c,MalBlkPtr mb, int flag);
#endif /* _SQL_OPTIMIZER_H_ */

