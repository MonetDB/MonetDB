/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 * 
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

#ifndef _ODBC_DRIVER_INI_H_
#define _ODBC_DRIVER_INI_H_

int __SQLGetPrivateProfileString(char *section, char *entry, char *def_value, char *buf, int buf_len, char *filename);	/* used in SQLConnect.c */

#endif
