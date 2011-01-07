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

#ifndef _FILEINFO_H_
#define _FILEINFO_H_

#include <stdio.h>
#include "list.h"

class FileInfo {
      public:
	FileInfo(FILE *f, int lineno, char *filename, char *basename, FileInfo * parent = NULL);
	~FileInfo();
	static char *find_module(char *name, List * dir_list);
	static FileInfo *open(char *filename, char *basename = NULL, FileInfo * parent = NULL);
	void close();
	int LineNo();
	void LineNo(int lineno);
	FILE *FilePtr();
	char *FileName();
	void FileName(char *fname);
	char *BaseName();
	FileInfo *Parent();
	void *Buffer ();
	void Buffer (void *buf);
      private:
	FILE *_f;
	int _lineno;
	char *_filename;
	char *_basename;
	FileInfo *_parent;
	void *_buf;
};

#endif // _FILEINFO_H_

