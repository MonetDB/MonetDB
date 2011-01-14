// The contents of this file are subject to the MonetDB Public License
// Version 1.1 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
//
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
// License for the specific language governing rights and limitations
// under the License.
//
// The Original Code is the MonetDB Database System.
//
// The Initial Developer of the Original Code is CWI.
// Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
// Copyright August 2008-2011 MonetDB B.V.
// All Rights Reserved.

#include "monetdb_config.h"
#include "mel.h"
#include "parser.h"
#ifdef HAVE_IOSTREAM
#include <iostream>
#include <fstream>
using namespace std;
#else
#include <iostream.h>
#include <fstream.h>
#endif

#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#ifdef HAVE_IO_H
# include <io.h>
#endif

#include "use.h"
#include "link.h"
#include "depend.h"
#include "proto.h"
#include "html.h"
#include "glue.h"
#include "mil.h"

int parse_error = 0;		/* number of parse errors. */
int sem_error = 0;		/* number of semantic errors. */
char *progname = 0;
char *outfile = 0;
Symtable *table = NULL;
Symbol *root = NULL;
FileInfo *fileinfo = NULL;
List *dirs = NULL;
const char *config_h = "monetdb4_config.h";

language *l = NULL;

#if defined(__MINGW32_VERSION) || !defined(HAVE_STRDUP)
char *
strdup(const char *s)
{
	return strcpy((char *) malloc(strlen(s) + 1), s);
}
#endif

const char *
token2str(int token)
{
	switch (token) {
	case MEL_MODULE:
		return "module";
	case MEL_ITERATOR:
		return "iterator";
	case MEL_USE:
		return "use";
	case MEL_PRELUDE:
		return "prelude";
	case MEL_EPILOGUE:
		return "epilogue";
	case MEL_END:
		return "end";
	case MEL_IDENTIFIER:
		return "identifier";
	case MEL_STRING:
		return "string";
	case MEL_BUILTIN:
		return "builtin";
	case MEL_OBJECT:
		return "object";
	case MEL_COMMAND:
		return "command";
	case MEL_OPERATOR0:
		return "operator0";
	case MEL_OPERATOR:
		return "operator";
	case MEL_OPERATOR1:
		return "operator1";
	case MEL_ATOM:
		return "atom";
	case MEL_ATOMOP:
		return "atom TODO";
	case MEL_ANY:
		return "any";
	case MEL_BAT:
		return "BAT";
	case MEL_TYPE:
		return "builtin type";
	case MEL_VARARGS:
		return "...";
	case MEL_SEP:
		return "::";
	case MEL_NUMBER:
		return "number";
	}
	return "default";
}

const char *
type2str(int t)
{
	switch (t) {
	case TYPE_BIT:
		return "bit";
	case TYPE_CHR:
		return "chr";
	case TYPE_BTE:
		return "bte";
	case TYPE_SHT:
		return "sht";
	case TYPE_INT:
		return "int";
	case TYPE_PTR:
		return "ptr";
	case TYPE_OID:
		return "oid";
	case TYPE_WRD:
		return "wrd";
	case TYPE_FLT:
		return "flt";
	case TYPE_DBL:
		return "dbl";
	case TYPE_LNG:
		return "lng";
	case TYPE_STR:
		return "str";
	}
	return "void";
}

void
usage(char *prg)
{
	cerr << "usage: " << prg << " [options] [type] [type_options] file\n";
	cerr << "options\n";
	cerr << "\t -Iinclude_dir\n";
	cerr << "\t -o output_file\n";
	cerr << "types\n";
	cerr << "\t -html     /* Documentation generation            */\n";
	cerr << "\t -proto    /* Proto-type generation               */\n";
	cerr << "\t -depend   /* Dependency generation               */\n";
	cerr << "\t -use      /* Use list generation                 */\n";
	cerr << "\t -L        /* Link information                    */\n";
	cerr << "\t -glue     /* generation of code to glue this mel module to Monet */\n";
	cerr << "\t -mil      /* generation of mil module load code */\n";
}

int
handle_args(int argc, char *argv[])
{
	int i = 1;

	if (argc < 2) {
		usage(argv[0]);
		return 0;
	}
	progname = argv[0];
	while (i < argc - 1) {
		if (argv[i][0] != '-') {
			cerr << "Unknown option " << argv[i] << "\n";
			usage(argv[0]);
			return 0;
		}
		switch (argv[i][1]) {
		case 'I':
			if (argv[i][2] == '\0') {
				i++;
				dirs->insert(strdup(argv[i]));
			} else {
				dirs->insert(strdup(argv[i] + 2));
			}
			break;
		case 'D':
			/* ignore -D option */
			if (argv[i][2] == '\0')
				i++;
			break;
		case 'd':
			l = new depend ();

			break;
		case 'u':
			l = new use();
			break;
		case 'o':
			if (argv[i][2] == '\0') {
				i++;
				dirs->insert(strdup(argv[i]));
				outfile = strdup(argv[i]);
			} else {
				outfile = strdup(argv[i] + 2);
			}
			break;
		case 'm':
			l = new mil();
			break;
		case 'p':
			l = new proto ();

			break;
		case 'g':
			l = new glue ();

			break;
		case 'h':
			l = new html();
			break;
		case 'L':
			l = new Link();
			break;
		case 'c':
			if (argv[i][2] == 0) {
				i++;
				config_h = argv[i];
			} else {
				config_h = argv[i] + 2;
			}
			break;
		default:
			cerr << "Unknown option " << argv[i] << "\n";
			usage(argv[0]);
			return 0;
		}
		i++;
		if (l) {
			return l->handle_args(argc - 1 - i, argv + i);
		}
	}
	return 1;
}

int
main(int argc, char *argv[])
{
	/* argument parsing will be done in two parts
	 * First general arguments
	 * second lang specific
	 */
	dirs = new List (50);

	if (!handle_args(argc, argv)) {
		exit(EXIT_FAILURE);
	}
	char *filename = argv[argc - 1];
	fileinfo = FileInfo::open(filename);

	if (fileinfo) {
		yyin = fileinfo->FilePtr();
		table = new Symtable (500);
		table->insert(*new Symbol (MEL_USE, fileinfo->BaseName()));

		(void) yyparse();
		if (parse_error || sem_error) {
			if (parse_error) {
				cerr << "Found " << parse_error;
				cerr << " syntax errors.\n";
			}
			if (sem_error) {
				cerr << "Found " << sem_error;
				cerr << " semantic errors.\n";
			}
			exit(EXIT_FAILURE);
		}
		if (!l) {
			l = new proto ();
		}
		if (outfile) {
			ofstream & o = *new ofstream(outfile);
			if (!o) {
				cerr << "Error: output file exists.\n";
				exit(EXIT_FAILURE);
			}
			l->generate_table(o, table);
			l->generate_code(o, root);
			o.close();
			delete(&o);
		} else {
			l->generate_table(cout, table);
			l->generate_code(cout, root);
		}
		delete(l);
		delete(table);
		delete(root);
	} else {
		cerr << "File not found " << filename << "\n";
		exit(EXIT_FAILURE);
	}
	delete(fileinfo);
	return 0;
}
