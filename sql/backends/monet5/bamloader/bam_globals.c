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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (author) R Cijvat
 * This file contains some global definitions, used by multiple bam library files
 */

#include "monetdb_config.h"

#include "bam_globals.h"


#ifdef BAM_DEBUG
/**
 * Function prepares a string for the log by adding hashes in front of every line
 * Returned string has to be freed
 */
char *
prepare_for_log(const char *str, bit first_line_dash) {
    int l = strlen(str);
    char *prepared = (char *)GDKmalloc(3 * l * sizeof(char)); /* Worst case: every character is newline */
    int i, j = 0;
    
    if(prepared == NULL) return NULL;
    
    if(first_line_dash) {
        prepared[j++] = '#';
        prepared[j++] = ' ';
    }
    for(i=0; i<l; ++i) {
        prepared[j++] = str[i];
        if(str[i] == '\n') {
            prepared[j++] = '#';
            prepared[j++] = ' ';
        }
    }
    prepared[j] = '\0';
    return prepared;
}

/** 
 * Function that adds a dash before every printed line, so Mtest.py will not notice a difference in whether or not we are
 * debugging. Arguments to this function should already follow this convention. So calling functions should use
 * prepare_for_log to prepare all arguments.
 */
int 
dash_fprintf(FILE *f, const char *format, ...) {
    va_list arg;
    int done;
    
    char *format_prepared = prepare_for_log(format, TRUE);
    
    if(format_prepared == NULL) return -1;
    
    va_start(arg, format);
    done = vfprintf(f, format_prepared, arg);
    va_end(arg);

    GDKfree(format_prepared);
    return done;
}


#endif
