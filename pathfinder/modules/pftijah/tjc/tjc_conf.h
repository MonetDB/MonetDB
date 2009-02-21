/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
* Copyright Notice:
* -----------------
*
* The contents of this file are subject to the PfTijah Public License
* Version 1.1 (the "License"); you may not use this file except in
* compliance with the License. You may obtain a copy of the License at
* http://dbappl.cs.utwente.nl/Legal/PfTijah-1.1.html
*
* Software distributed under the License is distributed on an "AS IS"
* basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
* License for the specific language governing rights and limitations
* under the License.
*
* The Original Code is the PfTijah system.
*
* The Initial Developer of the Original Code is the "University of Twente".
* Portions created by the "University of Twente" are
* Copyright (C) 2006-2009 "University of Twente".
*
* Portions created by the "CWI" are
* Copyright (C) 2008-2009 "CWI".
*
* All Rights Reserved.
* 
* Author(s): Henning Rode 
*            Jan Flokstra
*/

#ifndef TJC_CONF_H
#define TJC_CONF_H

#define TJCmalloc GDKmalloc
#define TJCfree   GDKfree

#define MAXMILSIZE      32000

typedef struct tjc_config {
	char* dotFile;
	char* milFile;
	short debug;
	char timing;
	char* ftindex;
	char* irmodel;
	char* conceptirmodel;
	char* orcomb;
	char* andcomb;
	char* upprop;
	char* downprop;
	char* prior;
	double scorebase;
	double lambda;
	double okapik1;
	double okapib;
	char returnall;
	char rmoverlap;
	char errBUFF[1024];
	char milBUFF[MAXMILSIZE];
	char dotBUFF[MAXMILSIZE];
} tjc_config;


#define TJCPRINTF sprintf
#define MILOUT    &(tjc_c->milBUFF[strlen(tjc_c->milBUFF)])
#define DOTOUT    &(tjc_c->dotBUFF[strlen(tjc_c->dotBUFF)])

extern tjc_config* tjc_c_GLOBAL;

extern void setTJCscanstring(const char *);

#endif
