#ifndef disclaimer_H
#define disclaimer_H

#include <stdio.h>
#include <stdlib.h>

#define DISC_WIDTH  79

extern char disclaimerfile[256];

extern int disclaimer;
void insertDisclaimer(FILE *,char *);

#endif
