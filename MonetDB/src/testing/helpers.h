
#ifndef HELPERS_H
#define HELPERS_H

#include <stdio.h>

#define MAX(a,b) (((a)>(b))?(a):(b))
#define MIN(a,b) (((a)<(b))?(a):(b))

void ErrXit (char* text1, char* text2, int num);
FILE* Rfopen (char* name);
FILE* Wfopen (char* name);
FILE* Afopen (char* name);
char* strconcat (char* a, char* b);
int isalpha_ (int c);
char* filename (char* path);
char* tmpdir();

#endif /* HELPERS_H */

