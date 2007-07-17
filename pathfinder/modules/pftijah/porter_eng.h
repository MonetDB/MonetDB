
/*******************************   stem.h   ***********************************

   Purpose: Header file for an implementation of the Porter stemming 
            algorithm.

   Notes:   This module implemnts a fast stemming function whose results
            are about as good as any other.
**/

#ifndef STEM_H
#define STEM_H

/******************************************************************************/
/****************************   Public Routines   *****************************/

extern int Stem( char *word );      /* returns 1 on success, 0 otherwise */

#endif

