
/*******************************   stem.h   ***********************************

                                   UPLIFT 

  Purpose: 
  Header file for a version of Porter's algorithm for DUTCH based on the
  implementation by Frakes as documented in:
  Porter, M.F., "An Algorithm For Suffix Stripping," 
               Program 14 (3), July 1980, pp. 130-137

  The code is a result of the UPLIFT project (Utrecht Project: Linguistic
  Information for Free Text Retrieval) and is based on the joint efforts of
  Renee Pohlmann and Wessel Kraaij.

  Research Institute for Language and Speech, Utrecht University 1994.

  Copyright (C) 1995  Wessel Kraaij & Renee Pohlmann
    
  email: Wessel.Kraaij@let.ruu.nl  Renee.C.Pohlmann@let.ru.nl

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.
  
  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.
  
  You should have received a copy of the GNU Library General Public
  License along with this library; if not, write to the Free
  Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.


**/

#ifndef DSTEM_H
#define DSTEM_H

/******************************************************************************/
/****************************   Public Routines   *****************************/

#ifdef __STDC__

extern int dStem( char *word );
/* returns: 1 --> non-word containing numbers and hyphens, not stemmed
                  e.g. 12-5-1995
            2 --> word was not changed, e.g. 'loop' --> 'loop'
	    3 --> word was changed */

#else

extern int Stem();

#endif

#endif

