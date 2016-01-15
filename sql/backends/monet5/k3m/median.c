/**************************************************************************
 *  This file is part of the K3Match library.                             *
 *  Copyright (C) 2010 Pim Schellart <P.Schellart@astro.ru.nl>            *
 *                                                                        *
 *  This library is free software: you can redistribute it and/or modify  *
 *  it under the terms of the GNU General Public License as published by  *
 *  the Free Software Foundation, either version 3 of the License, or     *
 *  (at your option) any later version.                                   *
 *                                                                        * 
 *  This library is distributed in the hope that it will be useful,       *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 *  GNU General Public License for more details.                          *
 *                                                                        *
 *  You should have received a copy of the GNU General Public License     *
 *  along with this library. If not, see <http://www.gnu.org/licenses/>.  *
 **************************************************************************/

#include <stdlib.h>

#include <median.h>

#define SWAP(a,b) { temp=(a); (a)=(b); (b)=temp; }

point_t* k3m_median(point_t **array, const unsigned long n, const unsigned int axis)
{
  point_t *temp = NULL;
  unsigned long low, high ;
  unsigned long median;
  unsigned long middle, ll, hh;

  low = 0 ; high = n-1 ; median = (low + high) / 2;
  for (;;)
  {
    if (high <= low)
    {
      /* One element only */
      return array[median];
    }

    if (high == low + 1)
    {
      /* Two elements only */
      if (array[low]->value[axis] > array[high]->value[axis])
        SWAP(array[low], array[high]);
      return array[median];
    }

    /* Find median of low, middle and high items; swap into position low */
    middle = (low + high) / 2;
    if (array[middle]->value[axis] > array[high]->value[axis]) SWAP(array[middle], array[high]);
    if (array[low]->value[axis] > array[high]->value[axis]) SWAP(array[low], array[high]);
    if (array[middle]->value[axis] > array[low]->value[axis]) SWAP(array[middle], array[low]);

    /* Swap low item (now in position middle) into position (low+1) */
    SWAP(array[middle], array[low+1]);

    /* Nibble from each end towards middle, swapping items when stuck */
    ll = low + 1;
    hh = high;
    for (;;)
    {
      do ll++; while (array[low]->value[axis] > array[ll]->value[axis]);
      do hh--; while (array[hh]->value[axis]  > array[low]->value[axis]);

      if (hh < ll) break;

      SWAP(array[ll], array[hh]);
    }

    /* Swap middle item (in position low) back into correct position */
    SWAP(array[low], array[hh]);

    /* Re-set active partition */
    if (hh <= median) low = ll;
    if (hh >= median) high = hh - 1;
  }
}

