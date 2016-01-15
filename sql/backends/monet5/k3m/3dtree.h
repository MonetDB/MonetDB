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

#ifndef __K3MATCH_3DTREE_H__
#define __K3MATCH_3DTREE_H__

typedef struct node_t node_t;

struct node_t {
  int axis;

  point_t* point;

  node_t *parent, *left, *right;
};

void k3m_build_balanced_tree(node_t *tree, point_t **points, int_t npoints, int axis, int_t *npool);

void k3m_print_tree(node_t *tree);

void k3m_print_dot_tree(node_t *tree);

node_t* k3m_closest_leaf(node_t *tree, point_t *point);

node_t* k3m_nearest_neighbour(node_t *tree, point_t *point);

int_t k3m_in_range(node_t *tree, point_t **match, point_t *search, real_t ds);

#endif // __K3MATCH_3DTREE_H__

