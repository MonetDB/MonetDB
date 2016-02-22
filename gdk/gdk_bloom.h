/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef GDK_BLOOM_H
#define GDK_BLOOM_H

/* round hashing */

#define smpl_xor_rng(R,X) {		\
R = X;					\
R ^= (R<<13);				\
R ^= (R>>17);				\
R ^= (R<<5);				\
}

#define hash_init(S,X,Y,Z) {		\
smpl_xor_rng(X,S);			\
smpl_xor_rng(Y,X);			\
smpl_xor_rng(Z,Y);			\
}

#define next_hash(N,X,Y,Z) {		\
N = (X^(X<<3))^(Y^(Y>>19))^(Z^(Z<<6));	\
X = Y;					\
Y = Z;					\
Z = N;					\
}

#define modulor(X,mask) ((X) & (mask))

#define remainder8(X)   ((X) &  7)

#define quotient8(X)    ((X) >> 3)

#endif /* GDK_BLOOM_H */
