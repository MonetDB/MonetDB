/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @- The Problem
 * When creating a join, we want to make a unique key of the attributes on both
 * sides and then join these keys. Consider the following BATs.
 *
 * @verbatim
 * orders                  customer                link
 * ====================    =====================   ===========
 *         zipcode h_nr            zipcode hnr    oid     cid
 * o1      13      9       c1      11      10      o1      c5
 * o2      11      10      c2      11      11      o2      c1
 * o3      11      11      c3      12      2       o3      c2
 * o4      12      5       c4      12      1       o4      nil
 * o5      11      10      c5      13      9       o5      c1
 * o6      12      2       c6      14      7       o6      c3
 * o7      13      9                               o7      c5
 * o8      12      1                               o8      c4
 * o9      13      9                               o9      c5
 * @end verbatim
 *
 * The current approach is designed to take minimal memory, as our previous
 * solutions to the problem did not scale well. In case of singular keys,
 * the link is executed by a simple join. Before going into the join, we
 * make sure the end result size is not too large, which is done by looking
 * at relation sizes (if the other key is unique) or, if that is not possible,
 * by computing the exact join size.
 *
 * The join algorithm was also improved to do dynamic sampling to determine
 * with high accuracy the join size, so that we can alloc in one go a memory
 * region of sufficient size. This also reduces the ds\_link memory requirements.
 *
 * For compound keys, those that consist of multiple attributes, we now compute
 * a derived column that contains an integer hash value derived from all
 * key columns.
 * This is done by computing a hash value for each individual key column
 * and combining those by bitwise XOR and left-rotation. That is, for each
 * column,we rotate the working hash value by N bits and XOR the hash value
 * of the column over it. The working hash value is initialized with zero,
 * and after all columns are processed, this working value is used as output.
 * Computing the hash value for all columns in the key for one table is done
 * by the command hash(). Hence, we do hash on both sides, and join
 * that together with a simple join:
 *
 * @code{join(hash(keys), hash(keys.reverse);}
 *
 * One complication of this procedure are nil values:
 * @table
 * @itemize
 * @item
 * it may happen that the final hash-value (an int formed by a
 * random bit pattern) accidentally has the value of int(nil).
 * Notice that join never matches nil values.
 * Hence these accidental nils must be replaced by a begin value (currently: 0).
 * @item
 * in case any of the compound key values is nil, our nil semantics
 * require us that those tuples may never match on a join. Consequently,
 * during the hash() processing of all compound key columns for computing
 * the hash value, we also maintain a bit-bat that records which tuples had
 * a nil value. The bit-bat is initialized to false, and the results of the
 * nil-check on each column is OR-ed to it.
 * Afterwards, the hash-value of all tuples that have this nil-bit set to
 * TRUE are forced to int(nil), which will exclude them from matching.
 * @end itemize
 *
 * Joining on hash values produces a @emph{superset} of the join result:
 * it may happen that  two different key combinations hash on the same value,
 * which will make them match on the join (false hits). The final part
 * of the ds\_link therefore consists of filtering out the false hits.
 * This is done incrementally by joining back the join result to the original
 * columns, incrementally one by one for each pair of corresponding
 * columns. These values are compared with each other and we AND the
 * result of this comparison together for each pair of columns.
 * The bat containing these bits is initialized to all TRUE and serves as
 * final result after all column pairs have been compared.
 * The initial join result is finally filtered with this bit-bat.
 *
 * Joining back from the initial join-result to the original columns on
 * both sides takes quite a lot of memory. For this reason, the false
 * hit-filtering is done in slices (not all tuples at one time).
 * In this way the memory requirements of this phase are kept low.
 * In fact, the most memory demanding part of the join is the int-join
 * on hash number, which takes N*24 bytes (where N= |L| = |R|).
 * In comparison, the previous CTmultigroup/CTmultiderive approach
 * took N*48 bytes. Additionally, by making it possible to use merge-sort,
 * it avoids severe performance degradation (memory thrashing) as produced
 * by the old ds\_link when the inner join relation would be larger than memory.
 *
 * If ds\_link performance is still an issue, the sort-merge join used here
 * could be replaced by partitioned hash-join with radix-cluster/decluster.
 *
 * @+ Implementation
 */
#ifndef _MKEY_H
#define _MKEY_H

/*#define _DEBUG_MKEY_  */

#include "mal.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

mal_export str  MKEYrotate(lng *ret, const lng *v, const int *nbits);
mal_export str  MKEYhash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str  MKEYrotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str  MKEYbulk_rotate_xor_hash(bat *ret, const bat *hid, const int *nbits, const bat *bid);
mal_export str  MKEYbulkconst_rotate_xor_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str  MKEYconstbulk_rotate_xor_hash(bat *ret, const lng *h, const int *nbits, const bat *bid);
mal_export str  MKEYbathash(bat *res, const bat *bid);

#endif /* _MKEY_H */
