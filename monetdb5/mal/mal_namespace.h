/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAL_NAMESPACE_H
#define _MAL_NAMESPACE_H

/* ! please keep this list sorted for easier maintenance ! */
#define FOREACH_NAME(FUNC) \
	FUNC(affectedRows); \
	FUNC(aggr); \
	FUNC(alarm); \
	FUNC(algebra); \
	FUNC(alter_add_range_partition); \
	FUNC(alter_add_table); \
	FUNC(alter_add_value_partition); \
	FUNC(alter_del_table); \
	FUNC(alter_seq); \
	FUNC(alter_set_table); \
	FUNC(alter_table); \
	FUNC(alter_user); \
	FUNC(append); \
	FUNC(appendBulk); \
	FUNC(assert); \
	FUNC(avg); \
	FUNC(bandjoin); \
	FUNC(bat); \
	FUNC(batalgebra); \
	FUNC(batcalc); \
	FUNC(batcapi); \
	FUNC(batmal); \
	FUNC(batmkey); \
	FUNC(batmmath); \
	FUNC(batmtime); \
	FUNC(batpyapi3); \
	FUNC(batrapi); \
	FUNC(batsql); \
	FUNC(batstr); \
	FUNC(bbp); \
	FUNC(between); \
	FUNC(bind); \
	FUNC(bind_dbat); \
	FUNC(bind_idxbat); \
	FUNC(block); \
	FUNC(bstream); \
	FUNC(calc); \
	FUNC(capi); \
	FUNC(claim); \
	FUNC(clear_table); \
	FUNC(columnBind); \
	FUNC(comment_on); \
	FUNC(compress); \
	FUNC(connect); \
	FUNC(contains); \
	FUNC(copy_from); \
	FUNC(corr); \
	FUNC(count); \
	FUNC(count_no_nil); \
	FUNC(create_function); \
	FUNC(create_role); \
	FUNC(create_schema); \
	FUNC(create_seq); \
	FUNC(create_table); \
	FUNC(create_trigger); \
	FUNC(create_type); \
	FUNC(create_user); \
	FUNC(create_view); \
	FUNC(crossproduct); \
	FUNC(cume_dist); \
	FUNC(dataflow); \
	FUNC(dbl); \
	FUNC(decompress); \
	FUNC(defaultpipe); \
	FUNC(define); \
	FUNC(delete); \
	FUNC(delta); \
	FUNC(dense_rank); \
	FUNC(depend); \
	FUNC(deregister); \
	FUNC(dict); \
	FUNC(diff); \
	FUNC(diffcand); \
	FUNC(difference); \
	FUNC(disconnect); \
	FUNC(drop_constraint); \
	FUNC(drop_function); \
	FUNC(drop_index); \
	FUNC(drop_role); \
	FUNC(drop_schema); \
	FUNC(drop_seq); \
	FUNC(drop_table); \
	FUNC(drop_trigger); \
	FUNC(drop_type); \
	FUNC(drop_user); \
	FUNC(drop_view); \
	FUNC(emptybind); \
	FUNC(emptybindidx); \
	FUNC(endsWith); \
	FUNC(eval); \
	FUNC(exec); \
	FUNC(export_bin_column); \
	FUNC(exportOperation); \
	FUNC(export_table); \
	FUNC(fetch); \
	FUNC(find); \
	FUNC(firstn); \
	FUNC(first_value); \
	FUNC(for); \
	FUNC(generator); \
	FUNC(get); \
	FUNC(getVariable); \
	FUNC(grant); \
	FUNC(grant_function); \
	FUNC(grant_roles); \
	FUNC(group); \
	FUNC(groupby); \
	FUNC(groupdone); \
	FUNC(groupedfirstn); \
	FUNC(grow); \
	FUNC(hge); \
	FUNC(identity); \
	FUNC(ifthenelse); \
	FUNC(importColumn); \
	FUNC(int); \
	FUNC(intersect); \
	FUNC(intersectcand); \
	FUNC(io); \
	FUNC(iterator); \
	FUNC(join); \
	FUNC(json); \
	FUNC(lag); \
	FUNC(language); \
	FUNC(last_value); \
	FUNC(lead); \
	FUNC(leftjoin); \
	FUNC(like); \
	FUNC(likejoin); \
	FUNC(likeselect); \
	FUNC(likeuselect); \
	FUNC(lng); \
	FUNC(lock); \
	FUNC(lookup); \
	FUNC(main); \
	FUNC(mal); \
	FUNC(manifold); \
	FUNC(mapi); \
	FUNC(markjoin); \
	FUNC(markselect); \
	FUNC(mask); \
	FUNC(mat); \
	FUNC(max); \
	FUNC(maxlevenshtein); \
	FUNC(mdb); \
	FUNC(mergecand); \
	FUNC(mergepack); \
	FUNC(mergetable); \
	FUNC(min); \
	FUNC(minjarowinkler); \
	FUNC(mirror); \
	FUNC(mitosis); \
	FUNC(mmath); \
	FUNC(mtime); \
	FUNC(multiplex); \
	FUNC(mvc); \
	FUNC(new); \
	FUNC(next); \
	FUNC(not); \
	FUNC(not_like); \
	FUNC(not_unique); \
	FUNC(nth_value); \
	FUNC(ntile); \
	FUNC(optimizer); \
	FUNC(outercrossproduct); \
	FUNC(outerjoin); \
	FUNC(outerselect); \
	FUNC(pack); \
	FUNC(packIncrement); \
	FUNC(parameters); \
	FUNC(pass); \
	FUNC(percent_rank); \
	FUNC(predicate); \
	FUNC(print); \
	FUNC(prod); \
	FUNC(profiler); \
	FUNC(project); \
	FUNC(projectdelta); \
	FUNC(projection); \
	FUNC(projectionpath); \
	FUNC(put); \
	FUNC(pyapi3); \
	FUNC(querylog); \
	FUNC(raise); \
	FUNC(rangejoin); \
	FUNC(rank); \
	FUNC(rapi); \
	FUNC(reconnect); \
	FUNC(register); \
	FUNC(register_supervisor); \
	FUNC(remap); \
	FUNC(remote); \
	FUNC(rename_column); \
	FUNC(rename_schema); \
	FUNC(rename_table); \
	FUNC(rename_user); \
	FUNC(renumber); \
	FUNC(replace); \
	FUNC(resultSet); \
	FUNC(revoke); \
	FUNC(revoke_function); \
	FUNC(revoke_roles); \
	FUNC(row_number); \
	FUNC(rpc); \
	FUNC(rsColumn); \
	FUNC(rtree); \
	FUNC(sample); \
	FUNC(select); \
	FUNC(selectNotNil); \
	FUNC(sema); \
	FUNC(semijoin); \
	FUNC(series); \
	FUNC(setAccess); \
	FUNC(set_protocol); \
	FUNC(setVariable); \
	FUNC(single); \
	FUNC(slice); \
	FUNC(sort); \
	FUNC(sortReverse); \
	FUNC(sql); \
	FUNC(sqlcatalog); \
	FUNC(startsWith); \
	FUNC(stoptrace); \
	FUNC(str); \
	FUNC(streams); \
	FUNC(strimps); \
	FUNC(subavg); \
	FUNC(subcount); \
	FUNC(subdelta); \
	FUNC(subdiff); \
	FUNC(subeval_aggr); \
	FUNC(subgroup); \
	FUNC(subgroupdone); \
	FUNC(submax); \
	FUNC(submin); \
	FUNC(subprod); \
	FUNC(subslice); \
	FUNC(subsum); \
	FUNC(subuniform); \
	FUNC(sum); \
	FUNC(take); \
	FUNC(thetajoin); \
	FUNC(thetaselect); \
	FUNC(tid); \
	FUNC(total); \
	FUNC(transaction); \
	FUNC(transaction_abort); \
	FUNC(transaction_begin); \
	FUNC(transaction_commit); \
	FUNC(transaction_release); \
	FUNC(transaction_rollback); \
	FUNC(umask); \
	FUNC(unionfunc); \
	FUNC(unique); \
	FUNC(unlock); \
	FUNC(update); \
	FUNC(user); \
	FUNC(window_bound); \
	FUNC(zero_or_one)			/* no ; on last entry */
/* ! please keep this list sorted for easier maintenance ! */

#define NAME_EXPORT(NAME) mal_export const char NAME##Ref[]

FOREACH_NAME(NAME_EXPORT);
mal_export const char divRef[];
mal_export const char eqRef[];
mal_export const char minusRef[];
mal_export const char modRef[];
mal_export const char mulRef[];
mal_export const char plusRef[];

mal_export void initNamespace(void);
mal_export const char *putName(const char *nme);
mal_export const char *putNameLen(const char *nme, size_t len);
mal_export const char *getName(const char *nme);
mal_export const char *getNameLen(const char *nme, size_t len);

#endif /* _MAL_NAMESPACE_H */
