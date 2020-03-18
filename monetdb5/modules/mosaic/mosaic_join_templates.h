#ifdef INNER_COMPRESSED_JOIN_LOOP_DEFINITION

#if (defined DO_DEFINE_INNER_JOIN_1 && defined DO_DEFINE_INNER_JOIN_2)

#if TYPE_IS_SUPPORTED(raw, TPE)
#include "mosaic_raw_templates.h"
#endif
#if TYPE_IS_SUPPORTED(runlength, TPE)
#include "mosaic_runlength_templates.h"
#endif
#if TYPE_IS_SUPPORTED(dict256, TPE)
#define METHOD dict256
#include "mosaic_dictionary_templates.h"
#undef METHOD
#endif
#if TYPE_IS_SUPPORTED(dict, TPE)
#define METHOD dict
#include "mosaic_dictionary_templates.h"
#undef METHOD
#endif
#if TYPE_IS_SUPPORTED(delta, TPE)
#include "mosaic_delta_templates.h"
#endif
#if TYPE_IS_SUPPORTED(linear, TPE)
#include "mosaic_linear_templates.h"
#endif
#if TYPE_IS_SUPPORTED(frame, TPE)
#include "mosaic_frame_templates.h"
#endif
#if TYPE_IS_SUPPORTED(prefix, TPE)
#include "mosaic_prefix_templates.h"
#endif

#endif // #if (defined DO_DEFINE_INNER_JOIN_1 && defined DO_DEFINE_INNER_JOIN_2)

#elif defined OUTER_LOOP_UNCOMPRESSED_DEFINITION

#ifndef DO_JOIN_INNER_LOOP
#define DO_JOIN_INNER_LOOP(METHOD, TPE, NIL, RIGHT_CI_NEXT)\
{\
	TRC_DEBUG(ALGO, "#MOSjoin_" STRINGIFY(METHOD) "\n");\
	str msg =  MOSjoinInnerLoop_ID(METHOD, TPE, NIL, RIGHT_CI_NEXT)(task, r1p, r2p, lo, lval);\
	if (msg != MAL_SUCCEED) return msg;\
	CONCAT4(MOSadvance_, METHOD, _, TPE)(task);\
}
#endif

#define MOSouterloopUncompressed_ID(TPE, NIL, NIL_SEMANTICS, LEFT_CI_NEXT, RIGHT_CI_NEXT) CONCAT6(OUTER_LOOP_UNCOMPRESSED, NIL, NIL_SEMANTICS, CONCAT2(_, TPE), CONCAT2(_, LEFT_CI_NEXT), CONCAT2(_, RIGHT_CI_NEXT) )
static inline str MOSouterloopUncompressed_ID(TPE, NIL, NIL_SEMANTICS, LEFT_CI_NEXT, RIGHT_CI_NEXT)
(MOStask* task, BAT* l, struct canditer* lci, struct canditer* rci)
{
	str msg = MAL_SUCCEED;

    BAT* r1p = task->lbat;
    BAT* r2p = task->rbat;

	TPE* vl = (TPE*) Tloc(l, 0);
	for (BUN li = 0; li < lci->ncand; li++, MOSinitializeScan(task, task->bsrc), canditer_reset(rci)) {
		oid lo = LEFT_CI_NEXT(lci);
		TPE lval = vl[lo-l->hseqbase];
#if !defined HAS_NO_NIL && defined NILS_DO_NOT_MATCH
			if ((IS_NIL(TPE, lval))) {continue;};
#endif

		while(task->start < task->stop ){
			BUN first = task->start;
			BUN last = first + MOSgetCnt(task->blk);

			oid c = RIGHT_CI_NEXT(rci);
			while (!is_oid_nil(c) && c < first ) {
				c = RIGHT_CI_NEXT(rci);
			}
			switch(MOSgetTag(task->blk)){
			case MOSAIC_RLE:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, runlength, TPE, NIL, RIGHT_CI_NEXT);
				break;
			case MOSAIC_DICT256:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, dict256, TPE, NIL, RIGHT_CI_NEXT);
				break;
			case MOSAIC_DICT:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, dict, TPE, NIL, RIGHT_CI_NEXT);
				break;
			case MOSAIC_FRAME:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, frame, TPE, NIL, RIGHT_CI_NEXT);
				break;
			case MOSAIC_DELTA:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, delta, TPE, NIL, RIGHT_CI_NEXT);
				break;
			case MOSAIC_PREFIX:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, prefix, TPE, NIL, RIGHT_CI_NEXT);
				break;
			case MOSAIC_LINEAR:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, linear, TPE, NIL, RIGHT_CI_NEXT);
				break;
			case MOSAIC_RAW:
				DO_OPERATION_IF_ALLOWED_VARIADIC(DO_JOIN_INNER_LOOP, raw, TPE, NIL, RIGHT_CI_NEXT);
				break;
			default:
				assert(0);
			}

			if (msg != MAL_SUCCEED) return msg;

		if (canditer_peekprev(task->ci) >= last) {
			/*Restore iterator if it went pass the end*/
			(void) canditer_prev(task->ci);
		}

			if (rci->next == rci->ncand) {
				/* We are at the end of the candidate list.
				* So we can stop now.
				*/
				break;
			}
		}
	}

	return MAL_SUCCEED;
}

#elif defined JOIN_WITH_NIL_INFO_DEFINITION

#define MOSjoinWithNilInfo_ID(TPE, NIL, NIL_SEMANTICS) CONCAT4(JOIN_WITH_NIL_INFO_, TPE, NIL, NIL_SEMANTICS)
static inline str MOSjoinWithNilInfo_ID(TPE, NIL, NIL_SEMANTICS) (MOStask* task, BAT* l, struct canditer* lci)
{
	struct canditer* rci = task->ci;

	if( (lci->tpe == cand_dense) && (rci->tpe == cand_dense)){
		return MOSouterloopUncompressed_ID(TPE, NIL, NIL_SEMANTICS, canditer_next_dense, canditer_next_dense)(task, l, lci, rci);
	}
	if( (lci->tpe != cand_dense) && (rci->tpe == cand_dense)){
		return MOSouterloopUncompressed_ID(TPE, NIL, NIL_SEMANTICS, canditer_next, canditer_next_dense)(task, l, lci, rci);
	}
	if( (lci->tpe == cand_dense) && (rci->tpe != cand_dense)){
		return MOSouterloopUncompressed_ID(TPE, NIL, NIL_SEMANTICS, canditer_next_dense, canditer_next)(task, l, lci, rci);
	}
	if( (lci->tpe != cand_dense) && (rci->tpe != cand_dense)){
		return MOSouterloopUncompressed_ID(TPE, NIL, NIL_SEMANTICS, canditer_next, canditer_next)(task, l, lci, rci);
	}

	assert(0);
}

#elif defined MOSjoin_DEFINITION

static str MOSjoin_ID(TPE) (MOStask* task, BAT* l, struct canditer* lci, bool nil_matches)
{
	bool maybe_nil = !l->tnonil;

	if( maybe_nil && nil_matches){
		return MOSjoinWithNilInfo_ID(TPE, _maybeHasNil, _nilsMatch) (task, l, lci);
	}
	if( !maybe_nil && nil_matches){
		return MOSjoinWithNilInfo_ID(TPE, _hasNoNil, _nilsMatch) (task, l, lci);
	}
	if( maybe_nil && !nil_matches){
		return MOSjoinWithNilInfo_ID(TPE, _maybeHasNil, _nilsDoNotMatch) (task, l, lci);
	}
	if( !maybe_nil && !nil_matches){
		return MOSjoinWithNilInfo_ID(TPE, _hasNoNil, _nilsDoNotMatch) (task, l, lci);
	}

	return MAL_SUCCEED;
}

// macro adiministration
#elif ! defined NIL
#define NIL _maybeHasNil
#include "mosaic_join_templates.h"
#undef NIL
#define NIL _hasNoNil
#define HAS_NO_NIL
#include "mosaic_join_templates.h"
#undef HAS_NO_NIL
#undef NIL
#define MOSjoin_DEFINITION
#include "mosaic_join_templates.h"
#undef MOSjoin_DEFINITION
#elif ! defined NIL_SEMANTICS
#define NIL_SEMANTICS _nilsMatch
#define DO_DEFINE_INNER_JOIN_1
#include "mosaic_join_templates.h"
#undef DO_DEFINE_INNER_JOIN_1
#undef NIL_SEMANTICS
#define NIL_SEMANTICS _nilsDoNotMatch
#define NILS_DO_NOT_MATCH
#include "mosaic_join_templates.h"
#undef NILS_DO_NOT_MATCH
#undef NIL_SEMANTICS
#elif ! defined LEFT_CI_NEXT
#define LEFT_CI_NEXT canditer_next
#define DO_DEFINE_INNER_JOIN_2
#include "mosaic_join_templates.h"
#undef DO_DEFINE_INNER_JOIN_2
#undef LEFT_CI_NEXT
#define LEFT_CI_NEXT canditer_next_dense
#include "mosaic_join_templates.h"
#undef LEFT_CI_NEXT
#define JOIN_WITH_NIL_INFO_DEFINITION
#include "mosaic_join_templates.h"
#undef JOIN_WITH_NIL_INFO_DEFINITION
#elif ! defined RIGHT_CI_NEXT
#define RIGHT_CI_NEXT canditer_next
#define INNER_COMPRESSED_JOIN_LOOP_DEFINITION
#include "mosaic_join_templates.h"
#undef INNER_COMPRESSED_JOIN_LOOP_DEFINITION
#define OUTER_LOOP_UNCOMPRESSED_DEFINITION
#include "mosaic_join_templates.h"
#undef OUTER_LOOP_UNCOMPRESSED_DEFINITION
#undef RIGHT_CI_NEXT
#define RIGHT_CI_NEXT canditer_next_dense
#define INNER_COMPRESSED_JOIN_LOOP_DEFINITION
#include "mosaic_join_templates.h"
#undef INNER_COMPRESSED_JOIN_LOOP_DEFINITION
#define OUTER_LOOP_UNCOMPRESSED_DEFINITION
#include "mosaic_join_templates.h"
#undef OUTER_LOOP_UNCOMPRESSED_DEFINITION
#undef RIGHT_CI_NEXT
#endif
