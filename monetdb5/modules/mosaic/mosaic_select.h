#ifndef _MOSAIC_SELECT_
#define _MOSAIC_SELECT_

#include "mosaic.h"

/* generic range select
 *
 * This macro is based on the combined behavior of ALGselect2 and BATselect.
 * It should return the same output on the same input.
 *
 * A complete breakdown of the various arguments follows.  Here, v, v1
 * and v2 are values from the appropriate domain, and
 * v != nil, v1 != nil, v2 != nil, v1 < v2.
 *	tl	th	li	hi	anti	result list of OIDs for values
 *	-----------------------------------------------------------------
 *	nil	nil	true	true	false	x == nil (only way to get nil)
 *	nil	nil	true	true	true	x != nil
 *	nil	nil	A*		B*		false	x != nil *it must hold that A && B == false.
 *	nil	nil	A*		B*		true	NOTHING *it must hold that A && B == false.
 *	v	v	A*		B*		true	x != nil *it must hold that A && B == false.
 *	v	v	A*		B*		false	NOTHING *it must hold that A && B == false.
 *	v2	v1	ignored	ignored	false	NOTHING
 *	v2	v1	ignored	ignored	true	x != nil
 *	nil	v	ignored	false	false	x < v
 *	nil	v	ignored	true	false	x <= v
 *	nil	v	ignored	false	true	x >= v
 *	nil	v	ignored	true	true	x > v
 *	v	nil	false	ignored	false	x > v
 *	v	nil	true	ignored	false	x >= v
 *	v	nil	false	ignored	true	x <= v
 *	v	nil	true	ignored	true	x < v
 *	v	v	true	true	false	x == v
 *	v	v	true	true	true	x != v
 *	v1	v2	false	false	false	v1 < x < v2
 *	v1	v2	true	false	false	v1 <= x < v2
 *	v1	v2	false	true	false	v1 < x <= v2
 *	v1	v2	true	true	false	v1 <= x <= v2
 *	v1	v2	false	false	true	x <= v1 or x >= v2
 *	v1	v2	true	false	true	x < v1 or x >= v2
 *	v1	v2	false	true	true	x <= v1 or x > v2
 */
#define select_general(HAS_NIL, ANTI, TPE, canditer_next, SCAN_LOOP)\
do {\
	if		( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && li && hi && !ANTI) {\
		if(HAS_NIL) {\
			SCAN_LOOP(TPE, canditer_next, is_##TPE##_nil(v)); \
		}\
		/*else*/\
			/*Empty result set.*/\
	}\
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && li && hi && ANTI) {\
		if(HAS_NIL) {\
			SCAN_LOOP(TPE, canditer_next, !is_##TPE##_nil(v)); \
		}\
		else SCAN_LOOP(TPE, canditer_next, true);\
	}\
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && !(li && hi) && !ANTI) {\
		if(HAS_NIL) {\
			SCAN_LOOP(TPE, canditer_next, !is_##TPE##_nil(v)); \
		}\
		else SCAN_LOOP(TPE, canditer_next, true);\
	}\
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && !(li && hi) && ANTI) {\
			/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl == th && !(li && hi) && ANTI) {\
		if(HAS_NIL) {\
			SCAN_LOOP(TPE, canditer_next, !is_##TPE##_nil(v)); \
		}\
		else SCAN_LOOP(TPE, canditer_next, true);\
	}\
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl == th && !(li && hi) && !ANTI) {\
		/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl > th && !ANTI) {\
		/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl > th && ANTI) {\
		if(HAS_NIL) {\
			SCAN_LOOP(TPE, canditer_next, !is_##TPE##_nil(v)); \
		}\
		else SCAN_LOOP(TPE, canditer_next, true);\
	}\
	else {\
		/*normal cases.*/\
		if( IS_NIL(TPE, tl) ){\
			SCAN_LOOP(TPE, canditer_next,\
				!(HAS_NIL && IS_NIL(TPE, v)) &&\
				(((hi && v <= th ) || (!hi && v < th )) == !ANTI));\
		} else\
		if( IS_NIL(TPE, th) ){\
			SCAN_LOOP(TPE, canditer_next,\
				!(HAS_NIL && IS_NIL(TPE, v)) &&\
				(((li && v >= tl ) || (!li && v > tl )) == !ANTI));\
		} else\
		if (tl == th){\
			SCAN_LOOP(TPE, canditer_next,\
				!(HAS_NIL && IS_NIL(TPE, v)) && \
				((hi && v == th)  == !ANTI));\
		}\
		else{\
			SCAN_LOOP(TPE, canditer_next,\
				!(HAS_NIL && IS_NIL(TPE, v)) && \
				((((hi && v <= th ) || (!hi && v < th )) &&\
				((li && v >= tl ) || (!li && v > tl )))  == !ANTI));\
		}\
	}\
} while (0)

#define MOSselect_SIGNATURE(NAME, TPE) \
str								\
MOSselect_##NAME##_##TPE(MOStask task,		\
	      TPE tl, TPE th, bool li, bool hi,		\
	      bool anti)	\

/* definition of type-specific core scan select function */
#define MOSselect_DEF(NAME, TPE)\
MOSselect_SIGNATURE(NAME, TPE) {\
	oid *o;\
	BUN first,last;\
	TPE v;\
\
	/* set the oid range covered and advance scan range*/\
	first = task->start;\
	last = first + MOSgetCnt(task->blk);\
	bool nil = !task->bsrc->tnonil;\
	o = task->lb;\
\
	/* Advance the candidate iterator to the first element within
	 * the oid range of the current block.
	 */\
	oid c = canditer_next(task->ci);\
	while (!is_oid_nil(c) && c < first ) {\
		c = canditer_next(task->ci);\
	}\
\
	if 		(is_oid_nil(c)) {\
		/* Nothing left to scan.
		 * So we can signal the generic select function to stop now.
		 */\
		return MAL_SUCCEED;\
	}\
	else if	( nil && anti){\
		if (task->ci->tpe == cand_dense)\
			select_general(true, true, TPE, canditer_next_dense, scan_loop_##NAME);\
		else\
			select_general(true, true, TPE, canditer_next, scan_loop_##NAME);\
	}\
	else if	( !nil && anti){\
		if (task->ci->tpe == cand_dense)\
			select_general(false, true, TPE, canditer_next_dense, scan_loop_##NAME);\
		else\
			select_general(false, true, TPE, canditer_next, scan_loop_##NAME);\
	}\
	else if	( nil && !anti){\
		if (task->ci->tpe == cand_dense)\
			select_general(true, false, TPE, canditer_next_dense, scan_loop_##NAME);\
		else\
			select_general(true, false, TPE, canditer_next, scan_loop_##NAME);\
	}\
	else if	( !nil && !anti){\
		if (task->ci->tpe == cand_dense)\
			select_general(false, false, TPE, canditer_next_dense, scan_loop_##NAME);\
		else\
			select_general(false, false, TPE, canditer_next, scan_loop_##NAME);\
	}\
\
	if ((c = canditer_peekprev(task->ci)) >= last) {\
		/*Restore iterator if it went pass the end*/\
		(void) canditer_prev(task->ci);\
	}\
\
	MOSskip_##NAME(task);\
	task->lb = o;\
	return MAL_SUCCEED;\
}

#define do_select(NAME, TPE) \
    MOSselect_##NAME##_##TPE(\
        task,\
        *(TPE*) low,\
        *(TPE*) hgh,\
        *li,\
        *hi,\
        *anti)

#define MOSselect_generic_DEF(TPE) \
static str MOSselect_##TPE(\
MOStask task, void* low, void* hgh, bit* li, bit* hi, bit* anti)\
{\
	while(task->start < task->stop ){\
		switch(MOSgetTag(task->blk)){\
		case MOSAIC_RLE:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_runlength\n");\
			DO_OPERATION_IF_ALLOWED(select, runlength, TPE);\
			break;\
		case MOSAIC_CAPPED:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_capped\n");\
			DO_OPERATION_IF_ALLOWED(select, capped, TPE);\
			break;\
		case MOSAIC_VAR:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_var\n");\
			DO_OPERATION_IF_ALLOWED(select, var, TPE);\
			break;\
		case MOSAIC_FRAME:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_frame\n");\
			DO_OPERATION_IF_ALLOWED(select, frame, TPE);\
			break;\
		case MOSAIC_DELTA:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_delta\n");\
			DO_OPERATION_IF_ALLOWED(select, delta, TPE);\
			break;\
		case MOSAIC_PREFIX:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_prefix\n");\
			DO_OPERATION_IF_ALLOWED(select, prefix, TPE);\
			break;\
		case MOSAIC_LINEAR:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_linear\n");\
			DO_OPERATION_IF_ALLOWED(select, linear, TPE);\
			break;\
		case MOSAIC_RAW:\
			ALGODEBUG mnstr_printf(GDKstdout, "MOSselect_raw\n");\
			DO_OPERATION_IF_ALLOWED(select, raw, TPE);\
			break;\
		}\
\
		if (task->ci->next == task->ci->ncand) {\
			/* We are at the end of the candidate list.
			 * So we can stop now.
			 */\
			return MAL_SUCCEED;\
		}\
	}\
\
	assert(task->blk == NULL);\
\
	return MAL_SUCCEED;\
}

#endif /* _MOSAIC_SELECT_ */
