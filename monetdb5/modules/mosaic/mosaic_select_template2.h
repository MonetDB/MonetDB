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
static inline void CONCAT6(select_general_, NAME, _, TPE, _, CAND_ITER)(
	const bool has_nil, const bool anti,
	MOStask* task, BUN first, BUN last,
	oid **po,
	TPE tl, TPE th, bool li, bool hi) {
	(void) first;

	oid* o = *po;

	TPE v;
	if		( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && li && hi && !anti) {
		if(has_nil) {
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, IS_NIL(TPE,v)); 
		}
		/*else*/
			/*Empty result set.*/
	}
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && li && hi && anti) {
		if(has_nil) {
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, !IS_NIL(TPE,v)); 
		}
		else CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, true);
	}
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && !(li && hi) && !anti) {
		if(has_nil) {
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, !IS_NIL(TPE,v)); 
		}
		else CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, true);
	}
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && !(li && hi) && anti) {
			/*Empty result set.*/
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl == th && !(li && hi) && anti) {
		if(has_nil) {
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, !IS_NIL(TPE,v)); 
		}
		else CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, true);
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl == th && !(li && hi) && !anti) {
		/*Empty result set.*/
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl > th && !anti) {
		/*Empty result set.*/
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl > th && anti) {
		if(has_nil) {
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, !IS_NIL(TPE,v)); 
		}
		else CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER, true);
	}
	else {
		/*normal cases.*/
		if( IS_NIL(TPE, tl) ){
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER,
				!(has_nil && IS_NIL(TPE, v)) &&
				(((hi && v <= th ) || (!hi && v < th )) == !anti));
		} else
		if( IS_NIL(TPE, th) ){
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER,
				!(has_nil && IS_NIL(TPE, v)) &&
				(((li && v >= tl ) || (!li && v > tl )) == !anti));
		} else
		if (tl == th){
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER,
				!(has_nil && IS_NIL(TPE, v)) && 
				((hi && v == th)  == !anti));
		}
		else{
			CONCAT2(scan_loop_, NAME)(TPE, CAND_ITER,
				!(has_nil && IS_NIL(TPE, v)) && 
				((((hi && v <= th ) || (!hi && v < th )) &&
				((li && v >= tl ) || (!li && v > tl )))  == !anti));
		}
	}

	*po = o;
}
