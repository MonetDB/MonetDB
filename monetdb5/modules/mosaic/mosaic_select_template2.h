#define _TEST_ALWAYS_TRUE	true
#define _TEST_IS_NIL		IS_NIL(TPE,v)
#define _TEST_IS_NOT_NIL	!IS_NIL(TPE,v)
#define _TEST_UPPER_BOUND	!(has_nil && IS_NIL(TPE, v)) && (((hi && v <= th ) || (!hi && v < th )) == !anti)
#define _TEST_LOWER_BOUND	!(has_nil && IS_NIL(TPE, v)) && (((li && v >= tl ) || (!li && v > tl )) == !anti)
#define _TEST_EQUAL			!(has_nil && IS_NIL(TPE, v)) && ((hi && v == th)  == !anti)
#define _TEST_RANGE			!(has_nil && IS_NIL(TPE, v)) && ((((hi && v <= th ) || (!hi && v < th )) && ((li && v >= tl ) || (!li && v > tl )))  == !anti)

#define METHOD_SPECIFIC_INCLUDE STRINGIFY(CONCAT3(mosaic_, NAME, _templates.h))

#define TEST TEST_ALWAYS_TRUE
#include METHOD_SPECIFIC_INCLUDE
#undef TEST
#define TEST TEST_IS_NIL
#include METHOD_SPECIFIC_INCLUDE
#undef TEST
#define TEST TEST_IS_NOT_NIL
#include METHOD_SPECIFIC_INCLUDE
#undef TEST
#define TEST TEST_UPPER_BOUND
#include METHOD_SPECIFIC_INCLUDE
#undef TEST
#define TEST TEST_LOWER_BOUND
#include METHOD_SPECIFIC_INCLUDE
#undef TEST
#define TEST TEST_EQUAL
#include METHOD_SPECIFIC_INCLUDE
#undef TEST
#define TEST TEST_RANGE
#include METHOD_SPECIFIC_INCLUDE
#undef TEST


#define SCAN_LOOP(TEST) CONCAT2(CONCAT4(scan_loop_, NAME, _, TPE), CONCAT4(_, CAND_ITER, _, TEST))(has_nil, anti, task, first, last, tl, th, li, hi)
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
	TPE tl, TPE th, bool li, bool hi) {
	(void) first;

	if		( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && li && hi && !anti) {
		if(has_nil) {
			SCAN_LOOP(TEST_IS_NIL); 
		}
		/*else*/
			/*Empty result set.*/
	}
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && li && hi && anti) {
		if(has_nil) {
			SCAN_LOOP(TEST_IS_NOT_NIL);
		}
		else SCAN_LOOP(TEST_ALWAYS_TRUE);
	}
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && !(li && hi) && !anti) {
		if(has_nil) {
			SCAN_LOOP(TEST_IS_NOT_NIL); 
		}
		else SCAN_LOOP(TEST_ALWAYS_TRUE);
	}
	else if	( IS_NIL(TPE, tl) &&  IS_NIL(TPE, th) && !(li && hi) && anti) {
			/*Empty result set.*/
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl == th && !(li && hi) && anti) {
		if(has_nil) {
			SCAN_LOOP(TEST_IS_NOT_NIL); 
		}
		else SCAN_LOOP(TEST_ALWAYS_TRUE);
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl == th && !(li && hi) && !anti) {
		/*Empty result set.*/
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl > th && !anti) {
		/*Empty result set.*/
	}
	else if	( !IS_NIL(TPE, tl) &&  !IS_NIL(TPE, th) && tl > th && anti) {
		if(has_nil) {
			SCAN_LOOP(TEST_IS_NOT_NIL); 
		}
		else SCAN_LOOP(TEST_ALWAYS_TRUE);
	}
	else {
		/*normal cases.*/
		if( IS_NIL(TPE, tl) ){
			SCAN_LOOP(TEST_UPPER_BOUND);
		} else
		if( IS_NIL(TPE, th) ){
			SCAN_LOOP(TEST_LOWER_BOUND);
		} else
		if (tl == th){
			SCAN_LOOP(TEST_EQUAL);
		}
		else{
			SCAN_LOOP(TEST_RANGE);
		}
	}
}
