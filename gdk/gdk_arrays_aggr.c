#include "monetdb_config.h"
#include "gdk_arrays.h"


/*the groupsBAT has been created using a dimensionBAT and is thus a dimension BAT*/
gdk_return dimensionBATgroupavg(BAT **outBAT, BAT **cntsp, BAT *valuesBAT, BAT *groupsBAT, BAT *extentsBAT, BAT *s, int tp, int skip_nils, int abort_on_error) {
	dbl *restrict dbls;
	BAT *resBAT;
	oid minGroupId, maxGroupId;
	BUN groupsNum;
	const char* err;

	/*used for candidates (I do not handle this case at the moment*/
	BUN start,end, cnt;
	const oid *cand, *candend;

	if(cntsp)
		return gdk_error_msg(general_error, "dimensionBATgroupavg", "cntsp is not NULL");

	/* initialise the minimum and maximum group is and the number of groups
 	 * and the variables for the candidates selection */
	if ((err = BATgroupaggrinit(valuesBAT, groupsBAT, extentsBAT, s, &minGroupId, &maxGroupId, &groupsNum,
					&start, &end, &cnt, &cand, &candend)) != NULL) {
        return gdk_error_msg(general_error, "dimensionBATgroupavg", err);
    }

	resBAT = BATnew(TYPE_void, TYPE_dbl, groupsNum, TRANSIENT);
	if (resBAT == NULL)
		return GDK_FAIL;
	dbls = (dbl *) Tloc(resBAT, BUNfirst(resBAT));

(void)skip_nils;
(void)abort_on_error;
(void)start;
(void)end;
(void)cnt;
(void)*cand;
(void)*candend;

(void) tp;      /* compatibility (with other BATgroup*
                 * functions) argument */

#define dimensionAggrAvg(TPE) \
    do {\
        oid minD, maxD, stepD; \
        long elR, grpR, el, grp; \
        BUN elsNum, aggrGrp; \
        const TPE *restrict vals = (const TPE *) Tloc(valuesBAT, BUNfirst(valuesBAT)); \
        double elsPerAggrGrp =0 ; \
\
        dimensionCharacteristics(oid, groupsBAT, &minD, &maxD, &stepD, &elR, &grpR); \
        elsNum = dimensionElementsNum(minD, maxD, stepD); \
        elsPerAggrGrp = elR*grpR; \
\
        for(aggrGrp=0; aggrGrp<elsNum; aggrGrp++) { \
            oid start = aggrGrp*elR;\
fprintf(stderr, "%u: %u\n", (unsigned int)aggrGrp, (unsigned int)start); \
            dbls[aggrGrp] = 0; /*initialise to 0*/\
            for(grp=0; grp<grpR; grp++) { \
                for(el=0; el<elR; el++) { \
                    dbls[aggrGrp]+=vals[start+el]; \
fprintf(stderr, "avg: group %u - (%u,%f) => %f\n", (unsigned int)aggrGrp, (unsigned int)(start+el), (double)vals[start+el], dbls[aggrGrp]); \
                }\
                start+=elsNum*elR; \
            } \
            dbls[aggrGrp]/=elsPerAggrGrp; \
        } \
    } while(0)

     switch(ATOMtype(BATttype(valuesBAT))) {
        case TYPE_bte:
            dimensionAggrAvg(bte);
			break;
        case TYPE_sht:
            dimensionAggrAvg(sht);
			break;
        case TYPE_int:
            dimensionAggrAvg(int);
			break;
        case TYPE_flt:
            dimensionAggrAvg(flt);
			break;
        case TYPE_dbl:
            dimensionAggrAvg(dbl);
			break;
        case TYPE_lng:
            dimensionAggrAvg(lng);
			break;
#ifdef HAVE_HGE
        case TYPE_hge:
            dimensionAggrAvg(hge);
			break;
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            dimensionAggrAvg(int);
#else
            dimensionAggrAvg(lng);
#endif
        	break;
        default:
            return gdk_error_msg(value_type, "dimensionBATgroupavg", NULL);
    }

	BATsetcount(resBAT, groupsNum);
	BATseqbase(resBAT, 0);
	BATderiveProps(resBAT, FALSE);
	*outBAT = resBAT;

    return GDK_SUCCEED;
}

