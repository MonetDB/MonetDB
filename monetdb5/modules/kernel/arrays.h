#ifndef _ARRAYS_H
#define _ARRAYS_H


#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define algebra_export extern __declspec(dllimport)
#else
#define algebra_export extern __declspec(dllexport)
#endif
#else
#define algebra_export extern
#endif

//algebra_export str ALGdimensionLeftfetchjoin1(bat* result, const bat* cands, const ptr *dim, const ptr *dims) ;
//algebra_export str ALGnonDimensionLeftfetchjoin1(bat* result, const bat* cands, const bat *vals, const ptr *dims);
algebra_export str ALGdimensionLeftfetchjoin1(bat* result, const ptr *dimsCands, const ptr *dim, const ptr *dims) ;
algebra_export str ALGnonDimensionLeftfetchjoin1(bat* result, const ptr *dimsCands, const bat *batCands, const bat *vals, const ptr *dims);
algebra_export str ALGnonDimensionLeftfetchjoin2(bat* result, ptr* dimsRes, const bat *tids, const bat *vals, const ptr *dims);

algebra_export str ALGdimensionSubselect2(ptr *dimsRes, const ptr *dim, const ptr* dims, const ptr *dimsCand,
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGdimensionSubselect1(ptr *dimsRes, const ptr *dim, const ptr* dims, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);

algebra_export str ALGdimensionThetasubselect2(ptr *dimsRes, const ptr *dim, const ptr* dims, const ptr *dimsCand, const void *val, const char **op);
algebra_export str ALGdimensionThetasubselect1(ptr *dimsRes, const ptr *dim, const ptr* dims, const void *val, const char **op);


algebra_export str ALGnonDimensionSubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const bat* values, const ptr *dimsCand, const bat* oidsCand, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGnonDimensionSubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const bat* values, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);

#if 0
algebra_export str ALGmbrsubselect(bat *result, const ptr *dims, const ptr* dim, const bat *sid, const bat *cid);
algebra_export str ALGmbrsubselect2(bat *result, const ptr *dims, const ptr* dim, const bat *sid);

algebra_export str ALGmbrproject(bat *result, const bat *bid, const bat *sid, const bat *rid);
#endif
algebra_export str ALGproject(bat *result, const ptr* candDims, const bat* candBAT);
#endif /* _ARRAYS_H */
