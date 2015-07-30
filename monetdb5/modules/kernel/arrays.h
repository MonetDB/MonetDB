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

algebra_export str ALGdimensionLeftfetchjoin1(bat* result, const bat* cands, const ptr *dims, const ptr *dim) ;
algebra_export str ALGdimensionLeftfetchjoin2(bat* result, const ptr* dimsCand, const ptr *dims, const ptr *dim) ;
algebra_export str ALGnonDimensionLeftfetchjoin(bat* result, const ptr* dimsCand, const bat *candBat, const bat *valsBat);
//algebra_export str ALGnonDimensionLeftfetchjoin(bat *result, const bat *lid, const bat *rid);
//algebra_export str ALGdimensionLeftfetchjoin(bat *result, const bat *lid, const bat *rid);

algebra_export str ALGdimensionSubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, const ptr *dimsCand, const bat* oidsCand,
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGdimensionSubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGdimensionThetasubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, const ptr *dimsCand, const bat* oidsCand, const void *val, const char **op);
algebra_export str ALGdimensionThetasubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, const void *val, const char **op);


algebra_export str ALGnonDimensionSubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const bat* values, const ptr *dimsCand, const bat* oidsCand, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGnonDimensionSubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const bat* values, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);

algebra_export str ALGmbrsubselect(bat *result, const ptr *dims, const ptr* dim, const bat *sid, const bat *cid);
algebra_export str ALGmbrsubselect2(bat *result, const ptr *dims, const ptr* dim, const bat *sid);

algebra_export str ALGmbrproject(bat *result, const bat *bid, const bat *sid, const bat *rid);

algebra_export str ALGproject(bat *result, const ptr* candDims, const bat* candBAT);
#endif /* _ARRAYS_H */
