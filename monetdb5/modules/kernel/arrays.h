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

algebra_export str ALGdimensionLeftfetchjoin1(bat* result, const ptr *dimsCands, const bat* oidsCands, const ptr *dim, const ptr *dims) ;
algebra_export str ALGdimensionLeftfetchjoin2(bat* result, const ptr* dimsCands, const bat* oidsCands, const ptr *array);
algebra_export str ALGnonDimensionLeftfetchjoin1(bat* result, const ptr *dimsCands, const bat* oidsCands, const bat *vals, const ptr *dims);
algebra_export str ALGnonDimensionLeftfetchjoin2(bat* result, ptr* dimsRes, const ptr *array, const bat *vals, const ptr *dims);

algebra_export str ALGdimensionSubselect2(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, const ptr *dimsCands, const bat* oidsCands,
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGdimensionSubselect1(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGdimensionSubselect3(ptr *dimsRes, bat *oidsResi, const ptr *array, const bat *vals,
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);

algebra_export str ALGdimensionThetasubselect2(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, const ptr *dimsCand, const bat *oidsCands,
											const void *val, const char **op);
algebra_export str ALGdimensionThetasubselect1(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, 
											const void *val, const char **op);

algebra_export str ALGnonDimensionSubselect1(ptr *dimsRes, bat *oidsRes, const bat *values, const ptr *dims, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGnonDimensionSubselect2(ptr *dimsRes, bat *oidsRes, const bat* values, const ptr *dims, const ptr *dimsCands, const bat* oidsCands, 
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);

algebra_export str ALGnonDimensionThetasubselect2(ptr *dimsRes, bat *oidsRes, const bat* vals, const ptr *dims, const ptr *dimsCands, const bat* oidsCands, 
			                            const void *val, const char **op);
algebra_export str ALGnonDimensionThetasubselect1(ptr *dimsRes, bat *oidsRes, const bat* vals, const ptr *dims, 
			                            const void *val, const char **op);

algebra_export str ALGprojectDimension(bat* result, const ptr *dim, const ptr *array);
algebra_export str ALGprojectNonDimension(bat *result, const bat *vals, const ptr *array);




algebra_export str ALGarrayCount(wrd *res, const ptr *array);
//algebra_export str ALGproject(bat *result, const ptr* candDims, const bat* candBAT);
#endif /* _ARRAYS_H */
