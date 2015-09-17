#ifndef _CALC_ARRAYS_H
#define _CALC_ARRAYS_H

#ifdef WIN32
#define batcalc_export extern __declspec(dllexport)
#else
#define batcalc_export extern
#endif


str CMDconvertDimension_lng(ptr *dimConverted, ptr *dims_out, const ptr *dim, const ptr *dims);


#endif /*_CALC_ARRAYS_H*/
