@f monet_utils
@a N.J. Nes
@* utils 
@T
The monet utils contains all general functionality needed by both clients and
server. 

@h
#ifndef _MU_H_
#define _MU_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef _MSC_VER
#ifndef LIBOPT
#define mutils_export extern __declspec(dllimport) 
#else
#define mutils_export extern __declspec(dllexport) 
#endif
#else
#define mutils_export extern 
#endif

#ifdef NATIVE_WIN32
#define DIR_SEP '\\' 
#else
#define DIR_SEP '/' 
#endif

#endif /* _MU_H_ */
