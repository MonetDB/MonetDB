#ifndef _SQL_ARRAYS_H
#define _SQL_ARRAYS_H

#include <sql_mem.h>
#include <gdk_arrays.h>

#ifdef WIN32
#ifndef LIBSQL
#define sql5_export extern __declspec(dllimport)
#else
#define sql5_export extern __declspec(dllexport)
#endif
#else
#define sql5_export extern
#endif

#include "mal_backend.h"
#include "sql_mvc.h"
#include <sql_backend.h>
#include <mal_session.h>

#include <mal_function.h>
#include <mal_stack.h>
#include <mal_interpreter.h>

sql5_export str mvc_bind_array_column(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str mvc_bind_array_dimension(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

//sql5_export str mvc_get_cells(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
//sql5_export str mvc_dimension_subselect_with_cand_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
//sql5_export str mvc_dimension_subselect_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
//sql5_export str mvc_create_dimension_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
//sql5_export str materialiseDimension(bat* res, bat* in);
//sql5_export str mvc_create_cells_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

//BAT* mvc_fill_values(sql_column *c, BAT *b_in, unsigned int cellsNum, void* defVal);

#endif /* _SQL_ARRAYS_H */
