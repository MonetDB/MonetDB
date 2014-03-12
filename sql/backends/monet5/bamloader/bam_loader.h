#ifndef _BAM_LOADER_H
#define _BAM_LOADER_H

#include "sql_scenario.h"
#include "sql_mvc.h"

#ifdef WIN32
#ifndef LIBBAM
#define bam_export extern __declspec(dllimport)
#else
#define bam_export extern __declspec(dllexport)
#endif
#else
#define bam_export extern
#endif

bam_export str bam_loader_repos(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_loader_files(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_loader_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_drop_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
/* TODO Consider drop on file_location/repos bases */


#endif
