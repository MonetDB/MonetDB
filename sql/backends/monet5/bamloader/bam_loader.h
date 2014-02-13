#ifndef _BAM_LOADER_H
#define _BAM_LOADER_H

#include "sql_scenario.h"
#include "sql_mvc.h"
#include "bam_globals.h"


#ifdef WIN32
#ifndef LIBBAM
#define bam_export extern __declspec(dllimport)
#else
#define bam_export extern __declspec(dllexport)
#endif
#else
#define bam_export extern
#endif

#define LOGPATH MONETDB_HOME"/bam_loader.log"

#define HEADER_STORAGE_LIMIT 10000
#define ALIGNMENTS_STORAGE_LIMIT 10000000
#define ALIGNMENTS_EXTRA_STORAGE_LIMIT 100000000
#define DIR_FLUSH ROOT"/bam_loader_flush"

#define MAX_NR_BAM_FILES 32
#define MAX_BAM_FILEPATH_LENGTH 256
#define MAX_HEADER_COMMENT_LENGTH 16384


bam_export str bam_loader_repos(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_loader_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
bam_export str bam_drop_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
