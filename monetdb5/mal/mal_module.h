#ifndef _MAL_SCOPE_H_
#define _MAL_SCOPE_H_
#include "mal_box.h"
#include "mal_xml.h"

/* #define MAL_SCOPE_DEBUG  */

#define MAXSCOPE 256

typedef struct SCOPEDEF {
	struct SCOPEDEF   *outer; /* outer level in the scope tree */
	struct SCOPEDEF   *sibling; /* module with same start */
	str	    name;			/* index in namespace */
	int		inheritance; 	/* set when it plays a role in inheritance */
	Symbol *subscope; 		/* type dispatcher table */
	Box box;    			/* module related objects */
	int isAtomModule; 		/* atom module definition ? */
	void *dll;				/* dlopen handle */
	str help;   			/* short description of module functionality*/
} *Module, ModuleRecord;


mal_export void     setModuleJump(str nme, Module cur);
mal_export Module   newModule(Module scope, str nme);
mal_export Module   fixModule(Module scope, str nme);
mal_export void		deriveModule(Module scope, str nme);
mal_export void     freeModule(Module cur);
mal_export void     freeModuleList(Module cur);
mal_export void     insertSymbol(Module scope, Symbol prg);
mal_export void     deleteSymbol(Module scope, Symbol prg);
mal_export void		setInheritanceMode(Module head,int flag);
mal_export Module	setInheritance(Module head,Module first, Module second);
mal_export Module   findModule(Module scope, str name);
mal_export Symbol   findSymbol(Module nspace, str mod, str fcn);
mal_export int 		isModuleDefined(Module scope, str name);
mal_export Symbol   findSymbolInModule(Module v, str fcn);
mal_export int		findInstruction(Module scope, MalBlkPtr mb, InstrPtr pci);
mal_export int      displayModule(stream *f, Module v, str fcn,int listing);
mal_export void     showModules(stream *f, Module v);
mal_export void     debugModule(stream *f, Module v, str nme);
mal_export void     dumpManual(stream *f, Module v, int recursive);
mal_export void     dumpManualSection(stream *f, Module v);
mal_export void 	dumpManualHelp(stream *f, Module s, int recursive);
mal_export void 	dumpHelpTable(stream *f, Module s, str text, int flag);
mal_export void 	dumpSearchTable(stream *f, str text);
mal_export void     dumpManualOverview(stream *f, Module v, int recursive);
mal_export void     dumpManualHeader(stream *f);
mal_export void     dumpManualFooter(stream *f);
mal_export void     showModuleStatistics(stream *f,Module s); /* used in src/mal/mal_debugger.c */
mal_export char **getHelp(Module m, str pat, int flag);
mal_export char **getHelpMatch(char *pat);
mal_export void showHelp(Module m, str txt,stream *fs);

#define getSubScope(N)  (*(N))

#endif /* _MAL_SCOPE_H_ */
