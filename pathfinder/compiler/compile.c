/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Compiler driver for the Pathfinder compiler
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */



#include "pf_config.h"
#include "pathfinder.h"


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#if HAVE_SIGNAL_H
#include <signal.h>
#endif

#include "pf_xq.h"        /* Global XQuery information (PFquery_t) */
#include "compile.h"
#include "compile_interface.h"
#include "parser.h"       /* parsing XQuery syntax */
#include "abssyn.h"
#include "varscope.h"     /* variable scoping */
#include "normalize.h"    /* parse tree normalization */
#include "heuristic.h"    /* selection pushdown for MonetDB */
#include "qname.h"        /* Pathfinder's QName handling */
#include "ns.h"           /* namespaces */
#include "nsres.h"        /* namespace resolution */
#include "options.h"      /* extract option declarations from parse tree */
#include "xquery_fo.h"    /* built-in XQuery F&O */
#include "func_chk.h"     /* function resolution */
#include "abssynprint.h"
#include "coreprint.h"
#include "logdebug.h"
#include "timer.h"
#include "fs.h"           /* core mapping (formal semantics) */
#include "types.h"        /* type system */
#include "import.h"       /* XML Schema import */
#include "simplify.h"     /* core simplification */
#include "core.h"
#include "coreopt.h"
#include "typecheck.h"    /* type inference and check */
#include "core2alg.h"     /* Compile Core to Relational Algebra */
#include "algopt.h"
#include "algebra_cse.h"
#include "planner.h"
#include "physdebug.h"
#include "intro_borders.h"
#include "milgen.h"       /* MIL command tree generation */
#include "mil_dce.h"      /* dead MIL code elimination */
#include "milprint.h"     /* create string representation of MIL tree */
#include "milprint_summer.h" /* create MILcode directly from the Core tree */
#include "oops.h"
#include "mem.h"
#include "coreopt.h"
#include "lalg2sql.h"
#include "sql_opt.h"
#include "sqlprint.h"
#include "lalg2msa.h"
#include "msaprint.h"
#include "load_stats.h"  /* to create the guide list */
#include "alg_cl_mnemonic.h"

#include "xml2lalg.h" /* xml importer */

#ifndef NDEBUG
/* to invoke PFty_debug_subtyping() if --debug subtyping */
#include "subtyping.h"
#endif

/* include libxml2 library to parse module definitions from an URI */
#include "libxml/xmlIO.h"

/*
extern PFarray_t *modules;
*/

static char *phases[] = {
    [ 1]  = "right after input parsing",
    [ 2]  = "after loading XQuery modules",
    [ 3]  = "after parse/abstract syntax tree has been normalized",
    [ 4]  = "after namespaces have been checked and resolved",
    [ 5]  = "after option declarations have been extracted from the parse tree",
    [ 6]  = "after guide tree has been build",
    [ 7]  = "after variable scoping has been checked",
    [ 8]  = "after XQuery built-in functions have been loaded",
    [ 9]  = "after XML Schema predefined types have been loaded",
    [10]  = "after valid function usage has been checked",
    [11]  = "after XML Schema document has been imported (if any)",
    [12]  = "after the abstract syntax tree has been mapped to Core",
    [13]  = "after the Core tree has been simplified/normalized",
    [14]  = "after type inference and checking",
    [15]  = "after XQuery Core optimization",
    [16]  = "after the Core tree has been translated to the logical algebra",
    [17]  = "after the logical algebra tree has been rewritten/optimized",
    [18]  = "after the CSE on the logical algebra tree",
    [19]  = "after compiling logical algebra into the physical algebra (or SQL)",
    [20]  = "after introducing boundary operators",
    [21]  = "after compiling the physical algebra into MIL code",
    [22]  = "after the MIL program has been serialized"
};

/** initialize global state of the compiler */
void
PFstate_init (PFstate_t *status)
{
    status->quiet               = false;
    status->timing              = false;
    status->print_dot           = false;
    status->print_xml           = false;
    status->import_xml          = false;
    status->import_xml_filename = NULL;
    status->print_pretty        = false;
    status->stop_after          = 0;
    status->print_types         = false;
    status->optimize            = 1;
    status->print_parse_tree    = false;
    status->print_core_tree     = false;
    status->print_la_tree       = false;
    status->print_pa_tree       = false;
    status->output_format       = PFoutput_format_not_specified;
    status->dead_code_el        = true;

    status->standoff_axis_steps = false;

    status->opt_alg             = "OIKCG_VG_JIS_I_GECSVR_OIK_N"
                                       "}MT{JISAI_GYECSVR_"
                                       "}MT{JISAI_OIK_GYECSVR_"
                                       "DC_GP";
    status->opt_sql             = "OIKCG_VG_JIS_I_GECSVR_OIK_N"
                                    "QU_}MT{JISAI_GYECSVR_"
                                    "QU_}MT{JISAI_OIK_GYECSVR_"
                                    "QU_CGP";

    status->format              = NULL;
    status->genType             = "xml";

#ifndef NDEBUG
    status->debug.subtyping     = false;
#endif
}

jmp_buf PFexitPoint;

/** Compilation stage we've last been in. */
unsigned int last_stage = 0;

#define STOP_POINT(a) \
    last_stage = (a); \
    if ((a) == status->stop_after) \
        goto bailout;

void segfault_handler (int sig);

typedef struct _urlcache_t {
    struct _urlcache_t *next;
    xmlParserInputBufferPtr  buf;
    char url[1];
} urlcache_t;

/* NOTE: variable is re-used in subsequent pf runs (in server-side setup) */
urlcache_t *urlcache = NULL;

/**
 * read input file into the url cache
 */
char *PFurlcache (char *uri, int keep)
{
    int         num_read = -1;
    urlcache_t *cache = urlcache;
    xmlParserInputBufferPtr  buf;
    char *ret, url[1024];

    /* support for the xrpc://x.y.z/URI naming scheme (maps to http://x.y.z:<xrpc_port>/xrpc/URI) */
    if (strncmp(uri, "xrpc://", 7) == 0) {
        char *p = strchr(uri+7, '/');
        char *q = strchr(uri+7, ':');
        int port = 80; /* if a specific port is omitted, we use the current xrpcd port number */
        if (q) {
            if (p == NULL || p > q) port = atoi(q+1);  /* it is a port only if ':' before the first '/' */
            *q = 0;
        }
        if (p) *p = 0;
        snprintf(url, sizeof(url), "http://%s:%d/xrpc/%s", uri+7, port, p?(p+1):"");
        if (p) *p = '/';
        if (q) *q = ':';
    } else {
        strncpy(url, uri, sizeof(url));
    }
    url[sizeof(url) - 1] = 0;   /* neither strncpy nor (on Windwos) snprintf guarantee NULL-termination */

    if (keep) {
        int len = strlen(url);
        /*
         * URI that ends in .mil is converted into a .xq
         */
        if (len > 4 && url[len-4] == '.' && url[len-3] == 'm' && url[len-2] == 'i' && url[len-1] == 'l') {
            url[len-3] = 'x'; url[len-2] = 'q'; url[len-1] = 0;
        }
    }

    /*
     * first try the cache
     */
    while(cache) {
        if (strcmp(cache->url, url) == 0)
            return (char*) cache->buf->buffer->content;
        cache = cache->next;
    }


    /*
     * open file via parser from the libxml2 library
     * (capable of loading URIs also via HTTP or FTP)
     */
    buf = xmlParserInputBufferCreateFilename (url, XML_CHAR_ENCODING_NONE);
    if (buf) {
        /*
         * Load in chunks of 2048 chars.  libxml2 does not really
         * stick to these 2048 chars, but we don't care anyway...
         */
        while ((num_read = xmlParserInputBufferGrow (buf, 2048)))
            /* empty */;
    }
    if (num_read < 0) {
        if (buf) free(buf);
        return NULL;
    }
    ret = (char*) buf->buffer->content;
    if (keep) {
        /* cache the new url */
        cache = (urlcache_t*) malloc(sizeof(urlcache_t)+strlen(url));
        if (cache) {
            strcpy(cache->url, url);
            cache->buf = buf;
            cache->next = urlcache;
            urlcache = cache;
        }
    } else {
        buf->buffer->content = NULL; /* otherwise it will be deleted */
        xmlFreeParserInputBuffer (buf);
    }
    return ret;
}

/**
 * flush url cache
 */
void PFurlcache_flush(void) {
    urlcache_t *cache = urlcache;
    while(cache) {
        urlcache_t *del = cache;
        cache = cache->next;

        /* free resource allocated by libxml2 file reader */
        xmlFreeParserInputBuffer (del->buf);
        free(del);
    }
    urlcache = NULL;
}


/**
 * Compiler driver of the Pathfinder compiler,
 * It invokes the pipeline of query processing steps
 * (starting with the parser).
 */
int
PFcompile (char *url, FILE *pfout, PFstate_t *status)
{
    PFpnode_t      *proot  = NULL;
    PFcnode_t      *croot  = NULL;
    /* We either have a plan bundle in @a lapb
       or a logical query plan in @a laroot. */
    PFla_pb_t      *lapb   = NULL;
    PFla_op_t      *laroot = NULL;
    PFpa_op_t      *paroot = NULL;
    PFmil_t        *mroot  = NULL;
    PFchar_array_t *mil_program = NULL;
    char           *xquery = NULL;
    int             module_base;

    /* elapsed time for compiler phase */
    long tm, tm_first = 0;

    PFguide_list_t *guide_list = NULL; /* guide list */

    PFquery_t PFquery = {
        .version             = "1.0",
        .encoding            = NULL,
        .ordering            = true,  /* implementation defined: ordered */
        .empty_order         = undefined,
        .inherit_ns          = false, /* implementation def'd: inherit-ns: no */
        .pres_boundary_space = false,
        .revalid             = revalid_strict /* XQ update facility 1.0 [§2.3] */
    };

    /* initialize data structures in the Parser department */
    PFparser_init ();
    /* Initialize data structures in the Core department */
    PFcore_init ();
    /* Initialize data structures in the Functions department */
    PFfunctions_init ();
    /* Initialize data structures in the Options department */
    PFoptions_init ();
    /* Initialize data structures in the Varscope department */
    PFvarscope_init ();  

#if HAVE_SIGNAL_H
    /* setup sementation fault signal handler */
    signal (SIGSEGV, segfault_handler);
#endif

#ifndef NDEBUG
    PFarray_init ();
#endif

    /*******************************************/
    /* Split Point: Logical Algebra XML Import */
    /*******************************************/
    if (status->import_xml) {

        tm_first = tm = PFtimer_start ();

        /* todo: init-stuff */
        /* e.g. qnameInit */
        /* e.g namespaceInit */

        XML2LALGContext *ctx = PFxml2la_xml2lalgContext();

        /* Initialize data structures in the Namespace department */
        PFns_init ();

        /* Initialize data structures in the QName department */
        PFqname_init ();

        /* Initialize the column name counter */
        PFalg_init ();

        if (status->import_xml_filename) {
            /**
             * If the input is explicitely specified with an filename,
             * we drive the xml-importer with this filename instead with the
             * url-cache-buffer.
             * Reason: If the validation of the input-file against a schema
             * fails, we explicitely get the locations in the file
             * (linenumbers) from the xml-parser. But only, if the parser is
             * driven with an explicit filename. If the parser is driven
             * with an buffer, then we don't get explicit error-positions
             * from the parser in case of validation errors (which makes
             * fixing/locating this validation-errors unneccessary hard)
             */
            lapb = PFxml2la_importXMLFromFile (ctx,
                                               status->import_xml_filename);
        }
        else {
            /**
             * Note, that we don't get explicit error-positions (line
             * numbers of the xml-input) from the parser in case of
             * validation errors...
             *
             * Call PF from the command line with an explicit filename
             * (instead of using stdin/pipelining) if you want to get
             * explicit error positions (line numbers) from the xml-parser
             * in case of validation-errors
             */
            char *xml  = PFurlcache (url, 1);
            int   size = strlen(xml);
            lapb = PFxml2la_importXMLFromMemory (ctx, xml, size);
        }

        /* detect whether we have a plan bundle or only a single plan */
        if (PFla_pb_size (lapb) == 1 && PFla_pb_id_at (lapb, 0) == -1) {
           laroot = PFla_pb_op_at (lapb, 0);
           lapb = NULL;
        }

        tm = PFtimer_stop (tm);

        if (status->timing)
            PFlog ("XML algebra plan import:\t\t %s", PFtimer_str (tm));

        goto AFTER_CORE2ALG;
    }

    /* compiler chain below
     */
    xquery = PFurlcache (url, 1);

    if (!xquery)
        PFoops (OOPS_FATAL, "unable to load URL `%s'", url);

    tm_first = tm = PFtimer_start ();
    (void) PFparse (xquery, &proot, &PFquery, status->standoff_axis_steps);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("parsing:\t\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(1);

    tm = PFtimer_start ();
    module_base = PFparse_modules (proot, &PFquery, status->standoff_axis_steps);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("module import:\t\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(2);

    tm = PFtimer_start ();

    /* Abstract syntax tree normalization
     */
    proot = PFnormalize_abssyn (proot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("normalization:\t\t\t\t %s", PFtimer_str (tm));

    /* Heuristic path-reversal rewrites to start evaluation
       with indexable expressions. */
    tm = PFtimer_start ();
    if (status->output_format == PFoutput_format_mil ||
        status->output_format == PFoutput_format_milprint_summer) {
        proot = PFheuristic_index (proot);
        tm = PFtimer_stop (tm);
        if (status->timing)
            PFlog ("path heuristics:\t\t\t %s", PFtimer_str (tm));
    }
    STOP_POINT(3);

    /* Initialize data structures in the Namespace department */
    PFns_init ();

    /* Initialize data structures in the QName department */
    PFqname_init ();

    /* Resolve NS usage */
    PFns_resolve (proot);

    /*
     * NOTE: Abstract syntax tree printing requires the information
     *       whether namespaces have been resolved already (rather,
     *       if abstract syntax tree nodes contain PFqname_t structs
     *       or PFqname_raw_t structs).  This information is passed
     *       further down at bailout, based on the constant 4 that
     *       you see here.  If you change the constant, don't forget
     *       to change the value further down.
     */
    STOP_POINT(4);

    /*
     * Extract option declarations from the abstract syntax tree
     * and read them into a mapping table.  This table may then be
     * consulted by further processing steps down the compilation
     * chain.
     */
    PFextract_options (proot);

    STOP_POINT(5);

    /* create guide list */
    guide_list =  PFguide_list_load ();

    STOP_POINT(6);

    /* Check variable scoping and replace QNames by PFvar_t pointers */
    PFvarscope (proot);

    STOP_POINT(7);

    /* Load built-in XQuery F&O into function environment */
    PFfun_xquery_fo ();

    STOP_POINT(8);

    /* Load XML Schema/XQuery predefined types into the type environment */
    PFty_predefined ();

    STOP_POINT(9);

    tm = PFtimer_start ();

    /* Resolve function usage
     */
    PFfun_check (proot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("semantical analysis:\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(10);

    tm = PFtimer_start ();

    /* XML Schema import */
    PFschema_import (proot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("XML Schema import:\t\t\t %s", PFtimer_str (tm));

#ifndef NDEBUG
    /*
     * All types are available now, and we can invoke PFty_debug_subtyping()
     * if requested.
     */
    if (status->debug.subtyping)
        PFty_debug_subtyping ();
#endif

    STOP_POINT(11);

    tm = PFtimer_start ();

    /* XQuery core mapping
     */
    croot = PFfs (proot, &PFquery);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("core mapping:\t\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(12);

    /* Core simplification */
    tm = PFtimer_start ();

    croot = PFsimplify (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("core simplification:\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(13);

    /* Type inference and check */
    tm = PFtimer_start ();

    croot = PFty_check (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("type checking:\t\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(14);

    /* Core tree optimization */
    tm = PFtimer_start ();

    croot = PFcoreopt (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("core tree optimization:\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(15);

    /*
     * generate temporary MIL Code (summer branch version)
     */

    if (status->output_format == PFoutput_format_milprint_summer) {
        char *prologue = NULL, *query = NULL, *epilogue = NULL;
        tm = PFtimer_start ();
        if (PFprintMILtemp (croot, status->optimize, module_base, -1, tm_first,
                            &prologue, &query, &epilogue, url, status->standoff_axis_steps))
            goto failure;
        fputs(prologue, pfout);
        fputs(query, pfout);
        /* epilogue is not needed for standalone MIL scripts */
        tm = PFtimer_stop (tm);
        if (status->timing)
            PFlog ("MIL code output:\t\t\t %s", PFtimer_str (tm));
        goto bailout;
    }

    /* Initialize the column name counter */
    PFalg_init ();

    /*
     * map core to algebra tree
     */
    tm = PFtimer_start ();

    laroot = PFcore2alg (croot, &PFquery,
                         status->output_format);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("logical algebra tree generation:\t %s", PFtimer_str (tm));

/*******************************************/
/* Merge point logical algebra XML import. */
/*******************************************/
AFTER_CORE2ALG:

    STOP_POINT(16);

    /*
     * Rewrite/optimize algebra tree
     */
    tm = PFtimer_start ();

    if (laroot)
        laroot = PFalgopt (laroot, status->timing, guide_list, status->opt_alg);
    else {
        /* we have a plan bundle */
        assert (lapb);
        for (unsigned int i = 0; i < PFla_pb_size(lapb); i++)
            PFla_pb_op_at (lapb, i) = PFalgopt (PFla_pb_op_at (lapb, i),
                                                status->timing,
                                                guide_list,
                                                status->opt_alg);
    }

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("logical algebra optimization:\t\t %s",
               PFtimer_str (tm));

    STOP_POINT(17);

    /*
     * common subexpression elimination in the algebra tree
     */
    tm = PFtimer_start ();

    if (laroot)
        laroot = PFla_cse (laroot);
    else {
        /* we have a plan bundle */
        assert (lapb);
        for (unsigned int i = 0; i < PFla_pb_size(lapb); i++)
            PFla_pb_op_at (lapb, i) = PFla_cse (PFla_pb_op_at (lapb, i));
    }

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("CSE in logical algebra tree:\t\t %s",
               PFtimer_str (tm));

    STOP_POINT(18);

    /************************************/
    /* Split Point: SQL Code Generation */
    /************************************/
    if (status->output_format == PFoutput_format_sql) {
        PFchar_array_t *sql_str = PFchar_array (4096);

        /**
         * Iterate over the plan_bundle list lapb and bind every item to laroot_.
         *
         * BEWARE: macro has to be used in combination with macro PFla_pb_end.
         */
        PFla_pb_foreach (sql_str, laroot_, lapb, laroot)



            /* plan bundle emits SQL code wrapped in XML tags */ 
            if (lapb)
                PFarray_printf (sql_str, 
                                "    <query>"
                                "<![CDATA[\n");

            /* generate the SQL code */
            tm = PFtimer_start ();
            PFsql_t *sqlroot = PFlalg2sql(laroot_);
            tm = PFtimer_stop (tm);
            if (status->timing)
                PFlog ("Compilation to SQL:\t\t\t %s", PFtimer_str (tm));

            STOP_POINT(19);

            if (status->dead_code_el) {
                /* optimize the SQL code */
                tm = PFtimer_start ();
                sqlroot = PFsql_opt (sqlroot);
                tm = PFtimer_stop (tm);
                if (status->timing)
                    PFlog ("SQL dead code elimination:\t\t %s", PFtimer_str (tm));
            }

            STOP_POINT(20);

            /* serialize the internal SQL query tree */
            tm = PFtimer_start ();
            PFsql_print (sql_str, sqlroot);
            tm = PFtimer_stop (tm);
            if (status->timing)
                PFlog ("SQL Code generation:\t\t\t %s", PFtimer_str (tm));

            /* plan bundle emits SQL code wrapped in XML tags */ 
            if (lapb)
                PFarray_printf (sql_str, 
                                "]]>"
                                "</query>\n");



        /**
         * Iterate over the plan_bundle list lapb and bind every item to laroot_.
         *
         * BEWARE: macro has to be used in combination with macro PFla_pb_foreach.
         */
        PFla_pb_foreach_end (sql_str, lapb)

        /* print the resulting string to the output stream */
        fputs (sql_str->base, pfout);

        goto bailout;
    }

    if (status->output_format == PFoutput_format_m5sql) {
        unsigned int i = 0;
        /* If we have a plan bundle we have to generate
           a M5 SQL algebra plan for each query plan. */
        do {
            if (lapb) {
                laroot = PFla_pb_op_at (lapb, i);
                assert (laroot->kind == la_serialize_rel);
                i++;
            }

            /* generate the M5 SQL algebra plan */
            tm = PFtimer_start ();
            PFmsa_op_t *msaroot = PFlalg2msa(laroot);
            tm = PFtimer_stop (tm);
            if (status->timing)
                PFlog ("Compilation to M5 SQL algebra:\t %s", PFtimer_str (tm));

            STOP_POINT(19);

            /* print the internal M5 SQL algebra plan */
            tm = PFtimer_start ();
            PFmsa_dot (pfout, msaroot);
            tm = PFtimer_stop (tm);
            if (status->timing)
                PFlog ("M5 SQL algebra debug printing:\t %s", PFtimer_str (tm));

        /* iterate over the plans in the plan bundle */
        } while (lapb && i < PFla_pb_size (lapb));

        goto bailout;
    }

    if (lapb)
        PFoops (OOPS_FATAL, "unable to generate MIL code for a plan bundle");

    /* Compile algebra into physical algebra */
    tm = PFtimer_start ();
    paroot = PFplan (laroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("compilation to physical algebra:\t %s", PFtimer_str (tm));

    STOP_POINT(19);

    /* Extend the physical algebra with recursion and branch
       boundaries whenever a recursion or branch occurs */
    tm = PFtimer_start ();
    paroot = PFpa_intro_borders (paroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("introduction of boundary operators:\t %s", PFtimer_str (tm));

    STOP_POINT(20);

    /* Map physical algebra to MIL */
    tm = PFtimer_start ();
    mroot = PFmilgen (paroot, status->genType);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("MIL code generation:\t\t\t %s", PFtimer_str (tm));

    mroot = PFmil_seq (PFmil_module (PFmil_lit_str ("pathfinder")),
                       PFmil_module (PFmil_lit_str ("mkey")),
#ifdef HAVE_GEOXML
                       PFmil_module (PFmil_lit_str ("geoxml")),
#endif

                       mroot);
    
    /* make runtime timing available */
    if (status->timing) {
        mroot = PFmil_seq (mroot,
                           PFmil_printf (
                               PFmil_arg (PFmil_lit_str ("\nShred  % 10.3f msec"
                                                         "\nQuery  % 10.3f msec"
                                                         "\nPrint  % 10.3f msec\n"),
                               PFmil_arg (PFmil_var (PF_MIL_VAR_TIME_LOAD),
                               PFmil_arg (PFmil_var (PF_MIL_VAR_TIME_QUERY),
                                          PFmil_var (PF_MIL_VAR_TIME_PRINT))))));
    }

    if (status->dead_code_el) {
        tm = PFtimer_start ();

        mroot = PFmil_dce (mroot);

        tm = PFtimer_stop (tm);
        if (status->timing)
            PFlog ("dead code elimination:\t\t\t %s", PFtimer_str (tm));
    }

    STOP_POINT(21);

    /* Render MIL program in Monet MIL syntax
     */
    tm = PFtimer_start ();
    mil_program = PFmil_serialize (mroot);
    tm = PFtimer_stop (tm);

    if (!mil_program)
        goto failure;

    if (status->timing)
        PFlog ("MIL code serialization:\t\t\t %s", PFtimer_str (tm));

    STOP_POINT(22);

    /* Print MIL program to pfout */
    if (mil_program)
        PFmilprint (pfout, mil_program);

 bailout:

    if (status->timing) {
        PFlog ("----------------------------------------------");
        tm_first = PFtimer_stop (tm_first);
        PFlog ("overall compilation time:\t\t %s", PFtimer_str (tm_first));
    }

    /* print abstract syntax tree if requested */
    if (status->print_parse_tree) {
        if (proot) {
            if (status->print_pretty) {
                if (! status->quiet)
                    printf ("Parse tree %s:\n", phases[status->stop_after]);
                /* last_stage >= 4   <==>  namespaces resolved already? */
                PFabssyn_pretty (pfout, proot, last_stage >= 4);
            }
            if (status->print_dot)
                /* last_stage >= 4   <==>  namespaces resolved already? */
                PFabssyn_dot (pfout, proot, last_stage >= 4);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "parse tree not available at this point of compilation");
    }

    /* print core tree if requested */
    if (status->print_core_tree) {
        if (croot) {
            if (status->print_pretty) {
                if (! status->quiet)
                    printf ("Core tree %s:\n", phases[status->stop_after]);
                PFcore_pretty (pfout, croot, status->print_types);
            }
            if (status->print_dot)
                PFcore_dot (pfout, croot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "core tree not available at this point of compilation");
    }

    /* print algebra tree if requested */
    if (status->print_la_tree) {
        PFchar_array_t *out_str = PFchar_array (4096);
        if (laroot) {
            if (status->print_pretty) {
                PFinfo (OOPS_WARNING,
                        "Cannot prettyprint logical algebra tree. Sorry.");
            }
            if (status->print_dot)
                PFla_dot (out_str, laroot, status->format);
            if (status->print_xml)
                PFla_xml (out_str, laroot, status->format);
        }
        else if (lapb) {
            if (status->print_dot)
                PFla_dot_bundle (out_str, lapb, status->format);
            if (status->print_xml)
                PFla_xml_bundle (out_str, lapb, status->format);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "logical algebra tree not available "
                    "at this point of compilation");
        fputs (out_str->base, pfout);
    }

    /* print MIL algebra tree if requested */
    if (status->print_pa_tree) {
        if (paroot) {
            if (status->print_pretty) {
                PFinfo (OOPS_WARNING,
                        "Cannot prettyprint physical algebra tree. Sorry.");
            }
            if (status->print_dot)
                PFpa_dot (pfout, paroot, status->format);
            if (status->print_xml)
                PFinfo (OOPS_WARNING,
                        "No xml representation for physical algebra tree."
                        " Sorry.");
        }
        else
            PFinfo (OOPS_NOTICE,
                    "Physical algebra tree not available at this "
                    "point of compilation");
    }

    return ( 1 ); /* EXIT_SUCCESS */

 failure:
    return ( -1 ); /* EXIT_FAILURE */
}

/**
 * Compiler driver of the Pathfinder compiler interface for usage
 * by the Monet Runtime environment.
 *
 * MonetDB actually would like pathfinder to
 * - be thread-safe (now there are global vars all over the place)
 * - use string input/output rather than files.
 *
 * This interface fixes the second issue. For the moment, the MonetDB
 * Runtime environment uses a lock to stay stable under concurrent requests.
 */
char*
PFcompile_MonetDB (char *xquery, char* url,
                   char** prologue, char** query, char** epilogue,
                   int options, char *genType)
{
        PFstate_t PFstate;
        PFpnode_t  *proot  = NULL;
        PFcnode_t  *croot  = NULL;
        int num_fun;
        long timing;
        int module_base;
        /* for ALGEBRA (PFoutput_format_mil) & SQL (PFoutput_format_sql) */
        PFla_op_t      *laroot = NULL;
        PFpa_op_t      *paroot = NULL;
        PFmil_t        *mroot  = NULL;
        PFchar_array_t *serialized_mil_code = NULL;
        PFguide_list_t *guide_list = NULL; /* guide list */
        /* for MILPRINT_SUMMER (PFoutput_format_milprint_summer) */
        char *intern_prologue = NULL,
             *intern_query = NULL,
             *intern_epilogue = NULL;

        PFquery_t PFquery = {
            .version             = "1.0",
            .encoding            = NULL,
            .ordering            = true,  /* implementation defined: ordered */
            .empty_order         = undefined,
            .inherit_ns          = false, /* implementation def'd: inherit-ns: no */
            .pres_boundary_space = false,
            .revalid             = revalid_strict /* XQ update facility 1.0 [§2.3] */
        };

        PFmem_init ();

        /* initialize data structures in the Parser department */
        PFparser_init ();
        /* initialize the datastructures in the Core department */
        PFcore_init ();
        /* Initialize data structures in the functions department */
        PFfunctions_init ();
        /* Initialize data structures in the Options department */
        PFoptions_init ();
        /* Initialize data structures in the Varscope department */
        PFvarscope_init ();
        

#ifndef NDEBUG
        PFarray_init ();
#endif
        /** initialize global state of the compiler */
        PFstate_init (&PFstate);

        PFstate.invocation = invoke_monetdb;

        if (options & COMPILE_OPTION_ALGEBRA) {
                PFstate.output_format = PFoutput_format_mil;
        } else {
                PFstate.output_format = PFoutput_format_milprint_summer;
        }

        /* the state of the standoff_axis_steps support should be
         * passed through the function-arguments.
         */
        PFstate.standoff_axis_steps = (options & COMPILE_OPTION_STANDOFF);

        if (setjmp(PFexitPoint) != 0 ) {
                PFmem_destroy ();
                return PFerrbuf;
        }
        timing = PFtimer_start ();

        /* repeat PFcompile, which we can't reuse as we don't want to deal with files here */
        num_fun = PFparse (xquery, &proot, &PFquery,
                           PFstate.standoff_axis_steps);
        module_base = PFparse_modules (proot, &PFquery,
                                       PFstate.standoff_axis_steps);
        proot = PFnormalize_abssyn (proot);

        if (PFstate.output_format == PFoutput_format_mil ||
            PFstate.output_format == PFoutput_format_milprint_summer) {
            proot = PFheuristic_index (proot);
        }
        PFns_init ();
        PFqname_init ();
        PFns_resolve (proot);
        PFextract_options (proot);
        PFvarscope (proot);
        PFfun_xquery_fo ();
        PFty_predefined ();
        PFfun_check (proot);
        PFschema_import (proot);
        croot = PFfs (proot, &PFquery);
        croot = PFsimplify (croot);
        croot = PFty_check (croot);
        croot = PFcoreopt (croot);

    if (PFstate.output_format == PFoutput_format_milprint_summer) {
        (void)  PFprintMILtemp (croot, 1, module_base, num_fun, timing,
                                &intern_prologue, &intern_query, &intern_epilogue,
                                url, PFstate.standoff_axis_steps);

        /* make sure that we do NOT use memory that lies
           in the pathfinder heap -- MonetDB will use (and free) it */
        *prologue = malloc (strlen (intern_prologue) + 1);
        *query    = malloc (strlen (intern_query) + 1);
        *epilogue = malloc (strlen (intern_epilogue) + 1);
        strcpy (*prologue, intern_prologue);
        strcpy (*query, intern_query);
        strcpy (*epilogue, intern_epilogue);
    } else {
        /* compile into logical algebra */
        laroot = PFcore2alg (croot, &PFquery,
                             PFstate.output_format);
        /* optimize logical algebra */
        laroot = PFalgopt (laroot, false /* no timing output */,
                           guide_list, PFstate.opt_alg);
        /* common subexpression elimination on logical algebra */
        laroot = PFla_cse (laroot);
        /* compile logical into a physical plan */
        paroot = PFplan (laroot);
        /* add border operators to the physical plan */
        paroot = PFpa_intro_borders (paroot);

        /* generate internal MIL representation */
        mroot = PFmilgen (paroot, genType);

        if (!strncmp ("timing", genType, 6))
            /* make sure the timing variables are retained */
            mroot = PFmil_seq (mroot,
                               PFmil_use (PFmil_var (PF_MIL_VAR_TIME_LOAD)),
                               PFmil_use (PFmil_var (PF_MIL_VAR_TIME_QUERY)),
                               PFmil_use (PFmil_var (PF_MIL_VAR_TIME_PRINT)));

        /* some dead-code elimination */
        mroot = PFmil_dce (mroot);

        if (!strncmp ("timing", genType, 6))
            /* add timing information */
            mroot = PFmil_seq (mroot,
                               PFmil_printf (
                                   PFmil_arg (PFmil_lit_str ("\nTrans  % 10.3f msec"
                                                             "\nShred  % 10.3f msec"
                                                             "\nQuery  % 10.3f msec"
                                                             "\nPrint  % 10.3f msec\n"),
                                   PFmil_arg (PFmil_div (PFmil_cast (PFmil_type (mty_dbl),
                                                                     PFmil_lit_dbl (PFtimer_stop (timing))),
                                                         PFmil_lit_dbl(1000.0)),
                                   PFmil_arg (PFmil_var (PF_MIL_VAR_TIME_LOAD),
                                   PFmil_arg (PFmil_var (PF_MIL_VAR_TIME_QUERY),
                                              PFmil_var (PF_MIL_VAR_TIME_PRINT)))))));

        /* and serialize our internal representation into actual MIL code */
        serialized_mil_code = PFmil_serialize (mroot);

        /*
         * copy generated MIL code into a string buffer that is allocated
         * via malloc.  MonetDB will take care of freeing it.
         */
        *query = malloc (strlen ((char *) serialized_mil_code->base) + 1);
        if (! *query)
            PFoops (OOPS_OUTOFMEM, "problem allocating memory");
        strcpy (*query, (char *) serialized_mil_code->base);

        /* we don't actually need a prolog or epilogue */
        *prologue = malloc (1); **prologue = '\0';
        *epilogue = malloc (1); **epilogue = '\0';
    }

        PFmem_destroy ();
        return (*PFerrbuf) ? PFerrbuf : NULL;
}

#if HAVE_SIGNAL_H
/**
 * This handler is called whenever we get a SIGSEGV.
 *
 * It will print out some informative message and then terminate the program.
 */
RETSIGTYPE
segfault_handler (int sig)
{
    fprintf (stderr,
             "!ERROR: Segmentation fault.\n"
             "The Pathfinder compiler experienced an internal problem.\n"
             "You may want to report this problem to the Pathfinder \n"
             "development team (pathfinder@pathfinder-xquery.org).\n\n"
             "When reporting problems, please attach your XQuery input,\n"
             "as well as the following information:\n"
             "  Compilation stage: %u\n\n"
             "We apologize for the inconvenience...\n\n",
             last_stage);

    signal (sig, SIG_DFL);
    raise (sig);
}
#endif

/* vim:set shiftwidth=4 expandtab: */
