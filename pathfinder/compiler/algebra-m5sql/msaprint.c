/**
 * @file
 *
 * Debugging: dump M5 SQL algebra plan in AT&T dot format.
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
 * $Id: $
 */

/* always include pf_config.h first! */
#include "pf_config.h"
#include "pathfinder.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "msaprint.h"

#include "alg_dag.h"
#include "mem.h"
#include "oops.h"
#include "pfstrings.h"

#include "msa_mnemonic.h"


/** Node names to print out for all the SQL algebra operator nodes. */
static char *dot_op_id[]  = {
      [msa_op_serialize_rel]    = "REL SERIALIZE"
    , [msa_op_project]          = "PROJECT"
    , [msa_op_select]           = "SELECT"
    , [msa_op_literal_table]    = "LIT TABLE"
    , [msa_op_table]            = "TABLE"
    , [msa_op_union]            = "UNION"
    , [msa_op_except]           = "EXCEPT"
    , [msa_op_groupby]          = "GROUPBY"
    , [msa_op_join]             = "JOIN"
    , [msa_op_semijoin]         = "SEMIJOIN"
    , [msa_op_cross]            = "CROSS"
    , [msa_op_nil_node]         = "NIL"
};

/** Colors to print out for all the SQL algebra operator nodes. */
static char *dot_op_color[] = {
      [msa_op_serialize_rel]    = "#C0C0C0"
    , [msa_op_project]          = "#87CEFA"
    , [msa_op_select]           = "#87CEFA"
    , [msa_op_literal_table]    = "#BFEFFF"
    , [msa_op_table]            = "#BFEFFF"
    , [msa_op_union]            = "#00B2EE"
    , [msa_op_except]           = "#00B2EE"
    , [msa_op_groupby]          = "#1E90FF"
    , [msa_op_join]             = "#1874CD"
    , [msa_op_semijoin]         = "#1874CD"
    , [msa_op_cross]            = "#1874CD"
    , [msa_op_nil_node]         = "#FFFFFF"
};

/** Node names to print out for all the SQL algebra expression nodes. */
static char *dot_expr_id[]  = {
      [msa_expr_func]             = "func"
    , [msa_expr_num_gen]          = "num_gen"
    , [msa_expr_comp]             = "comp"
    , [msa_expr_aggr]             = "aggr"
    , [msa_expr_convert]          = "convert"
    , [msa_expr_column]           = "col"
    , [msa_expr_atom]             = "atom"
};

/** Names to print out for all the SQL algebra expression aggregation nodes. */
static char *dot_expr_aggr_id[]  = {
      [msa_aggr_count]          = "count"
    , [msa_aggr_sum]            = "sum"
    , [msa_aggr_avg]            = "avg"
    , [msa_aggr_max]            = "max"
    , [msa_aggr_min]            = "min"
    , [msa_aggr_dist]           = "dist"
    , [msa_aggr_seqty1]         = "seqty1"
    , [msa_aggr_all]            = "all"
    , [msa_aggr_prod]           = "prod"
};

/* Names to print out for all comparison types */
static char *dot_expr_comp_id[] = {
      [msa_comp_equal]          = "equal"
    , [msa_comp_notequal]       = "not_eq"
    , [msa_comp_lt]             = "lt"
    , [msa_comp_lte]            = "lte"
    , [msa_comp_gt]             = "gt"
    , [msa_comp_gte]            = "gte"
};

/** Names to print out for all the SQL algebra expression function nodes. */
static char *dot_expr_func_id[]  = {
      [msa_func_add]              = "add"
    , [msa_func_sub]            = "sub"
    , [msa_func_mult]           = "mult"
    , [msa_func_div]            = "div"
    , [msa_func_mod]            = "mod"
    , [msa_func_and]            = "and"
    , [msa_func_or]             = "or"
    , [msa_func_not]            = "not"
};

/** Names to print out for all number generating function types */
static char *dot_expr_num_gen_id[]  = {
      [msa_num_gen_rank]        = "rank"
    , [msa_num_gen_rowrank]     = "rowrank"
    , [msa_num_gen_rownum]      = "rownum"
    , [msa_num_gen_rowid]       = "rowid"
};

/** Colors to print out for all the SQL algebra expression nodes. */
static char *dot_expr_color[] = {
      [msa_expr_func]             = "#B9D3EE"
    , [msa_expr_num_gen]          = "#B9D3EE"
    , [msa_expr_comp]             = "#B9D3EE"
    , [msa_expr_aggr]             = "#9FB6CD"
    , [msa_expr_convert]          = "#C6E2FF"
    , [msa_expr_column]           = "#C6E2FF"
    , [msa_expr_atom]             = "#C6E2FF"
};

/* helper function that returns string with value of atom */
static char *
msa_atom_str(PFalg_atom_t atom)
{
    unsigned int maxlength = 50;
    char *ret = PFmalloc(maxlength * sizeof(char));
    
    switch (atom.type) {
        case aat_nat:
            sprintf(ret, "%i", atom.val.nat_);
            break;
        case aat_int:
            sprintf(ret, "%lli", atom.val.int_);
            break;
        case aat_str:
            if (strlen(atom.val.str) >= 10) {
                sprintf(ret, "%s", atom.val.str);
                ret[6] = '.';
                ret[7] = '.';
                ret[8] = '.';
                ret[9] = '\0';
            } else sprintf(ret, "%s", atom.val.str);
            break;
        case aat_dec:
            sprintf(ret, "%f", atom.val.dec_);
            break;
        case aat_dbl:
            sprintf(ret, "%f", atom.val.dbl);
            break;
        case aat_bln:
            sprintf(ret, "%s", atom.val.bln ? "true" : "false");
            break;
        case aat_qname:
            sprintf(ret, "%i", atom.val.qname);
            break;
        default:
            break;
    }
    
    /* make sure that string terminates after maxlength */
    ret[maxlength - 1] = '\0';
    
    return ret;
}

/**
 * Print SQL algebra expression in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 */
static void
msa_dot_expr (PFarray_t *dot, PFmsa_expr_t *n)
{
    unsigned int c;
    assert(n->node_id);
    printf("/* refctr here! %i */\n", n->refctr);
    /* open up label */
    PFarray_printf (dot, "expr_%i [label=\"%s\\n", n->node_id, dot_expr_id[n->kind]);
    
    /* create additional semantic information */
    switch (n->kind)
    {
        case msa_expr_convert:
            PFarray_printf(dot, "to: %s\\n", PFalg_simple_type_str(n->type));
            PFarray_printf(dot, "%s", PFcol_str(n->col));
            break;
            
        case msa_expr_func:
        {
            PFarray_printf(dot, "%s\\n", dot_expr_func_id[n->sem.func.name]);
            PFarray_printf(dot, "%s", PFcol_str(n->col));
        }
            break;
            
        case msa_expr_num_gen:
        {
            PFmsa_expr_t *expr;
            
            PFarray_printf(dot, "%s\\n", dot_expr_num_gen_id[n->sem.num_gen.kind]);
            
            /* print partitioning columns */
            if (n->sem.num_gen.kind == msa_num_gen_rownum)
            {
                PFarray_printf(dot, "part cols: <");
                for (c = 0; c < elsize(n->sem.num_gen.part_cols) - 1; c++) {
                    expr = elat(n->sem.num_gen.part_cols, c);
                    PFarray_printf(dot, "%s, ", PFcol_str(expr->col));
                }
                expr = elat(n->sem.num_gen.part_cols, c);
                PFarray_printf(dot, "%s>\\n", PFcol_str(expr->col));
            }
            
            PFarray_printf(dot, "%s", PFcol_str(n->col));
        }
            break;
        
        case msa_expr_comp:
            PFarray_printf(dot, "%s\\n", dot_expr_comp_id[n->sem.comp.kind]);
            PFarray_printf(dot, "%s", PFcol_str(n->col));
            break;    

        case msa_expr_aggr:
            PFarray_printf(dot, "%s\\n", dot_expr_aggr_id[n->sem.aggr.kind]);
            PFarray_printf(dot, "%s", PFcol_str(n->col));
            break;
            
        case msa_expr_atom:
            /* print atom value and type */
            PFarray_printf(dot, "val: %s (%s)\\n", msa_atom_str(n->sem.atom.atom),
                           PFalg_simple_type_str(n->type));

            PFarray_printf(dot, "%s", PFcol_str(n->col));
            break;
            
        case msa_expr_column:
            PFarray_printf(dot, "%s", PFcol_str(n->col));
            if ((n->col) != (n->sem.col.old))
                PFarray_printf (dot, " : %s", PFcol_str(n->sem.col.old));
            if ((n->sortorder) != DIR_ASC)
                PFarray_printf (dot, " (desc) \\n");
            break;

            
        default:
            break;
    }
    
    /* close up label */
    PFarray_printf (dot, "\", fillcolor=\"%s\" ];\n", dot_expr_color[n->kind]);
    
    for (c = 0; c < PFMSA_EXPR_MAXCHILD && n->child[c]; c++) {
        PFarray_printf (dot,
                        "expr_%i -> expr_%i;\n",
                        n->node_id, n->child[c]->node_id);
    }
    
    /* mark node visited */
    n->bit_dag = true;
    
    for (c = 0; c < PFMSA_EXPR_MAXCHILD && n->child[c]; c++) {
        if (!n->child[c]->bit_dag)
            msa_dot_expr (dot, n->child[c]);
    }
}

/**
 * Print SQL algebra operator in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 */
static void
msa_dot_op (PFarray_t *dot, PFarray_t *dot_expr, PFmsa_op_t *n)
{
    unsigned int c, i, j;
    assert(n->node_id);
    printf("/* refctr here! %i */\n", n->refctr);
    /* open up label */
    PFarray_printf (dot, 
                    "node_%i [label=\"{ <op%i> %s\\n",
                    n->node_id,
                    n->node_id,
                    dot_op_id[n->kind]);
    
    if (n->kind == msa_op_project &&
        n->distinct == true) PFarray_printf(dot, "DISTINCT\\n");
    
    /* NIL node: close label and return */
    if (n->kind == msa_op_nil_node){
        PFarray_printf (dot, "}\", fillcolor=\"%s\" ];\n", dot_op_color[n->kind]);
        
        /* mark node visited */
        n->bit_dag = true;
        return;
    }
    
    /* print schema for all operators */
    PFarray_printf(dot, "Schema: ( ");
    for (i = 0; i < n->schema.count - 1; i++)
        PFarray_printf (dot, "%s:%s \\| ",
                        PFcol_str(n->schema.items[i].name),
                        PFalg_simple_type_str(n->schema.items[i].type));
    PFarray_printf (dot, "%s:%s )\\n",
                    PFcol_str(n->schema.items[i].name),
                    PFalg_simple_type_str(n->schema.items[i].type));
    
    /* create additional semantic information for operator */
    switch (n->kind)
    {

        /* print table for literal table then return */
        case msa_op_literal_table:
            
            PFarray_printf(dot, "| { cols ");
            for (i = 0; i < n->schema.count; i++)
                PFarray_printf (dot, "| %s:%s",
                                PFcol_str(n->schema.items[i].name),
                                PFalg_simple_type_str(n->schema.items[i].type));
            PFarray_printf(dot, "}");
            break;

        case msa_op_serialize_rel:

            /* print iter, pos and items */
            PFarray_printf(dot, "| { iter:\\n%s | pos:\\n%s",
                           PFcol_str(n->sem.ser_rel.iter),
                           PFcol_str(n->sem.ser_rel.pos));
            PFarray_printf(dot, "| { items:");
            for (i = 0; i < PFalg_collist_size(n->sem.ser_rel.items); i++) {
                PFalg_col_t curr_col = PFalg_collist_at(n->sem.ser_rel.items,i);
                PFarray_printf(dot, "| %s", PFcol_str(curr_col));
            }
            PFarray_printf(dot, "}}");
            break;

            
        case msa_op_project:

            PFarray_printf(dot, "| { exprs:");
            for (i = 0; i < elsize(n->sem.proj.expr_list); i++) {
                PFmsa_expr_t *curr_expr = elat(n->sem.proj.expr_list,i);
                PFarray_printf (dot,
                                "| <expr%i> %s",
                                curr_expr->node_id,
                                PFcol_str(curr_expr->col));
                
                /* print edge from record entry to expression */
                PFarray_printf (dot_expr,
                                "node_%i:expr%i -> expr_%i;\n",
                                n->node_id,
                                curr_expr->node_id,
                                curr_expr->node_id);
                
                /* traverse associated expression trees */
                msa_dot_expr(dot_expr, curr_expr);
            }
            PFarray_printf(dot, "}");
            break;
            
        case msa_op_select:

            PFarray_printf(dot, "| { exprs:");
            for (i = 0; i < elsize(n->sem.select.expr_list); i++) {
                PFmsa_expr_t *curr_expr = elat(n->sem.select.expr_list,i);
                PFarray_printf (dot,
                                "| <expr%i> %s",
                                curr_expr->node_id,
                                PFcol_str(curr_expr->col));

                /* print edge from record entry to expression */
                PFarray_printf (dot_expr,
                                "node_%i:expr%i -> expr_%i;\n",
                                n->node_id,
                                curr_expr->node_id,
                                curr_expr->node_id);
                
                /* traverse associated expression trees */
                msa_dot_expr(dot_expr, curr_expr);
            }
            PFarray_printf(dot, "}");
            break;
            
        case msa_op_groupby:

            PFarray_printf(dot, "| { prj:");
            for (i = 0; i < elsize(n->sem.groupby.prj_list); i++) {
                PFmsa_expr_t *curr_expr = elat(n->sem.groupby.prj_list,i);
                PFarray_printf (dot,
                                "| <prj%i> %s",
                                curr_expr->node_id,
                                PFcol_str(curr_expr->col));
                
                /* print edge from record entry to expression */
                PFarray_printf (dot_expr,
                                "node_%i:prj%i -> expr_%i;\n",
                                n->node_id,
                                curr_expr->node_id,
                                curr_expr->node_id);
                
                /* traverse associated expression trees */
                msa_dot_expr(dot_expr, curr_expr);
            }
            PFarray_printf(dot, "}");
            PFarray_printf(dot, "| { grp:");
            for (i = 0; i < elsize(n->sem.groupby.grp_list); i++) {
                PFmsa_expr_t *curr_expr = elat(n->sem.groupby.grp_list,i);
                PFarray_printf (dot,
                                "| <grp%i> %s",
                                curr_expr->node_id,
                                PFcol_str(curr_expr->col));
                
                /* print edge from record entry to expression */
                PFarray_printf (dot_expr,
                                "node_%i:grp%i -> expr_%i;\n",
                                n->node_id,
                                curr_expr->node_id,
                                curr_expr->node_id);
                
                /* traverse associated expression trees */
                msa_dot_expr(dot_expr, curr_expr);
            }
            PFarray_printf(dot, "}");
            break;
            
        case msa_op_table:
        {
            PFarray_printf(dot, "%s\\n", n->sem.table.name);
            /* print original names of table */
            PFarray_printf(dot, "| { cols: ");
            for (i = 0; i < elsize(n->sem.table.expr_list); i++) {
                PFmsa_expr_t *curr_name = elat(n->sem.table.col_names, i);
                
                PFarray_printf(dot, "| %s: %s (%s) ", PFcol_str(n->schema.items[i].name),
                               msa_atom_str(curr_name->sem.atom.atom),
                               PFalg_simple_type_str(n->schema.items[i].type));
            }
            PFarray_printf(dot, "}");
        }
            break;

            
        case msa_op_join:
        case msa_op_semijoin:

            PFarray_printf(dot, "| { exprs:");
            for (i = 0; i < elsize(n->sem.join.expr_list); i++) {
                PFmsa_expr_t *curr_expr = elat(n->sem.join.expr_list,i);
                PFarray_printf (dot,
                                "| <expr%i> %s",
                                curr_expr->node_id,
                                PFcol_str(curr_expr->col));
                
                /* print edge from record entry to expression */
                PFarray_printf (dot_expr,
                                "node_%i:expr%i -> expr_%i;\n",
                                n->node_id,
                                curr_expr->node_id,
                                curr_expr->node_id);
                
                /* traverse associated expression trees */
                msa_dot_expr(dot_expr, curr_expr);
            }
            PFarray_printf(dot, "}");
            break;
            
        default:
            break;
    }
    
    /* close up label */
    PFarray_printf (dot, "}\", fillcolor=\"%s\" ];\n", dot_op_color[n->kind]);
    
    /* print extra node for literal table */
    if (n->kind == msa_op_literal_table) {
        PFarray_printf(dot, "subgraph clusterOp%i {\n"
                       "color=\"%s\";\n",
                       n->node_id,
                       dot_op_color[n->kind]);
        /* print table node */
        PFarray_printf (dot, 
                        "node_%i_table [label=",
                        n->node_id);
        
        /* print table. if table is empty just print 'EMPTY TABLE'*/
        if (n->sem.literal_table.tuples_count > 0){
            PFarray_printf(dot, "<<table border=\"0\" cellborder=\"0\">");
            PFarray_printf(dot, "<tr>");
            for (i = 0; i < n->schema.count; i++)
                PFarray_printf (dot, "<td>%s:%s</td>",
                                PFcol_str(n->schema.items[i].name),
                                PFalg_simple_type_str(n->schema.items[i].type));
            PFarray_printf(dot, "</tr>");
            for (i = 0; i < n->sem.literal_table.tuples_count; i++) {
                PFalg_tuple_t curr_tuple = n->sem.literal_table.tuples[i];
                PFarray_printf(dot, "<tr>");
                for (j = 0; j < curr_tuple.count; j++) {
                    PFalg_atom_t curr_atom = curr_tuple.atoms[j];
                    PFarray_printf (dot,
                                    "<td>%s</td>",
                                    msa_atom_str(curr_atom));
                }
                PFarray_printf(dot, "</tr>");
            }
            PFarray_printf(dot, "</table>>, ");
        } else {
            PFarray_printf(dot, "\"EMPTY TABLE\", ");
        }
        PFarray_printf (dot, "fillcolor=\"#FFFFFF\" ];\n");
        PFarray_printf (dot, "node_%i -> node_%i_table [style=invis];\n", n->node_id,
                        n->node_id);
        PFarray_printf(dot, "}\n");
    }
    

    /* remember if current operator resides inside a cluster (for drawing arrows) */
    bool cluster = false;
    
    /* flush expression buffer (if filled) and reset it afterwards */
    if (PFarray_last(dot_expr)) {
        PFarray_printf (dot, 
                        "subgraph clusterOp%i {\n"
                        "color=\"%s\";\n"
                        "style=bold;\n"
                        "node [shape=ellipse];\n"
                        "edge [style=solid];\n"
                        "%s"
                        "}\n",
                        n->node_id,
                        dot_op_color[n->kind],
                        (char *) dot_expr->base);
        cluster = true;
        PFarray_last(dot_expr) = 0;
    }

    for (c = 0; c < PFMSA_OP_MAXCHILD && n->child[c]; c++) {
        PFarray_printf (dot,
                        "node_%i -> node_%i",
                        n->node_id,
                        n->child[c]->node_id);
        /* arrow points to cluster? */
        if (cluster) PFarray_printf(dot, "[ltail=clusterOp%i]", n->node_id);
        PFarray_printf(dot, ";\n");
    }

    /* mark node visited */
    n->bit_dag = true;

    for (c = 0; c < PFMSA_OP_MAXCHILD && n->child[c]; c++) {
        if (!n->child[c]->bit_dag)
            msa_dot_op (dot, dot_expr, n->child[c]);
    }
}

/* stub declaration */
static unsigned int
create_expr_id_in_list(PFmsa_exprlist_t *list, unsigned int i);

static unsigned int
create_expr_id_worker(PFmsa_expr_t *n, unsigned int i)
{
    if (n->bit_dag)
        return i;
    else
        n->bit_dag = true;
    
    n->node_id = i++;
    
    /* If expression has expression list, set node id for expressions as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            i = create_expr_id_in_list(n->sem.num_gen.sort_cols, i);
            i = create_expr_id_in_list(n->sem.num_gen.part_cols, i);
            break;
        default:
            break;
    }
    
    for (unsigned int c = 0; c < PFMSA_OP_MAXCHILD && n->child[c]; c++)
        i = create_expr_id_worker (n->child[c], i);
    
    return i;
}

/* helper function to create node ids for
   expressions in expression list */
static unsigned int
create_expr_id_in_list(PFmsa_exprlist_t *list, unsigned int i)
{
    unsigned int j;
    for (j = 0; j < elsize(list); j++) {
        PFmsa_expr_t *curr_expr = elat(list, j);
        i = create_expr_id_worker(curr_expr, i);
    }
    return i;
}

/* stub declaration */
static unsigned int
create_expr_id_in_list_(PFmsa_exprlist_t *list, unsigned int i);

static unsigned int
create_expr_id_worker_(PFmsa_expr_t *n, unsigned int i)
{
    if (n->bit_dag)
        return i;
    else
        n->bit_dag = true;
    
    n->node_id = i++;
    
    /* If expression has expression list, set node id for expressions as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            i = create_expr_id_in_list_(n->sem.num_gen.sort_cols, i);
            i = create_expr_id_in_list_(n->sem.num_gen.part_cols, i);
            break;
        default:
            break;
    }
    
    return i;
}

/* helper function to create node ids for
 expressions in expression list */
static unsigned int
create_expr_id_in_list_(PFmsa_exprlist_t *list, unsigned int i)
{
    unsigned int j;
    for (j = 0; j < elsize(list); j++) {
        PFmsa_expr_t *curr_expr = elat(list, j);
        i = create_expr_id_worker_(curr_expr, i);
    }
    return i;
}

static unsigned int
create_node_id_worker (PFmsa_op_t *n, unsigned int i)
{
    if (n->bit_dag)
        return i;
    else
        n->bit_dag = true;

    n->node_id = i++;
    
    /* If op has expression list, set node id for expressions as well */
    switch (n->kind) {
        case msa_op_project:
            i = create_expr_id_in_list(n->sem.proj.expr_list, i);
            break;
        case msa_op_select:
            i = create_expr_id_in_list(n->sem.select.expr_list, i);
            break;
        case msa_op_table:
            i = create_expr_id_in_list(n->sem.table.expr_list, i);
            i = create_expr_id_in_list(n->sem.table.col_names, i);
            break;
        case msa_op_groupby:
            i = create_expr_id_in_list(n->sem.groupby.grp_list, i);
            i = create_expr_id_in_list(n->sem.groupby.prj_list, i);
            break;
        case msa_op_join:
        case msa_op_semijoin:
            i = create_expr_id_in_list(n->sem.join.expr_list, i);
            break;
        default:
            break;
    }
    

    for (unsigned int c = 0; c < PFMSA_OP_MAXCHILD && n->child[c]; c++)
        i = create_node_id_worker (n->child[c], i);

    return i;
}

static void
create_node_id (PFmsa_op_t *n)
{
    (void) create_node_id_worker (n, 1);
    PFmsa_dag_reset (n);
}

/* stub declaration */
static void
reset_expr_id_in_list(PFmsa_exprlist_t *list);

static void
reset_expr_id_worker (PFmsa_expr_t *n)
{
    if (n->bit_dag)
        return;
    else
        n->bit_dag = true;
    
    n->node_id = 0;
    
    /* If expression has expression list, reset node id for expressions as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            reset_expr_id_in_list(n->sem.num_gen.sort_cols);
            reset_expr_id_in_list(n->sem.num_gen.part_cols);
            break;
        default:
            break;
    }
    
    for (unsigned int c = 0; c < PFMSA_EXPR_MAXCHILD && n->child[c]; c++)
        reset_expr_id_worker (n->child[c]);
}

/* helper function to reset node ids for
 expressions in expression list */
static void
reset_expr_id_in_list(PFmsa_exprlist_t *list)
{
    unsigned int j;
    for (j = 0; j < elsize(list); j++) {
        PFmsa_expr_t *curr_expr = elat(list, j);
        reset_expr_id_worker(curr_expr);
    }
}

static void
reset_node_id_worker (PFmsa_op_t *n)
{
    if (n->bit_dag)
        return;
    else
        n->bit_dag = true;

    n->node_id = 0;

    /* If op has expression list, reset node id for expressions as well */
    switch (n->kind) {
        case msa_op_project:
            reset_expr_id_in_list(n->sem.proj.expr_list);
            break;
        case msa_op_select:
            reset_expr_id_in_list(n->sem.select.expr_list);
            break;
        case msa_op_table:
            reset_expr_id_in_list(n->sem.table.expr_list);
            reset_expr_id_in_list(n->sem.table.col_names);
            break;    
        case msa_op_groupby:
            reset_expr_id_in_list(n->sem.groupby.grp_list);
            reset_expr_id_in_list(n->sem.groupby.prj_list);
            break;
        case msa_op_join:
        case msa_op_semijoin:
            reset_expr_id_in_list(n->sem.join.expr_list);
            break;
        default:
            break;
    }
    
    for (unsigned int c = 0; c < PFMSA_OP_MAXCHILD && n->child[c]; c++)
        reset_node_id_worker (n->child[c]);
}

static void
reset_node_id (PFmsa_op_t *n)
{
    reset_node_id_worker (n);
    PFmsa_dag_reset (n);
}

/**
 * Dump SQL algebra tree initialization in AT&T dot format
 */
static void
msa_dot_init (FILE *f)
{
    fprintf (f, "digraph XQueryAlgebra {\n"
                "compound=true;\n"
                "ordering=out;\n"
                "node [shape=record];\n"
                "node [height=0.1];\n"
                "node [width=0.2];\n"
                "node [style=filled];\n"
                "node [color=\"#050505\"];\n"
                "node [fontsize=10];\n"
                "edge [fontsize=9];\n"
                "edge [dir=back];\n"
                "edge [style=bold];\n");
}

/**
 * Worker for PFmsa_dot and PFmsa_dot_bundle
 */
static void
msa_dot_internal (FILE *f, PFmsa_op_t *root)
{
    /* initialize array to hold dot output */
    PFarray_t *dot = PFarray (sizeof (char), 32000);
    PFarray_t *dot_helper = PFarray (sizeof (char), 3200);

    /* inside debugging we need to reset the dag bits first */
    PFmsa_dag_reset (root);
    create_node_id (root);
    msa_dot_op (dot, dot_helper, root); // worker anstossen
    PFmsa_dag_reset (root);
    reset_node_id (root);

    /* put content of array into file */
    fprintf (f, "%s}\n", (char *) dot->base);
}

/**
 * Dump M5 SQL algebra in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param root root of SQL algebra plan
 */
void
PFmsa_dot (FILE *f, PFmsa_op_t *root)
{
    assert (root);

    msa_dot_init (f);
    msa_dot_internal (f, root);
}

/* vim:set shiftwidth=4 expandtab: */
