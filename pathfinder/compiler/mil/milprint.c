/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */
/**
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include "pathfinder.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "milprint.h"

#include "array.h"
#include "pfstrings.h"
#include "oops.h"

/* inserted for PFmalloc in temporary MILprint */
#include "mem.h"

/* ============================================================================ */
/* ====================                                        ================ */
/* ====================  added MIL Hack ouput (iter|pos|item)  ================ */
/* ====================                                        ================ */
/* ============================================================================ */

    /* saves the actual level (corresponding to the for nodes) */
    static unsigned int act_level = 0;
    /* 'counter' is the number of saved intermediate results */
    static unsigned int counter = 0;

    static void
    translate2MIL (PFcnode_t *c);

    static void
    init (void)
    {
        printf("# init ()\n");
        /* pathfinder functions (scj, doc handling) are made visible */
        printf("# module(\"pathfinder\");\n");
        printf("# module(\"pf_support\");\n");
        printf("# module(\"aggrX3\");\n");
        printf("# module(\"xtables\");\n");
        printf("# module(\"malalgebra\");\n");

        /* for debugging purposes "foo.xml" is loaded */
        printf("var ws := create_ws();\n");
        printf("ws := add_doc(ws, \"foo.xml\");\n");
        /* the first loop is initialized */
        printf("var loop000 := bat(void,oid).seqbase(0@0);\n");
        printf("loop000.insert(0@0, 1@0);\n");
        /* variable environment vars */
        printf("var vu_fid;\n");
        printf("var vu_vid;\n");
        printf("var inner000 := loop000;\n");
        printf("var outer000 := loop000;\n");
        printf("var v_vid000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_iter000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_pos000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_item000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_kind000 := bat(void,chr).access(BAT_APPEND).seqbase(0@0);\n");

        /* value containers */
        printf("var str_values := bat(void,str).seqbase(0@0).access(BAT_WRITE);\n");
        printf("str_values.reverse.key(true);\n");
        printf("var int_values := bat(void,int).seqbase(0@0).access(BAT_WRITE);\n");
        printf("int_values.reverse.key(true);\n");
        printf("var dbl_values := bat(void,dbl).seqbase(0@0).access(BAT_WRITE);\n");
        printf("dbl_values.reverse.key(true);\n");
        printf("var dec_values := bat(void,dbl).seqbase(0@0).access(BAT_WRITE);\n");
        printf("dec_values.reverse.key(true);\n");

        /* variable binding for loop-lifting of the empty sequence */
        printf("var empty_bat := bat(void,oid).seqbase(0@0);\n");
        printf("var empty_kind_bat := bat(void,int).seqbase(0@0);\n");

        /* variables for (intermediate) results */
        printf("var iter;\n");
        printf("var pos;\n");
        printf("var item;\n");
        printf("var kind;\n");
        
        /* variable for empty scj */
        printf("var empty_res_bat := bat(void,bat);\n");

        /* boolean mapping */
        printf("var bool_map := bat(bit,oid).insert(false,0@0).insert(true,1@0);\n");
        printf("var bool_not := bat(oid,oid).insert(0@0,1@0).insert(1@0,0@0);\n");

    }

    /**
     * the variables iter, pos, item, kind are used to
     * create an human readable output (iter|pos|item),
     * by converting the underlying value of item|kind
     * into a string
     */
    static void
    print_output (void)
    {
        printf("{ # print_output ()\n");
        /* the values of the different kinds are combined
           by inserting the converted bats into 'output_item' */
        printf("var output_item := bat(oid, str);\n");

        /* gets string values for string kind */ 
        printf("var temp1_str := kind.get_type(STR);\n");
        printf("temp1_str := temp1_str.mirror.leftfetchjoin(item);\n");
        printf("temp1_str := temp1_str.leftfetchjoin(str_values);\n");
        printf("output_item.insert(temp1_str);\n");
        printf("temp1_str := nil;\n");

        printf("var temp1_node := kind.get_type(NODE).mark(0@0).reverse;\n");
        printf("var backup_oids := temp1_node.reverse;\n");
        printf("var temp1_frag := temp1_node.leftfetchjoin(kind).get_fragment;\n");
        printf("var oid_pre := temp1_node.leftfetchjoin(item);\n");
        printf("{\n"
               "var oid_kind := mposjoin(oid_pre, temp1_frag, ws.fetch(PRE_KIND));\n"
               "var oid_elems := oid_kind.ord_uselect(ELEMENT).mark(0@0).reverse;\n"
               "var oid_texts := oid_kind.ord_uselect(TEXT).mark(0@0).reverse;\n"
               "var e_pres := oid_elems.leftfetchjoin(oid_pre);\n"
               "var e_frags := oid_elems.leftfetchjoin(temp1_frag);\n"
               "var t_pres := oid_texts.leftfetchjoin(oid_pre);\n"
               "var t_frags := oid_texts.leftfetchjoin(temp1_frag);\n");

        printf("temp1_node := [str](e_pres);\n"
               "temp1_node := temp1_node.[+](\" of frag: \");\n"
               "temp1_node := temp1_node.[+](e_frags.[str]);\n"
               "temp1_node := temp1_node.[+](\" (node) name: \");\n"
               "temp1_node := temp1_node.[+](mposjoin(mposjoin(e_pres, e_frags, ws.fetch(PRE_PROP)), "
                                                     "mposjoin(e_pres, e_frags, ws.fetch(PRE_FRAG)), "
                                                     "ws.fetch(QN_LOC)));\n"
               "temp1_node := temp1_node.[+](\"; size: \");\n"
               "temp1_node := temp1_node.[+](mposjoin(e_pres, e_frags, ws.fetch(PRE_SIZE)));\n"
               "temp1_node := temp1_node.[+](\"; level: \");\n"
               "temp1_node := temp1_node.[+]([int](mposjoin(e_pres, e_frags, ws.fetch(PRE_LEVEL))));\n");

        printf("var temp2_node := [str](t_pres);\n"
               "temp2_node := temp2_node.[+](\" of frag: \");\n"
               "temp2_node := temp2_node.[+](t_frags.[str]);\n"
               "temp2_node := temp2_node.[+](\" (text-node) value: '\");\n"
               "temp2_node := temp2_node.[+](mposjoin(mposjoin(t_pres, t_frags, ws.fetch(PRE_PROP)), "
                                                     "mposjoin(t_pres, t_frags, ws.fetch(PRE_FRAG)), "
                                                     "ws.fetch(PROP_TEXT)));\n"
               "temp2_node := temp2_node.[+](\"'; level: \");\n"
               "temp2_node := temp2_node.[+]([int](mposjoin(t_pres, t_frags, ws.fetch(PRE_LEVEL))));\n");

        printf("if (oid_elems.count = 0) temp1_node := temp2_node;\n"
               "else if (oid_texts.count != 0) {\n"
               "var res_mu := merged_union(oid_elems, oid_texts, "
                                          "temp1_node.reverse.mark(0@0).reverse, "
                                          "temp2_node.reverse.mark(0@0).reverse);\n"
               "temp1_node := res_mu.fetch(1);\n"
               "}\n"
               "}\n");
        printf("oid_pre := nil;\n");
        printf("temp1_frag := nil;\n");
        printf("output_item.insert(backup_oids.leftfetchjoin(temp1_node));\n");
        printf("backup_oids := nil;\n");
        printf("temp1_node := nil;\n");

        printf("var temp1_attr := kind.get_type(ATTR).mark(0@0).reverse;\n");
        printf("backup_oids := temp1_attr.reverse;\n");
        printf("var temp1_frag := temp1_attr.leftfetchjoin(kind).get_fragment;\n");
        printf("var oid_attr := temp1_attr.leftfetchjoin(item);\n");
        printf("temp1_attr := [str](oid_attr);\n");
        printf("temp1_attr := temp1_attr.[+](\" (attr) owned by: \");\n");
        printf("var owner_str := oid_attr.mposjoin(temp1_frag, ws.fetch(ATTR_OWN)).[str];\n"
               "{\n"
               "var nil_bool := owner_str.[isnil];\n"
               "var no_owner_str := nil_bool.ord_uselect(true).mark(0@0).reverse;\n"
               "var with_owner_str := nil_bool.ord_uselect(false).mark(0@0).reverse;\n"
               "var res_mu := merged_union(with_owner_str, no_owner_str, "
                                          "with_owner_str.leftfetchjoin(owner_str), "
                                          "no_owner_str.project(\"nil\"));\n"
               "owner_str := res_mu.fetch(1);\n"
               "if (owner_str.count != temp1_attr.count) "
               "ERROR (\"thinking error in attribute output printing\");\n"
               "}\n");
        printf("temp1_attr := temp1_attr.[+](owner_str);\n");
        printf("temp1_attr := temp1_attr.[+](\" of frag: \");\n");
        printf("temp1_attr := temp1_attr.[+](oid_attr.mposjoin(temp1_frag, ws.fetch(ATTR_FRAG)));\n");
        printf("temp1_attr := temp1_attr.[+](\"; \");\n");
        printf("temp1_attr := temp1_attr.[+](mposjoin(mposjoin(oid_attr, temp1_frag, ws.fetch(ATTR_QN)), "
                                                     "mposjoin(oid_attr, temp1_frag, ws.fetch(ATTR_FRAG)), "
                                                     "ws.fetch(QN_LOC)));\n");
        printf("temp1_attr := temp1_attr.[+](\"='\");\n");
        printf("temp1_attr := temp1_attr.[+](mposjoin(mposjoin(oid_attr, temp1_frag, ws.fetch(ATTR_PROP)), "
                                                     "mposjoin(oid_attr, temp1_frag, ws.fetch(ATTR_FRAG)), "
                                                     "ws.fetch(PROP_VAL)));\n");
        printf("temp1_attr := temp1_attr.[+](\"'\");\n");
        printf("oid_attr := nil;\n");
        printf("temp1_frag := nil;\n");
        printf("output_item.insert(backup_oids.leftfetchjoin(temp1_attr));\n");
        printf("backup_oids := nil;\n");
        printf("temp1_attr := nil;\n");

        printf("var temp1_qn := kind.get_type(QNAME).mirror;\n");
        printf("var oid_qnID := temp1_qn.leftfetchjoin(item);\n");
        printf("temp1_qn := [str](oid_qnID);\n");
        printf("temp1_qn := temp1_qn.[+](\" (qname) '\");\n");
        printf("temp1_qn := temp1_qn.[+](oid_qnID.leftfetchjoin(ws.fetch(QN_NS).fetch(WS)));\n");
        printf("temp1_qn := temp1_qn.[+](\":\");\n");
        printf("temp1_qn := temp1_qn.[+](oid_qnID.leftfetchjoin(ws.fetch(QN_LOC).fetch(WS)));\n");
        printf("temp1_qn := temp1_qn.[+](\"'\");\n");
        printf("oid_qnID := nil;\n");
        printf("output_item.insert(temp1_qn);\n");
        printf("temp1_qn := nil;\n");

        printf("var bool_strings := bat(oid,str).insert(0@0,\"false\").insert(1@0,\"true\");\n");
        printf("var temp1_bool := kind.get_type(BOOL);\n");
        printf("temp1_bool := temp1_bool.mirror.leftfetchjoin(item);\n");
        printf("temp1_bool := temp1_bool.leftfetchjoin(bool_strings);\n");
        printf("bool_strings := nil;\n");
        printf("output_item.insert(temp1_bool);\n");
        printf("temp1_bool := nil;\n");

        printf("var temp1_int := kind.get_type(INT);\n");
        printf("temp1_int := temp1_int.mirror.leftfetchjoin(item);\n");
        printf("temp1_int := temp1_int.leftfetchjoin(int_values);\n");
        printf("temp1_int := [str](temp1_int);\n");
        printf("output_item.insert(temp1_int);\n");
        printf("temp1__int := nil;\n");

        printf("var temp1_dbl := kind.get_type(DBL);\n");
        printf("temp1_dbl := temp1_dbl.mirror.leftfetchjoin(item);\n");
        printf("temp1_dbl := temp1_dbl.leftfetchjoin(dbl_values);\n");
        printf("temp1_dbl := [str](temp1_dbl);\n");
        printf("output_item.insert(temp1_dbl);\n");
        printf("temp1_dbl := nil;\n");

        printf("var temp1_dec := kind.get_type(DEC);\n");
        printf("temp1_dec := temp1_dec.mirror.leftfetchjoin(item);\n");
        printf("temp1_dec := temp1_dec.leftfetchjoin(dec_values);\n");
        printf("temp1_dec := [str](temp1_dec);\n");
        printf("output_item.insert(temp1_dec);\n");
        printf("temp1_dec := nil;\n");

        /*
        printf("print (iter, pos, item, kind);\n");
        printf("print (output_item);\n");
        */
        printf("printf(\"====================\\n\");\n");
        printf("printf(\"====== result ======\\n\");\n");
        printf("printf(\"====================\\n\");\n");
        printf("print (iter, pos, output_item);\n");
        printf("output_item := nil;\n");

        printf("printf(\"====================\\n\");\n");
        printf("printf(\"=== working set ====\\n\");\n");
        printf("printf(\"====================\\n\");\n");
        printf("if (ws.fetch(PRE_SIZE).count < 5) {\n");
        printf("printf(\"- loaded documents -\\n\");\n");
        printf("ws.fetch(DOC_LOADED).print;\n");
        printf("i := 0;\n");
        printf("while (i < ws.fetch(PRE_SIZE).count) {\n");
        printf("        if (i = 0) print(\"WS\");\n");
        printf("        else ws.fetch(DOC_LOADED).fetch(oid(i)).print;\n");
        printf("        printf(\"---- attributes ----\\n\");\n");
        printf("        if (ws.fetch(ATTR_OWN).fetch(i).count < 100) {\n");
        printf("                print(ws.fetch(ATTR_OWN).fetch(i), "
                                     "mposjoin(ws.fetch(ATTR_QN).fetch(i), "
                                              "ws.fetch(ATTR_FRAG).fetch(i), "
                                              "ws.fetch(QN_LOC)));\n");
        printf("        } else {\n");
        printf("                print(ws.fetch(ATTR_OWN).fetch(i).count);\n");
        printf("        }\n");
        printf("        printf(\"----- elements -----\\n\");\n");
        printf("        if (ws.fetch(PRE_SIZE).fetch(i).count < 100) {\n");
        /* have to handle TEXT and ELEMENT nodes different because
           otherwise fetch causes error */
        printf("                ws.fetch(PRE_KIND).fetch(i).access(BAT_READ);\n"
               "                var elems := ws.fetch(PRE_KIND).fetch(i).ord_uselect(ELEMENT).mark(0@0).reverse;\n"
               "                var e_props := elems.leftfetchjoin(ws.fetch(PRE_PROP).fetch(i));\n"
               "                var e_frags := elems.leftfetchjoin(ws.fetch(PRE_FRAG).fetch(i));\n"
               "                var e_qns := mposjoin(e_props, e_frags, ws.fetch(QN_LOC));\n"
               "                e_props := nil;\n"
               "                e_frags := nil;\n"
               "                var texts := ws.fetch(PRE_KIND).fetch(i).ord_uselect(TEXT).mark(0@0).reverse;\n"
               "                t_names := texts.project(\"(TEXT)\");\n"
               "                var res_mu := merged_union(elems, texts, e_qns, t_names);\n"
               "                elems := nil;\n"
               "                texts := nil;\n"
               "                ws.fetch(PRE_KIND).fetch(i).access(BAT_WRITE);\n"
               "                e_qns := nil;\n"
               "                t_names := nil;\n"
               "                var names := res_mu.fetch(0).reverse.leftfetchjoin(res_mu.fetch(1));\n");
        printf("                print(ws.fetch(PRE_SIZE).fetch(i), "
                                     "ws.fetch(PRE_LEVEL).fetch(i).[int], "
                                     "names);\n");
        printf("        } else {\n");
        printf("                print(ws.fetch(PRE_SIZE).fetch(i).count);\n");
        printf("        }\n");
        printf("i :+= 1;\n");
        printf("}\n");
        printf("} else {\n");
        printf("printf(\"to much content in the WS to print it for debugging purposes\\n\");\n");
        printf("if (ws.fetch(DOC_LOADED).count > 25) \n"
               "printf(\"(number of loaded documents: %%i)\\n\", ws.fetch(DOC_LOADED).count);\n"
               "else {\n"
               "printf(\"- loaded documents -\\n\");\n"
               "ws.fetch(DOC_LOADED).print;\n"
               "}\n");
        printf("}\n");
        printf("} # end of print_output ()\n");
    }

    static void
    translateEmpty (void)
    {
        printf("# translateEmpty ()\n");
        printf("iter := empty_bat;\n");
        printf("pos := empty_bat;\n");
        printf("item := empty_bat;\n");
        printf("kind := empty_kind_bat;\n");
    }

    static void
    cleanUpLevel (void)
    {
        printf("# cleanUpLevel ()\n");
        printf("inner%03u := nil;\n", act_level);
        printf("outer%03u := nil;\n", act_level);
        printf("loop%03u := nil;\n", act_level);

        printf("v_vid%03u := nil;\n", act_level);
        printf("v_iter%03u := nil;\n", act_level);
        printf("v_pos%03u := nil;\n", act_level);
        printf("v_item%03u := nil;\n", act_level);
        printf("v_kind%03u := nil;\n", act_level);
    }
                                                                                                                                                        
    /**
     * lookup a variable in the variable environment
     */
    static void
    translateVar (PFcnode_t *c)
    {
        printf("{ # translateVar (c)\n");
        printf("var vid := v_vid%03u.ord_uselect(%i@0);\n", act_level, c->sem.var->vid);
        printf("vid := vid.mark(0@0).reverse;\n");
        printf("iter := vid.leftfetchjoin(v_iter%03u);\n", act_level);
        printf("pos := vid.leftfetchjoin(v_pos%03u);\n", act_level);
        printf("item := vid.leftfetchjoin(v_item%03u);\n", act_level);
        printf("kind := vid.leftfetchjoin(v_kind%03u);\n", act_level);
        printf("vid := nil;\n");
        printf("} # end of translateVar (c)\n");
    }

    /**
     * saves a intermediate result
     */
    static int
    saveResult (void)
    {
        counter++;
        printf("{ # saveResult () : int\n");

        printf("iter%03u := iter;\n", counter);
        printf("pos%03u := pos;\n", counter);
        printf("item%03u := item;\n", counter);
        printf("kind%03u := kind;\n", counter);
        printf("iter := nil;\n");
        printf("pos := nil;\n");
        printf("item := nil;\n");
        printf("kind := nil;\n");

        printf("# saveResult () : int\n");
        return counter;
    }
    /**
     * gives back a intermediate result
     */
    static void
    deleteResult (void)
    {
        printf("# deleteResult ()\n");

        printf("iter%03u := nil;\n", counter);
        printf("pos%03u := nil;\n", counter);
        printf("item%03u := nil;\n", counter);
        printf("kind%03u := nil;\n", counter);

        printf("} # deleteResult ()\n");
        counter--;
    }

    static void
    translateSeq (int i)
    {
        printf("if (iter.count = 0) {\n");
        printf("        iter := iter%03u;\n", i);
        printf("        pos := pos%03u;\n", i);
        printf("        item := item%03u;\n", i);
        printf("        kind := kind%03u;\n", i);
        printf("} else if (iter%03u.count != 0)\n",i);
        printf("{ # translateSeq (counter)\n");
        printf("var merged_result := merged_union (iter%03u, iter, item%03u, item, kind%03u, kind);\n", i, i, i, i);
        printf("iter := merged_result.fetch(0);\n");
        printf("item := merged_result.fetch(1);\n");
        printf("kind := merged_result.fetch(2);\n");
        printf("merged_result := nil;\n");
        printf("pos := iter.mark_grp(iter.reverse.project(1@0));\n");
        printf("} # end of translateSeq (counter)\n");
    }

    /**
     * create the variables for the next for-scope
     */
    static void
    project (void)
    {
        printf("# project ()\n");
        printf("var outer%03u := iter;\n", act_level);
        printf("iter := iter.mark(1@0);\n");
        printf("var inner%03u := iter;\n", act_level);
        printf("pos := iter.project(1@0);\n");
        printf("var loop%03u := inner%03u;\n", act_level, act_level);

        printf("var v_vid%03u;\n", act_level);
        printf("var v_iter%03u;\n", act_level);
        printf("var v_pos%03u;\n", act_level);
        printf("var v_item%03u;\n", act_level);
        printf("var v_kind%03u;\n", act_level);
    }

    /**
     * find the variables which are used in a deeper nesting 
     */
    static void
    getExpanded (int fid)
    {
        printf("{ # getExpanded (fid)\n");
        printf("var vu_nil := vu_fid.ord_uselect(%i@0);\n",fid);
        printf("var vid_vu := vu_vid.reverse;\n");
        printf("var oid_nil := vid_vu.leftjoin(vu_nil);\n");
        printf("vid_vu := nil;\n");
        printf("expOid := v_vid%03u.leftjoin(oid_nil);\n", act_level - 1);
        printf("oid_nil := nil;\n");
        printf("expOid := expOid.mirror;\n");
        printf("} # end of getExpanded (fid)\n");
    }

    /**
     * create the expanded (inner_outer |X| iter) version of
     * the mapping
     */
    static void
    expand (void)
    {
        printf("{ # expand ()\n");

        printf("var expOid_iter := expOid.leftfetchjoin(v_iter%03u);\n", act_level-1); 
                                               /* -1 is important */
        printf("var iter_expOid := expOid_iter.reverse;\n");
        printf("expOid_iter := nil;\n");
        printf("var oidMap_expOid := outer%03u.leftjoin(iter_expOid);\n", act_level);
        printf("iter_expOid := nil;\n");
        printf("var expOid_oidMap := oidMap_expOid.reverse;\n");
        printf("oidMap_expOid := nil;\n");
        printf("expOid_iter := expOid_oidMap.leftfetchjoin(inner%03u);\n", act_level);
        printf("expOid_oidMap := nil;\n");
        printf("v_iter%03u := expOid_iter;\n", act_level);
        printf("oidNew_expOid := expOid_iter.mark(0@0).reverse;\n");
        printf("expOid_iter := nil;\n");

        printf("} # end of expand ()\n");
    }

    /**
     * map the columns to the next scope
     */
    static void
    join (void)
    {
        printf("# join ()\n");
        printf("v_iter%03u := v_iter%03u.reverse.mark(0@0).reverse;\n", act_level, act_level);

        printf("var new_v_iter := v_iter%03u;\n", act_level);
        printf("v_iter%03u := bat(void,oid,count(new_v_iter)*2);\n", act_level);
        printf("v_iter%03u.seqbase(0@0);\n", act_level);
        printf("v_iter%03u.access(BAT_APPEND);\n", act_level);
        printf("v_iter%03u.insert(new_v_iter);\n", act_level);
        printf("new_v_iter := nil;\n");

        printf("var new_v_vid := oidNew_expOid.leftjoin(v_vid%03u);\n", act_level - 1);
        printf("v_vid%03u := bat(void,oid,count(new_v_vid)*2);\n", act_level);
        printf("v_vid%03u.seqbase(0@0);\n", act_level);
        printf("v_vid%03u.access(BAT_APPEND);\n", act_level);
        printf("v_vid%03u.insert(new_v_vid);\n", act_level);
        printf("new_v_vid := nil;\n");

        printf("var new_v_pos := oidNew_expOid.leftjoin(v_pos%03u);\n", act_level - 1);
        printf("v_pos%03u := bat(void,oid,count(new_v_pos)*2);\n", act_level);
        printf("v_pos%03u.seqbase(0@0);\n", act_level);
        printf("v_pos%03u.access(BAT_APPEND);\n", act_level);
        printf("v_pos%03u.insert(new_v_pos);\n", act_level);
        printf("new_v_pos := nil;\n");

        printf("var new_v_item := oidNew_expOid.leftjoin(v_item%03u);\n", act_level - 1);
        printf("v_item%03u := bat(void,oid,count(new_v_item)*2);\n", act_level);
        printf("v_item%03u.seqbase(0@0);\n", act_level);
        printf("v_item%03u.access(BAT_APPEND);\n", act_level);
        printf("v_item%03u.insert(new_v_item);\n", act_level);
        printf("new_v_item := nil;\n");

        printf("var new_v_kind := oidNew_expOid.leftjoin(v_kind%03u);\n", act_level - 1);
        printf("v_kind%03u := bat(void,chr,count(new_v_kind)*2);\n", act_level);
        printf("v_kind%03u.seqbase(0@0);\n", act_level);
        printf("v_kind%03u.access(BAT_APPEND);\n", act_level);
        printf("v_kind%03u.insert(new_v_kind);\n", act_level);
        printf("new_v_kind := nil;\n");

        /* should work now with leftjoin !?!
        printf("# sort inside join ()\n");
        printf("var temp1 := v_iter%03u.reverse;\n",act_level);
        printf("temp1 := temp1.sort;\n");
        printf("temp1 := temp1.reverse;\n");
        printf("temp1 := temp1.CTrefine(v_pos%03u);\n",act_level);
        printf("temp1 := temp1.mark(0@0);\n");
        printf("temp1 := temp1.reverse;\n");
        printf("v_vid%03u := temp1.join(v_vid%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_iter%03u := temp1.join(v_iter%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_pos%03u := temp1.join(v_pos%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_item%03u := temp1.join(v_item%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_kind%03u := temp1.join(v_kind%03u).access(BAT_APPEND);\n",act_level,act_level);
        */
        /*
        printf("print (\"testoutput in join() expanded to level %i\");\n",act_level);
        printf("print (v_vid%03u, v_iter%03u, v_pos%03u, v_kind%03u);\n",act_level,act_level,act_level,act_level);
        */
    }

    /**
     * map the result back to the next outer scope
     */
    static void
    mapBack (void)
    {
        printf("{ # mapBack ()\n");
        /* the iters are mapped back to the next outer scope */
        printf("var iter_oidMap := inner%03u.reverse;\n", act_level);
        printf("var oid_oidMap := iter.leftfetchjoin(iter_oidMap);\n");
        printf("iter_oidMap := nil;\n");
        printf("iter := oid_oidMap.leftfetchjoin(outer%03u);\n", act_level);
        printf("oid_oidMap := nil;\n");
        /* FIXME: instead of mark the partitioned mark should be used */
        /* printf("pos := pos.mark(1@0);\n"); */
        printf("pos := iter.mark_grp(iter.reverse.project(1@0));\n");
        printf("item := item;\n");
        printf("kind := kind;\n");
        printf("} # end of mapBack ()\n");
    }

    /**
      * if join is pruned the variables for the next scope have to be
      * initialized without content instead
      */
    static void
    createNewVarTable (void)
    {
        printf("# createNewVarTable ()\n");
        printf("v_iter%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_vid%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_pos%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_item%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_kind%03u := bat(void,int).seqbase(0@0).access(BAT_APPEND);\n", act_level);
    }

    /**
     * appends the information of a variable to the 
     * corresponding column of the variable environment
     */
    static void
    append (char *name, int level)
    {
        printf("{ # append (%s, level)\n", name);
        printf("var seqb := oid(v_%s%03u.count);\n",name, level);
        printf("var temp_%s := %s.reverse.mark(seqb).reverse;\n", name, name);
        printf("seqb := nil;\n");
        printf("v_%s%03u.insert(temp_%s);\n", name, level, name);
        printf("temp_%s := nil;\n", name);
        printf("} # append (%s, level)\n", name);
    }

    /**
     * inserts a variable binding into the variable environment
     * of the actual level (scope)
     */
    static void
    insertVar (int vid)
    {
        printf("{ # insertVar (vid)\n");
        printf("var vid := iter.project(%i@0);\n", vid);

        append ("vid", act_level);
        append ("iter", act_level);
        append ("pos", act_level);
        append ("item", act_level);
        append ("kind", act_level);
        printf("vid := nil;\n");

        /*
        printf("print (\"testoutput in insertVar(%i@0) expanded to level %i\");\n", vid, act_level);
        printf("print (v_vid%03u, v_iter%03u, v_pos%03u, v_kind%03u);\n",act_level,act_level,act_level,act_level);
        */
        printf("} # insertVar (vid)\n");
    }

    /**
     * loop-lifting of a Constant
     * - before calling a variable 'itemID' with an oid
     *   has to be bound
     * @param kind the kind of the item
     */
    static void
    translateConst (char *kind)
    {
        printf("# translateConst (kind)\n");
        printf("iter := loop%03u;\n",act_level);
        printf("iter := iter.reverse.mark(0@0).reverse;\n");
        printf("pos := iter.project(1@0);\n");
        printf("item := iter.project(itemID);\n");
        printf("kind := iter.project(%s);\n", kind);
    }

    /**
     * iterative loop-lifted staircasejoin
     */
    static void
    loop_liftedSCJ (char *axis, char *kind, char *ns, char *loc)
    {
        /* iter|pos|item input contains only nodes (kind=NODE) */
        printf("# loop_liftedSCJ (axis, kind, ns, loc)\n");

        if (!strcmp (axis, "attribute"))
        {
                printf("{ # attribute axis\n");
                /* get all unique iter|item combinations */
                printf("var unq := CTgroup(iter).CTgroup(item)"
                                 ".CTgroup(kind).tunique.mark(0@0).reverse;\n");
                /* if unique destroys the order a sort is needed */
                /* printf("iter_item := iter_item.sort;\n"); */

                printf("var oid_iter := unq.leftfetchjoin(iter);\n");
                printf("var oid_item := unq.leftfetchjoin(item);\n");
                printf("var oid_frag := unq.leftfetchjoin(kind.get_fragment);\n");
                printf("unq := nil;\n");

                printf("var temp1 := mvaljoin (oid_item, oid_frag, ws.fetch(ATTR_OWN));\n");
                printf("oid_item := nil;\n");
                printf("oid_frag := temp1.mark(0@0).reverse.leftfetchjoin(oid_frag);\n");
                printf("var oid_attr := temp1.reverse.mark(0@0).reverse;\n");
                printf("oid_iter := temp1.mark(0@0).reverse.leftfetchjoin(oid_iter);\n");
                printf("temp1 := nil;\n");

                /* kind test could be necessary if qnames are saved together
                   (see tagname test for other axis) */
                if (ns)
                {
                        printf("temp1 := mposjoin(mposjoin(oid_attr, oid_frag, ws.fetch(ATTR_QN)), "
                                                 "mposjoin(oid_attr, oid_frag, ws.fetch(ATTR_FRAG)), "
                                                 "ws.fetch(QN_NS));\n");
                        printf("temp1 := temp1.ord_uselect(\"%s\");\n", ns);
                        printf("temp1 := temp1.mark(0@0).reverse;\n");
                        printf("oid_attr := temp1.leftfetchjoin(oid_attr);\n");
                        printf("oid_frag := temp1.leftfetchjoin(oid_frag);\n");
                        printf("oid_iter := temp1.leftfetchjoin(oid_iter);\n");
                        printf("temp1 := nil;\n");
                }
                if (loc)
                {
                        printf("temp1 := mposjoin(mposjoin(oid_attr, oid_frag, ws.fetch(ATTR_QN)), "
                                                 "mposjoin(oid_attr, oid_frag, ws.fetch(ATTR_FRAG)), "
                                                 "ws.fetch(QN_LOC));\n");
                        printf("temp1 := temp1.ord_uselect(\"%s\");\n", loc);
                        printf("temp1 := temp1.mark(0@0).reverse;\n");
                        printf("oid_attr := temp1.leftfetchjoin(oid_attr);\n");
                        printf("oid_frag := temp1.leftfetchjoin(oid_frag);\n");
                        printf("oid_iter := temp1.leftfetchjoin(oid_iter);\n");
                        printf("temp1 := nil;\n");
                }

                /* add '.reverse.mark(0@0).reverse' to be sure that the head of 
                   the results is void */
                printf("res_scj := bat(void,bat).seqbase(0@0);\n");
                printf("res_scj.insert(nil, oid_iter.reverse.mark(0@0).reverse);\n");
                printf("oid_iter := nil;\n");
                printf("res_scj.insert(nil, oid_attr.reverse.mark(0@0).reverse);\n");
                printf("oid_attr := nil;\n");
                printf("res_scj.insert(nil, oid_frag.reverse.mark(0@0).reverse);\n");
                printf("oid_frag := nil;\n");
                printf("temp1 := nil;\n");

                printf("} # end of attribute axis\n");
        }
        else
        {
                /* FIXME: in case iter is not sorted do it to be sure ?!? */
                /* FIXME: this should be resolved by pf:distinct-doc-order */
                /*
                printf("var sorting := iter.reverse.sort.reverse.mark(0@0).reverse;\n");
                printf("iter := sorting.join(iter);\n");
                printf("item := sorting.join(item);\n");
                printf("sorting := nil;\n");
                */
                if (kind)
                {
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step", axis);
                        printf("_with_kind_test_joined("
                               "iter, item, kind.get_fragment, ws, %s);\n", kind);
                }
                else if (ns && loc)
                {
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step", axis);
                        printf("_with_nsloc_test_joined("
                               "iter, item, kind.get_fragment, "
                               "ws, \"%s\", \"%s\");\n", ns, loc);
                }
                else if (loc)
                {
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step", axis);
                        printf("_with_loc_test_joined("
                               "iter, item, kind.get_fragment, "
                               "ws, \"%s\");\n", loc);
                }
                else if (ns)
                {
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step", axis);
                        printf("_with_ns_test_joined("
                               "iter, item, kind.get_fragment, "
                               "ws, \"%s\");\n", ns);
                }
                else
                {
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step_joined("
                               "iter, item, kind.get_fragment, ws);\n", axis);
                }

        }
    }


    static void
    translateLocsteps (PFcnode_t *c)
    {
        char *ns, *loc;
        char *axis = (char *) PFmalloc (sizeof ("descendant_or_self"));

        printf("{ # translateLocsteps (c)\n");

        /* variable for the (iterative) scj */
        printf("var res_scj := empty_res_bat;");

        /* make this path step only for nodes */
        printf("var sel_ls := kind.get_type(NODE);\n");
        printf("sel_ls := sel_ls.mark(0@0).reverse;\n");
        printf("item := sel_ls.leftfetchjoin(item);\n");
        printf("iter := sel_ls.leftfetchjoin(iter);\n");
        printf("kind := sel_ls.leftfetchjoin(kind);\n");
        printf("sel_ls := nil;\n");

        switch (c->kind)
        {
                case c_ancestor:
                    axis = "ancestor";
                    break;
                case c_ancestor_or_self:
                    axis = "ancestor_or_self";
                    break;
                case c_attribute:
                    axis = "attribute";
                    break;
                case c_child:
                    axis = "child";
                    break;
                case c_descendant:
                    axis = "descendant";
                    break;
                case c_descendant_or_self:
                    axis = "descendant_or_self";
                    break;
                case c_following:
                    axis = "following";
                    break;
                case c_following_sibling:
                    axis = "following_sibling";
                    break;
                case c_parent:
                    axis = "parent";
                    break;
                case c_preceding:
                    axis = "preceding";
                    break;
                case c_preceding_sibling:
                    axis = "preceding_sibling";
                    break;
                case c_self:
                    axis = "attribute";
                    break;
                default:
                    PFoops (OOPS_FATAL, "illegal XPath axis in MIL-translation");
        }

        switch (c->child[0]->kind)
        {
                case c_namet:
                    ns = c->child[0]->sem.qname.ns.uri;
                    loc = c->child[0]->sem.qname.loc;

                    /* translate wildcard '*' as 0 and missing ns as "" */
                    if (!ns)
                        ns = "";
                    else if (!strcmp (ns,"*"))
                        ns = 0;

                    /* translate wildcard '*' as 0 */
                    if (loc && (!strcmp(loc,"*")))
                        loc = 0;

                    loop_liftedSCJ (axis, 0, ns, loc); 
                    break;
                case c_kind_node:
                    loop_liftedSCJ (axis, 0, 0, 0);
                    break;
                case c_kind_comment:
                    loop_liftedSCJ (axis, "COMMENT", 0, 0);
                    break;
                case c_kind_text:
                    loop_liftedSCJ (axis, "TEXT", 0, 0);
                    break;
                case c_kind_pi:
                    loop_liftedSCJ (axis, "PI", 0, 0);
                    break;
                case c_kind_doc:
                    loop_liftedSCJ (axis, "DOCUMENT", 0, 0);
                    break;
                case c_kind_elem:
                    loop_liftedSCJ (axis, "ELEMENT", 0, 0);
                    break;
                case c_kind_attr:
                    loop_liftedSCJ (axis, "ATTRIBUTE", 0, 0);
                    break;
                default:
                    PFoops (OOPS_FATAL, "illegal node test in MIL-translation");
                    break;
        }

        /* res_scj = iter|item bat */
        printf("iter := res_scj.fetch(0);\n");
        printf("pos := iter.mark_grp(iter.tunique.project(1@0));\n");
        printf("item := res_scj.fetch(1);\n");
        if (!strcmp (axis, "attribute"))
                printf("kind := res_scj.fetch(2).get_kind(ATTR);\n");
        else
                printf("kind := res_scj.fetch(2).get_kind(NODE);\n");

        printf("res_scj := nil;\n");
        printf("} # end of translateLocsteps (c)\n");
    }

    /**
     * changes item and inserts if needed
     * the int values to 'int_values'
     */
    static void
    createEnumeration (void)
    {
        printf("{ # createEnumeration ()\n");
        /* the head of item has to be void with the seqbase 0@0 */
        printf("var ints_cE := item.mirror.[int];\n");
        /* the original version is replaced (see beneath) */
        printf("int_values.insert(ints_cE);\n");
        /* FIXME: it's not 100% sure, that order is not changed and so
                 mark could have a negative effect and switch values */
        printf("int_values := int_values.reverse.mark(0@0).reverse;\n");
        printf("int_values.seqbase(0@0).access(BAT_WRITE);\n");
        printf("int_values.reverse.key(true);\n");
        /* compares the integers and gives back the ones not in
           the int_values bat */
        /*
        printf("var new_ints := ints_cE.reverse.kdiff(int_values.reverse);\n");
        printf("seqb := oid(int_values.count);\n");
        printf("new_ints := new_ints.mark(seqb).reverse;\n");
        printf("seqb := nil;\n");
        */
        /* add the new integers to the int_values bat */
        /*
        printf("int_values.insert(new_ints);\n");
        printf("new_ints := nil;\n");
        */
        /* get the oids for the integers */
        printf("item := ints_cE.leftjoin(int_values.reverse);\n");
        printf("ints_cE := nil;\n");
        /* change kind information to int */
        printf("kind := kind.project(INT);\n");
        printf("} # end of createEnumeration ()\n");
    }

    /**
     * loop-lifted conversion of the element name from a string
     * or a text node to a qname (can't handle namespaces) 
     */
    static void
    getQName (void)
    {
        printf("{ # getQName ()\n");

        /* FIXME: is it possible to have more than one kind */
        /* test if the name consists only of QNames */
/*
        printf("var qnames := kind.get_type(QNAME);\n");
        printf("var all_s2q := kind.count;\n");
        printf("var qn_s2q := qnames.count;\n");


        printf("if (all_s2q != qn_s2q) {\n");

        printf("        var nodes := kind.get_type(NODE).mark(0@0).reverse;\n");
        printf("        var original_oid := nodes;\n");
        printf("        nodes := nodes.leftfetchjoin(item);\n");
        printf("        var frags := nodes.leftfetchjoin(kind).get_fragment;\n");
        printf("        var temp_s2q := mposjoin(nodes, frags, ws.fetch(PRE_KIND));\n");
        printf("        temp_s2q := temp_s2q.ord_uselect(TEXT).mark(0@0).reverse;\n");
        printf("        original_oid := temp_s2q.leftfetchjoin(original_oid);\n");
        printf("        var textNodes := temp_s2q.leftfetchjoin(nodes);\n");
        printf("        frags := temp_s2q.leftfetchjoin(frags);\n");
        printf("        var tN_s2q := textNodes.count;\n");
        printf("        temp_s2q := nil;\n");
        printf("        if (nodes.count != tN_s2q)\n");
        printf("           ERROR (\"no nodes except textnodes allowed in Qname conversion\");\n");
        printf("        nodes := nil;\n");
*/
        printf("        var strings_s2q := kind.ord_uselect(STR);\n");
/*
        printf("        var added := strings_s2q.count + qn_s2q + tN_s2q;\n");
        printf("        if (all_s2q != added) ");
        printf(                 "ERROR (\"only strings, text nodes and qnames can be");
        printf(                 " used as element names\");\n");
        printf("        all_s2q := nil;\n");
        printf("        tN_s2q := nil;\n");
        printf("        added := nil;\n");
        printf("        qn_s2q := nil;\n");
*/
        /* end of kind testing */
/*
        printf("        textNodes := mposjoin(mposjoin(textNodes, frags, ws.fetch(PRE_PROP)), "
                                             "mposjoin(textNodes, frags, ws.fetch(PRE_FRAG)), "
                                             "ws.fetch(PROP_TEXT));\n");
*/
        printf("        var oid_oid := strings_s2q.mark(0@0).reverse;\n");
        printf("        strings_s2q := nil;\n");
        /* get all the unique strings */
        printf("        var oid_item := oid_oid.leftfetchjoin(item);\n");
        /* FIXME: is the copy next line really needed? */

        printf("        var oid_item_backup := oid_item.copy;\n");
        printf("        oid_item := oid_item.tunique.mark(0@0).reverse;\n");
        printf("        var oid_str := oid_item.leftfetchjoin(str_values);\n");
        printf("        oid_item := nil;\n");

        /* string name is only translated into local name, because
           no URIs for the namespace are available */
        printf("        var prop_name := ws.fetch(QN_NS).fetch(WS).ord_uselect(\"\");\n");

        printf("        prop_name := prop_name.mirror.leftfetchjoin(ws.fetch(QN_LOC).fetch(WS));\n");

        /* find all strings which are not in the qnames of the WS */
/*
        printf("        oid_str := oid_str.union(textNodes.tunique.reverse);\n");
*/
        printf("        var str_oid := oid_str.reverse.kdiff(prop_name.reverse);\n");
        printf("        oid_str := nil;\n");
        printf("        prop_name := nil;\n");
        printf("        oid_str := str_oid.mark(oid(ws.fetch(QN_LOC).fetch(WS).count)).reverse;\n");
        printf("        str_oid := nil;\n");
        /* add the strings as local part of the qname into the working set */
        printf("        ws.fetch(QN_LOC).fetch(WS).insert(oid_str);\n");
        printf("        oid_str := oid_str.project(\"\");\n");
        printf("        ws.fetch(QN_NS).fetch(WS).insert(oid_str);\n");
        printf("        oid_str := nil;\n");

        /* get all the possible matching names from the updated working set */
        printf("        prop_name := ws.fetch(QN_NS).fetch(WS).ord_uselect(\"\");\n");
        printf("        prop_name := prop_name.mirror.leftfetchjoin(ws.fetch(QN_LOC).fetch(WS));\n");

        printf("        oid_str := oid_oid.leftfetchjoin(oid_item_backup);\n");
        printf("        oid_oid := nil;\n");
        printf("        oid_str := oid_str.leftfetchjoin(str_values);\n");
        printf("        oid_item_backup := nil;\n");
/*
        printf("        var res_mu := merged_union(textNodes.mark(0@0).reverse.leftfetchjoin(original_oid), "
                                               "oid_str.mark(0@0).reverse, "
                                               "textNodes, oid_str);\n");
        printf("        oid_str := res_mu.fetch(1);\n");
*/
        /* get property ids for each string */
        printf("        var oid_prop := oid_str.leftjoin(prop_name.reverse);\n");
        printf("        oid_str := nil;\n");
        printf("        prop_name := nil;\n");
        /* oid_prop now contains the items with property ids
           which were before strings */
        /* FIXME: see fixme above: if only one kind is possible 
           this is overhead */
/*
        printf("        if (qnames.count = 0)\n");
        printf("                item := oid_prop;\n");
        printf("        else {\n");
*/
        /* qnames and newly generated qnames are merged (first 2 parameters are
           the oids for the sorting) */
/*
        printf("                item := merged_union(res_mu.fetch(0), "
                                                    "qnames.mark(0@0).reverse, "
                                                    "oid_prop.reverse.mark(0@0).reverse, "
                                                    "qnames.reverse.mark(0@0).reverse)"
                                                    ".fetch(1);\n");
*/
/* after removing everything now adding one extra line */
        printf("item := oid_prop;\n");
        printf("                item := item.reverse.mark(0@0).reverse;\n");
/*
        printf("                ");
        printf("        }\n");
*/
        printf("        kind := item.project(QNAME);\n");
        printf("        oid_prop := nil;\n");
/*
        printf("        qnames := nil;\n");
        printf("}\n");
*/
        printf("} # end of getQName ()\n");
    }

    /**
     * loop-lifted element construction 
     * @param i the counter of the actual saved result (elem name)
     */
    static void
    loop_liftedElemConstr (int i)
    {
        printf("{ # loop_liftedElemConstr (counter)\n"
               "var root_level;\n"
               "var root_size;\n"
               "var root_kind;\n"
               "var root_frag;\n"
               "var root_prop;\n");

        /* attr */ printf("var preNew_preOld;\n");
        /* attr */ printf("var preNew_frag;\n");
        /* attr */ printf("var attr := kind.get_type(ATTR).mark(0@0).reverse;\n");
        /* attr */ printf("var attr_iter := attr.leftfetchjoin(iter);\n");
        /* attr */ printf("var attr_item := attr.leftfetchjoin(item);\n");
        /* attr */ printf("var attr_frag := attr.leftfetchjoin(kind).get_fragment;\n");
        /* attr */ printf("attr := nil;\n");

        /* in a first version only nodes are handled */
        printf("var nodes := kind.get_type(NODE);\n");
        /* if no nodes are found we jump right to the end and only
           have to execute the stuff for the root construction */
        printf("if (nodes.count != 0) {\n");
        
        printf("var oid_oid := nodes.mark(0@0).reverse;\n"
               "nodes := nil;\n"
               "var node_items := oid_oid.leftfetchjoin(item);\n"
               "var node_frags := oid_oid.leftfetchjoin(kind).get_fragment;\n"
               /* set iter to a distinct list and therefore don't
                  prune any node */
               "var iter_input := oid_oid.mirror;\n");

        /* get all subtree copies */
        printf("var res_scj := "
               "loop_lifted_descendant_or_self_step_unjoined"
               "(iter_input, node_items, node_frags, ws);\n");

        printf("iter_input := nil;\n");
        /* variables for the result of the scj */
        printf("var pruned_input := res_scj.fetch(0);\n"
                /* pruned_input comes as ctx|iter */
               "var ctx_dn_item := res_scj.fetch(1);\n"
               "var ctx_dn_frag := res_scj.fetch(2);\n"
               "res_scj := nil;\n"
        /* res_ec is the iter|dn table resulting from the scj */
               "var res_item := pruned_input.reverse.leftjoin(ctx_dn_item);\n"
        /* create content_iter as sorting argument for the merged union */
               "var content_void := res_item.mark(0@0).reverse;\n"
               "var content_iter := content_void.leftfetchjoin(oid_oid).leftfetchjoin(iter);\n"
               "content_void := nil;\n"
        /* only the dn_items and dn_frags from the joined result are needed
           in the following (getting the values for content_size, 
           content_prop, ...) and the input for a mposjoin has to be void */
               "res_item := res_item.reverse.mark(0@0).reverse;\n"
               "var res_frag := pruned_input.reverse.leftjoin(ctx_dn_frag);\n"
               "res_frag := res_frag.reverse.mark(0@0).reverse;\n");

        /* create subtree copies for all bats except content_level */
        printf("var content_size := mposjoin(res_item, res_frag, "
                                            "ws.fetch(PRE_SIZE));\n"
               "var content_prop := mposjoin(res_item, res_frag, "
                                            "ws.fetch(PRE_PROP));\n"
               "var content_kind := mposjoin(res_item, res_frag, "
                                            "ws.fetch(PRE_KIND));\n"
               "var content_frag := mposjoin(res_item, res_frag, "
                                            "ws.fetch(PRE_FRAG));\n");

        /* attr */ /* content_pre is needed for attribute subtree copies */
        /* attr */ printf("var content_pre := res_item;\n");
        /* attr */ printf("res_item := nil;\n");
        /* attr */ printf("res_frag := nil;\n");

        /* change the level of the subtree copies */
        /* get the level of the content root nodes */
        /* - unique is needed, if pruned_input has more than once an ctx value
           - join with iter between pruned_input and item is not needed, because
           in this case pruned_input has the void column as iter value */
        printf("nodes := pruned_input.kunique;\n" /* creates unique ctx-node list */
               "var temp_ec_item := nodes.reverse.mark(0@0).reverse;\n"
               "temp_ec_item := temp_ec_item.leftfetchjoin(node_items);\n"
               "var temp_ec_frag := nodes.reverse.mark(0@0).reverse;\n"
               "temp_ec_frag := temp_ec_frag.leftfetchjoin(node_frags);\n"
               "nodes := nodes.mark(0@0);\n"
               "var contentRoot_level := mposjoin(temp_ec_item, "
                                                 "temp_ec_frag, "
                                                 "ws.fetch(PRE_LEVEL));\n"
               "contentRoot_level := nodes.leftfetchjoin(contentRoot_level);\n"
               "temp_ec_item := nil;\n"
               "temp_ec_frag := nil;\n"
               "nodes := nil;\n"

               "temp_ec_item := ctx_dn_item.reverse.mark(0@0).reverse;\n"
               "temp_ec_frag := ctx_dn_frag.reverse.mark(0@0).reverse;\n"
               "nodes := ctx_dn_item.mark(0@0);\n"
               "var content_level := mposjoin(temp_ec_item, temp_ec_frag, "
                                             "ws.fetch(PRE_LEVEL));\n"
               "content_level := nodes.leftfetchjoin(content_level);\n"
               "content_level := content_level.[-](contentRoot_level);\n"
               "contentRoot_level := nil;\n"
               "content_level := content_level.[+](chr(1));\n"
        /* join is made after the multiplex, because the level has to be change only
           once for each dn-node. With the join the multiplex is automatically
           expanded */
               "content_level := pruned_input.reverse.leftjoin(content_level);\n"
               "content_level := content_level.reverse.mark(0@0).reverse;\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"content\");\n");
        printf("print(content_iter, content_size, [int](content_level), ");
        printf("[int](content_kind), content_prop, content_pre);\n");
        */

        /* get the maximum level of the new constructed nodes
           and set the maximum of the working set */
        printf("{\n");
        printf("var height := int(content_level.max) + 1;\n");
        printf("ws.fetch(HEIGHT).replace(WS, max(ws.fetch(HEIGHT).fetch(WS), height));\n");
        printf("height := nil;\n");
        printf("}\n");

        /* calculate the sizes for the root nodes */
        printf("var contentRoot_size := mposjoin(node_items, node_frags, "
                                                "ws.fetch(PRE_SIZE)).[+](1);\n"
               "var size_oid := contentRoot_size.reverse;\n"
               "contentRoot_size := nil;\n"
               "size_oid := size_oid.leftfetchjoin(oid_oid);\n"
               "oid_oid := nil;\n"
               "var size_iter := size_oid.leftfetchjoin(iter);\n"
               "size_oid := nil;\n"
               "var iter_size := size_iter.reverse;\n"
               "size_iter := nil;\n"
        /* sums up all the sizes into an size for each iter */
        /* every element must have a name, but elements don't need content.
           Therefore the second argument of the grouped sum has to be from the
           names result */
               "iter_size := {sum}(iter_size, iter%03u.tunique);\n", i);

        printf("root_level := iter_size.project(chr(0));\n"
               "root_size := iter_size;\n"
               "root_kind := iter_size.project(ELEMENT);\n"
               "root_prop := iter%03u.reverse.leftfetchjoin(item%03u);\n"
               "root_frag := iter_size.project(WS);\n", i, i);

        printf("root_level := root_level.reverse.mark(0@0).reverse;\n"
               "root_size := root_size.reverse.mark(0@0).reverse;\n"
               "root_kind := root_kind.reverse.mark(0@0).reverse;\n"
               "root_prop := root_prop.reverse.mark(0@0).reverse;\n"
               "root_frag := root_frag.reverse.mark(0@0).reverse;\n"
               "var root_iter := iter_size.mark(0@0).reverse;\n"
               "iter_size := nil;\n");

        /* attr */ /* root_pre is a dummy needed for merge union with content_pre */
        /* attr */ printf("var root_pre := root_iter.project(nil);\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"root\");\n");
        printf("print(root_iter, root_size, [int](root_level), [int](root_kind), root_prop);\n");
        */

        /* merge union root and nodes */
        printf("{\n");
        printf("var merged_result := merged_union ("
               "root_iter, content_iter, root_size, content_size, "
               "root_level, content_level, root_kind, content_kind, "
               "root_prop, content_prop, root_frag, content_frag, "
               "root_pre, content_pre);\n");
        printf("root_iter := nil;\n"
               "content_iter := nil;\n"
               "root_size := merged_result.fetch(1);\n"
               "content_size := nil;\n"
               "root_level := merged_result.fetch(2);\n"
               "content_level := nil;\n"
               "root_kind := merged_result.fetch(3);\n"
               "content_kind := nil;\n"
               "root_prop := merged_result.fetch(4);\n"
               "content_prop := nil;\n"
               "root_frag := merged_result.fetch(5);\n"
               "content_frag := nil;\n"
               "root_pre := merged_result.fetch(6);\n"
               "content_pre := nil;\n"
               "merged_result := nil;\n");
        /* printing output for debugging purposes */
        /* 
        printf("merged_result.print;\n");
        printf("print(\"merged (root & content)\");\n"
               "print(root_size, [int](root_level), [int](root_kind), root_prop);\n");
        */
        printf("}\n");
       
        
        /* attr */ /* preNew_preOld has in the tail old pre
           values merged with nil values */
        /* attr */ printf("preNew_preOld := root_pre;\n");
        /* attr */ printf("root_pre := nil;\n");

        printf("} else { # if (nodes.count != 0) ...\n");

        printf("root_level := item%03u.project(chr(0));\n", i);
        printf("root_size := item%03u.project(0);\n", i);
        printf("root_kind := item%03u.project(ELEMENT);\n", i);
        printf("root_prop := item%03u;\n", i);
        printf("root_frag := item%03u.project(WS);\n", i);

        /* attr */ printf("preNew_preOld := item%03u.project(nil);\n", i);
        /* attr */ printf("preNew_preOld := preNew_preOld.reverse.mark(0@0).reverse;\n");

        printf("root_level := root_level.reverse.mark(0@0).reverse;\n"
               "root_size := root_size.reverse.mark(0@0).reverse;\n"
               "root_kind := root_kind.reverse.mark(0@0).reverse;\n"
               "root_prop := root_prop.reverse.mark(0@0).reverse;\n"
               "root_frag := root_frag.reverse.mark(0@0).reverse;\n");

        printf("} # end of else in 'if (nodes.count != 0)'\n");

        /* set the offset for the new created trees */
        printf("{\n"
               "var seqb := count(ws.fetch(PRE_SIZE).fetch(WS));\n"
               "seqb := oid(seqb);\n"
               "root_level.seqbase(seqb);\n"
               "root_size.seqbase(seqb);\n"
               "root_kind.seqbase(seqb);\n"
               "root_prop.seqbase(seqb);\n"
               "root_frag.seqbase(seqb);\n"
               /* get the new pre values */
        /* attr */ "preNew_preOld.seqbase(seqb);\n"
        /* attr */ "preNew_frag := root_frag;\n"
               "}\n");
        /* insert the new trees into the working set */
        printf("ws.fetch(PRE_LEVEL).fetch(WS).insert(root_level);\n"
               "ws.fetch(PRE_SIZE).fetch(WS).insert(root_size);\n"
               "ws.fetch(PRE_KIND).fetch(WS).insert(root_kind);\n"
               "ws.fetch(PRE_PROP).fetch(WS).insert(root_prop);\n"
               "ws.fetch(PRE_FRAG).fetch(WS).insert(root_frag);\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"actual working set\");\n");
        printf("print(Tpre_size, [int](Tpre_level), [int](Tpre_kind), Tpre_prop);\n");
        */

        /* save the new roots for creation of the intermediate result */
        printf("var roots := root_level.ord_uselect(chr(0));\n");
        printf("roots := roots.mark(0@0).reverse;\n");

        /* resetting the temporary variables */
        printf("root_level := nil;\n"
               "root_size := nil;\n"
               "root_prop := nil;\n"
               "root_kind := nil;\n"
               "root_frag := nil;\n");

        /* adding the new constructed roots to the WS_FRAG bat of the working
           set, that a following (preceding) step can check the fragment
           boundaries */
        printf("{ # adding new fragments to the WS_FRAG bat\n"
               "var seqb := ws.fetch(WS_FRAG).count;\n"
               "seqb := oid(seqb);\n"
               "var new_pres := roots.reverse.mark(seqb).reverse;\n"
               "seqb := nil;\n"
               "ws.fetch(WS_FRAG).insert(new_pres);\n"
               "new_pres := nil;\n"
               "}\n");

        /* return the root elements in iter|pos|item|kind representation */
        /* should contain for each iter exactly 1 root element
           unless there is a thinking error */
        printf("iter := iter%03u;\n", i);
        printf("pos := roots.mark(0@0);\n");
        printf("item := roots;\n");
        printf("kind := roots.project(NODE);\n");

        /* attr translation */
        /* actually there are three variables bound:
           preNew_preOld - for mapping new attributes to new pre values
           preNew_frag - to save the fragments, where the old pre values are from
           attrIDs - the content root attributes
        */
        /* 1. step: add subtree copies of attributes */
        printf("{ # create attribute subtree copies\n");
        /* get the attributes of the subtree copy elements */
        /* because also nil values from the roots are used for matching
           and 'select(nil)' inside mvaljoin gives back all the attributes
           not bound to a pre value first all root pre values have to
           be thrown out */
        printf("var content_preNew_preOld := preNew_preOld.ord_select(nil,nil);\n"
               "var oid_preOld := content_preNew_preOld.reverse.mark(0@0).reverse;\n"
               "var oid_preNew := content_preNew_preOld.mark(0@0).reverse;\n"
               "var oid_frag := oid_preNew.leftfetchjoin(preNew_frag);\n");
        printf("var temp_attr := mvaljoin(oid_preOld, oid_frag, ws.fetch(ATTR_OWN));\n");
        printf("oid_preOld := nil;\n");
        printf("oid_attr := temp_attr.reverse.mark(0@0).reverse;\n");
        printf("oid_frag := temp_attr.mark(0@0).reverse.leftfetchjoin(oid_frag);\n");
        printf("oid_preNew := temp_attr.mark(0@0).reverse.leftfetchjoin(oid_preNew);\n");
        printf("temp_attr := nil;\n");

        printf("var seqb := oid(ws.fetch(ATTR_QN).fetch(WS).count);\n");

        /* get the values of the QN/OID offsets for the reference to the
           string values */
        printf("var attr_qn := mposjoin(oid_attr, oid_frag, ws.fetch(ATTR_QN));\n");
        printf("attr_qn.seqbase(seqb);\n");
        printf("var attr_oid := mposjoin(oid_attr, oid_frag, ws.fetch(ATTR_PROP));\n");
        printf("attr_oid.seqbase(seqb);\n");
        printf("oid_preNew.seqbase(seqb);\n");
        printf("oid_frag.seqbase(seqb);\n");
        printf("seqb := nil;\n");
        
        /* insert into working set WS the attribute subtree copies 
           only 'offsets' where to find strings are copied 
           (QN/FRAG, OID/FRAG) */
        printf("ws.fetch(ATTR_QN).fetch(WS).insert(attr_qn);\n");
        printf("ws.fetch(ATTR_PROP).fetch(WS).insert(attr_oid);\n");
        printf("ws.fetch(ATTR_OWN).fetch(WS).insert(oid_preNew);\n");
        printf("ws.fetch(ATTR_FRAG).fetch(WS).insert(oid_frag);\n");

        printf("} # end of create attribute subtree copies\n");

        /* 2. step: add attribute bindings of new root nodes */
        printf("{ # create attribute root entries\n");
        /* use iter, qn and frag to find unique combinations */
        printf("var unq_attrs := CTgroup(attr_iter)"
                                ".CTgroup(mposjoin(attr_item, attr_frag, ws.fetch(ATTR_QN)))"
                                ".CTgroup(mposjoin(attr_item, attr_frag, ws.fetch(ATTR_FRAG)))"
                                ".tunique;\n");
        /* test uniqueness */
        printf("if (unq_attrs.count != attr_iter.count)\n"
               "{\n"
               "   if (item%03u.count > 0)\n"
               "      ERROR (\"attributes are not unique in element"
               " construction of '%%s' within each iter\",\n"
               "             item%03u.leftfetchjoin(ws.fetch(QN_LOC).fetch(WS)).fetch(0));\n"
               "   else\n"
               "     ERROR (\"attributes are not unique in element"
               " construction within each iter\");\n"
               "}\n", i, i);

        /* insert it into the WS after everything else */
        printf("var seqb := oid(ws.fetch(ATTR_QN).fetch(WS).count);\n");
        /* get old QN reference and copy it into the new attribute */
        printf("var attr_qn := mposjoin(attr_item, attr_frag, ws.fetch(ATTR_QN));\n"
               "attr_qn.seqbase(seqb);\n");
        /* get old OID reference and copy it into the new attribute */
        printf("var attr_oid := mposjoin(attr_item, attr_frag, ws.fetch(ATTR_PROP));\n"
               "attr_oid.seqbase(seqb);\n");
        /* get the iters and their corresponding new pre value (roots) and
           multiply them for all the attributes */
        printf("var attr_own := iter%03u.reverse.leftfetchjoin(roots);\n"
               "roots := nil;\n"
               "attr_own := attr_iter.leftjoin(attr_own);\n"
               "attr_iter := nil;\n"
               "attr_own := attr_own.reverse.mark(seqb).reverse;\n", i);
        /* use the old FRAG values as reference */
        printf("attr_frag.seqbase(seqb);\n");
        printf("seqb := nil;\n");

        printf("ws.fetch(ATTR_QN).fetch(WS).insert(attr_qn);\n"
               "attr_qn := nil;\n");
        printf("ws.fetch(ATTR_PROP).fetch(WS).insert(attr_oid);\n"
               "attr_oid := nil;\n");
        printf("ws.fetch(ATTR_OWN).fetch(WS).insert(attr_own);\n"
               "attr_own := nil;\n");
        printf("ws.fetch(ATTR_FRAG).fetch(WS).insert(attr_frag);\n"
               "attr_frag := nil;\n");

        printf("} # end of create attribute root entries\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"Theight\"); Theight.print;\n");
        printf("print(\"Tdoc_pre\"); Tdoc_pre.print;\n");
        printf("print(\"Tdoc_name\"); Tdoc_name.print;\n");
        */

        printf("} # end of loop_liftedElemConstr (counter)\n");
    }
   

    /**
     * prints to stdout MIL-expressions for the c_apply core node
     * @param fnQname the name of the function
     * @args the head of the argument list
     */
    static void
    translateFunction (PFqname_t fnQname, PFcnode_t *args)
    {
        if (!PFqname_eq(fnQname,PFqname (PFns_fn,"doc")))
        {
                translate2MIL (args->child[0]);
                /* FIXME: doesn't work with actual attribute representation */
                /* FIXME: only works with strings - no error handling */
                printf("{ # translate fn:doc (string?) as document?\n");
                printf("var docs := item.tunique.mark(0@0).reverse;\n");
                printf("docs := docs.leftfetchjoin(str_values);\n");
                printf("var frag := docs;\n");
                printf("docs := docs.reverse.kdiff(ws.fetch(DOC_LOADED).reverse).mark(0@0).reverse;\n");
                printf("docs@batloop () {\n");
                printf("ws := add_doc(ws, $t);\n");
                printf("}\n");
                printf("docs := nil;\n");
                printf("frag := frag.leftjoin(ws.fetch(DOC_LOADED).reverse);\n");
                printf("frag := frag.reverse.mark(0@0).reverse;\n");
                printf("kind := get_kind(frag, NODE);\n");
                printf("item := kind.project(0@0);\n");
                printf("} # end of translate fn:doc (string?) as document?\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_pf,"distinct-doc-order")))
        {
                translate2MIL (args->child[0]);
                printf("{ # translate pf:distinct-doc-order (node*) as node*\n");
                /* this shouldn't be necessary because we already have only nodes
                printf("var temp_ddo := kind.get_type(NODE).mark(0@0).reverse;\n");
                printf("iter := temp_ddo.leftfetchjoin(iter);\n");
                printf("item := temp_ddo.leftfetchjoin(item);\n");
                printf("var frag := temp_ddo.leftfetchjoin(kind).get_fragment;\n");
                printf("temp_ddo := nil;\n");
                */
                printf("var temp_ddo := CTgroup(iter).CTgroup(item).CTgroup(kind);\n");
                printf("temp_ddo := temp_ddo.tunique.mark(0@0).reverse;\n");
                printf("iter := temp_ddo.leftfetchjoin(iter);\n");
                printf("item := temp_ddo.leftfetchjoin(item);\n");
                printf("kind := temp_ddo.leftfetchjoin(kind);\n");
                printf("temp_ddo := nil;\n");
                printf("var sorting := iter.reverse.sort.reverse;\n");
                printf("sorting := sorting.CTrefine(kind);");
                printf("sorting := sorting.CTrefine(item);");
                printf("sorting := sorting.mark(0@0).reverse;\n");
                printf("iter := sorting.leftfetchjoin(iter);\n");
                printf("pos := iter.mark(1@0);\n");
                printf("item := sorting.leftfetchjoin(item);\n");
                printf("kind := sorting.leftfetchjoin(kind);\n");
                printf("sorting := nil;\n");
                printf("} # end of translate pf:distinct-doc-order (node*) as node*\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"count")))
        {
                translate2MIL (args->child[0]);
                printf("{ # translate fn:count (item*) as integer\n");
                /* counts for all iters the number of items */
                /* uses the actual loop, to collect the iters, which are translated 
                   into empty sequences */
                printf("var iter_count := {count}(iter.reverse,loop%03u.reverse);\n", act_level);
                printf("iter := iter_count.mark(0@0).reverse;\n");
                printf("pos := iter.project(1@0);\n");
                printf("int_values.insert(iter_count);\n");
                /* FIXME: it's not 100% sure, that order is not changed and so
                          mark could have a negative effect and switch values */
                printf("int_values := int_values.reverse.mark(0@0).reverse;\n");
                printf("int_values.seqbase(0@0).access(BAT_WRITE);\n");
                printf("int_values.reverse.key(true);\n");
                /* get the oids for the integers */
                printf("item := iter_count.leftjoin(int_values.reverse);\n");
                printf("item := item.reverse.mark(0@0).reverse;\n"); 
                printf("kind := iter.project(INT);\n");
                printf("iter_count := nil;\n");
                printf("} # end of translate fn:count (item*) as integer\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"empty")))
        {
                translate2MIL (args->child[0]);
                printf("{ # translate fn:empty (item*) as boolean\n");
                printf("var iter_count := {count}(iter.reverse,loop%03u.reverse);\n", act_level);
                printf("var iter_bool := iter_count.[=](0);\n");
                printf("iter_count := nil;\n");
                printf("iter_bool := iter_bool.leftjoin(bool_map);\n");
                printf("iter := iter_bool.mark(0@0).reverse;\n");
                printf("pos := iter.project(1@0);\n");
                printf("item := iter_bool.reverse.mark(0@0).reverse;\n");
                printf("kind := iter.project(BOOL);\n");
                printf("iter_bool := nil;\n");
                printf("} # end of translate fn:empty (item*) as boolean\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"not")))
        {
                translate2MIL (args->child[0]);
                printf("# translate fn:not (boolean) as boolean\n");
                printf("item := item.leftfetchjoin(bool_not);\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"boolean")))
        {
                translate2MIL (args->child[0]);
                
                printf("{ # translate fn:boolean (item*) as boolean\n");
                printf("iter := iter.reverse;\n");
                printf("var iter_count := {count}(iter,loop%03u.reverse);\n", act_level);
                printf("var test := iter_count.[=](1).ord_uselect(true);\n");
                printf("var trues := iter_count.[!=](0);\n");
                printf("trues.access(BAT_WRITE);\n");
                printf("iter_count := nil;\n");
                printf("item := iter.leftfetchjoin(item);\n");
                printf("kind := iter.leftfetchjoin(kind);\n");
                printf("test := test.mirror;\n");
                printf("test := test.leftjoin(kind);\n");
                printf("var str_test := test.ord_uselect(STR);\n");
                printf("var int_test := test.ord_uselect(INT);\n");
                printf("var dbl_test := test.ord_uselect(DBL);\n");
                printf("var dec_test := test.ord_uselect(DEC);\n");
                printf("var bool_test := test.ord_uselect(BOOL);\n");
                printf("test := nil;\n");
                printf("str_test := str_test.mirror;\n");
                printf("int_test := int_test.mirror;\n");
                printf("dbl_test := dbl_test.mirror;\n");
                printf("dec_test := dec_test.mirror;\n");
                printf("bool_test := bool_test.mirror;\n");
                printf("str_test := str_test.leftjoin(item);\n");
                printf("int_test := int_test.leftjoin(item);\n");
                printf("dec_test := dec_test.leftjoin(item);\n");
                printf("dbl_test := dbl_test.leftjoin(item);\n");
                printf("bool_test := bool_test.leftjoin(item);\n");
                printf("str_test := str_test.leftfetchjoin(str_values);\n");
                printf("int_test := int_test.leftfetchjoin(int_values);\n");
                printf("dec_test := dec_test.leftfetchjoin(dec_values);\n");
                printf("dbl_test := dbl_test.leftfetchjoin(dbl_values);\n");
                printf("bool_test := bool_test.ord_uselect(0@0);\n");
                printf("str_test := str_test.ord_uselect(\"\");\n");
                printf("int_test := int_test.ord_uselect(0);\n");
                printf("dec_test := dec_test.ord_uselect(dbl(0));\n");
                printf("dbl_test := dbl_test.ord_uselect(dbl(0));\n");
                printf("str_test := str_test.project(false);\n");
                printf("int_test := int_test.project(false);\n");
                printf("dec_test := dec_test.project(false);\n");
                printf("dbl_test := dbl_test.project(false);\n");
                printf("bool_test := bool_test.project(false);\n");
                printf("trues.replace(str_test);\n");
                printf("str_test := nil;\n");
                printf("trues.replace(int_test);\n");
                printf("int_test := nil;\n");
                printf("trues.replace(dec_test);\n");
                printf("dec_test := nil;\n");
                printf("trues.replace(dbl_test);\n");
                printf("dbl_test := nil;\n");
                printf("trues.replace(bool_test);\n");
                printf("bool_test := nil;\n");
                
                printf("trues := trues.leftjoin(bool_map);\n");
                printf("iter := trues.mark(0@0).reverse;\n");
                printf("pos := iter.project(1@0);\n");
                printf("item := trues.reverse.mark(0@0).reverse;\n");
                printf("kind := iter.project(BOOL);\n");
                printf("trues := nil;\n");
                printf("} # end of translate fn:boolean (item*) as boolean\n");
                
        }
        else translateEmpty ();
    }

    /**
     * loop-lifted attribute construction
     * @param i the counter of the actual saved result (attr name)
     */
    static void
    loop_liftedAttrConstr (int i)
    {
        PFlog("first short version of attribute constructor: can only handle one string or integer per iter");
        printf("{ # loop_liftedAttrConstr (int i)\n");
        printf("var temp_int := kind.ord_uselect(INT);\n");
        printf("temp_int := temp_int.mark(0@0).reverse;\n");
        printf("var oid_int := temp_int;\n");
        printf("temp_int := temp_int.leftfetchjoin(item);\n");
        printf("temp_int := temp_int.leftfetchjoin(int_values);\n");
        printf("temp_int := temp_int.[str];\n");
        printf("var temp_str := kind.ord_uselect(STR);\n");
        printf("temp_str := temp_str.mark(0@0).reverse;\n");
        printf("var oid_str := temp_str;\n");
        printf("temp_str := temp_str.leftfetchjoin(item);\n");
        printf("temp_str := temp_str.leftfetchjoin(str_values);\n");
        printf("var temp_node := kind.get_type(NODE);\n");
        printf("temp_node := temp_node.mark(0@0).reverse;\n");
        /* FIXME: small test */
        printf("var test := {count}(iter.reverse,iter.tunique);\n"
               "if (test.count != test.sum) ERROR (\"more than 1 argument in attribute constructor\");\n");

        printf("var temp_nitem := temp_node.leftfetchjoin(item);\n");
        printf("var temp_nfrag := temp_node.leftfetchjoin(kind).get_fragment;\n");
        printf("var temp_kind := mposjoin(temp_nitem, "
                                         "temp_nfrag, "
                                         "ws.fetch(PRE_KIND));\n");
        printf("temp_kind := temp_kind.ord_uselect(TEXT);\n");
        printf("temp_kind := temp_kind.mark(0@0).reverse;\n");
        printf("temp_node := temp_kind.leftfetchjoin(temp_node);\n");
        printf("temp_nitem := temp_kind.leftfetchjoin(temp_nitem);\n");
        printf("temp_nfrag := temp_kind.leftfetchjoin(temp_nfrag);\n");
        printf("temp_kind := nil;\n");
        printf("var temp_text := mposjoin(mposjoin(temp_nitem, temp_nfrag, ws.fetch(PRE_PROP)), "
                                         "mposjoin(temp_nitem, temp_nfrag, ws.fetch(PRE_FRAG)), "
                                         "ws.fetch(PROP_TEXT));\n");
        printf("var res_mu := merged_union (oid_str, oid_int, temp_str, temp_int);\n");
        printf("res_mu := merged_union (res_mu.fetch(0), temp_node, "
                                       "res_mu.fetch(1), temp_text);\n");
        printf("var attr_val := res_mu.fetch(1);\n");
        printf("temp_str := nil;\n");
        printf("oid_str := nil;\n");
        printf("temp_int := nil;\n");
        printf("oid_int := nil;\n");
        printf("temp_text := nil;\n");
        printf("temp_nitem := nil;\n");
        printf("temp_nfrag := nil;\n");
        printf("temp_node := nil;\n");
        printf("res_mu := nil;\n");

        /* FIXME: rewrite attribute constructor to handle everything */
        printf("if (attr_val.count != item%03u.count) "
               "ERROR (\"number of values and qnames doesn't fit\");\n", i);
        printf("var seqb := oid(ws.fetch(ATTR_OWN).fetch(WS).count);\n");
        printf("var seqb_text := oid(ws.fetch(PROP_VAL).fetch(WS).count);\n");
        /* attr_val contains attribute values */
        printf("attr_val.seqbase(seqb_text);\n");
        printf("ws.fetch(PROP_VAL).fetch(WS).insert(attr_val);\n");
        printf("var attr_oid := attr_val.seqbase(seqb).mark(seqb_text);\n");
        printf("ws.fetch(ATTR_PROP).fetch(WS).insert(attr_oid);\n");
        /* itemXXX contains the references to the
           new created qnames in the WS */
        printf("item%03u.seqbase(seqb);\n", i);
        printf("ws.fetch(ATTR_QN).fetch(WS).insert(item%03u);\n", i);
        printf("ws.fetch(ATTR_FRAG).fetch(WS).insert(item%03u.project(WS));\n", i);
        printf("ws.fetch(ATTR_OWN).fetch(WS).insert(item%03u.mark(nil));\n", i);

        printf("item := item%03u.mirror;\n", i);
        printf("item.seqbase(0@0);\n");
        printf("kind := kind.project(ATTR);\n");
        printf("} # end of loop_liftedAttrConstr (int i)\n");
    }

    static void
    loop_liftedTextConstr (void)
    {
        /* expects exactly one string for each iter */
        printf("if (iter.tunique.count != kind.uselect(STR).count) ");
        printf("ERROR (\"Text Constructor awaits exactly one string for each iter\");\n");

        printf("{ # adding new strings to text node content and create new nodes\n"
               "var oid_str := item.leftfetchjoin(str_values);\n"
               "var seqb := oid(ws.fetch(PROP_TEXT).fetch(WS).count);\n"
               "var oid_oid := oid_str.mark(seqb);\n"
               "oid_str.seqbase(seqb);\n"
               "seqb := nil;\n"
               "ws.fetch(PROP_TEXT).fetch(WS).insert(oid_str);\n"
               "oid_str := nil;\n"
               "seqb := oid(ws.fetch(PRE_KIND).fetch(WS).count);\n"
               "var newPre_prop := oid_oid.reverse.mark(seqb).reverse;\n"
               "ws.fetch(PRE_PROP).fetch(WS).insert(newPre_prop);\n"
               "ws.fetch(PRE_SIZE).fetch(WS).insert(newPre_prop.project(0));\n"
               "ws.fetch(PRE_LEVEL).fetch(WS).insert(newPre_prop.project(chr(0)));\n"
               "ws.fetch(PRE_KIND).fetch(WS).insert(newPre_prop.project(TEXT));\n"
               "ws.fetch(PRE_FRAG).fetch(WS).insert(newPre_prop.project(WS));\n"
               "newPre_prop := nil;\n"
               "item := item.mark(seqb);\n"
               "seqb := nil;\n"
               "kind := kind.project(NODE);\n"
               "}\n");

        /* adding the new constructed roots to the WS_FRAG bat of the working
           set, that a following (preceding) step can check the fragment
           boundaries */
        printf("{ # adding new fragments to the WS_FRAG bat\n"
               "var seqb := ws.fetch(WS_FRAG).count;\n"
               "seqb := oid(seqb);\n"
               "var new_pres := item.reverse.mark(seqb).reverse;\n"
               "seqb := nil;\n"
               "ws.fetch(WS_FRAG).insert(new_pres);\n"
               "new_pres := nil;\n"
               /* get the maximum level of the new constructed nodes
                  and set the maximum of the working set */
               "ws.fetch(HEIGHT).replace(WS, max(ws.fetch(HEIGHT).fetch(WS), 1));\n"
               "}\n");
    }

    /*
     * translates either then or else block of an if-then-else
     *
     * to avoid more than one expansion of the subtree for each 
     * branch three branches (PHASES) are added in MIL. They
     * avoid the expansion of the variable environment and of the 
     * subtree if the if-clause produces either only true or only
     * false values. If then- or else-clause is empty (c_empty)
     * the function will only be called for the other.
     *
     *  '-' = not      |   skip  |  c_empty 
     *        executed | 0  1  2 | then  else
     *  PHASE 1 (then) |    -  - |  -
     *  PHASE 2 (then) |       - |  -
     *  PHASE 3 (then) |    -  - |  -
     *  PHASE 1 (else) |    -  - |        -
     *  PHASE 2 (else) |    -    |        -
     *  PHASE 3 (else) |    -  - |        -
     *
     * @param c the then/else expression
     * @param then is the boolean saving if the branch (then/else)
     * @param bool_res the number, where the result of the if-clause
     *        is saved 
     */
    static void
    translateIfThen (PFcnode_t *c, int then, int bool_res)
    {
        act_level++;
        printf("{ # translateIfThen\n");

        /* initial setting of new 'scope' */
        printf("var loop%03u := loop%03u;\n", act_level, act_level-1);
        printf("var inner%03u := inner%03u;\n", act_level, act_level-1);
        printf("var outer%03u := outer%03u;\n", act_level, act_level-1);
        printf("var v_vid%03u := v_vid%03u;\n", act_level, act_level-1);
        printf("var v_iter%03u := v_iter%03u;\n", act_level, act_level-1);
        printf("var v_pos%03u := v_pos%03u;\n", act_level, act_level-1);
        printf("var v_item%03u := v_item%03u;\n", act_level, act_level-1);
        printf("var v_kind%03u := v_kind%03u;\n", act_level, act_level-1);

        /* 1. PHASE: create all mapping stuff to next 'scope' */
        printf("if (skip = 0)\n{\n");
        /* output for debugging
        printf("\"PHASE 1 of %s-clause active\".print;\n",then?"then":"else");
        */

        /* get the right set of sequences, which have to be processed */
        if (!then)
                printf("selected := item%03u.ord_uselect(0@0);\n", bool_res);

        printf("iter := selected.mirror.join(iter%03u);\n", bool_res);
        printf("iter := iter.reverse.mark(0@0).reverse;\n");
        printf("outer%03u := iter;\n", act_level);
        printf("iter := iter.mark(1@0);\n");
        printf("inner%03u := iter;\n", act_level);
        printf("loop%03u := inner%03u;\n", act_level, act_level);
        printf("iter := nil;\n");

        /* - in a first version no variables are pruned
             at an if-then-else node 
           - if-then-else is executed more or less like a for loop */
        printf("var expOid := v_iter%03u.mirror;\n", act_level);
        //getExpanded (c->sem.num);
        //printf("if (expOid.count != 0) {\n");
                printf("var oidNew_expOid;\n");
                expand ();
                join ();
        //printf("} else {\n");
        //        createNewVarTable ();
        //printf("} # end if\n");
        printf("expOid := nil;\n");

        printf("}\n");

        /* 2. PHASE: execute then/else expression if there are 
           true/false values in the boolean expression */
        if (then)
                printf("if (skip != 1)\n{\n");
        else
                printf("if (skip != 2)\n{\n");
        /* output for debugging
        printf("\"PHASE 2 of %s-clause active\".print;\n",then?"then":"else");
        */

        translate2MIL (c);
        printf("}\n");
        printf("else\n{\n");
        translateEmpty ();
        printf("}\n");

        /* 3. PHASE: create all mapping stuff from to actual 'scope' */
        printf("if (skip = 0)\n{\n");
        /* output for debugging
        printf("\"PHASE 3 of %s-clause active\".print;\n",then?"then":"else");
        */
                mapBack ();
        printf("}\n");

        cleanUpLevel ();
        printf("} # end of translateIfThen\n");
        act_level--;
    }

    /**
     * prints to stdout MIL-expressions, for the following
     * core nodes:
     * c_var, c_seq, c_for, c_let
     * c_lit_str, c_lit_dec, c_lit_dbl, c_lit_int
     * c_empty, c_true, c_false
     * c_locsteps, (axis: c_ancestor, ...)
     * (tests: c_namet, c_kind_node, ...)
     * c_ifthenelse,
     * (constructors: c_elem, ...)
     *
     * the following list is not supported so far:
     * c_nil
     * c_apply, c_arg,
     * c_typesw, c_cases, c_case, c_seqtype, c_seqcast
     * c_error, c_root, c_int_eq
     */
    static void
    translate2MIL (PFcnode_t *c)
    {
        char *ns, *loc;
        int inter_res, bool_res;

        assert(c);
        switch (c->kind)
        {
                case c_var:
                        translateVar(c);
                        break;
                case c_seq:
                        if ((c->child[0]->kind == c_empty)
                            && (c->child[1]->kind != c_empty))
                                translateEmpty ();
                        else if (c->child[0]->kind == c_empty)
                                translate2MIL (c->child[1]);
                        else if (c->child[1]->kind == c_empty)
                                translate2MIL (c->child[0]);
                        else
                        {
                                translate2MIL (c->child[0]);
                                inter_res = saveResult ();

                                translate2MIL (c->child[1]);

                                translateSeq (inter_res);
                                deleteResult ();
                        }
                        break;
                case c_let:
                        translate2MIL (c->child[1]);
                        if (c->child[0]->sem.var->used)
                                insertVar (c->child[0]->sem.var->vid);

                        translate2MIL (c->child[2]);
                        break;
                case c_for:
                        translate2MIL (c->child[2]);
                        /* not allowed to overwrite iter,pos,item */

                        act_level++;
                        printf("{\n");
                        project ();

                        printf("var expOid;\n");
                        getExpanded (c->sem.num);
                        printf("if (expOid.count != 0) {\n");
                                printf("var oidNew_expOid;\n");
                                expand ();
                                join ();
                        printf("} else {\n");
                                createNewVarTable ();
                        printf("} # end if\n");
                        printf("expOid := nil;\n");

                        if (c->child[0]->sem.var->used)
                                insertVar (c->child[0]->sem.var->vid);
                        if ((c->child[1]->kind == c_var)
                            && (c->child[1]->sem.var->used))
                        {
                                /* changes item and kind and inserts if needed
                                   new int values to 'int_values' bat */
                                createEnumeration ();
                                insertVar (c->child[1]->sem.var->vid);
                        }
                        /* end of not allowed to overwrite iter,pos,item */

                        translate2MIL (c->child[3]);
                        
                        mapBack ();
                        cleanUpLevel ();
                        printf("}\n");
                        act_level--;
                        break;
                case c_ifthenelse:
                        translate2MIL (c->child[0]);
                        bool_res = saveResult ();
                        printf("{\n");
                        /* idea:
                        select trues
                        if (trues = count) or (trues = 0)
                             only give back one of the results
                        else
                             do the whole stuff
                        */
                        printf("var selected := item%03u.ord_uselect(1@0);\n", bool_res);
                        printf("var skip := 0;\n");
                        printf("if (selected.count = item%03u.count) "
                               "skip := 2;\n", bool_res);
                        printf("else if (selected.count = 0) "
                               "skip := 1;\n");
                        /* if at compile time one argument is already known to
                           be empty don't do the other */
                        if (c->child[2]->kind == c_empty)
                        {
                                translateIfThen (c->child[1], 1, bool_res);
                        }
                        else if (c->child[1]->kind == c_empty)
                        {
                                translateIfThen (c->child[2], 0, bool_res);
                        }
                        else
                        {
                                translateIfThen (c->child[1], 1, bool_res);
                                inter_res = saveResult ();
                                translateIfThen (c->child[2], 0, bool_res);
                                translateSeq (inter_res);
                                deleteResult ();
                        }
                        printf("}\n");
                        deleteResult ();
                        break;
                case c_locsteps:
                        translate2MIL (c->child[1]);
                        translateLocsteps (c->child[0]);
                        break;
                case c_elem:
                        translate2MIL (c->child[0]);

                        if (c->child[0]->kind != c_tag)
                            getQName ();

                        inter_res = saveResult ();

                        translate2MIL (c->child[1]);

                        loop_liftedElemConstr (inter_res);
                        deleteResult ();
                        break;
                case c_attr:
                        translate2MIL (c->child[0]);

                        if (c->child[0]->kind != c_tag)
                            getQName ();

                        inter_res = saveResult ();

                        translate2MIL (c->child[1]);

                        loop_liftedAttrConstr (inter_res);
                        deleteResult ();
                        break;
                case c_tag:
                        ns = c->sem.qname.ns.uri;
                        loc = c->sem.qname.loc;

                        /* translate missing ns as "" */
                        if (!ns)
                            ns = "";

                        printf("{\n");
                        printf("var propID := ws.fetch(QN_NS).fetch(WS).ord_uselect(\"%s\").mirror;\n", ns);
                        printf("propID := propID.leftfetchjoin(ws.fetch(QN_LOC).fetch(WS));\n");
                        printf("propID := propID.ord_uselect(\"%s\");\n", loc);
                        printf("var itemID;\n");

                        printf("if (propID.count = 0) {\n");
                        /* perhaps max is better to get boundary then count 
                           (if seqbase is not 0@0 or if head is not void) */
                        printf("         itemID := oid(ws.fetch(QN_LOC).fetch(WS).count);\n");
                        printf("         ws.fetch(QN_NS).fetch(WS).insert (itemID,\"%s\");\n", ns);
                        printf("         ws.fetch(QN_LOC).fetch(WS).insert (itemID,\"%s\");\n", loc);
                        printf("} else ");
                        printf(         "itemID := propID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst ("QNAME");
                        printf("propID := nil;\n");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_text:
                        translate2MIL (c->child[0]);
                        loop_liftedTextConstr ();
                        break;
                case c_lit_str:
                        /* the value of the string is looked up in the 
                           str_values table. If it already exists the oid
                           is given back else it is inserted and a new
                           oid is created */
                        /* old version
                        printf("{\n");
                        printf("var itemID := str_values.ord_uselect(\"%s\");\n",
                               PFesc_string (c->sem.str));
                        printf("if (itemID.count = 0) {\n");
                        printf("         itemID := oid(str_values.count);\n");
                        printf("         str_values.insert (nil,\"%s\");\n",
                                        PFesc_string (c->sem.str));
                        printf("} else ");
                        printf(         "itemID := itemID.reverse.fetch(0);\n");
                        */
                        printf("{\n");
                        printf("str_values.insert (nil,\"%s\");\n",
                               PFesc_string (c->sem.str));
                        printf("var itemID := str_values.ord_uselect(\"%s\");\n",
                               PFesc_string (c->sem.str));
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst("STR");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_lit_int:
                        printf("{\n");
                        printf("int_values.insert (nil,%u);\n",
                               c->sem.num);
                        printf("var itemID := int_values.ord_uselect(%u);\n",
                               c->sem.num);
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst("INT");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_lit_dec:
                        printf("{\n");
                        printf("dec_values.insert (nil,dbl(%g));\n",
                               c->sem.dec);
                        printf("var itemID := dec_values.ord_uselect(dbl(%g));\n",
                               c->sem.dec);
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst("DEC");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_lit_dbl:
                        printf("{\n");
                        printf("dbl_values.insert (nil,dbl(%g));\n",
                               c->sem.dbl);
                        printf("var itemID := dbl_values.ord_uselect(dbl(%g));\n",
                               c->sem.dbl);
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst("DBL");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_true:
                        printf("{\n");
                        printf("var itemID := 1@0;\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst("BOOL");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_false:
                        printf("{\n");
                        printf("var itemID := 0@0;\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst("BOOL");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_root:
                        /* builtin function, which is only translated for 
                           debugging purposes (with "foo.xml" - see init) */
                        printf("{\n");
                        printf("var itemID := 0@0;\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst("NODE");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_empty:
                        translateEmpty ();
                        break;
                case c_seqcast:
                        /* seqcast just ignores the cast */
                        PFlog("cast to '%s' ignored",PFty_str (c->child[0]->sem.type));
                        translate2MIL (c->child[1]);
                        break;
                case c_apply:
                        translateFunction (c->sem.fun->qname, c->child[0]);
                        break;
                case c_nil:
                default: 
                        PFoops (OOPS_WARNING, "not supported feature is translated");
                        break;
        }
    }

    /* the fid increasing for every for node */ 
    static int fid = 0;
    /* the actual fid saves the level (the right fid) */
    static int act_fid = 0;
    /* the vid increasing for every new variable binding */
    static int vid = 0;

    /**
      * the pairs of vid and fid are inserted in the var_usage bat
      *
      * @param var the pointer to the variable and its vid
      * @param fid the id of the for expression
      */
    static void
    add_level (PFvar_t *var, int fid)
    {
        printf("var_usage.insert(%i@0,%i@0);\n", var->vid, fid); 
    }

    /**
      * for a variable usage all fids between the definition of the 
      * variable and its usage are added to the var_usage bat
      *
      * @param c the variable core node
      * @param way the path of the for ids (active for-expression)
      */
    static void
    update_expansion (PFcnode_t *c,  PFarray_t *way)
    {
        int m;
        PFvar_t *var;

        assert(c->sem.var);
        var = c->sem.var;

        for (m = PFarray_last (way) - 1; m >= 0 
             && *(int *) PFarray_at (way, m) > var->base; m--)
        {
            add_level (var, *(int *) PFarray_at (way, m));
        }
    }

    /**
      * for each variable a vid (variable id) and
      * for each for expression a fid (for id) is added;
      * for each variable usage the needed fids are added to a 
      * bat variable_usage
      *
      * @param c a core tree node
      * @param way an array containing the path of the for ids
      *        (active for-expression)
      */
    static void
    append_lev (PFcnode_t *c,  PFarray_t *way)
    {
        unsigned int i;

        if (c->kind == c_var) 
        {
           /* inserts fid|vid combinations into var_usage bat */
           update_expansion (c, way);
           /* the field used is for pruning the MIL code and
              avoid translation of variables which are later
              not used */
           c->sem.var->used = 1;
        }

        /* only in for and let variables can be bound */
        else if (c->kind == c_for)
        {
           if (c->child[2])
               append_lev (c->child[2], way);
           
           fid++;
           c->sem.num = fid;
           *(int *) PFarray_add (way) = fid;
           act_fid = fid;

           c->child[0]->sem.var->base = act_fid;
           c->child[0]->sem.var->vid = vid;
           c->child[0]->sem.var->used = 0;
           vid++;

           if (c->child[1]->kind == c_var)
           {
                c->child[1]->sem.var->base = act_fid;
                c->child[1]->sem.var->vid = vid;
                c->child[1]->sem.var->used = 0;
                vid++;
           }

           if (c->child[3])
               append_lev (c->child[3], way);
           
           act_fid = *(int *) PFarray_at (way, PFarray_last (way) - 1);
           PFarray_del (way);
        }

        else if (c->kind == c_let)
        {
           if (c->child[1])
               append_lev (c->child[1], way);

           c->child[0]->sem.var->base = act_fid;
           c->child[0]->sem.var->vid = vid;
           c->child[0]->sem.var->used = 0;
           vid++;

           if (c->child[2])
               append_lev (c->child[2], way);
        }

        else 
        {
           for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
              append_lev (c->child[i], way);
        } 
    }

    /**
      * first MIL generation from Pathfinder Core
      *
      * first to each `for' and `var' node additional
      * information is appended. With this information
      * the core tree is translated into MIL.
      *
      * @param c the root of the core tree
      */
    void
    PFprintMILtemp (PFcnode_t *c)
    {
        PFarray_t *way;

        way = PFarray (sizeof (int));
        assert (way);
        
        /* some bats and module get initialized, variables get bound */
        init ();

        /* append_lev appends information to the core nodes and
           creates a var_usage table, which is later split in vu_fid
           and vu_vid */
        printf("{\n"
               "var_usage := bat(oid,oid);\n");/* [vid, fid] */
        append_lev (c, way);
        /* the contents of var_usage will be sorted by fid and
           then refined (sorted) by vid */
        printf("var_usage := var_usage.unique.reverse;\n"
               "var_usage.access(BAT_READ);\n"
               "vu_fid := var_usage.mark(1000@0).reverse;\n"
               "vu_vid := var_usage.reverse.mark(1000@0).reverse;\n"
               "var_usage := nil;\n"
               "var sorting := vu_fid.reverse.sort.reverse;\n"
               "sorting := sorting.CTrefine(vu_vid);\n"
               "sorting := sorting.mark(1000@0).reverse;\n"
               "vu_vid := sorting.leftfetchjoin(vu_vid);\n"
               "vu_fid := sorting.leftfetchjoin(vu_fid);\n"
               "sorting := nil;\n"
               "}\n");

        /* recursive translation of the core tree */
        translate2MIL (c);

        /* print result in iter|pos|item representation */
        print_output ();

        if (counter) PFoops (OOPS_FATAL, 
                             "wrong number of saveResult() and "
                             "deleteResult() calls in milprint.c");
        printf("printf(\"mil-programm without crash finished :)\\n\");\n");
    }
/* vim:set shiftwidth=4 expandtab: */
