###################################
# axis.mil : XQuery axis operations
#
# This file contains all axis operations of XQuery (more accurately XPath).
# Except for root, they all take a bat with context nodes (only head is
# used which contains the pre-values of the nodes) and return a new
# context. 
# Each axis operation is implemented using a main function which
# selects and calls one of the other functions which each implement a
# particular algorithm.
#

var SHOW_TIMING:=false;

PROC show_timing(bit st) : void := 
{
        SHOW_TIMING:=st;
}
ADDHELP("show_timing","keulen","June 10, 2003",
"Set configuration parameter to show/hide timing information about axis steps.\nst: boolean on/off.",
"pathfinder");

PROC show_timing() : bit := 
{
        return SHOW_TIMING;
}
ADDHELP("show_timing","keulen","June 10, 2003",
"Returns value of configuration parameter to show/hide timing information about axis steps.",
"pathfinder");


### root
PROC root(str name) : bat :=
{
        return root_AlgoBasic(name);
}
ADDHELP("root","keulen","June 10, 2003",
"Returns context sequence with the root node of document with name 'name'.\nname: string with name of document.",
"pathfinder");

PROC root_AlgoBasic(str name) : bat :=
{
        if (SHOW_TIMING) { t0:=time; }
        docid:=doc_name.reverse.find(name);
        rootpre:=Tdoc_rootpre.find(docid);
        context:=new(oid,oid); 
        context.insert(rootpre,nil);
        if (SHOW_TIMING)
        {
                t1:=time;
                printf("root(Basic): %dms\n", t1 - t0);
        }
        return context;
}
ADDHELP("root_AlgoBasic","keulen","June 10, 2003",
"Implementation of root(name) that uses a basic algorithm: find document id of document with name 'name'; find preorder rank of document id.\nname: string with name of document.",
"pathfinder");


### child
PROC child(pair ctx) : pair :=
{
m4_ifdef(`m4_level',
` return child_AlgoLevel(ctx);',
` return child_AlgoParent(ctx);')
}
ADDHELP("child","keulen","June 10, 2003",
"Returns context sequence with all children of nodes in 'ctx'.\nctx: context node sequence.",
"pathfinder");

PROC child_AlgoParent(bat ctx) : bat :=
{
        if (SHOW_TIMING) { t0:=time; }
        children :=
                ctx.mirror.join(Tdoc_parent.reverse).reverse.project(nil);
        if (SHOW_TIMING)
        {
                t1:=time;
                printf("child(Parent): %dms\n", t1 - t0);
        }
        return children;
}
ADDHELP("child_AlgoParent","keulen","June 10, 2003",
"Implementation of child(ctx) that uses an algorithm based on a join with the parent BAT.\nctx: context node sequence.",
"pathfinder");

PROC child_AlgoLevel(bat ctx) : bat :=
{
        ERROR("child_AlgoLevel not implemented.\n");
}
ADDHELP("child_AlgoLevel","keulen","June 10, 2003",
"Implementation of child(ctx) that uses an algorithm based on a scan of the level BAT.\nctx: context node sequence.",
"pathfinder");




### descendant
PROC descendant(bat ctx) : bat :=
{
        return descendant_AlgoScj(ctx);
}
ADDHELP("descendant","keulen","August 6, 2003",
"Returns context sequence with all descendants of nodes in 'ctx'.\nctx: context node sequence.",
"pathfinder");

PROC descendant_AlgoScj(bat ctx) : bat :=
{
        if (SHOW_TIMING) { t0:=time; }
        ctx_with_post := ctx.mirror.join(Tdoc_prepost);
        if (SHOW_TIMING) { t1:=time; }
        descendants :=
                staircasejoin_br_void_excl(Tdoc_prepost,ctx_with_post,
                        Tdoc_height,Tdoc_prepost.count);
        if (SHOW_TIMING)
        {
                t2:=time;
                printf("descendant(Scj): %dms (join:%dms; scj:%dms)\n",
                        t2 - t0, t1 - t0, t2 - t1);
        }
        return descendants;
}
ADDHELP("descendant_AlgoScj","keulen","August 6, 2003",
"Implementation of descendant(ctx) that uses an algorithm based on a staircase join with the document BAT.\nctx: context node sequence.",
"pathfinder");

