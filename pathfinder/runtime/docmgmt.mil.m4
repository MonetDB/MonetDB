m4_include(config.m4)

#############################################
# docmgmt.mil : Document management functions
#
# Each XML-document is stored in a collection of BATs whose name starts
# with the name of the document. During execution of an XQuery, the
# 'working set' of documents is copied to a 'global document' represented
# by the Tdoc* collection of BATs.
#
# Each collection contains the following BATs:
# - prepost : BAT[oid,oid]  Pre/Post-values of nodes
m4_ifdef(`m4_level',
`# - level   : BAT[oid,chr]  Pre-value of node/and its level',
`# - parent  : BAT[oid,oid]  Pre-value of node/Pre-value of its parent')
# - tag     : BAT[oid,str]  Pre-value of node/Tag name
#                           Text nodes do not occur in this bat
# - text    : BAT[oid,str]  Pre-value/Value of text nodes
# - pi      : BAT[oid,str]  Pre-value/Value of processing instructions
# - com     : BAT[oid,str]  Pre-value/Value of comment nodes
# - aname   : BAT[oid,str]  Attribute ID/Name of attribute nodes
# - avalue  : BAT[oid,str]  Attribute ID/Value of attribute nodes
# - aowner  : BAT[oid,oid]  Attribute ID/Pre-value of owner
# These are stored in a doc BAT[str,bat] with the name (left) in the
# head and the BAT (right) in the tail.
#
# Document information
# - doc_name    : BAT[oid,str]  Document Id/Document name (incl XML)
# - doc_doc     : BAT[oid,bat]  Document Id/Collection of doc bats
# - doc_height  : BAT[oid,int]  Document Id/Height of document
# - Tdoc_rootpre : BAT[oid,oid] Document Id/Pre-value of working document
# - Tdoc_height : int           Max. height of docs in working set

var Tdoc_prepost;
m4_ifdef(`m4_level',
`var Tdoc_level;',
`var Tdoc_parent;')
var Tdoc_tag;
var Tdoc_text;
var Tdoc_pi;
var Tdoc_com;
var Tdoc_aname;
var Tdoc_avalue;
var Tdoc_aowner;
var Tdoc_rootpre;
var Tdoc_height:=0;

var enumtag_type;
var enumaname_type;

PROC doc_finalize() : void :=
{
	# collect all distinct tag and attribute names
	alltags := new(oid,str);
	allnames := new(oid,str);
	alltags.insert(nil,"globalroot");
	doc_doc@batloop
	{
		docid := $h;
		doc := $t;
		s := doc.find("tag").reverse.project(nil).kunique.reverse;
		alltags := alltags.kunion(s);
		s := doc.find("aname").reverse.project(nil).kunique.reverse;
		allnames := allnames.kunion(s);
	}

	# create enumeration type
	enumtag_type := enum_create("enumtag", alltags);
	enumaname_type := enum_create("enumaname", allnames);

	# encode all tag BATs
	doc_doc@batloop
	{
		docid := $h;
		doc := $t;
		b := doc.find("tag");
		doc.replace("tag", [encode](enumtag_type, b));
		b := doc.find("aname");
		doc.replace("aname", [encode](enumaname_type, b));
	}
m4_ifdef(`m4_level',
`
        # shrink size of the level table
	doc_doc@batloop
	{
                docid := $h;
		doc := $t;
		b := doc.find("level");
		doc.replace("level", b.[chr]);
	}')
        # generate document statistics
	doc_doc@batloop
	{
                docid := $h;
		doc := $t;
	        doc.insert("stats", get_statistics(doc.find("prepost"), 
		                                   doc.find("level"), 
						   doc.find("tag")));
	}
	
}
ADDHELP("doc_finalize", "keulen", "June 5, 2003",
"After having imported all required documents, it has to be 'finalized', i.e., the tag-BAT is converted into using an enum for tags.",
"pathfinder");

PROC bat_renum(bat b,int d) : bat :=
{
	if (d = 0) { return b; }
	if (b.info.find("tail") = "void")
	{
		bcopy := b.copy;
		bcopy.reverse.seqbase(oid(int(b.reverse.seqbase()) + d));
		return bcopy;
	}
	else if (b.info.find("head") = "str")
	{
		return b.reverse.mark(0@0).reverse.[int].[+](d).[oid].reverse
			.join(b.mark(0@0).reverse).reverse;
	}
	else if (b.info.find("tail") = "chr")
	{
		return b.[int].[+](d).[chr];
	}
	else
	{
	# b.[+](int) does not preserve a possibly present sorted-property
   	# on the tail of 'b'. This is, however, important for the aowner-BAT.
   	# The trick with debug masks switches on additional property
   	# checking (see monet.conf for details), 
	# which means the result BAT is checked about its
	# properties. This will re-introduce the sorted-property on the
	        var ret := b.[int];
		var orgDebugMask := debugmask();
		var newDebugMask := or(or(2,8),orgDebugMask);
		debugmask(newDebugMask);
		ret := ret.[+](d);
		debugmask(orgDebugMask);
		return ret.[oid];
	}
}

PROC doc_to_working_set(str name) : void :=
{
	docid := doc_name.reverse.find(name);
	doc := doc_doc.find(docid);
	doc_prepost := doc.find("prepost");

	grootpre := 0@0;
	grootpost := Tdoc_prepost.find(grootpre);
	Trootpre := grootpost;
	if (Trootpre = 0@0) { Trootpre:=1@0; }
	Tfirstattr := oid(int(Tdoc_aowner.reverse.max) + 1);
	if (isnil(Tfirstattr)) { Tfirstattr:=0@0; }
	Prootpre := 0@0;
	Prootpost := doc_prepost.find(Prootpre);
	Pfirstattr := 0@0;

	deltapre := int(Trootpre) - int(Prootpre);
	deltaattr := int(Tfirstattr) - int(Pfirstattr);

	Tdoc_rootpre.insert(docid,Trootpre);
	Tdoc_height:=max(Tdoc_height,doc_height.find(docid));
	
	Tdoc_prepost.insert(
		doc_prepost.bat_renum(deltapre)
			.reverse.bat_renum(deltapre).reverse);
m4_ifdef(`m4_level',			
`	Tdoc_level.insert(
		doc.find("level").bat_renum(1)
			.reverse.bat_renum(deltapre).reverse);',
`	Tdoc_parent.insert(Trootpre,grootpre);
	Tdoc_parent.insert(
		doc.find("parent").bat_renum(deltapre)
			.reverse.bat_renum(deltapre).reverse);')
	Tdoc_tag.insert(
		doc.find("tag").reverse.bat_renum(deltapre).reverse);
	Tdoc_text.insert(
		doc.find("text").reverse.bat_renum(deltapre).reverse);
	Tdoc_pi.insert(
		doc.find("pi").reverse.bat_renum(deltapre).reverse);
	Tdoc_com.insert(
		doc.find("com").reverse.bat_renum(deltapre).reverse);
	Tdoc_aname.insert(
		doc.find("aname").reverse.bat_renum(deltaattr).reverse);
	Tdoc_avalue.insert(
		doc.find("avalue").reverse.bat_renum(deltaattr).reverse);
	Tdoc_aowner.insert(
		doc.find("aowner").bat_renum(deltapre)
			.reverse.bat_renum(deltaattr).reverse);

	sb := Tdoc_prepost.seqbase();
	Tdoc_prepost.seqbase(nil);
	Tdoc_prepost.replace(grootpre, oid(int(Prootpost) + deltapre + 1));
	Tdoc_prepost.seqbase(sb);
}
ADDHELP("doc_to_working_set", "keulen", "May 28, 2003",
"Load persistent document into working set.",
"pathfinder");

PROC import_doc(str flnm, str name, int height) : oid :=
{
	printf(">>> Importing '%s' with name '%s' and height '%d'\n",
		flnm,name,height);
	docid := new_Pdoc(name,height);
	doc := doc_doc.find(docid);
	doc.find("prepost").import(flnm+".prepost");
m4_ifdef(`m4_level',
`	doc.find("level").import(flnm+".level");',
`	doc.find("parent").import(flnm+".parent");')
	doc.find("tag").import(flnm+".tag");
	doc.find("text").import(flnm+".text");
	doc.find("pi").import(flnm+".pi");
	doc.find("com").import(flnm+".com");
	doc.find("aname").import(flnm+".@name");
	doc.find("avalue").import(flnm+".@val");
	doc.find("aowner").import(flnm+".@owner");
}
ADDHELP("import_doc", "keulen", "May 28, 2003",
"Import files flnm.* generated by xml-loader as persistent document\nwith name and height.","pathfinder");

PROC new_Pdoc(str name, int height) : oid :=
{
	if (doc_name.reverse.exist(name))
	{
		ERROR("new_Pdoc: Document %s already exists\n",name);
	}
	doc:=new(str,bat);
	doc.insert("prepost",new(void,oid).seqbase(0@0));
m4_ifdef(`m4_level',	
`	doc.insert("level",new(void,oid).seqbase(0@0));',
`	doc.insert("parent",new(void,oid).seqbase(0@0));')
	doc.insert("tag",new(oid,str));
	doc.insert("text",new(oid,str));
	doc.insert("pi",new(oid,str));
	doc.insert("com",new(oid,str));
	doc.insert("aname",new(void,str).seqbase(0@0));
	doc.insert("avalue",new(void,str).seqbase(0@0));
	doc.insert("aowner",new(void,oid).seqbase(0@0));
	
	doc_name.insert(nil,name);
	doc_doc.insert(nil,doc);
	doc_height.insert(nil,height);

	return doc_name.reverse.fetch(-(doc_name.count,1));
}
ADDHELP("new_Pdoc", "keulen", "May 28, 2003",
"Create an empty persistent document with name and height.","pathfinder");

PROC Pdoc_init() : void :=
{
	new(void,str).persists(true).seqbase(0@0).bbpname("doc_name");
	new(void,bat).persists(true).seqbase(0@0).bbpname("doc_doc");
	new(void,int).persists(true).seqbase(0@0).bbpname("doc_height");
}
ADDHELP("Pdoc_init", "keulen", "May 28, 2003",
"Initialize the BATs for storage of persistent documents.","pathfinder");

PROC Tdoc_init() : void :=
{
	enumtag_type := enum_load("enumtag");
	enumaname_type := enum_load("enumaname");

	Tdoc_prepost := new(void,oid).seqbase(0@0);
m4_ifdef(`m4_level',	
`	Tdoc_level := new(void,chr).seqbase(0@0);',
`	Tdoc_parent := new(void,oid).seqbase(1@0);')
	Tdoc_tag := new(oid,enumtag_type);
	Tdoc_text := new(oid,str);
	Tdoc_pi := new(oid,str);
	Tdoc_com := new(oid,str);
	Tdoc_aname := new(void,enumaname_type).seqbase(0@0);
	Tdoc_avalue := new(void,str).seqbase(0@0);
	Tdoc_aowner := new(void,oid).seqbase(0@0);
	doc_name := new(void,str).seqbase(0@0);
	doc_height := new(void,int).seqbase(0@0);
	Tdoc_rootpre := new(void,oid).seqbase(0@0);

	# global root
	Tdoc_prepost.insert(0@0,0@0);
	Tdoc_tag.insert(0@0,encode(enumtag_type,"globalroot"));
m4_ifdef(`m4_level',	
`	Tdoc_level.insert(0@0,chr(0));')
}
ADDHELP("Tdoc_init", "keulen", "May 28, 2003",
"Initialize the BATs for storage of the working set of documents.",
"pathfinder");


PROC get_statistics(BAT Tdoc_prepost, BAT Tdoc_level, BAT Tdoc_tag) : BAT[void,BAT] :=
{
   var maxdepth := Tdoc_level.max.int;
   
   # fan-out groups
   var parents := parent_lev(Tdoc_level, Tdoc_prepost, maxdepth, Tdoc_level.count);
   var num_children := new(oid,int);
   var tmp := new(oid,oid);
   
   parents@batloop
   {
      tmp.delete;
      tmp.insert($h,1@0);
      num_children.insert($h, child_lev(Tdoc_level, tmp, maxdepth, Tdoc_level.count).count);
   }
  
   var group_parents := num_children.reverse.{count}.sort;
   var group_histogram := group_parents.mark(0@0).reverse;
   group_parents := group_parents.reverse.mark(0@0).reverse;
   group_histogram := group_histogram.[*](group_parents);
   tmp := group_parents.copy;
   group_parents := tmp;

   # summarized fan-out groups
   
   var g_size_limit, g_size, g_par;
   var gsum_histogram := new(void,int).seqbase(0@0);
   var gsum_parents := new(void,int).seqbase(0@0);
   g_size_limit := group_histogram.fetch(0) - 2;
   g_size := 0;
   g_par := 0;
   group_histogram@batloop
   {
      g_size :+= $t;
      g_par :+= group_parents.fetch($h);
      if (g_size > g_size_limit)
      {
         gsum_histogram.insert(nil, g_size);
	 gsum_parents.insert(nil, g_par);
	 g_size := 0;
	 g_par := 0;
      }
   }
   if (g_size > 0)
   {
      gsum_histogram.insert(nil, g_size);
      gsum_parents.insert(nil, g_par);
   }

   # tag-name groups
   var upperbound := Tdoc_level.count;
   var tag_names := Tdoc_tag.reverse.kunique.sort.mirror;
   var tag_nodes := [select](const Tdoc_tag, tag_names).[sort];
   var tag_histogram := [count](tag_nodes);
   var tag_parents := [parent_lev](const Tdoc_level, tag_nodes, maxdepth, upperbound).[count]; 
   var tag_children := [child_lev](const Tdoc_level, tag_nodes, maxdepth, upperbound).[count];
   var fs_histogram := [followingsibling_lev](const Tdoc_level, tag_nodes, maxdepth, upperbound).[count];
   var ps_histogram := [precedingsibling_lev](const Tdoc_level, tag_nodes, maxdepth, upperbound).[count];
   
   # tag_histogram.access(BAT_APPEND);
   # tag_parents.access(BAT_APPEND);
   # fs_histogram.access(BAT_APPEND);
   # ps_histogram.access(BAT_APPEND);
   # 
   # tag_histogram.insert("*",Tdoc_tag.count);
   # tag_parents.insert("*",parent_lev(Tdoc_level, Tdoc_tag, maxdepth, upperbound).count);
   # ps_histogram.insert("*",precedingsibling_lev(Tdoc_level, Tdoc_tag, maxdepth, upperbound).count);
   # fs_histogram.insert("*",followingsibling_lev(Tdoc_level, Tdoc_tag, maxdepth, upperbound).count);
   
   var fs1_histogram := new(enumtag_type,int);
   var ps1_histogram := new(enumtag_type,int);
   tmp := new(oid,oid);
   
   tag_nodes@batloop
   {
       fs := 0;
       ps := 0;
       
       $t@batloop
       {
          tmp.delete;
	  tmp.insert($h,1@0);
          fs :+= followingsibling_lev(Tdoc_level, tmp, maxdepth, upperbound).count;
          ps :+= precedingsibling_lev(Tdoc_level, tmp, maxdepth, upperbound).count;
       }
       
       fs1_histogram.insert($h,fs);
       ps1_histogram.insert($h,ps);
   }
  
   #level groups
   
   var nodes_in_level;
   var level_histogram := new(void,int).seqbase(0@0);
   var level_parents := new(void,int).seqbase(0@0);
   
   #root_level
   level_histogram.insert(nil, 1);
   level_parents.insert(nil, 0);
   
   var x := 1;
   while(x <= maxdepth)
   {
      nodes_in_level := Tdoc_level.uselect(chr(x));
      level_histogram.insert(nil, nodes_in_level.count);
      level_parents.insert(nil, parent_lev(Tdoc_level, nodes_in_level, maxdepth, Tdoc_level.count).count);
      x :+= 1;
   }

   # summarized level groups
   
   var levelf_histogram := new(void,int).seqbase(0@0);
   var levelf_parents := new(void,int).seqbase(0@0);
   var tmp_histogram, tmp_parents;
   
   levelf_histogram.insert(nil, -1);
   levelf_parents.insert(nil, -1);
      
   x := 1;
   while(x < maxdepth)
   {
      nodes_in_level := Tdoc_level.uselect(chr(x));
      tmp_parents := num_children.semijoin(nodes_in_level).reverse.{count}.sort;
      tmp_histogram := tmp_parents.mark(0@0).reverse;
      tmp_parents := tmp_parents.reverse.mark(0@0).reverse;
      tmp_histogram := tmp_histogram.[*](tmp_parents);
      
      g_size_limit := tmp_histogram.fetch(0) - 2;
      g_size := 0;
      g_par := 0;
      
      tmp_histogram@batloop
      {
         g_size :+= $t;
         g_par :+= tmp_parents.fetch($h);
         if (g_size > g_size_limit)
         {
            levelf_histogram.insert(nil, g_size);
   	    levelf_parents.insert(nil, g_par);
   	    g_size := 0;
   	    g_par := 0;
	    g_size_limit :*= 2;
         }
      }
      if (g_size > 0)
      {
         levelf_histogram.insert(nil, g_size);
         levelf_parents.insert(nil, g_par);
      }
      levelf_histogram.insert(nil, tmp_histogram.sum);
      levelf_parents.insert(nil, tmp_parents.sum);
      
      levelf_histogram.insert(nil, -1);
      levelf_parents.insert(nil, -1);
      
      x :+= 1;
   }
   
   # ancestor pruned level histogram
   var pruned := new(void, oid).seqbase(0@0);
   var min_post := Tdoc_prepost.fetch(0);
   Tdoc_prepost@revdocorder()
   {
      if ($t < min_post)
      {
         pruned.insert(nil, $h);
         min_post := $t;
      }	 
   }
   pruned := pruned.reverse.mirror.join(Tdoc_level).[sht];
   var pl_histogram := [uselect](const pruned, level_histogram.reverse.[sht])
                       .[count].reverse.mark(0@0).reverse.copy;

   #
   # gathering of all generated statistics
   # 
   
   var stats := new(str,BAT);
   
   stats.insert("g_size", group_histogram);
   stats.insert("g_par", group_parents);
   stats.insert("gsum_size", gsum_histogram);
   stats.insert("gsum_par", gsum_parents);
   stats.insert("tag_size", tag_histogram);
   stats.insert("tag_par", tag_parents);
   stats.insert("tag_chl", tag_children);
   stats.insert("tag_fs", fs_histogram);
   stats.insert("tag_ps", ps_histogram);
   stats.insert("tag_fs1", fs1_histogram);
   stats.insert("tag_ps1", ps1_histogram);
   stats.insert("pr_lev_size", pl_histogram);
   # stats.insert("lev_size", level_histogram);
   # stats.insert("lev_par", level_parents);
   stats.insert("lev_gsum_size", levelf_histogram);
   stats.insert("lev_gsum_par", levelf_parents);

   return stats;
}
ADDHELP("get_statistics", "rode", "Jul 31, 2003",
"evaluates document specific statistics for result size estimation on xpath axes.","pathfinder");

# vim:set shiftwidth=4 expandtab:
