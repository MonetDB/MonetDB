%{
/**
 * Parses output of the Haskell XQuery-to-Algebra Mapper into Pathfinder
 * algebra.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "pathfinder.h"
#include "hsk_parser.h"
#include "algebra_mnemonic.h"
#include "oops.h"
#include "mem.h"
#include "qname.h"

extern int hsklex(void); /* function prototype */

PFalg_op_t *root;        /* result of the parsing process */

extern int hskparse (void);

/* bison error callback */
void hskerror (const char *s)
{
    PFoops(OOPS_FATAL, "error parsing Haskell output: %s\n", s);
}

/* bison: generate verbose parsing error messages */
#define YYERROR_VERBOSE

/* Generic arrays to store all sorts of list items. */
PFarray_t *list_arr = NULL;
PFarray_t *proj_arr = NULL;
PFarray_t *join_arr = NULL;
PFarray_t *tupl_arr = NULL;
PFarray_t *atom_arr = NULL;
PFarray_t *schm_arr = NULL;

%}

/* Which types of terminals and non-terminals do we have? */
%union {
        int ival;
	char *sval;
        float fval;
        double dval;
        PFalg_op_t *algop;       /* algebra operator */
        PFalg_att_t attval;      /* attribute value */
        PFalg_attlist_t attlist; /* list of attributes */
        PFalg_proj_t projitm;    /* pair of new ans old attribute name */
        PFalg_axis_t xpaxis;     /* an XPath axis */
        PFalg_atom_t atomval;    /* atomic value (e.g. int, str, bln) */
        PFalg_tuple_t tup;       /* tuple representation */
        PFarray_t *arr;          /* generic array construct */
	}

/* Tokens from the lexer (terminals) with semantic string value. */
%token <sval> TYPEREF RELREF SCHMATT SCHMTYPE STR TAG

/* Tokens from the lexer (terminals) with semantic integer value. */
%token <ival> INT NODEREF BLN NAT

/* Tokens from the lexer (terminals) with semantic float value. */
%token <fval> DEC

/* Tokens from the lexer (terminals) with semantic double value. */
%token <dval> DBL

%token <attval> ATTREF

/* Tokens from the lexer (terminals) without semantic value. */
%token 
    LSQBR RSQBR LBRACK RBRACK COLON DBLCOLON
    COMMA DIVIDE PLUS TIMES MINUS DIV LT GT
    EQUAL PROJ NOT AND OR NEG ROWNUM EQJOIN
    SCJOIN CROSSPR UNION SEL TYPE TBL ELEM
    REL DESC DESCSELF ANC ANCSELF FOL PREC
    FOLSIBL PRECSIBL CHILD PARENT SELF ATTRIB
    XMLELEM NODES TEXT SUM COUNT DIFF DIST
    CINT CSTR CDEC CDBL TEXT

/* Types of non-terminals. */
%type <algop> query operator rownum project eqjoin
 scjoin crosspr disunion binaryop unaryop select
 type table element relation sum count differ distinct
 castint caststr castdec castdbl textnode ktest xpath

%type <attval> item schmitem

%type <attlist> braclist

%type <projitm> projitem

%type <xpaxis> axis

%type <atomval> atom

%type <tup> tuple

%type <arr> projlist joincond schmlist atomlist tuples list

/* The start symbol. */
%start query

%%

query   :    operator           {root=$1;} 
        ;

operator:    rownum             {$$=$1;}
        |    project            {$$=$1;}
        |    eqjoin             {$$=$1;}
        |    scjoin             {$$=$1;}
        |    crosspr            {$$=$1;}
        |    disunion           {$$=$1;}
        |    binaryop           {$$=$1;}
        |    unaryop            {$$=$1;}
        |    select             {$$=$1;}
        |    type               {$$=$1;}
        |    table              {$$=$1;}
        |    element            {$$=$1;}
        |    relation           {$$=$1;}
        |    sum                {$$=$1;}
        |    count              {$$=$1;}
        |    differ             {$$=$1;}
        |    distinct           {$$=$1;}
        |    castint            {$$=$1;}
        |    caststr            {$$=$1;}
        |    castdec            {$$=$1;}
        |    castdbl            {$$=$1;}
        |    textnode           {$$=$1;}
        ;

rownum  :    LSQBR ROWNUM LBRACK item COLON braclist RBRACK operator RSQBR
             {
               /* [ROW# (attr:(sortby_atts)) operator] */
	       $$=rownum ($8, $4, $6, NULL);
             }
        |    LSQBR ROWNUM LBRACK item COLON braclist DIVIDE item RBRACK
             operator RSQBR
             {
               /* [ROW# (attr:(sortby_atts)/attr) operator] */
               $$=rownum ($10, $4, $6, $8);
             }
        ;

project :   LSQBR PROJ LBRACK projlist RBRACK operator RSQBR
            {
              /* [¶ (old_attr:new_attr, ...) operator] */
              $$=PFalg_project_ ($6, PFarray_last ($4),
				 ((PFalg_proj_t *) $4->base));
            }
        ;

projlist:   projitem
            {
              /* Comma-separated list of (renamed) projection items
	       * (old_attr1:new_attr1, old_attr2:new_attr2, ...).
	       */
              proj_arr = PFarray (sizeof (PFalg_proj_t));
              *((PFalg_proj_t *) PFarray_add (proj_arr)) = $1;
              $$=proj_arr;
            }
        |   projlist COMMA projitem
            {
              /* Comma-separated list of (renamed) projection items
	       * (old_attr1:new_attr1, old_attr2:new_attr2, ...).
	       */
              *((PFalg_proj_t *) PFarray_add ($1)) = $3;
              $$=$1;
            }
        ;

projitem:   ATTREF COLON ATTREF
            {
              /* A (renamed) projection item (new_att:old_att). */
              PFalg_proj_t p = {
                  new               : $1,
                  old               : $3 };
              $$=p;
            }
        ;

eqjoin  :   LSQBR EQJOIN joincond operator operator RSQBR
            {
              /* [|X| (attr=attr) operator operator] */
	      $$=eqjoin ($4, $5, *((PFalg_att_t*) PFarray_at ($3, 0)),
			 *((PFalg_att_t*) PFarray_at ($3, 1)));
            }
        ;

joincond:   LBRACK item EQUAL item RBRACK
            {
              /* Collect the two attributes that make up the equijoin
	       * condition.
	       */
               join_arr = PFarray (sizeof (PFalg_att_t));
               *((PFalg_att_t *) PFarray_add (join_arr)) = $2;
               *((PFalg_att_t *) PFarray_add (join_arr)) = $4;
               $$=join_arr;
            }
        ;

scjoin  :   LSQBR SCJOIN xpath operator operator RSQBR
            {
              /* [/| axis::kind_test operator operator] */
              $$=scjoin ($4, $5, $3);
            }
        ;

xpath   :   axis DBLCOLON ktest
            {
              $3->sem.scjoin.axis = $1;
              $$=$3;
            }
        ;

axis    :   DESC                {$$=aop_desc;}
        |   DESCSELF            {$$=aop_desc_s;}
        |   ANC                 {$$=aop_anc;}
        |   ANCSELF             {$$=aop_anc_s;}
        |   FOL                 {$$=aop_fol;}
        |   PREC                {$$=aop_prec;}
        |   FOLSIBL             {$$=aop_fol_s;}
        |   PRECSIBL            {$$=aop_prec_s;}
        |   CHILD               {$$=aop_chld;}
        |   PARENT              {$$=aop_par;}
        |   SELF                {$$=aop_self;}
        |   ATTRIB              {$$=aop_attr;}
        ;

ktest   :   XMLELEM
            {
              PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));
              ret->sem.scjoin.test = aop_elem;
              $$=ret;
            }
        |   NODES
            {
              PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));
              ret->sem.scjoin.test = aop_node;
              $$=ret;
            }
        |   TEXT
            {
              PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));
              ret->sem.scjoin.test = aop_text;
              $$=ret;
            }
        |   TAG
            {
              PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));
              ret->sem.scjoin.test = aop_name;
              /* TODO: which namespace to use? */
              ret->sem.scjoin.str.qname = PFqname (PFns_local, $1);
              $$=ret;
            }
        ;

crosspr :    LSQBR CROSSPR operator operator RSQBR
            {
              /* [× operator operator] */
              $$=cross ($3, $4);
            }
        ;

disunion:   LSQBR UNION operator operator RSQBR
            {
              /* [U operator operator] */
              $$=disjunion ($3,$4);
            }
        ;

binaryop :   LSQBR PLUS item COLON braclist operator RSQBR
             {
               /* [PLUS attr:(att1,att2) operator] */
	       assert ($5.count == 2);
	       $$ = add ($6, $3, $5.atts[0], $5.atts[1]);
             }
         |   LSQBR MINUS item COLON braclist operator RSQBR
             {
               /* [MINUS attr:(att1,att2) operator] */
	       assert ($5.count == 2);
	       $$ = subtract ($6, $3, $5.atts[0], $5.atts[1]);
             }
         |   LSQBR TIMES item COLON braclist operator RSQBR
             {
               /* [TIMES attr:(att1,att2) operator] */
	       assert ($5.count == 2);
	       $$ = multiply ($6, $3, $5.atts[0], $5.atts[1]);
             }
         |   LSQBR DIVIDE item COLON braclist operator RSQBR
             {
               /* [DIVIDE attr:(att1,att2) operator] */
	       assert ($5.count == 2);
	       $$ = divide ($6, $3, $5.atts[0], $5.atts[1]);
             }
         |   LSQBR LT item COLON braclist operator RSQBR
             {
               /* [< attr:(att1,att2,...) operator] */
	       assert ($5.count == 2);
	       $$ = gt ($6, $3, $5.atts[1], $5.atts[0]); /* gt(b,a) = lt(a,b) */
             }
         |   LSQBR GT item COLON braclist operator RSQBR
             {
               /* [> attr:(att1,att2,...) operator] */
	       assert ($5.count == 2);
	       $$ = gt ($6, $3, $5.atts[0], $5.atts[1]);
             }
         |   LSQBR EQUAL item COLON braclist operator RSQBR
             {
               /* [= attr:(att1,att2,...) operator] */
	       assert ($5.count == 2);
	       $$ = eq ($6, $3, $5.atts[0], $5.atts[1]);
             }
         |   LSQBR AND item COLON braclist operator RSQBR
             {
               /* [AND attr:(att1,att2,...) operator] */
	       assert ($5.count == 2);
	       $$=and ($6, $5.atts[0], $5.atts[1], $3);
             }
         |   LSQBR OR item COLON braclist operator RSQBR
             {
               /* [OR attr:(att1,att2,...) operator] */
	       assert ($5.count == 2);
	       $$=or ($6, $5.atts[0], $5.atts[1], $3);
             }
         ;

unaryop :   LSQBR NOT item COLON LBRACK item RBRACK operator RSQBR
             {
               /* [NOT attr:(attr) operator] */
	       $$=not ($8, $6, $3);
             }
        |   LSQBR NEG item COLON LBRACK item RBRACK operator RSQBR
             {
               /* [NEG attr:(attr) operator] */
	       $$=neg ($8, $6, $3);
             }
        ;

select  :   LSQBR SEL LBRACK item RBRACK operator RSQBR
            {
              /* [SEL (attr) operator] */
              $$ = select_ ($6, $4);
            }
        ;

type    :   LSQBR TYPE list COMMA TYPEREF operator RSQBR
            {
              /* [TYPE (attr,attr,type) operator] */
              assert (PFarray_last ($3) == 2);
	      if (!strcmp ($5, "int"))
                  $$=type ($6, *((PFalg_att_t*) PFarray_at ($3, 1)),
			   *((PFalg_att_t*) PFarray_at ($3, 0)),
			   PFty_integer ());
	      else if (!strcmp ($5, "str"))
                  $$=type ($6, *((PFalg_att_t*) PFarray_at ($3, 1)),
			   *((PFalg_att_t*) PFarray_at ($3, 0)),
			   PFty_string ());
	      else if (!strcmp ($5, "bool"))
                  $$=type ($6, *((PFalg_att_t*) PFarray_at ($3, 1)),
			   *((PFalg_att_t*) PFarray_at ($3, 0)),
			   PFty_boolean ());
	      else if (!strcmp ($5, "dec"))
                  $$=type ($6, *((PFalg_att_t*) PFarray_at ($3, 1)),
			   *((PFalg_att_t*) PFarray_at ($3, 0)),
			   PFty_decimal ());
	      else if (!strcmp ($5, "dbl"))
                  $$=type ($6, *((PFalg_att_t*) PFarray_at ($3, 1)),
			   *((PFalg_att_t*) PFarray_at ($3, 0)),
			   PFty_double ());
	      else if (!strcmp ($5, "node"))
                  $$=type ($6, *((PFalg_att_t*) PFarray_at ($3, 1)),
			   *((PFalg_att_t*) PFarray_at ($3, 0)),
			   PFty_node ());
	      else
		  hskerror ("unknown type in typeswitch");
           }
        ;

table   :   LSQBR TBL LBRACK schmlist RBRACK tuples RSQBR
            {
              /* [TBL ((attr,type),(attr,type))[tuples]] */
              $$=PFalg_lit_tbl_ (PFalg_attlist_ (PFarray_last($4),
						 ((PFalg_att_t *) $4->base)),
				 PFarray_last($6),
				 ((PFalg_tuple_t *) $6->base));
            }
        |   LSQBR TBL LBRACK schmlist RBRACK RSQBR
            {
              /* [TBL ((attr,type),(attr,type))] */
              $$=PFalg_lit_tbl_ (PFalg_attlist_ (PFarray_last($4),
						 ((PFalg_att_t *) $4->base)),
				 0, NULL);
            }
        ;

tuples  :   tuple
            {
              /* Create a comma-separated list of tuples.
	       * This is either the first or only list item.
	       */
              tupl_arr = PFarray (sizeof (PFalg_tuple_t));
              *((PFalg_tuple_t *) PFarray_add (tupl_arr)) = $1;
              $$=tupl_arr;
            }
        |   tuples COMMA tuple
            {
              /* Create a comma-separated list of tuples. */
              *((PFalg_tuple_t *) PFarray_add ($1)) = $3;
              $$=$1;
            }
        ;

tuple   :   LSQBR atomlist RSQBR
            {
              $$=PFalg_tuple_ (PFarray_last($2),
			       ((PFalg_atom_t *) $2->base));
            }
        ;

atomlist:   atom
            {
              /* Create a comma-separated list of atoms.
	       * This is either the first or only list item.
	       */
              atom_arr = PFarray (sizeof (PFalg_atom_t));
              *((PFalg_atom_t *) PFarray_add (atom_arr)) = $1;
              $$=atom_arr;
            }
        |   atomlist COMMA atom
            {
              /* Create a comma-separated list of atoms. */
              *((PFalg_atom_t *) PFarray_add ($1)) = $3;
              $$=$1;
            }
        ;

atom    :   STR     {$$=lit_str ($1);}
        |   NODEREF {$$=(PFalg_atom_t) { .type = aat_node,
					 .val = { .node = $1 } }; }
        |   BLN     {
                      if ($1 == 0)
                          $$=lit_bln(false);
                      else
                          $$=lit_bln(true);
                    }
        |   NAT     {$$=lit_nat ($1);}
        |   INT     {$$=lit_int ($1);}
        |   DEC     {$$=lit_dec ($1);}
        |   DBL     {$$=lit_dbl ($1);}
        ;

schmlist:   schmitem
            {
              /* Create a comma-separated list of attributes.
	       * This is either the first or only list item.
	       */
              schm_arr = PFarray (sizeof (PFalg_att_t));
              *((PFalg_att_t *) PFarray_add (schm_arr)) = $1;
              $$=schm_arr;
	    }
        |   schmlist COMMA schmitem
            {
              /* Create a comma-separated list of attributes. */
              *((PFalg_att_t *) PFarray_add ($1)) = $3;
              $$=$1;
            }
        ;

schmitem:   LBRACK SCHMATT COMMA SCHMTYPE RBRACK
            {
              /* We discard the requested type , because the
	       * type can be derived from the 'atom' rule.
	       */
              $$=$2;
            }
            ;

element :   LSQBR ELEM operator operator operator RSQBR
            {
             /* [ELEM operator operator operator] */
             $$=element ($3, $4, $5);
            }
        ;

relation:   LSQBR REL RELREF RSQBR
            {
             /* [REL name] */
	     $$=doc_tbl ($3);
            }
        ;

sum     :   LSQBR SUM item COLON LBRACK item RBRACK operator RSQBR
            {
              /* [SUM attr:(attr) operator] */
	      $$=sum ($8, $6, $3, NULL);
            }
        |   LSQBR SUM item COLON LBRACK item RBRACK DIVIDE item operator RSQBR
            {
              /* [SUM attr:(attr)/attrlist operator] */
	      $$=sum ($10, $6, $3, $9);
            }
        ;

count   :   LSQBR COUNT item operator RSQBR
            {
              /* [COUNT attr operator] */
	      $$=count ($4, $3, NULL);
            }
        |   LSQBR COUNT item DIVIDE item operator RSQBR
            {
              /* [COUNT attr/attrlist operator] */
	      $$=count ($6, $3, $5);
            }
        ;

differ  :   LSQBR DIFF operator operator RSQBR
            {
              /* [\\ operator operator] */
	      $$=difference ($3, $4);
            }
        ;

distinct:   LSQBR DIST operator RSQBR
            {
              /* [DIST operator] */
	      $$=distinct ($3);
            }
        ;

castint :   LSQBR CINT LBRACK item RBRACK operator RSQBR
            {
              /* [CINT (attr) operator] */
	      $$=cast ($6, $4, aat_int);
            }
        ;

caststr :   LSQBR CSTR LBRACK item RBRACK operator RSQBR
            {
              /* [CSTR (attr) operator] */
              $$=cast ($6, $4, aat_str);
            }
        ;

castdec :   LSQBR CDEC LBRACK item RBRACK operator RSQBR
            {
              /* [CDEC (attr) operator] */
              $$=cast ($6, $4, aat_dec);
            }
        ;

castdbl :   LSQBR CDBL LBRACK item RBRACK operator RSQBR
            {
              /* [CDBL (attr) operator] */
              $$=cast ($6, $4, aat_dbl);
            }
        ;

textnode :  LSQBR TEXT operator operator RSQBR
            {
              /* [TEXT operator operator] */
	      $$=textnode ($3, $4);
            }
        ;

braclist:   LBRACK list RBRACK
            {
              /* Turn array of attributes into full-fledged attribute
	       * list (PFalg_attlist_t).
	       */
              $$=PFalg_attlist_ (PFarray_last($2),
				 ((PFalg_att_t *) $2->base));
            }
        ;

list    :   item
            {
              /* Create a comma-separated list of attributes.
	       * This is either the first or only list item.
	       */
              list_arr = PFarray (sizeof (PFalg_att_t));
              *((PFalg_att_t *) PFarray_add (list_arr)) = $1;
              $$=list_arr;
            }
        |   list COMMA item
            {
              /* Create a comma-separated list of attributes. */
              *((PFalg_att_t *) PFarray_add ($1)) = $3;
              $$=$1;
            }
        ;

item    :   ATTREF    {$$=$1;}
        ;
%%

/* 
 */
PFalg_op_t *
PFhsk_parse (void)
{
    if (hskparse ())		/* parsing failed */
	PFoops(OOPS_FATAL, "parsing of Haskell XQuery-to-Algebra output"
	       "failed");

    return root;
}
