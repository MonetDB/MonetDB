/* ==================================================================== */
/* == Program: rewrite                                               == */
/* == Author: Peter Boncz                                            == */
/* == Function:                                                      == */
/* == a One Afternoon's Hack that rewrites typical Monet macro calls == */
/* == in order to achieve better optimizable code for gcc.           == */
/* ==================================================================== */

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

/* ==================================================================== */
/* == what does it do?                                               == */
/* ==                                                                == */
/* == replaces 'macro loops' consisting of a 'macro call', followed  == */
/* == by a C-block, by code that is supposed to be faster.           == */
/* == Incorporated here are the rewrite rules for the                == */
/* ==      BATloop(<b>,<p>,<q>) <block>                              == */
/* ==      BATloopFast(<b>,<p>,<q>,<x>) <block>                      == */
/* ==      DELloop(<b>,<p>,<q>) <block>                              == */
/* == macros. You can add more..                                     == */
/* ==================================================================== */
/* == For each rewrite rule you should supply:                       == */
/* == 1) a head code. It is inserted in place of the macro call.     == */
/* == 2) a tail code. It is inserted after the C-block.              == */
/* == 3) substitutions. They are done in the C-block;                == */
/* ==                                                                == */
/* == You can use these special identifiers in all rewrite code:     == */
/* == The @1,@2,..,@9 stand for the first to ninth actual parameters == */ 
/* == to the macro. The special variable @0 is unique number for     == */ 
/* == every macro-rewrite.                                           == */
/* ==                                                                == */
/* == Don't forget to compile the results with gcc -O3, otherwise it == */
/* == might even get slower.                                         == */
/* ==================================================================== */

char *batloopHDR =
"{	@2 = BUNfirst(@1); @3 = BUNlast(@1);\\\n\
    {	register int bunsize@0 = BUNsize(@1);\\\n\
       	register char* hcur@0  = @2+@1->hloc;\\\n\
       	register char* hbase@0 = @1->hheap.base;\\\n\
       	register char* tcur@0  = @2+@1->tloc;\\\n\
       	register char* tbase@0 = @1->theap.base;\\\n\
       	register char* hval@0, *tval@0;\\\n\
       	register int hloc@0 = @1->hloc;\\\n\
       	register int tloc@0 = @1->tloc;\\\n\
	while(@2 < @3) {\\\n\
		hval@0 = hbase@0 + *(int*) hcur@0;\\\n\
		tval@0 = tbase@0 + *(int*) tcur@0;\\\n";

char *batloopTAIL =
"		@2 += bunsize@0;\\\n\
		hcur@0 += bunsize@0;\\\n\
		tcur@0 += bunsize@0;\\\n\
}    }  }\\\n";

char *batloopSUBST[] = 
{	 "BUNhvar(@1,@2)", "hval@0", 
	 "BUNhloc(@1,@2)", "hcur@0",
	 "BUNtvar(@1,@2)", "tval@0", 
	 "BUNtloc(@1,@2)", "tcur@0",
	 "BUNhead(@1,@2)", "(hbase@0?hval@0:hcur@0)", 
	 "BUNtail(@1,@2)", "(tbase@0?tval@0:tcur@0)", 
	 "BUNhead(@1,@#1)", 
  "(hbase@0?(hbase@0 + *((int*) ((char*) @#1 + hloc@0))):((char*) @#1 + hloc@0))",
	 "BUNtail(@1,@#1)", 
  "(tbase@0?(tbase@0 + *((int*) ((char*) @#1 + tloc@0))):((char*) @#1 + tloc@0))",
	 "BUNhloc(@1,@#1)", "((char*) @#1 + hloc@0)", 
	 "BUNhvar(@1,@#1)", "(hbase@0 + *((int*) ((char*) @#1 + hloc@0)))", 
	 "BUNtloc(@1,@#1)", "((char*) @#1 + tloc@0)", 
	 "BUNtvar(@1,@#1)", "(tbase@0 + *((int*) ((char*) @#1 + tloc@0)))", 
		0, 0, };

char *delloopHDR =
"{    @2 = @1->deleted; @3 = @1->batHole;\\\n\
     {	register int bunsize@0 = BUNsize(@1);\\\n\
       	register char* hcur@0  = @2+@1->hloc;\\\n\
       	register char* hbase@0 = @1->hheap.base;\\\n\
       	register char* tcur@0  = @2+@1->tloc;\\\n\
       	register char* tbase@0 = @1->theap.base;\\\n\
       	register char* hval@0, *tval@0;\\\n\
       	register int hloc@0 = @1->hloc;\\\n\
       	register int tloc@0 = @1->tloc;\\\n\
	while(@2 < @3) {\\\n\
		hval@0 = hbase@0 + *(int*) hcur@0;\\\n\
		tval@0 = tbase@0 + *(int*) tcur@0;\\\n";

char *idxloopHDR =
"{  {   register char* hbase@0 = @1->hheap.base;\\\n\
        register char* tbase@0 = @1->theap.base;\\\n\
        register int hloc@0 = @1->hloc;\\\n\
        register int tloc@0 = @1->tloc;\\\n\
        register char* hcur@0, *tcur@0;\\\n\
        register char* hval@0, *tval@0;\\\n\
        for(;@2 <= @3; @2++) {\\\n\
                hcur@0 = *((char**) @2) + hloc@0;\\\n\
                tcur@0 = *((char**) @2) + tloc@0;\\\n\
                hval@0 = hbase@0 + *(int*) hcur@0;\\\n\
                tval@0 = tbase@0 + *(int*) tcur@0;\\\n";

char *idxfromHDR =
"{  @2 = IDXfndfirst(@1, @2);\\\n\
    {   register char* hbase@0 = @1->hheap.base;\\\n\
        register char* tbase@0 = @1->theap.base;\\\n\
        register int hloc@0 = @1->hloc;\\\n\
        register int tloc@0 = @1->tloc;\\\n\
        register char* hcur@0, *tcur@0;\\\n\
        register char* hval@0, *tval@0;\\\n\
        for((@2)++; @2 <= @3; @2++) {\\\n\
                hcur@0 = *((char**) @2) + hloc@0;\\\n\
                tcur@0 = *((char**) @2) + tloc@0;\\\n\
                hval@0 = hbase@0 + *(int*) hcur@0;\\\n\
                tval@0 = tbase@0 + *(int*) tcur@0;\\\n";

char *idxstartHDR =
"{  {   register char* hbase@0 = @1->hheap.base;\\\n\
        register char* tbase@0 = @1->theap.base;\\\n\
        register int hloc@0 = @1->hloc;\\\n\
        register int tloc@0 = @1->tloc;\\\n\
        register char* hcur@0, *tcur@0;\\\n\
        register char* hval@0, *tval@0;\\\n\
        for(@2 = IDXfirst(@1); @2 <= @3; @2++) {\\\n\
                hcur@0 = *((char**) @2) + hloc@0;\\\n\
                tcur@0 = *((char**) @2) + tloc@0;\\\n\
                hval@0 = hbase@0 + *(int*) hcur@0;\\\n\
                tval@0 = tbase@0 + *(int*) tcur@0;\\\n";

char *idxendHDR =
"{  {   register char* hbase@0 = @1->hheap.base;\\\n\
        register char* tbase@0 = @1->theap.base;\\\n\
        register int hloc@0 = @1->hloc;\\\n\
        register int tloc@0 = @1->tloc;\\\n\
        register char* hcur@0, *tcur@0;\\\n\
        register char* hval@0, *tval@0;\\\n\
        for(@2 = IDXlast(@1); @2 > @3; @2--) {\\\n\
                hcur@0 = *((char**) @2) + hloc@0;\\\n\
                tcur@0 = *((char**) @2) + tloc@0;\\\n\
                hval@0 = hbase@0 + *(int*) hcur@0;\\\n\
                tval@0 = tbase@0 + *(int*) tcur@0;\\\n";

char *sortloopHDR =
"{  @2 = (ATOMcmp(@1->ttype,@4,ATOMnil(@1->ttype))?\\\n\
        SORTfndfirst(@1,@4):BUNfirst(@1));\\\n\
    @3 = (ATOMcmp(@1->ttype,@5,ATOMnil(@1->ttype))?\\\n\
        SORTfndlast(@1,@5):BUNlast(@1));\\\n\
    {   register int bunsize@0 = @6 = BUNsize(@1);\\\n\
        register char* hcur@0  = @2+@1->hloc;\\\n\
        register char* hbase@0 = @1->hheap.base;\\\n\
        register char* tcur@0  = @2+@1->tloc;\\\n\
        register char* tbase@0 = @1->theap.base;\\\n\
        register char* hval@0, *tval@0;\\\n\
        register int hloc@0 = @1->hloc;\\\n\
        register int tloc@0 = @1->tloc;\\\n\
        while(@2 < @3) {\\\n\
                hval@0 = hbase@0 + *(int*) hcur@0;\\\n\
                tval@0 = tbase@0 + *(int*) tcur@0;\\\n";

char *idxloopTAIL =  "}   }   }\\\n";

char *hashloop_intHDR =
"{	register int hash@0 = simple_HASH(@4, int, int);\\\n\
	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3=@2->hash[ABS(hash@0)%@2->mask]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    @5 = ((char*)  base@0) + bunsize@0*@3;\\\n\
	    if (*(int*) @4 == *(int*) (((char*) @5) + hloc@0))\\\n";

char *hashloop_shtHDR =
"{	register int hash@0 = simple_HASH(@4, sht, int);\\\n\
	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3=@2->hash[ABS(hash@0)%@2->mask]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    @5 = ((char*)  base@0) + bunsize@0*@3;\\\n\
	    if (*(sht*) @4 == *(sht*) (((char*) @5) + hloc@0))\\\n";

char *hashloop_chrHDR =
"{	register int hash@0 = simple_HASH(@4, chr, int);\\\n\
	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3=@2->hash[ABS(hash@0)%@2->mask]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    @5 = ((char*)  base@0) + bunsize@0*@3;\\\n\
	    if (*(char*) @4 == *(((char*) @5) + hloc@0))\\\n";

char *hashloop_lngHDR =
"{	register lng hash@0 = simple_HASH(@4, lng, int);\\\n\
	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3=@2->hash[(int) (ABS(hash@0)%@2->mask)]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    @5 = ((char*)  base@0) + bunsize@0*@3;\\\n\
	    if (*(lng*) @4 == *(lng*) (((char*) @5) + hloc@0))\\\n";

char *hashloop_strHDR =
"{	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3 = @2->hash[strHash(@4, lng, int)%@2->mask]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    @5 = ((char*)  base@0) + bunsize@0*@3;\\\n\
	    if (strcmp(@4, hbase@0+ *(int*) (((char*) @5) + hloc@0))==0)\\\n";

char *hashloop_locHDR =
"{	register int (*cmp@0)() = BATatoms[@1->htype].atomCmp;\\\n\
	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3 = @2->hash[HASHprobe(@1->hhash, @4)]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    @5 = ((char*)  base@0) + bunsize@0*@3;\\\n\
	    if ((*cmp@0)(@4, ((char*) @5) + hloc@0)==0)\\\n";

char *hashloop_varHDR =
"{	register int (*cmp@0)() = BATatoms[@1->htype].atomCmp;\\\n\
	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3=@2->hash[HASHprobe(@1->hhash,@4)]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    @5 = ((char*) base@0) + bunsize@0*@3;\\\n\
	    if ((*cmp@0)(@4, hbase@0+ *(int*) (((char*) @5) + hloc@0))==0)\\\n";

char *hashloopHDR =
"{	register int (*cmp@0)() = BATatoms[@1->htype].atomCmp;\\\n\
	register int *link@0 = @2->link;\\\n\
	register int hloc@0 = @1->hloc;\\\n\
	register int tloc@0 = @1->tloc;\\\n\
	register int bunsize@0 = BUNsize(@1);\\\n\
	register char* base@0 = @1->batBuns->base;\\\n\
	register char* hbase@0 = @1->hheap.base;\\\n\
	register char* tbase@0 = @1->theap.base;\\\n\
	for (@3 = @2->hash[HASHprobe(@1->hhash, @4)]; @3 >= 0; @3 = link@0[@3]) {\\\n\
	    register char *ptr@0 = ((char*) base@0) + bunsize@0*@3;\\\n\
	    if ((*cmp@0)(@4, hbase@0?(hbase@0+ *(int*) (ptr@0 + hloc@0)):(ptr@0 + hloc@0))==0)\\\n";

char *hashloopSUBST[] =
{	 "BUNhloc(@1,@#1)", "((char*) @#1 + hloc@0)",
	 "BUNhvar(@1,@#1)", "(hbase@0 + *((int*) ((char*) @#1 + hloc@0)))",
         "BUNhead(@1,@#1)", 
  "(hbase@0?(hbase@0 + *((int*) ((char*) @#1 + hloc@0))):((char*) @#1 + hloc@0))",
         "BUNtail(@1,@#1)", 
  "(tbase@0?(tbase@0 + *((int*) ((char*) @#1 + tloc@0))):((char*) @#1 + tloc@0))",
         "BUNtloc(@1,@#1)", "((char*) @#1 + tloc@0)",
         "BUNtvar(@1,@#1)", "(tbase@0 + *((int*) ((char*) @#1 + tloc@0)))",
                0, 0, };

char *hashloop_locSUBST[] =
{	 "BUNhloc(@1,@#1)", "((char*) @#1 + hloc@0)",
         "BUNhead(@1,@#1)", "((char*) @#1 + hloc@0)",
         "BUNtail(@1,@#1)", 
  "(tbase@0?(tbase@0 + *((int*) ((char*) @#1 + tloc@0))):((char*) @#1 + tloc@0))",
         "BUNtloc(@1,@#1)", "((char*) @#1 + tloc@0)",
         "BUNtvar(@1,@#1)", "(tbase@0 + *((int*) ((char*) @#1 + tloc@0)))",
                0, 0, };

char *hashloop_varSUBST[] =
{	 "BUNhvar(@1,@#1)", "(hbase@0 + *((int*) ((char*) @#1 + hloc@0)))",
	 "BUNhead(@1,@#1)", "(hbase@0 + *((int*) ((char*) @#1 + hloc@0)))",
         "BUNtail(@1,@#1)", 
  "(tbase@0?(tbase@0 + *((int*) ((char*) @#1 + tloc@0))):((char*) @#1 + tloc@0))",
         "BUNtloc(@1,@#1)", "((char*) @#1 + tloc@0)",
         "BUNtvar(@1,@#1)", "(tbase@0 + *((int*) ((char*) @#1 + tloc@0)))",
                0, 0, };


char *hashloopTAIL = "}       }\\\n";

/* ================================================================== */
/* == rewrite.c: the code. let's kick off defining things.         == */ 
/* ================================================================== */

char TMPFILE[1024];
#define isident(x)  (isalpha(x) || (x=='#') || (x=='*') || (x=='-'))
#define isword(x)   (isalnum(x) || (x=='_') || (x=='#') || isop(x))
#define isop(x)	    ((x=='.') || (x=='*') || (x=='-') || (x=='>') ||\
		     (x=='^') || (x=='/') || (x=='%') || (x=='<') || (x=='='))
#define TYPE_word	1
#define TYPE_ctrl	2
#define TYPE_space	3
#define MAXTOKENS	500000
#define STRLEN		512	
#define TEN		28 /* ain't life sweet? */

#ifdef ANSI_C
#       define  _(x)    (x)
#else
#       define  _(x)    ()
#endif
 
/* ================================================================== */
/* == some useful typedefs                                         == */ 
/* ================================================================== */

typedef char name_t[STRLEN];

typedef struct {
	name_t 	in;	
	name_t 	out;	
	int	nparams;
	int	params[TEN];
	name_t  act[TEN];
} subst_t;
	
typedef struct {
	char 	str[STRLEN];	
	int	nparams;
	char 	*head;
	char 	*tail;
	subst_t subst[TEN];
	int	nsub;
} pattern_t;

typedef struct {
	int  type;
	char *str;
	int  insert;	
} token_t;

/* ================================================================== */
/* == forward declarations.                                        == */
/* ================================================================== */
 
subst_t parse_substs _((char* in, char* out));
        /* parse the substitution strings for table init. return result. 
	 */
void    act_substs   _((name_t *act,int pat));
        /* forall substs of a pattern: install actual params in their structs. 
         */
int     tokenize     _(());
        /* partition the C-file into simple tokens.  return # found tokens.
	 */
void    match        _((int cur,    int end));
	/* try to match macro's in the text between token 'cur' and 'end'.
	 */
int     next_pattern _((int* start, int* end,  name_t* act));
	/* bool: find next pattern between 'start' and 'end', return into 'act'.
         */
int     check_params _((int cur,    int end,   int nparams, name_t* act));
        /* check whether the params described in 'act' fit @ token 'cur'.
	 */
int     next_block   _((int cur,    int end));
	/* find the next C statement after 'cur'. return token number.
	 */
char*   next_subst   _((int* start, int* end,  act, int pat));
	/* find the next substitution in the C-block between 'start' and 'end'.
 	 */
void    insert_token _((int cur,    char* str, name_t* act));
	/* insert a text-token at position 'cur', using actual params 'act'.
         */
void    write_tokens _((int start,  int end,   FILE* fp));
	/* write the token-list back as a file.
         */

/* ================================================================== */
/* == global variables                                             == */ 
/* ================================================================== */

pattern_t pt[TEN];          /* the macros we'll looking for */ 
int       npats = 0;        /* # of them */
token_t   tk[MAXTOKENS];    /* tokens in our file */
int       ntokens;          /* # of them */
char      *buf, *tokenbuf;  /* buf for textual description of tokens */
char      substbuf[2000000]; /* scratch space used for 2b inserted text */
char      *substcur;        /* ptr in the above */
int       rewrite = 0;      /* #of rewrite actions performed so far. */

/*=================================================================== */
/* == routines                                                     == */ 
/*=================================================================== */

/* parse the substitution strings for table init. return result. 
 */
subst_t parse_substs(char *in, char* out)
{
	subst_t s;
	int i = 0;

	s.nparams = 0;
	strcpy(s.out, out);
	while(*in && (*in != '('))
		s.in[i++] = *in++;
	s.in[i] = 0;
	while(*in && (*in != ')')) {
		while(*in && (*in++ != '@'));
		if (*in == '#') {
		 	s.params[s.nparams++] = -(*++in-'0');
		} else if (isdigit(*in)) {
			s.params[s.nparams++] = *in-'0';
		}
		in++;
	}
	return s;
}

/* partition the C-file into simple tokens.  return # found tokens.
 */
int tokenize()
{
	char *p1 = buf, *p2 = tokenbuf;
	int i = 0;

	/* simplistic C: we know only space, words and ctrl tokens */
	while(*p1) {
		tk[i].str = p2;
		if (isspace(*p1)) {
			tk[i].type = TYPE_space;
			do {
				*p2++ = *p1++;
			} while (isspace(*p1));
		} else if ((p1[0] == '/') && (p1[1] == '*')) {
			tk[i].type = TYPE_space;
			*p2++ = *p1++;
			do {
				*p2++ = *p1++;
				if ((p1[0] == '*') && (p1[1] == '/')) {
					*p2++ = *p1++;
					*p2++ = *p1++;
					break;
				}
			} while (*p1);
		} else if (isident(*p1)) {
			tk[i].type = TYPE_word;
			do {
				*p2++ = *p1++;
			} while (isword(*p1));
		} else {
			tk[i].type = TYPE_ctrl;
			*p2++ = *p1++;
		}
		*p2++ = tk[i++].insert = 0; /* dirty harry was here! */
	}
	return ntokens = i;
}

/* jointokens: join word(A) binop word(B) -> word(A binop B) */
/* trimtokens: (/, SPACE ( SPACE ( SPACE (word)..) -> (/,word  */

/* get next ctrl token, skip space tokens. */
#define MATCH_CTRL(x) {	while(tk[j].type == TYPE_space)		\
				if (++j > end) return 0;	\
			if ((tk[j].type != TYPE_ctrl) ||	\
			    (*tk[j].str != x)) return 0; 	\
			if (++j > end) return 0;		}

/* check whether the params described in 'act' fit @ token 'cur'.
 */
int check_params(int cur, int end, int nparams, name_t *act)
{
	int i, j=cur;

	MATCH_CTRL('(');
	for(i=1; i <= nparams; i++) {
		while(tk[j].type == TYPE_space) 
			if (++j > end) return 0;
		if (tk[j].type != TYPE_word) return 0;
		if (act[i][0]) {
			/* if already there: actual params should match */
			if (strcmp(act[i], tk[j].str)) return 0;
		} else {
			/* install actual param */
			strcpy(act[i], tk[j].str);
		}
		if (++j > end) return 0;
		if (i < nparams) MATCH_CTRL(',');
	}
	MATCH_CTRL(')');

	/* something matches: delete old content (this is out of place here) 
         */
	for(i=cur-1; i<j; i++) {
		tk[i].type = TYPE_space;
		*tk[i].str = 0;
	}		
	return j;
}



/* bool: find next pattern between 'start' and 'end', return into 'act'.
 */
int next_pattern(int* start, int* end, name_t* act)
{
	int i,j,k,cur;
	for(cur = *start; cur < *end; cur++) {
		if (tk[cur].type != TYPE_word) continue; 
		/* check all patterns here */
		for(i = 0; i < npats; i++) {
			if (strcmp(pt[i].str, tk[cur].str)) continue;
			for(j=1; j<TEN; j++) act[j][0] = 0;
			if (k = check_params(cur+1,*end,pt[i].nparams,act))
			{
				*end = k;
				*start = cur;
				return i;
			}
		}
	}
	return -1;
}


/* forall substs of a pattern: install actual params in their structs. 
 */
void act_substs(name_t *act, int pat)
{
	int i,j,cur;
	subst_t *s;

	/* for all substitutions: install actual params in their structs */	
	for(i = 0, s = pt[pat].subst; i < pt[pat].nsub; i++, s++) {
		for(j = 0; j < s->nparams; j++)
			if (s->params[j] < 0) { /* wildcard? */
				s->act[j+1][0] = 0;
			} else {
				strcpy(s->act[j+1], act[s->params[j]]);
			}
	}
}

char *filltemplate(char *template, subst_t *s)
{
	static char output[4096];
	char *p = output;
	
	while(*template) {
		if (*template == '@' && template[1] == '#') {
			int j,i = template[2] - '0'; 	
			for(j=0; j<s->nparams; j++) 
			if (-s->params[j] == i) {
				char *p2 = s->act[j+1]; 
				while(*p2) *p++ = *p2++; 
				template += 3;
				break;
			}
		} else {
			*p++ = *template++;
		} 
	}
	*p = 0;
	return output;
}  

/* find the next substitution in the C-block between 'start' and 'end'.
 */
char *next_subst(int *start, int* end, name_t *act, int pat)
{
	int i,j,cur;
	subst_t *s;

	/* matching the substitutions. return at first hit.
	 */
	for(cur=*start; cur < *end; cur++) {
		if (tk[cur].type != TYPE_word) continue; 
		for(i = 0, s = pt[pat].subst; i < pt[pat].nsub; i++, s++) {
			if ((strcmp(s->in, tk[cur].str)==0) &&
			    (j = check_params(cur+1,*end,s->nparams,s->act)))
			{
				*end = j;	
				*start = cur;	
				return filltemplate(s->out,s);
			}
		}
	}
	return 0;
}

/* find the next C statement after 'cur'. return token number.
 */
int next_block(int cur, int end)
{
	int nesting = 0;
	for(;cur < end; cur++) {
		if (tk[cur].type != TYPE_ctrl) 
			continue;
		if (*tk[cur].str == '{') {
			nesting++;
		} else if (*tk[cur].str == '}') {
			if (--nesting == 0) break;
		} else if ((nesting == 0) && (*tk[cur].str == ';')) {
			break;
		}
	}
	return cur;
	
}

/* insert a text-token at position 'cur', using actual params 'act'.
 */
void insert_token(int cur, char *str, name_t* act)
{
	int new;
	
	/* find next token where to add */
	while(tk[cur].insert) 
		cur = tk[cur].insert;

	/* find empty token, fill it */
	tk[cur].insert = new = ntokens++;
	tk[new].type = TYPE_space;
	tk[new].insert = 0;
	tk[new].str = substcur;

	/* install text, filling in actual params */
	do { 
		if (*str == '@') {
			char *ptr = act[*++str - '0'];
			while(*ptr) *substcur++ = *ptr++;
		} else *substcur++ = *str; 
	} while(*++str); 
	*substcur++ = 0;
}

/* write the token-list back as a file.
 */
void write_tokens(int start, int end, FILE* fp)
{
	int i,j;
	for(i=j=start; i < end; j=++i) {
		fputs(tk[i].str, fp);
		while(j = tk[j].insert)
			fputs(tk[j].str, fp);
	}
}

/* match(): the heart of the matter 
 * try to match macro's in the text between token 'cur' and 'end'.
 */
void match(int cur, int end)
{
	int pat, blk, next=end;
	name_t actual[TEN] = { 0 };
	char *str;

	/* find next pattern that matches */
	while((pat = next_pattern(&cur,&next,actual)) >= 0) {
		/* the @0 actual param: a unique identifier */
		sprintf(actual[0], "%d", rewrite++);

		/* find C-block after the macro */
		blk = next_block(next,end);

		/* match inside it (it may contain macros) */
		match(next, blk);

		/* adapt the substitutions of pat for the actual params */
		act_substs(actual,pat);

		/* insert head text at old macro position */
		insert_token(cur, pt[pat].head, actual);

		cur = next; next = blk;
		while(str = next_subst(&cur, &next, actual, pat)) {
			/* insert substitution inside C-block */
			insert_token(cur, str, actual);
			cur = next; next = blk;
		}
		/* insert tail text after C-block */
		insert_token(blk, pt[pat].tail, actual);
		cur = blk; next = end;
	}
}

	
int main(argc,argv)
int argc;
char **argv;
{
	FILE *fp_in, *fp_ou;
	char **ptr;
	int i,j;

	/* insert BATloop transformation into tables */
	strcpy(pt[npats].str, "BATloop");
	pt[npats].nparams = 3;
	for(ptr=batloopSUBST,i=0; *ptr; ptr+=2,i++)
		pt[npats].subst[i] = parse_substs(ptr[0],ptr[1]);
	pt[npats].nsub = i;
	pt[npats].head = batloopHDR;
	pt[npats++].tail = batloopTAIL;

	/* insert BATloopFast transformation into tables */
	pt[npats] = pt[npats-1];
	pt[npats].nparams = 4;
	strcpy(pt[npats++].str, "BATloopFast");

	/* insert DELloop transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "DELloop");
	pt[npats++].head = delloopHDR;

	/* insert SORTloop transformation into tables */
	pt[npats] = pt[npats-1];
	pt[npats].nparams = 6;
	strcpy(pt[npats].str, "SORTloop");
	pt[npats++].head = sortloopHDR;

	/* insert IDXloop transformation into tables */
	pt[npats] = pt[0];
	strcpy(pt[npats].str, "IDXloop");
	pt[npats].head = idxloopHDR;
	pt[npats++].tail = idxloopTAIL;

	/* insert IDXfrom transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "IDXfrom");
	pt[npats++].head = idxfromHDR;

	/* insert IDXstart transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "IDXstart");
	pt[npats++].head = idxstartHDR;

	/* insert IDXend transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "IDXend");
	pt[npats++].head = idxendHDR;

	/* insert HASHloop transformation into tables */
	strcpy(pt[npats].str, "HASHloop");
	pt[npats].nparams = 4;
	for(ptr=hashloopSUBST,i=0; *ptr; ptr+=2,i++)
		pt[npats].subst[i] = parse_substs(ptr[0],ptr[1]);
	pt[npats].nsub = i;
	pt[npats].head = hashloopHDR;
	pt[npats++].tail = hashloopTAIL;

	/* insert HASHloop_loc transformation into tables */
	strcpy(pt[npats].str, "HASHlooploc");
	pt[npats].nparams = 5;
	for(ptr=hashloop_locSUBST,i=0; *ptr; ptr+=2,i++)
		pt[npats].subst[i] = parse_substs(ptr[0],ptr[1]);
	pt[npats].nsub = i;
	pt[npats].head = hashloop_locHDR;
	pt[npats++].tail = hashloopTAIL;

	/* insert HASHloop_chr transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "HASHloop_chr");
	pt[npats++].head = hashloop_chrHDR;

	/* insert HASHloop_bit transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats++].str, "HASHloop_bit");

	/* insert HASHloop_sht transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "HASHloop_sht");
	pt[npats++].head = hashloop_shtHDR;

	/* insert HASHloop_int transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "HASHloop_int");
	pt[npats++].head = hashloop_intHDR;

	/* insert HASHloop_bat transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats++].str, "HASHloop_bat");

	/* insert HASHloop_ptr transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats++].str, "HASHloop_ptr");

	/* insert HASHloop_oid transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats++].str, "HASHloop_oid");

	/* insert HASHloop_flt transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats++].str, "HASHloop_flt");

	/* insert HASHloop_lng transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "HASHloop_lng");
	pt[npats++].head = hashloop_lngHDR;

	/* insert HASHloop_dbl transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats++].str, "HASHloop_dbl");

	/* insert HASHloop_var transformation into tables */
	strcpy(pt[npats].str, "HASHloopvar");
	pt[npats].nparams = 5;
	for(ptr=hashloop_varSUBST,i=0; *ptr; ptr+=2,i++)
		pt[npats].subst[i] = parse_substs(ptr[0],ptr[1]);
	pt[npats].nsub = i;
	pt[npats].head = hashloop_varHDR;
	pt[npats++].tail = hashloopTAIL;

	/* insert HASHloop_str transformation into tables */
	pt[npats] = pt[npats-1];
	strcpy(pt[npats].str, "HASHloop_str");
	pt[npats++].head = hashloop_strHDR;

	for(i = 1; i < argc; i++) {
		struct stat st;
		int ok = 0;
		char *s = argv[1]+strlen(argv[1]);
		while (s>argv[1] && s[-1] != '/') s--;
	
		sprintf(TMPFILE, "%s.rewrite.tmp",s);
		if (!(fp_in = fopen(argv[1],"r")))
			continue;
		if (!(fp_ou = fopen(TMPFILE,"w")))
			goto xit1;
		fstat(fileno(fp_in), &st);
		if ((buf = (char*) malloc(st.st_size+1)) == 0) 
			goto xit2;
		if ((tokenbuf = (char*) malloc(st.st_size+MAXTOKENS)) == 0) 
			goto xit3;
		if (fread(buf, 1, st.st_size, fp_in) != st.st_size) 
			goto xit4;

		buf[st.st_size] = 0;
		j = tokenize();
/*		j = jointokens(j); */
		substcur = substbuf;

		match(0,j);
		write_tokens(0,j,fp_ou);
		ok=1;

xit4:		free(tokenbuf);
xit3:		free(buf);
xit2:		fclose(fp_ou);
xit1:		fclose(fp_in);	

		if (ok) {
			unlink(argv[1]);
			link(TMPFILE,argv[1]);
			unlink(TMPFILE);
		}
	}
	return 0;
}

