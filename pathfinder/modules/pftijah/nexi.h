/**
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the PfTijah Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://dbappl.cs.utwente.nl/Legal/PfTijah-1.1.html
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the PfTijah system.
 * 
 * The Initial Developer of the Original Code is the "University of Twente".
 * Portions created by the "University of Twente" are 
 * Copyright (C) 2006-2007 "University of Twente".
 * All Rights Reserved.
 *
 * Author(s): Vojkan Mihajlovic
 *	      Jan Flokstra
 *            Henning Rode
 *            Roel van Os
 *
 *  Main module for generating logical query plans and MIL query plans 
 *  from NEXI queries
 */

/*Constants for preprocessing and processing of NEXI queries */
#define CO_TOPIC 1
#define CAS_TOPIC 2

#define PLAIN 1
#define NO_MODIFIER 2
#define VAGUE_NO_PHRASE 3
#define STRICT_NO_PHRASE 4
#define VAGUE_MODIF 5
#define STRICT_MODIF 6

#define NO_STOP_STEM 1
#define STOP_WORD 2
#define STEMMING 3
#define STOP_STEM 4

#define ENGLISH 1
#define DUTCH 2

#define BASIC 1
#define SIMPLE 2
#define ADVANCED 3

#define ZERO 1
#define ONE 2


/* Generator type */
#define ASPECT  1
#define COARSE  2
#define COARSE2 3


/* The processing bounds:
1. maximum number of variables in the MIL queries (in INEX 2004 it is 60)
2. maximum number of queries for continual processing (in INEX 2004 36)
*/
#define MAX_VARS 150
#define MAX_QUERIES 75

/* Bounds for stack structures and for arrays */
#define STACK_MAX 400
#define TERM_LENGTH 100

/* Retrieval model */
#define MODEL_BOOL 1
#define MODEL_LM 2
#define MODEL_LMS 3
#define MODEL_TFIDF 4
#define MODEL_OKAPI 5
#define MODEL_GPX 6
#define MODEL_LMA 7
#define MODEL_LMSE 8
#define MODEL_LMVFLT 9
#define MODEL_LMVLIN 10

#define MODEL_NLLR 11
#define MODEL_PRF 12

#define MODEL_I_GMM 8

#define OR_SUM 1
#define OR_MAX 2
#define OR_PROB 3
#define OR_EXP 4
#define OR_MIN 5
#define OR_PROD 6

#define AND_PROD 1
#define AND_MIN 2
#define AND_SUM 3
#define AND_EXP 4
#define AND_MAX 5
#define AND_PROB 6

#define UP_SUM 1
#define UP_MAX 2
#define UP_WSUMD 3
#define UP_WSUMA 4

#define DOWN_SUM 1
#define DOWN_MAX 2
#define DOWN_WSUMD 3
#define DOWN_WSUMA 4

#define IMAGE_AVG 1

#define NO_PRIOR 1
#define LENGTH_PRIOR 2
#define LOG_NORMAL_PRIOR 3
#define LOG_LENGTH_PRIOR 4

#define PREF_ELEM_SIZE 1000

#define NAME_LENGTH 30

/* Relevance feedback */
#define NO_REL_FEEDBACK 1
#define RF_JOURNAL 2
#define RF_ELEMENT 3
#define RF_SIZE 4
#define RF_JOURNAL_ELEM 5
#define RF_JOURNAL_SIZE 6
#define RF_ELEMENT_SIZE 7
#define RF_ALL 8

/* Constants for encoding commands */
#define EMPTY -1

/* parsing and temporal */
#define UNION 6
#define INTERSECT 7
#define VALUE 8

#define GREATER 10
#define LESS 11
#define EQUAL 12
#define GEQ 13
#define LEQ 14

#define CURRENT 21
#define DSC 22
#define STAR 23
#define PARAMNODES 99
#define JOURNAL_ROOT 24
#define VAGUE 25
#define IMAGE 26

#define GR 31
#define LS 32
#define EQ 33
#define AND 34
#define OR 35

#define ABOUT 41
#define OPEN 42
#define CLOSE 43
#define CTX 44
#define COMMA 45
#define OB 46
#define CB 47
#define QUOTE 48
#define STRUCT_OR 49

#define IMAGE_ABOUT 86

// Coarse 2
#define CREATE_QUERY_OBJECT 92
#define QUERY_ADD_TERM 93
#define QUERY_ADD_MODIFIER 94
#define P_SELECT_NODE_Q 95

#define NORMAL 71
#define PLUS 72
#define MINUS 73
#define MUST 74
#define MUST_NOT 75

#define QUERY_END 100

/* algebra */
#define ROOT 1

#define SELECT_NODE 1
#define SELECT_TERM 2
#define SELECT_ADJ 3
#define CONTAINING 4
#define CONTAINED_BY 5
#define SELECT_NODE_VAGUE 9

/* #define P_SELECT 51  not used, but conflicts with /usr/include/sys/proc.h on Darwin */
#define P_CONTAINING 52
#define P_CONTAINED_BY 53
#define P_UNION 54
#define P_INTERSECT 55
#define NEAR_VAL 56
#define P_PRIOR 57
#define P_SELECT_NODE_T 58

#define P_CONTAINING_T 61
#define P_NOT_CONTAINING_T 62
#define MUST_CONTAIN_T 63
#define MUST_NOT_CONTAIN_T 64
#define SCALE 65
#define P_AND 66
#define P_OR 67
#define P_CONTAINING_I 68
#define P_NOT_CONTAINING_I 69

#define P_SELECT_GR 81
#define P_SELECT_LS 82
#define P_SELECT_EQ 83
#define P_SELECT_GEQ 84
#define P_SELECT_LEQ 85

#define P_SELECT_IMAGE 89

#define P_ADJ 90
#define P_ADJ_NOT 91

/* types for algebra operators (commands) */
typedef int command;
typedef int bool;

#define FALSE 0
#define TRUE 1

/*
 * Comand tree structure
 */
typedef struct command_tree command_tree;
struct command_tree {
  int number;
  int operator;
  command_tree *left;
  command_tree *right;
  char argument[TERM_LENGTH];
};


/*
 * Text retrieval model structure
 */
typedef struct struct_RMT struct_RMT;
struct struct_RMT {
  int qnumber;
  int model;
  int or_comb;
  int and_comb;
  int up_prop;
  int down_prop;
  char e_class[TERM_LENGTH];
  char exp_class[TERM_LENGTH];
  bool stemming;
  float param1;
  float param2;
  int param3;
  int prior_type;
  int prior_size;
  bool rmoverlap;
  char context[TERM_LENGTH];
  float extra;
  struct_RMT *next;
};


/*
 * Text retrieval model structure
 */
typedef struct struct_RMI struct_RMI;
struct struct_RMI {
  int qnumber;
  int model;
  char descriptor[TERM_LENGTH];
  char attr_name[TERM_LENGTH];
  int computation;
  struct_RMI *next;
};


/*
 * Relevance feedback structure
 */
typedef struct struct_RF struct_RF;
struct struct_RF {
  int qnumber;
  int rf_type;
  int prior_size;
  char journal_name[NAME_LENGTH];
  char elem1_name[NAME_LENGTH];
  char elem2_name[NAME_LENGTH];
  char elem3_name[NAME_LENGTH];
  struct_RF *next;
};

/*
 *
 *
 */

/* Maximum characters for file name size */
#define FILENAME_SIZE 128

/* Maximum characters per query */
#define QUERY_SIZE 1024

#define MAXMILSIZE	32000
#define MILPRINTF sprintf
#define MILOUT    &parserCtx->milBUFF[strlen(parserCtx->milBUFF)]

typedef struct TijahStringList {
    char*	label;
    int		cnt;
    int		max;
    char**	val;
} TijahStringList;

typedef struct TijahNumberList {
    char*	label;
    int		cnt;
    int		max;
    int*	val;
} TijahNumberList;

#define TSL_DFLTMAX 32

extern int   tsl_init(TijahStringList* tsl, char* label, int max);
extern int   tsl_clear(TijahStringList* tsl);
extern int   tsl_free(TijahStringList* tsl);
extern char* tsl_append(TijahStringList* tsl, char* v);
extern char* tsl_appendq(TijahStringList* tsl, char* v);

extern int   tnl_init(TijahNumberList* tnl, char* label, int max);
extern int   tnl_clear(TijahNumberList* tnl);
extern int   tnl_free(TijahNumberList* tnl);
extern int   tnl_append(TijahNumberList* tnl, int v);

typedef struct TijahParserContext {
    char*	milFILEname; /* name of the generated milfile */
    FILE* 	milFILEdesc; /* descriptor for the MIL file output */
    /* */
    TijahNumberList commandLIST;
    TijahNumberList command_preLIST;
    TijahStringList tokenLIST;
    TijahStringList token_preLIST;
    /* */
    int		useFragments;   /* use fragmentation code */
    const char* ffPfx;		/* fragfunPrefix */
    const char* flastPfx;	/* fraglast par Postfix */
    /* */ 
    const char* collection;
    const char* queryText;
    char	errBUFF[QUERY_SIZE];
    char        milBUFF[MAXMILSIZE];
    /* */
    struct tijahContextStruct* tjCtx;
} TijahParserContext;

extern TijahParserContext* parserCtx;

#define WORKDIR	"/tmp/"

#define NEXI_RESULT_BAT "nexi_result"

/*
 *
 */
void setNEXIscanstring(const char *s);

extern int parseNEXI(TijahParserContext* parserCtx, int *query_end_num);
extern int preprocess(int preproc_type);
int process(char* stemmer, bool stem_stop, bool stop_quoted);

extern int COtoCPlan(int query_num, int type, struct_RMT *txt_retr_model, struct_RF *rel_feedback);
int CAStoCPlan(int query_num, int type, bool rm_set);
extern int SRA_to_MIL(TijahParserContext* parserCtx, int query_num, int use_startNodes, struct_RMT *txt_retr_model, struct_RMI *img_retr_model, struct_RF *rel_feedback, char *mil_fname, char *sxqxl_fname, command_tree **p_command_array, bool phrase_in);
