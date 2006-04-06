 /*

     AlgebraToExe.h
     =========================
     Author: Vojkan Mihajlovic
     University of Twente

     Header file for module that generates MIL query plans out of SRA query plan

*/

/* maximum number of adjacent terms in a phrase expression */
#define ADJ_TERM_MAX 10

/* Constants for command parse tree structure */
#define OPERAND_MAX 200

/* maximum number of operands before reseting the score for avoiding numeric problems */
#define RESET_NUM 60

/* stack for commands */
#define PUSH_COMMAND(p_com) (assert (com_sp < STACK_MAX),  com_lifo[com_sp] = p_com)

#define PUSH_COMMAND_REV(p_com) (assert ((*com_sp) < STACK_MAX),  com_lifo[*com_sp] = p_com)

#define POP_COMMAND() (assert (com_sp > 0),  p_com = com_lifo[com_sp] )


/* stack for command numbers */
//#define PUSH_NUMBER(n,t) (assert (num_sp < STACK_MAX),  num_lifo[num_sp] = n, time_lifo[num_sp] = t)

//#define POP_NUMBER() (assert (num_sp > 0),  n = num_lifo[num_sp], t = time_lifo[num_sp])

/* Function declarations */
int tree_traverse_count(command_tree *p_command, command_tree *com_lifo[], int *com_sp, int op_number[], int *op_num, int topic_num);
int tree_traverse_opt(command_tree *p_command, command_tree *com_lifo[], int *com_sp, int op_number[], int *op_num, int topic_num);
char *unquote(char *q_term);
char *split_terms(char *adj_term);

char *term_cut;
char *unq_term;

