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
 * Header file for module that generates MIL query plans out of SRA query plan
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

