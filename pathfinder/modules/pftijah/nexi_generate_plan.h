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
 * Header for module that generates logical query plans and MIL query plans 
 * from NEXI queries
 *
 */

/* Constants for stack structures */
#define END_PHRASE -1

/* Constants for command parse tree structure */
#define OPERAND_MAX 200

/*
unsigned int term_sp, phrase_sp;
char t[TERM_LENGTH];
command s;
char term_lifo[STACK_MAX][TERM_LENGTH];
int ts_lifo[STACK_MAX];
char phrase_lifo[STACK_MAX][TERM_LENGTH];
int ps_lifo[STACK_MAX];
*/

/* stack for terms */
#define PUSH_TERM(t,s) (assert (term_sp < STACK_MAX),  strcpy(term_lifo[term_sp],t), ts_lifo[term_sp] = s)

#define POP_TERM() (assert (term_sp > 0), strcpy(t, term_lifo[term_sp]), s = ts_lifo[term_sp])

/* stack for phrases */
#define PUSH_PHRASE(p,s) (assert (phrase_sp < STACK_MAX),  strcpy(phrase_lifo[phrase_sp],p), ps_lifo[phrase_sp] = s)

#define POP_PHRASE() (assert (phrase_sp > 0), strcpy(p, phrase_lifo[phrase_sp]), s = ps_lifo[phrase_sp])


/* stack for AND and OR predicates */
#define PUSH_OP(op_cod,p_leftop) (assert (op_sp < STACK_MAX),  op_lifo[op_sp] = op_cod, opp_lifo[op_sp] = p_leftop)

#define POP_OP() (assert (op_sp > 0), op_cod = op_lifo[op_sp], p_leftop = opp_lifo[op_sp])


/* stack for intermediate results */
#define PUSH_RES(p_result) (assert (res_sp < STACK_MAX),  res_lifo[res_sp] = p_result)

#define POP_RES() (assert (res_sp > 0), p_result = res_lifo[res_sp])


/* stack for steps inside predicate */
#define PUSH_STEP(step_ty, p_step) (assert (step_sp < STACK_MAX),  stt_lifo[step_sp] = step_ty, step_lifo[step_sp] = p_step)

#define POP_STEP() (assert (step_sp > 0), step_ty = stt_lifo[step_sp], p_step = step_lifo[step_sp])



