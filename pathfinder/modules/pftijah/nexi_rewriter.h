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
 * Help functions for rewriting internal query representation
 */


#define END_ICOM -1

/* stack for storing terms */
#define PUSH_TRM(term) (assert (term_sp < STACK_MAX),  strcpy(term_lifo[term_sp],term))

#define POP_TRM() (assert (term_sp > 0), strcpy(term, term_lifo[term_sp]))

#define PUSH_MTRM(term) (assert (mterm_sp < STACK_MAX),  strcpy(mterm_lifo[mterm_sp],term))

#define POP_MTRM() (assert (mterm_sp > 0), strcpy(term, mterm_lifo[mterm_sp]))

#define PUSH_ITRM(term) (assert (iterm_sp < STACK_MAX),  strcpy(iterm_lifo[iterm_sp],term))

#define POP_ITRM() (assert (iterm_sp > 0), strcpy(term, iterm_lifo[iterm_sp]))

/* stack for storing commands */
#define PUSH_COM(com_code) (assert (com_sp < STACK_MAX),  com_lifo[com_sp] = com_code)

#define POP_COM() (assert (com_sp > 0), com_code = com_lifo[com_sp])

#define PUSH_MCOM(com_code) (assert (mcom_sp < STACK_MAX),  mcom_lifo[mcom_sp] = com_code)

#define POP_MCOM() (assert (mcom_sp > 0), com_code = mcom_lifo[mcom_sp])

#define PUSH_ICOM(com_code) (assert (icom_sp < STACK_MAX),  icom_lifo[icom_sp] = com_code)

#define POP_ICOM() (assert (icom_sp > 0), com_code = icom_lifo[icom_sp])

