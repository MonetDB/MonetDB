/*

     query_rewriter.h
     =========================
     Author: Vojkan Mihajlovic
     University of Twente

     Help functions for rewriting internal query representation
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

