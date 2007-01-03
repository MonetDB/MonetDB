/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file prettyp.c
 *
 * Generic pretty printer (based on [Oppen80]).
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>

#include "prettyp.h"

#include "oops.h"
#include "mem.h"
/* PFarray_t */
#include "array.h"

/**
 * buffer to collect material to be pretty-printed later
 */
static PFarray_t *collect = 0;

/**
 * You'll find an explanation of the variables below in [Oppen80].
 */
static PFarray_t *stream;
static PFarray_t *size;

#define STREAM(n) (*((char **) PFarray_at (stream, (n))))
#define SIZE(n)   (*((int *) PFarray_at (size, (n))))

/**
 * A simple dynamic stack.
 */
typedef struct stack stack;

struct stack {
    int        sp;     /**< stack pointer */
    PFarray_t *lifo;   /**< dynamic stack space */
};

/**
 * Format the prettyprinted output to cover no more than @a margin
 * characters (we may exceed this margin only if we absolutely need
 * to).
 *
 * A new indentation level is indented by @a indent_by characters.
 */
#define margin    60
/** Format parameter for prettyprinting, see #margin */
#define indent_by 2

/** space left */
static int space;

/**
 * Stacks local to print() (@a S) and scan() (@a S1).
 */
static stack S;
static stack S1;   /**< see #S */

/* prototypes */
static void indent (FILE *f, int indentation);

/**
 * @var left see #stream 
 * @var right see #stream 
 * @var righttotal see #stream 
 */
static int left, right, righttotal;

/**
 * Markup found in the prettyprinted stream.
 */
static char start_block = START_BLOCK;
static char end_block   = END_BLOCK;    /**< see #start_block */
static char blank       = ' ';          /**< see #start_block */
static char eof         = '\0';         /**< see #start_block */

/**
 * Send color switching codes (currently: X11 xterm).
 */
static void
switch_color (FILE *f, char color)
{
    switch (color)
        {
            /*
             * \033[m\017 turns bold off, \033[1m turns bold on.
             * (see md and me entries in /etc/termcap)
             * The others are color changing escape sequences for an xterm.
             */
        case PFBLACK:       fprintf (f, "\033[m\017\033[30m"); break;
        case PFBLUE:        fprintf (f, "\033[m\017\033[34m"); break;
        case PFGREEN:       fprintf (f, "\033[m\017\033[32m"); break;
        case PFPINK:        fprintf (f, "\033[m\017\033[35m"); break;
        case PFRED:         fprintf (f, "\033[m\017\033[31m"); break;
        case PFYELLOW:      fprintf (f, "\033[m\017\033[33m"); break;
        case PFBOLD_BLACK:  fprintf (f, "\033[1m\033[30m");    break;
        case PFBOLD_BLUE:   fprintf (f, "\033[1m\033[34m");    break;
        case PFBOLD_GREEN:  fprintf (f, "\033[1m\033[32m");    break;
        case PFBOLD_PINK:   fprintf (f, "\033[1m\033[35m");    break;
        case PFBOLD_RED:    fprintf (f, "\033[1m\033[31m");    break;
        case PFBOLD_YELLOW: fprintf (f ,"\033[1m\033[33m");
        }
}

/*
 * What follows is a faithful implementation of the prettyprinting
 * algorithm discussed in [Oppen80].  The code below is designed to
 * match the description in [Oppen80] as closely as possible.
 * 
 * (We have augmented the `print' routine to be able to cope with
 * color switching codes.)
 *
 * Refer to [Oppen80] for a thorough explanation of what's going on
 * here.
 */

/** Initialize stack for prettyprinting routine */
static void
init (stack *s)
{
    s->sp   = 0;
    s->lifo = PFarray (sizeof (int));

    assert (s->lifo);
}

/** Empty stack for prettyprinting routine */
static bool
empty (stack *s)
{
    return (s->sp == 0);
}

/** Pop element from stack */
static int
pop (stack *s)
{
    if (! empty (s))
        return *((int *) PFarray_at (s->lifo, --(s->sp)));
    
    PFoops (OOPS_NOTPRETTY, "missing START_BLOCK?");

    /* just to pacify picky compilers; never reached due to "exit" in PFoops */
    return 0;
}

/** Return the topmost element of the stack without removing it */
static int
top (stack *s)
{
    if (! empty (s))
        return *((int *) PFarray_at (s->lifo, s->sp - 1));
    
    PFoops (OOPS_NOTPRETTY, "missing START_BLOCK?");

    /* just to pacify picky compilers; never reached due to "exit" in PFoops */
    return 0;
}

/** Push an element onto the stack */
static void 
push (stack *s, int n)
{
    *((int *) PFarray_at (s->lifo, (s->sp)++)) = n;
}


/**
 * Break the current line and indent the next line 
 * by @a ind characters.
 *
 * @param f file to print to
 * @param ind indentation level
 */
static void
indent (FILE *f, int ind)
{
    fputc ('\n', f);

    while (ind-- > 0)
        fputc (' ', f);
}


/**
 * Print the next chunk of characters (pointed to by @a x) of length
 * @a l to the given file @a f.  Break and indent if the material does
 * not fit on the current line.
 *
 * (This is the print () routine of [Oppen80].)
 *
 * @param f file to print to
 * @param x characters to print
 * @param l length of string to print
 */
static void 
print (FILE *f, char *x, int l)
{
    switch (*x) {
    case START_BLOCK:
        push (&S, space);
        break;
    case END_BLOCK:
        pop (&S);
        break;
    default:
        if (isspace ((unsigned char) *x))
            if (l > space) {
                space = top (&S) - indent_by;
                indent (f, margin - space);
            }
            else {
                fputc (*x, f);
                space = space - 1;
            }
        else {
            /* x: string */
            space = space - l;
            while (l--)
                if ((*x & 0xf0) == 0xf0)
                    switch_color (f, *(x++));
                else
                    fputc (*(x++), f);
        }
    }
}

/**
 * Extract the next token (START_BLOCK, END_BLOCK, blank, or string)
 * from the prettyprint buffer.
 *
 * @return pointer to next token (markup symbol or zero-terminated string)
 */
static char *
receive (void)
{
    char *s, *e;

    /* address of next character, advance to next character */
    s = (char *) PFarray_add (collect);

    switch (*s) {
    case '\0':
        return &eof;
    case START_BLOCK:
        return &start_block;
    case END_BLOCK:
        return &end_block;
    default:
        if (isspace ((int)(*s))) {
            while (isspace ((int)(*((char *) PFarray_add (collect)))))
                ;
            PFarray_last (collect)--;
            return &blank;
        }
        else {
            char *string;

            /* a string, s marks it start, e marks its end */
            do 
                e = (char *) PFarray_add (collect);
            while (*e && ! isspace ((int)(*e)) && *e != START_BLOCK && *e != END_BLOCK);
            PFarray_last (collect)--;

            /* copy the string and return it */
            string = PFstrndup (s, e - s);

            return string;
        }
    }
}

/**
 * Scan the prettyprint buffer token-wise and drive the prettyprinter.
 *
 * (This is the scan () routine from [Oppen80].)
 *
 * @param f file to print to
 */
static void
scan (FILE *f)
{
    char *x;
    int x1;

    while (true) {
        x = receive ();
    
        switch (*x) {
        case '\0':
            return;

        case START_BLOCK:
            if (empty (&S1)) 
                left = right = righttotal = 1;
            else
                right = right + 1;
            STREAM (right) = x;
            SIZE (right) = -righttotal;
            push (&S1, right);
            break;

        case END_BLOCK:
            right = right + 1;
            STREAM (right) = x;
            SIZE (right) = 0;
            x1 = pop (&S1);
            SIZE (x1) = righttotal + SIZE (x1);
            if (isspace ((unsigned char) *STREAM (x1))) {
                x1 = pop (&S1);
                SIZE (x1) = righttotal + SIZE (x1);
            }
            if (empty (&S1))
                do {
                    print (f, STREAM (left), SIZE (left));
                    left = left + 1;
                } while (left <= right);
            break;

        default:
            if (isspace ((unsigned char) *x)) {
                right = right + 1;
                x1 = top (&S1);
                if (isspace ((unsigned char) *STREAM (x1)))
                    SIZE (pop (&S1)) = righttotal + SIZE (x1);
                STREAM (right) = x;
                SIZE (right) = -righttotal;
                push (&S1, right);
                righttotal = righttotal + 1;
            }
            else {
                /* x: string */
                if (empty (&S1))
                    print (f, x, strlen (x));
                else {
                    right = right + 1;
                    STREAM (right) = x;
                    SIZE (right) = strlen (x);
                    righttotal = righttotal + strlen (x);
                }
            }
        }
    }
}

/**
 * Print a chunk of characters (with prettyprinting markup) to a
 * dynamically growing buffer.  You need to call this routine
 * (repeatedly) before you may call @a PFprettyp () to prettyprint the
 * buffer contents.
 *
 * @attention NB. You need to obey to the rules given in [Oppen80] to
 * make sure the buffer holds well-formed prettyprinting markup.  The
 * buffer is well-formed, iff either (1) or (2) holds:
 *
 * (1) it contains a string
 *     (NB. if the  string contains white space, be sure to embrace it 
 *      with START_BLOCK...END_BLOCK, because the pretty printer will try
 *      to break long lines at white space positions)
 * (2) it contains 
 *
 * @verbatim
         START_BLOCK b1 <blank> b2 <blank> ... <blank> bk END_BLOCK
@endverbatim
 *
 *     (with the the bi being well-formed buffers).
 *
 *
 * You may interleave the printed characters with color switching codes
 * (PFBLACK, ...).
 *
 * @param rep characters to print
 */
void
PFprettyprintf (const char *rep, ...)
{
    va_list reps;

    /* initialize the collect, stream, and size arrays if this is the
     * first material to be printed
     */
    if (! collect) {
        collect = PFarray (sizeof (char));
        stream  = PFarray (sizeof (char *));
        size    = PFarray (sizeof (int));

        assert (collect && stream && size);
    }

    va_start (reps, rep);

    if (PFarray_vprintf (collect, rep, reps) < 0)
        PFoops (OOPS_NOTPRETTY, "failed to collect prettyprinting material");

    va_end (reps);
}

/**
 * Prettyprint temporary buffer to file @a f.
 * (You have to print to the buffer using PFprettyprintf () first.)
 *
 * @param f file to print to
 */
void
PFprettyp (FILE *f)
{
    /* has material been collected yet? */
    if (collect) {
        init (&S);
        init (&S1);
    
        space = margin;
   
        /* scan the collected material from its beginning and prettyprint */
        PFarray_last (collect) = 0;
        scan (f);

        /* collect buffer has been printed, re-initialize for next
         * prettyprint job 
         */
        collect = 0;
    }
}

/*
 * References:
 *
 * [Oppen80]  Derek C. Oppen, Stanford University. "Prettyprinting".
 *            In ACM Transactions on Programming Languages and Systems,
 *            Vol. 2, No. 4, October 1980, Pages 465--483.
 */

/* vim:set shiftwidth=4 expandtab: */
