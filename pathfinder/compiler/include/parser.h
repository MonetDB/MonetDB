/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/** 
 * @file parser.h
 * Pathfinder parser and lexer interface.  Parse tree interface.
 *
 * $Id$
 */

#ifndef PARSER_H
#define PARSER_H

#include "abssyn.h"

/**
 * Parse an XQuery coming in on stdin (or whatever stdin might have
 * been dup'ed to)
 * @return error code indicating successful or failed parse
 */
PFrc_t PFparse (PFpnode_t **r);

#endif   /* PARSER_H */

/* vim:set shiftwidth=4 expandtab: */
