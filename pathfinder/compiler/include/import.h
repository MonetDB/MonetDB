/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Importing XML Schema types into the Pathfinder type environment.
 *
 * See Section `Importing Schemas' in the W3C XQuery 1.0 and XPath 2.0
 * Formal Semantics and Jan Rittinger's BSc thesis.
 *
 * $Id$
 */

#ifndef IMPORT_H
#define IMPORT_H

/* PFpnode_t */
#include "abssyn.h"

/** Walk the query prolog and process `import schema' instructions */
void PFschema_import (PFpnode_t *);

#endif /* IMPORT_H */

/* vim:set shiftwidth=4 expandtab: */
