/**
 * @file
 *
 * Declarations for MIL tree structure
 *
 * $Id$
 */

#ifndef MIL_H
#define MIL_H

/** maximum number of children of a MIL tree node */
#define MIL_MAXCHILD 5

/** MIL oid's are unsigned integers */
typedef unsigned int oid;

/** MIL identifiers are strings */
typedef char * PFmil_ident_t;

/** Node kinds for MIL tree representation */
enum PFmil_kind_t {
      m_lit_int      /**< literal integer */
    , m_lit_oid      /**< literal oid */
    , m_lit_str      /**< literal string */

    , m_nil          /**< MonetDB's special value `nil' */

    , m_type         /**< MIL type */
    , m_var          /**< MIL variable */

    , m_seq          /**< Sequence of two MIL statements */

    , m_assgn        /**< MIL assignment statement (`:=') */
    , m_new          /**< MIL new() operator (creates new BATs) */
    , m_seqbase      /**< MIL seqbase() function */
    , m_insert       /**< MIL insert() function to insert single BUNs */
    , m_binsert      /**< MIL insert() function to insert a BAT at once */
    , m_project      /**< MIL project() function */
    , m_mark         /**< MIL mark() function */
    , m_access       /**< change access restrictions to a BAT */
    , m_join         /**< MIL join operator */
    , m_reverse      /**< MIL reverse operator, swap head/tail */
    , m_copy         /**< MIL copy, returns physical copy of a BAT */

    , m_cast         /**< typecast */
    , m_mcast        /**< multiplexed typecast */

    , m_plus         /**< arithmetic plus */
    , m_mplus        /**< multiplexed arithmetic plus */

    , m_max          /**< MIL max() function */

    , m_nop          /**< `no operation', do nothing.
                          (This may be produced during compilation.) */
};
typedef enum PFmil_kind_t PFmil_kind_t;

enum PFmil_type_t {
      m_oid
    , m_void
    , m_int
    , m_str
};
typedef enum PFmil_type_t PFmil_type_t;

enum PFmil_access_t {
      BAT_READ      /**< BAT is read-only */
    , BAT_APPEND    /**< BUNs may be inserted, but no updates or deletions */
    , BAT_WRITE     /**< full read/write access to this BAT */
};
typedef enum PFmil_access_t PFmil_access_t;

/** semantic content for MIL tree nodes */
union PFmil_sem_t {
    int           i;     /**< literal integer */
    oid           o;     /**< literal oid */
    char         *s;     /**< literal string */

    PFmil_type_t  t;     /**< MIL type (for #m_type nodes) */
    PFmil_ident_t ident; /**< MIL identifier (a string) */
    PFmil_access_t access; /**< BAT access specifier, only for #m_access nodes*/
};
typedef union PFmil_sem_t PFmil_sem_t;

/** MIL tree node */
struct PFmil_t {
    PFmil_kind_t     kind;
    PFmil_sem_t      sem;
    struct PFmil_t  *child[MIL_MAXCHILD];
};
typedef struct PFmil_t PFmil_t;

/** a literal integer */
PFmil_t * PFmil_lit_int (int i);

/** a literal string */
PFmil_t * PFmil_lit_str (char *s);

/** a literal oid */
PFmil_t * PFmil_lit_oid (oid o);

/** a MIL variable */
PFmil_t * PFmil_var (PFmil_ident_t name);

/** MIL type */
PFmil_t * PFmil_type (PFmil_type_t);

/** MIL `no operation' (statement that does nothing) */
PFmil_t * PFmil_nop (void);

/** MIL keyword `nil' */
PFmil_t * PFmil_nil (void);

/** shortcut for MIL variable `unused' */
#define PFmil_unused() PFmil_var ("unused")

/** MIL new() statement */
PFmil_t * PFmil_new (PFmil_t *, PFmil_t *);

/** assignment statement: assign expression e to variable v */
PFmil_t * PFmil_assgn (PFmil_t *v, PFmil_t *e);

/** MIL seqbase() function */
PFmil_t * PFmil_seqbase (PFmil_t *, PFmil_t *);

/** MIL insert() function to insert a single BUN (3 arguments) */
PFmil_t * PFmil_insert (PFmil_t *, PFmil_t *, PFmil_t *);

/** MIL insert() function to insert a whole BAT at once (2 arguments) */
PFmil_t * PFmil_binsert (PFmil_t *, PFmil_t *);

/** MIL project() function */
PFmil_t * PFmil_project (PFmil_t *, PFmil_t *);

/** MIL mark() function */
PFmil_t * PFmil_mark (PFmil_t *, PFmil_t *);

/** Set access restrictions for a BAT */
PFmil_t * PFmil_access (PFmil_t *, PFmil_access_t);

/** MIL join() operator */
PFmil_t * PFmil_join (PFmil_t *, PFmil_t *);

/** MIL reverse() function, swap head/tail */
PFmil_t * PFmil_reverse (PFmil_t *);

/** MIL copy operator, returns physical copy of a BAT */
PFmil_t * PFmil_copy (PFmil_t *);

/** MIL max() function, return maximum tail value */
PFmil_t * PFmil_max (PFmil_t *);

/** typecast */
PFmil_t * PFmil_cast (PFmil_t *, PFmil_t *);

/** multiplexed typecast */
PFmil_t * PFmil_mcast (PFmil_t *, PFmil_t *);

/** MIL plus operator */
PFmil_t * PFmil_plus (PFmil_t *, PFmil_t *);

/** MIL multiplexed plus operator */
PFmil_t * PFmil_mplus (PFmil_t *, PFmil_t *);

#define PFmil_seq(...) \
    PFmil_seq_ (sizeof ((PFmil_t *[]) { __VA_ARGS__} ) / sizeof (PFmil_t *), \
                (PFmil_t *[]) { __VA_ARGS__ } )
PFmil_t *PFmil_seq_ (int count, PFmil_t **stmts);

#endif   /* MIL_H */
