/** atomic natural number construction */
#define lit_nat(n)      PFalg_lit_nat (n)

/** atomic integer construction */
#define lit_int(i)      PFalg_lit_int (i)

/** atomic string construction */
#define lit_str(s)      PFalg_lit_str (s)

/** tuple construction */
#define tuple(...)      PFalg_tuple (__VA_ARGS__)

/** (schema) attribute construction */
#define att(n,t)        PFalg_att ((n),(t))

/** attribute list construction */
#define attlist(...)    PFalg_attlist (__VA_ARGS__)

/** a sort specification list is just another attribute list */
#define sortby(...)     PFalg_attlist (__VA_ARGS__)

/** literal table construction */
#define lit_tbl(...)    PFalg_lit_tbl (__VA_ARGS__)

/** cartesian product */
#define cross(a,b)      PFalg_cross ((a),(b))

/** disjoint union */
#define disjunion(a,b)  PFalg_disjunion ((a),(b))

/** projection operator */
#define project(...)    PFalg_project (__VA_ARGS__)

/** item in the projection list */
#define proj(a,b)       PFalg_proj ((a),(b))

/** rownum operator */
#define rownum(a,b,c,d) PFalg_rownum ((a),(b),(c),(d))

/** serialization */
#define serialize(a)    PFalg_serialize(a)
