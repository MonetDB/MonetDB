
#ifndef DIFFLIB_H
#define DIFFLIB_H


#ifdef DEBUG
#define TRACE(x) x
#else
#define TRACE(x)
#endif


int oldnew2u_diff (int context, char* ignore, char* old_fn, char* new_fn, char* u_diff_fn);
int oldnew2l_diff (int context, char* ignore, char* old_fn, char* new_fn, char* l_diff_fn);
int oldnew2w_diff (int context, char* ignore, char* old_fn, char* new_fn, char* w_diff_fn);
int oldnew2c_diff (int context, char* ignore, char* old_fn, char* new_fn, char* c_diff_fn);
int oldnew2lwc_diff (int LWC, int context, char* ignore, char* old_fn, char* new_fn, char* lwc_diff_fn);

int u_diff2l_diff (char* u_diff_fn, char* l_diff_fn);
int u_diff2w_diff (char* u_diff_fn, char* w_diff_fn);
int u_diff2c_diff (char* u_diff_fn, char* c_diff_fn);
int u_diff2lwc_diff (int LWC, char* u_diff_fn, char* lwc_diff_fn);

int l_diff2w_diff (char* l_diff_fn, char* w_diff_fn);
int l_diff2c_diff (char* l_diff_fn, char* c_diff_fn);
int w_diff2c_diff (char* w_diff_fn, char* c_diff_fn);

int oldnew2html (int LWC, int context, char* ignore, char* old_fn, char* new_fn, char* html_fn,
                 char* caption, char* revision);
int u_diff2html (int LWC, char* u_diff_fn, char* html_fn,
                 char* caption, char* revision);
int lwc_diff2html (char* old_fn, char* new_fn,
                   char* lwc_diff_fn, char* html_fn,
                   char* caption, char* revision);

#endif /* DIFFLIB_H */

