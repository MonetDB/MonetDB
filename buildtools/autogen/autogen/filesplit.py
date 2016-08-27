# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


# get the right-most extension of a filename
def rsplit_filename(f):
    base = f
    ext = ""
    s = f.rfind(".")
    if s >= 0:
        return f[:s], f[s+1:]
    return base, ext

automake_ext = ['', 'c', 'def', 'h', 'lo', 'o', 'pm.c', 'tab.c', 'tab.h', 'yy.c', 'pm.i']
automake_extra_extensions = set([rsplit_filename(x)[1] for x in automake_ext if '.' in x])
extra_extensions = ['in', 'bat', 'sed']



# get the left-most extension of a filename
# we skip over known 'extra' extensions to get the real left-most extension
# this allows us to work with files with periods in them (file.bla.c would have 'c' as extension)
# while supporting files with multiple extensions (file.c.in would result in 'c' rather than 'in')
def split_filename(f):
    base,ext = rsplit_filename(f)
    # special case for automake extensions (tab.c,tab.h)
    # we want: file.tab.c -> 'tab.c'
    #          file.bla.c -> 'c'
    if ext in automake_extra_extensions:
        new_base,new_ext = rsplit_filename(base)
        new_ext = new_ext + '.' + ext
        if new_ext in automake_ext:
            return new_base,new_ext
    while ext in extra_extensions:
        new_base,new_ext = rsplit_filename(base)
        if len(new_ext) > 0 and '/' not in new_ext:
            base,ext = new_base,new_ext + '.' + ext
        else:
            break
    return base,ext
