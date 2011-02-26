# The contents of this file are subject to the Pathfinder Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
# the License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the Pathfinder system.
#
# The Original Code has initially been developed by the Database &
# Information Systems Group at the University of Konstanz, Germany and
# the Database Group at the Technische Universitaet Muenchen, Germany.
# It is now maintained by the Database Systems Group at the Eberhard
# Karls Universitaet Tuebingen, Germany.  Portions created by the
# University of Konstanz, the Technische Universitaet Muenchen, and the
# Universitaet Tuebingen are Copyright (C) 2000-2005 University of
# Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
# 2008-2011 Eberhard Karls Universitaet Tuebingen, respectively.  All
# Rights Reserved.

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

pf = process.pf(args = ['-b', '%s.xq' % os.path.join(os.environ['TSTSRCDIR'],
                                                     os.environ['TST'])],
                stdout = process.PIPE, stderr = process.PIPE, log = True)
srv = process.server(lang = 'xquery', args = ['--set', 'standoff=enabled'],
                     stdin = pf.stdout,
                     stdout = process.PIPE, stderr = process.PIPE,
                     log = True, notrace = True)
pf.stdout = None                        # given away
out, err = pf.communicate()
sys.stderr.write(err)
out, err = srv.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
