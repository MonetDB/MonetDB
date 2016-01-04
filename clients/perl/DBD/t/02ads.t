#!perl -I./t

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

$| = 1;

use strict;
use warnings;
use DBI();

use Test::More tests => 2;


pass('Data sources tests');

my @ds = DBI->data_sources('monetdb');

print "\n# Data sources:\n";
print '# ', $_, "\n" for @ds;

pass('Data sources tested');
