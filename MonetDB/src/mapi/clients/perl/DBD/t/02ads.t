#!perl -I./t

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
