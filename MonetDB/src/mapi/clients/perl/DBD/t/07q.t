#!perl -I./t

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

$| = 1;

use strict;
use warnings;
use DBI();
use DBD_TEST();

use Test::More;

if ( defined $ENV{DBI_DSN} ) {
  plan tests => 12;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Quote tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
   $dbh->{RaiseError} = 1;
   $dbh->{PrintError} = 0;
pass('Database connection created');

eval { $dbh->quote };
ok( $@,"Call to quote() with 0 arguments, error expected: $@");

my $val =
[
  [ 1                      , q{'1'}                      ]
, [ 2                      , q{'2'}                      ]
, [ undef                  ,   'NULL'                    ]
, ['NULL'                  , q{'NULL'}                   ]
, ['ThisIsAString'         , q{'ThisIsAString'}          ]
, ['This is Another String', q{'This is Another String'} ]
, ["This isn't unusual"    , q{'This isn''t unusual'}    ]
];
for ( @$val ) {
  my $val0 = $_->[0];
  my $val1 = defined $val0 ? $val0 : 'undef';
  my $val2 = $dbh->quote( $val0 );
  is( $val2, $_->[1],"quote on $val1 returned $val2");
}

my $ti = DBD_TEST::get_type_for_column( $dbh,'A');
is( $dbh->quote( 1, $ti->{DATA_TYPE} ), 1,"quote( 1, $ti->{DATA_TYPE} )");

ok( $dbh->disconnect,'Disconnect');
