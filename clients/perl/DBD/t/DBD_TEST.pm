# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

package DBD_TEST;

=head1 DESCRIPTION

This package is a common set of routines for the DBD::monetdb tests.

=cut

use strict;
use warnings;
use DBI qw(:sql_types);

our $VERSION = '0.07';
our $table_name = 'PERL_DBD_TEST';

our %TestFieldInfo = (
 'A' => [SQL_INTEGER, SQL_SMALLINT, SQL_TINYINT, SQL_NUMERIC, SQL_DECIMAL]
,'B' => [SQL_WVARCHAR, SQL_VARCHAR, SQL_WCHAR, SQL_CHAR]
,'C' => [SQL_WLONGVARCHAR, SQL_LONGVARCHAR, SQL_WVARCHAR, SQL_VARCHAR]
,'D' => [SQL_TYPE_DATE, SQL_TYPE_TIMESTAMP, SQL_DATE, SQL_TIMESTAMP]
);


sub get_type_for_column {
  my $dbh = shift;
  my $col = shift;

  $dbh->type_info( $TestFieldInfo{$col} );
}


sub tab_create {
  my $dbh = shift;
  my $tbl = shift || $table_name;
  {
    local ($dbh->{PrintError}, $dbh->{RaiseError}, $dbh->{Warn});
    $dbh->{PrintError} = $dbh->{RaiseError} = $dbh->{Warn} = 0;
    $dbh->do("DROP TABLE $tbl");
  }
  my $fields;
  for my $f ( sort keys %TestFieldInfo ) {
    my $ti = get_type_for_column( $dbh, $f );
    $fields .= ', ' if $fields;
    $fields .= "$f ";
    $fields .= $ti->{TYPE_NAME};

    if ( defined $ti->{CREATE_PARAMS} ) {
      my $size = $ti->{COLUMN_SIZE};
      $size = 50 if $f eq 'B';  # TODO
      $fields .= "( $size )"    if $ti->{CREATE_PARAMS} =~ /LENGTH/i;
      $fields .= "( $size, 0 )" if $ti->{CREATE_PARAMS} =~ /PRECISION,SCALE/i;
    }
  }
  print "# Using fields: $fields\n";
  return $dbh->do("CREATE TABLE $tbl( $fields )");
}


sub tab_delete {
  my $dbh = shift;
  my $tbl = shift || $table_name;

  $dbh->do("DELETE FROM $tbl");
}


sub dump_results {
  my $sth = shift;
  my $rows = 0;

  return 0 unless $sth;

  while ( my $row = $sth->fetch ) {
    $rows++;
    print '# ', DBI::neat_list( $row ),"\n";
  }
  print "# $rows rows\n";
  $rows;
}

1;
