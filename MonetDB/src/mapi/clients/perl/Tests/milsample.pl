#!/usr/bin/perl

# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

use DBI;
use DBI qw(:sql_types);

print "\nstart simple Monet MIL interaction\n";
  # No way to determine the sources
  # my @sources = DBI->data_sources("monet");
  # print "sources:@sources\n";

  # the predefined constants, deal with type coercion in DBD
  #foreach (@{ $DBI::EXPORT_TAGS{sql_types} }) {
	#printf "%s=%s\n", $_, &{"DBI::$_"};
	#}

 # Connect to the database.
  my $dbh = DBI->connect("dbi:monetdb:database=test;host=localhost;port=50000;language=mil",
                         "joe", "joe's password",
                         {'PrintError' =>0, 'RaiseError' => 0});

  my $sth;
  $sth= $dbh->prepare("print(2);\n");
  $sth->execute() || die "Execution error:\n".$sth->{errstr};
  my @row= $sth->fetchrow_array();
  print "field[0]:".$row[0]."size:".$#row."\n";

  $sth= $dbh->prepare("print(3);\n");
  $sth->execute() || die "Execution error:\n".$sth->{errstr};
  my @row= $sth->fetchrow_array();
  print "field[0]:".$row[0]."size:".$#row."\n";

   $sth= $dbh->prepare("( xyz 1);\n");
   $sth->execute() ;#||  die "Excution error:\n".$sth->{errstr};
   if($sth->{err}){
 	print "ERROR REPORTED:".$sth->{errstr}."\n";
   }
   print "STH:".$sth->{row}."\n";

 $dbh->do("b:=new(int,int);");
 $dbh->do("insert(b,3,7);");
#|| die "Execution Error:\n".$dbh->errstr;
#
 # variable binding stuff
 my $head= 11;
 my $tail= 13;

  $sth= $dbh->prepare("insert(b,?,?);");
  $sth->bind_param(1,$head,{TYPE => SQL_INTEGER });
  $sth->bind_param(2,$tail, {TYPE => SQL_INTEGER });
  my $rv = $sth->execute();

  $sth= $dbh->prepare("print(b);");
  $sth->execute() ;#||  die "Excution error:\n".$sth->{errstr};
  while ( my $aref = $sth->fetchrow_arrayref() ){
	  print "bun:".$aref->[0].",".$aref->[1]."\n";
  }
  # get all rows at once
  my $tab = $sth->fetchall_arrayref();
  my $r = $#{$tab};		# how to get the array bounds
  my $f = $#{$tab->[0]};	# how to get the array bounds
  print "rows returned:".$r."\n";
  for( my $i =0; $i <= $r; $i++){
	for( $j=0; $j <= $f; $j++){
		print "field[$i,$j]:".$tab->[$i]->[$j]."\n";
	}
  }
  # get values of first column of table NOT SUPPORTED YET
 # my $tab = $sth->selectcol_arrayref("print(b);"); 
 # for( my $i =0; $i < 2; $i++){
 #		print "field[$i]:".$tab->[$i]."\n";
 # }


 #my @answ = $dbh->selectrow_array("print(b);");
 #print "field[0]:".$answ[0]."\n";
 #print "field[1]:".$answ[1]."\n";

  #my $ar = $dbh->selectrow_arrayref("print(b);");
  #print "field[0]:".$ar->[0]."\n";
  #print "field[1]:".$ar->[1]."\n";

  # retrieve all tuples from the bat, assuming 2 tuples

  $dbh->disconnect();
  print "Finished\n";
