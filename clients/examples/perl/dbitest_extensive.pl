#!/usr/bin/perl
# MonetDB DBI test script
# ---------------------
# This scripts will call all methods as defined in DBI 1.37 (Perl 5.8.1)
#
# written by Frans Oliehoek <faolieho@science.uva.nl>
#
use DBI;
use strict;
use Data::Dump qw(dump);

# used parameters:
my $database = "testDBI";
my $username = "monetdb";
my $password = "monetdb";
my $host = "localhost";
my $port = "50000"; #def. port
my $driver = "monetdb";

my $def_dsn = "dbi:$driver:database=$database;host=$host;port=$port;language=sql";
#my $def_dsn = "dbi:$driver:database=$database;host=$host";
my @failed_methods;

my $testnr = 0;

=head1 Name

DBI test script for MonetDB 4.3.16

=head1 Description

This scripts will call all methods as defined in DBI 1.37 (Perl 5.8.1).
It basically is copy of the DBI pod text interchanged with the actual
funtion calls.

As the creation of handles depends on execution of functions on higher
levels, the script is not able to exactly analyse what's working and what's
not. For example, if the DBI->connect() function fails, none of the tests here
will work, as we are not able to create any handles. This means that you should
use this script if the main part of the DBI implementation is working, but you
want to check on what functionality might be missing.


=head2 Notation and Conventions

The following conventions are used in this document:

  $dbh    Database handle object
  $sth    Statement handle object
  $drh    Driver handle object (rarely seen or used in applications)
  $h      Any of the handle types above ($dbh, $sth, or $drh)
  $rc     General Return Code  (boolean: true=ok, false=error)
  $rv     General Return Value (typically an integer)
  @ary    List of values returned from the database, typically a row of data
  $rows   Number of rows processed (if available, else -1)
  $fh     A filehandle
  undef   NULL values are represented by undefined values in Perl
  \%attr  Reference to a hash of attribute values passed to methods


=head2 Outline Usage

Make sure MonetDB is running. This script by default uses a database called
'testDBI'. Either run MonetDB --dbname=testDBI or edit the code.
Also this script makes use of a table with country codes and names, which
should be included.

=cut



=head1 THE DBI PACKAGE AND CLASS

This section handles the DBI class methods, utility functions,
and the dynamic attributes associated with generic DBI handles in
the DBI pod text. As most of these are not independent of the  
MonetDB DBD and Mapi most if these are skipped.

=cut

print ">>>THE DBI PACKAGE AND CLASS\n";
print ">>>-------------------------\n";

=head2 DBI Constants

<skipped> - (should function alright)

=head2 DBI Class Methods

The following methods are provided by the DBI class:

=over 4

=item C<connect>

There is I<no standard> for the text following the driver name. Each
driver is free to use whatever syntax it wants. Therefore we perform
a few common sense connect statements:
 
  $dbh = DBI->connect($data_source, $username, $password)

using $data_source :

  dbi:DriverName:database_name
  dbi:DriverName:database=database_name;language=sql
  dbi:DriverName:database=database_name;host=hostname;port=port;language=sql

As future versions of the DBI may issue a warning if C<AutoCommit> is 
not explicitly defined, we also include:

  $dbh = DBI->connect($data_source, $user, $pass, {
	AutoCommit => 0
  });

and

  $dbh = DBI->connect($data_source, $user, $pass, {
	AutoCommit => 1
  });

(using:
  dbi:DriverName:database=database_name;host=hostname;port=port;language=sql)

=cut

my $dbh;
my $data_source;
my $res;

print "\n<connect> tests\n";
print "---------------\n";

$testnr++;
$data_source = "dbi:$driver:$host:$port";
$res = eval{$dbh = DBI->connect($data_source, $username, $password)
	|| push(@failed_methods, "\$dbh = DBI->connect($data_source, $username, $password)");};
print "test# $testnr - result: $res\n";

$testnr++;
$data_source = "dbi:$driver:database=$database;language=sql";
$res = eval{$dbh = DBI->connect($data_source, $username, $password)
	|| push(@failed_methods, "\$dbh = DBI->connect($data_source, $username, $password)");};
print "test# $testnr - result: $res\n";

$testnr++;
$data_source = "dbi:$driver:database=$database;host=$host;port=$port;language=sql";
$res = eval{$dbh = DBI->connect($data_source, $username, $password)
	|| push(@failed_methods, "\$dbh = DBI->connect($data_source, $username, $password)");};
print "test# $testnr - result: $res\n";

$testnr++;
$res =  eval{$dbh = DBI->connect($data_source, $username, $password, {AutoCommit => 0})
	|| push(@failed_methods, "\$dbh = DBI->connect($data_source, $username, $password, {AutoCommit => 0} ");};
print "test# $testnr - result: $res\n";
if($res == undef){
	push(@failed_methods, "\$dbh = DBI->connect($data_source, $username, $password, {AutoCommit => 0} ");
}


$testnr++;
$res = eval{$dbh = DBI->connect($data_source, $username, $password, {AutoCommit => 1})
	|| push(@failed_methods, "\$dbh = DBI->connect($data_source, $username, $password, {AutoCommit => 1} ");};
print "test# $testnr - result: $res\n";
if($res == undef){
	push(@failed_methods, "\$dbh = DBI->connect($data_source, $username, $password, {AutoCommit => 1} ");
}



=item C<connect_cached>

We already tested different dsn's at the regular connect, we'll stick with:

  $dbh = DBI->connect_cached($data_source, $username, $password)
            or die $DBI::errstr;

here, using the elaborate dsn (as this works at the moment...)

=cut

print "\n<connect_cached> tests\n";
print "------------------------\n";

$testnr++;
$res = eval{$dbh = DBI->connect_cached($def_dsn, $username, $password)
	|| push(@failed_methods, "\$dbh = DBI->connect_cached($def_dsn, $username, $password)");};
if($res == undef){
	push(@failed_methods, "\$dbh = DBI->connect_cached($def_dsn, $username, $password)");
}
print "test# $testnr - result: $res\n";

=item C<available_drivers>

We check whether the monetdb driver (monetdb.pm) is found correctly:

  @ary = DBI->available_drivers;

=cut

print "\n<available_driver> tests\n";
print "--------------------------\n";

my @drivers;
$testnr++;

@drivers = DBI->available_drivers;

print "test# $testnr - result: ";
if(scalar(@drivers) == 0){
	push(@failed_methods, "\$dbh = DBI->available_drivers (no drivers found at all!)");
}else{
	for (my $i = 0; $i < scalar(@drivers); $i++){
		print "$i - $drivers[$i], ";
	}
	if( grep(/^monetdb/, @drivers) ){
		print "monetdb driver found";
	}else{
		push(@failed_methods, "\$dbh = DBI->available_drivers (monetdb driver not found)");
	}
	print "\n";
}




=item C<data_sources>

We check:

  @ary = DBI->data_sources($driver);

=cut

print "\n<data_sources> tests\n";
print "----------------------\n";

my @ary;
$testnr++;

@ary = DBI->data_sources("monetdb");

print "test# $testnr - result: ";
if(scalar(@ary) == 0){
	push(@failed_methods, "\$dbh = DBI->data_sources (no datasources found!)");
}else{
	for (my $i = 0; $i < scalar(@ary); $i++){
		print "$i - $ary[$i], ";
	}
	print "(=data_sources found)\n";
}


=item C<trace>

We will skip: 

  DBI->trace($trace_level)

This method only involves the actual DBI, we will test C<trace> for the
actual handles (see METHODS COMMON TO ALL HANDLES)

=back


=head2 DBI Utility Functions

The DBI package also provides some utility functions, as implementation
purely relies on the DBI package we skip these.

=head2 DBI Dynamic Attributes

Describes the dynamic attributes that are always associated with 
the I<last handle used>, also skipped.

=head1 METHODS COMMON TO ALL HANDLES

The following methods can be used by all types of DBI handles.

=cut


print "\n>>>METHODS COMMON TO ALL HANDLES\n";
print ">>>-----------------------------\n";


=over 4

=item C<set_err>, C<err>, C<errstr>, and C<state>

The following functions are tested here.

  $rv = $h->err;

  $str = $h->errstr;

  $str = $h->state;

  $rv = $h->set_err($err, $errstr);
  $rv = $h->set_err($err, $errstr, $state, $method);
  $rv = $h->set_err($err, $errstr, $state, $method, $rv);

This is done for both dbh and sth.

In order to test the $method property we also test h->{PrintError}
(this is under ATTRIBUTES COMMON TO ALL HANDLES in the DBI pod)

=cut
print "\n<set_err>, <err>, <errstr> and <status> tests for dbh\n";
print "-------------------------------------------------------\n";

my $rv;
my $str;
my $err = 1;
my $errstr = "test error";
my $state = "S1000";
my $method_called = 0;
my $method = sub{
	$method_called = 1;
	print "<method called>";
};
my $rv2 = 5;


#connecting with printError=>1
$testnr++;
print "test# $testnr - using \$dbh->set_err with printError => 1\n";
print "\nATTENTION!!! you should see an error message (referring to set_err) here:\n";

$dbh = DBI->connect($def_dsn, $username, $password, {printError => 1});

if(($dbh->set_err($err, $errstr))==undef){ 
; 
}else{
push(@failed_methods, "\$dbh->set_err(\$err, \$errstr) != undef (with printError = 1")
};

print "\ntest# $testnr (connecting with printError=>1) - result:". eval{$dbh?("success\n"):("failed\n")};

#connecting with printError=>0
#(also turning off error messages, because we do not want set_err to disturb us all the time...)
$testnr++;
$dbh = DBI->connect($def_dsn, $username, $password, {PrintError => 0,RaiseError => 0});
if(!$dbh){
	push(@failed_methods, "\$dbh = DBI->connect($def_dsn, $username, $password, {PrintError => 0,RaiseError => 0});");
	$dbh = DBI->connect($def_dsn, $username, $password);
}
print "test# $testnr (connecting with printError=>0) - result:". eval{$dbh?("success\n"):("failed -> this will cause some error messages...\n")};

#basic set_err($err, $errstr) test
$testnr++;
$res = "success\n";
$rv = $dbh->set_err($err, $errstr);
if($rv != undef){
	push(@failed_methods, "\$rv = \$dbh->set_err(\$err, \$errstr) returned $rv (expected: undef)");
	$res = "failed\n";
}
if($err != $dbh->err()){
	push(@failed_methods, "\$dbh->err != \$err after \$dbh->set_err(\$err, \$errstr)");
	$res = "failed\n";
}
if($errstr != $dbh->errstr()){
	push(@failed_methods, "\$dbh->errstr != \$errstr after \$dbh->set_err(\$err, \$errstr)");
	$res = "failed\n";
}
print "test# $testnr (set_err(\$err, \$errstr) ) - result: $res";


# checking whether we can print the error method and whether state is handled correctly:
$testnr++;

$dbh = DBI->connect($def_dsn, $username, $password, {PrintError => 0,RaiseError => 0});
if(!$dbh){$dbh = DBI->connect($def_dsn, $username, $password);}

$res = "success\n";
$dbh->{PrintError} = 1;
$method = "method-causing-error";

print "\nATTENTION!!! again, you should see an error message (now referring to $method) here:\n";

$rv = $dbh->set_err($err, $errstr, $state, $method);
if($rv != undef){
	push(@failed_methods, "\$rv = \$dbh->set_err(\$err, \$errstr, \$state, \$method) returned $rv (expected: undef)");
	$res = "failed\n";
}
if($err != $dbh->err()){
	push(@failed_methods, "\$dbh->err != \$err after \$dbh->set_err(\$err, \$errstr, \$state, \$method)");
	$res = "failed\n";
}
if($errstr != $dbh->errstr()){
	push(@failed_methods, "\$dbh->errstr != \$errstr after \$dbh->set_err(\$err, \$errstr, \$state, \$method)");
	$res = "failed\n";
}
if($state != $dbh->state()){
	push(@failed_methods, "\$dbh->state != \$state after \$dbh->set_err(\$err, \$errstr, \$state, \$method)");
	$res = "failed\n";
}
print "\ntest# $testnr (set_err(\$err, \$errstr, \$state, \$method) ) - result: $res";




# checking whether we can affect the return value correctly:
$testnr++;
$res = "success\n";
$method = "undef";

$dbh = DBI->connect($def_dsn, $username, $password, {PrintError => 0,RaiseError => 0});
if(!$dbh){$dbh = DBI->connect($def_dsn, $username, $password);}

$rv = $dbh->set_err($err, $errstr, $state, undef, $rv2);
if($rv != $rv2){
	push(@failed_methods, "\$rv = \$dbh->set_err(\$err, \$errstr, \$state, \$method, $rv2) returned $rv (expected: $rv2)");
	$res = "failed\n";
}
if($err != $dbh->err()){
	push(@failed_methods, "\$dbh->err != \$err after \$dbh->set_err(\$err, \$errstr, \$state, \$method, $rv2)");
	$res = "failed\n";
}
if($errstr != $dbh->errstr()){
	push(@failed_methods, "\$dbh->errstr != \$errstr after \$dbh->set_err(\$err, \$errstr, \$state, \$method, $rv2)");
	$res = "failed\n";
}
if($state != $dbh->state()){
	push(@failed_methods, "\$dbh->state != \$state after \$dbh->set_err(\$err, \$errstr, \$state, \$method)");
	$res = "failed\n";
}
print "test# $testnr (set_err(\$err, \$errstr, \$state, \$method, \$rv2 ) - result: $res";







my $sth;

print "\n<trace> and <trace_msg> for dbh\n";
print "---------------------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

#dbh->trace
$testnr++;
my $level = 9;
print "test# $testnr - setting trace($level) (this will cause some debug info here...)\n";
$dbh->trace($level);
$res = "success";
if($dbh->{TraceLevel} != $level){
	push(@failed_methods, "\$dbh->trace(\$level)");
	$res = "failed";
}
print "test# $testnr - result : $res\n";


$testnr++;
$level = 4;
my $file = "temptrace";
print "removing tracefile \`rm $file\` gives: \"" . `rm $file`."\"\n";
print "test# $testnr - setting trace($level, $file) (this will cause some debug info here...)\n";
$dbh->trace($level, $file);
$res = "success";
if($dbh->{TraceLevel} != $level){
	push(@failed_methods, "\$dbh->trace(\$level)");
	$res = "failed";
}
print "test# $testnr - result : $res\n";


$testnr++;
$res = "success";
my $msg = "Some interesting trace message, blabla...";
print "test# $testnr - trace_msg($msg) (this will cause some debug info here...)\n";
$dbh->trace_msg($msg."\n");

my $msg2 = "Another interesting trace message, blabla...";
print "test# $testnr - trace_msg($msg, $level+1) (this will cause some debug info here...)\n";
$dbh->trace_msg($msg2."\n", $level+1);

my $msg3 = "Yet... another interesting message...";
print "test# $testnr - trace_msg($msg, $level-1) (this will cause some debug info here...)\n";
$dbh->trace_msg($msg3."\n", $level-1);

#open trace file, msg should be found $msg2 not, msg3 should
my $filecontent = `cat $file`;
print "read $file:\n$filecontent";
if(!($filecontent =~ m/$msg/)){
	push(@failed_methods, "\$dbh->trace_msg(\$msg) failed (\$msg not found in tracefile)");
	$res = "failed";
}
if(!($filecontent =~ m/$msg3/)){
	push(@failed_methods, "\$dbh->trace_msg(\$msg3, $level-1) failed (\$msg not found in tracefile)");
	$res = "failed";
}
if(($filecontent =~ m/$msg2/)){
	push(@failed_methods, "\$dbh->trace_msg(\$msg, $level+1) failed (\$msg WAS found in tracefile)");
	$res = "failed";
}
print "test# $testnr - result : $res\n";



$sth = $dbh->prepare("select * from countries;");

print "\n<trace> and <trace_msg> for sth\n";
print "---------------------------------\n";

#sth->trace
$testnr++;
my $level = 9;
print "test# $testnr - setting trace($level) (this will cause some debug info here...)\n";
$sth->trace($level);
$res = "success";
if($sth->{TraceLevel} != $level){
	push(@failed_methods, "\$sth->trace(\$level)");
	$res = "failed";
}
print "test# $testnr - result : $res\n";

$testnr++;
$level = 4;
my $file = "temptrace";
print "removing tracefile \`rm $file\` gives: \"" . `rm $file`."\"\n";
print "test# $testnr - setting trace($level, $file) (this will cause some debug info here...)\n";
$sth->trace($level, $file);
$res = "success";
if($sth->{TraceLevel} != $level){
	push(@failed_methods, "\$sth->trace(\$level)");
	$res = "failed";
}
print "test# $testnr - result : $res\n";


$testnr++;
$res = "success";
my $msg = "Some interesting trace message, blabla...";
print "test# $testnr - trace_msg($msg) (this will cause some debug info here...)\n";
$sth->trace_msg($msg."\n");

my $msg2 = "Another interesting trace message, blabla...";
print "test# $testnr - trace_msg($msg, $level+1) (this will cause some debug info here...)\n";
$sth->trace_msg($msg2."\n", $level+1);

my $msg3 = "Yet... another interesting message...";
print "test# $testnr - trace_msg($msg, $level-1) (this will cause some debug info here...)\n";
$sth->trace_msg($msg3."\n", $level-1);

#open trace file, msg should be found $msg2 not, msg3 should
my $filecontent = `cat $file`;
print "read $file:\n$filecontent";
if(!($filecontent =~ m/$msg/)){
	push(@failed_methods, "\$sth->trace_msg(\$msg) failed (\$msg not found in tracefile)");
	$res = "failed";
}
if(!($filecontent =~ m/$msg3/)){
	push(@failed_methods, "\$sth->trace_msg(\$msg3, $level-1) failed (\$msg not found in tracefile)");
	$res = "failed";
}
if(($filecontent =~ m/$msg2/)){
	push(@failed_methods, "\$sth->trace_msg(\$msg, $level+1) failed (\$msg WAS found in tracefile)");
	$res = "failed";
}
print "test# $testnr - result : $res\n";



print "trying to determine whether trace() is used by monetdb and mapi\n";
print "-------------------------------------------------------------\n";
#this might be used to determine whether the driver actualy uses 'TraceLevel'...
$level = 0;

$dbh->trace($level, undef);
$sth = $dbh->prepare("select * from countries;");

$level = 9;
$file = "drivertrace";
`rm $file`;
$dbh->trace($level, $file);
$rv = $sth->execute;

$dbh->trace(0, undef);

my $drtrace = `cat $file`;
my $likeliness = 0;
print "\nfound \"mapi\":". ($res = ($drtrace =~ m/mapi/i));
$likeliness += ($res?1:0);
print "\nfound \"monetdb\":". ($res = ( $drtrace =~ m/monetdb/i));
$likeliness += ($res)?1:0;
print "\nfound \"send\":". ($res = ($drtrace =~ m/send/i));
$likeliness += ($res)?1:0;
print "\nfound \"receiv\":". ($res = ($drtrace =~ m/receiv/i));
$likeliness += ($res)?1:0;
print "\nfound \"buf\":". ($res = ($drtrace =~ m/buf/i));
$likeliness += ($res)?1:0;
print "-> found $likeliness different matching terms: ";

if($likeliness >=3){
	print "driver probably uses tracelevel set by trace\n";
}else{

	print "driver probably does NOT use tracelevel set by trace\n";
	push(@failed_methods, "driver probably does NOT use tracelevel set by trace");
}

#TODO

=item C<func>

TODO: implement dummy function in driver and test this...
no priority though...

=item C<can>

=back

=cut

#TODO

=head1 ATTRIBUTES COMMON TO ALL HANDLES

TODO...

=head1 DBI DATABASE HANDLE OBJECTS

=head2 Database Handle Methods

=cut

print "\n>>>DBH METHODS\n";
print   ">>>-----------\n";

=over 4

=item C<clone>

=item C<do>

=item C<selectrow_array>

=item C<selectrow_arrayref>

  $ary_ref = $dbh->selectrow_arrayref($statement);
  $ary_ref = $dbh->selectrow_arrayref($statement, \%attr);
  $ary_ref = $dbh->selectrow_arrayref($statement, \%attr, @bind_values);

with 
  $statement = "select name, id from tables";

for now (will have to change this to something useful)
  %attr = undef;
  @bind_values= []; 


=cut

print "\nselectrow_arrayref\n";
print   "------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

my $ary_ref;
my $statement = "select * from countries";
$testnr++;
$dbh->trace(9);
$ary_ref = $dbh->selectrow_arrayref($statement)
	|| push (@failed_methods, "\$dbh->selectrow_arrayref($statement) failed (return undef)");

print dump($ary_ref) . "\n";
print "test# $testnr - result : @$ary_ref\n";

#TODO to do: something useful:
my %attr = undef;
$testnr++;
$ary_ref = $dbh->selectrow_arrayref($statement, \%attr)
	|| push (@failed_methods, "\$dbh->selectrow_arrayref($statement) failed (return undef)");
print "test# $testnr - result : @$ary_ref\n";

#TODO to do: something useful:
#my @bind_values= [];
#$testnr++;
#$ary_ref = $dbh->selectrow_arrayref($statement, \%attr, @bind_values)
#	|| push (@failed_methods, "\$dbh->selectrow_arrayref($statement) failed (return undef)");
#print "test# $testnr - result : @$ary_ref\n";

=item C<selectrow_hashref>

All following three forms are tested:
  $hash_ref = $dbh->selectrow_hashref($statement);
  $hash_ref = $dbh->selectrow_hashref($statement, \%attr);
  $hash_ref = $dbh->selectrow_hashref($statement, \%attr, @bind_values);

with 
  $statement = "select name, id from tables";

for now (will have to change this to something useful)
  %attr = undef;
  @bind_values= []; 


=cut

#XXX this gives errors:

print "\nselectrow_hashref\n";
print   "------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

my $hash_ref;
my %h;
$h{"aap"} = "foo";
$statement = "select * from countries";
$testnr++;
$hash_ref = $dbh->selectrow_hashref($statement)
	|| push (@failed_methods, "\$dbh->selectrow_hashref($statement) failed (return undef)");
print "test# $testnr - result : %$hash_ref\n";

#TODO to do: something useful:
%attr = undef;
$testnr++;
$hash_ref = $dbh->selectrow_hashref($statement, \%attr)
	|| push (@failed_methods, "\$dbh->selectrow_hashref($statement) failed (return undef)");
print "test# $testnr - result : %$hash_ref\n";

#TODO to do: something useful:
#@bind_values= []; 
#$testnr++;
#$hash_ref = $dbh->selectrow_hashref($statement, \%attr, @bind_values)
#	|| push (@failed_methods, "\$dbh->selectrow_hashref($statement) failed (return undef)");
#print "test# $testnr - result : %$hash_ref\n";



=item C<selectall_arrayref>
We test:

  $ary_ref = $dbh->selectall_arrayref($statement);
  $ary_ref = $dbh->selectall_arrayref($statement, \%attr);
  $ary_ref = $dbh->selectall_arrayref($statement, \%attr, @bind_values);

with 
  $statement = "select name, id from tables";

for now (will have to change this to something useful)
  %attr = undef;
  @bind_values= []; 

=cut


# print "\nselectall_arrayref\n";
# print   "------------------\n";

# $dbh = DBI->connect($def_dsn, $username, $password);

# $ary_ref;
# $statement = "select cty_name, cty_code from countries";
# $testnr++;
# print "1\n";
# $ary_ref = $dbh->selectall_arrayref($statement)
# 	|| push (@failed_methods, "\$dbh->selectall_arrayref($statement) failed (return undef)");
# print "2\n";
# print "test# $testnr - result : @$ary_ref\n";

# #TODO: something useful:
# my %attr = undef;
# $testnr++;
# $ary_ref = $dbh->selectall_arrayref($statement, \%attr)
# 	|| push (@failed_methods, "\$dbh->selectall_arrayref($statement) failed (return undef)");
# print "test# $testnr - result : @$ary_ref\n";

#TODO: something useful:
#my @bind_values= []; 
#$testnr++;
#$ary_ref = $dbh->selectall_arrayref($statement, \%attr, @bind_values)
#	|| push (@failed_methods, "\$dbh->selectall_arrayref($statement) failed (return undef)");
#print "test# $testnr - result : @$ary_ref\n";



=item C<selectall_hashref>

=cut

#TODO skipped for now (first solve the selectrow_hashref)


=item C<selectcol_arrayref>

=item C<prepare>

=item C<prepare_cached>

=item C<commit>

=item C<rollback>

=item C<begin_work>

=item C<disconnect>

=item C<ping>

=item C<get_info>

  $value = $dbh->get_info( $info_type );

"Because some DBI methods make use of get_info(), drivers are strongly
encouraged to support I<at least> the following very minimal set
of information types to ensure the DBI itself works properly:"

so these we check...

 Type  Name                        Example A     Example B
 ----  --------------------------  ------------  ------------
   17  SQL_DBMS_NAME               'ACCESS'      'Oracle'
   18  SQL_DBMS_VER                '03.50.0000'  '08.01.0721'
   29  SQL_IDENTIFIER_QUOTE_CHAR   '`'           '"'
   41  SQL_CATALOG_NAME_SEPARATOR  '.'           '@'
  114  SQL_CATALOG_LOCATION        1             2

=cut

print "\ndbh->get_info\n";
print   "-------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

my @info_types = (17,18,29,41,114);
my $info_type;
foreach $info_type(@info_types){
	$testnr++;
	#print "\$rv= \$dbh->get_info( $info_type );";
	$rv = $dbh->get_info( $info_type );
	if(!$rv){
		push(@failed_methods, "\$rv= \$dbh->get_info( $info_type );" );
	}
	print "test# $testnr - result: $rv\n";
}

=item C<table_info>

=item C<column_info>

=item C<primary_key_info>

=item C<primary_key>

=item C<foreign_key_info>

=item C<tables>

=item C<type_info_all>

=item C<type_info>

=over 4

=item TYPE_NAME (string)

=item DATA_TYPE (integer)

=item COLUMN_SIZE (integer)

=item LITERAL_PREFIX (string)

=item LITERAL_SUFFIX (string)

=item CREATE_PARAMS (string)

=item NULLABLE (integer)

=item CASE_SENSITIVE (boolean)

=item SEARCHABLE (integer)

=item UNSIGNED_ATTRIBUTE (boolean)

=item FIXED_PREC_SCALE (boolean)

=item AUTO_UNIQUE_VALUE (boolean)

=item LOCAL_TYPE_NAME (string)

=item MINIMUM_SCALE (integer)

=item MAXIMUM_SCALE (integer)

=item SQL_DATA_TYPE (integer)

=item SQL_DATETIME_SUB (integer)

=item NUM_PREC_RADIX (integer)

=item INTERVAL_PRECISION (integer)

=back

=item C<quote>

=item C<quote_identifier>

=item C<take_imp_data>

=back


=head2 Database Handle Attributes

=cut

print "\n>>>DBH ATTRIBUTES\n";
print   "-----------------\n";

=over 4

=item C<AutoCommit>  (boolean)

We check whether we can get and set $dbh->{AutoCommit}

TODO: check whether setting is respected (at the time of writing it is not...)

=cut


print "\ndbh->{AutoCommit}\n";
print   "-----------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

#fatal error if unsupported, so we use eval...
my $result;

$testnr++;

print "\ntest# $testnr: checking whether we can read the AutoCommit settings:\n";
my $ac_status;

$result = eval{$ac_status = $dbh->{AutoCommit}}; 
if($result == undef){
	print "->NO!\n";
	push (@failed_methods, "Reading of \$dbh->{AutoCommit} failed.");
}else{
	print "->Yes, value = $ac_status\n";

	if($ac_status == 0){
		print "However, default is 0 (false), DBI prescribes 1 (true)\n";
		push (@failed_methods, "\$dbh->{AutoCommit} defaults to 0...");
	}
}

$testnr++;
print "\ntest# $testnr: checking whether we can set the AutoCommit settings:\n";
$result = eval{$dbh->{AutoCommit} = 0;};
if($result == undef){
	print "->NO!\n";
	push (@failed_methods, "Setting of \$dbh->{AutoCommit} failed.");
}else{
	print "->Yes\n";
}

#TODO check whether autocommit setting is respected 
#XXX because right now, it is not.

=item C<Driver>  (handle)

Let's check whether we can get the driver handle.
We also check if we can read the name of the driver using:
  $dbh->{Driver}->{Name}

=cut

print "\ndbh->{Driver}\n";
print   "-------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

$testnr++;
print "test#: $testnr - checking if we can read \$dbh->{Driver}...\n";

$result = eval{$dbh->{Driver}};
if(not $result){
	print "NO!";
	push(@failed_methods, "Getting \$dbh->{Driver} failed");
}else{	
	print "Yes, ";
	$result = eval{$dbh->{Driver}->{Name}};
	if($result){print "and name is: $result\n";}
	else{
		print "but can't read \$dbh->{Driver}->{Name}!\n";
		push(@failed_methods, "Getting \$dbh->{Driver}->{Name} failed");
	}
}




=back

=item C<Name>  (string)

Test whether we can read the db name

=cut
print "\ndbh->{Name}\n";
print   "-----------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

$testnr++;
print "test#: $testnr - checking if we can read \$dbh->{Name}...\n";

$result = eval{$dbh->{Name}};
if(not $result){
	print "NO!";
	push(@failed_methods, "Getting \$dbh->{Name} failed");
}else{	
	print "Yes, ";
	print "and name is: $result\n";
}


=item C<Statement>  (string, read-only)

Whether we read the statement is tested.

=cut
print "\ndbh->{Statement}\n";
print   "----------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$testnr++;
print "test#: $testnr - checking if we can read \$dbh->{Statement}...\n";

$result = eval{$dbh->{Statement}};
if(not $result){
	print "NO!";
	push(@failed_methods, "Getting \$dbh->{Statement} failed");
}else{	
	print "Yes, ";
	print "and statement is: $result\n";
}


=item C<RowCacheSize>  (integer)

Tested whether we can read this and whether after setting to 5,
the result of reading is 5.

=cut
print "\ndbh->{RowCacheSize}\n";
print   "-------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
#$sth = $dbh->prepare_cached("select * from countries;");
$testnr++;
print "test#: $testnr - checking if we can read \$dbh->{RowCacheSize}...\n";

my $rcs;
$result = eval{$rcs = $dbh->{RowCacheSize}};
if($result == undef){
	print "No -> seems not implemented\n";
	push(@failed_methods, "Getting \$dbh->{RowCacheSize} failed (rowcache not implemented?)");
}else{	
	print "Yes, ";
	print "and RowCacheSize is: $rcs\n";
}

#XXX setting gives no error, regardless undef is returned, therefore 
# the following fails.
$testnr++;
print "test#: $testnr - checking if we can set \$dbh->{RowCacheSize}...\n";

$dbh->{RowCacheSize} = 5;
$rcs = $dbh->{RowCacheSize};
$result = eval{$dbh->{RowCacheSize}};
if($result != 5){
	print "No -> seems not implemented\n";
	push(@failed_methods, "\$dbh->{RowCacheSize} != 5 after being set to that (not implemented?)");
}

=item C<Username>  (string)

Tested whether we can read this.

=cut

print "\ndbh->{Username}\n";
print   "--------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$testnr++;
print "test#: $testnr - checking if we can read \$dbh->{Username}...\n";

$result = eval{$dbh->{Username}};
if(not $result){
	print "NO!";
	push(@failed_methods, "Getting \$dbh->{Username} failed");
}else{	
	print "Yes, ";
	print "and is: $result\n";
}






=head1 DBI STATEMENT HANDLE OBJECTS

Methods and attributes associated with DBI statement handles.

=cut

print "\n>>>STATEMENT HANDLE METHODS\n";
print   ">>>------------------------\n";

=head2 Statement Handle Methods

The DBI defines the following methods for use on DBI statement handles:

=over 4

=item C<bind_param>

We test functionality of this function using:
  $sth = $dbh->prepare("select * from countries where cty_name like ?;");
  $rv = $sth->bind_param(1, 'N%');

And trying to execute this.
(doesn't work at time of writing)

=cut

#XXX this doesn't work.

print "\n\$sth->bind_param\n";
print    "----------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

$testnr++;
print "test#: $testnr - checking whether we can use bind_param...\n";

$sth = $dbh->prepare("select * from countries where cty_name like ?;");
$rv = $sth->bind_param(1, "N%");
$sth->execute();
my $error;
$error = $sth->err();
if(!$error){
	$ary_ref = $sth->fetchrow_arrayref();
	my $cty_name = @$ary_ref[1];
	print "result : no error (@$ary_ref, cty_name = $cty_name)\n";
}else{
	push(@failed_methods, "\$sth->bind_param failed.");
	print "result : error \"$error\" (return value of bind call=$rv, errstr after execute=".$sth->errstr().")\n";
}




=item C<bind_param_inout>

DBI states:
"It is expected that few drivers will support this method. The only driver cur­
rently known to do so is DBD::Oracle (DBD::ODBC may support it in a future
release). Therefore it should not be used for database independent applications."

As bin_param also fails at the moment this is skipped for now.

=cut

#TODO test should be implemented as soon as bind_param does work


=item C<bind_param_array>

As bin_param also fails at the moment this is skipped for now.


=item C<execute>

The plain execute() is assumed to work. (if it doesn't almost nothing in this script
will work...)

We do check excution with bind parameters:
  $sth = $dbh->prepare("select * from countries where cty_name like ?;");
  @bind_values = ['N%'];
  $rv = $sth->execute(@bind_values) 

=cut

print "\n\$sth->execute\n";
print   "--------------\n";

print "execute() is assumed to work (otherwise almost nothing in this script
will)";

$testnr++;
print "test#: $testnr - checking whether we can use execute(\@bind_values)...\n";

$sth = $dbh->prepare("select * from countries where cty_name like ?;");
my @bind_values = ('N%');
$rv = $sth->execute(@bind_values); 

$error;
$error = $sth->err();

if(!$error){
	$ary_ref = $sth->fetchrow_arrayref();
	my $cty_name = @$ary_ref[1];
	print "result :\$sth->execute(@bind_values) -  no error (@$ary_ref, cty_name = $cty_name)\n";
	print "(return value of execute=$rv, errstr after execute=".$sth->errstr().")\n";
}else{
	push(@failed_methods, "\$sth->execute(@bind_values) failed.");
	print "result : error (return value=$rv, errstr after execute=".$sth->errstr().")\n";
}




=item C<execute_array>

This is one of the more complex functions and typically would require a larger number of tests
to verify whether the implementation works for all the DBI specifications.
We limit ourselves to the typical execute_array(\@bind_values), though.

We do this by creating a temporary table and inserting some stuff in there, testing
return values and seeing whether the inserted 'stuff' is actually current after a commit.

The code tested is:

$sth = $dbh->prepare("insert into arr_test values(?, ?);");

my @bind_values1 = (1, 2, 3);
my @bind_values2 = ('lala1', '2e naam', 'nog een naam');

$rv = $sth->execute_array(
	{ ArrayTupleStatus => \my @tuple_status },
	\@bind_values1,
	\@bind_values2
); 

=cut

print "\n\$sth->execute_array\n";
print    "-------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);

$statement = "create table arr_test(id integer not null, name varchar(30));commit;";
$sth = $dbh->prepare($statement);
$rv = $sth->execute();

$error = $sth->err();
my $error_str = $sth->errstr();

if(!$error){
	print "table arr_test created successfully\n";
	print "(return value of execute=$rv, errstr after execute=$error_str)\n";
}else{
	print "table arr_test NOT created successfully\n";
	print "result : error (return value=$rv, errstr after execute=$error_str)\n";
}

$testnr++;
print "test#: $testnr - checking whether we can use execute_array(\@bind_values)...\n";

$sth = $dbh->prepare("insert into arr_test values(?, ?);");
my @bind_values1 = (1, 2, 3);
my @bind_values2 = ('lala1', '2e naam', 'nog een naam');
$rv = $sth->execute_array(
	{ ArrayTupleStatus => \my @tuple_status },
	\@bind_values1,
	\@bind_values2
); 

$error = $sth->err();
my $error_str = $sth->errstr();

if(!$error){
	$ary_ref = $dbh->selectall_arrayref("select * from arr_test");
	print "result :\$sth->execute(@bind_values) -  no error (@$ary_ref)\n";
	my $first_rec = @$ary_ref[0];
	print "first record = @$first_rec\n";
	print "(return value of execute_array=$rv, errstr after execute=$error_str)\n";
}else{
	push(@failed_methods, "\$sth->execute_arr failed. (tried to do multiple insert");
	print "result : error (return value=$rv, errstr after execute=$error_str)\n";
}

$dbh->do("drop table arr_test; commit;");




=item C<fetchrow_arrayref>

This is checked by checking the error status after:

$dbh->prepare("select * from countries;");
$ary_ref = $sth->fetchrow_arrayref();

=cut

print "\n\$sth->fetchrow_arrayref\n";
print    "-----------------------\n";
$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

$ary_ref = $sth->fetchrow_arrayref();
my $ary_size = scalar(@$ary_ref);

$testnr++;
print "test#: $testnr - checking \$sth->fetchrow_arrayref...\n";
print "result: size of returned array= $ary_size - array=@$ary_ref\n";
if($sth->err){
	push(@failed_methods, "\$sth->fetchrow_arrayref failed.");
}

=item C<fetchrow_array>

This is checked by checking the error status after:

$dbh->prepare("select * from countries;");
$ary_ref = $sth->fetchrow_array();

=cut

print "\n\$sth->fetchrow_array\n";
print    "--------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

my @ary = $sth->fetchrow_array();
my $ary_size = scalar(@ary);

$testnr++;
print "test#: $testnr - checking \$sth->fetchrow_array...\n";
print "result: size of returned array= $ary_size - array=@$ary_ref\n";
if($sth->err){
	push(@failed_methods, "\$sth->fetchrow_array failed.");
}


=item C<fetchrow_hashref>

This is checked by checking the error status after:

$dbh->prepare("select * from countries;");
$ary_ref = $sth->fetchrow_hashref();

=cut

print "\n\$sth->fetchrow_hashref\n";
print    "----------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;
my $size;
my $hash_ref = $sth->fetchrow_hashref();

print "test#: $testnr - checking \$sth->fetchrow_hashref...\n";
if($sth->err){
	push(@failed_methods, "\$sth->fetchrow_hashref failed.");
	print "result : error (errstr after fetchrow_hashref=$error_str)\n";
}else{
	$size = scalar(%$hash_ref);
	print "result: size of returned hash= $size - hash=%$hash_ref\n";
}




=item C<fetchall_arrayref>

This is checked by checking the error status after:

$dbh->prepare("select * from countries;");
$ary_ref = $sth->fetchall_arrayref();

=cut

print "\n\$sth->fetchall_arrayref\n";
print    "-----------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

$ary_ref = $sth->fetchall_arrayref();
print "test#: $testnr - checking \$sth->fetchall_arrayref...\n";
if($sth->err){
	push(@failed_methods, "\$sth->fetchall_arrayref failed.");
	print "result : error (errstr after fetchall_arrayref=$error_str)\n";
}else{
	my $first_rec = @$ary_ref[0];
	$size = scalar(@$ary_ref);
	print "result: size of returned hash= $size - first record=@$first_rec\n";
}



=item C<fetchall_hashref>

This is checked by checking the error status after:

$dbh->prepare("select * from countries;");
$ary_ref = $sth->fetchall_hashref('cty_code');

=cut

print "\n\$sth->fetchall_hashref\n";
print    "-----------------------\n";
$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

$hash_ref = $sth->fetchall_hashref('cty_code');

print "test#: $testnr - checking \$sth->fetchall_hashref...\n";
if($sth->err){
	push(@failed_methods, "\$sth->fetchall_hashref failed.");
	print "result : error (errstr after fetchall_hashref=$error_str)\n";
}else{
	print "result: ".$hash_ref->{'NLD'}->{cty_name}."\n";
}


=item C<finish>

=cut

print "\n\$sth->finish\n";
print    "------------\n";

=item C<rows>

=cut

print "\n\$sth->rows\n";
print    "----------\n";

=item C<bind_col>

We try to bind a column to a variable for highperformance data output:

  $sth->bind_col(2, \$c_name);

This doesn't work at the moment (because the statement handle properties
aren't properly handled??)

=cut

#XXX "Statement has no result columns to bind (perhaps you need to call execute first) at dbi_test_d.pl line 1554."

print "\n\$sth->bind_col\n";
print    "--------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

$testnr++;
print "test#: $testnr - checking \$sth->bind_col(2, \\\$c_name)...\n";

my $c_name;
my $rc; 
$rc= eval{$sth->bind_col(2, \$c_name)};

print "\$rc = '$rc', err = '".$sth->err."', errstr='".$sth->errstr."'\n";
#my @ary = $sth->fetchrow_array();
#my $ary_size = scalar(@ary);

$sth->fetch();

if(($rc == undef)&&($c_name == undef)){
	print "result: failed! - after \$sth->fetch: errstr=\"".$sth->errstr."\", ";
	print "err=\"".$sth->err."\"\n";
	push(@failed_methods, "\$sth->bind_col(2, \\\$c_name) failed.");
}else{
	print "result: success (c_name = $c_name)\n";
}



=item C<bind_columns>

Same as above, only this binds all columns:

  my ($c_id, $c_name, $c_code);
  $sth->bind_columns(\$c_id, \$c_name, \$c_code);

Same error as above too.

=cut

#XXX see above
print "\n\$sth->bind_columns\n";
print    "-----------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

$testnr++;
print "test#: $testnr - checking \$sth->bind_columns(2, \\\$c_name)...\n";

my ($c_id, $c_name, $c_code);
my $rc;
$rc = eval{$sth->bind_columns(\$c_id, \$c_name, \$c_code)};
#$sth->bind_columns(\$c_id, \$c_name, \$c_code);
#print "\$rc = '$rc', err = '".$sth->err."', errstr='".$sth->errstr."'\n";
#my @ary = $sth->fetchrow_array();
#my $ary_size = scalar(@ary);

$sth->fetch();

if(!$rc){
	print "result: failed! - after \$sth->fetch: errstr=\"".$sth->errstr."\", ";
	print "err=\"".$sth->err."\"\n";
	push(@failed_methods, "\$sth->bind_columns(\$c_id, \$c_name, \$c_code) failed.");
}else{
	print "result: success (c_name = $c_name, c_code = $c_code)\n";
}


=item C<dump_results>

We check
  $rows = $sth->dump_results();

So we only check with the default values - full version is:
  $rows = $sth->dump_results($maxlen, $lsep, $fsep, $fh);

C<$fh>		defaults to C<STDOUT>
C<$lsep>	default C<"\n">
C<$fsep> 	defaults to C<", "> 
C<$maxlen> 	defaults to 35.

=cut

print "\n\$sth->dump_results\n";
print    "------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - checking dump_results:...\n";
my $rows = $sth->dump_results();

if(! $rows){
	push(@failed_methods, "\$sth->dump_results didn't return rows.");
}





print "\n>>>\$Statement Handle Attributes\n";
print    ">>>----------------------------\n";
#XXX none of these are working...

=head2 Statement Handle Attributes

At the time of writing none of these works, it seems that $sth is protected?
(when evaluating from main script during execution, $sth is a 
reference to an empty hash, evaluated within a function call does
reveal contents)

=over 4

=item C<NUM_OF_FIELDS>  (integer, read-only)

=cut

print "\n\$sth->{NUM_OF_FIELDS}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NUM_OF_FIELDS}...\n";

$result = eval{$sth->{NUM_OF_FIELDS}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NUM_OF_FIELDS} failed.");
}

 
=item C<NUM_OF_PARAMS>  (integer, read-only)

=item C<NAME>  (array-ref, read-only)

=cut

print "\n\$sth->{NAME}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NAME}...\n";
$result = eval{$sth->{NAME}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NAME} failed.");
}


=item C<NAME_lc>  (array-ref, read-only)

=cut

print "\n\$sth->{NAME_lc}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NAME_lc}...\n";
$result = eval{$sth->{NAME_lc}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NAME_lc} failed.");
}


=item C<NAME_uc>  (array-ref, read-only)

=cut

print "\n\$sth->{NAME_uc}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NAME_uc}...\n";
$result = eval{$sth->{NAME_uc}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NAME_uc} failed.");
}

=item C<NAME_hash>  (hash-ref, read-only)

=cut

print "\n\$sth->{NAME_hash}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NAME_hash}...\n";
$result = eval{$sth->{NAME_hash}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NAME_hash} failed.");
}


=item C<NAME_lc_hash>  (hash-ref, read-only)

=cut

print "\n\$sth->{NAME_lc_hash}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NAME_lc_hash}...\n";
$result = eval{$sth->{NAME_lc_hash}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NAME_lc_hash} failed.");
}


=item C<NAME_uc_hash>  (hash-ref, read-only)

=cut

print "\n\$sth->{NAME_uc_hash}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NAME_uc_hash}...\n";
$result = eval{$sth->{NAME_uc_hash}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NAME_uc_hash} failed.");
}


=item C<TYPE>  (array-ref, read-only)

=cut

print "\n\$sth->{TYPE}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{TYPE}...\n";
$result = eval{$sth->{TYPE}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{TYPE} failed.");
}


=item C<PRECISION>  (array-ref, read-only)

=cut

print "\n\$sth->{PRECISION}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{PRECISION}...\n";
$result = eval{$sth->{PRECISION}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{PRECISION} failed.");
}



=item C<SCALE>  (array-ref, read-only)

=cut

print "\n\$sth->{SCALE}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{SCALE}...\n";
$result = eval{$sth->{SCALE}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{SCALE} failed.");
}
=item C<NULLABLE>  (array-ref, read-only)

=cut

print "\n\$sth->{NULLABLE}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{NULLABLE}...\n";
$result = eval{$sth->{NULLABLE}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{NULLABLE} failed.");
}
=item C<CursorName>  (string, read-only)

=cut

print "\n\$sth->{CursorName}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{CursorName}...\n";
$result = eval{$sth->{CursorName}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{CursorName} failed.");
}
=item C<Database>  (dbh, read-only)

=cut

print "\n\$sth->{Database}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{Database}...\n";
$result = eval{$sth->{Database}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{Database} failed.");
}
=item C<ParamValues>  (hash ref, read-only)

=cut

print "\n\$sth->{ParamValues}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{ParamValues}...\n";
$result = eval{$sth->{ParamValues}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{ParamValues} failed.");
}
=item C<Statement>  (string, read-only)

=cut

print "\n\$sth->{Statement}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{Statement}...\n";
$result = eval{$sth->{Statement}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{Statement} failed.");
}

=item C<RowsInCache>  (integer, read-only)

=cut

print "\n\$sth->{RowsInCache}\n";
print    "---------------------\n";

$dbh = DBI->connect($def_dsn, $username, $password);
$sth = $dbh->prepare("select * from countries;");
$sth->execute();
$testnr++;

print "test#: $testnr - Checking if we can read \$sth->{RowsInCache}...\n";
$result = eval{$sth->{RowsInCache}};
if($result != undef){
	print "Yes, value:".$result."\n";
}else{
	print "NO!\n";
	push(@failed_methods, "reading of \$sth->{RowsInCache} failed.");
}

=back






=head1 Authors

DBI by Tim Bunce.  The DBI pod text by Tim Bunce, J. Douglas Dunlop,
Jonathan Leffler and others.  Perl by Larry Wall and the
C<perl5-porters>. This DBI test script by Frans Oliehoek.

=head1 COPYRIGHT

The DBI module is Copyright (c) 1994-2002 Tim Bunce. Ireland.
All rights reserved.

You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file.

=cut




#output results:
print "\n\nTests completed\n";
print     "---------------\n";

my $num_failed = scalar(@failed_methods);

print "Number of methods failed: $num_failed\n";

for(my $i=0; $i < $num_failed; $i++){
	print eval($i+1) ." - $failed_methods[$i]\n";
}

