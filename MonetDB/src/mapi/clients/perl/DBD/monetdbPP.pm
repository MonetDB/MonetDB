#!/usr/bin/perl -w
# The MonetDB DBI driver implementation, Pure Perl variant.
#
# THE USE OF THIS DRIVER IS STRONGLY DISCOURAGED, AS THE UNDERLYING Mapi.pm
# needs updating due to protocol changes. use the 'monetdb' DBI driver instead.
#

package DBD::monetdbPP;
use strict;

use DBI;
use Carp;
use vars qw($VERSION $err $errstr $state $drh);

$VERSION = '0.01';
$err = 0;
$errstr = '';
$state = undef;
$drh = undef;

sub driver
{
	return $drh if $drh;

	my $class = shift;
	my $attr  = shift;
	$class .= '::dr';

	$drh = DBI::_new_drh($class, {
		Name        => 'monetdbPP',
		Version     => $VERSION,
		Err         => \$DBD::monetdbPP::err,
		Errstr      => \$DBD::monetdbPP::errstr,
		State       => \$DBD::monetdbPP::state,
		Attribution => 'DBD::monetdbPP derived from mysqlPP by Hiroyuki OYAMA',
	}, {});
}

# The monetdb dsn structure is DBI:monetdb:host:port:dbname:language
sub _parse_dsn
{
	my $class = shift;
	my ($dsn, $args) = @_;
	my($hash, $var, $val);
	return if ! defined $dsn;

	while (length $dsn) {
		if ($dsn =~ /([^:;]*)[:;](.*)/) {
			$val = $1;
			$dsn = $2;
		}
		else {
			$val = $dsn;
			$dsn = '';
		}
		if ($val =~ /([^=]*)=(.*)/) {
			$var = $1;
			$val = $2;
			if ($var eq 'hostname' || $var eq 'host') {
				$hash->{'host'} = $val;
			}
			elsif ($var eq 'db' || $var eq 'dbname') {
				$hash->{'database'} = $val;
			}
			else {
				$hash->{$var} = $val;
			}
		}
		else {
			for $var (@$args) {
				if (!defined($hash->{$var})) {
					$hash->{$var} = $val;
					last;
				}
			}
		}
	}
	return $hash;
}


sub _parse_dsn_host
{
	my($class, $dsn) = @_;
	my $hash = $class->_parse_dsn($dsn, ['host', 'port']);
	($hash->{'host'}, $hash->{'port'});
}



package DBD::monetdbPP::dr;

$DBD::monetdbPP::dr::imp_data_size = 0;

use IO::Socket;
use IO::Handle;
use Mapi;
use strict;


sub connect
{
	my $drh = shift;
	my ($dsn, $user, $password, $attrhash) = @_;

	my $data_source_info = DBD::monetdbPP->_parse_dsn(
		$dsn, ['host', 'port','database','language'],
	);

	my $lang= $data_source_info->{language};
	$lang     ||= "mal";
	if( ! ($lang eq "mal" || $lang eq "sql" || $lang eq "mil") ){
		die "!ERROR languages permitted are 'sql', 'mal', and 'mil'\n";
	}

	#print "host:".$data_source_info->{host}."\n";
	#print "port:".$data_source_info->{port}."\n";
	#print "database:".($data_source_info->{database})."\n";
	#print "language:".$data_source_info->{language}."\n";
	$user     ||= '';
	$password ||= '';
	#print "user: $user\n";

	my $host = $data_source_info->{host};
	my $port = $data_source_info->{port};
	$host	  ||= "localhost";
	my $server = $host.":".$data_source_info->{port};
	#print "server ".$server."\n";
	
	my $dbh = DBI::_new_dbh($drh, {
		Name         => $dsn,
		USER         => $user,
		CURRENT_USER => $user,
		Language     => $data_source_info->{language}
	}, {});
	eval {
		my $mapi =  new Mapi($host.':'.$port, $user,$password,$lang)
			|| die "!ERROR can't connect to $server : $!";

		$dbh->STORE(monetdbpp_connection => $mapi);
	};
	if ($@) {
		return $dbh->DBI::set_err(1, $@);
	}
	return $dbh;
}


sub data_sources
{
	return ("dbi:monetdbpp:");
}


sub disconnect_all {}



package DBD::monetdbPP::db;

$DBD::monetdbPP::db::imp_data_size = 0;
use strict;


sub quote
{
	my $dbh = shift;
	my ($statement, $type) = @_;
	return 'NULL' unless defined $statement;

	for ($statement) {
		s/	/\\t/g;
		s/\\/\\\\/g;
		s/\n/\\n/g;
		s/\r/\\r/g;
		s/"/\\"/g;
	}
	return "'$statement'";
}

sub _count_param
{
	my @statement = split //, shift;
	my $num = 0;

	while (defined(my $c = shift @statement)) {
		if ($c eq '"' || $c eq "'") {
			my $end = $c;
			while (defined(my $c = shift @statement)) {
				last if $c eq $end;
				@statement = splice @statement, 2 if $c eq '\\';
			}
		}
		elsif ($c eq '?') {
			$num++;
		}
	}
	return $num;
}

sub prepare
{
	my $dbh = shift;
	my ($statement, @attribs) = @_;

	# Make sure the instruction finishes with a ';'
	if( $statement !~ /;$/){
		$statement = "$statement;";
	}

	my $sth = DBI::_new_sth($dbh, {
		Statement => $statement,
	});
	$sth->STORE(monetdbpp_handle => $dbh->FETCH('monetdbpp_connection'));
	$sth->STORE(monetdbpp_params => []);
	$sth->STORE(NUM_OF_PARAMS => _count_param($statement));
	$sth->STORE(is_error => 0);
	$sth;
}


sub commit
{
	my $dbh = shift;
	if ($dbh->FETCH('Warn')) {
		warn 'Commit ineffective while AutoCommit is on';
	}
	1;
}


sub rollback
{
	my $dbh = shift;
	if ($dbh->FETCH('Warn')) {
		warn 'Rollback ineffective while AutoCommit is on';
	}
	1;
}


sub tables
{
	my $dbh = shift;
	my @args = @_;
	my $mapi = $dbh->FETCH('monetdbpp_connection');

	my @database_list;
	eval {
		$mapi->wrapup();
		$mapi->doRequest("select name from tables;");
		$dbh->{row} = $mapi->getFirstAnswer();
		$dbh->{errstr} = $mapi->{errstr};
		$dbh->{err} = $mapi->{error};

		die $mapi->{errstr} if $mapi->{err};
		while( my $ref = $dbh->fetchrow_arrayref()){
			push @database_list, $ref->[0];
		}
	};
	if ($@) {
		warn $mapi->{errstr};
	}
	return $mapi->{err}
		? undef
		: @database_list;
}


sub _ListDBs
{
	my $dbh = shift;
	my @args = @_;
	my $mapi = $dbh->FETCH('monetdbpp_connection');

	my @database_list;
	eval {
		$mapi->wrapup();
		$mapi->doRequest("show databases;");
		$dbh->{row} = $mapi->getFirstAnswer();
		$dbh->{errstr} = $mapi->{errstr};
		$dbh->{err} = $mapi->{error};

		die $mapi->{errstr} if $mapi->{err};
		while( my $ref = $dbh->fetchrow_arrayref()){
			push @database_list, $ref->[0];
		}

	};
	if ($@) {
		warn $mapi->get_error_message;
	}
	return $mapi->is_error
		? undef
		: @database_list;
}


sub _ListTables
{
	my $dbh = shift;
	return $dbh->tables;
}


sub disconnect
{
	my $dbh = shift;
	my $mapi = $dbh->FETCH('monetdbpp_connection');
	$mapi->disconnect();
	return 1;
}


sub FETCH
{
	my $dbh = shift;
	my $key = shift;

	return 1 if $key eq 'AutoCommit';
	return $dbh->{$key} if $key =~ /^(?:monetdbpp_.*)$/;
	return $dbh->SUPER::FETCH($key);
}


sub STORE
{
	my $dbh = shift;
	my ($key, $value) = @_;

	if ($key eq 'AutoCommit') {
		die "Can't disable AutoCommit" unless $value;
		return 1;
	}
	elsif ($key =~ /^(?:monetdbpp_.*)$/) {
		$dbh->{$key} = $value;
		return 1;
	}
	return $dbh->SUPER::STORE($key, $value);
}


sub DESTROY
{
	my $dbh = shift;
	my $monetdb = $dbh->FETCH('monetdbpp_connection');
	$monetdb->close;
}


package DBD::monetdbPP::st;

use DBI qw(:sql_types);

$DBD::monetdbPP::st::imp_data_size = 0;

sub bind_param
{
	my $sth = shift;
	my ($index, $value, $attr) = @_;
	my $type = (ref $attr) ? $attr->{TYPE} : $attr;
	if ($type != SQL_INTEGER) {
		my $dbh = $sth->{Database};
		$value = $dbh->quote($sth, $type);
	}
	my $params = $sth->FETCH('monetdbpp_params');
	my $paramtype = $sth->FETCH('monetdbpp_types');
	#print "converted:".$value." type:".$type."\n";
	$params->[$index - 1] = $value;
	$paramtype->[$index - 1] = $type;
}

sub execute
{
	my $sth = shift;
	my @bind_values = @_;
	my $mparams = $sth->FETCH('monetdbpp_params');
	my $params = (@bind_values) ?  \@bind_values : $mparams;
	my $num_param = $sth->FETCH('NUM_OF_PARAMS');
	if (@$params != $num_param) {
		# ...
	}
	my $statement = $sth->{Statement};
	my $dbh = $sth->{Database};
	for (my $i = 0; $i < $num_param; $i++) {
		# decode the parameter type
		#my $quoted_param = $dbh->quote($params->[$i]);
		#$statement =~ s/\?/$quoted_param/e;
		my $val= $mparams->[$i];
		if( $val == undef) {
			#print "value undef ";
			$val = $dbh->quote($bind_values[$i]);
		}
		#print "value used:$val\n";
		$statement =~ s/\?/$val/e;
	}
	my $mapi = $sth->FETCH('monetdbpp_handle');
	my $result = eval {
		#print "EXE:".$statement."\n";
		$mapi->wrapup();
		$mapi->doRequest($statement);
		$sth->{row} = $mapi->getFirstAnswer();
		$sth->{errstr} = $mapi->{errstr};
		$sth->{err} = $mapi->{error};
		$sth->{rows} = -1;	# no count provided
		#print "GOT:".$sth->{row}."\n";
	};
	if ($@) {
		$sth->DBI::set_err(
			$mapi->{error}, $mapi->{row}
		);
		return undef;
	}
	if( $mapi->{error} ){
		$sth->DBI::set_err(
			$mapi->{error}, $mapi->{row}
		);
	}

	return $sth->{is_error}
		? undef : $result
			? $result : '0E0';
}


sub fetch
{
	my $sth = shift;

# next row has already been read, decode it and prepare for next.

	#print "called fetch:".$sth->{row}."\n";
	my $line = $sth->{row};
	return undef unless $line;

        $line =~ s/\[//;
        $line =~ s/\]//;
        my @fields = split(/,\t*/,$line);
	my $fcnt = $sth->FETCH('NUM_OF_FIELDS');
	if( $fcnt == undef){
		$sth->STORE(NUM_OF_FIELDS => $#fields+1);
	}
        my $i=0;
        for($i=0; $i<= $#fields; $i++){
                $fields[$i] =~ s/^ *(.*)/\1/;
                $fields[$i] =~ s/^\"(.*)\"/\1/;
                $fields[$i] =~ s/^\'(.*)\'/\1/;
		#print "target[$i];$fields[$i]\n";
        }
	# fetch next row into buffer
	my $mapi = $sth->FETCH('monetdbpp_handle');
	if( $mapi->{active} ==1) {
		$mapi->getReply();
		$sth->{row} = $mapi->{row};
	}
	return $sth->_set_fbav(\@fields);
}
*fetchrow_arrayref = \&fetch;


sub rows
{
	my $sth = shift;
	$sth->FETCH('monetdbpp_rows');
}


sub FETCH
{
	my $dbh = shift;
	my $key = shift;

	return 1 if $key eq 'AutoCommit';
	return $dbh->{NAME} if $key eq 'NAME';
	return $dbh->{$key} if $key =~ /^monetdb_/;
	return $dbh->SUPER::FETCH($key);
}


sub STORE
{
	my $dbh = shift;
	my ($key, $value) = @_;

	if ($key eq 'AutoCommit') {
		die "Can't disable AutoCommit" unless $value;
		return 1;
	}
	elsif ($key eq 'NAME') {
		$dbh->{NAME} = $value;
		return 1;
	}
	elsif ($key =~ /^monetdb_/) {
		$dbh->{$key} = $value;
		return 1;
	}
	return $dbh->SUPER::STORE($key, $value);
}


sub DESTROY
{
	my $dbh = shift;

}


1;
__END__

=head1 NAME

DBD::monetdbPP - Pure Perl MonetDB Database driver for the DBI

=head1 SYNOPSIS

    use DBI;

    $dsn = "dbi:monetdb:database=$database;host=$hostname";

    $dbh = DBI->connect($dsn, $user, $password);

    $drh = DBI->install_driver("monetdb");

    $sth = $dbh->prepare("SELECT * FROM foo WHERE bla");
    $sth->execute;
    $numRows = $sth->rows;
    $numFields = $sth->{'NUM_OF_FIELDS'};
    $sth->finish;

=head1 EXAMPLE

  #!/usr/bin/perl

  use strict;
  use DBI;

  # Connect to the database.
  my $dbh = DBI->connect("dbi:monetdb:database=test;host=localhost",
                         "joe", "joe's password",
                         {'RaiseError' => 1});

  # Drop table 'foo'. This may fail, if 'foo' doesn't exist.
  # Thus we put an eval around it.
  eval { $dbh->do("DROP TABLE foo") };
  print "Dropping foo failed: $@\n" if $@;

  # Create a new table 'foo'. This must not fail, thus we don't
  # catch errors.
  $dbh->do("CREATE TABLE foo (id INTEGER, name VARCHAR(20))");

  # INSERT some data into 'foo'. We are using $dbh->quote() for
  # quoting the name.
  $dbh->do("INSERT INTO foo VALUES (1, " . $dbh->quote("Tim") . ")");

  # Same thing, but using placeholders
  $dbh->do("INSERT INTO foo VALUES (?, ?)", undef, 2, "Jochen");

  # Now retrieve data from the table.
  my $sth = $dbh->prepare("SELECT id, name FROM foo");
  $sth->execute();
  while (my $ref = $sth->fetchrow_arrayref()) {
    print "Found a row: id = $ref->[0], name = $ref->[1]\n";
  }
  $sth->finish();

  # Disconnect from the database.
  $dbh->disconnect();


=head1 DESCRIPTION

DBD::monetdbPP is a Pure Perl client interface for the MonetDB Database Server. It means this module enables you to connect to MonetDB server from any platform where Perl is running, but MonetDB has not been installed.

From perl you activate the interface with the statement

    use DBI;

After that you can connect to multiple MonetDB database servers
and send multiple queries to any of them via a simple object oriented
interface. Two types of objects are available: database handles and
statement handles. Perl returns a database handle to the connect
method like so:

  $dbh = DBI->connect("dbi:monetdb:database=$db;host=$host",
		      $user, $password, {RaiseError => 1});

Once you have connected to a database, you can can execute SQL
statements with:

  my $query = sprintf("INSERT INTO foo VALUES (%d, %s)",
		      $number, $dbh->quote("name"));
  $dbh->do($query);

See L<DBI(3)> for details on the quote and do methods. An alternative
approach is

  $dbh->do("INSERT INTO foo VALUES (?, ?)", undef,
	   $number, $name);

in which case the quote method is executed automatically. See also
the bind_param method in L<DBI(3)>. See L<DATABASE HANDLES> below
for more details on database handles.

If you want to retrieve results, you need to create a so-called
statement handle with:

  $sth = $dbh->prepare("SELECT id, name FROM $table");
  $sth->execute();

This statement handle can be used for multiple things. First of all
you can retreive a row of data:

  my $row = $sth->fetchow_arrayref();

If your table has columns ID and NAME, then $row will be array ref with
index 0 and 1. See L<STATEMENT HANDLES> below for more details on
statement handles.

I's more formal approach:


=head2 Class Methods

=over

=item B<connect>

    use DBI;

    $dsn = "dbi:monetdb:$database";
    $dsn = "dbi:monetdb:database=$database;host=$hostname";
    $dsn = "dbi:monetdb:database=$database;host=$hostname;port=$port";

    $dbh = DBI->connect($dsn, $user, $password);

A C<database> must always be specified.

=over

=item host

The default host to connect to is 'localhost', i.e. your workstation.

=item port

The port where MonetDB daemon listens to. default for SQL is ???.

=back

=back

=head2 MetaData Method

=over 4

=item B<tables>

    @names = $dbh->tables;

Returns a list of table and view names, possibly including a schema prefix. This list should include all tables that can be used in a "SELECT" statement without further qualification.

=back

=head2 Private MetaData Methods

=over 4

=item ListDBs

    @dbs = $dbh->func('_ListDBs');

Returns a list of all databases managed by the MonetDB SQL daemon.

=item ListTables

B<WARNING>: This method is obsolete due to DBI's $dbh->tables().

    @tables = $dbh->func('_ListTables');

Once connected to the desired database on the desired MonetDB server with the "DBI-"connect()> method, we may extract a list of the tables that have been created within that database.

"ListTables" returns an array containing the names of all the tables present within the selected database. If no tables have been created, an empty list is returned.

    @tables = $dbh->func('_ListTables');
    foreach $table (@tables) {
        print "Table: $table\n";
    }

=back


=head1 DATABASE HANDLES
=head1 STATEMENT HANDLES

The statement handles of DBD::monetdbPP support a number
of attributes. You access these by using, for example,

  my $numFields = $sth->{'NUM_OF_FIELDS'};

=over

=item NUM_OF_FIELDS

Number of fields returned by a I<SELECT> statement. You may use this for checking whether a statement returned a result.
A zero value indicates a non-SELECT statement like I<INSERT>, I<DELETE> or I<UPDATE>.

=back

=head1 INSTALLATION

To install this module type the following:

   perl Makefile.PL
   make
   make test
   make install

=head1 SUPPORT OPERATING SYSTEM

This module has not been tested on all these OSes.

=over 4

=item * MacOS 9.x

TODO with MacPerl5.6.1r.

=item * MacOS X

TODO with perl5.6.0 build for darwin.

=item * Windows2000

TODO with ActivePerl5.6.1 build631.

=item * FreeBSD 3.4 and 4.x

TODO with perl5.6.1 build for i386-freebsd.

TODO with perl5.005_03 build for i386-freebsd.

=back

=head1 DEPENDENCIES

This module requires these other modules and libraries:

  DBI
  IO::Socket;
  IO::Handle;
  Mapi;

B<Mapi> is a Pure Perl client interface routines to setup
the communication with the MonetDB server.

=head2 Database Handles

Cannot be used

=over 4

=item * $dbh->{info}

=back

=head2 Statement Handles

=over 4

=item * The return value of I<execute('SELECT * from table')>

<DBD::monetdbPP> surely returns <0E0>.

=back

Cannot be used.


=back

=head2 SQL Extensions

=over 4

=back

=head1 TODO

Encryption of the password independent of I<Math::BigInt>.

Enables access to much metadata.

Inclusion of info commands as presented in Mapi.mx

=head1 SEE ALSO

=head1 AUTHORS

Martin Kersten <lt>Martin.Kersten@cwi.nl<gt>

=head1 COPYRIGHT AND LICENCE

Copyright (C) 2003-2004 CWI Netherlands. All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
