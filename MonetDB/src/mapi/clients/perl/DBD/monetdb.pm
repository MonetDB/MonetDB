#!/usr/bin/perl -w

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

# The MonetDB DBI driver implementation. New version based on the MapiLib SWIG.
# by Arjan Scherpenisse <acscherp@science.uva.nl>

package DBD::monetdb;
use strict;

use DBI;
use Carp;
use vars qw($VERSION $err $errstr $state $drh);
use sigtrap;
# use Data::Dump qw(dump);

$VERSION = '0.02';
$err = 0;
$errstr = '';
$state = undef;
$drh = undef;

sub driver {
    return $drh if $drh;

    my $class = shift;
    my $attr  = shift;
    $class .= '::dr';

    $drh = DBI::_new_drh($class, {
				  Name        => 'monetdb',
				  Version     => $VERSION,
				  Err         => \$DBD::monetdb::err,
				  Errstr      => \$DBD::monetdb::errstr,
				  State       => \$DBD::monetdb::state,
				  Attribution => 'DBD::monetdb derived from monetdb.pm by Arjan Scherpenisse',
				 }, {});
}

# The monetdb dsn structure is DBI:monetdb:host:port:dbname:language
sub _parse_dsn {
    my $class = shift;
    my ($dsn, $args) = @_;
    my($hash, $var, $val);
    return if ! defined $dsn;

    while (length $dsn) {
	if ($dsn =~ /([^:;]*)[:;](.*)/) {
	    $val = $1;
	    $dsn = $2;
	} else {
	    $val = $dsn;
	    $dsn = '';
	}
	if ($val =~ /([^=]*)=(.*)/) {
	    $var = $1;
	    $val = $2;
	    if ($var eq 'hostname' || $var eq 'host') {
		$hash->{'host'} = $val;
	    } elsif ($var eq 'db' || $var eq 'dbname') {
		$hash->{'database'} = $val;
	    } else {
		$hash->{$var} = $val;
	    }
	} else {
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


sub _parse_dsn_host {
    my($class, $dsn) = @_;
    my $hash = $class->_parse_dsn($dsn, ['host', 'port']);
    ($hash->{'host'}, $hash->{'port'});
}



package DBD::monetdb::dr;

$DBD::monetdb::dr::imp_data_size = 0;

use MapiLib;
use strict;


sub connect {
    my $drh = shift;
    my ($dsn, $user, $password, $attrhash) = @_;

    my $data_source_info = DBD::monetdb->_parse_dsn(
						   $dsn, ['host', 'port','database','language'],
						  );

    my $lang= $data_source_info->{language};
    $lang     ||= 'sql';
    if ( ! ($lang eq 'mal' || $lang eq 'sql' || $lang eq 'mil') ) {
	die "!ERROR languages permitted are 'sql', 'mal', and 'mil'\n";
    }

    $user     ||= 'monetdb';
    $password ||= 'monetdb';

    my $host  = $data_source_info->{host};
    my $port  = $data_source_info->{port};
    $port     ||= ($lang eq 'sql')? 45123 : 50000;
    $host	  ||= 'localhost';

    my $server = $host.':'.$port;
    my $dbh = DBI::_new_dbh($drh, {
				   Name         => $dsn,
				   USER         => $user,
				   Username     => $user,
				   CURRENT_USER => $user,
				   Language     => $lang
				  }, {});
    my $mapi;
    eval {
	$mapi = MapiLib::mapi_connect($host, $port, $user, $password, $lang);
	$dbh->STORE(monetdb_connection => $mapi);
    };
    if ($@) {
	return $dbh->DBI::set_err(1, $@);
    }
    if (MapiLib::mapi_error($mapi)) {
	return $dbh->DBI::set_err(MapiLib::mapi_error($mapi), MapiLib::mapi_error_str($mapi));
    }
    $dbh->STORE('Active', 1 );
    return $dbh;
}


sub data_sources {
    return ('dbi:monetdb:');
}


sub disconnect_all {}

package DBD::monetdb::db;

$DBD::monetdb::db::imp_data_size = 0;
use MapiLib;
use strict;


sub ping {
    my $dbh = shift;
    my $mapi = $dbh->FETCH('monetdb_connection');

    MapiLib::mapi_ping($mapi) ? 0 : 1;
}

sub quote {
    my $dbh = shift;
    my ($statement, $type) = @_;
    return 'NULL' unless defined $statement;

    for ($statement) {
	s/	/\\t/g;
	s/\\/\\\\/g;
	s/\n/\\n/g;
	s/\r/\\r/g;
	s/"/\\"/g;
	s/'/''/g;
    }
    return "'$statement'";
}

sub _count_param {
    my @statement = split //, shift;
    my $num = 0;

    while (defined(my $c = shift @statement)) {
	if ($c eq '"' || $c eq "'") {
	    my $end = $c;
	    while (defined(my $c = shift @statement)) {
		last if $c eq $end;
		@statement = splice @statement, 2 if $c eq '\\';
	    }
	} elsif ($c eq '?') {
	    $num++;
	}
    }
    return $num;
}


sub prepare {
    my ($dbh, $statement, $attr) = @_;

    my $mapi = $dbh->{monetdb_connection};
    my $hdl = MapiLib::mapi_new_handle($mapi);
    my $err = MapiLib::mapi_error($mapi);
    return $dbh->set_err($err, MapiLib::mapi_error_str($mapi)) if $err;

    my ($outer, $sth) = DBI::_new_sth($dbh, { Statement => $statement });

    $sth->STORE('NUM_OF_PARAMS', _count_param($statement));

    $sth->{monetdb_hdl} = $hdl;
    $sth->{monetdb_params} = [];
    $sth->{monetdb_types} = [];
    $sth->{monetdb_rows} = -1;

    return $outer;
}


sub commit {
    my $dbh = shift;
    if ($dbh->FETCH('AutoCommit')) {
	warn 'Commit ineffective while AutoCommit is on';
    } else {
	MapiLib::mapi_query($dbh->FETCH('monetdb_connection'), 'commit;');
    }
    1;
}


sub rollback {
    my $dbh = shift;
    if ($dbh->FETCH('AutoCommit')) {
	warn 'Rollback ineffective while AutoCommit is on';
    } else {
	MapiLib::mapi_query($dbh->FETCH('monetdb_connection'), ($dbh->FETCH('Language') ne 'sql')?'abort;':'rollback;');
    }
    1;
}


sub get_info {
    my($dbh, $info_type) = @_;
    require DBD::monetdb::GetInfo;
    my $v = $DBD::monetdb::GetInfo::info{int($info_type)};
    $v = $v->($dbh) if ref $v eq 'CODE';
    return $v;
}


sub type_info_all {
    my ($dbh) = @_;
    require DBD::monetdb::TypeInfo;
    return $DBD::monetdb::TypeInfo::type_info_all;
}


sub tables {
    my $dbh = shift;
    my @args = @_;
    my $mapi = $dbh->FETCH('monetdb_connection');

    my @table_list;

    my $hdl = MapiLib::mapi_query($mapi, ($dbh->FETCH('Language') ne 'sql')?'ls;':'SELECT name FROM tables;');
    die MapiLib::mapi_error_str($mapi) if MapiLib::mapi_error($mapi);

    while (MapiLib::mapi_fetch_row($hdl)) {
	push @table_list, MapiLib::mapi_fetch_field($hdl, 0);
    }
    return MapiLib::mapi_error($mapi)
      ? undef
	: @table_list;
}


sub _ListDBs {
    my $dbh = shift;
    my @database_list;
    push @database_list, MapiLib::mapi_get_dbname($dbh->FETCH('monetdb_connection'));
    return @database_list;
}


sub _ListTables {
    my $dbh = shift;
    return $dbh->tables;
}


sub disconnect {
    my $dbh = shift;
    my $mapi = $dbh->FETCH('monetdb_connection');
    MapiLib::mapi_disconnect($mapi);
    $dbh->STORE('Active', 0 );
    return 1;
}


sub FETCH {
    my $dbh = shift;
    my $key = shift;
    return $dbh->{$key} if $key =~ /^monetdb_/;
    return $dbh->SUPER::FETCH($key);
}


sub STORE {
    my $dbh = shift;
    my ($key, $new) = @_;

    if ($key eq 'AutoCommit') {
	my $old = $dbh->{$key} || 0;
	if ($new != $old) {
	    my $mapi = $dbh->FETCH('monetdb_connection');
	    MapiLib::mapi_setAutocommit($mapi, $new);
	    $dbh->{$key} = $new;
	}
	return 1;

    } elsif ($key =~ /^monetdb_/) {
	$dbh->{$key} = $new;
	return 1;
    }
    return $dbh->SUPER::STORE($key, $new);
}


sub DESTROY {
    my $dbh = shift;
    $dbh->disconnect if $dbh->FETCH('Active');
    my $mapi = $dbh->FETCH('monetdb_connection');
    MapiLib::mapi_destroy($mapi) if $mapi;
}


package DBD::monetdb::st;

use DBI qw(:sql_types);
use MapiLib;


$DBD::monetdb::st::imp_data_size = 0;


sub bind_param {
    my ($sth, $index, $value, $attr) = @_;
    $sth->{monetdb_params}[$index-1] = $value;
    $sth->{monetdb_types}[$index-1] = ref $attr ? $attr->{TYPE} : $attr;
    return 1;
}


sub execute {
    my($sth, @bind_values) = @_;
    my $statement = $sth->{Statement};
    my $dbh = $sth->{Database};

    $sth->bind_param($_, $bind_values[$_-1]) or return for 1 .. @bind_values;

    my $params = $sth->{monetdb_params};
    my $num_of_params = $sth->FETCH('NUM_OF_PARAMS');
    return $sth->set_err(-1, @$params ." values bound when $num_of_params expected")
        unless @$params == $num_of_params;

    for ( 1 .. $num_of_params ) {
        # TODO: parameter type
        my $quoted_param = $dbh->quote($params->[$_-1]);
        $statement =~ s/\?/$quoted_param/;  # TODO: '?' inside quotes/comments
    }
    $sth->trace_msg("    -- Statement: $statement\n", 5);

    my $mapi = $dbh->{monetdb_connection};
    my $hdl = $sth->{monetdb_hdl};
    MapiLib::mapi_query_handle($hdl, $statement);
    my $err = MapiLib::mapi_error($mapi);
    return $sth->set_err($err, MapiLib::mapi_error_str($mapi)) if $err;
    my $result_error = MapiLib::mapi_result_error($hdl);
    return $sth->set_err(-1, $result_error) if $result_error;

    my $rows = MapiLib::mapi_rows_affected($hdl);

    if ( MapiLib::mapi_get_querytype($hdl) != 3 ) {
        $sth->{monetdb_rows} = $rows;
        return $rows || '0E0';
    }
    my ( @names, @types, @precisions, @nullables );
    my $field_count = MapiLib::mapi_get_field_count($hdl);
    for ( 0 .. $field_count-1 ) {
        push @names     , MapiLib::mapi_get_name($hdl, $_);
        push @types     , MapiLib::mapi_get_type($hdl, $_);
        push @precisions, MapiLib::mapi_get_len ($hdl, $_);
        push @nullables , 0;  # TODO
    }
    $sth->STORE('NUM_OF_FIELDS', $field_count) unless $sth->FETCH('NUM_OF_FIELDS');
    $sth->STORE('NAME'         , \@names     );
#   $sth->STORE('TYPE'         , \@types     );  # TODO: monetdb2dbi
#   $sth->STORE('PRECISION'    , \@precisions);  # TODO
#   $sth->STORE('NULLABLE'     , \@nullables );  # TODO

    $sth->{monetdb_rows} = 0;

    return $rows || '0E0';
}


sub fetch {
    my ($sth) = @_;
    my $hdl = $sth->{monetdb_hdl};
    my $field_count = MapiLib::mapi_fetch_row($hdl);
    unless ( $field_count ) {
        my $mapi = $sth->{Database}{monetdb_connection};
        my $err = MapiLib::mapi_error($mapi);
        $sth->set_err($err, MapiLib::mapi_error_str($mapi)) if $err;
        return;
    }
    my @row = map MapiLib::mapi_fetch_field($hdl, $_), 0 .. $field_count-1;
    map { s/\s+$// } @row if $sth->FETCH('ChopBlanks');

    $sth->{monetdb_rows}++;
    return $sth->_set_fbav(\@row);
}

*fetchrow_arrayref = \&fetch;

sub rows {
    my $sth = shift;
    $sth->FETCH('monetdb_rows');
}


sub FETCH {
    my $sth = shift;
    my $key = shift;

    return $sth->{NAME} if $key eq 'NAME';
    return $sth->{$key} if $key =~ /^monetdb_/;
    return $sth->SUPER::FETCH($key);
}


sub STORE {
    my $sth = shift;
    my ($key, $value) = @_;

    if ($key eq 'NAME') {
	$sth->{NAME} = $value;
	return 1;
    } elsif ($key =~ /^monetdb_/) {
	$sth->{$key} = $value;
	return 1;
    }
    return $sth->SUPER::STORE($key, $value);
}


sub DESTROY {
    my $sth = shift;
    MapiLib::mapi_close_handle($sth->FETCH('monetdb_hdl')) if $sth->FETCH('monetdb_hdl');
}


1;
__END__

=head1 NAME

DBD::monetdb - DBD implementation on top of SWIG bindings

=head1 SYNOPSIS

    use DBI;

    $dsn = "dbi:monetdb:database=$database;host=$hostname";

    $dbh = DBI->connect($dsn, $user, $password);

    $drh = DBI->install_driver('monetdb');

    $sth = $dbh->prepare('SELECT * FROM foo WHERE bla');
    $sth->execute;
    $numRows = $sth->rows;
    $numFields = $sth->{'NUM_OF_FIELDS'};
    $sth->finish;

=head1 EXAMPLE

  #!/usr/bin/perl

  use strict;
  use DBI;

  # Connect to the database.
  my $dbh = DBI->connect('dbi:monetdb:database=test;host=localhost',
                         'joe', "joe's password",
                         {'RaiseError' => 1});

  # Drop table 'foo'. This may fail, if 'foo' doesn't exist.
  # Thus we put an eval around it.
  eval { $dbh->do('DROP TABLE foo') };
  print "Dropping foo failed: $@\n" if $@;

  # Create a new table 'foo'. This must not fail, thus we don't
  # catch errors.
  $dbh->do('CREATE TABLE foo (id INTEGER, name VARCHAR(20))');

  # INSERT some data into 'foo'. We are using $dbh->quote() for
  # quoting the name.
  $dbh->do('INSERT INTO foo VALUES (1, ' . $dbh->quote('Tim') . ')');

  # Same thing, but using placeholders
  $dbh->do('INSERT INTO foo VALUES (?, ?)', undef, 2, 'Jochen');

  # Now retrieve data from the table.
  my $sth = $dbh->prepare('SELECT id, name FROM foo');
  $sth->execute();
  while (my $ref = $sth->fetchrow_arrayref()) {
    print "Found a row: id = $ref->[0], name = $ref->[1]\n";
  }
  $sth->finish();

  # Disconnect from the database.
  $dbh->disconnect();


=head1 DESCRIPTION

DBD::monetdb is a Pure Perl client interface for the MonetDB Database Server. It means this module enables you to connect to MonetDB server from any platform where Perl is running, but MonetDB has not been installed.

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

  my $query = sprintf('INSERT INTO foo VALUES (%d, %s)',
		      $number, $dbh->quote('name'));
  $dbh->do($query);

See L<DBI(3)> for details on the quote and do methods. An alternative
approach is

  $dbh->do('INSERT INTO foo VALUES (?, ?)', undef,
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

The port where MonetDB daemon listens to. default for SQL is 45123.

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

The statement handles of DBD::monetdb support a number
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

<DBD::monetdb> surely returns <0E0>.

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
