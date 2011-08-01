package MonetDB::CLI;

our $VERSION = '0.03';

our @Modules = split /;/, $ENV{PERL_MONETDB_CLI_MODULES}
  || 'MonetDB::CLI::MapiPP';

sub connect
{
  my $class = shift;

  eval "require( $_ )" and return $_->connect( @_ ) for @Modules;

  chomp $@; die "No MonetDB::CLI implementation found: $@";
}

__PACKAGE__;

=head1 NAME

MonetDB::CLI - MonetDB Call Level Interface

=head1 SYNOPSIS

  use MonetDB::CLI();

  my $cxn = MonetDB::CLI->connect( $host, $port, $user, $pass, $lang, $db );

  my $req = $cxn->query('select * from env() env');
  while ( my $cnt = $req->fetch ) {
    print $req->field( $_ ) for 0 .. $cnt-1;
  }

=head1 DESCRIPTION

MonetDB::CLI is a call level interface for MonetDB, somewhat similar
to SQL/CLI, ODBC, JDBC or DBI.

B<Note:> In its current incarnation, this interface resembles the MonetDB
Application Programming Interface.
In the future, MAPI will be replaced by the MonetDB/Five Communication Layer
(MCL).
It is not guaranteed that this call level interface stays the same!

=head2 The C<connect()> method

  my $cxn = MonetDB::CLI->connect( $host, $port, $user, $pass, $lang, $db );

This method tries to load an implementation module from C<@Modules> and
delegates to the C<connect()> method of the first successful loaded module.
Otherwise, an exception is raised.

The default list of implementation modules can be changed with the
C<PERL_MONETDB_CLI_MODULES> environment variable.
A semicolon-separated list of module names is expected.

=head2 Connection object methods

It's up to the implementation modules to provide the methods for the
connection object:

  my $req = $cxn->query( $statement );  # request object

=head2 Request object methods

It's up to the implementation modules to provide the methods for the
request object:

  print $req->querytype;
  print $req->id;
  print $req->rows_affected;
  print $req->columncount;

  for ( 0 .. $req->columncount - 1 ) {
    print $req->name  ( $_ );
    print $req->type  ( $_ );
    print $req->length( $_ );
  }
  while ( my $cnt = $req->fetch ) {
    print $req->field( $_ ) for 0 .. $cnt-1;
  }

=head1 AUTHORS

Steffen Goeldner E<lt>sgoeldner@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

The contents of this file are subject to the MonetDB Public License
Version 1.1 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://www.monetdb.org/Legal/MonetDBLicense

Software distributed under the License is distributed on an "AS IS"
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
License for the specific language governing rights and limitations
under the License.

The Original Code is the MonetDB Database System.

The Initial Developer of the Original Code is CWI.
Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
Copyright August 2008-2011 MonetDB B.V.
All Rights Reserved.


=head1 SEE ALSO

=head2 MonetDB

  Homepage    : http://www.monetdb.org/

=head2 Perl modules

L<MonetDB::CLI::MapiLib>, L<MonetDB::CLI::MapiXS>, L<MonetDB::CLI::MapiPP>

=cut
