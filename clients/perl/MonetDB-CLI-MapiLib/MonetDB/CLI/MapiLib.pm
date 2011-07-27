package MonetDB::CLI::MapiLib;

use MapiLib();
use strict;
use warnings;

our $VERSION = '0.03';


sub connect
{
  my ($class, $host, $port, $user, $pass, $lang, $db) = @_;

  my $h = MapiLib::mapi_connect( $host, $port, $user, $pass, $lang, $db )
    or die 'Handle is undefined';
  die MapiLib::mapi_error_str( $h )
    if MapiLib::mapi_error( $h );
  bless { h => $h },'MonetDB::CLI::MapiLib::Cxn';
}


package MonetDB::CLI::MapiLib::Cxn;

sub query
{
  my ($self, $statement) = @_;

  my $h = MapiLib::mapi_query( $self->{h}, $statement )
    or die 'Handle is undefined ('. MapiLib::mapi_error_str( $self->{h} ) .')';
  die MapiLib::mapi_result_error( $h )
    if MapiLib::mapi_result_error( $h );
  die MapiLib::mapi_error_str( $self->{h} )
    if MapiLib::mapi_error( $self->{h} );
  bless { h => $h, p => $self },'MonetDB::CLI::MapiLib::Req';
}

sub new_handle
{
  my ($self) = @_;

  my $h = MapiLib::mapi_new_handle( $self->{h} )
    or die 'Handle is undefined ('. MapiLib::mapi_error_str( $self->{h} ) .')';
  die MapiLib::mapi_result_error( $h )
    if MapiLib::mapi_result_error( $h );
  die MapiLib::mapi_error_str( $self->{h} )
    if MapiLib::mapi_error( $self->{h} );
  bless { h => $h, p => $self },'MonetDB::CLI::MapiLib::Req';
}

sub DESTROY
{
  my ($self) = @_;

  MapiLib::mapi_destroy( $self->{h});
  die MapiLib::mapi_error_str( $self->{h} )
    if MapiLib::mapi_error( $self->{h} );
  return;
}


package MonetDB::CLI::MapiLib::Req;

sub query
{
  my ($self, $statement) = @_;

  MapiLib::mapi_query_handle( $self->{h}, $statement );
  die MapiLib::mapi_result_error( $self->{h} )
    if MapiLib::mapi_result_error( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return;
}

sub querytype
{
  my ($self) = @_;

  my $r = MapiLib::mapi_get_querytype( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub id
{
  my ($self) = @_;

  my $r = MapiLib::mapi_get_tableid( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub rows_affected
{
  my ($self) = @_;

  my $r = MapiLib::mapi_rows_affected( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub columncount
{
  my ($self) = @_;

  my $r = MapiLib::mapi_get_field_count( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub name
{
  my ($self, $fnr) = @_;

  my $r = MapiLib::mapi_get_name( $self->{h}, $fnr );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub type
{
  my ($self, $fnr) = @_;

  my $r = MapiLib::mapi_get_type( $self->{h}, $fnr );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub length
{
  my ($self, $fnr) = @_;

  my $r = MapiLib::mapi_get_len( $self->{h}, $fnr );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub fetch
{
  my ($self) = @_;

  my $r = MapiLib::mapi_fetch_row( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub field
{
  my ($self, $fnr) = @_;

  my $r = MapiLib::mapi_fetch_field( $self->{h}, $fnr );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return $r;
}

sub finish
{
  my ($self) = @_;

  MapiLib::mapi_finish( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return;
}

sub DESTROY
{
  my ($self) = @_;

  MapiLib::mapi_close_handle( $self->{h} );
  die MapiLib::mapi_error_str( $self->{p}{h} )
    if MapiLib::mapi_error( $self->{p}{h} );
  return;
}

__PACKAGE__;

=head1 NAME

MonetDB::CLI::MapiLib - MonetDB::CLI implementation, using MapiLib

=head1 DESCRIPTION

MonetDB::CLI::MapiLib is an implementation of the MonetDB call level interface
L<MonetDB::CLI>.
It uses the SWIG generated L<MapiLib> - a wrapper module for libMapi.
Normally, you don't use this module directly, but let L<MonetDB::CLI>
choose an implementation module.

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

  Homepage    : http://monetdb.cwi.nl
  SourceForge : http://sourceforge.net/projects/monetdb

=head2 Perl modules

L<MonetDB::CLI>, L<MapiLib>

=cut
