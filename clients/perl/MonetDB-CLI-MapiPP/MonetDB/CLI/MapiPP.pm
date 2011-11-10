package MonetDB::CLI::MapiPP;

use Text::ParseWords();
use Mapi;
use strict;
use warnings;

our $VERSION = '0.04';


my %unescape = ( n => "\n", t => "\t", r => "\r", f => "\f");

sub unquote
{
  my ($class, $v) = @_;

  return undef if !$v || $v eq 'NULL' || $v eq 'nil';

  if ( $v =~ /^["']/) {
    $v =~ s/^["']//;
    $v =~ s/["']$//;
    $v =~ s/\\(.)/$unescape{$1}||$1/eg;
  }
  return $v;
}


sub connect
{
  my ($class, $host, $port, $user, $pass, $lang, $db) = @_;

  my $h = new Mapi($host, $port, $user, $pass, $lang, $db, 0)
  	or die "Making connection failed: $@";

  bless { h => $h },'MonetDB::CLI::MapiPP::Cxn';
}


package MonetDB::CLI::MapiPP::Cxn;

sub query
{
  my ($self, $statement) = @_;

  my $h = $self->new_handle;
  $h->query($statement);

  return $h;
}

sub new_handle
{
  my ($self) = @_;

  bless { h => $self->{h} },'MonetDB::CLI::MapiPP::Req';
}

sub DESTROY
{
  my ($self) = @_;

  $self->{h}->disconnect();

  return;
}


package MonetDB::CLI::MapiPP::Req;

sub query
{
  my ($self, $statement) = @_;

  my $h = $self->{h};
  $h->doRequest($statement);

  $self->{i} = -1;
  $self->{rows} = [];
  $self->{querytype} = -1;
  $self->{id} = -1;
  $self->{affrows} = -1;
  $self->{colcnt} = -1;
  $self->{colnames} = [];
  $self->{coltypes} = [];
  $self->{collens} = [];

  my $tpe = $h->getReply();
  if ($tpe > 0) {
    # "regular" resultset
    $self->{querytype} = 1;
    $self->{id} = $h->{id};
    $self->{affrows} = $h->{count};
    $self->{colcnt} = $h->{nrcols};

    my $hdr;
    foreach $hdr (@{$h->{hdrs}}) {
      my $nme = substr($hdr, rindex($hdr, "# "));
      $hdr = substr($hdr, 2, -(length($nme) + 1));
      if ($nme eq "# name") {
        @{$self->{colnames}} = split(/,\t/, $hdr);
      } elsif ($nme eq "# type") {
        @{$self->{coltypes}} = split(/,\t/, $hdr);
      } elsif ($nme eq "# length") {
        @{$self->{collens}} = split(/,\t/, $hdr);
      }
      # TODO: table_name
    }
  } elsif ($tpe == -1) {
    # error
    die $h->{errstr};
  } elsif ($tpe == -2) {
    # update count/affected rows
    $self->{affrows} = $h->{count};
  }
}

sub querytype
{
  my ($self) = @_;

  return $self->{querytype};
}

sub id
{
  my ($self) = @_;

  return $self->{id};
}

sub rows_affected
{
  my ($self) = @_;

  return $self->{affrows};
}

sub columncount
{
  my ($self) = @_;

  return $self->{colcnt};
}

sub name
{
  my ($self, $fnr) = @_;

  return $self->{colnames}[$fnr] || '';
}

sub type
{
  my ($self, $fnr) = @_;

  return $self->{coltypes}[$fnr] || '';
}

sub length
{
  my ($self, $fnr) = @_;

  return $self->{collens}[$fnr] || 0;
}

sub fetch
{
  my ($self) = @_;
  
  return if ++$self->{i} >= $self->{affrows};
  
  my @cols = split(/,\t */, $self->{h}->{row});
  my $i = -1;
  while (++$i < @cols) {
    $cols[$i] =~ s/^\[ //;
    $cols[$i] =~ s/[ \t]+\]$//;
    $cols[$i] = MonetDB::CLI::MapiPP->unquote($cols[$i]);
  }
  $self->{currow} = [@cols];
  
  $self->{h}->getReply();
  
  return $self->{colcnt};
}

sub field
{
  my ($self, $fnr) = @_;

  return $self->{currow}[$fnr];
}

sub finish
{
  my ($self) = @_;

  $self->{$_} = -1 for qw(querytype id tuplecount columncount i);
  $self->{$_} = "" for qw(query);
  $self->{$_} = [] for qw(rows name type length);

  return;
}

sub DESTROY
{
  my ($self) = @_;

  return;
}

__PACKAGE__;

=head1 NAME

MonetDB::CLI::MapiPP - MonetDB::CLI implementation, using the Mapi protocol

=head1 DESCRIPTION

MonetDB::CLI::MapiPP is an implementation of the MonetDB call level interface
L<MonetDB::CLI>.
It's a Pure Perl module.
It uses the Mapi protocol - a text based communication layer on top of TCP.
Normally, you don't use this module directly, but let L<MonetDB::CLI>
choose an implementation module.

=head1 AUTHORS

Steffen Goeldner E<lt>sgoeldner@cpan.orgE<gt>.
Fabian Groffen E<lt>fabian@cwi.nlE<gt>.

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

L<MonetDB::CLI>

=cut

# vim: set ts=2 sw=2 expandtab:
