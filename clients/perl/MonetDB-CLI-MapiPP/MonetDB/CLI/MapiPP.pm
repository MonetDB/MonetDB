package MonetDB::CLI::MapiPP;

use Text::ParseWords();
use Encode ();
use Mapi;
use strict;
use warnings;

our $VERSION = '0.04';


my %unescape = ( n => "\n", t => "\t", r => "\r", f => "\f");

sub unquote
{
  if (!defined($_) || $_ eq 'NULL' || $_ eq 'nil') { $_ = undef; return; }

  if ( /^["']/) {
    s/^["']//;
    s/["']$//;
    s/\\([0-7]{3}|.)/length($1) == 3 ? chr(oct($1)) : ($unescape{$1} || $1)/eg;
  }
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
    # "regular" resultset, or just "tuple"
    $self->{querytype} = 1;
    $self->{id} = $h->{id} || -1;
    $self->{affrows} = $h->{count} if $h->{count};
    $self->{colcnt} = $h->{nrcols} if $h->{nrcols};

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
    # we must pre-fetch if this is not an SQL result-set
    if (!defined $h->{count}) {
      do {
        utf8::decode($self->{h}->{row});
        my @cols = split(/,\t */, $h->{row});
        my $i = -1;
        for (@cols) {
          s/^\[ //;
          s/[ \t]+\]$//;
          MonetDB::CLI::MapiPP::unquote();
        }
        push(@{$self->{rows}}, [@cols]);
      } while (($tpe = $h->getReply()) > 0);
      $self->{affrows} = @{$self->{rows}};
      undef $self->{id};
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

  return $self->{id} || -1;
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
  
  if ($self->{id}) {
    utf8::decode($self->{h}->{row});
    my @cols = split(/,\t */, $self->{h}->{row});
    my $i = -1;
    $cols[0] =~ s/^\[ //;
    $cols[-1] =~ s/[ \t]+\]$//;
    for (@cols) {
      MonetDB::CLI::MapiPP::unquote();
    }
    $self->{currow} = [@cols];
    $self->{h}->getReply();
  } else {
    $self->{currow} = $self->{rows}[$self->{i}];
  }
  
  return @{$self->{currow}};
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

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

=head1 SEE ALSO

=head2 MonetDB

  Homepage    : http://www.monetdb.org/

=head2 Perl modules

L<MonetDB::CLI>

=cut

# vim: set ts=2 sw=2 expandtab:
