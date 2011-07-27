package MonetDB::CLI::MapiPP;

use IO::Socket::INET();
use Text::ParseWords();
use strict;
use warnings;

our $VERSION = '0.03';


my %unescape = ( n => "\n", t => "\t", r => "\r", f => "\f");

sub unquote
{
  my ($class, $v) = @_;

  return undef if $v eq 'NULL' || $v eq 'nil';

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

  my $h = IO::Socket::INET->new( PeerAddr => $host, PeerPort => $port )
    or die "Handle is undefined: $@";
  <$h>;
  print $h "$user:$pass:$lang:line\n" or die $!;
  while ( local $_ = <$h> ) {
    last if /^\001/;
  }
  bless { h => $h, lang => $lang },'MonetDB::CLI::MapiPP::Cxn';
}


package MonetDB::CLI::MapiPP::Cxn;

sub query
{
  my ($self, $statement) = @_;

  my $h = $self->new_handle;
  $h->query( $statement );

  return $h;
}

sub new_handle
{
  my ($self) = @_;

  bless { p => $self },'MonetDB::CLI::MapiPP::Req';
}

sub DESTROY
{
  my ($self) = @_;

  $self->{h}->close;

  return;
}


package MonetDB::CLI::MapiPP::Req;

sub query
{
  my ($self, $statement) = @_;

  my $lang  = $self->{p}{lang};
  my $h     = $self->{p}{h};
  my $delim = $lang eq 'sql' ? qr(\s*,\s*) : qr(\s+);
  my @err;

  if ( $lang eq 'sql') {
    my @statement = split /\n/, $statement;
    s/--.*// for @statement;  # TODO: -- inside '' (or blocked mode?)
    $statement  = join ' ', @statement;
    $statement .= ';' unless $statement =~ /;$/;
    $statement  = 's' . $statement;
  }
  else {
    $statement  =~ s/\n/ /g;
  }
  print $h $statement,"\n" or die $!;

  $self->finish;

  while ( local $_ = <$h> ) {
    chomp;
    if (/^\[/) {
      die "Incomplete tuple: $_" unless /\]$/;
      s/^\[\s*//;
      s/\s*\]$//;
      my @a = Text::ParseWords::parse_line( qr(\s*,\s*), 0, $_ );
      push @{$self->{rs}}, [ map { MonetDB::CLI::MapiPP->unquote( $_ ) } @a ];
    }
    elsif (/^&(\d) (\d+) (\d+) (\d+)/) {
      $self->{querytype}   = $1 if $self->{querytype}   < 0;
      $self->{id}          = $2 if $self->{id}          < 0;
      $self->{tuplecount}  = $3 if $self->{tuplecount}  < 0;
      $self->{columncount} = $4 if $self->{columncount} < 0;
    }
    elsif (/^&(\d) (\d+)/) {
      $self->{querytype}   = $1 if $self->{querytype}   < 0;
      $self->{tuplecount}  = $2 if $self->{tuplecount}  < 0;
    }
    elsif (/^#\s+\b(.*)\b\s+# (name|type|length)$/) {
      $self->{$2} = [ split $delim, $1 ];
    }
    elsif (/^!/) {
      push @err, $_;
    }
    elsif (/^\001\001/) {
      last;
    }
    elsif (/^\001\002/) {
      die "Incomplete query: $statement";
    }
  }
  $self->{columncount}   = @{$self->{name}} if $self->{columncount} < 0;;
  $self->{columncount} ||= @{$self->{rs}[0]} if $self->{rs}[0];
  $self->{tuplecount}    = @{$self->{rs}} if $lang ne 'sql';

  die join "\n", @err if @err;

  return;
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

  return $self->{tuplecount};
}

sub columncount
{
  my ($self) = @_;

  return $self->{columncount};
}

sub name
{
  my ($self, $fnr) = @_;

  return $self->{name}[$fnr] || '';
}

sub type
{
  my ($self, $fnr) = @_;

  return $self->{type}[$fnr] || '';
}

sub length
{
  my ($self, $fnr) = @_;

  return $self->{length}[$fnr] || 0;
}

sub fetch
{
  my ($self) = @_;

  return if ++$self->{i} > $#{$self->{rs}};
  return $self->{columncount};
}

sub field
{
  my ($self, $fnr) = @_;

  return $self->{rs}[$self->{i}][$fnr];
}

sub finish
{
  my ($self) = @_;

  $self->{$_} = -1 for qw(querytype id tuplecount columncount i);
  $self->{$_} = [] for qw(rs name type length);

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
