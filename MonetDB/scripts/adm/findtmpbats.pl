#!/usr/bin/perl
#
# (C) 2002, Arjen P. de Vries
#

#
# Finds under a dbfarm directory all database directories, 
# i.e., directories in which a BBP.dir file resides.
# Next, finds all bats in that database directory that are stored on disk,
# but do not exist in the BBP.
#
# Usage example (DISCLAIMER!):
#   ./findtmpbats.pl ~/data/monetdb | xargs rm -f
# 

use File::Find;
use File::stat qw(:FIELDS);

sub usage() {
  print "Usage: $0 [--print-size] <dbfarm>\n";
  exit -1;
}

my $dbfarm      = shift || usage;
my $PRINT_SIZE        = 0;
if ($dbfarm =~ /--print-size/) {
  $PRINT_SIZE   = 1;
  $dbfarm = shift || usage;
}

my $size	= 0;
my @dbs         = ();
my %bats        = ();

sub readBBP ($) {
  my $bbp = shift;
  open BBP, "<$bbp";
  <BBP>;
  while (<BBP>) {
    my @fs = split /,\s*/;
    $bats{$fs[3]}++;
  }
  close BBP;
}

sub checkBBP ($) {
  my $bbp = shift;
  my $dbdir;
  %bats = ();
  readBBP $bbp;
  ($dbdir = $bbp) =~ s|(.*/bat)(?:/BACKUP)?/BBP.dir$|$1|;
  # print "DIR: ", $dbdir, "\n";
  find(\&findtmpbats, $dbdir);
}

sub findtmpbats {
  return unless -f;
  return unless $File::Find::name =~ m|bat/(\d+)|gc;
  my $bat = $1;
  $bat .= $1 while $File::Find::name =~ m|(/\d+)|gc;
  if (not defined $bats{$bat}) { 
    print $File::Find::name, "\n"; 
    if ($PRINT_SIZE) {
      stat($File::Find::name);
      $size += $st_size;
    }
  }
}

#
# Main:
#

find 
  sub { 
    push @dbs, $File::Find::name if -f && $File::Find::name =~ m|.*/BBP.dir$|; 
  }, 
  $dbfarm;
map { checkBBP $_; } @dbs;
print STDERR "Total size: ", $size >> 20, "Mb\n" if $PRINT_SIZE;
