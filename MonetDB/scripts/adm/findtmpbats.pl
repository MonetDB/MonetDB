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

sub usage() {
  print "Usage: $0 <dbfarm>\n";
  exit -1;
}

my $dbfarm      = shift || usage;
my @dbs         = ();
my %bats        = ();

sub readBBP ($) {
  my $dbdir = shift;
  open BBP, "<$dbdir/BBP.dir";
  <BBP>;
  while (<BBP>) {
    my @fs = split /,\s*/;
    $bats{$fs[3]}++;
  }
  close BBP;
}

sub checkBBP ($) {
  my $dbdir = shift;
  # print $dbdir, "\n";
  %bats = ();
  readBBP $dbdir;
  find(\&findtmpbats, $dbdir);
}

sub findtmpbats {
  return unless -f;
  return unless $File::Find::name =~ m|bat/(\d+)|gc;
  my $bat = $1;
  $bat .= $1 while $File::Find::name =~ m|(/\d+)|gc;
  print $File::Find::name, "\n" if not defined $bats{$bat};
}

#
# Main:
#

find 
  sub { 
    push @dbs, $1 if -f && $File::Find::name =~ m|(.*)/BBP.dir|; 
  }, 
  $dbfarm;
map { checkBBP $_; } @dbs;
