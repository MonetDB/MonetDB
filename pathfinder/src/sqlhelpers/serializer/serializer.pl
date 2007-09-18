#!/usr/bin/perl -w
# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

use strict;

# we require Perl's database interfaces
use DBI;

# change `XML' to your database name, or the entire string to
# match something other than DB2
my $DSN = 'DBI:DB2:XML';

my $input_query = "";    # SQL code as received from PF
my %schema;              # schema information we got from PF

# read query from stdin or a file
sub read_query {

    my $state = 'START';

    while (<>) {
        chomp;
        my $line = $_;

        # start looking for schema information as soon as we
        # read this magic string
        if ($state eq 'START'
            && $line eq '-- !! START SCHEMA INFORMATION ** DO NOT EDIT THESE LINES !!')
        {
            $state = 'IN_SCHEMA_INFORMATION';
        }
        
        #
        # If we are in the IN_SCHEMA_INFORMATION state, look out for
        # relation/attribute names specified in the query
        #
        # We collect all instructions
        #
        #   <name>: <val>
        #
        # in the Perl hash $schema (with key <name> and value <val>).
        #
        elsif ($state eq 'IN_SCHEMA_INFORMATION') {

            # these are the strings we look for
            if ($line =~ /\b(Type|
                             result-item_int|
                             result-item_str|
                             result-item_dec|
                             result-item_dbl):\s+
                             \b(\w+)\b/x) {
                $schema{$1} = $2;
            }

            elsif ($line eq '-- !! END SCHEMA INFORMATION ** DO NOT EDIT THESE LINES !!') {
                $state = 'DONE_SCHEMA_INFORMATION';
            }
        }

        # simply collect all the other lines into the SQL query
        else {
            $input_query .= "$line\n";
        }
    }

    die "No schema information found in query.\n"
        unless $state eq 'DONE_SCHEMA_INFORMATION';

#    die "Schema information on document relation incomplete.\n"
#        unless ($schema{'document-pre'}
#                && $schema{'document-size'} && $schema{'document-kind'}
#                && $schema{'document-value'} && $schema{'document-tag'})
#                && $schema{'document-name'};

    # if Type is NODES_ONLY then only Type has to be defined
    # if Type is ATOM_ONLY or ATOMIC_AND_NODES one of the
    # result items has to be defined
    die "Schema information on result relation incomplete.\n"
        unless (defined $schema{'Type'}
                && (($schema{'Type'} eq 'NODES_ONLY')
                    || ($schema{'Type'} eq 'ATOMIC_ONLY'
                       || $schema{'Type'} eq 'ATOMIC_AND_NODES')
                         && (defined $schema{'result-item_int'}
                            || defined $schema{'result-item_str'}
                            || defined $schema{'result-item_dec'}
                            || defined $schema{'result-item_dbl'})))
}

# read user input
&read_query;

my $query_type;   # query with atoms only, or are there nodes also?
my %idx;          # in which of the return columns can we find all of
                  # the values we are interested in?

# set up the query we want to send to the database
if (defined $schema{'Type'}
    && $schema{'Type'} eq 'ATOMIC_AND_NODES') {

    # This query contains atomic values AND nodes
    $query_type = 'ATOMIC_AND_NODES';

    my $cur = 0;

    foreach my $t ('int', 'str', 'dbl', 'dec') {

        if (defined $schema{"result-item_$t"}) {
            $idx{$t} = $cur;
            $cur++;
        }
    }

    $idx{'pre'} = $cur;
}
elsif (defined $schema{'Type'}
       && $schema{'Type'} eq 'NODES_ONLY') {

    # This query returns nodes, only
    $query_type = 'NODES_ONLY';

    $idx{'pre'} = 0; 
}
else {
    # This query returns atomic values, only
    $query_type = 'ATOMIC_ONLY';

    my $cur = 0;

    foreach my $t ('int', 'str', 'dbl', 'dec') {
        if (defined $schema{"result-item_$t"}) {
            $idx{$t} = $cur;
            $cur++;
        }
    }
}

my $final_query = "";    # actual query sent to DBMS
$final_query = "$input_query";
#print $final_query;

# connect to the database
my $dbh = DBI->connect_cached ($DSN)
    or die "Could not connect to database with DSN `$DSN': " . DBI->errstr;

# prepare and execute the query
my $sth = $dbh->prepare ($final_query)
    or die "Could not prepare query: " . DBI->errstr;
$sth->execute () or die "Could not execute query: " . DBI->errstr;

# print XML header for result
print "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
# print a root node to make a valid XQuery document
print "\n<XQuery>";

my @stack = ();       # stack to print all the closing tags
my $last = 'ELEM';    # assume the last thing we printed was an element

while (my @data = $sth->fetchrow_array ()) {

    # is it a node that we currently print?
    if ($query_type eq 'NODES_ONLY'
        || ($query_type eq 'ATOMIC_AND_NODES' && defined $data[$idx{'pre'}])) {

        # print trailing > to close an element
        if (($last eq 'ATTR') && ($data[$idx{'pre'} + 2] != 2)) {
            print '>';
            $last = 'ELEM';
        }
            

        if ($data[$idx{'pre'} + 2] == 1) {           # kind == elem

            # newline between adjacent result elements
            if ($#stack < 0 && $last eq 'ELEM') {
                print "\n";
            }

            # remove trailing spaces as they are returned by DB2
            $data[$idx{'pre'} + 5] =~ s/ +$//;

            print '<'.$data[$idx{'pre'} + 5].'';    # print doc.tag
            push @stack, [ $data[$idx{'pre'}],       # pre
                           $data[$idx{'pre'} + 1],   # size
                           $data[$idx{'pre'} + 5] ]; # tag
            $last = 'ATTR';
        }
        elsif ($data[$idx{'pre'} + 2] == 2) {        #kind == attr
            print " ".$data[$idx{'pre'} + 4]."=\"".$data[$idx{'pre'} + 3]."\"";
            $last = 'ATTR';
        }
        elsif ($data[$idx{'pre'} + 2] == 3) {        # kind == text
            # remove trailing spaces as they are returned by DB2
            $data[$idx{'pre'} + 3] =~ s/ +$//;
            print $data[$idx{'pre'} + 3];            # print doc.prop
            $last = 'NODE';
        }
        elsif ($data[$idx{'pre'} + 2] == 4) {           # kind == comm
            # remove trailing spaces as they are returned by DB2
            $data[$idx{'pre'} + 3] =~ s/ +$//;
            print '<!--'.$data[$idx{'pre'} + 3].'-->';  # print doc.prop
            $last = 'NODE';
        }
        elsif ($data[$idx{'pre'} + 2] == 5) {           # kind == pi
            # remove trailing spaces as they are returned by DB2
            $data[$idx{'pre'} + 3] =~ s/ +$//;
            print '<?'.$data[$idx{'pre'} + 3].'?>';  # print doc.prop
            $last = 'NODE';
        }

        # print closing tags if necessary
        while ($#stack >= 0
                && ($stack[-1][0] + $stack[-1][1]) <= $data[$idx{'pre'}]) {
           if ($last eq 'ATTR') { print '>'; $last = 'ELEM'; next; }
           print "</".$stack[-1][2].">";
           pop @stack;
           $last = 'ELEM'
        }
    }
    else {

        # space between adjacent atoms
        if ($last eq 'ATOM') {
            print ' ';
        }

        foreach my $t ('int', 'str', 'dbl', 'dec') {

            if (defined $idx{$t} && defined $data[$idx{$t}]) {
                print $data[$idx{$t}];
                $last = 'ATOM';
            }
        }
    }
}
print "\n</XQuery>";
print "\n";
# close database connection
$dbh->disconnect;
