#!/usr/bin/perl -w
#
# Run a loop-lifted XQuery SQL expression and serialize the
# result into the XQuery Data Model.
#
# (c) 2007 Jens Teubner, TU Muenchen, Database Systems Group

use strict;

# we require Perl's database interfaces
use DBI;

# change `XML' to your database name, or the entire string to
# match something other than DB2
my $DSN = 'DBI:DB2:XML';

my $input_query = "";    # SQL code as received from PF
my $final_query = "";    # actual query sent to DBMS
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
            if ($line =~ /\b(document-relation|
                             document-pre|
                             document-size|
                             document-kind|
                             document-prop|
                             document-tag|
                             result-relation|
                             result-pos-nat|
                             result-item-pre|
                             result-item-int|
                             result-item-str|
                             result-item-dbl|
                             result-item-dec):\s+
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

    die "Schema information on document relation incomplete.\n"
        unless ($schema{'document-relation'} && $schema{'document-pre'}
                && $schema{'document-size'} && $schema{'document-kind'}
                && $schema{'document-prop'} && $schema{'document-tag'});

    die "Schema information on result relation incomplete.\n"
        unless ($schema{'result-relation'} && $schema{'result-pos-nat'}
                && ($schema{'result-item-pre'} || $schema{'result-item-int'}
                    || $schema{'result-item-str'} || $schema{'result-item-dbl'}
                    || $schema{'result-item-dec'}));
}

# read user input
&read_query;

my $query_type;   # query with atoms only, or are there nodes also?
my %idx;          # in which of the return columns can we find all of
                  # the values we are interested in?

# set up the query we want to send to the database
if (defined $schema{'result-item-pre'}
    && (defined $schema{'result-item-int'} || defined $schema{'result-item-str'}
        || defined $schema{'result-item-dbl'}
        || defined $schema{'result-item-dec'})) {

    # This query contains atomic values AND nodes
    $query_type = 'ATOMIC_AND_NODES';

    $final_query = "$input_query\nSELECT ";

    my $cur = 0;

    foreach my $t ('int', 'str', 'dbl', 'dec') {

        if (defined $schema{"result-item-$t"}) {
            if ($cur) { $final_query.= ',' }
            $idx{$t} = $cur;
            $cur++;
            $final_query.= "\nr.".$schema{"result-item-$t"}." AS $t";
        }
    }

    $idx{'pre'} = $cur;

    $final_query.= ',';
    $final_query.= <<"EOT";
             d2.$schema{'document-pre'} AS pre,
             d2.$schema{'document-size'} AS size,
             d2.$schema{'document-kind'} AS kind,
             d2.$schema{'document-prop'} AS prop,
             d2.$schema{'document-tag'} AS tag
        FROM $schema{'result-relation'} AS r
             LEFT OUTER JOIN $schema{'document-relation'} AS d1
               ON d1.pre = r.$schema{'result-item-pre'}
             LEFT OUTER JOIN $schema{'document-relation'} AS d2
               ON d2.pre >= d1.pre AND d2.pre <= d1.pre + d1.size
       ORDER BY r.pos_nat, d2.pre;
EOT

}
elsif (defined $schema{'result-item-pre'}) {

    # This query returns nodes, only
    $query_type = 'NODES_ONLY';

    $final_query = <<"EOT";
      $input_query
      SELECT d2.$schema{'document-pre'} AS pre,
             d2.$schema{'document-size'} AS size,
             d2.$schema{'document-kind'} AS kind,
             d2.$schema{'document-prop'} AS prop,
             d2.$schema{'document-tag'} AS tag
        FROM $schema{'result-relation'} AS r
             INNER JOIN $schema{'document-relation'} AS d1
               ON d1.pre = r.$schema{'result-item-pre'}
             INNER JOIN $schema{'document-relation'} AS d2
               ON d2.pre >= d1.pre AND d2.pre <= d1.pre + d1.size
       ORDER BY r.pos_nat, d2.pre;
EOT

    $idx{'pre'} = 0; 

}
else {

    # This query returns atomic values, only
    $query_type = 'ATOMIC_ONLY';

    $final_query = "$input_query\nSELECT ";

    my $cur = 0;

    foreach my $t ('int', 'str', 'dbl', 'dec') {

        if (defined $schema{"result-item-$t"}) {
            if ($cur) { $final_query.= ',' }
            $idx{$t} = $cur;
            $cur++;
            $final_query.= "\nr.".$schema{"result-item-$t"}." AS $t";
        }
    }

    $final_query .= <<"EOT";
        FROM $schema{'result-relation'} AS r
       ORDER BY r.pos_nat;
EOT

}

#print $final_query;

# connect to the database
my $dbh = DBI->connect ($DSN)
    or die "Could not connect to database with DSN `$DSN': " . DBI->errstr;

# prepare and execute the query
my $sth = $dbh->prepare ($final_query)
    or die "Could not prepare query: " . DBI->errstr;
$sth->execute () or die "Could not execute query: " . DBI->errstr;

# print XML header for result
print "<?xml version='1.0'?>";

my @stack = ();       # stack to print all the closing tags
my $last = 'ELEM';    # assume the last thing we printed was an element

while (my @data = $sth->fetchrow_array ()) {

    # is it a node that we currently print?
    if ($query_type eq 'NODES_ONLY'
        || ($query_type eq 'ATOMIC_AND_NODES' && defined $data[$idx{'pre'}])) {

        if ($data[$idx{'pre'} + 2] == 1) {           # kind == elem

            # newline between adjacent result elements
            if ($#stack < 0 && $last eq 'ELEM') {
                print "\n";
            }

            # remove trailing spaces as they are returned by DB2
            $data[$idx{'pre'} + 4] =~ s/ +$//;

            print '<'.$data[$idx{'pre'} + 4].'>';    # print doc.tag
            push @stack, [ $data[$idx{'pre'}],       # pre
                           $data[$idx{'pre'} + 1],   # size
                           $data[$idx{'pre'} + 4] ]; # tag
            $last = 'ELEM';
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

# close database connection
$dbh->disconnect;
