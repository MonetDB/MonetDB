package DBD::monetdb;

use strict;
use DBI();
use Encode();
use MonetDB::CLI();

our $VERSION = '0.10';
our $drh = undef;

require DBD::monetdb::GetInfo;
require DBD::monetdb::TypeInfo;


sub driver {
    return $drh if $drh;

    my ($class, $attr) = @_;

    $drh = DBI::_new_drh($class .'::dr', {
        Name        => 'monetdb',
        Version     => $VERSION,
        Attribution => 'DBD::monetdb by Martin Kersten, Arjan Scherpenisse and Steffen Goeldner',
    });
}


sub CLONE {
    undef $drh;
}



package DBD::monetdb::dr;

$DBD::monetdb::dr::imp_data_size = 0;


sub connect {
    my ($drh, $dsn, $user, $password, $attr) = @_;

    my %dsn;
    for ( split /;|:/, $dsn ||'') {
        if ( my ( $k, $v ) = /(.*?)=(.*)/) {
            $k = 'host'     if $k eq 'hostname';
            $k = 'database' if $k eq 'dbname' || $k eq 'db';
            $dsn{$k} = $v;
            next;
        }
        for my $k ( qw(host port database language) ) {
            $dsn{$k} = $_, last unless defined $dsn{$k};
        }
    }
    my $lang  = $dsn{language} || 'sql';
    my $host  = $dsn{host} || 'localhost';
    my $port  = $dsn{port} || 50000;
    $user     ||= 'monetdb';
    $password ||= 'monetdb';
    my $db = $dsn{database} || 'demo';

    my $cxn = eval { MonetDB::CLI->connect($host, $port, $user, $password, $lang, $db) };
    return $drh->set_err(-1, $@) if $@;

    my ($outer, $dbh) = DBI::_new_dbh($drh, { Name => $dsn });

    $dbh->STORE('Active', 1 );

    $dbh->{monetdb_connection} = $cxn;
    $dbh->{monetdb_language} = $lang;

    return $outer;
}


sub data_sources {
    return ('dbi:monetdb:');
}



package DBD::monetdb::db;

$DBD::monetdb::db::imp_data_size = 0;


sub ping {
    my ($dbh) = @_;

    my $statement = $dbh->{monetdb_language} eq 'sql' ? 'select 7' : 'io.print(7);';
    my $rv = $dbh->selectrow_array($statement) || 0;
    $dbh->set_err(undef, undef);
    $rv == 7 ? 1 : 0;
}


sub quote {
    my ($dbh, $value, $type) = @_;

    return $dbh->{monetdb_language} eq 'sql' ? 'NULL' : 'nil'
        unless defined $value;

    $value = Encode::encode_utf8($value);

    for ($value) {
      s/\\/\\\\/g;
      s/\n/\\n/g;
      s/"/\\"/g;
      s/'/''/g;
    }

    $type ||= DBI::SQL_VARCHAR();

    my $prefix = $DBD::monetdb::TypeInfo::prefixes{$type} || '';
    my $suffix = $DBD::monetdb::TypeInfo::suffixes{$type} || '';

    if ( $dbh->{monetdb_language} ne 'sql') {
        $prefix = q(") if $prefix eq q(');
        $suffix = q(") if $suffix eq q(');
    }
    return $prefix . $value . $suffix;
}


sub _count_param {
    my $statement = shift;
    my $num = 0;

    $statement =~ s{
        ' (?: \\. | [^\\']++ )*+ ' |
        " (?: \\. | [^\\"]++ )*+ '
    }{}gx;

    return $statement =~ tr/?/?/;
}


sub prepare {
    my ($dbh, $statement, $attr) = @_;

    my $cxn = $dbh->{monetdb_connection};
    my $hdl = eval { $cxn->new_handle };
    return $dbh->set_err(-1, $@) if $@;

    my ($outer, $sth) = DBI::_new_sth($dbh, { Statement => $statement });

    $sth->STORE('NUM_OF_PARAMS', _count_param($statement));

    $sth->{monetdb_hdl} = $hdl;
    $sth->{monetdb_params} = [];
    $sth->{monetdb_types} = [];
    $sth->{monetdb_rows} = -1;

    return $outer;
}


sub commit {
    my($dbh) = @_;

    if ($dbh->FETCH('AutoCommit')) {
        warn 'Commit ineffective while AutoCommit is on' if $dbh->FETCH('Warn');
        return 0;
    }
    if ($dbh->{monetdb_language} eq 'sql') {
        return $dbh->do('commit')
            && $dbh->do('start transaction');
    }
    else {
        return $dbh->do('commit();');
    }
}


sub rollback {
    my($dbh) = @_;

    if ($dbh->FETCH('AutoCommit')) {
        warn 'Rollback ineffective while AutoCommit is on' if $dbh->FETCH('Warn');
        return 0;
    }
    if ($dbh->{monetdb_language} eq 'sql') {
        return $dbh->do('rollback')
            && $dbh->do('start transaction');
    }
    else {
        return $dbh->do('abort();');
    }
}


*get_info = \&DBD::monetdb::GetInfo::get_info;


sub monetdb_catalog_info {
    my($dbh) = @_;
    my $sql = <<'SQL';
select cast( null as varchar( 128 ) ) as table_cat
     , cast( null as varchar( 128 ) ) as table_schem
     , cast( null as varchar( 128 ) ) as table_name
     , cast( null as varchar( 254 ) ) as table_type
     , cast( null as varchar( 254 ) ) as remarks
 where 0 = 1
 order by table_cat
SQL
    my $sth = $dbh->prepare($sql) or return;
    $sth->execute or return;
    return $sth;
}


sub monetdb_schema_info {
    my($dbh) = @_;
    my $sql = <<'SQL';
select cast( null as varchar( 128 ) ) as table_cat
     , "name"                         as table_schem
     , cast( null as varchar( 128 ) ) as table_name
     , cast( null as varchar( 254 ) ) as table_type
     , cast( null as varchar( 254 ) ) as remarks
  from sys."schemas"
 order by table_schem
SQL
    my $sth = $dbh->prepare($sql) or return;
    $sth->execute or return;
    return $sth;
}


my $ttp = {
 'TABLE'            => 't."type" = 0  and t."system" = false and t."temporary" = 0 and s.name <> \'tmp\''
,'GLOBAL TEMPORARY' => 't."type" = 0  and t."system" = false and t."temporary" = 0 and s.name = \'tmp\''
,'SYSTEM TABLE'     => 't."type" = 0  and t."system" = true  and t."temporary" = 0'
,'LOCAL TEMPORARY'  => 't."type" = 0  and t."system" = false and t."temporary" = 1'
,'VIEW'             => 't."type" = 1                                              '
};


sub monetdb_tabletype_info {
    my($dbh) = @_;
    my $sql = <<"SQL";
select distinct
       cast( null as varchar( 128 ) ) as table_cat
     , cast( null as varchar( 128 ) ) as table_schem
     , cast( null as varchar( 128 ) ) as table_name
     , case
         when $ttp->{'TABLE'           } then cast('TABLE'               as varchar( 254 ) )
         when $ttp->{'SYSTEM TABLE'    } then cast('SYSTEM TABLE'        as varchar( 254 ) )
         when $ttp->{'LOCAL TEMPORARY' } then cast('LOCAL TEMPORARY'     as varchar( 254 ) )
         when $ttp->{'GLOBAL TEMPORARY'} then cast('GLOBAL TEMPORARY'    as varchar( 254 ) )
         when $ttp->{'VIEW'            } then cast('VIEW'                as varchar( 254 ) )
         else                                 cast('INTERNAL TABLE TYPE' as varchar( 254 ) )
       end                            as table_type
     , cast( null as varchar( 254 ) ) as remarks
  from sys."tables" t, sys."schemas" s
  where t."schema_id" = s."id"
 order by table_type
SQL
    my $sth = $dbh->prepare($sql) or return;
    $sth->execute or return;
    return $sth;
}


sub monetdb_table_info {
    my($dbh, $c, $s, $t, $tt) = @_;
    my $sql = <<"SQL";
select cast( null as varchar( 128 ) ) as table_cat
     , s."name"                       as table_schem
     , t."name"                       as table_name
     , case
         when $ttp->{'TABLE'           } then cast('TABLE'               as varchar( 254 ) )
         when $ttp->{'SYSTEM TABLE'    } then cast('SYSTEM TABLE'        as varchar( 254 ) )
         when $ttp->{'LOCAL TEMPORARY' } then cast('LOCAL TEMPORARY'     as varchar( 254 ) )
         when $ttp->{'GLOBAL TEMPORARY'} then cast('GLOBAL TEMPORARY'    as varchar( 254 ) )
         when $ttp->{'VIEW'            } then cast('VIEW'                as varchar( 254 ) )
         else                                 cast('INTERNAL TABLE TYPE' as varchar( 254 ) )
       end                            as table_type
     , cast( null as varchar( 254 ) ) as remarks
  from sys."schemas" s
     , sys."tables"  t
 where t."schema_id" = s."id"
SQL
    my @bv = ();
    $sql .= qq(   and s."name"   like ?\n), push @bv, $s if $s;
    $sql .= qq(   and t."name"   like ?\n), push @bv, $t if $t;
    if ( @$tt ) {
        $sql .= "   and ( 1 = 0\n";
        for ( @$tt ) {
            my $p = $ttp->{uc $_};
            $sql .= "      or $p\n" if $p;
        }
        $sql .= "       )\n";
    }
    $sql .=   " order by table_type, table_schem, table_name\n";
    my $sth = $dbh->prepare($sql) or return;
    $sth->execute(@bv) or return;
    
    $dbh->set_err(0,"Catalog parameter c has to be an empty string, as MonetDB does not support multiple catalogs") if $c ne "";
    return $sth;
}


sub table_info {
    my($dbh, $c, $s, $t, $tt) = @_;
            
    if ( defined $c && defined $s && defined $t ) {
        if    ( $c eq '%' && $s eq ''  && $t eq '') {
            return monetdb_catalog_info($dbh);
        }
        elsif ( $c eq ''  && $s eq '%' && $t eq '') {
            return monetdb_schema_info($dbh);
        }
        elsif ( $c eq ''  && $s eq ''  && $t eq '' && defined $tt && $tt eq '%') {
            return monetdb_tabletype_info($dbh);
        }
    }
    my @tt;
    if ( defined $tt ) {
        @tt = split /,/, $tt;
        s/^\s*'?//, s/'?\s*$// for @tt;
    }
    return monetdb_table_info($dbh, $c, $s, $t, \@tt);
}


sub column_info {
    my($dbh, $catalog, $schema, $table, $column) = @_;
     # TODO: test $catalog for equality with empty string
    my $sql = <<'SQL';
select cast( null            as varchar( 128 ) ) as table_cat
     , s."name"                                  as table_schem
     , t."name"                                  as table_name
     , c."name"                                  as column_name
     , cast( 0               as smallint       ) as data_type          -- ...
     , c."type"                                  as type_name          -- TODO
     , cast( c."type_digits" as integer        ) as column_size        -- TODO
     , cast( null            as integer        ) as buffer_length      -- TODO
     , cast( c."type_scale"  as smallint       ) as decimal_digits     -- TODO
     , cast( null            as smallint       ) as num_prec_radix     -- TODO
     , case c."null"
         when false then cast( 0 as smallint )  -- SQL_NO_NULLS
         when true  then cast( 1 as smallint )  -- SQL_NULLABLE
       end                                       as nullable
     , cast( null            as varchar( 254 ) ) as remarks
     , c."default"                               as column_def
     , cast( 0               as smallint       ) as sql_data_type      -- ...
     , cast( null            as smallint       ) as sql_datetime_sub   -- ...
     , cast( null            as integer        ) as char_octet_length  -- TODO
     , cast( c."number" + 1  as integer        ) as ordinal_position
     , case c."null"
         when false then cast('NO'  as varchar( 254 ) )
         when true  then cast('YES' as varchar( 254 ) )
       end                                       as is_nullable
  from sys."schemas" s
     , sys."tables"  t
     , sys."columns" c
 where t."schema_id" = s."id"
   and c."table_id"  = t."id"
SQL
    my @bv = ();
    $sql .= qq(   and s."name"   like ?\n), push @bv, $schema if $schema;
    $sql .= qq(   and t."name"   like ?\n), push @bv, $table  if $table;
    $sql .= qq(   and c."name"   like ?\n), push @bv, $column if $column;
    $sql .=   " order by table_cat, table_schem, table_name, ordinal_position\n";
    my $sth = $dbh->prepare($sql) or return;
    $sth->execute(@bv) or return;
    $dbh->set_err(0,"Catalog parameter catalog has to be an empty string, as MonetDB does not support multiple catalogs") if $catalog ne "";
    my $rows;
    while ( my $row = $sth->fetch ) {
        $row->[ 4] = $DBD::monetdb::TypeInfo::typeinfo{$row->[5]}->[ 1];
        $row->[13] = $DBD::monetdb::TypeInfo::typeinfo{$row->[5]}->[15];
        $row->[14] = $DBD::monetdb::TypeInfo::typeinfo{$row->[5]}->[16];
        push @$rows, [ @$row ];
    }
    return DBI->connect('dbi:Sponge:','','', { RaiseError => 1 } )->prepare(
        $sth->{Statement},
        { rows => $rows, NAME => $sth->{NAME}, TYPE => $sth->{TYPE} }
    );
}


sub primary_key_info {
    my($dbh, $catalog, $schema, $table) = @_;
    # TODO: test $catalog for equality with empty string
    return $dbh->set_err(-1,'Undefined schema','HY009') unless defined $schema;
    return $dbh->set_err(-1,'Undefined table' ,'HY009') unless defined $table;
    my $sql = <<'SQL';
select cast( null        as varchar( 128 ) ) as table_cat
     , s."name"                              as table_schem
     , t."name"                              as table_name
     , c."name"                            as column_name
     , cast( c."nr" + 1  as smallint       ) as key_seq
     , k."name"                              as pk_name
  from sys."schemas"     s
     , sys."tables"      t
     , sys."keys"        k
     , sys."objects"  c
 where t."schema_id"   = s."id"
   and k."table_id"    = t."id"
   and c."id"          = k."id"
   and s."name"        = ?
   and t."name"        = ?
   and k."type"        = 0
 order by table_schem, table_name, key_seq
SQL
    my $sth = $dbh->prepare($sql) or return;
    $sth->execute($schema, $table) or return;
    $dbh->set_err(0,"Catalog parameter catalog has to be an empty string, as MonetDB does not support multiple catalogs") if $catalog ne "";
    return $sth;
}


sub foreign_key_info {
    my($dbh, $c1, $s1, $t1, $c2, $s2, $t2) = @_;
    my $sql = <<'SQL';
select cast( null         as varchar( 128 ) ) as uk_table_cat
     , uks."name"                             as uk_table_schem
     , ukt."name"                             as uk_table_name
     , ukc."name"                           as uk_column_name
     , cast( null         as varchar( 128 ) ) as fk_table_cat
     , fks."name"                             as fk_table_schem
     , fkt."name"                             as fk_table_name
     , fkc."name"                           as fk_column_name
     , cast( fkc."nr" + 1 as smallint       ) as ordinal_position
     , cast( 3            as smallint       ) as update_rule    -- SQL_NO_ACTION
     , cast( 3            as smallint       ) as delete_rule    -- SQL_NO_ACTION
     , fkk."name"                             as fk_name
     , ukk."name"                             as uk_name
     , cast( 7            as smallint       ) as deferability   -- SQL_NOT_DEFERRABLE
     , case  ukk."type"
         when 0 then cast('PRIMARY'   as varchar( 7 ) )
         when 1 then cast('UNIQUE'    as varchar( 7 ) )
         else        cast( ukk."type" as varchar( 7 ) )
       end                                    as unique_or_primary
  from sys."schemas"    uks
     , sys."tables"     ukt
     , sys."keys"       ukk
     , sys."objects" ukc
     , sys."schemas"    fks
     , sys."tables"     fkt
     , sys."keys"       fkk
     , sys."objects" fkc
 where ukt."schema_id"  = uks."id"
   and ukk."table_id"   = ukt."id"
   and ukc."id"         = ukk."id"
   and fkt."schema_id"  = fks."id"
   and fkk."table_id"   = fkt."id"
   and fkc."id"         = fkk."id"
-- and ukk."type"      IN ( 0, 1 )
-- and fkk."type"       = 2
-- and fkk."rkey"       > -1
   and fkk."rkey"       = ukk."id"
   and fkc."nr"         = ukc."nr"
SQL
    my @bv = ();
    $sql .= qq(   and uks."name"       = ?\n), push @bv, $s1 if $s1;
    $sql .= qq(   and ukt."name"       = ?\n), push @bv, $t1 if $t1;
    $sql .= qq(   and fks."name"       = ?\n), push @bv, $s2 if $s2;
    $sql .= qq(   and fkt."name"       = ?\n), push @bv, $t2 if $t2;
    $sql .= qq(   and ukk."type"       = 0\n)                if $t1 && !$t2;
    $sql .= " order by uk_table_schem, uk_table_name, fk_table_schem, fk_table_name, ordinal_position\n";
    my $sth = $dbh->prepare($sql) or return;
    $sth->execute(@bv) or return;
	$dbh->set_err(0,"Catalog parameters c1 and c2 have to be an empty strings, as MonetDB does not support multiple catalogs") if $c1 ne "" || $c2 ne "";
    return $sth;
}


*type_info_all = \&DBD::monetdb::TypeInfo::type_info_all;


sub tables {
    my ($dbh, @args) = @_;

    # TODO: !! warn: 0 CLEARED by call to fetchall_arrayref method
    return $dbh->SUPER::tables( @args ) if $dbh->{monetdb_language} eq 'sql';

    return eval{ @{$dbh->selectcol_arrayref('ls();')} };
}


sub disconnect {
    my ($dbh) = @_;

    delete $dbh->{monetdb_connection};
    $dbh->STORE('Active', 0 );
    return 1;
}


sub FETCH {
    my ($dbh, $key) = @_;

    return $dbh->{$key} if $key =~ /^monetdb_/;
    return $dbh->SUPER::FETCH($key);
}


sub STORE {
    my ($dbh, $key, $value) = @_;

    if ($key eq 'AutoCommit') {
        return 1 if $dbh->{monetdb_language} ne 'sql';
        my $old_value = $dbh->{$key};
        if ($value && defined $old_value && !$old_value) {
            $dbh->do('commit')
                or return $dbh->set_err($dbh->err, $dbh->errstr);
        }
        elsif (!$value && (!defined $old_value || $old_value)) {
            $dbh->do('start transaction')
                or return $dbh->set_err($dbh->err, $dbh->errstr);
        }
        $dbh->{$key} = $value;
        return 1;
    }
    elsif ($key =~ /^monetdb_/) {
        $dbh->{$key} = $value;
        return 1;
    }
    return $dbh->SUPER::STORE($key, $value);
}


sub DESTROY {
    my ($dbh) = @_;

    $dbh->disconnect if $dbh->FETCH('Active');
}



package DBD::monetdb::st;

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

    $sth->STORE('Active', 0 );  # we don't need to call $sth->finish because
                                # mapi_query_handle() calls finish_handle()

    $sth->bind_param($_, $bind_values[$_-1]) or return for 1 .. @bind_values;

    my $params = $sth->{monetdb_params};
    my $num_of_params = $sth->FETCH('NUM_OF_PARAMS');
    return $sth->set_err(-1, @$params ." values bound when $num_of_params expected")
        unless @$params == $num_of_params;

    for ( 1 .. $num_of_params ) {
        my $quoted_param = $dbh->quote($params->[$_-1], $sth->{monetdb_types}[$_-1]);
        $statement =~ s/\?/$quoted_param/;  # TODO: '?' inside quotes/comments
    }
    $sth->trace_msg("    -- Statement: $statement\n", 5);

    my $hdl = $sth->{monetdb_hdl};
    eval{ $hdl->query($statement) };
    return $sth->set_err(-1, $@) if $@;

    my $rows = $hdl->rows_affected;

    if ( $dbh->{monetdb_language} eq 'sql' && $hdl->querytype != 1 ) {
        $sth->{monetdb_rows} = $rows;
        return $rows || '0E0';
    }
    my ( @names, @types, @precisions, @nullables );
    my $field_count = $hdl->columncount;
    for ( 0 .. $field_count-1 ) {
        push @names     , $hdl->name  ($_);
        push @types     , $hdl->type  ($_);
        push @precisions, $hdl->length($_);
        push @nullables , 2;  # TODO
    }
    $sth->STORE('NUM_OF_FIELDS', $field_count) unless $sth->FETCH('NUM_OF_FIELDS');
    $sth->{NAME}      = \@names;
    $sth->{TYPE}      = [ map { $DBD::monetdb::TypeInfo::typeinfo{$_}->[1] } @types ];
    $sth->{PRECISION} = \@precisions;  # TODO
    $sth->{SCALE}     = [];
    $sth->{NULLABLE}  = \@nullables;
    $sth->STORE('Active', 1 );

    $sth->{monetdb_rows} = 0;

    return $rows || '0E0';
}


sub fetch {
    my ($sth) = @_;

    return $sth->set_err(-900,'Statement handle not marked as Active')
        unless $sth->FETCH('Active');
    my $hdl = $sth->{monetdb_hdl};
    my $field_count = eval{ $hdl->fetch };
    unless ( $field_count ) {
        $sth->STORE('Active', 0 );
        $sth->set_err(-1, $@) if $@;
        return;
    }
    my @row = map $hdl->{currow}[$_], 0 .. $field_count-1; # encapsulation break but saves a microsecond per cell
    map { s/\s+$// } @row if $sth->FETCH('ChopBlanks');

    $sth->{monetdb_rows}++;
    return $sth->_set_fbav(\@row);
}

*fetchrow_arrayref = \&fetch;


sub rows {
    my ($sth) = @_;

    return $sth->{monetdb_rows};
}


sub finish {
    my ($sth) = @_;
    my $hdl = $sth->{monetdb_hdl};

    eval{ $hdl->finish };
    return $sth->set_err(-1, $@) if $@;

    return $sth->SUPER::finish;  # sets Active off
}


sub FETCH {
    my ($sth, $key) = @_;

    if ( $key =~ /^monetdb_/) {
        return $sth->{$key};
    }
    elsif ( $key eq 'ParamValues') {
        my $p = $sth->{monetdb_params};
        return { map { $_ => $p->[$_-1] } 1 .. $sth->FETCH('NUM_OF_PARAMS') };
    }
    return $sth->SUPER::FETCH($key);
}


sub STORE {
    my ($sth, $key, $value) = @_;

    if ($key =~ /^monetdb_/) {
        $sth->{$key} = $value;
        return 1;
    }
    return $sth->SUPER::STORE($key, $value);
}


sub DESTROY {
    my ($sth) = @_;

    $sth->STORE('Active', 0 );
}


1;

__END__

=head1 NAME

DBD::monetdb - MonetDB Driver for DBI

=head1 SYNOPSIS

  use DBI();

  my $dbh = DBI->connect('dbi:monetdb:');

  my $sth = $dbh->prepare('SELECT * FROM env() env');
  $sth->execute;
  $sth->dump_results;

=head1 DESCRIPTION

DBD::monetdb is a Pure Perl client interface for the MonetDB Database Server.
It requires MonetDB::CLI (and one of its implementations).

=head2 Outline Usage

From perl you activate the interface with the statement

  use DBI;

After that you can connect to multiple MonetDB database servers
and send multiple queries to any of them via a simple object oriented
interface. Two types of objects are available: database handles and
statement handles. Perl returns a database handle to the connect
method like so:

  $dbh = DBI->connect("dbi:monetdb:host=$host",
    $user, $password, { RaiseError => 1 } );

Once you have connected to a database, you can can execute SQL
statements with:

  my $sql = sprintf('INSERT INTO foo VALUES (%d, %s)',
    $number, $dbh->quote('name'));
  $dbh->do($sql);

See L<DBI> for details on the quote and do methods. An alternative
approach is

  $dbh->do('INSERT INTO foo VALUES (?, ?)', undef, $number, $name);

in which case the quote method is executed automatically. See also
the bind_param method in L<DBI>.

If you want to retrieve results, you need to create a so-called
statement handle with:

  $sth = $dbh->prepare("SELECT id, name FROM $table");
  $sth->execute;

This statement handle can be used for multiple things. First of all
you can retreive a row of data:

  my $row = $sth->fetch;

If your table has columns ID and NAME, then $row will be array ref with
index 0 and 1.

=head2 Example

  #!/usr/bin/perl

  use strict;
  use DBI;

  # Connect to the database.
  my $dbh = DBI->connect('dbi:monetdb:host=localhost',
    'joe', "joe's password", { RaiseError => 1 } );

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
  $sth->execute;
  while ( my $row = $sth->fetch ) {
    print "Found a row: id = $row->[0], name = $row->[1]\n";
  }

  # Disconnect from the database.
  $dbh->disconnect;

=head1 METHODS

=head2 Driver Handle Methods

=over

=item B<connect>

  use DBI();

  $dsn = 'dbi:monetdb:';
  $dsn = "dbi:monetdb:host=$host";
  $dsn = "dbi:monetdb:host=$host;port=$port";
  $dsn = "dbi:monetdb:host=$host;database=$database";

  $dbh = DBI->connect($dsn, $user, $password);

=over

=item host

The default host to connect to is 'localhost', i.e. your workstation.

=item port

The port the MonetDB daemon listens to. Default for MonetDB is 50000.

=item database

The name of the database to connect to.

=back

=back

=head2 Database Handle Methods

The following methods are currently not supported:

  last_insert_id

All MetaData methods are supported. However, column_info() currently doesn't
provide length (size, ...) related information.
The foreign_key_info() method returns a SQL/CLI like result set,
because it provides additional information about unique keys.

=head2 Statement Handle Methods

The following methods are currently not supported:

  bind_param_inout
  more_results
  blob_read

=head1 ATTRIBUTES

The following attributes are currently not supported:

  LongReadLen
  LongTruncOk

=head2 Database Handle Attributes

The following attributes are currently not supported:

  RowCacheSize

=head2 Statement Handle Attributes

The following attributes are currently not (or not correctly) supported:

  PRECISION  (MonetDB semantic != DBI semantic)
  SCALE      (empty)
  NULLABLE   (SQL_NULLABLE_UNKNOWN = 2)
  CursorName
  RowsInCache

=head1 AUTHORS

Martin Kersten E<lt>Martin.Kersten@cwi.nlE<gt> implemented the initial Mapi
based version of the driver (F<monet.pm>).
Arjan Scherpenisse E<lt>acscherp@science.uva.nlE<gt> renamed this module to
F<monetdbPP.pm> and derived the new MapiLib based version (F<monetdb.pm>).
Current maintainer is Steffen Goeldner E<lt>sgoeldner@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


Contributor(s): Steffen Goeldner.

=head1 SEE ALSO

=head2 MonetDB

  Homepage    : http://www.monetdb.org/

=head2 Perl modules

L<DBI>, L<MonetDB::CLI>

=cut
