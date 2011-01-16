use ExtUtils::MakeMaker;

WriteMakefile
(
  NAME          => 'DBD::monetdb'
, VERSION_FROM  => 'monetdb.pm'
, ABSTRACT_FROM => 'monetdb.pm'
, PREREQ_PM     => { DBI => 1.45, 'MonetDB::CLI' => 0.01 }
);
