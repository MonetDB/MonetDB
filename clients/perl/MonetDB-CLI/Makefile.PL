use ExtUtils::MakeMaker;

WriteMakefile
(
  NAME          => 'MonetDB::CLI'
, VERSION_FROM  => 'CLI.pm'
, ABSTRACT_FROM => 'CLI.pm'
, PREREQ_PM     => { 'MonetDB::CLI::MapiPP' => 0.01 }
);
