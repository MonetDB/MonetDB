use ExtUtils::MakeMaker;

WriteMakefile
(
  NAME          => 'MonetDB::CLI::MapiLib'
, VERSION_FROM  => 'MapiLib.pm'
, ABSTRACT_FROM => 'MapiLib.pm'
, PREREQ_PM     => { MapiLib => 0 }
);
