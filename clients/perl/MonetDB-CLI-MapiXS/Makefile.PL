use ExtUtils::MakeMaker;

WriteMakefile
(
  NAME          => 'MonetDB::CLI::MapiXS'
, VERSION_FROM  => 'MapiXS.pm'
, ABSTRACT_FROM => 'MapiXS.pm'
, LIBS          => ['-lMapi']
);
