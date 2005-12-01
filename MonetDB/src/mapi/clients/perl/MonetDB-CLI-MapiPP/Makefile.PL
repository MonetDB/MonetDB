use ExtUtils::MakeMaker;

WriteMakefile
(
  NAME          => 'MonetDB::CLI::MapiPP'
, VERSION_FROM  => 'MapiPP.pm'
, ABSTRACT_FROM => 'MapiPP.pm'
, PREREQ_PM     => { 'IO::Socket::INET' => 0 }
);
