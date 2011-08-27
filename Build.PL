
use strict;
use warnings;

use Module::Build 0.3601;


my %module_build_args = (
  "build_requires" => {
    "Module::Build" => "0.3601",
    "Test::More" => 0
  },
  "configure_requires" => {
    "Module::Build" => "0.3601"
  },
  "dist_abstract" => "AnyEvent::HTTP backend for ElasticSearch",
  "dist_author" => [
    "Clinton Gormley <drtech\@cpan.org>"
  ],
  "dist_name" => "ElasticSearch-Transport-AEHTTP",
  "dist_version" => "0.02",
  "license" => "perl",
  "module_name" => "ElasticSearch::Transport::AEHTTP",
  "recommends" => {},
  "recursive_test_files" => 1,
  "requires" => {
    "AnyEvent" => 0,
    "AnyEvent::HTTP" => 0,
    "ElasticSearch" => "0.44",
    "ElasticSearch::Transport" => 0,
    "ElasticSearch::Util" => 0,
    "Encode" => 0,
    "Scalar::Util" => 0,
    "parent" => 0,
    "strict" => 0,
    "warnings" => 0
  },
  "script_files" => []
);


my $build = Module::Build->new(%module_build_args);

$build->create_build_script;
