#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

my ($node_id) = keys %{ $es->nodes->{nodes} };
ok $es->camel_case(1), 'Camel case on';
ok $es->nodes->{nodes}{$node_id}{transportAddress}, ' - got camel case';
ok $es->camel_case(0) == 0, ' - camel case off';
ok $es->nodes->{nodes}{$node_id}{transport_address}, ' - got underscores';

# error_trace
ok $es->error_trace(1), 'Error trace on';
eval {
    $es->transport->request(
        { cmd => '/_search', data => \'foo', qs => { error_trace => 1 } } );
};
my $e = $@;
isa_ok $e, 'ElasticSearch::Error::Request';
ok $e->{-vars}{error_trace}, ' - has error_trace';
ok $es->error_trace(0) == 0, ' error_trace off';

1
