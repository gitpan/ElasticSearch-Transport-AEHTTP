#!perl

use Test::More;
use strict;
use warnings;
our $es;
my ( $r1, $r2 );

ok $es->transport->deflate(1), 'Set deflate on';
isa_ok $r1 = $es->cluster_health, 'HASH', ' - hash received';

ok !$es->transport->deflate(0), 'Set deflate off';
isa_ok $r2 = $es->cluster_health, 'HASH', ' - hash received';

is_deeply $r1, $r2, ' - hashes match';

1
