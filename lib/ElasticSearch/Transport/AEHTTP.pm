package ElasticSearch::Transport::AEHTTP;

use strict;
use warnings;

use ElasticSearch 0.44 ();
use parent 'ElasticSearch::Transport';
use AnyEvent::HTTP qw(http_request);
use Encode qw(decode_utf8 encode_utf8);
use ElasticSearch::Util qw(build_error);
use Scalar::Util qw(weaken isweak);

our $VERSION = '0.02';

#===================================
sub protocol     {'http'}
sub default_port {9200}
#===================================

#===================================
sub request         { shift->_cv_wrap( '_request',         @_[ 0 .. 1 ] ) }
sub send_request    { shift->_cv_wrap( '_send_request',    @_ ) }
sub current_server  { shift->_cv_wrap( '_current_server',  @_ ) }
sub next_server     { shift->_cv_wrap( '_next_server',     @_ ) }
sub refresh_servers { shift->_cv_wrap( '_refresh_servers', @_ ) }
#===================================

#===================================
sub _request {
#===================================
    my $self   = shift;
    my $params = shift;
    my $s_srvr = shift;

    my $weak_cb = my $cb = shift;
    weaken $weak_cb;

    my $args = $self->_tidy_params($params);
    $self->reset_failed_servers();

    my $weak_req;
    my $request_cb = $weak_req = sub {
        my $srvr = shift
            or return $weak_cb->( undef, @_ );

        $self->log_request( $srvr, $args ) unless $s_srvr;

        $self->_send_request(
            $srvr, $args,
            sub {
                my $json = shift || '{"ok": true}';
                if ( my $error = shift ) {
                    if ( !$s_srvr && $self->_should_retry( $srvr, $error ) ) {
                        my @guard;
                        my $next_cb = sub { $weak_req->(@_); @guard = () };
                        return @guard = $self->_next_server($next_cb);
                    }

                    $error = $self->_handle_error( $srvr, $params, $error );
                    return $weak_cb->( undef, $error );
                }
                local $@;
                return $weak_cb->(
                    eval { $self->_response( $json, $params, $s_srvr ) }
                        || undef,
                    $@
                );
            }
        );
    };

    weaken $weak_req;
    return ( $cb, $request_cb, $s_srvr
        ? $request_cb->($s_srvr)
        : $self->_next_server($request_cb) );

}

#===================================
sub _send_request {
#===================================
    my $self   = shift;
    my $server = shift;
    my $params = shift;
    my $cb     = shift;

    my $method = $params->{method};
    my $uri = $self->http_uri( $server, $params->{cmd}, $params->{qs} );

    my $data = $params->{data};
    $data = encode_utf8($data) if defined $data;

    my $request_cb = sub {
        my ( $content, $hdr ) = @_;

        $content = '' unless defined $content;
        $content = $self->inflate($content)
            if ( $hdr->{'content-encoding'} || '' ) eq 'deflate';

        $content = decode_utf8($content);
        my $code = $hdr->{Status};

        if ( $code =~ /^2/ ) {
            return $cb->($content);
        }
        my $msg = $hdr->{Reason};

        my $type
            = $code eq '409'                  ? 'Conflict'
            : $code eq '404'                  ? 'Missing'
            : $msg  eq 'Connection timed out' ? 'Timeout'
            : $msg =~ /Broken pipe|Connection (reset by peer|refused)/
            ? 'Connection'
            : 'Request';

        my $error_params = {
            server      => $server,
            status_code => $code,
            status_msg  => $msg,
        };

        $error_params->{content} = $content
            unless $type eq 'Timeout';

        my $error = build_error( $self, $type, $msg, $error_params );
        $cb->( undef, $error );
    };

    my $headers;
    $headers = { 'Accept-Encoding' => 'deflate' }
        if $self->deflate;

    return http_request(
        $method    => $uri,
        body       => $data,
        timeout    => $self->timeout,
        persistent => 0,
        headers    => $headers,
        $request_cb
    );
}

#===================================
sub _refresh_servers {
#===================================
    my $self = shift;
    my $cb   = shift;

    my $queue = $self->{_queue} ||= [];
    push @$queue, $cb;
    weaken $queue->[-1];

    my $requests = $self->{_requests} ||= [];
    weaken $self->{_requests} unless isweak $self->{_requests};
    @$requests = () unless grep $_, @$requests;
    return ( @$requests, $cb ) if ( grep $_, @$queue ) > 1;

    $self->{_refresh_in} = 0;
    delete $self->{_current_server};

    my %servers = map { $_ => 1 }
        ( @{ $self->servers }, @{ $self->default_servers } );

    my @all_servers = keys %servers;
    my $protocol    = $self->protocol;

    my $count         = @all_servers;
    my $process_queue = sub {
        my $ok = shift;
        my $error;
        if ( !$ok ) {
            return if --$count;
            $error = build_error(
                $self, 'NoServers',
                "Could not retrieve a list of active servers:\n$_[0]",
                { servers => \@all_servers }
            );
        }
        @$requests = ();
        while (@$queue) {
            my $cb = shift @$queue or next;
            $cb->( $ok, $error );
        }
    };

    my $request_cb = sub {
        my $nodes = shift() || return $process_queue->( undef, @_ );
        my @servers = grep {$_}
            map {m{/([^]]+)}}
            map {
                   $_->{ $protocol . '_address' }
                || $_->{ $protocol . 'Address' }
                || ''
            } values %{ $nodes->{nodes} };

        return $process_queue->(
            undef,
            build_error(
                $self, 'Internal',
                "ElasticSearch returned no live servers"
            )
        ) unless @servers;

        $self->servers( \@servers );
        $self->{_refresh_in} = $self->max_requests - 1;
        %servers = ();
        return $process_queue->(1);
    };

    foreach my $server (@all_servers) {
        next unless $server;
        push @$requests,
            $self->_request( { cmd => '/_cluster/nodes' },
            $server, $request_cb );
    }

    my @requests = @$requests;
    weaken $requests->[$_] for 0 .. $#requests;

    return ( @$requests, $cb );
}

#===================================
sub _current_server {
#===================================
    my $self = shift;
    my $cb   = shift;
    return $cb->( $self->{_current_server}{$$} )
        if $self->{_current_server}{$$};

    return $self->_next_server($cb);
}

#===================================
sub _next_server {
#===================================
    my $self = shift;
    my $cb   = shift;
    local $@;

    return $cb->( eval { $self->SUPER::next_server() } || undef, $@ )
        if $self->{_refresh_in} || $self->no_refresh;

    return $self->_refresh_servers(
        sub {
            return $cb->(@_) if $_[1];
            $self->{_refresh_in}++;
            $cb->( eval { $self->SUPER::next_server() } || undef, $@ );
            $self->{_refresh_in}--;
        }
    );
}

#===================================
sub cv { shift; ElasticSearch::Transport::AnyEvent::CondVar->new(@_) }
#===================================

#===================================
sub _cv_wrap {
#===================================
    my $self    = shift;
    my $method  = shift;
    my $cv_weak = my $cv = $self->cv;
    weaken $cv_weak;

    $cv->guard(
        $self->$method(
            @_,
            sub {
                my $content = shift;
                $_[0]
                    ? $cv_weak->croak( shift() )
                    : $cv_weak->send($content);
                $cv_weak->clear_guard() if $cv_weak;
            }
        )
    );

    $cv->guard($cv) unless defined wantarray;
    return $cv;
}

#===================================
sub _make_sync {
#===================================
    my $class = shift;
    my $old   = \&_cv_wrap;
    no warnings 'redefine';
    *_cv_wrap   = sub { $old->(@_)->recv };
    *_make_sync = sub { };
}

#===================================
#===================================
package ElasticSearch::Transport::AnyEvent::CondVar;
#===================================
#===================================

use AnyEvent();
our @ISA = qw(AnyEvent::CondVar);

my $created = my $destroyed = 0;
our %CV;

#===================================
sub new {
#===================================
    my $proto = shift;
    my $class = ref $proto || $proto;
    my $self  = AnyEvent->condvar;
    bless $self, $class;
    $self->{_guard} = [];

    if ($ElasticSearch::DEBUG) {
        $created++;
        $self->{_line}
            = join( ' ', $created, ( caller(3) )[3], ( caller(2) )[2] );
        print $self->{_line} . "\n";
        $CV{ $self->{_line} } = $self;
        Scalar::Util::weaken $CV{ $self->{_line} };
    }
    return $self;
}

#===================================
sub guard {
#===================================
    my $self = shift;
    push @{ $self->{_guard} }, @_;
}

#===================================
sub clear_guard { shift->{_guard} = []; }
#===================================

#===================================
sub cb {
#===================================
    my $self = shift;
    return $self->SUPER::cb unless @_;

    my $cb = shift;
    my $cv = $self;

    $self->SUPER::cb(
        sub {
            local $@ = $cv->{_ae_croak};
            $cb->( @{ $cv->{_ae_sent} } );
        }
    );
}

#===================================
sub recv {
#===================================
    $_[0]->_wait;
    die $_[0]{_ae_croak} if $_[0]{_ae_croak};
    wantarray ? @{ $_[0]{_ae_sent} } : $_[0]{_ae_sent}[0];
}

#===================================
sub DESTROY {
#===================================
    my $self = shift;
    if ( $ElasticSearch::DEBUG && $self->{_line} ) {
        print "Destroyed: " . $self->{_line} . "\n";
        delete $CV{ $self->{_line} };
        $destroyed++;
    }
    $self->clear_guard;
}

#===================================
sub stats {
#===================================
    print "Created: $created\nDestroyed: $destroyed\n";
}
1;

# ABSTRACT: AnyEvent::HTTP backend for ElasticSearch


__END__
=pod

=head1 NAME

ElasticSearch::Transport::AEHTTP - AnyEvent::HTTP backend for ElasticSearch

=head1 VERSION

version 0.02

=head1 SYNOPSIS

    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9200',
        transport   => 'aehttp',
    );

    # blocking request
    $e->cluster_health->recv;

    # non-blocking request
    $e->cluster_health->cb( sub {
        if ($@) {
            log "An error occurred: $@";
        } else {
            my $response = shift;
            log $response;
        }
    });
    AE::cv->recv();

    # fire-and-forget vs scoped
    {
        $e->delete_index(index => 'foo');
        $e->delete_index(index => 'bar')->cb( sub { print "Done"});
        my $cv = $e->delete_index(index=>'baz');
    }
    AE::cv->recv;
    #   - foo and bar will be deleted
    #   - baz will not be deleted

=head1 DESCRIPTION

ElasticSearch::Transport::AEHTTP uses L<AnyEvent::HTTP> to talk to
L<ElasticSearch> asynchronously over HTTP.

=head1 USING AEHTTP

Any request to ElasticSearch returns an L<AnyEvent::CondVar>. You have
three options for how you use them:

=head2 Blocking

    $cv = $e->cluster_health;
    $result = $cv->recv;

When you call C<recv()> on a CondVar, your program will block until
that CondVar is ready to return a value.

If an error was thrown, then C<recv()> will C<die>.  You will need to wrap
C<recv()> in C<eval> if you don't want to die.

If your C<$cv> goes out of scope, then the request will be aborted.

=head2 Callback

    $e->cluster_health->cb( sub {
        if ($@) {
            log "Error $@";
        }
        else {
            my $result = shift;
            log "$result"
        }
    })
    AE::cv->recv()

If you set a callback on a CondVar, then the callback will be called once
the CondVar is ready (which will only happen after you start the event loop).

In the callback, C<$@> will contain any error, otherwise the result (if any)
will be the first value in C<@_>.

Once you set a callback on a CondVar, it will not be aborted when it goes
out of scope.

=head2 Fire-and-Forget

    $e->delete_index(index=>'foo');

If a request is called in C<void> context, then it will be executed once
the event loop is started. No errors will be thrown, even if the request
does not complete succesfully.

It will not be aborted with a change in scope, because there is no scope. If
you exit the application without running an event loop, then any pending
requests will not be run.

=head1 BLOCKING METHODS

L<ElasticSearch/"scrolled_search()"> and L<ElasticSearch/"reindex()"> will
be executed synchronously.

=head1 SEE ALSO

=over

=item * L<ElasticSearch>

=item * L<ElasticSearch::Transport>

=item * L<ElasticSearch::Transport::HTTPLite>

=item * L<ElasticSearch::Transport::HTTPTiny>

=item * L<ElasticSearch::Transport::Curl>

=item * L<ElasticSearch::Transport::AEHTTP>

=item * L<ElasticSearch::Transport::AECurl>

=item * L<ElasticSearch::Transport::Thrift>

=back

1;

=head1 AUTHOR

Clinton Gormley <drtech@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Clinton Gormley.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

