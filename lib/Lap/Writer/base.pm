package Lap::Writer::base;
use strict;
use warnings;
use Carp;
use 5.010;
use IO::Socket;
use JSON::XS qw(encode_json decode_json);
use Anet::CORE::IO qw(fhbuffermode);
use bytes;
use Time::HiRes qw(sleep time);
use encoding::warnings 'FATAL';

use enum::fields qw(
	FH
	NETLOC
	SLEEP
);

my $getload = do {
	if (eval "use Sys::CpuLoad ();1") {
		\&Sys::CpuLoad::load
	}
	elsif (eval "use Sys::Load ();1") {
		\&Sys::Load::getload
	}
	else {
		carp "load average will not be reported: cannot load neither Sys::CpuLoad nor Sys::Load in Lap::Writer::base";
		undef
	}
};

sub new {
	my $class = shift;
	my %args = (
		netloc => undef,
		timeout => 10, # seconds, 0=infinity
		sleep => undef, # seconds
		@_
	);

	my $netloc = delete($args{netloc}) || croak "missing argument 'netloc'";
	my $timeout = delete($args{timeout});
	my $sleep = delete($args{sleep});

	%args && croak "bad arguments: '", join("', '", sort keys %args), "'";

	my $self = bless [], $class;

	$self->[NETLOC] = $netloc;
	$self->[FH] = $self->_handshake($netloc, $timeout);
	$self->[SLEEP] = $sleep;

	$self
}

sub _handshake {
	my ($self, $netloc, $timeout) = @_;

	local $SIG{ALRM} = sub {die "timeout\n"};

	alarm 10 if $timeout;
	my $fh;
	eval {
		$fh = IO::Socket::INET->new($netloc) || die "could not connect $netloc\n";
		$fh->autoflush;
		fhbuffermode($fh, -1); # tcp_nodelay
		print $fh "LAP-WRITER";
		read($fh, my $buf='', 10);
		die "wrong LAP handshake with $netloc\n" unless $buf eq 'LAP-MASTER';

		say $fh encode_json({
			os => undef,
			writer_agent => ref($self),
			$self->info,
		});

		my $info = decode_json scalar <$fh>;
		die $info->{status} unless $info->{ok};
		$self->on_handshake($info);
		say "LAP version: $info->{version}";
	};
	alarm 0 if $timeout;
	if ($@) {
		my $msg = $@;
		chomp $msg;
		croak $msg;
	}

	$fh
}

sub loop {
	my $self = shift;

	my $fh = $self->[FH];
	local $/ = "\n";

	my $count = 0;
	my $size = 0;
	my $start_time = CORE::time * 1000;

	while (my $json = <$fh>) {
		my ($type, $hash) = @{ decode_json($json) };
		if ($type eq 'report') {
			# say "\nREPORT BACK!";
			say $fh encode_json [report => {
				received_count => $count,
				received_size => $size,
				start_time => $start_time,
				load_average => $getload && join(' ', $getload->()),
				used_memory => undef,
				max_memory => undef,
				available_storage => undef,
				$self->report,
			}];
			next;
		}
		my $meta = $hash;
		my $to_read = delete $meta->{to_read};
		my $id = delete $meta->{id};

		++$count;
		
		my $data_cb;
		if (defined $to_read) {
			$size += $to_read;
			$data_cb = sub {
				return unless defined $to_read;
				my $buf='';
				if ($to_read) {
					my $chunk_size = shift;
					$chunk_size = $to_read if !$chunk_size or $chunk_size > $to_read;
					my $read = read($fh, $buf, $chunk_size);
					die "bad size" if $read != $chunk_size;
					$to_read -= $read;
					$to_read ||= undef;
				}
				\$buf
			};
		}

		my $cb = sub {
			say $fh encode_json [done => $id];
		};

		$self->on_token($meta, $data_cb, $cb);
		if ($data_cb && $to_read) {
			# empty
			1 while $data_cb->(65536)
		}

		sleep $self->[SLEEP] if $self->[SLEEP];
	}
}

sub info {}
sub report {}
sub on_handshake {}

1