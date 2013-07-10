package Vortex::Harbor::Lap;
use strict;
use warnings;
use Carp;
use Anet;
use Anet::Stream::TCP;
use Anet::Watcher::FIFO;
# use JSON::XS ();
use Digest::SHA qw(sha256_hex);
use bytes;
use List::Util qw(sum);
use Digest;
use Anet::CORE::Util qw(size_as_string);
use encoding::warnings 'FATAL';
use Compress::LZ4 qw(compress decompress);
use Scalar::Util qw(refaddr);
use File::Path qw(mkpath rmtree);
use Time::HiRes qw(time);

use base 'Vortex::Harbor::base';

use enum::fields::extending 'Vortex::Harbor::base', qw(
	PARAMS
	WRITER_LISTENER
	QUEUE
	LIMBO
	DIGEST
	BLOOM
	FREE_WRITERS
	WRITERS
	AUTOINCREMENT
	HEARTBEAT
	CONTENT_MAX_MEMORY
	CONTENT_MAX_ABSOLUTE
	CONTENT_DIR

	FIFO_REQS_SHORT
	FIFO_REQS_LONG
	FIFO_DATA_SIZE_SHORT
	FIFO_DATA_SIZE_LONG
	FIFO_TCP_SIZE_SHORT
	FIFO_TCP_SIZE_LONG
	FIFO_RAM_SHORT
	FIFO_DISK_SHORT
	FIFO_RAM_LONG
	FIFO_DISK_LONG
);

use constant SHORT_FIFO_LENGTH => 120;
use constant LONG_FIFO_LENGTH => 120;
use constant SHORT_FIFO_TEMPO => 1;
use constant LONG_FIFO_TEMPO => 60;

use constant LAP_VERSION => 0.3;
use constant COMPRESS => 0;

use constant DEBUG_CHECK_DIGEST => 0;

sub _short_fifo { shift; Anet::Watcher::FIFO->new(refresh => SHORT_FIFO_TEMPO, window => SHORT_FIFO_LENGTH*SHORT_FIFO_TEMPO, @_) }
sub _long_fifo  { shift; Anet::Watcher::FIFO->new(refresh => LONG_FIFO_TEMPO,  window => LONG_FIFO_LENGTH*LONG_FIFO_TEMPO  , @_) }

sub new {
	my $class = shift;
	my %args = (
		writer_port	=> undef,
		bloom_netloc => undef,
		digest => undef,
		content_max_memory => undef,
		content_max_absolute => undef,
		content_dir => undef,
		@_
	);
	
	my %params = %args;
	my $writer_port = delete($args{writer_port}) || croak "missing argument 'writer_port'";
	my $bloom_netloc = delete($args{bloom_netloc});
	my $digest = delete($args{digest});
	if ($bloom_netloc && !$digest) {
		$digest = 'SHA-256';
		print STDERR "[no digest specified for bloomfilter, default to $digest] ";
	}
	my $content_max_memory = eval(delete($args{content_max_memory}) || croak "missing argument 'content_max_memory'") || croak "failed to parse 'content_max_memory' value";
	my $content_max_absolute = delete($args{content_max_absolute});
	my $content_dir = delete($args{content_dir}) || croak "missing argument 'content_dir'";

	if ($digest) {
		eval {Digest->new($digest)} || croak "unknown digest algorithm '$digest'";
	}

	my $self = $class->SUPER::new(%args);

	delete @params{keys %args};
	$self->[PARAMS] = {
		%params,
		start_date => int(1000*time),
		port => $self->vortex->port,
		ip => $self->vortex->ip,
		host => $self->vortex->host,
		proxy => $ENV{HTTP_PROXY},
		harbor_tld => $self->vortex->harbor_tld,
	};

	$self->[CONTENT_MAX_MEMORY] = $content_max_memory;
	$self->[CONTENT_MAX_ABSOLUTE] = $content_max_absolute;
	$self->[CONTENT_DIR] = $content_dir;
	$self->[CONTENT_DIR] =~ s!/$!!;

	if (-d $self->[CONTENT_DIR]) {
		if (my @files = glob("$self->[CONTENT_DIR]/*")) {
			printf STDERR "\n'$self->[CONTENT_DIR]' content directory is not empty: %d file%s found (%s)\nErase (Yes/No) ? ",
				0+@files,
				(@files>1 ? 's' : ''),
				size_as_string(sum map {-s $_} @files),
			;
			local $SIG{INT};
			if (<> =~ /^y(?:es)?$/i) {
				rmtree($self->[CONTENT_DIR]) && mkpath($self->[CONTENT_DIR]) || croak "cannot recreate content directory '$self->[CONTENT_DIR]': $!"
			} else {
				croak "content directory must be empty\n"
			}
		}
	}
	elsif (!mkpath $self->[CONTENT_DIR]) {
		croak "cannot create content directory '$self->[CONTENT_DIR]': $!"
	}

	$self->[WRITER_LISTENER] = Anet::Stream::TCP->new(
		autoflush	=> 2,
		on_error	=> closure { die "@_\n" },
		priority	=> 2,
	);
	$self->[WRITER_LISTENER]->listen(
		undef,
		$writer_port,
		cb(writer_connection => $self)->weaken,
	);

	if ($bloom_netloc) {
		require Anet::BloomFilter::AtomicClient;
		$self->[BLOOM] = Anet::BloomFilter::AtomicClient->new(
			netloc => $bloom_netloc,
			autoflush => 2,
			priority => 2,
		) or croak "failed to connect bloom server at '$bloom_netloc'";
	}

	$self->[LIMBO] = {};
	$self->[QUEUE] = [];
	$self->[FREE_WRITERS] = [];
	$self->[WRITERS] = {};
	$self->[AUTOINCREMENT] = 0;

	for (FIFO_REQS_SHORT, FIFO_DATA_SIZE_SHORT, FIFO_TCP_SIZE_SHORT) {
		$self->[$_] = $self->_short_fifo
	}

	for (FIFO_REQS_LONG, FIFO_DATA_SIZE_LONG, FIFO_TCP_SIZE_LONG) {
		$self->[$_] = $self->_long_fifo
	}

	for (FIFO_RAM_SHORT, FIFO_DISK_SHORT) {
		$self->[$_] = $self->_short_fifo(static => 1)
	}

	for (FIFO_RAM_LONG, FIFO_DISK_LONG) {
		$self->[$_] = $self->_long_fifo(static => 1)
	}

	if ($self->[BLOOM]) {
		delay {
			$self->[BLOOM]->uncommited(closure {
				my $tbd = 0+@_;
				print "\n$tbd uncommited tokens\n" if $tbd;
				#my @tbd = map {unpack H64 => $_} @_;
				#print join("\n", '', 'still to be done:', @tbd, '') if @tbd;
			});
		} 5,5;
	}

	$self->[DIGEST] = $digest;

	$self->[HEARTBEAT] = delay {
		# print "ASK INFO\n" if %{$self->[WRITERS]};
		for my $writer (values %{$self->[WRITERS]}) {
			$writer->{stream}->write($self->encode_json(["report", {}])."\n");
		}
	} 5,5;
	
	$self
}

sub writer_connection {
	my ($self, $stream) = @_;
	my $timeout = delay { $stream->close } 5;
	$stream->read_length(10 => closure {
		my $buf = shift;
		$timeout->close;
		if ($$buf eq 'LAP-WRITER') {
			$stream->write('LAP-MASTER');
			$stream->read_line(cb(new_writer => $self, $stream));
		} else {
			$stream->close
		}
	})
}

sub new_writer {
	my ($self, $stream, $json) = @_;
	print "new writer\n";
	my $writer = {
		stream => $stream,
		buffer => {},
	};

	my $ok = 1;
	my $status = 'ready';

	my $info = eval { $self->decode_json($$json) } || do {
		warn $@;
		$ok = 0;
		$status = "failed decoding writer json info line: $@";
	};

	if ($ok) {
		$writer->{info} = $info;
	}

	my $response_info = {
		ok		=> $ok,
		status	=> $status,
		version	=> LAP_VERSION,
		digest	=> $self->[DIGEST],
	};
	$stream->write($self->encode_json($response_info)."\n");

	if ($ok) {
		$self->[WRITERS]->{refaddr $writer} = $writer;
		push @{$self->[FREE_WRITERS]}, $writer;
		# $self->[WRITER] = $writer;
		$stream->on_close(cb(lost_writer => $self, $writer));
		$stream->read_all_lines(cb(info_writer => $self, $writer));
		$self->consume;
	} else {
		$stream->close_when_done_write;
	}
}

sub info_writer {
	my ($self, $writer, $line) = @_;
	return unless $$line =~ /\n$/;
	my ($type, $data) = eval { @{$self->decode_json($$line)} };
	die $@ if $@;
	if ($type eq 'done') {
		if (my $token = delete $writer->{buffer}->{$data}) {
			if ($self->[BLOOM] && defined $token->{meta}->{has_read}) {
				$self->[BLOOM]->commit_hex($token->{meta}->{info}->{digest})
			}
		} else {
			warn "token $data was not in buffer, close writer $writer->{info}->{writer_agent}";
			$writer->{stream}->close;
		}
	}
	elsif ($type eq 'report') {
		$writer->{report} = $data;
	}
	else {
		print "\nUNKNOWN INFO from writer: $$line\n";
	}
}

sub lost_writer {
	my ($self, $writer) = @_;
	print "lost writer\n";
	delete $self->[WRITERS]->{refaddr $writer};
	@{$self->[FREE_WRITERS]} = grep {$_ != $writer} @{$self->[FREE_WRITERS]};
	if (%{$writer->{buffer}}) {
		printf "Restore %s failed tokens\n", 0+keys %{$writer->{buffer}};
		unshift @{$self->[QUEUE]}, values %{$writer->{buffer}};
		$self->consume;
	}
}

sub done {
	my ($self, $token, $writer, $from) = @_;
	$self->_destroy_data($token);
	push @{$self->[FREE_WRITERS]}, $writer;
	$self->consume;
}

sub consume {
	my $self = shift;
	return unless @{$self->[FREE_WRITERS]} && @{$self->[QUEUE]};
	my $token = shift @{$self->[QUEUE]};
	my $writer = shift @{$self->[FREE_WRITERS]};
	$writer->{buffer}->{$token->{meta}->{id}} = $token;# if $writer->{info}->{ack};

	my $json_meta = do {
		if ($writer->{info}->{skip_data}) {
			my %meta = %{$token->{meta}}; # explicit copy
			delete $meta{to_read};
			$self->encode_json([meta => \%meta])
		} else {
			$self->encode_json([meta => $token->{meta}])
		}
	};

	if ($token->{meta}->{to_read} && !$writer->{info}->{skip_data}) {
		$writer->{stream}->write($json_meta."\n");
		if ($token->{data_file}) {
			$writer->{stream}->sendfile($token->{data_file}, undef, undef, cb(done => $self, $token, $writer, 'A'));
		} else {
			$writer->{stream}->write((COMPRESS ? decompress(${$token->{data}}) : $token->{data}), undef, undef, cb(done => $self, $token, $writer, 'B'));
		}
	} else {
		$writer->{stream}->write($json_meta."\n", undef, undef, cb(done => $self, $token, $writer, 'C'));
	}
}

sub handle_http {
	my ($self, $state) = @_;
	$self->respond_path($state, {
		LINK_BASE => $state->link_base,
		URL_FRONT => $state->url_front,
		HARBOR_TLD => $self->vortex->harbor_tld
		#INFO => $self->info($state),
	});
}

sub rpc_buffer_size {
	my ($self, $state) = @_;
	+{
		queue => 0+@{$self->[QUEUE]},
		limbo => 0+keys %{$self->[LIMBO]},
	}
}

sub rpc_histo_short {
	my ($self, $state, $nb) = @_;
	+{
		reqs => $self->[FIFO_REQS_SHORT]->get_window($nb),
		data_size => $self->[FIFO_DATA_SIZE_SHORT]->get_window($nb),
		tcp_size => $self->[FIFO_TCP_SIZE_SHORT]->get_window($nb),
		ram_size => $self->[FIFO_RAM_SHORT]->get_window($nb),
		disk_size => $self->[FIFO_DISK_SHORT]->get_window($nb),
	}
}

sub rpc_histo_long {
	my ($self, $state, $nb) = @_;
	+{
		reqs => $self->[FIFO_REQS_LONG]->get_window($nb),
		data_size => $self->[FIFO_DATA_SIZE_LONG]->get_window($nb),
		tcp_size => $self->[FIFO_TCP_SIZE_LONG]->get_window($nb),
		ram_size => $self->[FIFO_RAM_LONG]->get_window($nb),
		disk_size => $self->[FIFO_DISK_LONG]->get_window($nb),
	}
}

sub rpc_close_writer {
	my ($self, $state, $writer_addr) = @_;
	$self->[WRITERS]->{$writer_addr}->{stream}->close;
	1
}

sub rpc_info {
	my ($self, $state) = @_;
	my @limbo = values %{$self->[LIMBO]};
	my @queue = @{$self->[QUEUE]};
	
	+{
		%{$self->[PARAMS]},
		before_deduplication => 0+@limbo,
		before_deduplication_size => (sum map { $_->{meta}->{to_read} ? $_->{meta}->{to_read} : 0 } @limbo) || 0,
		before_writers => 0+@queue,
		before_writers_size => (sum map { $_->{meta}->{to_read} ? $_->{meta}->{to_read} : 0 } @queue) || 0,
		writers => [map { +{info => $_->{info}, report => $_->{report}} } values %{$self->[WRITERS]}],
	}
}

# sub info {
	# my ($self, $state) = @_;
	# my $str = sprintf("writers: %d\nfree writers: %d\n\n",
		# 0+keys %{$self->[WRITERS]},
		# 0+@{$self->[FREE_WRITERS]},
	# );

	# $str .= "LAP LIMBO: (pre bloom)\n". $self->_tokens_info(values %{$self->[LIMBO]}). "\n\n";
	# $str .= "LAP QUEUE:\n". $self->_tokens_info(@{$self->[QUEUE]}). "\n\n";
	# for my $writer (values %{$self->[WRITERS]}) {
		# use YAML::XS;
		# $str .= sprintf(qq!Writer (<a href="%s">close</a>):\n%sinfo:\n%sreport:\n%s\n!,
			# $self->get_ppc($state, {redirection => 'REFRESH'})->get_url(close_writer => refaddr $writer),
			# $self->_tokens_info(values %{$writer->{buffer}}),
			# Dump($writer->{info}),
			# Dump($writer->{report}),
		# );
	# }

	# $str
# }

sub _tokens_info {
	my $self = shift;
	sprintf("buffer elements: %d\nduplicated size: %s\ndeduplicated size: %s\nbuffer size: %s\n",
		0+@_,
		size_as_string(sum map {$_->{meta}->{info}->{size}+0} @_),
		size_as_string(sum map {$_->{meta}->{to_read} ? $_->{meta}->{to_read} : 0} @_),
		size_as_string(sum map {length ${$_->{data}}} grep {$_->{data}} @_),
	)
}

sub filter_request {
	my ($self, $state) = @_;
	# return if $self->vortex->url_is_harbor($state->{url});

	my $headers = $state->req_headers;
	$state->{LAP_request_metadata} = {
		map {
			lc($_) => $headers->remove("lap-$_")
		}
		grep {
			s/^LAP-//i
		} $headers->get_keys
	};

	# print $$headers, " => ", YAML::XS::Dump($state->{LAP_request_metadata}), "\n\n";
}

# compat when streaming==0
sub filter_response {
	my ($self, $state, $original_headers) = @_;
	my $streamer = $self->reponse_streamer($state) || return;
	$streamer->(0, $state->res_body->as_string_ref, $state->res_body->size);
	$streamer->(1);
}

sub reponse_streamer {
	my ($self, $state) = @_;
	# warn "IS WEB=", $state->{is_web}||0;
	return unless $state->{is_web};

	my $id = ++$self->[AUTOINCREMENT];
	my ($content, $fh, $file);
	my $digester = $self->[DIGEST] && Digest->new($self->[DIGEST]);
	# my $expected_size = $state->{req_headers}->get('Content-Length');
	my $size = 0;
	my $response_start_time = int(1000*time);

	$self->[FIFO_REQS_SHORT]->log;
	$self->[FIFO_REQS_LONG]->log;

	my $headers_size = $state->{res_headers_web_client}->length + $state->{req_headers_web_client}->length;

	$self->[FIFO_DATA_SIZE_SHORT]->log($headers_size);
	$self->[FIFO_DATA_SIZE_LONG]->log($headers_size);
	$self->[FIFO_TCP_SIZE_SHORT]->log($headers_size);
	$self->[FIFO_TCP_SIZE_LONG]->log($headers_size);

# warn $headers_size;

	# my $done;
	my $first = 1;

	sub {
		# return if $done;
		my ($status, $chunk, $tcp_chunk_size) = @_;

		if ($status==0) {
			if ($first) {
				$first = 0;
				# first, and client stream not closed (or ignore close)
				if (($state->{res_headers_web_client}->get('Content-Length')||0) > $self->[CONTENT_MAX_MEMORY]) {
					# warn "SIZE from start\n";
					$file = sprintf('%s/%012d', $self->[CONTENT_DIR], $id);
					open($fh, '>', $file) or die "$file: $!";
					binmode $fh;
				} else {
					$content = ''
				}
			}

			my $chunk_size = length $$chunk;
			# warn "$chunk_size >= $tcp_chunk_size";
# if $tcp_chunk_size < $chunk_size;
			$self->[FIFO_DATA_SIZE_SHORT]->log($chunk_size);
			$self->[FIFO_DATA_SIZE_LONG]->log($chunk_size);
			$self->[FIFO_TCP_SIZE_SHORT]->log($tcp_chunk_size);
			$self->[FIFO_TCP_SIZE_LONG]->log($tcp_chunk_size);

			$digester->add($$chunk) if $digester;
			if ($fh) {
				print $fh $$chunk;
				$self->[FIFO_DISK_SHORT]->log($chunk_size);
				$self->[FIFO_DISK_LONG]->log($chunk_size);
			} else {
				if ($size+$chunk_size > $self->[CONTENT_MAX_MEMORY]) {
					$file = sprintf('%s/%012d', $self->[CONTENT_DIR], $id);
					open($fh, '>', $file) or die "$file: $!";
					binmode $fh;
					print $fh $content, $$chunk;
					$content = undef;
					$self->[FIFO_RAM_SHORT]->log(-$size);
					$self->[FIFO_RAM_LONG]->log(-$size);
					$self->[FIFO_DISK_SHORT]->log($size+$chunk_size);
					$self->[FIFO_DISK_LONG]->log($size+$chunk_size);
				} else {
					$content .= $$chunk;
					$self->[FIFO_RAM_SHORT]->log($chunk_size);
					$self->[FIFO_RAM_LONG]->log($chunk_size);
				}
			}
			$size += $chunk_size;
		}
		elsif ($status<0) {
			# closed by client (error)
			# $done = 1;
			if ($fh) {
				$self->[FIFO_DISK_SHORT]->log(-$size);
				$self->[FIFO_DISK_LONG]->log(-$size);
				$fh = undef;
				async {	unlink $file }
			}
			elsif ($size) {
				$self->[FIFO_RAM_SHORT]->log(-$size);
				$self->[FIFO_RAM_LONG]->log(-$size);
				$content = undef;
			}
		}
		else {
			# $done = 1;
			my $digest = $digester && $digester->hexdigest;
			$fh = undef;

			if (DEBUG_CHECK_DIGEST && $digester && $content) {
				$digester->reset;
				$digester->add($content);
				die unless $digest eq $digester->hexdigest;
			}

			my $token = {
				meta => {
					info => {
						url => $state->{url},
						method => $state->{method},
						request_time => int ($state->{req_time} * 1000),
						response_start_time => $response_start_time,
						response_stop_time => int(1000*time),
						status => "success", # LAP status
						digest => $digest,
						size => $size,
						request_ip => $state->{client_ip},
						request_physical_ip => $state->{req_ip},
						# response_ip => 
					},
					'request-info' => $state->{LAP_request_metadata},
					'request-headers' => $state->{req_headers_web_client}->as_string,
					'response-headers' => $state->{res_headers_web_client}->as_string,
					'request-body' => $state->{req_body}->as_scalar,
					to_read => $size,
					id => $id,
				},
				data => $size && defined($content) && \$content,
				data_file => $size && $file
			};

			$self->[LIMBO]->{$token->{meta}->{id}} = $token;

			if ($self->[BLOOM]) {
				my $cb = cb(_enqueue_token => $self, $token);
				$self->[BLOOM]->add_hex($digest, $cb);
				# $self->[BLOOM]->has_hex($digest, $cb);
			} else {
				$self->_enqueue_token($token, 1);
			}
		}
	}
}

sub _enqueue_token {
	my ($self, $token, $new) = @_;
	# $new = !$new;

	delete $self->[LIMBO]->{$token->{meta}->{id}} || die;

	if (!$new) {
		$self->_destroy_data($token);
		$token->{meta}->{has_read} = 0;
	}
	elsif (COMPRESS && $token->{meta}->{info}->{size} && $token->{data}) {
		$token->{data} = \compress(${$token->{data}});
	}

	push @{$self->[QUEUE]}, $token;

	$self->consume;
}

sub _destroy_data {
	my ($self, $token) = @_;
	my $size = delete($token->{meta}->{to_read});
	$token->{meta}->{has_read} = $size;
	if (my $file = delete $token->{data_file}) {
		$self->[FIFO_DISK_SHORT]->log(-$size);
		$self->[FIFO_DISK_LONG]->log(-$size);
		unlink $file;
	}
	elsif ($size) {
		$self->[FIFO_RAM_SHORT]->log(-$size);
		$self->[FIFO_RAM_LONG]->log(-$size);
		delete $token->{data};
	}
}

sub DESTROY {
	my $self = shift || return;
	$self->[HEARTBEAT]->close if $self->[HEARTBEAT];
}

1