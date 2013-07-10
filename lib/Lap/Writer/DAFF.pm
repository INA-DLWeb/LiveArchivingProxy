package Lap::Writer::DAFF;
use strict;
use warnings;
use Carp;
use 5.010;
use YAML::XS;
use bytes;
use File::Path qw(mkpath);
use File::Copy qw(move);
use URL::String qw(normalize);
use File::DAFF::Util qw(daff_status_from_HTTP);
use File::DAFF::CoreUtil qw(parse_size size_as_string);
use File::DAFF::Writer;
use Anet::HTTP::Headers;
use base 'Lap::Writer::base';
use enum::fields::extending 'Lap::Writer::base', qw(
	MAX_DAFF_DATA_SIZE
	MAX_DAFF_META_SIZE
	DAFF_DATA_DIR
	DAFF_META_DIR
	DAFF_DATA
	DAFF_META
	FORCE_META
);

use constant VERBOSE => 1;

use constant DEFAULT_DAFF_PARAMS => (
	check_id => 1,
	check_metadata => 1,
);

our $VERSION = '0.1';

sub info {
	writer_agent => "DAFF-Writer/$VERSION (perl $])",
}

sub report {
	available_storage => undef,
	written_size => undef,
}

sub new {
	my $class = shift;
	my %args = (
		max_daff_data_size => undef,
		max_daff_meta_size => undef,
		daff_data_dir => undef,
		daff_meta_dir => undef,
		force_meta => undef, # or hash ref
		@_
	);

	my $max_daff_data_size = parse_size(delete($args{max_daff_data_size}) || '16GB');
	my $max_daff_meta_size = parse_size(delete($args{max_daff_meta_size}) || '16GB');
	my $daff_data_dir = delete($args{daff_data_dir}) || croak "missing argument 'daff_data_dir'";
	my $daff_meta_dir = delete($args{daff_meta_dir}) || croak "missing argument 'daff_meta_dir'";
	my $force_meta = delete($args{force_meta});

	my $self = $class->SUPER::new(%args);

	$self->[MAX_DAFF_DATA_SIZE] = $max_daff_data_size;
	$self->[MAX_DAFF_META_SIZE] = $max_daff_meta_size;
	$daff_data_dir .= '/' unless $daff_data_dir =~ m!/$!;
	$daff_meta_dir .= '/' unless $daff_meta_dir =~ m!/$!;
	$self->[DAFF_DATA_DIR] = $daff_data_dir;
	$self->[DAFF_META_DIR] = $daff_meta_dir;

	mkpath($self->[DAFF_DATA_DIR]);
	mkpath($self->[DAFF_META_DIR]);

	$self->check_writer(\$self->[DAFF_DATA], $self->[DAFF_DATA_DIR], $self->[MAX_DAFF_DATA_SIZE]);
	$self->check_writer(\$self->[DAFF_META], $self->[DAFF_META_DIR], $self->[MAX_DAFF_META_SIZE]);

	$self->[FORCE_META] = $force_meta;

	say "ready";
	$self
}

sub check_writer {
	my ($self, $writer_ref, $dir, $max_size) = @_;
	$dir .= '/' unless $dir =~ m!/$!;

	if ($$writer_ref) {
		return $$writer_ref if $$writer_ref->size < $max_size;
		my $file = $$writer_ref->file; # before close
		$$writer_ref->close;
		say "\ncommiting $file to $dir";
		move($file, $dir) or die $!;
	}

	my @small_daff = grep {-s $_ < $max_size} glob($dir.'*.daff');

	$$writer_ref = File::DAFF::Writer->new(
		DEFAULT_DAFF_PARAMS,
		file => shift(@small_daff) || $dir,
	)
}

sub on_handshake {
	my ($self, $info) = @_;
	die "DAFF writer requires 'SHA-256' digest" unless $info->{digest} && $info->{digest} eq 'SHA-256';
}

sub on_token {
	my ($self, $meta, $data_cb, $cb) = @_;

	VERBOSE && print STDERR 'M';
	my $daff_meta = $self->_lap_meta2daff_meta($meta);
	eval {
		$self->[DAFF_META]->put(
			type_flag => 10,
			data => $daff_meta,
		) or die;
	};
	if ($@) {
		warn "\n$@\n", Dump($daff_meta);
	}
	
	$self->check_writer(\$self->[DAFF_META], $self->[DAFF_META_DIR], $self->[MAX_DAFF_META_SIZE]);

	if ($data_cb) {
		VERBOSE && print STDERR 'D';
		eval {
			$self->[DAFF_DATA]->put(
				type_flag => 11,
				data => $data_cb,
				length => $daff_meta->{length},
				id => $daff_meta->{content},
				content_type => $daff_meta->{type},
			) or die;
		};
		die $@, Dump $meta->{info} if $@;
		$self->check_writer(\$self->[DAFF_DATA], $self->[DAFF_DATA_DIR], $self->[MAX_DAFF_DATA_SIZE]);
	}

	$cb->();
}

sub sync {
	my $self = shift;
	$self->[DAFF_META] && $self->[DAFF_META]->sync && VERBOSE && print STDERR "[sync meta]";
	$self->[DAFF_DATA] && $self->[DAFF_DATA]->sync && VERBOSE && print STDERR "[sync data]";
}

sub _lap_meta2daff_meta {
	my ($self, $meta) = @_;

	my $res_headers = Anet::HTTP::Headers->new($meta->{'response-headers'});
	my $req_headers = Anet::HTTP::Headers->new($meta->{'request-headers'});

	my $daff_meta = {};

	if (my $content_type = $res_headers->get('Content-Type')) {
		(my $mime) = $content_type =~ m!([\w\-]+/[\w\-]+)!;
		(my $charset) = $content_type =~ m!;\s*(?:charset\s*[:=]\s*)?([^\s]+)!;
		$daff_meta->{type} = $mime;
		$daff_meta->{charset} = $charset if $charset;
	}

	if ($res_headers->has('Content-Disposition') && $res_headers->get('Content-Disposition') =~ m!attachment;\s*filename="?([^;"]+)!i) {
		$daff_meta->{filename} = $1
	}

	if (my $last_modified = $res_headers->get('Last-Modified')) {
		$daff_meta->{last_modified} = $last_modified;
	}
		
	if (my $etag = $res_headers->get('ETag')) {
		$daff_meta->{etag} = $etag;
	}
		
	if ($res_headers->is_redirection) {
		my $location = $res_headers->get('Location');
		if (defined $location) {
			$daff_meta->{location} = $location
		} else {
			$daff_meta->{code} = 404; # eg 300 multiple choices, with alternative URLs in HTML body
		}
	}

	my $meta_info = $meta->{info};
	my $req_info = $meta->{'request-info'};
		
	$daff_meta->{length} = $meta_info->{size};
	$daff_meta->{agent} = $req_headers->get('User-Agent') if $req_headers->has('User-Agent');

	$daff_meta->{content} = $meta_info->{digest};
	$daff_meta->{date} = _epoch2isoz(int ($meta_info->{request_time} / 1000));
	
	$daff_meta->{original_url} = $meta_info->{url};
	$daff_meta->{url} = normalize($meta_info->{url});

	{# level
		if (defined $req_info->{level}) {
			$daff_meta->{level} = $req_info->{level}
		}
		elsif ($req_headers->has('x-site-level')) {
			# crocket compat
			$daff_meta->{level} = $req_headers->get('x-site-level')
		}
	}

	{# crawl-session and corpus
		if (defined $req_info->{'crawl-session'}) {
			$daff_meta->{crawl_session} = $req_info->{'crawl-session'}
		}
		elsif ($req_headers->has('x-crawl-session')) {
			# crocket compat
			$daff_meta->{crawl_session} = $req_headers->get('x-crawl-session');
			if ($daff_meta->{crawl_session} =~ m!^([^@/]+)[@/][^@]+@!) {
				$daff_meta->{corpus} = $1;
				$daff_meta->{crawl_session} =~ s!^[^@/]+[@/]([^@]+@)!$1!;
			}
		}

		$daff_meta->{corpus} = $req_info->{corpus} if defined $req_info->{corpus};
	}

	{# page
		if (defined $req_info->{'is-page'}) {
			$daff_meta->{page} = $req_info->{'is-page'} ? 1:0
		}
		elsif ($req_headers->has('x-is-page')) {
			# dlweb browser plugin compat
			$daff_meta->{page} = $req_headers->get('x-is-page') ? 1:0
		}
	}

	$daff_meta->{client_ip} = $meta_info->{'request-ip'} if defined $meta_info->{'request-ip'};
	$daff_meta->{ip} = $meta_info->{'response-ip'} if defined $meta_info->{'response-ip'};
	$daff_meta->{client_lang} = $req_headers->get('Accept-Language') if $req_headers->has('Accept-Language');
	$daff_meta->{last_modified} = $req_headers->get('Last-Modified') if $req_headers->has('Last-Modified');
	$daff_meta->{etag} = $req_headers->get('ETag') if $req_headers->has('ETag');

	my $status = daff_status_from_HTTP($daff_meta->{code} || $res_headers->get_code);
	if ($status) {
		$daff_meta->{status} = $status;
		delete $daff_meta->{code};
	} else {
		warn "bad HTTP code '$daff_meta->{code}' for $daff_meta->{url}";
		return;
	}

	if ($self->[FORCE_META]) {
		@$daff_meta{keys %{$self->[FORCE_META]}} = values %{$self->[FORCE_META]}
	}

	$daff_meta
}

sub _epoch2isoz (;$) {
	my ($sec,$min,$hour,$mday,$mon,$year) = defined($_[0]) ? gmtime($_[0]) : gmtime();
	sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", $year+1900, $mon+1, $mday, $hour, $min, $sec);
}

sub DESTROY {
	my $self = shift || return;
	$self->sync;
}

1