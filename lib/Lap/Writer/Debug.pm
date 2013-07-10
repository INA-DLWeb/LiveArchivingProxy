package Lap::Writer::Debug;
use strict;
use warnings;
use 5.010;
use YAML::XS;
use bytes;
use Anet::CORE::Util qw(size_as_string);
use base 'Lap::Writer::base';

sub info {}

sub on_token {
	my ($self, $meta, $data_cb, $cb) = @_;
	print Dump $meta;
	my $data = $data_cb->();
	printf("DATA size: %s (%d)\n\n",
		size_as_string(length($$data)),
		length($$data),
	);
	$cb->()
}

1