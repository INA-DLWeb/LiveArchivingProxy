package Lap::Writer::Log;
use strict;
use warnings;
use 5.010;
use YAML::XS;
use base 'Lap::Writer::base';

use enum::fields::extending 'Lap::Writer::base', qw(
	WRITTEN_SIZE_LOG
);

sub info {
	writer_agent => "Log Writer",
	choucroute => 'bière',
}


sub report {
	my $self = shift;
	(
		written_size => $self->[WRITTEN_SIZE_LOG]||0,
		Wurst => 'Bier',
	)
}

sub on_token {
	my ($self, $meta, $data, $cb) = @_;
	say $meta->{info}->{url};
	$self->[WRITTEN_SIZE_LOG] += 1+length($meta->{info}->{url});
	$cb->()
}

sub on_handshake {
	my ($self, $info) = @_;
	print "LAP info:\n", Dump $info;
}

1