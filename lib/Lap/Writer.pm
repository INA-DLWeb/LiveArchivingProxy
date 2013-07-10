package Lap::Writer;
use Class::SimpleFactory -base;

use constant INTERFACE => qw(
	on_token

	loop
	info
	report
	on_handshake
);

1