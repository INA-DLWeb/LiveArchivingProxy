use ina;
use Lap::Writer;

my $writer = Lap::Writer->new(
	type => 'Log',
	netloc => 'localhost:4365',
);

$writer->loop;