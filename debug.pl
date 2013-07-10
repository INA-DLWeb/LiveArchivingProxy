use ina;
use Lap::Writer;

my $writer = Lap::Writer->new(
	type => 'Debug',
	netloc => 'localhost:4365',
);

$writer->loop;