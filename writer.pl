use ina;
use Lap::Writer;

my $id = shift;

my $writer = Lap::Writer->new(
	type => 'DAFF',
	netloc => 'localhost:4365',
	daff_data_dir => "/data/LAP/data/",
	daff_meta_dir => "/data/LAP/meta/",
	max_daff_data_size => '16GB',
	max_daff_meta_size => '16GB',
	force_meta => {
		client_country => 'fr',
	}
	# sleep => 0.20,
);

$SIG{INT} = sub {
	print STDERR "\nINT CATCHED\n";
	$writer->sync if $writer;
	exit;
};

$writer->loop;
