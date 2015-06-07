#Author: Barret Miller

use strict;

my $inFileName;
my $outFileName = "positionFinderOut.txt";
my $lines = "";
my $position = $ARGV[0];

print("Enter the input file name:\n");
$inFileName = <STDIN>;
chomp($inFileName);

open(IN, $inFileName);

open(IN, "< $inFileName") or die "Can't open input file.\n";

while(<IN>)
{
	$lines = $lines . $_;
}

close($inFileName);

print("Counting to position: $position\n");

open(OUT, "> $outFileName") or die "Can't open output file.\n";
substr($lines, $position, 1, '********');
print($lines . "\n");

print(OUT "$lines\n");
close(OUT);