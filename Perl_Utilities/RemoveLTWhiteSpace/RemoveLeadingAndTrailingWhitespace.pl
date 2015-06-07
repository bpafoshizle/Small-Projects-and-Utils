use strict;
use warnings;

open(IN, "< linesWithWhiteSpace.txt") or die "Can't open input file: linesWithWhiteSpace.txt.\n";
open(OUT, "> noWhiteSpace.txt") or die "Can't open output file: noWhiteSpace.txt.\n";;

my @lines = <IN>;
close(IN);

foreach my $line (@lines)
{
	chomp($line);
	print("Before: '$line'\n");
	print("After: '" . trimWS($line) . "'\n");
	print(OUT trimWS($line) . "\n");
}

sub trimWS
{
	my $inString = $_[0];
	$inString =~ s/^\s*(.*?)\s*$/$1/;
	return $inString;
}