use strict;
use warnings;

my $inFileName = "combined.txt";
my $productOut = "product.txt";
my $brandOut = "brand.txt";
my $noDash = "noDashes.txt";

my @lines;
my $dummyLine = "******************";
my $line;
my $noDashCount = 0;

open(IN, $inFileName) || die "Couldn't open input file $inFileName";
open(OUT1, ">".$productOut) || die "Couldn't open output file $productOut\n";
open(OUT2, ">".$brandOut) || die "Couldn't open output file $brandOut\n";
open(OUT3, ">".$noDash) || die "Couldn't open output file $noDash\n";

@lines = <IN>;
close(IN);


foreach $line (@lines)
{	
	if($line =~ /-/)
	{
		$line =~ /(\d+)-(\d+)/;
	
		print(OUT1 "$1\n");
		my $brand = $2;
		print(OUT2 "$brand\n");
	}
	else
	{
		print(OUT3 "$line");
		print(OUT1 "***************\n");
		print(OUT2 "***************\n");
		$noDashCount++;
	}
}

print("Number with no dashes: $noDashCount\n");

close(OUT1);
close(OUT2);
close(OUT3);