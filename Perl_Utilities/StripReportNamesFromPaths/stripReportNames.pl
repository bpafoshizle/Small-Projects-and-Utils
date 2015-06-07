# Author: Barret Miller
# Liscense: GPLv2

# This is a simple perl script to strip the hex characters 
# out of a URL and replace them with their equivalent 
# printable ascii characters

use strict;

my $inFileName = "pathsList.txt";
my $outFileName = "reportNames.txt";
my @lines;
my $path;

if(!(-e $inFileName)){ die "Create a file called \'reportNames.txt\' in the same directory as this script" }

open(IN, $inFileName) || die "Couldn't open input file $inFileName";
open(OUT, ">".$outFileName) || die "Couldn't open output file $outFileName";

@lines = <IN>;
close(IN);

foreach $path (@lines)
{
	chomp($path);
	if($path =~ /report\[\@name=/)
	{
		$path =~ s/.*report\[\@name='(.*)']/$1/;
		$path =~ s/.*report\[\@name="(.*)"]/$1/;
		print(OUT $path . "\n");
	}
}

close(OUT);