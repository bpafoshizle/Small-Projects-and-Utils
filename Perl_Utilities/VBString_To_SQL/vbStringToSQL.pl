# Author: Barret Miller
# Liscense: GPLv2

use strict;

print("Enter the filename to translate\n");
my $inFileName = <STDIN>;
chomp($inFileName);
my $outFileName = "SQL.txt";
my $text = "";
my $returnText = "";

if(!(-e $inFileName)){ die "Create a file called " . $inFileName . " in the same directory as this script" . 
			   "\nand put your  to be cleaned up in there."}

open(IN, $inFileName) || die "Couldn't open input file $inFileName";
open(OUT, ">".$outFileName) || die "Couldn't open output file $outFileName";

chomp($text);

# Handle the first line
$returnText = <IN>;
$returnText =~ s/query = "(.*)"/$1/;

# Loop through replacing all the cr-newlines with "\r\nquery = query & " (including quotes)
while(<IN>)
{
	$text = $_;
	# Replace all single quotes with an even number of single quotes
	# before them (including zero). 
	$text =~ s/query = query & "(.*)"/$1/;
	$returnText = $returnText . $text;
}
close(IN);

print(OUT $returnText);

close(OUT);