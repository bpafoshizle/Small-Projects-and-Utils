# Author: Barret Miller
# Liscense: GPLv2

# This is a simple perl script to turn XML reserved characters 
# in a document into their xml safe equivalents
use strict;

print("Enter the filename to translate\n");
my $inFileName = <STDIN>;
chomp($inFileName);
my $outFileName = "VBString.txt";
my $text = "";
my $returnText = "";

if(!(-e $inFileName)){ die "Create a file called " . $inFileName . " in the same directory as this script" . 
			   "\nand put your unsafe text to be cleaned up in there."}

open(IN, $inFileName) || die "Couldn't open input file $inFileName";
open(OUT, ">".$outFileName) || die "Couldn't open output file $outFileName";

$text = <IN>;
chomp($text);

# Format the first line
$returnText = "query = \"" . $text . "\" \nquery = query & \"";

# Loop through replacing all the cr-newlines with "\r\nquery = query & " (including quotes)
while(<IN>)
{
	$text = $_;
	$text;
	# Replace all single quotes with an even number of single quotes
	# before them (including zero). 
	#$text =~ s{--(.*)\n}{/\*$1\*/\n};
	$text =~ s/\n/ "\nquery = query & "/;
	$returnText = $returnText . $text;
}
close(IN);

# Remove the last \r\nquery = query & " part (leaving the first quotation)
$returnText = $returnText . "\"";
print(OUT $returnText);

close(OUT);