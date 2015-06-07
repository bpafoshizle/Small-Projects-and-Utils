# Author: Barret Miller
# Liscense: GPLv2

# This is a simple perl script to turn XML reserved characters 
# in a document into their xml safe equivalents
use strict;

print("Enter the filename to translate\n");
my $inFileName = <STDIN>;
chomp($inFileName);
my $outFileName = "xmlSafe.txt";
my $text;
my $recTerminator = $/;

if(!(-e $inFileName)){ die "Create a file called " . $inFileName . " in the same directory as this script" . 
			   "\nand put your unsafe text to be cleaned up in there."}

open(IN, $inFileName) || die "Couldn't open input file $inFileName";
open(OUT, ">".$outFileName) || die "Couldn't open output file $outFileName";

# Slurp the entire file into a single var
local $/ = undef;
$text = <IN>;
local $/ = $recTerminator;
close(IN);

# Swap out the unsafe for the safe
$text =~ s/&/&amp;/g;
$text =~ s/</&lt;/g;
$text =~ s/>/&gt;/g;
$text =~ s/%/&#37;/g;

print(OUT $text);

close(OUT);