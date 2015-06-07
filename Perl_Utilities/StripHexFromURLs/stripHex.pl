# Author: Barret Miller
# Liscense: GPLv2

# This is a simple perl script to strip the hex characters 
# out of a URL and replace them with their equivalent 
# printable ascii characters

use strict;

my $inFileName = "urlsWithHex.txt";
my $outFileName = "urlsWithoutHex.txt";
my @lines;
my $url;

if(!(-e $inFileName)){ die "Create a file called \'urlsWithHex.txt\' in the same directory as this script" . 
			   "\nand put your hexified URLS to be cleaned up in there."}

open(IN, $inFileName) || die "Couldn't open input file $inFileName";
open(OUT, ">".$outFileName) || die "Couldn't open output file $outFileName";

@lines = <IN>;
close(IN);

foreach $url (@lines)
{
	chomp($url);
	print(OUT stripHex($url)."\n");
}

close(OUT);

sub stripHex()
{
    my ($string) = @_;

    # hex(hexString) returns the number that the hex string represents 
    # this is a binary number inside the computer of course, but can be 
    # represented as hex or decimal. If it were printed, it would show
    # up as the decimal value of 175.  
    # pack("C", number) then takes the number result of hex(hexString)
    # and turns it into an unsigned char (octet) value, that is the byte
    # value that represents a character in ascii.  
    $string =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
    
    return $string;
}