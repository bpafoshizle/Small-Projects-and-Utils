#Author: Barret Miller

use strict;

my $inFileName1;
my $inFileName2;
my $outFileName;

my @lines1;
my @lines2;
my @saveLines;
my $line;
my $testLine;
my $outString;
my $repeatCount;
my $totalRepeated = 0;

print("Enter your first input file name:\n");
$inFileName1 = <STDIN>;
chomp($inFileName1);

print("Enter your second input file name:\n");
$inFileName2 = <STDIN>;
chomp($inFileName2);

print("Enter an output file name:\n");
$outFileName = <STDIN>;
chomp($outFileName);

open(IN, "< $inFileName1") or die "Can't open input file.\n";
@lines1 = <IN>;
close(IN);
open(IN, "< $inFileName2") or die "Can't open input file.\n";
@lines2 = <IN>;
close(IN);

open(OUT, "> $outFileName") or die "Can't open output file.\n";

foreach $line (@lines1)
{
	$outString = "";
	$repeatCount = 0;
	chomp($line);
	
	for(my $i = 0; $i < @lines2; $i++)
	{
		$testLine = $lines2[$i];
		chomp($testLine);

		if($line eq $testLine)
		{
			# If we have already found the word once before (where it should be)
			# Then the next time we find it is a repeat
			if($repeatCount == 1)
			{
				# on the first repeated instance found 
				# prepare the output string with the repeated element
				$outString = $line;
			}

			$repeatCount++;
		}
	}
	
	# Note: cannot test for null string by $string == "" or
	# $string eq "" or $string ne ""
	if($outString)
	{
		print(OUT "$outString (repeated " . $repeatCount . " times)\n");
		$totalRepeated++;
	}
}
print(OUT "Total repeated strings: " . $totalRepeated);
close(OUT);