use strict;

my $inFileName;
my $outFileName;

my @lines;
my @saveLines;
my $line;
my $testLine;

my $oldLineCount;
my $newLineCount;

my $sameWordCount;

print("Enter an input file name:\n");
$inFileName = <STDIN>;
chomp($inFileName);

print("Enter an output file name:\n");
$outFileName = <STDIN>;
chomp($outFileName);

my $filePath = "C:\\Documents and Settings\\MILLERBARR\\My Documents\\Tyson Report Catalog\\";

open(IN, "< $filePath$inFileName") or die "Can't open input file.\n";
open(OUT, "> $filePath$outFileName") or die "Can't open output file.\n";

@lines = <IN>;
close(IN);

$oldLineCount = @lines;
print("$oldLineCount lines in $inFileName\n");

@saveLines = @lines;

foreach $line (@saveLines)
{
	$sameWordCount = 0;

	for(my $i = 0; $i < @lines; $i++)
	{
		$testLine = $lines[$i];

		if($line eq $testLine)
		{
			# If we have already found the word once before
			if($sameWordCount != 0)
			{
				splice(@lines, $i, 1);

				# Must decrement $i here because the array shifts left
				# 1 place, and we don't want to skip the next entry
				$i--;
			}

			$sameWordCount++;
		}
	}
}
	

$newLineCount = @lines;
print("$newLineCount lines in $outFileName\n");
print(($oldLineCount - $newLineCount) . " repeated lines removed from $inFileName\n");
print(OUT @lines);
close(OUT);