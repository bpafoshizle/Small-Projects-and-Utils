use strict;
use warnings;

my @distinctList;
my @delimitedList;
my @columns;

# Read all files
open(IN, "< distinctList.txt") or die "Can't open input file: distinctList.txt.\n" ;
@distinctList = <IN>;
close(IN);
open(IN, "< delimitedList.txt") or die "Can't open input file: delimitedList.txt.\n";
@delimitedList = <IN>;
close(IN);

open(OUT, "> LauraMasterListStatus.csv") or die "Can't open output file.\n";

# take the headers off the list and split them out first
@columns = split(/\t/, shift(@delimitedList));
print(OUT trimWS($columns[0]) . "|" . trimWS($columns[1]) .  "|" . trimWS($columns[2]) . "\n");

foreach my $line (@delimitedList)
{
	chomp($line);
	@columns = split(/\t/, $line);
	$columns[2] = "Used";
	#print($columns[0] . "|" . $columns[1] .  "|" . $columns[2] . "\n");
	foreach my $distinctUser (@distinctList)
	{
		chomp($distinctUser);
		
		#print ("'" . uc(trimWS(trimQuotes($columns[0]))) . "' '" . uc(trimWS($distinctUser)) . "'\n");
		
		if(uc(trimWS(trimQuotes($columns[0]))) eq uc(trimWS($distinctUser)))
		{
			$columns[2] = "Not Used";
		}
	}
	print(OUT $columns[0] . "|" . $columns[1] .  "|" . $columns[2] . "\n");
}
close(OUT);

sub trimWS
{
	my $inString = $_[0];
	$inString =~ s/^\s*(.*?)\s*$/$1/;
	return $inString;
}

sub trimQuotes
{
	my $inString = $_[0];
	$inString =~ s/"(.*)"/$1/;
	return $inString;
}