# Perl script to remove leading zeros from a 
# number

my $inFileName;
my $outFileName;

my @lines;
my $line;

print("Enter your input file name:\n");
$inFileName = <STDIN>;

print("Enter your output file name:\n");
$outFileName = <STDIN>;

open(IN, "< $inFileName") or die "Can't open input file.\n";
@lines = <IN>;
close(IN);

open(OUT, "> $outFileName") or die "Can't open output file.\n";

foreach $line (@lines)
{
	$line =~ s/^0*//;
	print(OUT $line);
}

close(OUT);