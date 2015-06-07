use strict;
use warnings;

my $inMasterFileName = "MasterList.txt";
my $inC8FileName = "C8_UserIDList.txt";
my $in11FileName = "11_UserNameList.txt";
my $outFileName = "MasterStatus.csv";

my @MasterList;
my @C8List;
my @C11List;
my @outputObj;

# Read all files
open(IN, "< $inMasterFileName") or die "Can't open input file: $inMasterFileName.\n";
@MasterList = <IN>;
close(IN);
open(IN, "< $inC8FileName") or die "Can't open input file: $in11FileName.\n";
@C8List = <IN>;
close(IN);
open(IN, "< $in11FileName") or die "Can't open input file: $in11FileName.\n";
@C11List = <IN>;
close(IN);

open(OUT, "> MasterStatus.csv") or die "Can't open output file.\n";

foreach my $line (@MasterList)
{
	chomp($line);
	$outputObj[0] = $line;
	$outputObj[1] = "Not Used";
	
	$line =~ /(.*) \((.*)\)/;
	
	my $uname = $1;
	my $uid = $2;
	
	foreach my $C8_uid (@C8List)
	{
		chomp($C8_uid);
		#print("Testing '$uid' against '$C8_uid'\n");
		if(uc($uid) eq uc($C8_uid))
		{
			$outputObj[1] = "Used";
		}
	}
	
	foreach my $C11_uname (@C11List)
	{
		chomp($C11_uname);
		#print("Testing '$uname' against '$C11_uname'\n");
		if($uname eq $C11_uname)
		{
			$outputObj[1] = "Used";
		}
	}
	
	print(OUT $outputObj[0] . "|" . $outputObj[1] . "\n");
}
close(OUT);