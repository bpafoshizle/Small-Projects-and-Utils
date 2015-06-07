# Perl script to translate cognos case statements
# into decode statements

my $inXMLFile = $ARGV[0];
my $outXMLFile = $ARGV[1];
my $lineTermSave = $/;
my $xml;

open(IN, "$inXMLFile");
open(OUT, "> $outXMLFile");
while(<IN>)
{
	$xml .= $_;
}
close(IN);

$/ = undef;
$xml =~ s/case(.*?)when(.*?)then(.*?)else(.*?)end/decode(\1,\2,\3,\4)/sg;
$/ = $lineTermSave;

print(OUT $xml);
close(OUT);
