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

# Replace all 'case' with 'decode(' 
$xml =~ s/case/decode\(/isg;

#Replace all 'when' 'then' and 'else' with a comma
$xml =~ s/when|then|else/,/isg;

# Replace 'end' with a close paren
$xml =~ s/end/\)/isg;
$/ = $lineTermSave;

print(OUT $xml);
close(OUT);