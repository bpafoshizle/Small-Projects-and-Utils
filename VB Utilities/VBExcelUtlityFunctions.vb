*********************************************************

/* Function to test whether the given cell is a hyperlink
 * by comparing it against the list of all hyperlinks in the
 * currently active worksheet
 */

Function isHyperlinked(testRange As Range) As Boolean
Dim hlink As Hyperlink

isHyperlinked = False

For Each hlink In ActiveSheet.Hyperlinks
    If hlink.Range.Address = testRange.Address Then
        isHyperlinked = True
        Exit Function
    End If
Next
End Function

*************************************************************

/* Function to extract the hyperlink from a cell where it
 * may be hidden
 */

Function getAddress(HyperlinkCell As Range)
    getAddress = HyperlinkCell.Hyperlinks(1).Address
End Function

***************************************************************
  
/* Function to compare two URL's which strips out specials 
 * html characters in case one url has these and the other doesn't
 * which would cause the two URLs which are actually the same to 
 * appear different
 */

Function compareAddresses(a As Range, b As Range) As Boolean
    compareAddresses = (stripHex(getAddress(a)) = stripHex(getAddress(b)))
End Function

***************************************************************

/* Function to strip special HTML % characters out of a string
 * and replace them with their normal character equivalents
 */

Function stripHex(url As String) As String
    Set myRegEx = New RegExp
    myRegEx.IgnoreCase = True
    myRegEx.Global = True
    
    myRegEx.Pattern = "%2F"
    url = myRegEx.Replace(url, "/")
    
    myRegEx.Pattern = "%28"
    url = myRegEx.Replace(url, "(")
    
    myRegEx.Pattern = "%29"
    url = myRegEx.Replace(url, ")")
    
    myRegEx.Pattern = "%20"
    url = myRegEx.Replace(url, " ")
    
    myRegEx.Pattern = "%2D"
    url = myRegEx.Replace(url, "-")
    
    myRegEx.Pattern = "%5F"
    url = myRegEx.Replace(url, "_")
    
    myRegEx.Pattern = "%2E"
    url = myRegEx.Replace(url, ".")
    
    stripHex = url
End Function


**************************************************************


Function SubMatchTest(inpStr)

  Dim oRe, oMatch, oMatches

  Set oRe = New RegExp

  ' Look for an e-mail address (not a perfect RegExp)

  oRe.Pattern = "(\w+)@(\w+)\.(\w+)"

  ' Get the Matches collection

  Set oMatches = oRe.Execute(inpStr)

  ' Get the first item in the Matches collection

  Set oMatch = oMatches(0)

  ' Create the results string.

  ' The Match object is the entire match - dragon@xyzzy.com

  retStr = "Email address is: " & oMatch & vbNewLine

  ' Get the sub-matched parts of the address.

  retStr = retStr & "Email alias is: " & oMatch.SubMatches(0)  ' dragon

  retStr = retStr & vbNewLine

  retStr = retStr & "Organization is: " & oMatch.SubMatches(1) ' xyzzy

  SubMatchTest = retStr

End Function