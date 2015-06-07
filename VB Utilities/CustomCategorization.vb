Function returnMealCategory(productCode As String) As String
	Dim chickenCodes() As String = { "9305","9307"} 'Chicken
	Dim cbpmList() As String = {"6024","6087","6089","9821","9853","9871","9872","9896","9914","9924","9925","9926","9928","9932", "17983", "17984"}  'CBPM
	Dim lowAshList() As String = {"9070","9071","9829","9835","9895","9897", "17574"} 'Low Ash

	If (isInList(chickenCodes, productCode)) Then
		Return "Chicken"
	Elseif (isInList(cbpmList, productCode)) Then
		Return "CBPM"
	Elseif (isInList(lowAshList, productCode)) Then
		Return "Low Ash"
	Else
		Return productCode
	End If
	
End Function


Function isInList(list() As String, findThisString As String) As Boolean
	
	For Each Str As String In list
    		If Str.Contains(findThisString) Then
        			Return True
    		End If
	Next
	
	Return False

End Function