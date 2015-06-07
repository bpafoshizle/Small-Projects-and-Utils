Function nullCheck(value As Double) As Double
	If(value = Nothing) 
		Return 0
	Else
		Return value
	End If
End Function

Function divZero(num As Double, den As Double) As Double
	If(den = 0)
		Return 0
	Else
		Return num/den
	End If
End Function