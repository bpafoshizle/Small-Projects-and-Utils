-- AT, RAT, OCCURS, PADC, PADR, PADL,CHRTRAN, STRFILTER, STRTRAN, WORDTRAN
-- GETALLWORDS, GETWORDCOUNT, GETWORDNUM, GETNUMWORD, GETOCCURSWORD, GETATWORD, GETRATWORD
-- PROPER, RCHARINDEX, ARABTOROMAN, ROMANTOARAB
-- AT()  Returns the beginning numeric position of the nth occurrence of a character expression within
--       another character expression, counting from the leftmost character
-- RAT() Returns the numeric position of the last (rightmost) occurrence of a character string within 
--       another character string
-- OCCURS() Returns the number of times a character expression occurs within another character expression
-- PADL()   Returns a string from an expression, padded with spaces or characters to a specified length on the left side
-- PADR()   Returns a string from an expression, padded with spaces or characters to a specified length on the right side
-- PADC()   Returns a string from an expression, padded with spaces or characters to a specified length on the both sides
-- CHRTRAN()   Replaces each character in a character expression that matches a character in a second character expression
--             with the corresponding character in a third character expression.
-- STRFILTER() Removes all characters from a string except those specified. 
-- STRTRAN()   Searches a character expression for occurrences of a second character expression,
--             and then replaces each occurrence with a third character expression.
--             Unlike a built-in function replace, STRTRAN has three additional parameters.
-- WORDTRAN() Searches a character string for occurrences of a first word, and then replaces each occurrence with a second word.
-- GETALLWORDS()  Inserts the words from a string into the table
-- GETWORDCOUNT() Counts the words in a string
-- GETWORDNUM()  Returns a specified word from a string
-- GETNUMWORD()  Returns the index position of a word in a string.
-- GETOCCURSWORD() Returns the number of times a word occurs within the character string.
-- GETATWORD()  Returns the beginning numeric position of the nth occurrence of a word within the character string, counting from the leftmost character.
-- GETRATWORD() Returns the numeric position of the last (rightmost) occurrence of a word within the character string.
-- PROPER() Returns from a character expression a string capitalized as appropriate for proper names
-- RCHARINDEX()  Similar to the Transact-SQL function Charindex, with a Right search
-- ARABTOROMAN() Returns the character Roman numeral equivalent of a specified numeric expression (from 1 to 3999)
-- ROMANTOARAB() Returns the number equivalent of a specified character Roman numeral expression (from I to MMMCMXCIX)
-- Examples:   GETWORDCOUNT, GETWORDNUM, GETNUMWORD
-- select  dbo.GETWORDCOUNT('User-Defined marvellous string Functions Transact-SQL', default)
-- select  dbo.GETWORDNUM('User-Defined marvellous string Functions Transact-SQL', 2, default)
-- select  dbo.GETNUMWORD('O Canada! Our home and native land!', 'Canada!', default, 1, 0)  --  Displays 2
-- Examples:  CHRTRAN, STRFILTER
-- select dbo.CHRTRAN('ABCDEF', 'ACE', 'XYZ')   -- Displays XBYDZF
-- select dbo.STRFILTER('ABCDABCDABCD', 'AB')   -- Displays ABABAB
-- AT, RAT, OCCURS, PROPER  
-- select  dbo.AT ('marvellous', 'User-Defined marvellous string Functions Transact-SQL', default)
-- select  dbo.OCCURS ('F', 'User-Defined marvellous string Functions Transact-SQL')
-- select  dbo.PROPER ('User-Defined marvellous string Functions Transact-SQL')
-- PADC, PADR, PADL 
-- select  dbo.PADC (' marvellous string Functions', 60, '+*+')
-- ARABTOROMAN, ROMANTOARAB
-- select dbo.ARABTOROMAN(3888)      -- Displays MMMDCCCLXXXVIII
-- select dbo.ROMANTOARAB('CCXXXIV') -- Displays 234
-- For more information about string UDFs Transact-SQL please visit the 
-- http://www.universalthread.com/wconnect/wc.dll?LevelExtreme~2,54,33,27115   or 
-- http://nikiforov.developpez.com/espagnol/  (the Spanish language)
-- http://nikiforov.developpez.com/           (the French  language)
-- http://nikiforov.developpez.com/allemand/  (the German  language)
-- http://nikiforov.developpez.com/italien/   (the Italian language)
-- http://nikiforov.developpez.com/portugais/ (the Portuguese language)
-- http://nikiforov.developpez.com/roumain/   (the Roumanian  language)
-- http://nikiforov.developpez.com/russe/     (the Russian language)
-- http://nikiforov.developpez.com/bulgare/   (the Bulgarian language)
--------------------------------------------------------------------------------------------------------
-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- AT() User-Defined Function 
-- Returns the beginning numeric position of the first occurrence of a character expression within another character expression, counting from the leftmost character.
-- (including  overlaps)
-- AT(@cSearchExpression, @cExpressionSearched [, @nOccurrence]) Return Values smallint 
-- Parameters
-- @cSearchExpression nvarchar(4000) Specifies the character expression that AT( ) searches for in @cExpressionSearched. 
-- @cExpressionSearched nvarchar(4000) Specifies the character expression @cSearchExpression searches for. 
-- @nOccurrence smallint Specifies which occurrence (first, second, third, and so on) of @cSearchExpression is searched for in @cExpressionSearched. By default, AT() searches for the first occurrence of @cSearchExpression (@nOccurrence = 1). Including @nOccurrence lets you search for additional occurrences of @cSearchExpression in @cExpressionSearched. AT( ) returns 0 if @nOccurrence is greater than the number of times @cSearchExpression occurs in @cExpressionSearched. 
-- Remarks
-- AT() searches the second character expression for the first occurrence of the first character expression. It then returns an integer indicating the position of the first character in the character expression found. If the character expression is not found, AT() returns 0. The search performed by AT() is case-sensitive.
-- AT is nearly similar to a function Oracle PL/SQL INSTR
-- Example
-- declare @gcString nvarchar(4000), @gcFindString nvarchar(4000)
-- select @gcString = 'Now is the time for all good men', @gcFindString = 'is the'
-- select dbo.AT(@gcFindString, @gcString, default)  -- Displays 5
-- set @gcFindString = 'IS'
-- select dbo.AT(@gcFindString, @gcString, default)  -- Displays 0, case-sensitive
-- select @gcString = 'goood men', @gcFindString = 'oo'
-- select dbo.AT(@gcFindString, @gcString, 1)  -- Displays 2
-- select dbo.AT(@gcFindString, @gcString, 2)  -- Displays 3, including  overlaps
-- See Also RAT(), ATC(), AT2()  User-Defined Function 
-- UDF the name and functionality of which correspond  to the  Visual FoxPro function  
CREATE function AT  (@cSearchExpression nvarchar(4000), @cExpressionSearched  nvarchar(4000), @nOccurrence  smallint = 1 )
returns smallint
as
    begin
      if @nOccurrence > 0
         begin
            declare @i smallint,  @StartingPosition  smallint
            select  @i = 0, @StartingPosition  = -1
            while  @StartingPosition <> 0 and @nOccurrence > @i
               select  @i = @i + 1, @StartingPosition  = charindex(@cSearchExpression COLLATE Latin1_General_BIN, @cExpressionSearched COLLATE Latin1_General_BIN,  @StartingPosition+1 )
         end
      else
         set @StartingPosition =  NULL

     return @StartingPosition
    end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
-- RAT( ) User-Defined Function
-- Returns the numeric position of the last (rightmost) occurrence of a character string within another character string.
-- (including  overlaps)
-- RAT(@cSearchExpression, @cExpressionSearched [, @nOccurrence])
-- Return Values smallint 
-- Parameters
-- @cSearchExpression nvarchar(4000) Specifies the character expression that RAT( ) looks for in @cExpressionSearched. 
-- @cExpressionSearched nvarchar(4000) Specifies the character expression that RAT() searches. 
-- @nOccurrence smallint Specifies which occurrence, starting from the right and moving left, of @cSearchExpression RAT() searches for in @cExpressionSearched. By default, RAT() searches for the last occurrence of @cSearchExpression (@nOccurrence = 1). If @nOccurrence is 2, RAT() searches for the next to last occurrence, and so on. 
-- Remarks
-- RAT(), the reverse of the AT() function, searches the character expression in @cExpressionSearched starting from the right and moving left, looking for the last occurrence of the string specified in @cSearchExpression.
-- RAT() returns an integer indicating the position of the first character in @cSearchExpression in @cExpressionSearched. RAT() returns 0 if @cSearchExpression is not found in @cExpressionSearched, or if @nOccurrence is greater than the number of times @cSearchExpression occurs in @cExpressionSearched.
-- The search performed by RAT() is case-sensitive.
-- Example
-- declare @gcString nvarchar(4000), @gcFindString nvarchar(4000)
-- select @gcString = 'abracadabra', @gcFindString = 'a' 
-- select dbo.RAT(@gcFindString , @gcString, default)  -- Displays 11
-- select dbo.RAT(@gcFindString , @gcString , 3)       -- Displays 6
-- select @gcString = 'goood men', @gcFindString = 'oo'
-- select dbo.RAT(@gcFindString, @gcString, 1)  -- Displays 3
-- select dbo.RAT(@gcFindString, @gcString, 2)  -- Displays 2, including  overlaps
-- See Also AT()  User-Defined Function 
-- UDF the name and functionality of which correspond  to the  Visual FoxPro function     
CREATE function RAT  (@cSearchExpression nvarchar(4000), @cExpressionSearched  nvarchar(4000), @nOccurrence  smallint = 1 )
returns smallint
as
    begin
      if @nOccurrence > 0
         begin
            declare @i smallint, @length smallint, @StartingPosition  smallint
            select  @length  = datalength(@cExpressionSearched)/(case SQL_VARIANT_PROPERTY(@cExpressionSearched,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
            select  @cSearchExpression = reverse(@cSearchExpression), @cExpressionSearched = reverse(@cExpressionSearched)
            select  @i = 0, @StartingPosition  = -1 
            while @StartingPosition <> 0 and @nOccurrence > @i
               select  @i = @i + 1, @StartingPosition  = charindex(@cSearchExpression  COLLATE Latin1_General_BIN,
                                                                   @cExpressionSearched  COLLATE Latin1_General_BIN, @StartingPosition + 1)
            if @StartingPosition <> 0
              select @StartingPosition = 2 - @StartingPosition +  @length - datalength(@cSearchExpression)/(case SQL_VARIANT_PROPERTY(@cSearchExpression,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
         end
      else
         set @StartingPosition =  NULL
         
      return @StartingPosition
    end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- AT2() User-Defined Function 
-- Returns the beginning numeric position of the first occurrence of a character expression within another character expression, counting from the leftmost character.
-- (excluding  overlaps)
-- AT2(@cSearchExpression, @cExpressionSearched [, @nOccurrence]) Return Values smallint 
-- Parameters
-- @cSearchExpression nvarchar(4000) Specifies the character expression that AT2( ) searches for in @cExpressionSearched. 
-- @cExpressionSearched nvarchar(4000) Specifies the character expression @cSearchExpression searches for. 
-- @nOccurrence smallint Specifies which occurrence (first, second, third, and so on) of @cSearchExpression is searched for in @cExpressionSearched. By default, AT2() searches for the first occurrence of @cSearchExpression (@nOccurrence = 1). Including @nOccurrence lets you search for additional occurrences of @cSearchExpression in @cExpressionSearched. AT2( ) returns 0 if @nOccurrence is greater than the number of times @cSearchExpression occurs in @cExpressionSearched. 
-- Remarks
-- AT2() searches the second character expression for the first occurrence of the first character expression. It then returns an integer indicating the position of the first character in the character expression found. If the character expression is not found, AT2() returns 0. The search performed by AT2() is case-sensitive.
-- AT2 is nearly similar to a function Oracle PL/SQL INSTR
-- Example
-- declare @gcString nvarchar(4000), @gcFindString nvarchar(4000)
-- select @gcString = 'Now is the time for all good men', @gcFindString = 'is the'
-- select dbo.AT2(@gcFindString, @gcString, default)  -- Displays 5
-- set @gcFindString = 'IS'
-- select dbo.AT2(@gcFindString, @gcString, default)  -- Displays 0, case-sensitive 
-- select @gcString = 'goood men', @gcFindString = 'oo'
-- select dbo.AT2(@gcFindString, @gcString, 1)  -- Displays 2
-- select dbo.AT2(@gcFindString, @gcString, 2)  -- Displays 0, excluding  overlaps
-- See Also AT(), ATC(), ATC2()  User-Defined Function 
CREATE function AT2  (@cSearchExpression nvarchar(4000), @cExpressionSearched  nvarchar(4000), @nOccurrence  smallint = 1 )
returns smallint
as
    begin
      declare  @LencSearchExpression smallint
      select    @LencSearchExpression  = datalength(@cSearchExpression)/(case SQL_VARIANT_PROPERTY(@cSearchExpression,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

      if @nOccurrence > 0
         begin
            declare @i smallint,  @StartingPosition  smallint
            select  @i = 0, @StartingPosition  = -1 - @LencSearchExpression
            while  @StartingPosition <> 0 and @nOccurrence > @i
               select  @i = @i + 1, @StartingPosition  = charindex(@cSearchExpression COLLATE Latin1_General_BIN, @cExpressionSearched COLLATE Latin1_General_BIN,  @StartingPosition + @LencSearchExpression )
         end
      else
         set @StartingPosition =  NULL
  return @StartingPosition
 end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- ATC() User-Defined Function 
-- Returns the beginning numeric position of the first occurrence of a character expression within another character expression, counting from the leftmost character.
-- The search performed by ATC() is case-insensitive (including  overlaps). 
-- ATC(@cSearchExpression, @cExpressionSearched [, @nOccurrence]) Return Values smallint 
-- Parameters
-- @cSearchExpression nvarchar(4000) Specifies the character expression that ATC( ) searches for in @cExpressionSearched. 
-- @cExpressionSearched nvarchar(4000) Specifies the character expression @cSearchExpression searches for. 
-- @nOccurrence smallint Specifies which occurrence (first, second, third, and so on) of @cSearchExpression is searched for in @cExpressionSearched. By default, ATC() searches for the first occurrence of @cSearchExpression (@nOccurrence = 1). Including @nOccurrence lets you search for additional occurrences of @cSearchExpression in @cExpressionSearched.
-- ATC( ) returns 0 if @nOccurrence is greater than the number of times @cSearchExpression occurs in @cExpressionSearched. 
-- Remarks
-- ATC() searches the second character expression for the first occurrence of the first character expression,
-- without concern for the case (upper or lower) of the characters in either expression. Use AT( ) to perform a case-sensitive search.
-- It then returns an integer indicating the position of the first character in the character expression found. If the character expression is not found, ATC() returns 0. 
-- ATC is nearly similar to a function Oracle PL/SQL INSTR
-- Example
-- declare @gcString nvarchar(4000), @gcFindString nvarchar(4000)
-- select @gcString = 'Now is the time for all good men', @gcFindString = 'is the'
-- select dbo.ATC(@gcFindString, @gcString, default)  -- Displays 5
-- set @gcFindString = 'IS'
-- select dbo.ATC(@gcFindString, @gcString, default)  -- Displays 5, case-insensitive
-- See Also AT()  User-Defined Function 
-- UDF the name and functionality of which correspond  to the  Visual FoxPro function  
CREATE function ATC  (@cSearchExpression nvarchar(4000), @cExpressionSearched  nvarchar(4000), @nOccurrence  smallint = 1 )
returns smallint
as
    begin
      if @nOccurrence > 0
         begin
            declare @i smallint,  @StartingPosition  smallint
            select  @i = 0, @StartingPosition  = -1
            select  @cSearchExpression = lower(@cSearchExpression), @cExpressionSearched = lower(@cExpressionSearched)
            while  @StartingPosition <> 0 and @nOccurrence > @i
               select  @i = @i + 1, @StartingPosition  = charindex(@cSearchExpression COLLATE Latin1_General_CI_AS, @cExpressionSearched COLLATE Latin1_General_CI_AS,  @StartingPosition+1 )
         end
      else
         set @StartingPosition =  NULL

     return @StartingPosition
    end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
-- RATC( ) User-Defined Function
-- Returns the numeric position of the last (rightmost) occurrence of a character string within another character string.
-- The search performed by RATC() is case-insensitive (including  overlaps). 
-- RATC(@cSearchExpression, @cExpressionSearched [, @nOccurrence])
-- Return Values smallint 
-- Parameters
-- @cSearchExpression nvarchar(4000) Specifies the character expression that RATC( ) looks for in @cExpressionSearched. 
-- @cExpressionSearched nvarchar(4000) Specifies the character expression that RATC() searches. 
-- @nOccurrence smallint Specifies which occurrence, starting from the right and moving left, of @cSearchExpression RATC() searches for in @cExpressionSearched. By default, RATC() searches for the last occurrence of @cSearchExpression (@nOccurrence = 1). If @nOccurrence is 2, RATC() searches for the next to last occurrence, and so on. 
-- Remarks
-- RATC(), the reverse of the ATC() function, searches the character expression in @cExpressionSearched starting from the right and moving left, looking for the last occurrence of the string specified in @cSearchExpression.
-- RATC() returns an integer indicating the position of the first character in @cSearchExpression in @cExpressionSearched. RATC() returns 0 if @cSearchExpression is not found in @cExpressionSearched, or if @nOccurrence is greater than the number of times @cSearchExpression occurs in @cExpressionSearched.
-- Example
-- declare @gcString nvarchar(4000), @gcFindString nvarchar(4000)
-- select @gcString = 'abracadabra', @gcFindString = 'A' 
-- select dbo.RATC(@gcFindString , @gcString, default)  -- Displays 11
-- select dbo.RATC(@gcFindString , @gcString , 3)       -- Displays 6
-- See Also ATC()  User-Defined Function 
-- UDF the name and functionality of which correspond  to the  Visual FoxPro function     
CREATE function RATC  (@cSearchExpression nvarchar(4000), @cExpressionSearched  nvarchar(4000), @nOccurrence  smallint = 1 )
returns smallint
as
    begin
      if @nOccurrence > 0
         begin
            declare @i smallint, @length smallint, @StartingPosition  smallint
            select  @length  = datalength(@cExpressionSearched)/(case SQL_VARIANT_PROPERTY(@cExpressionSearched,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
            select  @cSearchExpression = lower(reverse(@cSearchExpression)), @cExpressionSearched = lower(reverse(@cExpressionSearched))
            select  @i = 0, @StartingPosition  = -1 
            while @StartingPosition <> 0 and @nOccurrence > @i
               select  @i = @i + 1, @StartingPosition  = charindex(@cSearchExpression  COLLATE Latin1_General_CI_AS,
                                                                   @cExpressionSearched  COLLATE Latin1_General_CI_AS, @StartingPosition + 1)
            if @StartingPosition <> 0
              select @StartingPosition = 2 - @StartingPosition +  @length - datalength(@cSearchExpression)/(case SQL_VARIANT_PROPERTY(@cSearchExpression,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
         end
      else
         set @StartingPosition =  NULL

     return @StartingPosition
    end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- ATC2() User-Defined Function 
-- Returns the beginning numeric position of the first occurrence of a character expression within another character expression, counting from the leftmost character.
-- The search performed by ATC2() is case-insensitive (excluding  overlaps).
-- ATC2(@cSearchExpression, @cExpressionSearched [, @nOccurrence]) Return Values smallint 
-- Parameters
-- @cSearchExpression nvarchar(4000) Specifies the character expression that ATC2( ) searches for in @cExpressionSearched. 
-- @cExpressionSearched nvarchar(4000) Specifies the character expression @cSearchExpression searches for. 
-- @nOccurrence smallint Specifies which occurrence (first, second, third, and so on) of @cSearchExpression is searched for in @cExpressionSearched. By default, ATC2() searches for the first occurrence of @cSearchExpression (@nOccurrence = 1). Including @nOccurrence lets you search for additional occurrences of @cSearchExpression in @cExpressionSearched.
-- ATC2() returns 0 if @nOccurrence is greater than the number of times @cSearchExpression occurs in @cExpressionSearched. 
-- Remarks
-- ATC2() searches the second character expression for the first occurrence of the first character expression. It then returns an integer indicating the position of the first character in the character expression found. If the character expression is not found, ATC2() returns 0. 
-- ATC2 is nearly similar to a function Oracle PL/SQL INSTR
-- Example
-- declare @gcString nvarchar(4000), @gcFindString nvarchar(4000)
-- select @gcString = 'Now is the time for all good men', @gcFindString = 'is the'
-- select dbo.ATC2(@gcFindString, @gcString, default)  -- Displays 5
-- set @gcFindString = 'IS'
-- select dbo.ATC2(@gcFindString, @gcString, default)  -- Displays 5, case-insensitive
-- select @gcString = 'goood men', @gcFindString = 'oo'
-- select dbo.ATC2(@gcFindString, @gcString, 1)  -- Displays 2
-- select dbo.ATC2(@gcFindString, @gcString, 2)  -- Displays 0, excluding  overlaps
-- See Also AT(), AT2(), ATC2()  User-Defined Function 
CREATE function ATC2  (@cSearchExpression nvarchar(4000), @cExpressionSearched  nvarchar(4000), @nOccurrence  smallint = 1 )
returns smallint
as
    begin
      declare  @LencSearchExpression smallint
      select    @LencSearchExpression  = datalength(@cSearchExpression)/(case SQL_VARIANT_PROPERTY(@cSearchExpression,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

      if @nOccurrence > 0
         begin
            declare @i smallint,  @StartingPosition  smallint
            select  @i = 0, @StartingPosition  = -1 - @LencSearchExpression
            select  @cSearchExpression = lower(@cSearchExpression), @cExpressionSearched = lower(@cExpressionSearched)
            while  @StartingPosition <> 0 and @nOccurrence > @i
               select  @i = @i + 1, @StartingPosition  = charindex(@cSearchExpression COLLATE Latin1_General_CI_AS, @cExpressionSearched COLLATE Latin1_General_CI_AS,  @StartingPosition + @LencSearchExpression)
         end
      else
         set @StartingPosition =  NULL

 return @StartingPosition
 end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
 -- PADL(), PADR(), PADC() User-Defined Functions
 -- Returns a string from an expression, padded with spaces or characters to a specified length on the left or right sides, or both.
 -- PADL(@eExpression, @nResultSize [, @cPadCharacter]) -Or-
 -- PADR(@eExpression, @nResultSize [, @cPadCharacter]) -Or-
 -- PADC(@eExpression, @nResultSize [, @cPadCharacter])
 -- Return Values nvarchar(4000)
 -- Parameters
 -- @eExpression nvarchar(4000) Specifies the expression to be padded.
 -- @nResultSize  smallint Specifies the total number of characters in the expression after it is padded. 
 -- @cPadCharacter nvarchar(4000) Specifies the value to use for padding. This value is repeated as necessary to pad the expression to the specified number of characters. 
 -- If you omit @cPadCharacter, spaces (ASCII character 32) are used for padding. 
 -- Remarks
 -- PADL() inserts padding on the left, PADR() inserts padding on the right, and PADC() inserts padding on both sides.
 -- Example
 -- declare @gcString  nvarchar(4000)
 -- select @gcString  = 'TITLE' 
 -- select dbo.PADL(@gcString, 40, default)
 -- select dbo.PADL(@gcString, 40, '=!=')
 -- select dbo.PADR(@gcString, 40, '=+=')
 -- select dbo.PADC(@gcString, 40, '=~')  
 -- UDF the name and functionality of which correspond  to the  Visual FoxPro function   
CREATE function PADC  (@cString nvarchar(4000), @nLen smallint, @cPadCharacter nvarchar(4000) = ' ' )
returns nvarchar(4000)
as
  begin
        declare @length smallint, @lengthPadCharacter smallint
        if @cPadCharacter is NULL or  datalength(@cPadCharacter) = 0
           set @cPadCharacter = space(1) 
        select  @length  = datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
        select  @lengthPadCharacter  = datalength(@cPadCharacter)/(case SQL_VARIANT_PROPERTY(@cPadCharacter,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
           
  	    if @length >= @nLen
	       set  @cString = left(@cString, @nLen)
 	    else
 	      begin
              declare @nLeftLen smallint, @nRightLen smallint
              set @nLeftLen = (@nLen - @length )/2            -- Quantity of characters, added at the left
              set @nRightLen  =  @nLen - @length - @nLeftLen  -- Quantity of characters, added on the right
              set @cString = left(replicate(@cPadCharacter, ceiling(@nLeftLen/@lengthPadCharacter) + 2), @nLeftLen)+ @cString + left(replicate(@cPadCharacter, ceiling(@nRightLen/@lengthPadCharacter) + 2), @nRightLen)
	      end

     return (@cString)
  end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
 -- PADL(), PADR(), PADC() User-Defined Functions
 -- Returns a string from an expression, padded with spaces or characters to a specified length on the left or right sides, or both.
 -- PADL similar to the Oracle function PL/SQL  LPAD 
CREATE function PADL  (@cString nvarchar(4000), @nLen smallint, @cPadCharacter nvarchar(4000) = ' ' )
returns nvarchar(4000)
as
  begin
        declare @length smallint, @lengthPadCharacter smallint
        if @cPadCharacter is NULL or  datalength(@cPadCharacter) = 0
           set @cPadCharacter = space(1) 
        select  @length = datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
        select  @lengthPadCharacter = datalength(@cPadCharacter)/(case SQL_VARIANT_PROPERTY(@cPadCharacter,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

        if @length >= @nLen
           set  @cString = left(@cString, @nLen)
        else
	       begin
              declare @nLeftLen smallint
              set @nLeftLen = @nLen - @length  -- Quantity of characters, added at the left
              set @cString = left(replicate(@cPadCharacter, ceiling(@nLeftLen/@lengthPadCharacter) + 2), @nLeftLen)+ @cString
           end

    return (@cString)
  end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
 -- PADL(), PADR(), PADC() User-Defined Functions
 -- Returns a string from an expression, padded with spaces or characters to a specified length on the left or right sides, or both.
 -- PADR similar to the Oracle function PL/SQL RPAD 
CREATE function PADR  (@cString nvarchar(4000), @nLen smallint, @cPadCharacter nvarchar(4000) = ' ' )
returns nvarchar(4000)
as
   begin
       declare @length smallint, @lengthPadCharacter smallint
       if @cPadCharacter is NULL or  datalength(@cPadCharacter) = 0
          set @cPadCharacter = space(1) 
       select  @length  = datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode
       select  @lengthPadCharacter  = datalength(@cPadCharacter)/(case SQL_VARIANT_PROPERTY(@cPadCharacter,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

       if @length >= @nLen
          set  @cString = left(@cString, @nLen)
       else
          begin
             declare  @nRightLen smallint
             set @nRightLen  =  @nLen - @length -- Quantity of characters, added on the right
             set @cString =  @cString + left(replicate(@cPadCharacter, ceiling(@nRightLen/@lengthPadCharacter) + 2), @nRightLen)
	  end

     return (@cString)
    end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- CHRTRAN() User-Defined Function
-- Replaces each character in a character expression that matches a character in a second character expression with the corresponding character in a third character expression.
-- CHRTRAN  (@cExpressionSearched,   @cSearchExpression,  @cReplacementExpression)
-- Return Values nvarchar(4000) 
-- Parameters
-- @cSearchedExpression   Specifies the expression in which CHRTRAN( ) replaces characters. 
-- @cSearchExpression  Specifies the expression containing the characters CHRTRAN( ) looks for in @cSearchedExpression. 
-- @cReplacementExpression Specifies the expression containing the replacement characters. 
-- If a character in@cSearchExpression is found in @cSearchedExpression, the character in @cSearchedExpression is replaced by a character from @cReplacementExpression
-- that is in the same position in @cReplacementExpression as the respective character in @cSearchExpression. 
-- If @cReplacementExpression has fewer characters than @cSearchExpression, the additional characters in @cSearchExpression are deleted from @cSearchedExpression. 
-- If @cReplacementExpression has more characters than @cSearchExpression, the additional characters in @cReplacementExpression are ignored. 
-- Remarks
-- CHRTRAN() translates the character expression @cSearchedExpression using the translation expressions @cSearchExpression and @cReplacementExpression and returns the resulting character string.
-- CHRTRAN similar to the Oracle function PL/SQL TRANSLATE
-- Example
-- select dbo.CHRTRAN('ABCDEF', 'ACE', 'XYZ')      -- Displays XBYDZF
-- select dbo.CHRTRAN('ABCDEF', 'ACE', 'XYZQRST')  -- Displays XBYDZF
-- See Also STRFILTER()  
-- UDF the name and functionality of which correspond  to the  Visual FoxPro function  
CREATE function CHRTRAN (@cExpressionSearched nvarchar(4000),   @cSearchExpression nvarchar(256),  @cReplacementExpression nvarchar(256))
returns  nvarchar(4000)
as
    begin
      declare @lenExpressionSearched smallint, @lenSearchExpression smallint, @lenReplacementExpression smallint,
              @i  smallint,  @j  smallint,  @flag bit, @cExpressionTranslated  nvarchar(4000)
      
      select  @cExpressionTranslated = N'',  @i = 1, @flag = 0, 
              @lenExpressionSearched =  datalength(@cExpressionSearched)/(case SQL_VARIANT_PROPERTY(@cExpressionSearched,'BaseType') when 'nvarchar' then 2  else 1 end),
              @lenSearchExpression =  datalength(@cSearchExpression)/(case SQL_VARIANT_PROPERTY(@cSearchExpression,'BaseType') when 'nvarchar' then 2  else 1 end),
              @lenReplacementExpression =  datalength(@cReplacementExpression)/(case SQL_VARIANT_PROPERTY(@cReplacementExpression,'BaseType') when 'nvarchar' then 2  else 1 end)  -- for unicode
    
     if @lenReplacementExpression > @lenSearchExpression
         select @cReplacementExpression = left(@cReplacementExpression, @lenSearchExpression),   @lenReplacementExpression = @lenSearchExpression

     while @i  <=   @lenReplacementExpression
       if  charindex(substring(@cReplacementExpression, @i, 1) COLLATE Latin1_General_BIN, @cSearchExpression COLLATE Latin1_General_BIN, @i + 1) > 0
         begin
            select  @flag = 1
            break
         end
       else
         select @i = @i + 1

   if @lenExpressionSearched = 4000  -- built-in function replace do not works correctly if the length of the string is 4000     MS SQL Server 2000, SP4
      if  charindex(right(@cExpressionSearched,1) COLLATE Latin1_General_BIN, @cSearchExpression COLLATE Latin1_General_BIN) > 0     -- I did run this example and validated the erroneous result
           select  @flag = 1                                                              -- select right(replace(replicate(N'z',3999)+N'i', N'i', N'B'),1) -- Displays i  but this is incorrect result, correct result is B

   select @i = 1

    if @flag  =  0
       -- this algorithm does not work always as
       -- select dbo.CHRTRAN('eaba','ba','a') -- Displays  e  Error !!!  ea  Correctly
       begin
          while @i  <=   @lenSearchExpression
            select  @cExpressionSearched = replace(@cExpressionSearched  COLLATE Latin1_General_BIN, 
                                                   substring(@cSearchExpression, @i, 1)   COLLATE Latin1_General_BIN,
                                                   substring(@cReplacementExpression, @i, 1)   COLLATE Latin1_General_BIN ),
                    @i =  @i + 1 
          select  @cExpressionTranslated = @cExpressionSearched
       end
    else
       while @i  <=   @lenExpressionSearched
          begin
             select @j  =  charindex(substring(@cExpressionSearched, @i, 1) COLLATE Latin1_General_BIN, @cSearchExpression COLLATE Latin1_General_BIN)
               if  @j  > 0
                   select  @cExpressionTranslated = @cExpressionTranslated + substring(@cReplacementExpression, @j , 1)
               else
                  select  @cExpressionTranslated = @cExpressionTranslated + substring(@cExpressionSearched, @i, 1)
             select   @i =  @i + 1 
          end

   return  @cExpressionTranslated
  end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- STRFILTER() User-Defined Function
-- Removes all characters from a string except those specified.
-- STRFILTER(@cExpressionSearched, @cSearchExpression)
-- Return Values nvarchar(4000)
-- Parameters
-- @cExpressionSearched  Specifies the character string to search.
-- @cSearchExpression Specifies the characters to search for and retain in @cExpressionSearched.
-- Remarks
-- STRFILTER( ) removes all the characters from @cExpressionSearched that are not in @cSearchExpression, then returns the characters that remain.
-- Example
-- select dbo.STRFILTER('asdfghh5hh1jk6f3b7mn8m3m0m6','0123456789')   -- Displays 516378306
-- select dbo.STRFILTER('ABCDABCDABCD', 'AB')   -- Displays ABABAB
-- See Also CHRTRAN()  
-- UDF the name and functionality of which correspond  to the Foxtools function ( Foxtools is a Visual FoxPro API library) 
CREATE function STRFILTER  (@cExpressionSearched nvarchar(4000),   @cSearchExpression nvarchar(256))
returns  nvarchar(4000)
as
    begin
      declare @len smallint,  @i  smallint, @StrFiltred  nvarchar(4000)
      select  @StrFiltred = N'', @i = 1,  @len =  datalength(@cExpressionSearched)/(case SQL_VARIANT_PROPERTY(@cExpressionSearched,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

      while  @i  <=  @len
          begin
               if charindex(substring(@cExpressionSearched, @i, 1) COLLATE Latin1_General_BIN, @cSearchExpression COLLATE Latin1_General_BIN) > 0
                     select  @StrFiltred = @StrFiltred + substring(@cExpressionSearched, @i, 1)
               select @i  =   @i  + 1
          end

     return  @StrFiltred
    end
GO


-- this is auxiliary function (for STRTRAN(), WORDTRAN())
-- returns 'U' if the string has a UPPER case
-- returns 'L' if the string has a lower case
-- returns 'P' if the string has a Proper case
-- else this function returns space(1)
CREATE function [dbo].[FWHATISCASE]    (@cStr nvarchar(4000))
  returns char(1)
as
begin
  declare @caseofstr char(1), @LenStr smallint, @j smallint
  select @LenStr  = datalength(@cStr)/(case SQL_VARIANT_PROPERTY(@cStr,'BaseType') when 'nvarchar' then 2  else 1 end)  -- for unicode

  select @caseofstr =  case
     when  charindex(lower(@cStr) COLLATE Latin1_General_BIN, upper(@cStr) COLLATE Latin1_General_BIN ) = 1  -- no letters in string
       then  space(1)
     when  charindex(@cStr COLLATE Latin1_General_BIN, upper(@cStr) COLLATE Latin1_General_BIN ) = 1
       then 'U'
     when   charindex(@cStr COLLATE Latin1_General_BIN, lower(@cStr) COLLATE Latin1_General_BIN ) = 1
       then  'L'
     else null  end

   if  @caseofstr is null
     if  charindex(upper(substring( @cStr, 1, 1) ) COLLATE Latin1_General_BIN, lower(substring( @cStr, 1, 1) ) COLLATE Latin1_General_BIN ) <> 1  and
         charindex(upper(substring( @cStr, 1, 1) ) COLLATE Latin1_General_BIN, substring( @cStr, 1, 1) COLLATE Latin1_General_BIN ) = 1   and
         charindex(lower(substring( @cStr, 2,  @LenStr - 1) ) COLLATE Latin1_General_BIN, substring( @cStr, 2,  @LenStr - 1) COLLATE Latin1_General_BIN ) = 1 
            select @caseofstr  =  'P'
      else
         begin
            select @j = 1
            while @j   <=  @LenStr
              begin
                if charindex(upper(substring( @cStr, @j, 1) ) COLLATE Latin1_General_BIN, lower(substring( @cStr, @j, 1) ) COLLATE Latin1_General_BIN ) <> 1    -- this is letter
                  begin
                    if charindex(substring( @cStr, @j, 1) COLLATE Latin1_General_BIN, lower(substring( @cStr, @j, 1) ) COLLATE Latin1_General_BIN ) = 1
                      select @caseofstr  =  'L'
                    else
                      select @caseofstr  =  space(1)
                    break
                  end
                 select @j = @j + 1
               end
           end

        if  @caseofstr is null
             select @caseofstr  =  space(1)

return @caseofstr
end

GO


-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- STRTRAN() User-Defined Function
-- Searches a character expression for occurrences of a second character expression,
-- and then replaces each occurrence with a third character expression.
-- STRTRAN  (@cSearched, @cExpressionSought , [@cReplacement]
-- [, @nStartOccurrence] [, @nNumberOfOccurrences] [, @nFlags])
-- Return Values nvarchar(4000) 
-- Parameters
-- @cSearched         Specifies the character expression that is searched.
-- @cExpressionSought Specifies the character expression that is searched for in @cSearched.
-- @cReplacement      Specifies the character expression that replaces every occurrence of @cExpressionSought in @cSearched.
-- If you omit @cReplacement, every occurrence of @cExpressionSought is replaced with the empty string. 
-- @nStartOccurrence  Specifies which occurrence of @cExpressionSought is the first to be replaced.
-- For example, if @nStartOccurrence is 4, replacement begins with the fourth occurrence of @cExpressionSought in @cSearched and the first three occurrences of @cExpressionSought remain unchanged.
-- The occurrence where replacement begins defaults to the first occurrence of @cExpressionSought if you omit @nStartOccurrence. 
-- @nNumberOfOccurrences  Specifies the number of occurrences of @cExpressionSought to replace.
-- If you omit @nNumberOfOccurrences, all occurrences of @cExpressionSought, starting with the occurrence specified with @nStartOccurrence, are replaced. 
-- @nFlags  Specifies the case-sensitivity of a search according to the following values:
---------------------------------------------------------------------------------------------------------------------------------------             
-- @nFlags     Description 
-- 0 (default) Search is case-sensitive, replace is with exact @cReplacement string.
-- 1           Search is case-insensitive, replace is with exact @cReplacement string. 
-- 2           Search is case-sensitive; replace is with the case of @cReplacement changed to match the case of the string found.
--             The case of @cReplacement will only be changed if the string found is all uppercase, lowercase, or proper case. 
-- 3           Search is case-insensitive; replace is with the case of @cReplacement changed to match the case of the string found.
--             The case of @cReplacement will only be changed if the string found is all uppercase, lowercase, or proper case. 
---------------------------------------------------------------------------------------------------------------------------------------             
-- Remarks
-- You can specify where the replacement begins and how many replacements are made.
-- STRTRAN( ) returns the resulting character string. 
-- Specify –1 for optional parameters you want to skip over if you just need to specify the @nFlags setting.
-- Example
-- select dbo.STRTRAN('ABCDEF', 'ABC', 'XYZ',-1,-1,0)      -- Displays XYZDEF
-- select dbo.STRTRAN('ABCDEF', 'ABC', default,-1,-1,0)    -- Displays DEF
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', default,2,-1,0)      -- Displays ABCDEFGHJabcQWE
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', default,2,-1,1)      -- Displays ABCDEFGHJQWE
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', 'XYZ',  2, 1, 1)      -- Displays ABCDEFXYZGHJabcQWE
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', 'XYZ',  2, 3, 1)      -- Displays ABCDEFXYZGHJXYZQWE
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', 'XYZ',  2, 1, 2)      -- Displays ABCDEFXYZGHJabcQWE
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', 'XYZ',  2, 3, 2)      -- Displays ABCDEFXYZGHJabcQWE
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', 'xyZ',  2, 1, 2)      -- Displays ABCDEFXYZGHJabcQWE
-- select dbo.STRTRAN('ABCDEFABCGHJabcQWE', 'ABC', 'xYz',  2, 3, 2)      -- Displays ABCDEFXYZGHJabcQWE
-- select dbo.STRTRAN('ABCDEFAbcCGHJAbcQWE', 'Aab', 'xyZ',  2, 1, 2)     -- Displays ABCDEFAbcCGHJAbcQWE
-- select dbo.STRTRAN('abcDEFabcGHJabcQWE', 'abc', 'xYz',  2, 3, 2)      -- Displays abcDEFxyzGHJxyzQWE
-- select dbo.STRTRAN('ABCDEFAbcCGHJAbcQWE', 'Aab', 'xyZ',  2, 1, 3)     -- Displays ABCDEFAbcCGHJAbcQWE
-- select dbo.STRTRAN('ABCDEFAbcGHJabcQWE', 'abc', 'xYz',  1, 3, 3)      -- Displays XYZDEFXyzGHJxyzQWE
-- See Also replace(), CHRTRAN()  
-- UDF the name and functionality of which correspond  to the  Visual FoxPro function  
CREATE FUNCTION [dbo].[STRTRAN] 
   (@cSearched nvarchar(4000),  @cExpressionSought nvarchar(4000), @cReplacement nvarchar(4000) = N'',
     @nStartOccurrence smallint = -1, @nNumberOfOccurrences smallint = -1, @nFlags tinyint = 0)
returns nvarchar(4000)
as
begin 
   declare @StartPart nvarchar(4000),  @FinishPart nvarchar(4000),  @nAtStartOccurrence smallint, @nAtFinishOccurrence smallint, @LencSearched smallint,  @LenExpressionSought smallint

   select   @StartPart = N'',  @FinishPart = N'',   @nAtStartOccurrence = 0, @nAtFinishOccurrence = 0,
            @LencSearched  = datalength(@cSearched)/(case SQL_VARIANT_PROPERTY(@cSearched,'BaseType') when 'nvarchar' then 2  else 1 end) ,
            @LenExpressionSought  = datalength(@cExpressionSought)/(case SQL_VARIANT_PROPERTY(@cExpressionSought,'BaseType') when 'nvarchar' then 2  else 1 end)  -- for unicode

if @nStartOccurrence = -1
  select @nStartOccurrence = 1

if @nFlags in ( 0, 2)
    select  @nAtStartOccurrence = dbo.AT2( @cExpressionSought,  @cSearched, @nStartOccurrence), 
               @nAtFinishOccurrence = case  @nNumberOfOccurrences  when -1 then 0  else   dbo.AT2( @cExpressionSought,  @cSearched, @nStartOccurrence + @nNumberOfOccurrences ) end
else
if @nFlags in (1, 3)
    select  @nAtStartOccurrence = dbo.ATC2( @cExpressionSought,  @cSearched, @nStartOccurrence), 
               @nAtFinishOccurrence = case  @nNumberOfOccurrences  when -1 then 0  else   dbo.ATC2( @cExpressionSought,  @cSearched, @nStartOccurrence + @nNumberOfOccurrences ) end
else 
  select @cSearched  =  'Error, sixth parameter must be 0, 1, 2, 3 ! '

   if @nAtStartOccurrence > 0
      begin
         select @StartPart = left(@cSearched, @nAtStartOccurrence - 1)
           if  @nAtFinishOccurrence  > 0
                select @FinishPart =  right(@cSearched,  @LencSearched - @nAtFinishOccurrence + 1) , @cSearched = substring(@cSearched,  @nAtStartOccurrence, @nAtFinishOccurrence - @nAtStartOccurrence )
           else           
                select  @cSearched = substring(@cSearched,  @nAtStartOccurrence, @LencSearched - @nAtStartOccurrence + 1)
     
          if @nFlags = 0 or (@nFlags = 2 and datalength(@cReplacement) = 0)
               select  @cSearched  = replace(@cSearched  COLLATE Latin1_General_BIN, 
                                                                 @cExpressionSought   COLLATE Latin1_General_BIN,
                                                                 @cReplacement   COLLATE Latin1_General_BIN   )  
          else
                if @nFlags = 1 or (@nFlags = 3 and datalength(@cReplacement) = 0)
                     select  @cSearched  = replace(@cSearched  COLLATE Latin1_General_CI_AS, 
                                                                        @cExpressionSought   COLLATE Latin1_General_CI_AS,
                                                                        @cReplacement   COLLATE Latin1_General_CI_AS  ) 
               else
                      if @nFlags in (2,  3)
                            begin
                               declare @cNewSearched  nvarchar(4000),  @cNewReplacement  nvarchar(4000), @LenReplacement smallint, 
                                       @nAtPreviousOccurrence smallint, @occurs2 smallint, @i smallint, @j smallint

                               select @i = 1,  @cNewSearched = N'',  @nAtPreviousOccurrence =  1,
                                      @LencSearched   = datalength(@cSearched)/(case SQL_VARIANT_PROPERTY(@cSearched,'BaseType') when 'nvarchar' then 2  else 1 end),
                                      @LenReplacement = datalength(@cReplacement)/(case SQL_VARIANT_PROPERTY(@cReplacement,'BaseType') when 'nvarchar' then 2  else 1 end),
                                      @nAtStartOccurrence =  1 - @LenExpressionSought, 
                                      @occurs2 = case  when  @nFlags = 3
                                                       then   ( datalength(@cSearched)
                                                                 - datalength(replace(@cSearched  COLLATE Latin1_General_CI_AS, 
                                                                  @cExpressionSought   COLLATE Latin1_General_CI_AS,  N'')))  / datalength(@cExpressionSought)
                                                       else  dbo.OCCURS2( @cExpressionSought,  @cSearched)  end

                               if @nFlags = 2
                                     select @cNewReplacement = case  dbo.FWHATISCASE(@cExpressionSought)
                                                               when  'U' then upper(@cReplacement)
                                                               when  'L' then lower(@cReplacement)
                                                               when  'P' then upper(left(@cReplacement,1))+lower(substring(@cReplacement, 2, @LenReplacement - 1))
                                                               else @cReplacement  end
                                   while @i  <= @occurs2
                                        begin
                                              select  @nAtStartOccurrence = case 
                                                                            when  @nFlags = 3
                                                                            then  charindex(@cExpressionSought COLLATE Latin1_General_CI_AS, @cSearched COLLATE Latin1_General_CI_AS,  @nAtStartOccurrence + @LenExpressionSought)
                                                                            else  charindex(@cExpressionSought COLLATE Latin1_General_BIN, @cSearched COLLATE Latin1_General_BIN,  @nAtStartOccurrence + @LenExpressionSought ) end
                                              select @cNewSearched  = @cNewSearched + case
                                                                           when @i > 1
                                                                           then  substring(@cSearched,  @nAtPreviousOccurrence + @LenExpressionSought, @nAtStartOccurrence  - (@nAtPreviousOccurrence + @LenExpressionSought) )  
                                                                           else  left(@cSearched, @nAtStartOccurrence - 1)  end 
                                               if @nFlags = 3
                                                   select @cNewReplacement = case  dbo.FWHATISCASE(substring(@cSearched,  @nAtStartOccurrence, @LenExpressionSought))
                                                                             when  'U' then upper(@cReplacement)
                                                                             when  'L' then lower(@cReplacement)
                                                                             when  'P' then upper(left(@cReplacement,1))+lower(substring(@cReplacement, 2, @LenReplacement - 1))
                                                                             else @cReplacement  end

                                              select @cNewSearched  = @cNewSearched + @cNewReplacement, @nAtPreviousOccurrence = @nAtStartOccurrence, @i = @i + 1
                                        end
                                  select  @cSearched = @cNewSearched  +  right(@cSearched,  @LencSearched +1 - (@nAtStartOccurrence + @LenExpressionSought) )
                            end
   end
  return  @StartPart + @cSearched + @FinishPart
end

GO


 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
 -- OCCURS() User-Defined Function
 -- Returns the number of times a character expression occurs within another character expression (including  overlaps).
 -- OCCURS is slowly than OCCURS2.
 -- OCCURS(@cSearchExpression, @cExpressionSearched)
 -- Return Values smallint 
 -- Parameters
 -- @cSearchExpression nvarchar(4000) Specifies a character expression that OCCURS() searches for within @cExpressionSearched. 
 -- @cExpressionSearched nvarchar(4000) Specifies the character expression OCCURS() searches for @cSearchExpression. 
 -- Remarks
 -- OCCURS() returns 0 (zero) if @cSearchExpression is not found within @cExpressionSearched.
 -- Example
 -- declare @gcString nvarchar(4000)
 -- select  @gcString = 'abracadabra'
 -- select dbo.OCCURS('a', @gcString )  -- Displays 5
 -- select dbo.OCCURS('b', @gcString )  -- Displays 2
 -- select dbo.OCCURS('c', @gcString )  -- Displays 1
 -- select dbo.OCCURS('e', @gcString )  -- Displays 0
 -- Including  overlaps !!!
 -- select dbo.OCCURS('ABCA', 'ABCABCABCA') -- Displays 3
 -- 1 occurrence of substring 'ABCA  .. BCABCA' 
 -- 2 occurrence of substring 'ABC...ABCA...BCA' 
 -- 3 occurrence of substring 'ABCABC...ABCA' 
 -- See Also AT(), RAT(), OCCURS2(), AT2(), ATC(), ATC2()    
 -- UDF the name and functionality of which correspond to the  Visual FoxPro function  
 -- (but function OCCURS of Visual FoxPro counts the 'occurs' excluding  overlaps !)
CREATE function OCCURS  (@cSearchExpression nvarchar(4000), @cExpressionSearched nvarchar(4000))
returns smallint
as
    begin
      declare @start_location smallint,  @occurs  smallint
      select  @start_location = -1,   @occurs = -1
      while @start_location <> 0
          select  @occurs = @occurs + 1,  @start_location  = charindex(@cSearchExpression COLLATE Latin1_General_BIN, @cExpressionSearched COLLATE Latin1_General_BIN, @start_location+1)

     return @occurs
    end
GO

 -- Author: Stephen Dobson, Toronto, EMail: sdobson@acc.org   
 -- OCCURS2() User-Defined Function
 -- Returns the number of times a character expression occurs  within another character expression (excluding  overlaps).
 -- OCCURS2 is faster than OCCURS.
 -- OCCURS2(@cSearchExpression, @cExpressionSearched)
 -- Return Values smallint 
 -- Parameters
 -- @cSearchExpression nvarchar(4000) Specifies a character expression that OCCURS2() searches for within @cExpressionSearched. 
 -- @cExpressionSearched nvarchar(4000) Specifies the character expression OCCURS2() searches for @cSearchExpression. 
 -- Remarks
 -- OCCURS2() returns 0 (zero) if @cSearchExpression is not found within @cExpressionSearched.
 -- Example
 -- declare @gcString nvarchar(4000)
 -- select  @gcString = 'abracadabra'
 -- select dbo.OCCURS2('a', @gcString )  -- Displays 5
 -- Attention !!!
 -- This function counts the 'occurs' excluding  overlaps !
 -- select dbo.OCCURS2('ABCA', 'ABCABCABCA') -- Displays 2
 -- 1 occurrence of substring 'ABCA  .. BCABCA' 
 -- 2 occurrence of substring 'ABCABC... ABCA' 
 -- UDF the functionality of which correspond to the  Visual FoxPro function 
 -- See Also OCCURS()  
CREATE function OCCURS2  (@cSearchExpression nvarchar(4000), @cExpressionSearched nvarchar(4000))
returns smallint
as
    begin
         return
           case  
              when datalength(@cSearchExpression) > 0
              then   ( datalength(@cExpressionSearched) 
                   - datalength(replace(@cExpressionSearched  COLLATE Latin1_General_BIN, 
                                         @cSearchExpression   COLLATE Latin1_General_BIN,  '')))  
                  / datalength(@cSearchExpression)
             else 0 
          end
    end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
 -- PROPER( ) User-Defined Function
 -- Returns from a character expression a string capitalized as appropriate for proper names.
 -- PROPER(@cExpression)
 -- Return Values nvarchar(4000)
 -- Parameters
 -- @cExpression nvarchar(4000) Specifies the character expression from which PROPER( ) returns a capitalized character string. 
 -- Example
 -- declare @gcExpr1 nvarchar(4000), @gcExpr2 nvarchar(4000)
 -- select @gcExpr1 = 'Visual Basic.NET', @gcExpr2 = 'VISUAL BASIC.NET'
 -- select dbo.PROPER(@gcExpr1)  -- Displays 'Visual Basic.net'
 -- select dbo.PROPER(@gcExpr2)  -- Displays 'Visual Basic.net'
 -- Remarks
 -- PROPER similar to the Oracle function PL/SQL  INITCAP 
 -- UDF the name and functionality of which correspond  to the  Visual FoxPro function  
CREATE function PROPER  (@expression nvarchar(4000))
returns nvarchar(4000)
as
    begin
      declare @i  smallint,   @properexpression nvarchar(4000),  @lenexpression  smallint, @flag bit, @symbol nchar(1)
      select  @flag = 1, @i = 1, @properexpression = '', @lenexpression =  datalength(@expression)/(case SQL_VARIANT_PROPERTY(@expression,'BaseType') when 'nvarchar' then 2  else 1 end) 

      while  @i <= @lenexpression
          begin
             select @symbol = lower(substring(@expression, @i, 1) )
               if @symbol in (nchar(32), nchar(9), nchar(10), nchar(11), nchar(12), nchar(13),  nchar(160)) and ascii(@symbol) <> 0
                   select @flag = 1
               else
                   if @flag = 1
                       select @symbol = upper(@symbol),  @flag = 0 
              select  @properexpression = @properexpression + @symbol,  @i = @i + 1 
          end

     return @properexpression 
    end
GO


-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- WORDTRAN() User-Defined Function
-- WORDTRAN (@cSearched, @cWordSought , [@cWordReplacement] [, @cDelimiters] [, @nStartOccurrence] [, @nNombreOccurrences] [, @nFlags])
-- Searches a character string for occurrences of a first word, and then replaces each occurrence with a second word.
-- Return Values nvarchar(4000)
-- Parameters
-- @cSearched Specifies the character string that is searched.
-- @cWordSought Specifies the word that is searched for in @cSearched.
-- @cWordReplacement Specifies the word that replaces every occurrence of @cWordSought  in @cSearched.
-- If you omit @cWordReplacement, every occurrence of @cWordSought is replaced with the empty string. 
-- @cDelimiters Optional. Specifies one or more optional characters used to separate words in cString. The default delimiters are space, tab, carriage return, and line feed.
-- The maximum length of lcDelimiters is 256.
-- Note that WORDTRAN() uses each of the characters in @cDelimiters as individual delimiters, not the entire string as a single delimiter.
-- @nStartOccurrence Specifies which occurrence of @cWordSought is the first to be replaced. For example, if @nStartOccurrence is 4, replacement begins with the fourth occurrence of @cWordSought in @cSearched and the first three occurrences of @cWordSought remain unchanged.
-- The occurrence where replacement begins defaults to the first occurrence of @cWordSought if you omit @nStartOccurrence. 
-- @nNombreOccurrences Specifies the number of occurrences of @cWordSought to replace.
-- If you omit @nNombreOccurrences, all occurrences of @cWordSought, starting with the occurrence specified with @nStartOccurrence, are replaced. 
-- @nFlags Specifies the case-sensitivity of a search according to the following values:
-- -----------------------------------------------------------------------------------------
-- @nFlags Description 
--  0 (default) Search is case-sensitive, replace is with exact @cWordReplacement word.
--  1 Search is case-insensitive, replace is with exact @cWordReplacement word. 
--  2 Search is case-sensitive; replace is with the case of @cWordReplacement changed to match the case of the word found. The case of @cWordReplacement will only be changed if the word found is all uppercase, lowercase, or proper case. 
--  3 Search is case-insensitive; replace is with the case of @cWordReplacement changed to match the case of the word found. The case of @cWordReplacement will only be changed if the word found is all uppercase, lowercase, or proper case. 
-- -----------------------------------------------------------------------------------------
--  Remarks
--  You can specify where the replacement begins and how many replacements are made.
--  WORDTRAN( ) returns the resulting character string. 
--  Specify default for optional parameters you want to skip over if you just need to specify the @nFlags setting.
-- Examples
-- select dbo.WORDTRAN('ABC DEF', 'ABC', 'XYZ', default, default, default, default)       -- Displays  XYZ DEF
-- select dbo.WORDTRAN('ABC DEF', 'ABC', '123', default, -1, -1, default)                 -- Displays  123 DEF
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', Space(1), default, default, 1, 0)  -- Displays    DEF ABC GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', Space(1), default, default, 1, 0)  -- Displays    DEF ABC GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', 'XYZ', Space(1), 2, 1, 1)          -- Displays  ABC DEF XYZ GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', 'XYZ', Space(1), 2, 3, 1)          -- Displays  ABC DEF XYZ GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', 'XYZ', Space(1), 2, 1, 2)          -- Displays  ABC DEF XYZ GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', 'XYZ', Space(1), 2, 3, 2)          -- Displays  ABC DEF XYZ GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', 'xyZ', Space(1), 2, 1, 2)          -- Displays  ABC DEF XYZ GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF ABC GHJabcQWE', 'ABC', 'xYz', Space(1), 2, 3, 2)          -- Displays  ABC DEF XYZ GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF Abc CGHJAbcQWE', 'Aab', 'xyZ', Space(1), 2, 1, 2 )        -- Displays  ABC DEF Abc CGHJAbcQWE
-- select dbo.WORDTRAN('abc DEF abc GHJabcQWE', 'abc', 'xYz', Space(1), 3, 2, 0)          -- Displays  abc DEF abc GHJabcQWE
-- select dbo.WORDTRAN('ABC DEF Abc CGHJAbcQWE', 'Aab', 'xyZ', Space(1), 2, 1, 3)         -- Displays  ABC DEF Abc CGHJAbcQWE
-- select dbo.WORDTRAN('ABC DEF Abc GHJabcQWE', 'abc', 'xYz', Space(1), 1, 3, 3)          -- Displays  XYZ DEF Xyz GHJabcQWE
-- Sometime the search for substring from a string will return the incorrect result,
-- it is necessary to search for the word in the string.
-- declare @lcAdmissibleCodes varchar(100), @lcCodeforReplacing varchar(100), @lcNewCode varchar(100)
-- select @lcAdmissibleCodes = 'W,WN,IR,IU,I', @lcCodeforReplacing = 'I', @lcNewCode = 'A'
-- select dbo.STRTRAN(@lcAdmissibleCodes, @lcCodeforReplacing, @lcNewCode, default, default, default)    -- Displays  W,WN,AR,AU,A
-- select dbo.WORDTRAN(@lcAdmissibleCodes, @lcCodeforReplacing, @lcNewCode, ','+Space(1), default, default, default ) --  Displays  W,WN,IR,IU,A
-- select @lcCodeforReplacing = 'IR,'
-- select dbo.WORDTRAN(@lcAdmissibleCodes, @lcCodeforReplacing, @lcNewCode, default, default, default, default)    --  Displays  W,WN,IR,IU,I
-- select dbo.WORDTRAN(@lcAdmissibleCodes, @lcCodeforReplacing, @lcNewCode, ','+Space(1), default, default, default )  --  Displays  W,WN,IR,IU,I
-- See Also STRTRAN(), GETATWORD(), GETRATWORD(), GETOCCURSWORD(), GETNUMWORD(), GETWORDNUM(), GETWORDCOUNT(), GETALLWORDS(), GETALLWORDS2() 
CREATE FUNCTION [dbo].[WORDTRAN] 
   (@cSearched nvarchar(4000),  @cWordSought nvarchar(4000), @cWordReplacement nvarchar(4000) = N'', @cDelimiters nvarchar(256),
     @nStartOccurrence smallint = -1, @nNumberOfOccurrences smallint = -1, @nFlags tinyint = 0)
returns nvarchar(4000)
as
begin 
if not @nFlags in ( 0, 2, 1, 3)
   return  'Error, seventh parameter must be 0, 1, 2, 3 ! '


declare @cStrWordReplaced nvarchar(4000), @cNewWordReplacement nvarchar(4000), @LenWordReplacement smallint, @EndPrevWord smallint

declare @k smallint, @nEndString smallint, @nLenWord smallint,
        @BegOfWord smallint,  @lnOccurrence smallint, @exit bit, @comparison bit 

   select  @EndPrevWord = 1, @BegOfWord = 1,  @lnOccurrence = 0, @exit = 0,  @cSearched =  isnull(@cSearched, ''),
           @k = 0, @cDelimiters = isnull(@cDelimiters, nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
           @nEndString = 1 + datalength(@cSearched)/(case SQL_VARIANT_PROPERTY(@cSearched,'BaseType') when 'nvarchar' then 2  else 1 end), -- for unicode,
           @nLenWord =  datalength(@cWordSought)/(case SQL_VARIANT_PROPERTY(@cWordSought,'BaseType') when 'nvarchar' then 2  else 1 end), -- for unicode
           @LenWordReplacement  = datalength(@cWordReplacement)/(case SQL_VARIANT_PROPERTY(@cWordReplacement,'BaseType') when 'nvarchar' then 2  else 1 end)

     select @cNewWordReplacement = @cWordReplacement,  @cStrWordReplaced = N''

 if @nStartOccurrence = -1
    select @nStartOccurrence = 1

 if @nFlags = 2 and @LenWordReplacement > 0
     select  @cNewWordReplacement  = case  dbo.FWHATISCASE(@cWordSought)
                                     when  'U' then upper(@cWordReplacement)
                                     when  'L' then lower(@cWordReplacement)
                                     when  'P' then upper(left(@cWordReplacement,1))+lower(substring(@cWordReplacement, 2, @LenWordReplacement - 1))
                                     else @cWordReplacement  end

 while 1 > 0
   begin
       if (@k > @BegOfWord)  -- BegOfWord begin of word
         begin  
	 	   if ( @nLenWord  = (@k-@BegOfWord)) -- length of word
              begin
		 	     if @nFlags in (1, 3)  --  Case insensitive search
                    select @comparison = charindex(lower(substring(@cSearched, @BegOfWord, @nLenWord)) COLLATE Latin1_General_BIN, lower(@cWordSought) COLLATE Latin1_General_BIN)
                 else --  Case-sensitive search
                    select @comparison = charindex(substring(@cSearched, @BegOfWord, @nLenWord) COLLATE Latin1_General_BIN, @cWordSought COLLATE Latin1_General_BIN)
				 if  @comparison = 1 
                     begin  
                       select  @lnOccurrence = @lnOccurrence + 1
					   if @lnOccurrence >= @nStartOccurrence and (@nNumberOfOccurrences = -1 or @lnOccurrence < @nStartOccurrence + @nNumberOfOccurrences )
                          begin  
                            if @nFlags = 3 and @LenWordReplacement > 0
                                select  @cNewWordReplacement  = case  dbo.FWHATISCASE(substring(@cSearched, @BegOfWord, @nLenWord))
                                     when  'U' then upper(@cWordReplacement)
                                     when  'L' then lower(@cWordReplacement)
                                     when  'P' then upper(left(@cWordReplacement,1))+lower(substring(@cWordReplacement, 2, @LenWordReplacement - 1))
                                     else @cWordReplacement  end
                              select @cStrWordReplaced = @cStrWordReplaced + substring(@cSearched, @EndPrevWord, @BegOfWord - @EndPrevWord) + @cNewWordReplacement
                              select @EndPrevWord = @k  -- end of previous word
				          end
				     end
			  end 
	      end 
       if  @exit  = 1 
           begin 
             if @EndPrevWord > 1
                 select @cSearched = @cStrWordReplaced +  substring(@cSearched, @EndPrevWord, @nEndString - @EndPrevWord)
             break
           end
       select  @k = @k + 1 -- Skip over the first break character.
       while charindex(substring(@cSearched, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) > 0  and  @nEndString > @k  -- skip  break characters, if any
            select @k = @k + 1 
       select  @BegOfWord  = @k 
       select  @k = @k + 1 -- Skip over the first character in the word. We know it can not be a break character.
       while charindex(substring(@cSearched, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) = 0  and  @nEndString > @k  -- skip  the character in the word
             select  @k = @k + 1 
       if  @k >= @nEndString 
            select  @exit = 1
    end 
  return  @cSearched
end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
 -- GETALLWORDS() User-Defined Function Inserts the words from a string into the table.
 -- GETALLWORDS(@cString[, @cDelimiters])
 -- Parameters
 -- @cString  nvarchar(4000) - Specifies the string whose words will be inserted into the table @GETALLWORDS. 
 -- @cDelimiters nvarchar(256) - Optional. Specifies one or more optional characters used to separate words in @cString.
 -- The default delimiters are space, tab, carriage return, and line feed. Note that GETALLWORDS( ) uses each of the characters in @cDelimiters as individual delimiters, not the entire string as a single delimiter. 
 -- Return Value table 
 -- Remarks GETALLWORDS() by default assumes that words are delimited by spaces or tabs. If you specify another character as delimiter, this function ignores spaces and tabs and uses only the specified character.
 -- Example
 -- declare @cString nvarchar(4000)
 -- set @cString = 'The default delimiters are space, tab, carriage return, and line feed. If you specify another character as delimiter, this function ignores spaces and tabs and uses only the specified character.'
 -- select * from  dbo.GETALLWORDS(@cString, default)     
 -- select * from dbo.GETALLWORDS(@cString, ' ,.')              
 -- See Also GETWORDNUM(), GETWORDCOUNT(), GETNUMWORD(), GETOCCURSWORD(),  GETATWORD(), GETRATWORD()  User-Defined Functions   
CREATE function GETALLWORDS  (@cString nvarchar(4000), @cDelimiters nvarchar(256))
returns  @GETALLWORDS  table (WORDNUM  smallint, WORD nvarchar(4000), STARTOFWORD smallint, LENGTHOFWORD  smallint)
    begin
         declare @k smallint, @wordcount smallint, @nEndString smallint, @BegOfWord smallint, @flag bit

         select   @k = 1, @wordcount = 1,  @BegOfWord = 1,  @flag = 0,  @cString =  isnull(@cString, ''), 
                  @cDelimiters = isnull(@cDelimiters, nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
                  @nEndString = 1 + datalength(@cString) /(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

                     while 1 > 0
                         begin
                                if  @k - @BegOfWord  >  0 
                                     begin
                                          insert into @GETALLWORDS (WORDNUM,  WORD, STARTOFWORD, LENGTHOFWORD)    values( @wordcount, substring(@cString, @BegOfWord, @k-@BegOfWord), @BegOfWord,  @k-@BegOfWord )   -- previous word
                                          select  @wordcount = @wordcount + 1,  @BegOfWord = @k 
                                      end   
                                 if  @flag  = 1 
                                        break

                                 while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) > 0  and  @nEndString > @k  -- skip  break characters, if any
                                        select @k = @k + 1,  @BegOfWord  = @BegOfWord +  1
                                 while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) = 0  and  @nEndString > @k  -- skip  the character in the word
                                        select  @k = @k + 1 
                                 if  @k >= @nEndString 
                                        select  @flag  = 1
                          end 
       return 
    end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
 -- GETALLWORDS2() User-Defined Function Inserts the words from a string into the table.
 -- GETALLWORDS2(@cString[, @cStringSplitting])
 -- Parameters
 -- @cString  nvarchar(4000) - Specifies the string whose words will be inserted into the table @GETALLWORDS2. 
 -- @cStringSplitting nvarchar(256) - Optional. Specifies the string used to separate words in @cString.
 -- The default delimiter is space.
 -- Note that GETALLWORDS2( ) uses  @cStringSplitting as a single delimiter. 
 -- Return Value table 
 -- Remarks GETALLWORDS2() by default assumes that words are delimited by space. If you specify another string as delimiter, this function ignores spaces and uses only the specified string.
 -- Example
 -- declare @cString nvarchar(4000), @nIndex smallint 
 -- select @cString = 'We hold these truths to be self-evident, that all men are created equal, that they are endowed by their Creator with certain unalienable Rights, that among these are Life, Liberty and the pursuit of Happiness.', @nIndex = 30
 -- select WORD from dbo.GETALLWORDS2(@cString, default) where WORDNUM = @nIndex  -- Displays 'Liberty'
 -- select top 1 WORDNUM from dbo.GETALLWORDS2(@cString, default) order by WORDNUM desc  -- Displays 35
 -- See Also GETWORDNUM(), GETWORDCOUNT(),  GETALLWORDS(), GETNUMWORD(), GETOCCURSWORD(),  GETATWORD(), GETRATWORD()   User-Defined Functions   
CREATE function GETALLWORDS2  (@cString nvarchar(4000), @cStringSplitting  nvarchar(256) = ' '  )   -- if no break string is specified, the function uses space to delimit words.
returns  @GETALLWORDS2  table (WORDNUM  smallint, WORD nvarchar(4000), STARTOFWORD smallint, LENGTHOFWORD  smallint)
    begin
        declare @k smallint,   @BegOfWord smallint,  @wordcount  smallint,  @nEndString smallint,  @nLenSrtingSplitting smallint, @flag bit

        select   @cStringSplitting = isnull(@cStringSplitting, space(1)) ,
                    @cString = isnull(@cString, '') ,
                    @BegOfWord = 1,   @wordcount = 1,  @k = 0 , @flag = 0,
                    @nEndString =  1+  datalength(@cString) /(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end),
                    @nLenSrtingSplitting =  datalength(@cStringSplitting) /(case SQL_VARIANT_PROPERTY(@cStringSplitting,'BaseType') when 'nvarchar' then 2  else 1 end)   -- for unicode

       while  1 > 0 
          begin
             if  @k - @BegOfWord  >  0  
                 begin
                      insert into @GETALLWORDS2 (WORDNUM,  WORD, STARTOFWORD, LENGTHOFWORD)    values( @wordcount,  substring(@cString,  @BegOfWord , @k - @BegOfWord ) , @BegOfWord,  @k - @BegOfWord)
                      select  @wordcount = @wordcount + 1,  @BegOfWord = @k 
                 end 

             if  @flag  = 1 
                 break

             while charindex( substring(@cString, @BegOfWord, @nLenSrtingSplitting) COLLATE Latin1_General_BIN, @cStringSplitting COLLATE Latin1_General_BIN) > 0 --  skip break strings, if any     
                 set  @BegOfWord  = @BegOfWord +  @nLenSrtingSplitting

             select   @k  = charindex(@cStringSplitting  COLLATE Latin1_General_BIN, @cString COLLATE Latin1_General_BIN, @BegOfWord)   

             if  @k = 0 
                select   @k  =  @nEndString,  @flag  = 1
          end

       return 
    end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
 -- GETWORDCOUNT() User-Defined Function Counts the words in a string.
 -- GETWORDCOUNT(@cString[, @cDelimiters])
 -- Parameters
 -- @cString  nvarchar(4000) - Specifies the string whose words will be counted. 
 -- @cDelimiters nvarchar(256) - Optional. Specifies one or more optional characters used to separate words in @cString.
 -- The default delimiters are space, tab, carriage return, and line feed. Note that GETWORDCOUNT( ) uses each of the characters in @cDelimiters as individual delimiters, not the entire string as a single delimiter. 
 -- Return Value smallint 
 -- Remarks GETWORDCOUNT() by default assumes that words are delimited by spaces or tabs. If you specify another character as delimiter, this function ignores spaces and tabs and uses only the specified character.
 -- If you use 'AAA aaa, BBB bbb, CCC ccc.' as the target string for dbo.GETWORDCOUNT(), you can get all the following results.
 -- declare @cString nvarchar(4000)
 -- set @cString = 'AAA aaa, BBB bbb, CCC ccc.'
 -- select dbo.GETWORDCOUNT(@cString, default)           -- 6 - character groups, delimited by ' '
 -- select dbo.GETWORDCOUNT(@cString, ',')               -- 3 - character groups, delimited by ','
 -- select dbo.GETWORDCOUNT(@cString, '.')               -- 1 - character group, delimited by '.'
 -- See Also GETWORDNUM(), GETALLWORDS(), GETNUMWORD(), GETOCCURSWORD(),  GETATWORD(), GETRATWORD()  User-Defined Functions  
 -- UDF the name and functionality of which correspond  to the  Visual FoxPro function  
CREATE function GETWORDCOUNT  (@cString nvarchar(4000), @cDelimiters nvarchar(256) )
returns smallint 
as
    begin
      declare @k smallint, @nEndString smallint, @wordcount smallint
      select  @k = 1, @wordcount = 0, @cDelimiters = isnull(@cDelimiters, nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
              @nEndString = 1 + datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

      while charindex(substring(@cString, @k, 1) COLLATE Latin1_General_BIN, @cDelimiters COLLATE Latin1_General_BIN) > 0  and  @nEndString > @k  -- skip opening break characters, if any
          set @k = @k + 1

      if @k < @nEndString
         begin
            set @wordcount = 1 -- count the one we are in now count transitions from 'not in word' to 'in word' 
                               -- if the current character is a break char, but the next one is not, we have entered a new word
            while @k < @nEndString
               begin
                  if @k +1 < @nEndString  and charindex(substring(@cString, @k, 1) COLLATE Latin1_General_BIN, @cDelimiters COLLATE Latin1_General_BIN) > 0
                                          and charindex(substring(@cString, @k+1, 1) COLLATE Latin1_General_BIN, @cDelimiters COLLATE Latin1_General_BIN) = 0
                        select @wordcount = @wordcount + 1, @k = @k + 1 -- Skip over the first character in the word. We know it cannot be a break character.
                  set @k = @k + 1
               end
         end

     return @wordcount
    end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
 -- GETWORDNUM() User-Defined Function 
 -- Returns a specified word from a string.
 -- GETWORDNUM(@cString, @nIndex[, @cDelimiters])
 -- Parameters @cString  nvarchar(4000) - Specifies the string to be evaluated 
 -- @nIndex smallint - Specifies the index position of the word to be returned. For example, if @nIndex is 3, GETWORDNUM( ) returns the third word (if @cString contains three or more words). 
 -- @cDelimiters nvarchar(256) - Optional. Specifies one or more optional characters used to separate words in @cString.
 -- The default delimiters are space, tab, carriage return, and line feed. Note that GetWordNum( ) uses each of the characters in @cDelimiters as individual delimiters, not the entire string as a single delimiter. 
 -- Return Value nvarchar(4000)
 -- Remarks Returns the word at the position specified by @nIndex in the target string, @cString. If @cString contains fewer than @nIndex words, GETWORDNUM( ) returns an empty string.
 -- Example
 -- select dbo.GETWORDNUM('To be, or not to be: that is the question:', 10, ' ,.:') -- Displays 'question'
 -- See Also
 -- GETWORDCOUNT(), GETALLWORDS(), GETNUMWORD(), GETOCCURSWORD(),  GETATWORD(), GETRATWORD()  User-Defined Functions 
 -- UDF the name and functionality of which correspond  to the Visual FoxPro function  
CREATE function GETWORDNUM  (@cString nvarchar(4000), @nIndex smallint, @cDelimiters nvarchar(256) )
returns nvarchar(4000)
as
    begin
      declare @i smallint,  @j smallint, @k smallint, @l smallint, @lmin smallint, @nEndString smallint, @LenDelimiters smallint, @cWord  nvarchar(4000)
      select  @i = 1, @k = 1, @l = 0, @cWord = '', @cDelimiters = isnull(@cDelimiters,  nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
              @nEndString = 1 + datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end), -- for unicode
              @LenDelimiters = datalength(@cDelimiters)/(case SQL_VARIANT_PROPERTY(@cDelimiters,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

      while @i <= @nIndex
         begin
            while charindex(substring(@cString, @k, 1) COLLATE Latin1_General_BIN, @cDelimiters COLLATE Latin1_General_BIN) > 0 and  @nEndString > @k   -- skip opening break characters, if any
               set @k = @k + 1

            if  @k >= @nEndString
               break

            select @j = 1, @lmin = @nEndString -- find next break character it marks the end of this word
            while @j <= @LenDelimiters
               begin
                  set @l = charindex(substring(@cDelimiters, @j, 1) COLLATE Latin1_General_BIN, @cString COLLATE Latin1_General_BIN, @k)
                  set @j = @j + 1
                  if @l > 0 and @lmin > @l
                     set @lmin = @l
               end

            if @i = @nIndex -- this is the actual word we are looking for
               begin
                  set @cWord =  substring(@cString, @k, @lmin-@k)
                  break
               end
             set @k = @lmin + 1

             if (@k >= @nEndString)
                 break
             set @i = @i + 1
         end

     return @cWord
    end
GO

 -- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
 -- GETNUMWORD() User-Defined Function Returns the index position of a word in a string.
 -- GETNUMWORD(@cString, @cWord[, @cDelimiters] [, @nOccurrence] [, @nFlags])
 -- Return Value smallint 
 -- Returns the index position of a word in a string.
 -- If @cString don't contain the word cWord, GETNUMWORD( ) returns 0.
 -- This function may be very useful to know if a word exists in a string or not.
 -- Parameters
 -- @cString nvarchar(4000) - Specifies the string to be evaluated
 -- @cWord nvarchar(4000) - Specifies the word to search for in cString.
 -- @cDelimiters nvarchar(256) - Optional. Specifies one or more optional characters used to separate words in cString.
 -- The default delimiters are space, tab, carriage return, and line feed.
 -- Note that GETNUMWORD( ) uses each of the characters in cDelimiters as individual delimiters, not the entire string as a single delimiter.
 -- @nOccurrence Specifies which occurrence, first, second, third, and so on, of cWord to search for in cString.
 -- By default, GETNUMWORD( ) searches for the first occurrence of cWord (nOccurrence = 1).
 -- @nFlags  Specifies Case-sensitive search criteria to apply to this function. Valid values are 0 (the default) and 1.
 -- 0 Case-sensitive search
 -- 1 Case insensitive search
 --  select dbo.GETNUMWORD('O Canada! Our home and native land!', 'Canada!', default, 1, 0)  --  Displays 2
 -- Sometime the search for substring from a string will return the incorrect result,
 -- it is necessary to search for the word in the string.
 -- declare @lcAdmissibleCodes nvarchar(4000), @lcCodeforChecking  nvarchar(4000)
 -- set @lcAdmissibleCodes = 'W,WN,IR,IU'
 -- set @lcCodeforChecking = 'I'
 -- select charindex(@lcCodeforChecking, @lcAdmissibleCodes)     -- Displays 6
 -- select dbo.GETNUMWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), default, default) -- Displays  0
 -- set @lcCodeforChecking = 'IR,'
 -- select charindex(@lcCodeforChecking, @lcAdmissibleCodes)      -- Displays 6
 -- select dbo.GETNUMWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), default, default) -- Displays  0
 -- set @lcAdmissibleCodes = 'W,WN,IR,IU,WN,DS'
 -- set @lcCodeforChecking = 'WN'
 -- select dbo.GETNUMWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), 2, default) -- Displays  5
 -- See Also GETWORDNUM(), GETWORDCOUNT(), GETALLWORDS(), GETNUMWORD(), GETOCCURSWORD(),  GETATWORD(), GETRATWORD()  User-Defined Functions  
CREATE function [dbo].[GETNUMWORD]
 (@cString nvarchar(4000), @cWord nvarchar(4000), @cDelimiters nvarchar(256) = NULL, @nOccurrence smallint = 1,  @nFlags bit = 0 )
returns smallint 
as
begin
   declare @k smallint, @nEndString smallint, @nLenWord smallint, @wordcount smallint,
           @BegOfWord smallint,  @lnOccurrence smallint, @exit bit, @comparison bit 

   select  @BegOfWord = 1,  @lnOccurrence = 0, @exit = 0,  @cString =  isnull(@cString, ''),
           @k = 0, @wordcount = 0, @cDelimiters = isnull(@cDelimiters, nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
           @nEndString = 1 + datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end), -- for unicode,
           @nLenWord =  datalength(@cWord)/(case SQL_VARIANT_PROPERTY(@cWord,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

 while 1 > 0
   begin
       if (@k > @BegOfWord)  -- BegOfWord begin of previous word
          begin  
             select  @wordcount = @wordcount + 1
	       if ( @nLenWord  = (@k-@BegOfWord)) -- length of previous word
                   begin
		        if @nFlags > 0  --  Case insensitive search
                            select @comparison = charindex(lower(substring(@cString, @BegOfWord, @nLenWord)) COLLATE Latin1_General_BIN, lower(@cWord) COLLATE Latin1_General_BIN)
                        else --  Case-sensitive search
                            select @comparison = charindex(substring(@cString, @BegOfWord, @nLenWord) COLLATE Latin1_General_BIN, @cWord COLLATE Latin1_General_BIN)
		        if  @comparison = 1 
                           begin  
			        select  @lnOccurrence = @lnOccurrence + 1
			        if @lnOccurrence = @nOccurrence 
			           return @wordcount
		          end
		  end 
          end 
       if  @exit  = 1 
            break
       select  @k = @k + 1 -- Skip over the first break character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) > 0  and  @nEndString > @k  -- skip  break characters, if any
            select @k = @k + 1 
       select  @BegOfWord  = @k 
       select  @k = @k + 1 -- Skip over the first character in the word. We know it can not be a break character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) = 0  and  @nEndString > @k  -- skip  the character in the word
             select  @k = @k + 1 
        if  @k >= @nEndString 
            select  @exit = 1
    end 

   return 0
 end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
-- GETOCCURSWORD() User-Defined Function Returns the number of times a word occurs within the character string.
-- GETOCCURSWORD(@cString, @cWord[, @cDelimiters] [, @nFlags])
-- Returns the number of times a word occurs within the character string.
-- If @cString don't contain the word @cWord, GETOCCURSWORD() returns 0.
-- Return Value  smallint
-- Parameters
-- @cString Specifies the string to be evaluated
-- @cWord Specifies the word to search for in @cString.
-- @cDelimiters Optional. Specifies one or more optional characters used to separate words in @cString. The default delimiters are space, tab, carriage return, and line feed.
-- The maximum length of lcDelimiters is 256.
-- Note that GETOCCURSWORD() uses each of the characters in @cDelimiters as individual delimiters, not the entire string as a single delimiter.
-- @nFlags  Specifies Case-sensitive search criteria to apply to this function. Valid Values are 0 (the Default) And 1.
-- 0 Case-sensitive search
-- 1 Case insensitive search 
-- In order to accommodate the @nFlags parameter, you will need to bypass the optional parameters by passing a value of default.
-- Examples 
-- declare @lcString as nvarchar(1000)
-- select @lcString = 'O Canada!
-- Our home and native land!
-- True patriot love in all thy sons command.
-- With glowing hearts we see thee rise,
-- The True North strong and free!
-- From far and wide, O Canada, we stand on guard for thee.
-- God keep our land glorious and free!
-- O Canada, we stand on guard for thee.
-- O Canada, we stand on guard for thee.'
-- select dbo.GETOCCURSWORD(@lcString,  'Canada', ' ,.!?', default)  --  Displays 4
-- Sometime the search for substring from a string will return the incorrect result,
-- it is necessary to search for the word in the string.
-- declare @lcAdmissibleCodes nvarchar(4000), @lcCodeforChecking  nvarchar(4000)
-- set @lcAdmissibleCodes = 'W,WN,IR,IU'
-- set @lcCodeforChecking = 'I'
-- select dbo.OCCURS(@lcCodeforChecking, @lcAdmissibleCodes)    -- Displays 2
-- select dbo.GETOCCURSWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), default) -- Displays  0
-- set @lcCodeforChecking = 'IR,'
-- select dbo.OCCURS(@lcCodeforChecking, @lcAdmissibleCodes)   --   Displays 1
-- select dbo.GETOCCURSWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), default) -- Displays  0
-- set @lcAdmissibleCodes = 'W,WN,IR,IU,WN,DS'
-- set @lcCodeforChecking = 'WN'
-- select dbo.GETOCCURSWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), default) -- Displays  2
-- See Also  GETATWORD(), WORDTRAN(), GETNUMWORD(), GETWORDNUM(), GETWORDCOUNT(), GETALLWORDS(), GETALLWORDS2()
CREATE function [dbo].[GETOCCURSWORD]
 (@cString nvarchar(4000), @cWord nvarchar(4000), @cDelimiters nvarchar(256) = NULL, @nFlags bit = 0 )
returns smallint 
as
begin
   declare @k smallint, @nEndString smallint, @nLenWord smallint, 
           @BegOfWord smallint,  @lnOccurrence smallint, @exit bit, @comparison bit 

   select  @BegOfWord = 1,  @lnOccurrence = 0, @exit = 0,  @cString =  isnull(@cString, ''),
           @k = 0, @cDelimiters = isnull(@cDelimiters, nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
           @nEndString = 1 + datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end), -- for unicode,
           @nLenWord =  datalength(@cWord)/(case SQL_VARIANT_PROPERTY(@cWord,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode


 while 1 > 0
   begin
       if (@k > @BegOfWord)  -- BegOfWord begin of previous word
         begin  
	      if ( @nLenWord  = (@k-@BegOfWord)) -- length of previous word
                 begin
	           if @nFlags > 0  --  Case insensitive search
                      select @comparison = charindex(lower(substring(@cString, @BegOfWord, @nLenWord)) COLLATE Latin1_General_BIN, lower(@cWord) COLLATE Latin1_General_BIN)
                   else --  Case-sensitive search
                      select @comparison = charindex(substring(@cString, @BegOfWord, @nLenWord) COLLATE Latin1_General_BIN, @cWord COLLATE Latin1_General_BIN)
			 if  @comparison = 1 
		 	   select  @lnOccurrence = @lnOccurrence + 1
		 end 
         end 
       if  @exit  = 1 
            break
       select  @k = @k + 1 -- Skip over the first break character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) > 0  and  @nEndString > @k  -- skip  break characters, if any
            select @k = @k + 1 
       select  @BegOfWord  = @k 
       select  @k = @k + 1 -- Skip over the first character in the word. We know it can not be a break character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) = 0  and  @nEndString > @k  -- skip  the character in the word
             select  @k = @k + 1 
        if  @k >= @nEndString 
            select  @exit = 1
    end 

   return @lnOccurrence
 end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
-- GETATWORD() User-Defined Function Returns the beginning numeric position of the nth occurrence of a word within the character string, counting from the leftmost character.
-- GETATWORD (@cString, @cWord[, @cDelimiters] [, @nOccurrence] [, @nFlags])
-- If @cString don't contain the word cWord, GETATWORD() returns 0.
-- Return Value  smallint
-- Parameters
-- @cString Specifies the string to be evaluated
-- @cWord Specifies the word to search for in @cString.
-- @cDelimiters Optional. Specifies one or more optional characters used to separate words in cString. The default delimiters are space, tab, carriage return, and line feed.
-- The maximum length of @cDelimiters is 256.
-- Note that GETATWORD() uses each of the characters in @cDelimiters as individual delimiters, not the entire string as a single delimiter.
-- @nOccurrence Specifies which occurrence, first, second, third, and so on, of @cWord to search for in @cString.  By default, GETATWORD( ) searches for the first occurrence of @cWord (@nOccurrence = 1).
-- @nFlags  Specifies Case-sensitive search criteria to apply to this function. Valid Values are 0 (the Default) And 1.
-- 0 Case-sensitive search
-- 1 Case insensitive search 
-- In order to accommodate the @nFlags parameter, you will need to bypass the optional parameters by passing a value of default.
-- Examples 
-- select dbo.GETATWORD('Ceterum censeo Carthaginem esse delendam', 'Carthaginem', default, default, default)  -- Displays 16
-- Sometime the search for substring from a string will return the incorrect result,
-- it is necessary to search for the word in the string.
-- declare @lcAdmissibleCodes nvarchar(4000), @lcCodeforChecking  nvarchar(4000)
-- set @lcAdmissibleCodes = 'W,WN,IR,IU'
-- set @lcCodeforChecking = 'I'
-- select dbo.AT(@lcCodeforChecking, @lcAdmissibleCodes, default)    -- Displays 6
-- select dbo.GETATWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), default, default) -- Displays  0
-- set @lcCodeforChecking = 'IR,'
-- select dbo.AT(@lcCodeforChecking, @lcAdmissibleCodes, default)   --   Displays 6
-- select dbo.GETATWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), default, default) -- Displays  0
-- set @lcAdmissibleCodes = 'W,WN,IR,IU,WN,DS'
-- set @lcCodeforChecking = 'WN'
-- select dbo.GETATWORD(@lcAdmissibleCodes, @lcCodeforChecking, ',' + Space(1), 2, default) -- Displays  12
-- See Also GETWORDNUM(), GETWORDCOUNT(), GETALLWORDS() User-Defined Functions  
CREATE function [dbo].[GETATWORD]
 (@cString nvarchar(4000), @cWord nvarchar(4000), @cDelimiters nvarchar(256) = NULL, @nOccurrence smallint = 1,  @nFlags bit = 0 )
returns smallint 
as
begin
   declare @k smallint, @nEndString smallint, @nLenWord smallint, 
           @BegOfWord smallint,  @lnOccurrence smallint, @exit bit, @comparison bit 

   select  @BegOfWord = 1,  @lnOccurrence = 0, @exit = 0,  @cString =  isnull(@cString, ''),
           @k = 0, @cDelimiters = isnull(@cDelimiters, nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
           @nEndString = 1 + datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end), -- for unicode,
           @nLenWord =  datalength(@cWord)/(case SQL_VARIANT_PROPERTY(@cWord,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode

 while 1 > 0
   begin
       if (@k > @BegOfWord)  -- BegOfWord begin of previous word
         begin  
	   if ( @nLenWord  = (@k-@BegOfWord)) -- length of previous word
              begin
		 	     if @nFlags > 0  --  Case insensitive search
                                 select @comparison = charindex(lower(substring(@cString, @BegOfWord, @nLenWord)) COLLATE Latin1_General_BIN, lower(@cWord) COLLATE Latin1_General_BIN)
                             else --  Case-sensitive search
                                 select @comparison = charindex(substring(@cString, @BegOfWord, @nLenWord) COLLATE Latin1_General_BIN, @cWord COLLATE Latin1_General_BIN)
				 if  @comparison = 1 
                                     begin  
					   select  @lnOccurrence = @lnOccurrence + 1
					   if @lnOccurrence = @nOccurrence 
				  	       return @BegOfWord
				     end
			   end 
	      end 
       if  @exit  = 1 
            break
       select  @k = @k + 1 -- Skip over the first break character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) > 0  and  @nEndString > @k  -- skip  break characters, if any
            select @k = @k + 1 
       select  @BegOfWord  = @k 
       select  @k = @k + 1 -- Skip over the first character in the word. We know it can not be a break character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) = 0  and  @nEndString > @k  -- skip  the character in the word
             select  @k = @k + 1 
        if  @k >= @nEndString 
            select  @exit = 1
    end 

   return 0
 end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com 
-- GETRATWORD() User-Defined Function Returns the numeric position of the last (rightmost) occurrence of a word within the character string.
-- GETRATWORD(@cString, @cWord[, @cDelimiters] [, @nOccurrence] [, @nFlags])
-- Returns the numeric position of the last (rightmost) occurrence of a word within the character string.
-- GETRATWORD(), the reverse of the GETATWORD() function, searches the word in @cString
-- starting from the right and moving left, looking for the last occurrence of the word specified in @cWord.
-- Return Value smallint 
-- See Also GETATWORD(), GETOCCURSWORD(), GETNUMWORD(), GETWORDNUM(), GETWORDCOUNT(), GETALLWORDS(), GETALLWORDS2(), WORDTRAN()  User-Defined Functions  
CREATE function [dbo].[GETRATWORD]
 (@cString nvarchar(4000), @cWord nvarchar(4000), @cDelimiters nvarchar(256) = NULL, @nOccurrence smallint = 1,  @nFlags bit = 0 )
returns smallint 
as
begin
   declare @k smallint, @nEndString smallint, @nLenWord smallint, 
           @BegOfWord smallint, @EndOfWord smallint, @lnOccurrence smallint, @exit bit, @comparison bit 

   select  @BegOfWord = 0, @EndOfWord = 0,  @lnOccurrence = 0, @exit = 0,  @cString =  isnull(@cString, ''),
           @cDelimiters = isnull(@cDelimiters, nchar(32)+nchar(9)+nchar(10)+nchar(13)), -- if no break string is specified, the function uses spaces, tabs, carriage return and line feed to delimit words.
           @k  =  1 + datalength(@cString)/(case SQL_VARIANT_PROPERTY(@cString,'BaseType') when 'nvarchar' then 2  else 1 end), -- for unicode,
           @nLenWord =  datalength(@cWord)/(case SQL_VARIANT_PROPERTY(@cWord,'BaseType') when 'nvarchar' then 2  else 1 end) -- for unicode


 while 1 > 0
   begin
       if (@EndOfWord > @BegOfWord)  -- BegOfWord begin of previous word
         begin  
	 	   if ( @nLenWord  = (@EndOfWord-@BegOfWord)) -- length of previous word
                        begin
		 	         if @nFlags > 0  --  Case insensitive search
                                   select @comparison = charindex(lower(substring(@cString, @BegOfWord, @nLenWord)) COLLATE Latin1_General_BIN, lower(@cWord) COLLATE Latin1_General_BIN)
                                    else --  Case-sensitive search
                                   select @comparison = charindex(substring(@cString, @BegOfWord, @nLenWord) COLLATE Latin1_General_BIN, @cWord COLLATE Latin1_General_BIN)
				 if  @comparison = 1 
                                     begin  
					   select  @lnOccurrence = @lnOccurrence + 1
					   if @lnOccurrence = @nOccurrence 
				  	       return @BegOfWord
				     end
		         end 
	      end 
       if  @exit  = 1 
            break
       select @k = @k - 1 -- Skip over the last break character. We know it can not be a word character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) > 0  and  @k > 0  -- skip  break characters, if any
            select @k = @k - 1 
       select  @EndOfWord  = @k + 1
       select @k = @k - 1 -- Skip over the last character in the word. We know it can not be a break character.
       while charindex(substring(@cString, @k, 1)  COLLATE Latin1_General_BIN,  @cDelimiters COLLATE Latin1_General_BIN) = 0  and  @k > 0  -- skip  the character in the word
             select  @k = @k - 1
       select  @BegOfWord  = @k + 1 
       if  @k <= 0
            select  @exit = 1
    end 

   return 0
 end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- Similar to the Transact-SQL function Charindex, with a Right search
-- Example
-- select dbo.RCHARINDEX('me', 'Now is the time for all good men', 1)  --  Displays 30
CREATE function RCHARINDEX(@expression1 nvarchar(4000), @expression2  nvarchar(4000), @start_location  smallint = 1 )
returns nvarchar(4000)
as
    begin
       declare @StartingPosition  smallint
       set  @StartingPosition = charindex( reverse(@expression1) COLLATE Latin1_General_BIN, reverse(@expression2) COLLATE Latin1_General_BIN, @start_location)

     return case 
               when  @StartingPosition > 0
               then  2 - @StartingPosition + datalength(@expression2)/(case SQL_VARIANT_PROPERTY(@expression2,'BaseType') when 'nvarchar' then 2  else 1 end) 
                       - datalength(@expression1)/(case SQL_VARIANT_PROPERTY(@expression1,'BaseType') when 'nvarchar' then 2  else 1 end)  -- for unicode  
            else 0 
            end
    end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- Similar to the Transact-SQL function Charindex, but regardless of collation settings,  
-- executes case-sensitive search  
CREATE function CHARINDEX_BIN(@expression1 nvarchar(4000), @expression2  nvarchar(4000), @start_location  smallint = 1)
returns nvarchar(4000)
as
    begin
       return charindex( @expression1  COLLATE Latin1_General_BIN, @expression2   COLLATE Latin1_General_BIN, @start_location )
    end
GO

-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com   
-- Similar to the Transact-SQL function Charindex, but regardless of collation settings,  
-- executes case-insensitive search  
CREATE function CHARINDEX_CI(@expression1 nvarchar(4000), @expression2  nvarchar(4000), @start_location  smallint = 1)
returns nvarchar(4000)
as
    begin
       return charindex( @expression1 COLLATE Latin1_General_CI_AS , @expression2   COLLATE Latin1_General_CI_AS , @start_location )
    end
GO

-- ARABTOROMAN() Returns the character Roman numeral equivalent of a specified numeric expression (from 1 to 3999) 
-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com ,  25 April 2005 or XXV April MMV :-)
-- ARABTOROMAN(@tNum) Return Values varchar(15) Parameters @tNum  number
-- Example
-- select dbo.ARABTOROMAN(3888)   -- Displays MMMDCCCLXXXVIII
-- select dbo.ARABTOROMAN('1888') -- Displays MDCCCLXXXVIII
-- select dbo.ARABTOROMAN(1)      -- Displays I
-- select dbo.ARABTOROMAN(234)    -- Displays CCXXXIV
 -- See also ROMANTOARAB()  
CREATE function ARABTOROMAN(@tNum sql_variant)
returns varchar(75)
as
   begin
      declare @type char(1), @lResult varchar(75), @lnNum int

      select  @type =  case
         when charindex('char', cast(SQL_VARIANT_PROPERTY(@tNum,'BaseType') as varchar(20)) ) > 0  then 'C'
         when charindex('int', cast(SQL_VARIANT_PROPERTY(@tNum,'BaseType') as varchar(20)) ) > 0   then 'N'
         when cast(SQL_VARIANT_PROPERTY(@tNum,'BaseType') as varchar(20))  IN ('real', 'float', 'numeric', 'decimal')  then 'N'
         else 'W'
         end 
 
     if @type = 'W'
        set @lResult = 'Wrong type of parameter, must be Integer, Numeric or Character'
     else
       begin
         set @lnNum = cast(@tNum as int) 
         if @lnNum  between 1 and 3999
            begin    
               declare @ROMANNUMERALS char(7), @lnI tinyint, @tcNum  varchar(4)
               select @ROMANNUMERALS = 'IVXLCDM', @tcNum = ltrim(rtrim(cast(@lnNum as varchar(4)))), @lResult = ''
               set @lnI = datalength(@tcNum)
               while  @lnI >= 1  
                  begin   
                    set @lnNum = cast(substring(@tcNum, datalength(@tcNum)-@lnI+1, 1) as smallint)
                    select @lResult = @lResult + case 
                       when @lnNum < 4 then replicate(substring(@ROMANNUMERALS, @lnI*2 - 1, 1),@lnNum )
                       when @lnNum = 4 or @lnNum = 9 then substring(@ROMANNUMERALS, @lnI*2 - 1, 1)+substring(@ROMANNUMERALS, @lnI*2 + case when @lnNum = 9 then 1 else 0 end, 1)
                       else substring(@ROMANNUMERALS, @lnI*2, 1)+replicate(substring(@ROMANNUMERALS, @lnI*2 - 1, 1),@lnNum -5)
                   end
                   set @lnI = @lnI - 1
               end
            end
         else
           set @lResult = 'Out of range, must be between 1 and 3999'
        end
     return  @lResult
   end

GO

-- ROMANTOARAB() Returns the number equivalent of a specified character Roman numeral expression (from I to MMMCMXCIX)
-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com ,  25 April 2005 or XXV April MMV :-)
-- ROMANTOARAB(@tcRomanNumber) Return Values smallint
-- Parameters @tcRomanNumber  varchar(15) Roman number  
-- Example
-- select dbo.ROMANTOARAB('MMMDCCCLXXXVIII') -- Displays 3888
-- select dbo.ROMANTOARAB('MDCCCLXXXVIII')   -- Displays 1888
-- select dbo.ROMANTOARAB('I')               -- Displays 1
-- select dbo.ROMANTOARAB('CCXXXIV')         -- Displays 234
-- See also ARABTOROMAN()  
CREATE function ROMANTOARAB(@tcRomanNumber varchar(15))
returns smallint
as
   begin
      declare @lnResult smallint, @lcRomanNumber varchar(15), @lnI tinyint, @ROMANNUMERALS char(7)
      select @tcRomanNumber = ltrim(rtrim(upper(@tcRomanNumber))), @ROMANNUMERALS = 'IVXLCDM', @lcRomanNumber = '', @lnI = 1, @lnResult = 0
   
     while  @lnI <= datalength(@tcRomanNumber)
       begin 
         if charindex(substring(@tcRomanNumber, @lnI, 1), @ROMANNUMERALS) > 0
           set @lcRomanNumber = @lcRomanNumber + substring(@tcRomanNumber, @lnI, 1)
         else
           begin
             set @lnResult = -1
             break
            end
         set @lnI =  @lnI + 1
       end
    
     if @lnResult >  -1
       begin
         declare @lnJ tinyint, @lnDelim smallint, @lnK tinyint
         select  @lnK = datalength(@lcRomanNumber), @lnI = 1
   
         while  @lnI <= 4
            begin
              if @lnK = 0
                  break
              set @lnDelim = power(10, @lnI-1)
              if @lnK >= 2 and substring(@lcRomanNumber, @lnK - 1, 2) = (substring(@ROMANNUMERALS, @lnI*2 - 1, 1)+substring(@ROMANNUMERALS, @lnI*2, 1)) -- CD or XL or IV
                select @lnResult = @lnResult + 4*@lnDelim, @lnK = @lnK - 2
              else  
              if @lnK >= 2 and  substring(@lcRomanNumber, @lnK - 1, 2) = (substring(@ROMANNUMERALS, @lnI*2 - 1, 1)+substring(@ROMANNUMERALS, (@lnI+1)*2 - 1, 1)) -- CM or XC or IX
                select @lnResult = @lnResult + 9*@lnDelim, @lnK = @lnK - 2
              else
                begin 
                  set @lnJ = 1
                  while  @lnJ <= 3  -- MMM or CCC or XXX or III
                    begin
                      if  @lnK >=1 and substring(@lcRomanNumber, @lnK, 1) = substring(@ROMANNUMERALS, @lnI*2 - 1, 1)
                        select @lnResult = @lnResult + @lnDelim, @lnK = @lnK - 1
                      set @lnJ =  @lnJ + 1
                    end 
                  if @lnK = 0
                    break
                  if substring(@lcRomanNumber, @lnK, 1) = substring(@ROMANNUMERALS, @lnI*2, 1) -- D or L or V
                    select @lnResult = @lnResult + 5*@lnDelim, @lnK = @lnK - 1
                end 
             set @lnI =  @lnI + 1
            end
         end      
        
        if @lnK > 0
          set @lnResult = -1
     
     return  @lnResult
   end

GO

-- ADDROMANNUMBERS() User-Defined Function is written just FYA
-- Returns the result of addition, subtraction, multiplication or division of two Roman numbers  
-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com ,  25 April 2005 or XXV April MMV :-)
-- ADDROMANNUMBERS(@tcRomanNumber1, @tcRomanNumber2, @tcOperator) Return Values varchar(75)
-- Parameters @tcRomanNumber1 varchar(15) Roman number
-- @tcRomanNumber2 varchar(15) Roman number, @tcOperator char(1) operator
-- Example
-- select dbo.ADDROMANNUMBERS('I','I',default)                       -- Displays II
-- select dbo.ADDROMANNUMBERS('MMMDCCCLXXXVIII','MDCCCLXXXVIII','-') -- Displays MM
-- select dbo.ADDROMANNUMBERS('DCCCLXXXVIII','VIII',default)         -- Displays DCCCXCVI
-- select dbo.ADDROMANNUMBERS('DCCCLXXXVIII','VIII','*')             -- Displays Out of range, must be between 1 and 3999
-- select dbo.ADDROMANNUMBERS('MMMDCCCLXXXVIII','II','/')            -- Displays MCMXLIV
-- See also ROMANTOARAB(), ARABTOROMAN()  
CREATE function ADDROMANNUMBERS(@tcRomanNumber1 varchar(15), @tcRomanNumber2 varchar(15), @tcOperator char(1)='+' )
returns varchar(75)
   begin
      declare @lcResult varchar(75)
      if @tcOperator in ('+','-','*','/')
        begin
          declare @lnN1 int, @lnN2 int
          select @lnN1 = dbo.ROMANTOARAB(@tcRomanNumber1),  @lnN2 = dbo.ROMANTOARAB(@tcRomanNumber2)
          if @lnN1 < 0
            set @lcResult = 'Wrong first roman number'
          else 
            if @lnN2 < 0
              set @lcResult = 'Wrong second roman number'
            else   
              begin
                select @lcResult = 
                case @tcOperator 
                  when '+' then dbo.ARABTOROMAN(@lnN1 + @lnN2)
                  when '-' then dbo.ARABTOROMAN(@lnN1 - @lnN2)
                  when '*' then dbo.ARABTOROMAN(@lnN1 * @lnN2)
                  when '/' then dbo.ARABTOROMAN(@lnN1 / @lnN2)
                end                
              end
        end
      else 
        set @lcResult = 'Wrong third parameter, must be +,-,*,/'
         
    return @lcResult
  end

GO

-- ARABTOARMENIAN() Returns the unicode character Armenian numeral equivalent of a specified numeric expression (from 1 to 9999) 
-- see http://en.wikipedia.org/wiki/Armenian_numerals
-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com ,  15 October 2006
-- ARABTOARMENIAN(@tNum) Return Values nvarchar(75) Parameters @tNum  number
-- Example
-- select dbo.ARABTOARMENIAN(3888)   -- Displays ????
-- select dbo.ARABTOARMENIAN('1888') -- Displays ????
-- select dbo.ARABTOARMENIAN(1)        -- Displays ?
-- select dbo.ARABTOARMENIAN(234)    -- Displays ???
-- See also ARMENIANTOARAB()  
CREATE function ARABTOARMENIAN(@tNum sql_variant)
returns nvarchar(75)
as
   begin
      declare @type char(1), @lResult nvarchar(75), @lnNum int

      select  @type =  case
         when charindex('char', cast(SQL_VARIANT_PROPERTY(@tNum,'BaseType') as varchar(20)) ) > 0  then 'C'
         when charindex('int', cast(SQL_VARIANT_PROPERTY(@tNum,'BaseType') as varchar(20)) ) > 0   then 'N'
         when cast(SQL_VARIANT_PROPERTY(@tNum,'BaseType') as varchar(20))  IN ('real', 'float', 'numeric', 'decimal')  then 'N'
         else 'W'
         end 
 
     if @type = 'W'
        set @lResult = N'Wrong type of parameter, must be Integer, Numeric or Character'
     else
       begin
         set @lnNum = cast(@tNum as int) 
         if @lnNum  between 1 and 9999
            begin    
               declare @lnI tinyint, @tcNum  varchar(4)
               select @tcNum = ltrim(rtrim(cast(@lnNum as varchar(4)))), @lResult = '', @lnI = 0
               while  @lnI <= len(@tcNum) 
                  begin   
                    select @lnNum = cast(substring(@tcNum, len(@tcNum)-@lnI, 1) as smallint), @lnI = @lnI + 1
                    if  @lnNum > 0
                      select @lResult = @lResult + nchar(unicode(N'?')+ @lnNum - 1+9*(@lnI-1))
                  end
               select @lResult = reverse(@lResult)
            end
         else
           set @lResult = N'Out of range, must be between 1 and 9999'
        end
     return  @lResult
   end

GO

-- ARMENIANTOARAB() Returns the number equivalent of a specified character Armenian numeral expression (from ? to ????)
-- see http://en.wikipedia.org/wiki/Armenian_numerals
-- Author:  Igor Nikiforov,  Montreal,  EMail: udfunctions@gmail.com , 15 October 2006 or ??  October ??  :-)
-- ARMENIANTOARAB(@tcArmenianNumber) Return Values smallint
-- Parameters @tcArmenianNumber  nvarchar(4) Armenian number  
-- Example
-- select dbo.ARMENIANTOARAB(N'????') -- Displays 3888
-- select dbo.ARMENIANTOARAB(N'????') -- Displays 1888
-- select dbo.ARMENIANTOARAB(N'?')    -- Displays 1
-- select dbo.ARMENIANTOARAB(N'???')  -- Displays 234
-- See also ARABTOARMENIAN()  
CREATE function ARMENIANTOARAB(@tcArmenianNumber nvarchar(10))
returns smallint
as
   begin
      declare @lnResult smallint, @lcArmenianNumber nvarchar(10), @lcArmenianLetter nchar(1), @lnI tinyint
      select @tcArmenianNumber = ltrim(rtrim(upper(@tcArmenianNumber))), @lcArmenianNumber = N'', @lnI = 1, @lnResult = 0
   
     while  @lnI <= len(@tcArmenianNumber)
       begin 
         select @lcArmenianLetter = substring(@tcArmenianNumber, @lnI, 1)
         if  @lcArmenianLetter between N'?' and  N'?' 
           set @lcArmenianNumber = @lcArmenianNumber + @lcArmenianLetter
         else
           if  @lcArmenianLetter <> nchar(32)  
             begin
               set @lnResult = -1
               break
             end
         set @lnI =  @lnI + 1
       end
    
     if @lnResult >  -1
       begin
         select  @lnI = 1
         while  @lnI <= len(@lcArmenianNumber) and @lnResult > - 1
            begin
              select @lcArmenianLetter = substring(@lcArmenianNumber, @lnI, 1), @lnI = @lnI + 1
                if @lcArmenianLetter >= N'?' and @lnResult = 0  -- 1000
                   select @lnResult = @lnResult + 1000*(unicode(@lcArmenianLetter)-unicode(N'?')+1)
                 else   
                   if @lcArmenianLetter >= N'?' and @lnResult % 1000 = 0  -- 100
                      select @lnResult = @lnResult + 100*(unicode(@lcArmenianLetter)-unicode(N'?')+1)
                   else   
                      if @lcArmenianLetter >= N'?' and @lnResult % 100 = 0  -- 10
                        select @lnResult = @lnResult + 10*(unicode(@lcArmenianLetter)-unicode(N'?')+1)
                      else   
                        if @lcArmenianLetter >= N'?' and @lnResult % 10 = 0  -- 1
                           select @lnResult = @lnResult + unicode(@lcArmenianLetter)-unicode(N'?')+1
                        else   
                           select @lnResult = - 1
                           
            end      
       end
     return  @lnResult
  end

GO
