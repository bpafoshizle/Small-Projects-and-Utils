""" 
	This python script is designed to find problem entries in SSRS data driven subscription (DDS) parameters. 
	It will compare parameter values passed to the SSRS server from a DDS query to the valid values for that 
	parameter in the report. It takes two files, one containing a query for the SSRS parameter being compared,
	against, and another containing the query for the SSRS DDS entries in question. The SSRS parameter query 
	should return the values for the parameter in the first column, and the DDS query should return the parameter
	values in the first column as well. It will make both queries against the Oracle DB and compare the lists from
	the first columns. It will output all entries from the DDS query for which, it did not find a matching parameter
	value in the parameter query, thereby showing you the offending rows. This can be a huge help when subscription
	entries returned by the query number in the thousands, but only 42 are failing. 
	
	The parameter query file should be in the same directory as the script and be named 'paramQuery.sql' 
	The DDS query file should also be in the directory of the script and be named 'ddsQuery.sql'
	The output file will be called 'nonMatchingEntries.txt'
	
	Uses cx_Oracle module: 
	http://www.orafaq.com/wiki/Python
	http://cx-oracle.sourceforge.net/html/cursor.html
"""
	

import cx_Oracle

# Function to compare a parameter value to a list of
# all valid values
def ParamValidValue(ddsParamValue, validValueList):
	# Loop through all parameter values and compare
	for paramRow in validValueList:
		validParamValue = paramRow[0]
		if(ddsParamValue == validParamValue):
			return True	
	return False

# Modify connection strings to point to appropriate sources
ddsConnString = "tdw_mgr/tdw_dev1@dbdtsn1_dtsn1"
paramConnString = "bi_rpt_app/bi_rpt_prd1@dbptsn1_ptsn1"

# Open query files and read in the query strings
ddsQueryFile = open("ddsQuery.sql", "r")
ddsQueryString = ddsQueryFile.read()
ddsQueryFile.close()
paramQueryFile = open("paramQuery.sql", "r")
paramQueryString = paramQueryFile.read()
paramQueryFile.close()

# Open the out file that will contain error entries
outFile = open("nonMatchingEntries.txt", "w")

# Execute query to get all parameter values into 
# paramValueList variable
paramConn = cx_Oracle.connect(paramConnString)
paramCursor = paramConn.cursor()
paramCursor.execute(paramQueryString)
paramValueList = paramCursor.fetchall()
paramCursor.close()
paramConn.close()

# Execute query to get all DDS parameter values
ddsConn = cx_Oracle.connect(ddsConnString)
ddsCursor = ddsConn.cursor()
ddsCursor.execute(ddsQueryString)

# Loop through all DDS param value rows
for ddsRow in ddsCursor.fetchall():
	ddsParamValue = ddsRow[0]
	
	# If parameter was not found, write out the entire row 
	# to the error entry file
	if(not ParamValidValue(ddsParamValue, paramValueList)):
		outFile.write(repr(ddsRow) + "\n")
		
# Close all open stuff
ddsCursor.close()
ddsConn.close()
outFile.close()