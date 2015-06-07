""" This program grabs all Measures from a tabular model bim file and searches for references to any column. 
    Any unused columns will be reported """

import sys
import os
import xml.dom.minidom

class tabColumn:
	'Class to represent a column on a tabular table'
	
	def __init__(self, name, visible, tableName):
		self.name = name
		self.visible = visible
		self.tableName = tableName

class tabMeasure:
	'Class to represent a measure on a tabular table'

	def __init__(self, name, text):
		self.name = name
		self.text = text
	
def main(argv):
	
	fileName = None
	file = None
	
	# Make sure all appropriate arguments are given
	if(len(argv) < 2):
		usage()
		sys.exit(2)
	
	# Make sure first argument is a valid file
	if(argv == None or (not os.path.isfile(argv[0]))):
		print("First argument must be a file.\n")
		usage()
		sys.exit(2)
	
	inFileName = argv[0]
	outFileName = argv[1]
	
	file = open(inFileName, 'r')
	dom = xml.dom.minidom.parse(file)
	file.close()
	
	mdxScriptCommands = getMdxScriptCommands(dom)
	tabularColumnList = getTabularColumnList(dom)
	print 'MeasureLength: %s' % (len(mdxScriptCommands))
	print 'ColumnLength: %s' % (len(tabularColumnList))
	
	#for command in mdxscriptcommands:
	#	print "name: %s text: %s" % (command.name, command.text)
	
	file = open(outFileName, 'w')
	
	for tabColumn in tabularColumnList:
		#print "tableName: %s columnName: %s visibility: %s" % (tabColumn.tableName, tabColumn.name, tabColumn.visible)
		if(tabColumn.visible == 'false' and (not checkMeasuresForColumnUsage(tabColumn, mdxScriptCommands))):
			s = '%s|%s|%s' % (tabColumn.tableName, tabColumn.name, tabColumn.visible)
			print s
			file.write(s)
			file.write('\r\n')
	
	file.close()
	


# function to loop over ever measure and check if the given column is used
def checkMeasuresForColumnUsage(tabColumn, mdxScriptCommands):
	for measure in mdxScriptCommands:
		if(measure.text.count(tabColumn.name) > 0):
			return True
	return False
	
# function to get MdxScript Command Object List from a tabular model xml dom
def getMdxScriptCommands(dom):
	commandList = []
	mdxScriptXml = dom.getElementsByTagName('MdxScript')[0]
	mdxScriptCommands = mdxScriptXml.getElementsByTagName('Commands')[0]
	commands = mdxScriptCommands.getElementsByTagName('Command')
	
	for command in commands:
		text = command.getElementsByTagName('Text')[0]
		name = command.getElementsByTagName('Name')
		if(not name.length == 0):
			name = name[0]
			commandList.append(tabMeasure(name=name.childNodes[0].data, text=text.childNodes[0].data))
		else:
			commandList.append(tabMeasure(name=None, text=text.childNodes[0].data))		
	return commandList
	
	
# Function that will return a list of all columns in every table in the tabular model
def getTabularColumnList(dom):
	columnList = []
	cubeXml = dom.getElementsByTagName('Cube')[0]
	dimensionsXml = cubeXml.getElementsByTagName('Dimensions')[0]
	dimensions = dimensionsXml.getElementsByTagName('Dimension')
	
	for dimension in dimensions:
		columnList.extend(getAttributesFromDimension(dimension))
		#print(dir(dimension))
		
	return columnList
	
# Function to extract all the attributes from a dimension and return a list 
# of tabColumn objects
def getAttributesFromDimension(dimension):
	tabColumnList = []
	tableName = dimension.getElementsByTagName('Name')[0].childNodes[0].data
	
	attributesXml = dimension.getElementsByTagName('Attribute')
	
	for attribute in attributesXml:
		tabColumnList.append(getAttributeData(tableName, attribute))
		
	return tabColumnList
	
	
# Function to extract attribute data from an Attribute XML node
def getAttributeData(tableName, attribute):
	attributeName = attribute.getElementsByTagName('AttributeID')[0].childNodes[0].data
	attributeVisibility = 'true'
	
	if(not len(attribute.getElementsByTagName('AttributeHierarchyVisible')) == 0):
		attributeVisibility = attribute.getElementsByTagName('AttributeHierarchyVisible')[0].childNodes[0].data
		
	return tabColumn(attributeName, attributeVisibility, tableName)
	
# Function to print usage
def usage():
	print "\nUsage: unusedTabCol.py inFileName outFileName"

	
if __name__ == "__main__":
	main(sys.argv[1:])