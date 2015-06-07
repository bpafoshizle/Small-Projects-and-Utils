import sys
import os
import re
import zipfile
import shutil
import subprocess
import argparse

parser = argparse.ArgumentParser(description='Modify connection strings in an Excel file.')
parser.add_argument('-x', '--excelFile', help='a string Excel 2007+ file (.xlsx).')
parser.add_argument('-f', '--fileList', help='a string text file containing a list of Excel 2007+ files.')
parser.add_argument('-s', '--searchString', help='the string representing the original connection to be replaced')
parser.add_argument('-r', '--replaceString', help='the string to replace the original connection with')

args = parser.parse_args()

def main(args):

	#print args
	
	if(args.excelFile):		
		changeConStringForExcel(args.excelFile, args.searchString, args.replaceString)
	elif(args.fileList):
		reportPathList = open(args.fileList, "r")
		lines = reportPathList.readlines()
		reportPathList.close()
		
		for filePathString in lines:
			#raw_input('DEBUGGING: Changing con string for ' + filePathString)
			changeConStringForExcel(filePathString, args.searchString, args.replaceString)
				

def changeConStringForExcel(filePathString, searchString, replaceString):
	strippedFilePathString = filePathString.strip()
	fileName = os.path.basename(strippedFilePathString)
	fileNameNoExt, fileExtension = os.path.splitext(fileName)
	dirName = os.path.dirname(strippedFilePathString)
	
	#raw_input('DEBUGGING: Changing file to zip. Hit enter to continue')
	zipFileName = fileNameNoExt + '.zip'
	zipPathString = (os.path.join(dirName, zipFileName)).strip()
				
	if os.path.exists(strippedFilePathString):
		#raw_input('DEBUGGING: ' + strippedFilePathString + ' exists. Hit enter to continue')
		os.rename(strippedFilePathString, zipPathString)
		try:
			zfile = zipfile.ZipFile(zipPathString)
			
			#raw_input('DEBUGGING: Extracting connection file. Hit enter to continue')
			zfile.extract('xl/connections.xml')
			zfile.close()
			modifyConnection(searchString, replaceString)

			#raw_input('DEBUGGING: Running 7-zip to delete existing connection file in archive. Hit enter to continue')
			sevenZipCommand = r'"C:\Program Files\7-Zip\7z.exe" d "' + zipPathString + '" xl\Connections.xml'
			subprocess.call(sevenZipCommand)
			
			# Can't overwrite a file using python's module. It will just create an additional one.
			# Must delete the thing first.
			#raw_input('DEBUGGING: Writing new connection file to zip. Hit enter to continue')
			zfile = zipfile.ZipFile(zipPathString, 'a')
			zfile.write('xl\connections.xml')
		except:
			print "Unexpected error:", sys.exc_info()
			if(zfile):
				zfile.close()
			os.rename(zipPathString, strippedFilePathString)
			raise RuntimeError("Unexpected error:", sys.exc_info())
		else:
			if(zfile):
				zfile.close()
			os.rename(zipPathString, strippedFilePathString)
	else:
		raise RuntimeError("File doesn't exist", strippedFilePathString)
				
def modifyConnection(fromString, toString):
	try:
		#raw_input('DEBUGGING: Modifying connection. Hit enter to continue')
		conXmlFile = open('xl/connections.xml', 'r+')
		conXml = conXmlFile.read()
	except:
		if(conXmlFile):
			conXmlFile.close()
		raise RuntimeError("problem opening xl connection file")
	else:
		fromPattern = re.compile(regifyString(fromString))
		toString = escapeBackslashes(toString)
			
		#raw_input('DEBUGGING: Changing server to ' + toString + '. Hit enter to continue')
		conXml = re.sub(fromPattern, toString, conXml)
		
		# Handled people having the old version of whqwtra02\tabular still in their reports
		#fromString2 = r'[wW][hH][qQ][wW][tT][rR][aA]02\\\\tabular'
		fromPattern2 = re.compile(r'[wW][hH][qQ][wW][tT][rR][aA]02\\\\tabular')
		
		
		#raw_input('DEBUGGING: Chaging provider to MSOLAP.5. Hit enter to continue')
		conXml = re.sub('MSOLAP\.4', 'MSOLAP.5', conXml)
		
		if(conXmlFile):
			conXmlFile.seek(0)
			conXmlFile.write(conXml)
			conXmlFile.truncate()
			conXmlFile.close()

def regifyString(s):
	s = s.replace('\\', '\\\\').replace('.', r'\.')
	return s
	
def escapeBackslashes(s):
	s = s.replace('\\', '\\\\')
	return s

	
if  __name__ =='__main__':main(args)				
			

