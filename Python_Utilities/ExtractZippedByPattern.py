import sys
import os
import time
import getopt
import shutil
import logging
import re
import zipfile
import itertools
#from sets import Set

def main(argv):

	sourceDir = None
	filePattern = None
	dirPattern = None
	outDirectory = None
	logLevel = "CRITICAL"
	
	
	try:
		opts, args = getopt.getopt(sys.argv[1:], "fpat:dpat:id:od:log", ["filePattern=","dirPattern=","inDirectory=","outDirectory=","logLevel="])
	except getopt.GetoptError:
			usage()
			sys.exit(2)
			
	for opt, arg in opts:
		if opt in ("-id", "--inDirectory"):
			sourceDir = arg
		elif opt in ("-fpat", "--filePattern"):
			filePattern = arg
		elif opt in ("-dpat", "--dirPattern"):
			dirPattern = arg
		elif opt in ("-od:", "--outDirectory"):
			outDirectory = arg
		elif opt in ("log", "--logLevel"):
			logLevel = arg
		else:
			assert False, "unhandled option"
	
	
	logging.basicConfig(filename="extractZippedByPattern.log", level=logLevel.upper())
	
	# Make sure these variables are defined
	if sourceDir is None:
		usage()
		sys.exit(2)
	
	inFileNames = []
	
	for root, subFolders, files in os.walk(sourceDir):
		for file in files:
			inFileNames.append(os.path.join(root,file))

	
	# Make sure that files were actually found in the subdir to be combined
	if inFileNames:
		for file in inFileNames:
			if(file.endswith(".zip")):
				extractFileMatchingPattern(file, filePattern, outDirectory)			
					
def extractFileMatchingPattern(file, filePattern, outDirectory):
	logging.debug("Decompressing files matching %s from %s" % (filePattern, file))
	ensureDir(outDirectory)
	zfile = zipfile.ZipFile(file)
	filterFunction = lambda x : re.search(filePattern, x) <> None
	for name in itertools.ifilter(filterFunction, zfile.namelist()):
		(dirname, filename) = os.path.split(name)
		logging.debug("Decompressing %s on %s" % (filename, outDirectory))
		try:
			fd = open(os.path.join(outDirectory,filename),"w")
			fd.write(zfile.read(name))
		except IOError, e:
			logging.critical(e)
			raise
		except NotImplementedError, e:
			logging.error(e)
		except Exception, e:
			logging.critical("Unexpected error: %s" % e)
			raise
		finally:
			fd.close()
	
	
def ensureDir(d):
	if not os.path.exists(d):
		logging.debug("Creating temp directory: %s" % d)
		os.makedirs(d)
	
def removeDir(d):
	logging.debug("Removing temp directory: %s" % d)
	shutil.rmtree(d)
	
# Function to print usage
def usage():
	print "\nUsage:\nExtractZippedByPattern.py <-pat|--filePattern> text <-dpat|--dirPattern> text <-d|--directory> text <-o|--out> text <-log|--logLevel> text"
		
if __name__ == "__main__":
	main(sys.argv[1:])
