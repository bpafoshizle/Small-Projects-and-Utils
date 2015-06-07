""" This program prints the first x lines of a file """

import sys
import os

def main(argv):
	
	fileName = None
	file = None
	numLines = None
	
	# Make sure all appropriate arguments are given
	if(len(argv) < 2):
		usage()
		sys.exit(2)
	
	# Make sure first argument is a valid file
	if(argv == None or (not os.path.isfile(argv[0]))):
		print("Fisr argument must be a file.\n")
		usage()
		sys.exit(2)
	
	fileName = argv[0]
	numLines = int(argv[1])
	
	file = open(fileName, "r")
	
	for i in range(numLines):
		sys.stdout.write(file.readline())
	
	file.close()
	
	# Function to print usage
def usage():
	print "\nUsage: pyHead fileName numLines"

	
if __name__ == "__main__":
	main(sys.argv[1:])