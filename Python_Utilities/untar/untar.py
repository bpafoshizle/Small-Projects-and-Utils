import sys
import tarfile

myTar = tarfile.open(sys.argv[1])
myTar.extractall(path=sys.argv[2])