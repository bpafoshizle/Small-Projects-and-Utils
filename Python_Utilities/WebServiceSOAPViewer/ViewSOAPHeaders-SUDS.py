import logging
import sys
import urllib2
from suds.transport.https import WindowsHttpAuthenticated
from suds.client import Client

LOG_FILENAME = "C:\\Documents and Settings\\MILLERBARR\\My Documents\\Python_Utilities\\WebServiceSOAPViewer\\SUDSOutput.txt"
logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
logging.getLogger("suds.client").setLevel(logging.DEBUG)

url = "http://localhost:8080/reportserver_isdev/ReportService2005.asmx?wsdl"
username = sys.argv[1]
password = sys.argv[2]

# Set up NTLM handling code
ntlm = WindowsHttpAuthenticated(username=username, password=password)

client = Client(url, transport=ntlm)
print client
if(callable(client.service.FireEvent)):
	result = client.service.FireEvent("TimedSubscription", "8F499BEB-5B8D-43E8-8C30-D7BFBC53416C")
	print result
