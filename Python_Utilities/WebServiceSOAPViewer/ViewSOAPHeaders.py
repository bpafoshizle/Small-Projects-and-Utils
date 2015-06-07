import logging
import sys
import urllib2
from ntlm import HTTPNtlmAuthHandler

url = 'http://localhost:8080/reportserver_isdev/ReportService2005.asmx?wsdl'
username = sys.argv[1]
password = sys.argv[2]

# Set up NTLM handling code
passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
passman.add_password(None, url, username, password)
auth_NTLM = HTTPNtlmAuthHandler.HTTPNtlmAuthHandler(passman)

# This is necessary to disable proxies. Without this, you get '404 Not Found'
proxy_handler = urllib2.ProxyHandler({})

# Create and install the opener
opener = urllib2.build_opener(proxy_handler, auth_NTLM)
urllib2.install_opener(opener)

# Get ur resultsir
response = urllib2.urlopen(url)
print(response.read())