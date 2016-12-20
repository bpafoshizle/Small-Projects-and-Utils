""" This program queries Active Directory for all the users in a group It outputs the 
	user-configurable-delimited list to the clipboard for easy pasting. """
	
	
# usage: ClipboardGroupsMembership.py "groupName" "joinString"

import sys
import win32net as wn
import win32clipboard as wcb

groupName = sys.argv[1]
joinString = sys.argv[2]
userDictList = []
userStringList = []
results = ()
resultText = ""
totalBefore = 0
totalAfter = 0

results = wn.NetGroupGetUsers('tyson.com', groupName, 0)
userDictList = results[0]

for user in userDictList:
	userStringList.append(str(user["name"]))

print("Total before call: " + str(results[1]))
print("Total left over: " + str(results[2]))

userStringList.sort()

if(joinString in [r"\r\n", r"\n"]):
	resultText = "\r\n".join(userStringList)
else:
	resultText = joinString.join(userStringList)

print resultText

wcb.OpenClipboard()
wcb.EmptyClipboard()
wcb.SetClipboardData(wcb.CF_TEXT, resultText)
wcb.CloseClipboard()