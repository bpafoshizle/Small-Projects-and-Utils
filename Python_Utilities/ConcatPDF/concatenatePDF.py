import sys, tkFileDialog
from Tkinter import *
from pyPdf import PdfFileWriter, PdfFileReader

fileNames = []
saveFileName = "combined.pdf"
root = Tk()
vIn = StringVar() # Holds a string; default value: ""
vOut = StringVar()

def append_pdf(input, output):
	for page_num in range(input.numPages):
		output.addPage(input.getPage(page_num))

def askOpenFileNames():
	global fileNames
	fileNames = tkFileDialog.askopenfilenames(parent=root)
	#txt_File.delete(0, END)
	if fileNames:
		#txt_File.insert(0, fileNames)
		vIn.set(fileNames)
	
def askSaveAsFileName():
	global saveFileName
	saveFileName = tkFileDialog.asksaveasfilename(parent=root)
	if saveFileName:
		#txt_SaveFile.delete(0, END)
		#txt_SaveFile.insert(0, saveFileName)
		vOut.set(saveFileName)
	
def combine():
	global fileNames
	global saveFileName
	
	# If there is at least 1 file 
	if  fileNames[0] != "" and saveFileName != "":
		output = PdfFileWriter()
		
		# Loop through each pdf file entered
		for pdf in fileNames:
			print "Combining: ", pdf
			append_pdf(PdfFileReader(file(pdf, "rb")), output)
		
		print "Writing to file: ", saveFileName
		output.write(file(saveFileName, "wb"))
	
	txt_File.delete(0, END)
	txt_SaveFile.delete(0, END)
	fileNames = []
	saveFileName = "combined.pdf"
	
root.title("PDF Combiner")

btn_File = Button(root, text="Open PDFs", command=askOpenFileNames)
btn_SaveFile = Button(root, text="Save as:", command=askSaveAsFileName)
btn_Combine = Button(root, text="Combine", command=combine)
txt_File = Entry(root, width=75, state=DISABLED, textvariable=vIn)
txt_SaveFile = Entry(root, width=75, state=DISABLED, textvariable=vOut)

btn_File.pack()
txt_File.pack()
btn_SaveFile.pack()
txt_SaveFile.pack()
btn_Combine.pack()
root.mainloop()