from evernote.api.client import EvernoteClient
from evernote.edam.notestore.ttypes import NoteFilter, NotesMetadataResultSpec

devToken = "S=s1:U=8fc8e:E=150baa2e888:C=14962f1b8a8:P=1cd:A=en-devtoken:V=2:H=00145dc7735d3b5e6f2a70b504aba079"

client = EvernoteClient(token = devToken)
noteStore = client.get_note_store()

filter = NoteFilter()
filter.ascending = False

spec = NotesMetadataResultSpec()
spec.includeTitle = True
spec.includeCreated = True
spec.includeUpdated = True
spec.includeTagGuids = True
spec.includeAttributes = True



noteList = noteStore.findNotesMetadata(devToken, filter, 0, 100, spec)

for n in noteList.notes:
	print(n.guid)
	print(n.title)
	print(n.tagGuids)