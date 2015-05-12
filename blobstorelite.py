import cPickle as pickle
import zlib
import os

# Fast
# Lightweight
# Embedded
# Key value store.

# GOALS:
# 1) No installation, light configuration.
# 2) Store 10,000 HTML documents quickly and compress them
# 3) References to all documents should be in memory
# 4) There will be meta data associate with the item. For example, default is {'marked':False, 'attempts':3}

# TODO: Consider swapping out the queue as chunks to disk. This will support more files, but means it isn't all in memory.
# The trade off is that random look up will take a performance hit.

# TODO: Don't save the queue to disk every write. This is wasteful. 
# TODO: Align to pages

# TODO: Make zipping of blob optional

# TODO: For small blobs, perhaps cheaper to append to one file, similar to sqlite.

# Pickle and then compress
def pickle_and_save(path, data): # 4 x smaller using compressed
	compressed = zlib.compress(pickle.dumps(data))	
	with open(path + "queue", "wb") as fw: # Over write the file each time
		fw.write(compressed)

# uncompress, then depickle
def read_and_depickle(path):
	try:
		with open(path + "queue", "rb") as fr:
			return pickle.loads(zlib.decompress(fr.read()))
	except:
		return {}

class BlobStoreLite:

	BIN_SIZE = 1000 # Create a new folder for every 1000 files
	#DEFAULT_SAVE_FREQUENCY = 10 # How often to save the queue

	def __init__(self, default_max_docs = 10, default_path = "./blobstorelite_data/"):
		self.path = default_path
		obj = read_and_depickle(self.path)
		self.max_docs = obj.get('max_docs', default_max_docs)
		self.head_ptr = obj.get('head_ptr', -1)
		self.queue = obj.get('queue', [])

	def increment_index(self, index):
		return (index + 1) % self.max_docs

	def decrement_index(self, index):
		return (index - 1) % self.max_docs


	def save(self):
		obj = { 
				'max_docs': self.max_docs,
				'head_ptr': self.head_ptr,
				'queue': self.queue,
				}
		pickle_and_save(self.path, obj)

	# For a particular index, get the associate folder
	def get_folder(self, index):
		folder_path = self.path + str(index / self.BIN_SIZE) # Group folders
		if not os.path.exists(folder_path): os.makedirs(folder_path)
		return folder_path

	def read_file(self, index):
		with open(self.get_folder(index) + "/" + str(index), "rb") as fr:
			return (zlib.decompress(fr.read()), self.queue[index]) # Return the document and meta

	def add(self, document, meta = {}):
		if type(document) == str:
			self.head_ptr = self.increment_index(self.head_ptr) # Increment the pointer in the circular queue

			if len(self.queue) < self.max_docs: # Grow the list
				self.queue.append(meta)
			else:  
				self.queue[self.head_ptr] = meta

			with open(self.get_folder(self.head_ptr) + "/" + str(self.head_ptr), "wb") as fw: # Over write the file each time
				fw.write(zlib.compress(document))

			
			# TODO: don't save for every write.
			self.save()
		else:
			raise "document is not type str"

		# TODO: Save meta data to last line of files.. So, when initialize object, check if there are more documents existing than in the queue
		# Add the missing files to the queue.

	# document, meta  = get()
	# if index = -1, gets the head document
	def get(self, index = -1):
		index = self.head_ptr if index == -1 else index
		return self.read_file(index)

	# start from head, work backwards
	# This is iterable
	def __iter__(self):
		current_index = self.head_ptr
		yield self.read_file(current_index)

		current_index  = self.decrement_index(current_index) 

		# If queue < max_docs, only get to zero. Otherwise, loop around and terminate when reaching the head_ptr
		while (current_index != self.head_ptr) and ((current_index < len(self.queue)) or (len(self.queue) == self.max_docs)):
			yield self.read_file(current_index)
			current_index  = self.decrement_index(current_index) 



d = BlobStoreLite()
d.add("the little dog")
d.add("asdfadsf;fj")
d.add("asdff;fj","extra meta data")
d.add("more dogs", {'dic':'meta'})

doc, meta = d.get()
print doc


for doc, meta in d:
	print doc