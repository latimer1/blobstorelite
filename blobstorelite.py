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
# TODO: Support for multiple symbolic names, rather than a single one.

# Pickle and then compress
def pickle_and_save(path, data): # 4 x smaller using compressed
	compressed = zlib.compress(pickle.dumps(data))	
	with open(path + "lookup", "wb") as fw: # Over write the file each time
		fw.write(compressed)

# uncompress, then depickle
def read_and_depickle(path):
	try:
		with open(path + "lookup", "rb") as fr:
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
		self.head_ptr = obj.get('head_ptr', -1) # Pointer to last entry in queue
		self.queue = obj.get('queue', []) # ID of blobs, associated meta data, and symbolic name reference
		self.sym_names = obj.get('sym_names', {}) # Dictionary mapping symbolic names to id

	def increment_index(self, index):
		return (index + 1) % self.max_docs

	def decrement_index(self, index):
		return (index - 1) % self.max_docs


	def save(self):
		obj = { 
				'max_docs': self.max_docs,
				'head_ptr': self.head_ptr,
				'queue': self.queue,
				'sym_names': self.sym_names,
				}
		pickle_and_save(self.path, obj)

	# For a particular index, get the associate folder
	def get_folder(self, index):
		folder_path = self.path + str(index / self.BIN_SIZE) # Group folders
		if not os.path.exists(folder_path): os.makedirs(folder_path)
		return folder_path

	def read_file(self, index):
		with open(self.get_folder(index) + "/" + str(index), "rb") as fr:
			return (zlib.decompress(fr.read()), self.queue[index][0], self.queue[index][1]) # Return the document and meta

	# If the name exists, remove previous sym link 
	def add(self, document = None, name = None, meta = {}):
		if type(document) == str:
			self.head_ptr = self.increment_index(self.head_ptr) # Increment the pointer in the circular queue

			# If the name was used before, remove the old reference
			if type(name) == str and name in self.sym_names:
				index = self.sym_names[name]
				entry = (None, self.queue[index][1])
				self.queue[index] = entry

			entry = (name, meta)

			if len(self.queue) < self.max_docs: # Grow the list
				self.queue.append(entry)
			else:
				self.sym_names.pop(self.queue[self.head_ptr][0], None) # Remove the old sym name, since circling back on the queue
				self.queue[self.head_ptr] = entry

			# Update the entry in symbolic lookup
			if type(name) == str:
				self.sym_names[name] = self.head_ptr

			with open(self.get_folder(self.head_ptr) + "/" + str(self.head_ptr), "wb") as fw: # Over write the file each time
				fw.write(zlib.compress(document))

			

			# TODO: don't save for every write.
			self.save()
		else:
			raise "document is not type str"

		# TODO: Save meta data to last line of files.. So, when initialize object, check if there are more documents existing than in the queue
		# Add the missing files to the queue.

	# document, meta  = get()
	# if key = -1, gets the head document
	def get(self, key = -1):
		if type(key) == str:
			if key in self.sym_names:
				index = self.sym_names[key]
			else:
				return (None, key, None) # Expired. Object no longer exists
		else:
			index = self.head_ptr if key == -1 else key
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
d.add("the little dog", name = "fido")
d.add("asdfadsf;fj")
d.add("asdff;fj",meta="extra meta data")
d.add("more dogs", meta={'dic':'meta'})

doc, name, meta = d.get()
print doc

doc, name, meta = d.get('fido')
print doc + " | name:" +  name

for doc, name, meta in d:
	print doc