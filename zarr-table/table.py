import zarr
import numpy as np
from collections import MutableMapping
import os

class ZarrTable():
	"""Use zarr to store a table, with each columns as an array of its own
	"""
	
	def __init__(self, store, data = None, store_type = 'sqlite', **kwarg):
		self._store_type = store_type
		store= store.replace('-','/')
		
		if not(os.access(os.path.dirname(store), mode=os.R_OK)):
			raise ValueError('Library does not exists')	
		if store_type=='sqlite':
			self.store = zarr.SQLiteStore(store)
		else:
			self.store = zarr.DirectoryStore(store)
		self.group = zarr.open_group(store = self.store, **kwarg)
		if isinstance(data, np.ndarray) and data.dtype.kind == 'V':
			self.store.cursor.execute('BEGIN TRANSACTION')
			for col in data.dtype.names:
				self.group[col] = zarr.array(data[col])
			self.store.cursor.execute('COMMIT')
	
	def close(self):
		if self._store_type == 'sqlite':
			self.store.close()
	
	@property
	def dtype(self):
		return np.dtype([(k, self.group[k].dtype.str, self.group[k].shape[1:]) for k in self.columns])
	
	@property
	def columns(self):
		return tuple(e for e in self.group.array_keys())
	
	@property
	def shape(self):
		return (min([self.group[e].shape[0] for e in self.columns]),)

	@property
	def nbytes(self):
		return sum([self.group[e].nbytes for e in self.columns])
	
	@property
	def nbytes_stored(self):
		return sum([self.group[e].nbytes_stored for e in self.columns])	
			
	def __getitem__(self, key):
		valid_index = (np.dtype('?'), np.dtype('i8'), np.dtype('i4'), np.dtype('u4'), np.dtype('u4'))
		if isinstance(key,str):
			return self.group[key]
		if isinstance(key,list):
			item = []
			dtype = []
			for k in key:
				item.append(self.group[k][:])
				dtype.append((k, self.group[k].dtype.str, self.group[k].shape[1:]))		
			return np.rec.fromarrays(item, dtype = np.dtype(dtype))
		elif isinstance(key, np.ndarray):
			if key.dtype in valid_index:
				item = []
				dtype = []
				for k in self.columns:
					item.append(self.group[k][:][key])
					dtype.append((k, self.group[k].dtype.str, self.group[k].shape[1:]))
				return np.rec.fromarrays(item, dtype = np.dtype(dtype))
		else:
			return type(key)
	
	def __setitem__(self, key, data):
		if isinstance(key, str):
			self.group[key] = zarr.array(data)
			
class ZarrSchema(MutableMapping):
	""" Function like table schemas
		"""
	def __init__(self, path, store_type='sqlite', date_store_mode = 'dir', **kwarg):
		self._path = path
		self._store_type = store_type
		self._opened_table = []
		self._date_store_mode = date_store_mode
	
	def process_path(self, name):
		if self._date_store_mode == 'dir':
			name = name.replace('-','/')
		if os.path.splitext(name)[1]!='.sqldb':
			name = os.path.splitext(name)[0]+'.sqldb'
		return os.path.join(self._path,name)
	
	def __contain__(self, name):
		store = self.process_path(name)
		return os.access(store, mode=os.R_OK)
	
	def __getitem__(self, name):
		store = self.process_path(name)
		if os.access(store, mode=os.R_OK):
			self._opened_table.append(name)
			return ZarrTable(store, store_type = self._store_type)
		else:
			raise ValueError('Table do not exists')
	
	def __setitem__(self,name, data):
		print('Does not supported at the moment')
		return 0
		
	def __delitem__(self, name):
		store = self.process_path(name)
		if os.access(store, mode=os.R_OK):
			os.remove(store)
	
	def __iter__(self):
		for root, dirs, files in os.walk(self._path, topdown=False):
			for name in files:
				yield os.path.join(root, name)
				
	def __len__(self):
		"""Number of members."""
		return sum(1 for _ in self)
	
	def create_table(self, name, data=None, replace = False):
		store = self.process_path(name)
		if not os.access(store, mode=os.R_OK):
			self._opened_table.append(name)
			return ZarrTable(store, data= data)
		elif replace:
			self._opened_table.append(name)
			os.remove(store)
			return ZarrTable(store, data= data)
		else:
			raise ValueError('Table already exists')
	
	def close_all(self):
		for name in self._opened_table:
			self[name].close()
		return 0
	
if __name__ == '__main__':
	m1 = ZarrTable('G:/array.sqldb')

