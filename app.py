import os
import os.path as p
import rethinkdb as r
from collections import defaultdict
import json
import time
from itertools import repeat
from functools import partial

class Master(object):
    def __init__(self, table, path, root=None):
        self.root = root or p.expandvars(path)
        self.foreign = Foreign(table, p.relpath(path, self.root))
        self.local = Local(path)
    
    def diff(self):
        if not self.local.isdir or not self.foreign.isdir:
            if self.local > self.foreign: return ([],[self.local])
            else: return ([self.foreign],[])
        child = partial(self.__class__, self.table, root=self.root)
        down,up = zip(*(child(p.join(self.path,i)).diff for i in f&l))
        down += f-l
        up += l-f
        return down,up

    def sync(self):
        down, up = self.diff()
        
    
    def __rshift__(self, foreign):
        pass
    def __lshift__(self, local):
        pass

    def __nonzero__(self):
        return self.local == self.foreign


class Local(object):
    def __init__(self, path):
        if not p.exists(path): os.mkdir(path)
        self.path = path

    @property
    def mtime(self):
        return p.getmtime(self.path)
    
    @property
    def isdir(self):
        return p.isdir(self.path)

    @property
    def children(self):
        return set(os.listdir(self.path) if self.isdir else [])

    def __getitem__(self, key):
        return self.__class__(p.join(self.path,key))

    def __iter__(self): return self.children
    def __contains__(self, key): return key in self.children
    def __and__(self, other): self.children & other.children
    def __xor__(self, other): self.children ^ other.children
    def __eq__(self, other): self.mtime == other.mtime

class Foreign(object):
    def __init__(self, table, path):
        self.table, self.path = table, path
        self._info = table.get_all(path, index='path').run()

    @property
    def children(self):
        return self._info['children']

    def __getitem__(self, key):
        return self.__class__(self.table,p.join(self.path,key))

    def __getattr__(self,attr):
        if attr not in self._info:
            raise AttributeError('Foreign record has no {}'.format(attr))
        return self._info[attr]

    def __iter__(self): return self.children
    def __contains__(self, key): return key in self.children
    def __and__(self, other): self.children & other.children
    def __xor__(self, other): self.children ^ other.children
    def __eq__(self, other): self.mtime == other.mtime

def main():
    home = os.environ.get('RESYNC_HOME')
    if not home: return []
    
    with open(p.join(home,'conf.json')) as f: conf = json.load(f)
    r.connect(conf['host'],conf['port'],conf['db']).repl()
    masterRecords = (Master(r.table(t),p) for p,t in conf['tbMap'].items())
    return masterRecords

if __name__ == '__main__': main()