#!/usr/bin/env python

import os
import pwd
import logging
import tempfile
import shutil
import errno
import json

from sys import argv, exit
from time import time

from fuse import FUSE, Operations, LoggingMixIn

from mercurial.dispatch import dispatch

try:
    from mercurial.dispatch import request
except:
    request = list

class HgFS(LoggingMixIn, Operations):
    '''
    A Mercurial filesystem.
    '''

    ATTRS = ('st_uid', 'st_gid', 'st_mode', 'st_atime', 'st_mtime', 'st_size')

    def __init__(self, repo, mountpoint='.'):
        self.log.setLevel('DEBUG')
        self.log.debug("repo: %s mountpoint: %s", repo, mountpoint)

        self.repo = repo
        self.mountpoint = os.path.abspath(mountpoint)
        self.log.debug("SELF: repo: %s mountpoint: %s", self.repo, self.mountpoint)

        self.tmp = os.path.abspath(tempfile.mkdtemp(prefix='hgfs-'))
        self.log.debug("Tmp: %s", self.tmp)

        dispatch(request(['clone', self.repo, self.tmp]))

        self.__load_attributes()

    def destroy(self, path):
        try:
            dispatch(request(['--cwd', self.tmp, 'add', self.tmp]))
            dispatch(request(['--cwd', self.tmp, 'commit', '-m \"cruft\"']))
            dispatch(request(['--cwd', self.tmp, 'push']))
        finally:
            shutil.rmtree(self.tmp)

    def __load_attributes(self):
        ahgfs = os.path.join(self.tmp, '.hgfs')

        try:
            os.mkdir(ahgfs)
            dispatch(request(['--cwd', self.tmp, 'add', ahgfs]))
            dispatch(request(['--cwd', self.tmp, 'commit', '-m \"Adding .hgfs\"']))
        except Exception, e:
            self.log.debug("Failed to create .hgfs.")
            pass

        attributes = os.walk(ahgfs)
        for (dirpath, dirnames, filenames) in attributes:
            for fname in filenames:
                apath = os.path.join(ahgfs, fname)
                with open(apath, 'r') as f:
                    attrs = json.load(f)
                    fname_no_ext = os.path.splitext(fname)[0]
                    atarget = os.path.join(self.tmp, fname_no_ext)

                    os.chmod(atarget, attrs['st_mode'])
                    os.chown(atarget, attrs['st_uid'], attrs['st_gid'])

    def __save_attributes(self, path, msg):
        try:
            ahgfs_path = os.path.join(self.tmp, '.hgfs', os.path.dirname(path[1:]))
            os.makedirs(ahgfs_path)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise

        apath = os.path.join(self.tmp, path[1:])
        st = os.stat(apath)

        attributes = dict((key, getattr(st, key)) for key in self.ATTRS)

        modpath = os.path.join(self.tmp, '.hgfs', path[1:] + '.attr')
        with open(modpath, 'w+') as f:
            f.write(json.dumps(attributes, sort_keys=True, indent=2))

        ahgfs = os.path.join(self.tmp, '.hgfs')

        dispatch(request(['--cwd', self.tmp, 'add', ahgfs]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m', msg]))
        dispatch(request(['--cwd', self.tmp, 'push']))

    def chmod(self, path, mode):
        apath = os.path.join(self.tmp, path[1:])
        status = os.chmod(apath, mode)

        self.__save_attributes(path, "chmod: %s %o" % (path[1:], mode))

        return status

    def chown(self, path, uid, gid):
        apath = os.path.join(self.tmp, path[1:])
        status = os.chown(apath, uid, gid)

        self.__save_attributes(path, "chown: %s %d %d" % (path[1:], uid, gid))

        return status

    def create(self, path, mode, fi=None):
        apath = os.path.join(self.tmp, path[1:])

        with open(apath, 'w+', mode) as f:
            pass

        dispatch(request(['--cwd', self.tmp, 'add', apath]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m \"create: %s\"' % path[1:]]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return 0

    def getattr(self, path, fh=None):
        apath = os.path.join(self.tmp, path[1:])
        st = os.stat(apath)
        return dict((key, getattr(st, key)) for key in self.ATTRS)

    def mkdir(self, path, mode):
        apath = os.path.join(self.tmp, path[1:])
        return os.mkdir(apath, mode)

    def read(self, path, size, offset, fh):
        dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        with open(apath, 'r') as f:
            data = f.read()
        return data

    def readdir(self, path, fh):
        apath = os.path.join(self.tmp, path[1:])
        paths = os.listdir(apath)
        clean = []

        for path in paths:
            if not path.startswith('.hg'):
                clean.append(path)

        return clean

    def readlink(self, path):
        apath = os.path.join(self.tmp, path[1:])
        return os.readlink(apath)

    def rename(self, old, new):
        aold = os.path.join(self.tmp, old[1:])
        anew = os.path.join(self.tmp, new[1:])

        dispatch(request(['--cwd', self.tmp, 'mv', aold, anew]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m \"rename: %s -> %s\"' % (old[1:], new[1:])]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return 0

    def rmdir(self, path):
        apath = os.path.join(self.tmp, path[1:])

        dispatch(request(['--cwd', self.tmp, 'rm', apath]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m \"rmdir: %s\"' % (path[1:])]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return 0

    def symlink(self, target, source):
        asource = os.path.join(self.tmp, source[1:])
        atarget = os.path.join(self.tmp, target[1:])

        dispatch(request(['--cwd', self.tmp, 'add', asource]))
        dispatch(request(['--cwd', self.tmp, 'add', atarget]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m \"symlink: %s -> %s\"' % (source[1:], target[1:])]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return os.symlink(asource, atarget)

    def truncate(self, path, length, fh=None):
        apath = os.path.join(self.tmp, path[1:])
        status = 0
        with open(apath, 'r+') as f:
            status = f.truncate(length)

        dispatch(request(['--cwd', self.tmp, 'add', apath]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m \"truncate: %s\"' % (path[1:])]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    def unlink(self, path):
        apath = os.path.join(self.tmp, path[1:])

        dispatch(request(['--cwd', self.tmp, 'rm', apath]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m \"unlink: %s\"' % (path[1:])]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return 0

    def utimens(self, path, times=None):
        apath = os.path.join(self.tmp, path[1:])
        return os.utime(apath, times)

    def write(self, path, data, offset, fh):
        apath = os.path.join(self.tmp, path[1:])
        with open(apath, 'r+') as f:
            f.seek(offset, 0)
            f.write(data)

        dispatch(request(['--cwd', self.tmp, 'add', apath]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m \"write: %s\"' % (path[1:])]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return len(data)

if __name__ == '__main__':
    logging.basicConfig()
    if len(argv) != 3:
        print('usage: %s <repo> <mountpoint>' % argv[0])
        exit(1)

    fuse = FUSE(HgFS(argv[1], argv[2]), argv[2], foreground=True, nothreads=True)
