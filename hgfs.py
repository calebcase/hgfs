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
            dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-m \"cruft\"']))
            dispatch(request(['--cwd', self.tmp, 'push']))
        finally:
            shutil.rmtree(self.tmp)

    def __load_attributes(self):
        ahgfs = os.path.join(self.tmp, '.hgfs')

        try:
            os.mkdir(ahgfs)
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
        _path = path[1:]
        try:
            ahgfs_path = os.path.join(self.tmp, '.hgfs', os.path.dirname(_path))
            os.makedirs(ahgfs_path)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise

        apath = os.path.join(self.tmp, _path)
        st = os.stat(apath)

        attributes = dict((key, getattr(st, key)) for key in self.ATTRS)

        modpath = os.path.join(self.tmp, '.hgfs', _path + '.attr')
        with open(modpath, 'w+') as f:
            f.write(json.dumps(attributes, sort_keys=True, indent=2))

        ahgfs = os.path.join(self.tmp, '.hgfs')

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-m', msg, '.hgfs', str(_path)]))
        dispatch(request(['--cwd', self.tmp, 'push']))

    def chmod(self, path, mode):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)
        status = os.chmod(apath, mode)

        self.__save_attributes(path, "chmod: %s %o" % (_path, mode))

        return status

    def chown(self, path, uid, gid):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)
        status = os.chown(apath, uid, gid)

        self.__save_attributes(path, "chown: %s %d %d" % (_path, uid, gid))

        return status

    def create(self, path, mode, fi=None):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        with open(apath, 'w+', mode) as f:
            pass


        self.__save_attributes(path, "create: %s %o" % (_path, mode))

        return 0

    def getattr(self, path, fh=None):
        dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        st = os.stat(apath)
        return dict((key, getattr(st, key)) for key in self.ATTRS)

    def mkdir(self, path, mode):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        status = os.mkdir(apath, mode)

        self.__save_attributes(path, "mkdir: %s %o" % (_path, mode))

        return status

    def read(self, path, size, offset, fh):
        dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        with open(apath, 'r') as f:
            data = f.read()
        return data

    def readdir(self, path, fh):
        dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        paths = os.listdir(apath)
        clean = []

        for path in paths:
            if path != '.hg' and path != '.hgfs':
                clean.append(path)

        return clean

    def readlink(self, path):
        dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        return os.readlink(apath)

    def rename(self, old, new):
        _old = old[1:]
        _new = new[1:]

        dispatch(request(['--cwd', self.tmp, 'mv', _old, _new]))
        dispatch(request(['--cwd', self.tmp, 'mv', os.path.join('.hgfs', _old + '.attr'), os.path.join('.hgfs', _new + '.attr')]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-m', "rename: %s -> %s" % (_old, _new), str(_old), str(_new)]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return 0

    def rmdir(self, path):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)
        dpath = os.path.join(self.tmp, '.hgfs', _path)
        hpath = os.path.join(self.tmp, '.hgfs', _path + '.attr')

        try:
            status = os.rmdir(dpath)
        except:
            pass

        status = os.rmdir(apath)

        try:
            status = os.unlink(hpath)
        except:
            pass

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-m', "rmdir: %s" % (_path), str(_path)]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    def unlink(self, path):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)
        hpath = os.path.join(self.tmp, '.hgfs', _path + '.attr')

        status = os.unlink(apath)

        try:
            status = os.unlink(hpath)
        except:
            pass

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-m', "unlink: %s" % (_path), str(_path)]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    def symlink(self, target, source):
        _source = source[1:]
        _target = target[1:]

        asource = os.path.join(self.tmp, _source)
        atarget = os.path.join(self.tmp, _target)

        status = os.symlink(source, atarget)

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-m', "symlink: %s -> %s" % (source, _target), str(source), str(_target)]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    def truncate(self, path, length, fh=None):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        status = 0
        with open(apath, 'r+') as f:
            status = f.truncate(length)

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-m', "truncate: %s" % (_path), str(_path)]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    def utimens(self, path, times=None):
        apath = os.path.join(self.tmp, path[1:])
        return os.utime(apath, times)

    def write(self, path, data, offset, fh):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        with open(apath, 'r+') as f:
            f.seek(offset, 0)
            f.write(data)

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-m', "write: %s" % (_path), str(_path)]))
        dispatch(request(['--cwd', self.tmp, 'push']))

        return len(data)

if __name__ == '__main__':
    logging.basicConfig()
    if len(argv) != 3:
        print('usage: %s <repo> <mountpoint>' % argv[0])
        exit(1)

    fuse = FUSE(HgFS(argv[1], argv[2]), argv[2], foreground=True, nothreads=True)
