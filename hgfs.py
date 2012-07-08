#!/usr/bin/env python

import argparse
import errno
import grp
import json
import logging
import os
import pwd
import shutil
import tempfile

from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, EACCES, Operations, LoggingMixIn, fuse_get_context

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
    STATV = ('f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax')

    def __init__(self, repo, mountpoint='.', args={}):
        self.log.setLevel(args.log)
        self.log.debug("repo: %s mountpoint: %s args: %s", repo, mountpoint, repr(args))

        self.repo = repo
        self.mountpoint = os.path.abspath(mountpoint)
        self.args = args

        if not self.args.clone:
            self.repo = os.path.abspath(repo)

        self.log.debug("SELF: repo: %s mountpoint: %s args: %s", self.repo, self.mountpoint, repr(self.args))

        if self.args.clone:
            self.tmp = os.path.abspath(tempfile.mkdtemp(prefix='hgfs-'))
            dispatch(request(['clone', self.repo, self.tmp]))
        else:
            self.tmp = self.repo

        self.log.debug("Tmp: %s", self.tmp)

        self.__load_attributes()

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
                apath = os.path.join(ahgfs, os.path.join(dirpath, fname))
                with open(apath, 'rb') as f:
                    attrs = json.load(f)
                    fname_no_ext = os.path.splitext(fname)[0]
                    atarget = os.path.join(self.tmp, os.path.join(dirpath[len(ahgfs) + 1:], fname_no_ext))

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

        uid, gid, pid = fuse_get_context()
        username = pwd.getpwuid(uid)[0]
        attributes['.hgfs'] = {
            'pw_uid': uid,
            'pw_name': username,
            'gr_gid': gid,
            'gr_name': grp.getgrgid(gid)[0],
        }

        modpath = os.path.join(self.tmp, '.hgfs', _path + '.attr')
        with open(modpath, 'wb+') as f:
            f.write(json.dumps(attributes, sort_keys=True, indent=2))

        ahgfs = os.path.join(self.tmp, '.hgfs')

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-u', username, '-m', msg, '.hgfs', str(_path)]))
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))

    def access(self, path, mode):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        if not os.access(apath, mode):
            raise FuseOSError(EACCES)

    def chmod(self, path, mode):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)
        status = os.chmod(apath, mode)

        self.__save_attributes(path, "hgfs[chmod]: %s %o" % (_path, mode))

        return status

    def chown(self, path, uid, gid):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)
        status = os.chown(apath, uid, gid)

        self.__save_attributes(path, "hgfs[chown]: %s %d %d" % (_path, uid, gid))

        return status

    def create(self, path, mode, fi=None):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        fh = -1
        if fi == None:
            fh = os.open(apath, os.O_CREAT | os.O_RDWR, mode)
            #f = open(apath, 'w+', mode)
            #fh = f.fileno()
        else:
            fi.fh = os.open(apath, os.O_CREAT | os.O_RDWR, mode)
            #f = open(apath, 'w+', mode)
            #fi.fh = f.fileno()
            fh = 0

        self.__save_attributes(path, "hgfs[create]: %s %o" % (_path, mode))

        return fh

    def destroy(self, path):
        try:
            dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-u', 'hgfs', '-m', 'hgfs[cruft]']))
            if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))
        finally:
            if self.args.clone: shutil.rmtree(self.tmp)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        return os.fsync(fh)

    fsyncdir = None
#    def fsyncdir(self, path, datasync, fh):
#        pass

    def getattr(self, path, fh=None):
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        st = os.lstat(apath)
        return dict((key, getattr(st, key)) for key in self.ATTRS)

    getxattr = None
#    def getxattr(self, path, name, position=0):
#        pass

    init = None
#    def init(self, path):
#        pass

    link = None
#    def link(self, target, source):
#        pass

    listxattr = None
#    def listxattr(self, path):
#        pass

    def mkdir(self, path, mode):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        status = os.mkdir(apath, mode)

        self.__save_attributes(path, "hgfs[mkdir]: %s %o" % (_path, mode))

        return status

    mknod = None

    def open(self, path, flags):
        if self.args.clone:
            dispatch(request(['--cwd', self.tmp, 'pull', '-u']))
            self.__load_attributes()

        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        return os.open(apath, flags)

#    opendir = None

    def read(self, path, size, offset, fh):
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        os.lseek(fh, offset, os.SEEK_SET)
        data = os.read(fh, size)

        return data

    def readdir(self, path, fh):
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        paths = os.listdir(apath)
        clean = []

        for path in paths:
            if path != '.hg' and path != '.hgfs':
                clean.append(path)

        return ['.', '..'] + clean

    def readlink(self, path):
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'pull', '-u']))

        apath = os.path.join(self.tmp, path[1:])
        return os.readlink(apath)

    def release(self, path, fh):
        return os.close(fh)

#    def releasedir(self, path, fh):
#        return os.close(fh)

    removexattr = None
#    def removexattr(self, path, name):
#        pass

    def rename(self, old, new):
        _old = old[1:]
        _new = new[1:]

        uid, gid, pid = fuse_get_context()
        username = pwd.getpwuid(uid)[0]

        dispatch(request(['--cwd', self.tmp, 'mv', _old, _new]))
        dispatch(request(['--cwd', self.tmp, 'mv', os.path.join('.hgfs', _old + '.attr'), os.path.join('.hgfs', _new + '.attr')]))
        dispatch(request(['--cwd', self.tmp, 'commit', '-u', username, '-m', "hgfs[rename]: %s -> %s" % (_old, _new), str(_old), str(_new)]))
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))

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

        uid, gid, pid = fuse_get_context()
        username = pwd.getpwuid(uid)[0]

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-u', username, '-m', "hgfs[rmdir]: %s" % (_path), str(_path)]))
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    setxattr = None
#    def setxattr(self, path, name, value, options, position=0):
#        pass

    def statfs(self, path):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        stv = os.statvfs(apath)

        return dict((key, getattr(stv, key)) for key in self.STATV)

    def symlink(self, target, source):
        _source = source[1:]
        _target = target[1:]

        asource = os.path.join(self.tmp, _source)
        atarget = os.path.join(self.tmp, _target)

        status = os.symlink(source, atarget)

        uid, gid, pid = fuse_get_context()
        username = pwd.getpwuid(uid)[0]

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-u', username, '-m', "hgfs[symlink]: %s -> %s" % (_target, source), str(source), str(_target)]))
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    # FIXME: You have to have this here, otherwise, fusepy won't call your
    # truncate with the fh set. This causes certain cases to fail where a file
    # was opened with write, but mode set to 0000. In that case you should NOT
    # be able to reopen the file, but you SHOULD be able to truncate the
    # existing handle. See iozone sanity check.
    def ftruncate(self, path, length, fh):
        pass

    def truncate(self, path, length, fh=None):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        if fh == None:
            with open(apath, 'wb') as f:
                pass
        else:
            os.ftruncate(fh, length)

        uid, gid, pid = fuse_get_context()
        username = pwd.getpwuid(uid)[0]

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-u', username, '-m', "hgfs[truncate]: %s" % (_path), str(_path)]))
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))

    def unlink(self, path):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)
        hpath = os.path.join(self.tmp, '.hgfs', _path + '.attr')

        status = os.unlink(apath)

        try:
            status = os.unlink(hpath)
        except:
            pass

        uid, gid, pid = fuse_get_context()
        username = pwd.getpwuid(uid)[0]

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-u', username, '-m', "hgfs[unlink]: %s" % (_path), str(_path)]))
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))

        return status

    def utimens(self, path, times=None):
        apath = os.path.join(self.tmp, path[1:])
        return os.utime(apath, times)

    def write(self, path, data, offset, fh):
        _path = path[1:]
        apath = os.path.join(self.tmp, _path)

        os.lseek(fh, offset, os.SEEK_SET)
        os.write(fh, data)

        uid, gid, pid = fuse_get_context()
        username = pwd.getpwuid(uid)[0]

        dispatch(request(['--cwd', self.tmp, 'commit', '-A', '-u', username, '-m', "hgfs[write]: %s" % (_path), str(_path)]))
        if self.args.clone: dispatch(request(['--cwd', self.tmp, 'push']))

        return len(data)

if __name__ == '__main__':
    logging.basicConfig()

    parser = argparse.ArgumentParser(description='HgFS')
    parser.add_argument('repository', help='Mercurial repository specifier.')
    parser.add_argument('mountpoint', help='Location of mount point.')

    parser.add_argument('-c', '--clone', help='Clone repository. If set to false, then repository MUST be local.', action='store', default='True')
    parser.add_argument('-l', '--log', help='Set the log level.', action='store', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='ERROR')

    args = parser.parse_args()

    args.clone = args.clone in ('True', 'true', '1')

    fuse = FUSE(HgFS(args.repository, args.mountpoint, args), args.mountpoint, foreground=True, nothreads=True)
