#!/usr/bin/env python3

import locale
import os
import pipes
import shutil
import socket
import subprocess
import time

def pickport():
        """Pick a free port number to listen on. """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        port = s.getsockname()[1]
        s.close()
        return port

class MonetDBD:
    VERBOSE = False

    def __init__(self, farmdir, keep=False, set_snapdir=True):
        self.farmdir = farmdir
        self.keep = keep
        # It's generally a bad idea to put the snapshot dir inside the database
        # farm but this test depends on it, see .prepare_dir
        self.snapdir = os.path.join(farmdir, 'MTESTSNAPS')
        self.qfarmdir = pipes.quote(farmdir)
        self.qsnapdir = pipes.quote(self.snapdir)
        self.port = pickport()
        self.proc = None # filled in by .start_monetdb() below

        self.prepare_dir(set_snapdir)
        self.start_monetdbd()
        # the above generated all sorts of output, give reader a (line) break
        print()

    def prepare_dir(self, set_snapdir):
        try:
            os.mkdir(self.farmdir)
        except FileExistsError as e:
            self.remove_dir()
            os.mkdir(self.farmdir)
        # now it exists. create the snapdir, which also marks it as ours
        os.mkdir(self.snapdir)

        self.run_monetdbd('create')
        self.run_monetdbd('set', 'listenaddr=0.0.0.0')
        self.run_monetdbd('set', f'port={self.port}')
        if set_snapdir:
            self.run_monetdbd('set', f'snapshotdir={self.qsnapdir}')

    def remove_dir(self):
        if not os.listdir(self.farmdir) or os.path.isdir(self.snapdir):
            # it's empty or ours, feel free to delete it
            shutil.rmtree(self.farmdir)
        else:
            raise Exception(f"Directory {self.farmdir} is nonempty but does not contain our marker file") from None

    def run_command(self, cmd, output=False, timeout=10):
        if self.VERBOSE:
            print(f"- running {' '.join(pipes.quote(a) for a in cmd)}")
        if output:
            out = subprocess.check_output(cmd, timeout=timeout)
            out = str(out, locale.getlocale()[1])
            return out
        else:
            subprocess.check_call(cmd, timeout=timeout)
            return None

    def run_monetdbd(self, *args, output=False):
        cmd = ['monetdbd', *args, self.qfarmdir]
        return self.run_command(cmd, output)

    def run_monetdb(self, *args, output=False):
        cmd = ['monetdb', '-p', str(self.port), *args]
        return self.run_command(cmd, output)

    def run_mclient(self, *args, db=None, output=False):
        if not db:
            raise Exception("forgot to pass db=")
        cmd = ['mclient', '-p', str(self.port), '-d', db, *args]
        return self.run_command(cmd, output)

    def start_monetdbd(self):
        cmd = ['monetdbd', 'start', '-n', self.qfarmdir]
        self.proc = subprocess.Popen(cmd)
        time.sleep(1)
        if self.proc.poll() != None:
            raise Exception(f"Process {cmd} exited early with status {self.proc.returncode}")

        for db in "foo1 foo2 bar".split():
            self.run_monetdb('create', db)
            self.run_monetdb('release', db)
            self.run_mclient('-s', 'create table t(s text)', db=db)
            self.run_mclient('-s', f"insert into t values ('{db}')", db=db)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.proc:
            self.run_monetdb('stop', '-a')
            self.proc.terminate()
        if exc_type == None:
            # clean exit
            if not self.keep:
                self.remove_dir()
        return False # do not suppress any exceptions
