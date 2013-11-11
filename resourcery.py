#!/usr/bin/env python
import warnings
import datetime as dt
import tempfile as tf
import sqlite3 as sql
import argparse as argp
import os.path as path
import os
import re
import shutil

def copytree(src, dst, symlinks=False):
    names = os.listdir(src)
    dst = path.normpath(dst)
    if path.isdir(dst):
        pass
    else:
        os.makedirs(dst)
    errors = []
    for name in names:
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        try:
            if symlinks and os.path.islink(srcname):
                linkto = os.readlink(srcname)
                os.symlink(linkto, dstname)
            elif os.path.isdir(srcname):
                copytree(srcname, dstname, symlinks)
            else:
                shutil.copy2(srcname, dstname)
            # XXX What about devices, sockets etc.?
        except (IOError, os.error) as why:
            errors.append((srcname, dstname, str(why)))
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except Error as err:
            errors.extend(err.args[0])
    try:
        shutil.copystat(src, dst)
    except WindowsError:
        # can't copy file access times on Windows
        pass
    except OSError as why:
        errors.extend((src, dst, str(why)))
    if errors:
        raise Error(errors)

class Resource:
    """Basic class to hold data on a resource.
    """
    def __init__(self, root, supplier, tags, category, comment):
        self.accession_number = None
        self.accession_time = dt.datetime.now()
        self.root = path.normpath(root)
        self.supplier = supplier
        self.tags = tags
        self.category = category
        self.comment = comment
        self.check_root()

    def check_root(self):
        if not path.exists(self.root):
            raise NameError('Resource root path does not exist.')


class Archive:
    """Basic class to manage resources.
    """
    def __init__(self, archive_root, init):
        self.archive_root = path.normpath(archive_root)
        self.connection = None
        self.cursor = None
        self.connect(init)

    def connect(self, init):
        self.check_root(init)
        archive_tables = path.join(self.archive_root,'archive.sqlite')
        self.connection = sql.connect(archive_tables)
        self.cursor = self.connection.cursor()
        if init:
            os.mkdir(path.join(self.archive_root,'store'))
            print(init)
            self.create_tables()
            init = False
        self.check_archive(init)

    def check_root(self, init):
        no_root = not path.exists(self.archive_root)
        not_dir = path.exists(self.archive_root) and not path.isdir(self.archive_root)
        if not_dir:
            raise NameError('Archive root exists but is not a directory, check archive root: ' + self.archive_root)
        if no_root:
            if init:
                os.mkdir(self.archive_root)
            elif not init:
                raise NameError('Archive root path does not exist.')

    def check_archive(self, init):
        no_root = not path.exists(self.archive_root)
        not_dir = path.exists(self.archive_root) and not path.isdir(self.archive_root)
        no_store = not path.isdir(path.join(self.archive_root,'store'))
        tables = path.join(self.archive_root,'archive.sqlite')
        tables_exist = path.exists(tables)
        no_db = not tables_exist
        no_clobber = tables_exist and init
        warn_create = not tables_exist and init
        if not_dir:
            raise NameError('Archive root exists but is not a directory, check archive root: ' + self.archive_root)
        if no_clobber:
            raise NameError('Archive tables already axist at: ' + tables)
        if no_root:
            if init:
                os.mkdir(self.archive_root)
            elif not init:
                raise NameError('Archive root path does not exist.')
        if no_db:
            if init:
                warnings.warn('Archive database does not exist, will create at: ' + path.join(self.archive_root,'archive.sqlite'))
                self.create_tables()
            elif not init:
                raise NameError('Archive database does not exist.')
        if no_store:
            if init:
                warnings.warn('Store directory does not exist, will create at: ' + path.join(self.archive_root,'store'))
                os.mkdir(path.join(self.archive_root,'store'))
            elif not init:
                raise NameError('Store directory does not exist, will not create.')


    def create_tables(self):
        print('Creating tables...')
        self.cursor.execute('''create table catalog
            (id integer primary key, atime timestamp, root varchar unique, supplier varchar, comment text);
        ''')
        self.cursor.execute('''create table tags
            (id integer primary key, anumber integer, tag text);
        ''')

    def get_resource_path(self, key):
        self.check_archive(init=False)
        self.cursor.execute('''select root from catalog where id = ?;''', key)
        for perm_path in self.cursor:
            return(perm_path)

    def list_resources(self):
        self.check_archive(init=False)
        self.cursor.execute('''select * from catalog;''')
        for resource in self.cursor:
            print(resource)

    def add_resource(self, resource, keep):
        self.check_archive(init=False)
        # 1) Move files to new_root
        new_root = tf.mkdtemp(
            suffix = '-' + re.sub(' ', '_', str(resource.accession_time)),
            prefix = resource.category + '-', dir=path.join(self.archive_root,'store'))
        copytree(resource.root,new_root)

        # 2) Add entry on new accession to catalog.
        new_root_store = path.relpath(path=new_root, start=self.archive_root)
        print(new_root_store)
        self.cursor.execute('insert into catalog (atime,root,supplier,comment) values (?,?,?,?);',
            (resource.accession_time, new_root_store, resource.supplier, resource.comment))

        # 2.1) Get the id
        resource.accession_number = self.cursor.lastrowid

        # 3) Add entry on new accession's tags to tags table.
        for tag in resource.tags:
            self.cursor.execute('insert into tags (anumber,tag) values (?,?);',
                (resource.accession_number, tag))

        # 4) Remove the old root...
        if not keep:
            if path.isdir(resource.root):
                os.rmdir(resource.root)
            else:
                os.remove(resource.root)

        # 5) Save
        self.connection.commit()
        self.cursor.close()

if __name__=="__main__":
    """CLI for the Resourcery.
    """
    parser = argp.ArgumentParser(description='Resource archiving software.')
    def is_true(val):
        if val.lower() in ['t','true']:
            return(True)
        elif val.lower() in ['f','false']:
            return(False)
        else:
            msg = "'{}' is not a boolean".format(val)
            raise argp.ArgumentTypeError(msg)
    parser.add_argument(
        '-d', '--debug', dest='debug',
        action='store', default=False,
        required=False,
        type=is_true, help='Should debug messages be issued?')
    parser.add_argument(
        '-z', '--zrob', dest='zrob',
        action='store', default=None,
        required=True,
        type=str, help='Action to perform.')
    parser.add_argument(
        '-r', '--resource', dest='root',
        action='store', default=None,
        required=False,
        type=str, help='Root path to place in archive.')
    parser.add_argument(
        '-s', '--supplier', dest='supplier',
        action='store', default=None,
        required=False,
        type=str, help='Name of entity which provided the item.')
    parser.add_argument(
        '-g', '--group', dest='category',
        action='store', default=None,
        required=False,
        type=str, help='Category to place the resource in.')
    parser.add_argument(
        '-t', '--tags', dest='tags',
        action='store', default=None,
        required=False,
        type=str, help='Tags to associate with the resource.')
    parser.add_argument(
        '-a', '--archive', dest='archive',
        action='store', default=None,
        required=True,
        type=str, help='Root of the archive to add to.')
    #parser.add_argument(
        #'-k', '--keep', dest='keep',
        #action='store', default=False,
        #required=False,
        #type=is_true, help='Should the original resource file/root be kept.')
    parser.add_argument(
        '-i', '--init', dest='initialize',
        action='store', default=False,
        required=False,
        type=is_true, help='Initialize a new archive.')
    parser.add_argument(
        '-c', '--comment', dest='comment',
        action='store', default = '',
        required=False,
        type=str, help='Comment to associate with the resource.')
    parser.add_argument(
        '-k', '--key', dest='key',
        action='store', default = 1,
        required=False,
        type=str, help='Key of the resource to retrieve.')
    args = parser.parse_args()
    if args.debug:
        print(args)

    archive = Archive(
        archive_root = args.archive,
        init = args.initialize
    )
    if args.zrob == 'add':
        resource = Resource(
            root = args.root,
            supplier = args.supplier,
            tags = args.tags.split(','),
            category = args.category,
            comment = args.comment
        )
        archive.add_resource(resource, keep = True)
    elif args.zrob == 'get_path':
        perm_path = archive.get_resource_path(key = args.key)[0]
        print(perm_path)
    elif args.zrob == 'get_resource':
        perm_path = archive.get_resource_path(key = args.key)[0]
        copytree(perm_path,args.root)
    elif args.zrob == 'list_resources':
        archive.list_resources()

# resourcery.py -r /var/lib/data/michaelMorrissey/leGrandModelDeux/ATSdMDataWBAIS4.RData -s "Ben Letcher" -g data -t dMData,salmon,ATS -a . -k True -c "Installing dMData file from Michael." -i True -z add
# resourcery.py -a /var/lib/data/store -z list_resources
# resourcery.py -a /var/lib/data/store -z get_path --k 1
# resourcery.py -a /var/lib/data/store -z get_resource -k 1 -r /tmp
