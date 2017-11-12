import bsddb
import sys
import glob
import os
import stat
import bsddb
from config import ConfigFile
import string

def read_data(path):
	df = os.open(path, os.O_RDONLY)
	data = ''
	while 1:
		d = os.read(df, 10000)
		if not d:	break
		data = data + d
	os.close(df)
	return data

def cvtDirRec(old_root, new_root, lpath):
	old_p_dir = old_root + '/' + lpath
	new_p_dir = new_root + '/' + lpath
	print 'cvtDirRec(%s)...' % (lpath,),
	#print 'cvtDirRec(%s): oldp=%s, newp=%s...' % (lpath, old_p_dir, new_p_dir)
	try:	os.makedirs(new_p_dir)
	except: pass
	dbfile = bsddb.btopen(new_p_dir + '/' + 'index.db', 'c')
	dbfile['..info..'] = read_data(old_p_dir + '/.info')
	dirs = []
	nfiles = 0
	for fn in glob.glob1(old_p_dir, '*'):
		lp = lpath + '/' + fn
		pp = old_p_dir + '/' + fn
		try:	st = os.stat(pp)
		except:	continue
		if stat.S_ISDIR(st[stat.ST_MODE]):
			dirs.append(fn)
		else:
			dbfile[fn] = read_data(pp)
			nfiles = nfiles + 1
	dbfile.sync()
	dbfile.close()
	print '%s files, %s subdirs' % (nfiles, len(dirs))
	for dn in dirs:
		cvtDirRec(old_root, new_root, lpath + '/' + dn)

if __name__ == '__main__':
	cfg = ConfigFile(os.environ['DFARM_CONFIG'])
	old_dbroot = cfg.getValue('vfssrv','*','db_root')
	if not old_dbroot:
		print 'VFS Database root directory is not defined in the configuration'
		sys.exit(1)
	one_up = string.join(string.split(old_dbroot, '/')[:-1],'/')
	new_dbroot = one_up + '/new_dbroot'
	save_dbroot = one_up + '/db_saved'
	cvtDirRec(old_dbroot, new_dbroot, '/')
	os.mkdir(new_dbroot + '/.cellinx')
	os.rename(old_dbroot, save_dbroot)
	os.rename(new_dbroot, old_dbroot)
	
	print 'VFS Database conversion is complete.'
	print 'Old database is renamed into %s' % save_dbroot
