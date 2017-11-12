/*
 * @(#) $Id: statfsmodule.c,v 1.1 2001/04/04 20:51:05 ivm Exp $
 *
 * $Log: statfsmodule.c,v $
 * Revision 1.1  2001/04/04 20:51:05  ivm
 * Added scripts, statfsmodule
 *
 * Revision 1.1  2000/08/14 16:02:01  ivm
 * Initial versions
 *
 */

#include "Python.h"

#ifdef	Linux
#include <vfs.h>
#else
#include	<sys/statfs.h>
#endif

static char *RCSInfo = "$Id: statfsmodule.c,v 1.1 2001/04/04 20:51:05 ivm Exp $";
static PyObject *PSVersion;

static char statfs__doc__[] =
"statfs(path) -> \n\
 (bsize, blocks, bfree, files, ffree)\n\
Perform a statfs system call on the given path.";

static PyObject *
error_with_filename(name)
	char* name;
{
	return PyErr_SetFromErrnoWithFilename(PyExc_OSError, name);
}

static PyObject *
my_statfs(self, args)
	PyObject *self;
	PyObject *args;
{
	char *path;
	int res;
	struct statfs st;
	if (!PyArg_ParseTuple(args, "s", &path))
		return NULL;
#ifdef	IRIX
	res = statfs(path, &st, sizeof(st), 0);
#else
	res = statfs(path, &st);
#endif
	if (res != 0)
		return error_with_filename(path);
	return Py_BuildValue("(lLLLL)",
		    (long) st.f_bsize,
		    (LONG_LONG) st.f_blocks,
#ifdef IRIX
		    (LONG_LONG) st.f_bfree,
#else	/* Linux */
		    (LONG_LONG) st.f_bavail,
#endif
		    (LONG_LONG) st.f_files,
		    (LONG_LONG) st.f_ffree);
}

#define MEGABYTE	(1024L*1024L)

static PyObject *
free_mb(self, args)
	PyObject *self;
	PyObject *args;
{
	char *path;
	int res;
	struct statfs st;
	unsigned long free_megs;
	
	if (!PyArg_ParseTuple(args, "s", &path))
		return NULL;
#ifdef	IRIX
	res = statfs(path, &st, sizeof(st), 0);
	if (res != 0)
		return error_with_filename(path);
	free_megs = st.f_bfree / (MEGABYTE/st.f_bsize);
#else
	res = statfs(path, &st);
	if (res != 0)
		return error_with_filename(path);
	free_megs = st.f_bavail / (MEGABYTE/st.f_bsize);
#endif
	return Py_BuildValue("L", (LONG_LONG) free_megs);
}

static PyMethodDef statfs_methods[] = {
	{"statfs",	my_statfs, 1, statfs__doc__},
	{"free_mb",	free_mb, 1, NULL},
	{NULL,		NULL}		 /* Sentinel */
};


void initstatfs()
{
	PyObject *m, *d;
	m = Py_InitModule("statfs", statfs_methods);
	d = PyModule_GetDict(m);
	PSVersion = PyString_FromString(RCSInfo);
	PyDict_SetItemString(d, "Version", PSVersion);
}
