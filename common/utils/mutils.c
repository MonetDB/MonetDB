/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "mutils.h"
#include "mstring.h"

#ifdef HAVE_MACH_O_DYLD_H
# include <mach-o/dyld.h>  /* _NSGetExecutablePath on OSX >=10.5 */
#endif

#include <limits.h>		/* PATH_MAX on Solaris */

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>  /* realpath on OSX */
#endif

#ifdef BSD /* BSD macro is defined in sys/param.h */
# include <sys/sysctl.h>  /* KERN_PROC_PATHNAME on BSD */
#endif

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

#ifdef NATIVE_WIN32

#include <windows.h>
#include <wchar.h>

/* Some definitions that we need to compile on Windows.
 * Note that Windows only runs on little endian architectures. */
#define BIG_ENDIAN	4321
#define LITTLE_ENDIAN	1234
#define BYTE_ORDER	LITTLE_ENDIAN

wchar_t *
utf8towchar(const char *src)
{
	wchar_t *dest;
	size_t i = 0;
	size_t j = 0;
	uint32_t c;

	if (src == NULL)
		return NULL;

	/* count how many wchar_t's we need, while also checking for
	 * correctness of the input */
	while (src[j]) {
		i++;
		if ((src[j+0] & 0x80) == 0) {
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+0] & 0x1E) != 0) {
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && ((src[j+0] & 0x0F) != 0
			       || (src[j+1] & 0x20) != 0)) {
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && (src[j+3] & 0xC0) == 0x80) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
			if (c < 0x10000
			    || c > 0x10FFFF
			    || (c & 0x1FF800) == 0x00D800) {
				return NULL;
			}
#if SIZEOF_WCHAR_T == 2
			i++;
#endif
			j += 4;
		} else {
			return NULL;
		}
	}
	dest = malloc((i + 1) * sizeof(wchar_t));
	if (dest == NULL)
		return NULL;
	/* go through the source string again, this time we can skip
	 * the correctness tests */
	i = j = 0;
	while (src[j]) {
		if ((src[j+0] & 0x80) == 0) {
			dest[i++] = src[j+0];
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0) {
			dest[i++] = (src[j+0] & 0x1F) << 6
				| (src[j+1] & 0x3F);
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0) {
			dest[i++] = (src[j+0] & 0x0F) << 12
				| (src[j+1] & 0x3F) << 6
				| (src[j+2] & 0x3F);
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
#if SIZEOF_WCHAR_T == 2
			dest[i++] = 0xD800 | ((c - 0x10000) >> 10);
			dest[i++] = 0xDE00 | (c & 0x3FF);
#else
			dest[i++] = c;
#endif
			j += 4;
		}
	}
	dest[i] = 0;

	/* dir manipulations fail in WIN32 if file name contains trailing
	 * slashes; work around this */
	while (i > 2 && dest[i - 1] == L'\\' && dest[i - 2] != L':')
		dest[--i] = 0;

	return dest;
}

char *
wchartoutf8(const wchar_t *ws)
{
	size_t len = 1;
	for (size_t i = 0; ws[i]; i++) {
		if (ws[i] <= 0x7F)
			len += 1;
		else if (ws[i] <= 0x7FF)
			len += 2;
		else if (
#if SIZEOF_WCHAR_T == 2
			(ws[i] & 0xF800) != 0xD800
#else
			ws[i] <= 0xFFFF
#endif
			) {
			assert((ws[i] & 0xF800) != 0xD800);
			len += 3;
		} else {
#if SIZEOF_WCHAR_T == 2
			assert((ws[i + 0] & 0xFC00) == 0xD800); /* high surrogate */
			assert((ws[i + 1] & 0xFC00) == 0xDC00); /* low surrogate */
			len += 4;
			i++;
#else
			assert(ws[i] <= 0x10FFFF);
			len += 4;
#endif
		}
	}
	unsigned char *us = malloc(len);
	if (us != NULL) {
		size_t j = 0;
		for (size_t i = 0; ws[i]; i++) {
			if (ws[i] <= 0x7F)
				us[j++] = (unsigned char) ws[i];
			else if (ws[i] <= 0x7FF) {
				us[j++] = (unsigned char) (ws[i] >> 6 | 0xC0);
				us[j++] = (unsigned char) ((ws[i] & 0x3F) | 0x80);
			} else if (
#if SIZEOF_WCHAR_T == 2
				(ws[i] & 0xF800) != 0xD800
#else
				ws[i] <= 0xFFFF
#endif
				) {
				us[j++] = (unsigned char) (ws[i] >> 12 | 0xE0);
				us[j++] = (unsigned char) (((ws[i] >> 6) & 0x3F) | 0x80);
				us[j++] = (unsigned char) ((ws[i] & 0x3F) | 0x80);
			} else {
				uint32_t wc;
#if SIZEOF_WCHAR_T == 2
				wc = ((ws[i+0] & 0x03FF) + 0x40) << 10 | (ws[i+1] & 0x03FF);
				i++;
#else
				wc = (uint32_t) ws[i];
#endif
				us[j++] = (unsigned char) (wc >> 18 | 0xF0);
				us[j++] = (unsigned char) (((wc >> 12) & 0x3F) | 0x80);
				us[j++] = (unsigned char) (((wc >> 6) & 0x3F) | 0x80);
				us[j++] = (unsigned char) ((wc & 0x3F) | 0x80);
			}
		}
		us[j] = 0;
	}
	return (char *) us;
}

/* translate Windows error code (GetLastError()) to Unix-style error */
int
winerror(int e)
{
	switch (e) {
	case ERROR_BAD_ENVIRONMENT:
		return E2BIG;
	case ERROR_ACCESS_DENIED:
	case ERROR_CANNOT_MAKE:
	case ERROR_CURRENT_DIRECTORY:
	case ERROR_DRIVE_LOCKED:
	case ERROR_FAIL_I24:
	case ERROR_LOCK_FAILED:
	case ERROR_LOCK_VIOLATION:
	case ERROR_NETWORK_ACCESS_DENIED:
	case ERROR_NOT_LOCKED:
	case ERROR_SEEK_ON_DEVICE:
		return EACCES;
	case ERROR_MAX_THRDS_REACHED:
	case ERROR_NESTING_NOT_ALLOWED:
	case ERROR_NO_PROC_SLOTS:
		return EAGAIN;
	case ERROR_DIRECT_ACCESS_HANDLE:
	case ERROR_INVALID_TARGET_HANDLE:
		return EBADF;
	case ERROR_CHILD_NOT_COMPLETE:
	case ERROR_WAIT_NO_CHILDREN:
		return ECHILD;
	case ERROR_ALREADY_EXISTS:
	case ERROR_FILE_EXISTS:
		return EEXIST;
	case ERROR_INVALID_ACCESS:
	case ERROR_INVALID_DATA:
	case ERROR_INVALID_FUNCTION:
	case ERROR_INVALID_HANDLE:
	case ERROR_INVALID_PARAMETER:
	case ERROR_NEGATIVE_SEEK:
		return EINVAL;
	case ERROR_TOO_MANY_OPEN_FILES:
		return EMFILE;
	case ERROR_BAD_NET_NAME:
	case ERROR_BAD_NETPATH:
	case ERROR_BAD_PATHNAME:
	case ERROR_FILE_NOT_FOUND:
	case ERROR_FILENAME_EXCED_RANGE:
	case ERROR_INVALID_DRIVE:
	case ERROR_NO_MORE_FILES:
	case ERROR_PATH_NOT_FOUND:
		return ENOENT;
	case ERROR_BAD_FORMAT:
		return ENOEXEC;
	case ERROR_ARENA_TRASHED:
	case ERROR_INVALID_BLOCK:
	case ERROR_NOT_ENOUGH_MEMORY:
	case ERROR_NOT_ENOUGH_QUOTA:
		return ENOMEM;
	case ERROR_DISK_FULL:
		return ENOSPC;
	case ERROR_DIR_NOT_EMPTY:
		return ENOTEMPTY;
	case ERROR_BROKEN_PIPE:
		return EPIPE;
	case ERROR_NOT_SAME_DEVICE:
		return EXDEV;
	default:
		return EINVAL;
	}
}

struct DIR {
	wchar_t *dir_name;
	int just_opened;
	HANDLE find_file_handle;
	void *find_file_data;
	struct dirent result;
};

DIR *
opendir(const char *dirname)
{
	DIR *result = NULL;
	wchar_t *mask;
	size_t k;
	DWORD e;

	if (dirname == NULL) {
		errno = EFAULT;
		return NULL;
	}

	result = (DIR *) malloc(sizeof(DIR));
	if (result == NULL) {
		errno = ENOMEM;
		return NULL;
	}
	result->find_file_data = malloc(sizeof(WIN32_FIND_DATAW));
	result->dir_name = utf8towchar(dirname);
	if (result->find_file_data == NULL || result->dir_name == NULL) {
		if (result->find_file_data)
			free(result->find_file_data);
		if (result->dir_name)
			free(result->dir_name);
		free(result);
		errno = ENOMEM;
		return NULL;
	}

	k = wcslen(result->dir_name);
	if (k && result->dir_name[k - 1] == L'\\') {
		result->dir_name[k - 1] = L'\0';
		k--;
	}
	size_t masklen = (wcslen(result->dir_name) + 3) * sizeof(wchar_t);
	mask = malloc(masklen);
	if (mask == NULL) {
		free(result->find_file_data);
		free(result->dir_name);
		free(result);
		errno = ENOMEM;
		return NULL;
	}
	swprintf(mask, masklen, L"%ls\\*", result->dir_name);

	result->find_file_handle = FindFirstFileW(mask, (LPWIN32_FIND_DATAW) result->find_file_data);
	if (result->find_file_handle == INVALID_HANDLE_VALUE) {
		e = GetLastError();
		free(mask);
		free(result->dir_name);
		free(result->find_file_data);
		free(result);
		SetLastError(e);
		errno = winerror(e);
		return NULL;
	}
	free(mask);
	result->just_opened = TRUE;

	return result;
}

static wchar_t *
basename(const wchar_t *file_name)
{
	const wchar_t *p;
	const wchar_t *base;

	if (file_name == NULL)
		return NULL;

	if (iswalpha(file_name[0]) && file_name[1] == L':')
		file_name += 2;	/* skip over drive letter */

	base = NULL;
	for (p = file_name; *p; p++)
		if (*p == L'\\' || *p == L'/')
			base = p;
	if (base)
		return (wchar_t *) base + 1;

	return (wchar_t *) file_name;
}

struct dirent *
readdir(DIR *dir)
{
	char *base;

	if (dir == NULL) {
		errno = EFAULT;
		return NULL;
	}

	if (dir->just_opened)
		dir->just_opened = FALSE;
	else if (!FindNextFileW(dir->find_file_handle,
			       (LPWIN32_FIND_DATAW) dir->find_file_data))
		return NULL;
	base = wchartoutf8(basename(((LPWIN32_FIND_DATAW) dir->find_file_data)->cFileName));
	if (base == NULL)
		return NULL;
	strcpy_len(dir->result.d_name, base, sizeof(dir->result.d_name));
	free(base);
	dir->result.d_namelen = (int) strlen(dir->result.d_name);

	return &dir->result;
}

void
rewinddir(DIR *dir)
{
	wchar_t *mask;

	if (dir == NULL) {
		errno = EFAULT;
		return;
	}

	if (!FindClose(dir->find_file_handle))
		fprintf(stderr, "#rewinddir(): FindClose() failed\n");

	size_t masklen = (wcslen(dir->dir_name) + 3) * sizeof(wchar_t);
	mask = malloc(masklen);
	if (mask == NULL) {
		errno = ENOMEM;
		dir->find_file_handle = INVALID_HANDLE_VALUE;
		return;
	}
	swprintf(mask, masklen, L"%ls\\*", dir->dir_name);
	dir->find_file_handle = FindFirstFileW(mask, (LPWIN32_FIND_DATAW) dir->find_file_data);
	free(mask);
	if (dir->find_file_handle == INVALID_HANDLE_VALUE)
		return;
	dir->just_opened = TRUE;
}

int
closedir(DIR *dir)
{
	if (dir == NULL) {
		errno = EFAULT;
		return -1;
	}

	if (!FindClose(dir->find_file_handle))
		return -1;

	free(dir->dir_name);
	free(dir->find_file_data);
	free(dir);

	return 0;
}

char *
dirname(char *path)
{
	char *p, *q;

	for (p = path, q = NULL; *p; p++)
		if (*p == '/' || *p == '\\')
			q = p;
	if (q == NULL)
		return ".";
	*q = '\0';
	return path;
}

/* see contract of unix MT_lockf */
int
MT_lockf(const char *filename, int mode)
{
	int ret = 1, fd = -1;
	OVERLAPPED ov;
	HANDLE fh;
	static struct lockedfiles {
		struct lockedfiles *next;
		wchar_t *wfilename;
		int fildes;
	} *lockedfiles;
	struct lockedfiles **fpp, *fp;
	wchar_t *wfilename;

	if ((wfilename = utf8towchar(filename)) == NULL)
		return -2;
	ov = (OVERLAPPED) {0};
#if defined(DUMMYSTRUCTNAME) && (defined(NONAMELESSUNION) || !defined(_MSC_EXTENSIONS))	/* Windows SDK v7.0 */
	ov.u.s.Offset = 4;
	ov.u.s.OffsetHigh = 0;
#else
	ov.Offset = 4;
	ov.OffsetHigh = 0;
#endif

	if (mode == F_ULOCK) {
		for (fpp = &lockedfiles; (fp = *fpp) != NULL; fpp = &fp->next) {
			if (wcscmp(fp->wfilename, wfilename) == 0) {
				free(fp->wfilename);
				fd = fp->fildes;
				fh = (HANDLE) _get_osfhandle(fd);
				fp = *fpp;
				*fpp = fp->next;
				free(fp);
				ret = UnlockFileEx(fh, 0, 1, 0, &ov);
				free(wfilename);
				return ret ? 0 : -1;
			}
		}
		/* didn't find the locked file, try opening the file
		 * directly */
		fh = CreateFileW(wfilename,
				GENERIC_READ | GENERIC_WRITE, 0,
				NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
		free(wfilename);
		if (fh == INVALID_HANDLE_VALUE)
			return -2;
		ret = UnlockFileEx(fh, 0, 1, 0, &ov);
		CloseHandle(fh);
		return 0;
	}

	if (_wsopen_s(&fd, wfilename, _O_CREAT | _O_RDWR | _O_TEXT, _SH_DENYNO, _S_IREAD | _S_IWRITE) != 0) {
		free(wfilename);
		return -2;
	}
	fh = (HANDLE) _get_osfhandle(fd);
	if (fh == INVALID_HANDLE_VALUE) {
		close(fd);
		free(wfilename);
		return -2;
	}

	if (mode == F_TLOCK) {
		ret = LockFileEx(fh, LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &ov);
	} else if (mode == F_LOCK) {
		ret = LockFileEx(fh, LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &ov);
	} else if (mode == F_TEST) {
		ret = LockFileEx(fh, LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &ov);
		if (ret != 0) {
			UnlockFileEx(fh, 0, 1, 0, &ov);
			close(fd);
			free(wfilename);
			return 0;
		}
	} else {
		close(fd);
		errno = EINVAL;
		free(wfilename);
		return -2;
	}
	if (ret != 0) {
		if ((fp = malloc(sizeof(*fp))) != NULL) {
			fp->wfilename = wfilename;
			fp->fildes = fd;
			fp->next = lockedfiles;
			lockedfiles = fp;
		} else {
			free(wfilename);
		}
		return fd;
	} else {
		close(fd);
		free(wfilename);
		return -1;
	}
}

FILE *
MT_fopen(const char *filename, const char *mode)
{
	wchar_t *wfilename, *wmode;
	wfilename = utf8towchar(filename);
	wmode = utf8towchar(mode);
	FILE *f = NULL;
	if (wfilename != NULL && wmode != NULL)
		f = _wfopen(wfilename, wmode);
	free(wfilename);
	free(wmode);
	return f;
}

int
MT_open(const char *filename, int flags)
{
	wchar_t *wfilename = utf8towchar(filename);
	if (wfilename == NULL)
		return -1;
	int fd;
	if (_wsopen_s(&fd, wfilename, flags, _SH_DENYNO, _S_IREAD | _S_IWRITE) != 0)
		fd = -1;
	free(wfilename);
	return fd;
}

int
MT_stat(const char *pathname, struct _stat64 *st)
{
	wchar_t *wpathname = utf8towchar(pathname);
	int ret;
	if (wpathname == NULL)
		return -1;

	ret = _wstat64(wpathname, st);
	free(wpathname);
	return ret;
}

int
MT_rmdir(const char *pathname)
{
	wchar_t *wpathname = utf8towchar(pathname);
	int ret;
	if (wpathname == NULL)
		return -1;

	ret = _wrmdir(wpathname);
#if 0
	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		TRC_DEBUG(IO_, "Retry rmdir %s\n", pathname);
		MT_sleep_ms(100);	/* wait a little */
		ret = _wrmdir(wpathname);
	}
#endif
	free(wpathname);
	return ret;
}

int
MT_remove(const char *pathname)
{
	wchar_t *wpathname = utf8towchar(pathname);
	int ret;
	if (wpathname == NULL)
		return -1;

	SetFileAttributesW(wpathname, FILE_ATTRIBUTE_NORMAL);
	ret = _wunlink(wpathname);
#if 0
	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		TRC_DEBUG(IO_, "Retry unlink %s\n", pathname);
		MT_sleep_ms(100);	/* wait a little */
		ret = _wunlink(wpathname);
	}
#endif
	free(wpathname);
	return ret;
}

int
MT_rename(const char *old, const char *dst)
{
	int ret = -1;
	wchar_t *wold, *wdst;
	wold = utf8towchar(old);
	wdst = utf8towchar(dst);

	if (wold && wdst) {
		ret = _wrename(wold, wdst);
		if (ret < 0 && errno == EEXIST) {
			(void) _wunlink(wdst);
			ret = _wrename(wold, wdst);
		}
#if 0
		if (ret < 0 && errno != ENOENT) {
			/* it could be the <expletive deleted> indexing
			 * service which prevents us from doing what we have a
			 * right to do, so try again (once) */
			TRC_DEBUG(IO_, "Retry rename %s %s\n", old, dst);
			MT_sleep_ms(100);	/* wait a little */
			ret = _wrename(wold, wdst);
		}
#endif
	}
	free(wold);
	free(wdst);
	return ret;
}

int
MT_mkdir(const char *pathname)
{
	wchar_t *wpathname = utf8towchar(pathname);
	if (wpathname == NULL)
		return -1;
	int ret = _wmkdir(wpathname);
	free(wpathname);
	return ret;
}

char *
MT_getcwd(char *buffer, size_t size)
{
	wchar_t *wcwd = _wgetcwd(NULL, 0);
	if (wcwd == NULL)
		return NULL;
	char *cwd = wchartoutf8(wcwd);
	free(wcwd);
	if (cwd == NULL)
		return NULL;
	size_t len = strcpy_len(buffer, cwd, size);
	free(cwd);
	return len < size ? buffer : NULL;
}

int
MT_access(const char *pathname, int mode)
{
	wchar_t *wpathname = utf8towchar(pathname);
	if (wpathname == NULL)
		return -1;
	int ret = _waccess(wpathname, mode);
	free(wpathname);
	return ret;
}

#else

#if defined(HAVE_LOCKF) && defined(__MACH__)
/* lockf() seems to be there, but I didn't find any header file that
   declares the prototype ... */
extern int lockf(int fd, int cmd, off_t len);
#endif

#ifndef HAVE_LOCKF
/* Cygwin implementation: struct flock is there, but lockf() is
   missing.
 */
static int
lockf(int fd, int cmd, off_t len)
{
	struct flock l;

	if (cmd == F_LOCK || cmd == F_TLOCK)
		l.l_type = F_WRLCK;
	else if (cmd == F_ULOCK)
		l.l_type = F_UNLCK;
	l.l_whence = SEEK_CUR;
	l.l_start = 0;
	l.l_len = len;
	return fcntl(fd, cmd == F_TLOCK ? F_SETLKW : F_SETLK, &l);
}
#endif

#ifndef O_TEXT
#define O_TEXT 0
#endif
/* returns -1 when locking failed,
 * returns -2 when the lock file could not be opened/created
 * returns 0 when mode is F_TEST and the lock file was not locked
 * returns the (open) file descriptor to the file when locking
 * returns 0 when unlocking */
int
MT_lockf(const char *filename, int mode)
{
	int fd = open(filename, O_CREAT | O_RDWR | O_TEXT | O_CLOEXEC, MONETDB_MODE);

	if (fd < 0)
		return -2;

	if (lseek(fd, 4, SEEK_SET) >= 0 &&
	    lockf(fd, mode, 1) == 0) {
		if (mode == F_ULOCK || mode == F_TEST) {
			close(fd);
			return 0;
		}
		/* do not close else we lose the lock we want */
		(void) lseek(fd, 0, SEEK_SET); /* move seek pointer back */
		return fd;
	}
	close(fd);
	return -1;
}

#endif

#ifndef PATH_MAX
# define PATH_MAX 1024
#endif
static char _bin_path[PATH_MAX];
char *
get_bin_path(void)
{
	/* getting the path to the executable's binary, isn't all that
	 * simple, unfortunately */
#ifdef NATIVE_WIN32
	static wchar_t wbin_path[PATH_MAX];
	if (GetModuleFileNameW(NULL, wbin_path, PATH_MAX) != 0) {
		char *path = wchartoutf8(wbin_path);
		size_t len = strcpy_len(_bin_path, path, PATH_MAX);
		free(path);
		if (len < PATH_MAX)
			return _bin_path;
	}
#elif defined(HAVE__NSGETEXECUTABLEPATH)  /* Darwin/OSX */
	char buf[PATH_MAX];
	uint32_t size = PATH_MAX;
	if (_NSGetExecutablePath(buf, &size) == 0 &&
			realpath(buf, _bin_path) != NULL)
	return _bin_path;
#elif defined(BSD) && defined(KERN_PROC_PATHNAME)  /* BSD */
	int mib[4];
	size_t cb = sizeof(_bin_path);
	mib[0] = CTL_KERN;
	mib[1] = KERN_PROC;
	mib[2] = KERN_PROC_PATHNAME;
	mib[3] = -1;
	if (sysctl(mib, 4, _bin_path, &cb, NULL, 0) == 0)
		return _bin_path;
#elif defined(HAVE_GETEXECNAME)  /* Solaris */
	char buf[PATH_MAX];
	const char *execn = getexecname();
	/* getexecname doesn't always return an absolute path, the only
	 * thing it seems to do is strip leading ./ from the invocation
	 * string. */
	if (*execn != '/') {
		if (getcwd(buf, PATH_MAX) != NULL) {
			snprintf(buf + strlen(buf), PATH_MAX - strlen(buf), "/%s", execn);
			if (realpath(buf, _bin_path) != NULL)
				return(_bin_path);
		}
	} else {
		if (realpath(execn, _bin_path) != NULL)
			return(_bin_path);
	}
#else  /* try Linux approach, also works on Cygwin */
	if (readlink("/proc/self/exe",
				_bin_path, sizeof(_bin_path)) != -1)
			return _bin_path;
#endif
	/* could use argv[0] (passed) to deduce location based on PATH, but
	 * that's a lot of work and unreliable */
	return NULL;
}
