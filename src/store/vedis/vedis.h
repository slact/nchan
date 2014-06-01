/* This file was automatically generated.  Do not edit (Except for compile time directives)! */ 
#ifndef  _VEDIS_H_
#define  _VEDIS_H_
/*
 * Symisc Vedis: A Highly Efficient Embeddable Data Store Engine.
 * Copyright (C) 2013, Symisc Systems http://vedis.symisc.net/
 * Version 1.2.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.symisc.net/
 */
/*
 * Copyright (C) 2013 Symisc Systems, S.U.A.R.L [M.I.A.G Mrad Chems Eddine <chm@symisc.net>].
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Redistributions in any form must be accompanied by information on
 *    how to obtain complete source code for the Vedis engine and any 
 *    accompanying software that uses the Vedis engine software.
 *    The source code must either be included in the distribution
 *    or be available for no more than the cost of distribution plus
 *    a nominal fee, and must be freely redistributable under reasonable
 *    conditions. For an executable file, complete source code means
 *    the source code for all modules it contains.It does not include
 *    source code for modules or files that typically accompany the major
 *    components of the operating system on which the executable file runs.
 *
 * THIS SOFTWARE IS PROVIDED BY SYMISC SYSTEMS ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
 * NON-INFRINGEMENT, ARE DISCLAIMED.  IN NO EVENT SHALL SYMISC SYSTEMS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
 * IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/* Make sure we can call this stuff from C++ */
#ifdef __cplusplus
extern "C" { 
#endif
 /* $SymiscID: vedis.h v1.2 Unix 2013-09-16 00:38 stable <chm@symisc.net> $ */
#include <stdarg.h> /* needed for the definition of va_list */
/*
 * Compile time engine version, signature, identification in the symisc source tree
 * and copyright notice.
 * Each macro have an equivalent C interface associated with it that provide the same
 * information but are associated with the library instead of the header file.
 * Refer to [vedis_lib_version()], [vedis_lib_signature()], [vedis_lib_ident()] and
 * [vedis_lib_copyright()] for more information.
 */
/*
 * The VEDIS_VERSION C preprocessor macroevaluates to a string literal
 * that is the vedis version in the format "X.Y.Z" where X is the major
 * version number and Y is the minor version number and Z is the release
 * number.
 */
#define VEDIS_VERSION "1.2.6"
/*
 * The VEDIS_VERSION_NUMBER C preprocessor macro resolves to an integer
 * with the value (X*1000000 + Y*1000 + Z) where X, Y, and Z are the same
 * numbers used in [VEDIS_VERSION].
 */
#define VEDIS_VERSION_NUMBER 1002006
/*
 * The VEDIS_SIG C preprocessor macro evaluates to a string
 * literal which is the public signature of the vedis engine.
 */
#define VEDIS_SIG "vedis/1.2.6"
/*
 * Vedis identification in the Symisc source tree:
 * Each particular check-in of a particular software released
 * by symisc systems have an unique identifier associated with it.
 * This macro hold the one associated with vedis.
 */
#define VEDIS_IDENT "vedis:e361b2f3d4a71ac17e9f2ac1876232a13467dea1"
/*
 * Copyright notice.
 * If you have any questions about the licensing situation, please
 * visit http://vedis.symisc.net/licensing.html
 * or contact Symisc Systems via:
 *   legal@symisc.net
 *   licensing@symisc.net
 *   contact@symisc.net
 */
#define VEDIS_COPYRIGHT "Copyright (C) Symisc Systems, S.U.A.R.L [Mrad Chems Eddine <chm@symisc.net>] 2013, http://vedis.symisc.net/"
/* Forward declaration to public objects */
typedef struct vedis_io_methods vedis_io_methods;
typedef struct vedis_kv_methods vedis_kv_methods;
typedef struct vedis_kv_engine vedis_kv_engine;
typedef struct vedis_context vedis_context;
typedef struct vedis_value vedis_value;
typedef struct vedis_vfs vedis_vfs;
typedef struct vedis vedis;
/*
 * ------------------------------
 * Compile time directives
 * ------------------------------
 * For most purposes, Vedis can be built just fine using the default compilation options.
 * However, if required, the compile-time options documented below can be used to omit Vedis
 * features (resulting in a smaller compiled library size) or to change the default values
 * of some parameters.
 * Every effort has been made to ensure that the various combinations of compilation options
 * work harmoniously and produce a working library.
 *
 * VEDIS_ENABLE_THREADS
 *  This option controls whether or not code is included in Vedis to enable it to operate
 *  safely in a multithreaded environment. The default is not. All mutexing code is omitted
 *  and it is unsafe to use Vedis in a multithreaded program. When compiled with the
 *  VEDIS_ENABLE_THREADS directive enabled, Vedis can be used in a multithreaded program
 *  and it is safe to share the same virtual machine and engine handle between two or more threads.
 *  The value of VEDIS_ENABLE_THREADS can be determined at run-time using the vedis_lib_is_threadsafe()
 *  interface.
 *  When Vedis has been compiled with threading support then the threading mode can be altered
 * at run-time using the vedis_lib_config() interface together with one of these verbs:
 *    VEDIS_LIB_CONFIG_THREAD_LEVEL_SINGLE
 *    VEDIS_LIB_CONFIG_THREAD_LEVEL_MULTI
 *  Platforms others than Windows and UNIX systems must install their own mutex subsystem via 
 *  vedis_lib_config() with a configuration verb set to VEDIS_LIB_CONFIG_USER_MUTEX.
 *  Otherwise the library is not threadsafe.
 *  Note that you must link Vedis with the POSIX threads library under UNIX systems (i.e: -lpthread).
 *
 */
/* Symisc public definitions */
#if !defined(SYMISC_STANDARD_DEFS)
#define SYMISC_STANDARD_DEFS
#if defined (_WIN32) || defined (WIN32) || defined(__MINGW32__) || defined (_MSC_VER) || defined (_WIN32_WCE)
/* Windows Systems */
#if !defined(__WINNT__)
#define __WINNT__
#endif 
/*
 * Determine if we are dealing with WindowsCE - which has a much
 * reduced API.
 */
#if defined(_WIN32_WCE)
#ifndef __WIN_CE__
#define __WIN_CE__
#endif /* __WIN_CE__ */
#endif /* _WIN32_WCE */
#else
/*
 * By default we will assume that we are compiling on a UNIX systems.
 * Otherwise the OS_OTHER directive must be defined.
 */
#if !defined(OS_OTHER)
#if !defined(__UNIXES__)
#define __UNIXES__
#endif /* __UNIXES__ */
#else
#endif /* OS_OTHER */
#endif /* __WINNT__/__UNIXES__ */
#if defined(_MSC_VER) || defined(__BORLANDC__)
typedef signed __int64     sxi64; /* 64 bits(8 bytes) signed int64 */
typedef unsigned __int64   sxu64; /* 64 bits(8 bytes) unsigned int64 */
#else
typedef signed long long int   sxi64; /* 64 bits(8 bytes) signed int64 */
typedef unsigned long long int sxu64; /* 64 bits(8 bytes) unsigned int64 */
#endif /* _MSC_VER */
/* Signature of the consumer routine */
typedef int (*ProcConsumer)(const void *, unsigned int, void *);
/* Forward reference */
typedef struct SyMutexMethods SyMutexMethods;
typedef struct SyMemMethods SyMemMethods;
typedef struct SyString SyString;
typedef struct syiovec syiovec;
typedef struct SyMutex SyMutex;
typedef struct Sytm Sytm;
/* Scatter and gather array. */
struct syiovec
{
#if defined (__WINNT__)
	/* Same fields type and offset as WSABUF structure defined one winsock2 header */
	unsigned long nLen;
	char *pBase;
#else
	void *pBase;
	unsigned long nLen;
#endif
};
struct SyString
{
	const char *zString;  /* Raw string (may not be null terminated) */
	unsigned int nByte;   /* Raw string length */
};
/* Time structure. */
struct Sytm
{
  int tm_sec;     /* seconds (0 - 60) */
  int tm_min;     /* minutes (0 - 59) */
  int tm_hour;    /* hours (0 - 23) */
  int tm_mday;    /* day of month (1 - 31) */
  int tm_mon;     /* month of year (0 - 11) */
  int tm_year;    /* year + 1900 */
  int tm_wday;    /* day of week (Sunday = 0) */
  int tm_yday;    /* day of year (0 - 365) */
  int tm_isdst;   /* is summer time in effect? */
  char *tm_zone;  /* abbreviation of timezone name */
  long tm_gmtoff; /* offset from UTC in seconds */
};
/* Convert a tm structure (struct tm *) found in <time.h> to a Sytm structure */
#define STRUCT_TM_TO_SYTM(pTM, pSYTM) \
	(pSYTM)->tm_hour = (pTM)->tm_hour;\
	(pSYTM)->tm_min	 = (pTM)->tm_min;\
	(pSYTM)->tm_sec	 = (pTM)->tm_sec;\
	(pSYTM)->tm_mon	 = (pTM)->tm_mon;\
	(pSYTM)->tm_mday = (pTM)->tm_mday;\
	(pSYTM)->tm_year = (pTM)->tm_year + 1900;\
	(pSYTM)->tm_yday = (pTM)->tm_yday;\
	(pSYTM)->tm_wday = (pTM)->tm_wday;\
	(pSYTM)->tm_isdst = (pTM)->tm_isdst;\
	(pSYTM)->tm_gmtoff = 0;\
	(pSYTM)->tm_zone = 0;

/* Convert a SYSTEMTIME structure (LPSYSTEMTIME: Windows Systems only ) to a Sytm structure */
#define SYSTEMTIME_TO_SYTM(pSYSTIME, pSYTM) \
	 (pSYTM)->tm_hour = (pSYSTIME)->wHour;\
	 (pSYTM)->tm_min  = (pSYSTIME)->wMinute;\
	 (pSYTM)->tm_sec  = (pSYSTIME)->wSecond;\
	 (pSYTM)->tm_mon  = (pSYSTIME)->wMonth - 1;\
	 (pSYTM)->tm_mday = (pSYSTIME)->wDay;\
	 (pSYTM)->tm_year = (pSYSTIME)->wYear;\
	 (pSYTM)->tm_yday = 0;\
	 (pSYTM)->tm_wday = (pSYSTIME)->wDayOfWeek;\
	 (pSYTM)->tm_gmtoff = 0;\
	 (pSYTM)->tm_isdst = -1;\
	 (pSYTM)->tm_zone = 0;

/* Dynamic memory allocation methods. */
struct SyMemMethods 
{
	void * (*xAlloc)(unsigned int);          /* [Required:] Allocate a memory chunk */
	void * (*xRealloc)(void *, unsigned int); /* [Required:] Re-allocate a memory chunk */
	void   (*xFree)(void *);                 /* [Required:] Release a memory chunk */
	unsigned int  (*xChunkSize)(void *);     /* [Optional:] Return chunk size */
	int    (*xInit)(void *);                 /* [Optional:] Initialization callback */
	void   (*xRelease)(void *);              /* [Optional:] Release callback */
	void  *pUserData;                        /* [Optional:] First argument to xInit() and xRelease() */
};
/* Out of memory callback signature. */
typedef int (*ProcMemError)(void *);
/* Mutex methods. */
struct SyMutexMethods 
{
	int (*xGlobalInit)(void);		/* [Optional:] Global mutex initialization */
	void  (*xGlobalRelease)(void);	/* [Optional:] Global Release callback () */
	SyMutex * (*xNew)(int);	        /* [Required:] Request a new mutex */
	void  (*xRelease)(SyMutex *);	/* [Optional:] Release a mutex  */
	void  (*xEnter)(SyMutex *);	    /* [Required:] Enter mutex */
	int (*xTryEnter)(SyMutex *);    /* [Optional:] Try to enter a mutex */
	void  (*xLeave)(SyMutex *);	    /* [Required:] Leave a locked mutex */
};
#if defined (_MSC_VER) || defined (__MINGW32__) ||  defined (__GNUC__) && defined (__declspec)
#define SX_APIIMPORT	__declspec(dllimport)
#define SX_APIEXPORT	__declspec(dllexport)
#else
#define	SX_APIIMPORT
#define	SX_APIEXPORT
#endif
/* Standard return values from Symisc public interfaces */
#define SXRET_OK       0      /* Not an error */	
#define SXERR_MEM      (-1)   /* Out of memory */
#define SXERR_IO       (-2)   /* IO error */
#define SXERR_EMPTY    (-3)   /* Empty field */
#define SXERR_LOCKED   (-4)   /* Locked operation */
#define SXERR_ORANGE   (-5)   /* Out of range value */
#define SXERR_NOTFOUND (-6)   /* Item not found */
#define SXERR_LIMIT    (-7)   /* Limit reached */
#define SXERR_MORE     (-8)   /* Need more input */
#define SXERR_INVALID  (-9)   /* Invalid parameter */
#define SXERR_ABORT    (-10)  /* User callback request an operation abort */
#define SXERR_EXISTS   (-11)  /* Item exists */
#define SXERR_SYNTAX   (-12)  /* Syntax error */
#define SXERR_UNKNOWN  (-13)  /* Unknown error */
#define SXERR_BUSY     (-14)  /* Busy operation */
#define SXERR_OVERFLOW (-15)  /* Stack or buffer overflow */
#define SXERR_WILLBLOCK (-16) /* Operation will block */
#define SXERR_NOTIMPLEMENTED  (-17) /* Operation not implemented */
#define SXERR_EOF      (-18) /* End of input */
#define SXERR_PERM     (-19) /* Permission error */
#define SXERR_NOOP     (-20) /* No-op */	
#define SXERR_FORMAT   (-21) /* Invalid format */
#define SXERR_NEXT     (-22) /* Not an error */
#define SXERR_OS       (-23) /* System call return an error */
#define SXERR_CORRUPT  (-24) /* Corrupted pointer */
#define SXERR_CONTINUE (-25) /* Not an error: Operation in progress */
#define SXERR_NOMATCH  (-26) /* No match */
#define SXERR_RESET    (-27) /* Operation reset */
#define SXERR_DONE     (-28) /* Not an error */
#define SXERR_SHORT    (-29) /* Buffer too short */
#define SXERR_PATH     (-30) /* Path error */
#define SXERR_TIMEOUT  (-31) /* Timeout */
#define SXERR_BIG      (-32) /* Too big for processing */
#define SXERR_RETRY    (-33) /* Retry your call */
#define SXERR_IGNORE   (-63) /* Ignore */
#endif /* SYMISC_PUBLIC_DEFS */
/* 
 * Marker for exported interfaces. 
 */
#define VEDIS_APIEXPORT SX_APIEXPORT
/* Standard Vedis return values */
#define VEDIS_OK      SXRET_OK      /* Successful result */
/* Beginning of error codes */
#define VEDIS_NOMEM    SXERR_MEM     /* Out of memory */
#define VEDIS_ABORT    SXERR_ABORT   /* Another thread have released this instance */
#define VEDIS_IOERR    SXERR_IO      /* IO error */
#define VEDIS_CORRUPT  SXERR_CORRUPT /* Corrupt pointer */
#define VEDIS_LOCKED   SXERR_LOCKED  /* Forbidden Operation */ 
#define VEDIS_BUSY	 SXERR_BUSY    /* The database file is locked */
#define VEDIS_DONE	 SXERR_DONE    /* Operation done */
#define VEDIS_PERM     SXERR_PERM    /* Permission error */
#define VEDIS_NOTIMPLEMENTED SXERR_NOTIMPLEMENTED /* Method not implemented by the underlying Key/Value storage engine */
#define VEDIS_NOTFOUND SXERR_NOTFOUND /* No such record */
#define VEDIS_NOOP     SXERR_NOOP     /* No such method */
#define VEDIS_INVALID  SXERR_INVALID  /* Invalid parameter */
#define VEDIS_EOF      SXERR_EOF      /* End Of Input */
#define VEDIS_UNKNOWN  SXERR_UNKNOWN  /* Unknown configuration option */
#define VEDIS_LIMIT    SXERR_LIMIT    /* Database limit reached */
#define VEDIS_EXISTS   SXERR_EXISTS   /* Record exists */
#define VEDIS_EMPTY    SXERR_EMPTY    /* Empty record */
#define VEDIS_FULL        (-73)       /* Full database (unlikely) */
#define VEDIS_CANTOPEN    (-74)       /* Unable to open the database file */
#define VEDIS_READ_ONLY   (-75)       /* Read only Key/Value storage engine */
#define VEDIS_LOCKERR     (-76)       /* Locking protocol error */
/* end-of-error-codes */
/*
 * If compiling for a processor that lacks floating point
 * support, substitute integer for floating-point.
 */
#ifdef VEDIS_OMIT_FLOATING_POINT
typedef sxi64 vedis_real;
#else
typedef double vedis_real;
#endif
typedef sxi64 vedis_int64;
/*
 * Vedis Configuration Commands.
 *
 * The following set of constants are the available configuration verbs that can
 * be used by the host-application to configure a Vedis datastore handle.
 * These constants must be passed as the second argument to [vedis_config()].
 *
 * Each options require a variable number of arguments.
 * The [vedis_config()] interface will return VEDIS_OK on success, any other
 * return value indicates failure.
 * For a full discussion on the configuration verbs and their expected 
 * parameters, please refer to this page:
 *      http://vedis.symisc.net/c_api/vedis_config.html
 */
#define VEDIS_CONFIG_ERR_LOG             1  /* TWO ARGUMENTS: const char **pzBuf, int *pLen */
#define VEDIS_CONFIG_MAX_PAGE_CACHE      2  /* ONE ARGUMENT: int nMaxPage */
#define VEDIS_CONFIG_KV_ENGINE           4  /* ONE ARGUMENT: const char *zKvName */
#define VEDIS_CONFIG_DISABLE_AUTO_COMMIT 5  /* NO ARGUMENTS */
#define VEDIS_CONFIG_GET_KV_NAME         6  /* ONE ARGUMENT: const char **pzPtr */
#define VEDIS_CONFIG_DUP_EXEC_VALUE      7  /* ONE ARGUMENT: vedis_value **ppOut */
#define VEDIS_CONFIG_RELEASE_DUP_VALUE   8  /* ONE ARGUMENT: vedis_value *pIn */
#define VEDIS_CONFIG_OUTPUT_CONSUMER     9  /* TWO ARGUMENTS: int (*xConsumer)(vedis_value *pOut,void *pUserdata), void *pUserdata */
/*
 * Storage engine configuration commands.
 *
 * The following set of constants are the available configuration verbs that can
 * be used by the host-application to configure the underlying storage engine (i.e Hash, B+tree, R+tree).
 * These constants must be passed as the first argument to [vedis_kv_config()].
 * Each options require a variable number of arguments.
 * The [vedis_kv_config()] interface will return VEDIS_OK on success, any other return
 * value indicates failure.
 * For a full discussion on the configuration verbs and their expected parameters, please
 * refer to this page:
 *      http://vedis.symisc.net/c_api/vedis_kv_config.html
 */
#define VEDIS_KV_CONFIG_HASH_FUNC  1 /* ONE ARGUMENT: unsigned int (*xHash)(const void *,unsigned int) */
#define VEDIS_KV_CONFIG_CMP_FUNC   2 /* ONE ARGUMENT: int (*xCmp)(const void *,const void *,unsigned int) */
/*
 * Global Library Configuration Commands.
 *
 * The following set of constants are the available configuration verbs that can
 * be used by the host-application to configure the whole library.
 * These constants must be passed as the first argument to [vedis_lib_config()].
 *
 * Each options require a variable number of arguments.
 * The [vedis_lib_config()] interface will return VEDIS_OK on success, any other return
 * value indicates failure.
 * Notes:
 * The default configuration is recommended for most applications and so the call to
 * [vedis_lib_config()] is usually not necessary. It is provided to support rare 
 * applications with unusual needs. 
 * The [vedis_lib_config()] interface is not threadsafe. The application must insure that
 * no other [vedis_*()] interfaces are invoked by other threads while [vedis_lib_config()]
 * is running. Furthermore, [vedis_lib_config()] may only be invoked prior to library
 * initialization using [vedis_lib_init()] or [vedis_init()] or after shutdown
 * by [vedis_lib_shutdown()]. If [vedis_lib_config()] is called after [vedis_lib_init()]
 * or [vedis_init()] and before [vedis_lib_shutdown()] then it will return VEDIS_LOCKED.
 * For a full discussion on the configuration verbs and their expected parameters, please
 * refer to this page:
 *      http://vedis.symisc.net/c_api/vedis_lib.html
 */
#define VEDIS_LIB_CONFIG_USER_MALLOC            1 /* ONE ARGUMENT: const SyMemMethods *pMemMethods */ 
#define VEDIS_LIB_CONFIG_MEM_ERR_CALLBACK       2 /* TWO ARGUMENTS: int (*xMemError)(void *), void *pUserData */
#define VEDIS_LIB_CONFIG_USER_MUTEX             3 /* ONE ARGUMENT: const SyMutexMethods *pMutexMethods */ 
#define VEDIS_LIB_CONFIG_THREAD_LEVEL_SINGLE    4 /* NO ARGUMENTS */ 
#define VEDIS_LIB_CONFIG_THREAD_LEVEL_MULTI     5 /* NO ARGUMENTS */ 
#define VEDIS_LIB_CONFIG_VFS                    6 /* ONE ARGUMENT: const vedis_vfs *pVfs */
#define VEDIS_LIB_CONFIG_STORAGE_ENGINE         7 /* ONE ARGUMENT: vedis_kv_methods *pStorage */
#define VEDIS_LIB_CONFIG_PAGE_SIZE              8 /* ONE ARGUMENT: int iPageSize */
/*
 * Synchronization Type Flags
 *
 * When Vedis invokes the xSync() method of an [vedis_io_methods] object it uses
 * a combination of these integer values as the second argument.
 *
 * When the VEDIS_SYNC_DATAONLY flag is used, it means that the sync operation only
 * needs to flush data to mass storage.  Inode information need not be flushed.
 * If the lower four bits of the flag equal VEDIS_SYNC_NORMAL, that means to use normal
 * fsync() semantics. If the lower four bits equal VEDIS_SYNC_FULL, that means to use
 * Mac OS X style fullsync instead of fsync().
 */
#define VEDIS_SYNC_NORMAL        0x00002
#define VEDIS_SYNC_FULL          0x00003
#define VEDIS_SYNC_DATAONLY      0x00010
/*
 * File Locking Levels
 *
 * Vedis uses one of these integer values as the second
 * argument to calls it makes to the xLock() and xUnlock() methods
 * of an [vedis_io_methods] object.
 */
#define VEDIS_LOCK_NONE          0
#define VEDIS_LOCK_SHARED        1
#define VEDIS_LOCK_RESERVED      2
#define VEDIS_LOCK_PENDING       3
#define VEDIS_LOCK_EXCLUSIVE     4
/*
 * CAPIREF: OS Interface: Open File Handle
 *
 * An [vedis_file] object represents an open file in the [vedis_vfs] OS interface
 * layer.
 * Individual OS interface implementations will want to subclass this object by appending
 * additional fields for their own use. The pMethods entry is a pointer to an
 * [vedis_io_methods] object that defines methods for performing
 * I/O operations on the open file.
*/
typedef struct vedis_file vedis_file;
struct vedis_file {
  const vedis_io_methods *pMethods;  /* Methods for an open file. MUST BE FIRST */
};
/*
 * CAPIREF: OS Interface: File Methods Object
 *
 * Every file opened by the [vedis_vfs] xOpen method populates an
 * [vedis_file] object (or, more commonly, a subclass of the
 * [vedis_file] object) with a pointer to an instance of this object.
 * This object defines the methods used to perform various operations
 * against the open file represented by the [vedis_file] object.
 *
 * If the xOpen method sets the vedis_file.pMethods element 
 * to a non-NULL pointer, then the vedis_io_methods.xClose method
 * may be invoked even if the xOpen reported that it failed.  The
 * only way to prevent a call to xClose following a failed xOpen
 * is for the xOpen to set the vedis_file.pMethods element to NULL.
 *
 * The flags argument to xSync may be one of [VEDIS_SYNC_NORMAL] or
 * [VEDIS_SYNC_FULL]. The first choice is the normal fsync().
 * The second choice is a Mac OS X style fullsync. The [VEDIS_SYNC_DATAONLY]
 * flag may be ORed in to indicate that only the data of the file
 * and not its inode needs to be synced.
 *
 * The integer values to xLock() and xUnlock() are one of
 *
 * VEDIS_LOCK_NONE
 * VEDIS_LOCK_SHARED
 * VEDIS_LOCK_RESERVED
 * VEDIS_LOCK_PENDING
 * VEDIS_LOCK_EXCLUSIVE
 * 
 * xLock() increases the lock. xUnlock() decreases the lock.
 * The xCheckReservedLock() method checks whether any database connection,
 * either in this process or in some other process, is holding a RESERVED,
 * PENDING, or EXCLUSIVE lock on the file. It returns true if such a lock exists
 * and false otherwise.
 * 
 * The xSectorSize() method returns the sector size of the device that underlies
 * the file. The sector size is the minimum write that can be performed without
 * disturbing other bytes in the file.
 */
struct vedis_io_methods {
  int iVersion;                 /* Structure version number (currently 1) */
  int (*xClose)(vedis_file*);
  int (*xRead)(vedis_file*, void*, vedis_int64 iAmt, vedis_int64 iOfst);
  int (*xWrite)(vedis_file*, const void*, vedis_int64 iAmt, vedis_int64 iOfst);
  int (*xTruncate)(vedis_file*, vedis_int64 size);
  int (*xSync)(vedis_file*, int flags);
  int (*xFileSize)(vedis_file*, vedis_int64 *pSize);
  int (*xLock)(vedis_file*, int);
  int (*xUnlock)(vedis_file*, int);
  int (*xCheckReservedLock)(vedis_file*, int *pResOut);
  int (*xSectorSize)(vedis_file*);
};
/*
 * CAPIREF: OS Interface Object
 *
 * An instance of the vedis_vfs object defines the interface between
 * the Vedis core and the underlying operating system.  The "vfs"
 * in the name of the object stands for "Virtual File System".
 *
 * Only a single vfs can be registered within the Vedis core.
 * Vfs registration is done using the [vedis_lib_config()] interface
 * with a configuration verb set to VEDIS_LIB_CONFIG_VFS.
 * Note that Windows and UNIX (Linux, FreeBSD, Solaris, Mac OS X, etc.) users
 * does not have to worry about registering and installing a vfs since Vedis
 * come with a built-in vfs for these platforms that implements most the methods
 * defined below.
 *
 * Clients running on exotic systems (ie: Other than Windows and UNIX systems)
 * must register their own vfs in order to be able to use the Vedis library.
 *
 * The value of the iVersion field is initially 1 but may be larger in
 * future versions of Vedis. 
 *
 * The szOsFile field is the size of the subclassed [vedis_file] structure
 * used by this VFS. mxPathname is the maximum length of a pathname in this VFS.
 * 
 * At least szOsFile bytes of memory are allocated by Vedis to hold the [vedis_file]
 * structure passed as the third argument to xOpen. The xOpen method does not have to
 * allocate the structure; it should just fill it in. Note that the xOpen method must
 * set the vedis_file.pMethods to either a valid [vedis_io_methods] object or to NULL.
 * xOpen must do this even if the open fails. Vedis expects that the vedis_file.pMethods
 * element will be valid after xOpen returns regardless of the success or failure of the
 * xOpen call.
 */
struct vedis_vfs {
  const char *zName;       /* Name of this virtual file system [i.e: Windows, UNIX, etc.] */
  int iVersion;            /* Structure version number (currently 1) */
  int szOsFile;            /* Size of subclassed vedis_file */
  int mxPathname;          /* Maximum file pathname length */
  int (*xOpen)(vedis_vfs*, const char *zName, vedis_file*,unsigned int flags);
  int (*xDelete)(vedis_vfs*, const char *zName, int syncDir);
  int (*xAccess)(vedis_vfs*, const char *zName, int flags, int *pResOut);
  int (*xFullPathname)(vedis_vfs*, const char *zName,int buf_len,char *zBuf);
  int (*xTmpDir)(vedis_vfs*,char *zBuf,int buf_len);
  int (*xSleep)(vedis_vfs*, int microseconds);
  int (*xCurrentTime)(vedis_vfs*,Sytm *pOut);
  int (*xGetLastError)(vedis_vfs*, int, char *);
  int (*xMmap)(const char *, void **, vedis_int64 *);  
  void (*xUnmap)(void *,vedis_int64);
};
/*
 * Flags for the xAccess VFS method
 *
 * These integer constants can be used as the third parameter to
 * the xAccess method of an [vedis_vfs] object.  They determine
 * what kind of permissions the xAccess method is looking for.
 * With VEDIS_ACCESS_EXISTS, the xAccess method
 * simply checks whether the file exists.
 * With VEDIS_ACCESS_READWRITE, the xAccess method
 * checks whether the named directory is both readable and writable
 * (in other words, if files can be added, removed, and renamed within
 * the directory).
 * The VEDIS_ACCESS_READWRITE constant is currently used only by the
 * [temp_store_directory pragma], though this could change in a future
 * release of Vedis.
 * With VEDIS_ACCESS_READ, the xAccess method
 * checks whether the file is readable.  The VEDIS_ACCESS_READ constant is
 * currently unused, though it might be used in a future release of
 * Vedis.
 */
#define VEDIS_ACCESS_EXISTS    0
#define VEDIS_ACCESS_READWRITE 1   
#define VEDIS_ACCESS_READ      2 
/*
 * The type used to represent a page number.  The first page in a file
 * is called page 1.  0 is used to represent "not a page".
 * A page number is an unsigned 64-bit integer.
 */
typedef sxu64 pgno;
/*
 * A database disk page is represented by an instance
 * of the follwoing structure.
 */
typedef struct vedis_page vedis_page;
struct vedis_page
{
  unsigned char *zData;       /* Content of this page */
  void *pUserData;            /* Extra content */
  pgno pgno;                  /* Page number for this page */
};
/*
 * Vedis handle to the underlying Key/Value Storage Engine (See below).
 */
typedef void * vedis_kv_handle;
/*
 * Vedis pager IO methods.
 *
 * An instance of the following structure define the exported methods of the Vedis pager
 * to the underlying Key/Value storage engine.
 */
typedef struct vedis_kv_io vedis_kv_io;
struct vedis_kv_io
{
	vedis_kv_handle  pHandle;     /* Vedis handle passed as the first parameter to the
									 * method defined below.
									 */
	vedis_kv_methods *pMethods;   /* Underlying storage engine */
	/* Pager methods */
	int (*xGet)(vedis_kv_handle,pgno,vedis_page **);
	int (*xLookup)(vedis_kv_handle,pgno,vedis_page **);
	int (*xNew)(vedis_kv_handle,vedis_page **);
	int (*xWrite)(vedis_page *);
	int (*xDontWrite)(vedis_page *);
	int (*xDontJournal)(vedis_page *);
	int (*xDontMkHot)(vedis_page *);
	int (*xPageRef)(vedis_page *);
	int (*xPageUnref)(vedis_page *);
	int (*xPageSize)(vedis_kv_handle);
	int (*xReadOnly)(vedis_kv_handle);
	unsigned char * (*xTmpPage)(vedis_kv_handle);
	void (*xSetUnpin)(vedis_kv_handle,void (*xPageUnpin)(void *)); 
	void (*xSetReload)(vedis_kv_handle,void (*xPageReload)(void *));
	void (*xErr)(vedis_kv_handle,const char *);
};
/*
 * Key/Value Cursor Object.
 *
 * An instance of a subclass of the following object defines a cursor
 * used to scan through a key-value storage engine.
 */
typedef struct vedis_kv_cursor vedis_kv_cursor;
struct vedis_kv_cursor
{
  vedis_kv_engine *pStore; /* Must be first */
  /* Subclasses will typically add additional fields */
};
/*
 * Possible seek positions.
 */
#define VEDIS_CURSOR_MATCH_EXACT  1
#define VEDIS_CURSOR_MATCH_LE     2
#define VEDIS_CURSOR_MATCH_GE     3
/*
 * Key/Value Storage Engine.
 *
 * A Key-Value storage engine is defined by an instance of the following
 * object.
 * Vedis works with run-time interchangeable storage engines (i.e. Hash, B+Tree, R+Tree, LSM, etc.).
 * The storage engine works with key/value pairs where both the key
 * and the value are byte arrays of arbitrary length and with no restrictions on content.
 * Vedis come with two built-in KV storage engine: A Virtual Linear Hash (VLH) storage
 * engine is used for persistent on-disk databases with O(1) lookup time and an in-memory
 * hash-table or Red-black tree storage engine is used for in-memory databases.
 * Future versions of Vedis might add other built-in storage engines (i.e. LSM). 
 * Registration of a Key/Value storage engine at run-time is done via [vedis_lib_config()]
 * with a configuration verb set to VEDIS_LIB_CONFIG_STORAGE_ENGINE.
 */
struct vedis_kv_engine
{
  const vedis_kv_io *pIo; /* IO methods: MUST be first */
   /* Subclasses will typically add additional fields */
};
/*
 * Key/Value Storage Engine Virtual Method Table.
 *
 * Key/Value storage engine methods is defined by an instance of the following
 * object.
 * Registration of a Key/Value storage engine at run-time is done via [vedis_lib_config()]
 * with a configuration verb set to VEDIS_LIB_CONFIG_STORAGE_ENGINE.
 */
struct vedis_kv_methods
{
  const char *zName; /* Storage engine name [i.e. Hash, B+tree, LSM, R-tree, Mem, etc.]*/
  int szKv;          /* 'vedis_kv_engine' subclass size */
  int szCursor;      /* 'vedis_kv_cursor' subclass size */
  int iVersion;      /* Structure version, currently 1 */
  /* Storage engine methods */
  int (*xInit)(vedis_kv_engine *,int iPageSize);
  void (*xRelease)(vedis_kv_engine *);
  int (*xConfig)(vedis_kv_engine *,int op,va_list ap);
  int (*xOpen)(vedis_kv_engine *,pgno);
  int (*xReplace)(
	  vedis_kv_engine *,
	  const void *pKey,int nKeyLen,
	  const void *pData,vedis_int64 nDataLen
	  ); 
    int (*xAppend)(
	  vedis_kv_engine *,
	  const void *pKey,int nKeyLen,
	  const void *pData,vedis_int64 nDataLen
	  );
  void (*xCursorInit)(vedis_kv_cursor *);
  int (*xSeek)(vedis_kv_cursor *,const void *pKey,int nByte,int iPos); /* Mandatory */
  int (*xFirst)(vedis_kv_cursor *);
  int (*xLast)(vedis_kv_cursor *);
  int (*xValid)(vedis_kv_cursor *);
  int (*xNext)(vedis_kv_cursor *);
  int (*xPrev)(vedis_kv_cursor *);
  int (*xDelete)(vedis_kv_cursor *);
  int (*xKeyLength)(vedis_kv_cursor *,int *);
  int (*xKey)(vedis_kv_cursor *,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);
  int (*xDataLength)(vedis_kv_cursor *,vedis_int64 *);
  int (*xData)(vedis_kv_cursor *,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);
  void (*xReset)(vedis_kv_cursor *);
  void (*xCursorRelease)(vedis_kv_cursor *);
};
/*
 * Vedis journal file suffix.
 */
#ifndef VEDIS_JOURNAL_FILE_SUFFIX
#define VEDIS_JOURNAL_FILE_SUFFIX "_vedis_journal"
#endif
/*
 * Call Context - Error Message Serverity Level.
 *
 * The following constans are the allowed severity level that can
 * passed as the second argument to the [vedis_context_throw_error()] or
 * [vedis_context_throw_error_format()] interfaces.
 * Refer to the official documentation for additional information.
 */
#define VEDIS_CTX_ERR       1 /* Call context error such as unexpected number of arguments, invalid types and so on. */
#define VEDIS_CTX_WARNING   2 /* Call context Warning */
#define VEDIS_CTX_NOTICE    3 /* Call context Notice */
/* 
 * C-API-REF: Please refer to the official documentation for interfaces
 * purpose and expected parameters. 
 */
/* Vedis Datastore Handle */
VEDIS_APIEXPORT int vedis_open(vedis **ppStore,const char *zStorage);
VEDIS_APIEXPORT int vedis_config(vedis *pStore,int iOp,...);
VEDIS_APIEXPORT int vedis_close(vedis *pStore);

/* Command Execution Interfaces */
VEDIS_APIEXPORT int vedis_exec(vedis *pStore,const char *zCmd,int nLen);
VEDIS_APIEXPORT int vedis_exec_fmt(vedis *pStore,const char *zFmt,...);
VEDIS_APIEXPORT int vedis_exec_result(vedis *pStore,vedis_value **ppOut);

/* Foreign Command Registar */
VEDIS_APIEXPORT int vedis_register_command(vedis *pStore,const char *zName,int (*xCmd)(vedis_context *,int,vedis_value **),void *pUserdata);
VEDIS_APIEXPORT int vedis_delete_command(vedis *pStore,const char *zName);

/* Raw Data Store/Fetch (http://vedis.org) */
VEDIS_APIEXPORT int vedis_kv_store(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen);
VEDIS_APIEXPORT int vedis_kv_append(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen);
VEDIS_APIEXPORT int vedis_kv_store_fmt(vedis *pStore,const void *pKey,int nKeyLen,const char *zFormat,...);
VEDIS_APIEXPORT int vedis_kv_append_fmt(vedis *pStore,const void *pKey,int nKeyLen,const char *zFormat,...);
VEDIS_APIEXPORT int vedis_kv_fetch(vedis *pStore,const void *pKey,int nKeyLen,void *pBuf,vedis_int64 /* in|out */*pBufLen);
VEDIS_APIEXPORT int vedis_kv_fetch_callback(vedis *pStore,const void *pKey,
	                    int nKeyLen,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);
VEDIS_APIEXPORT int vedis_kv_config(vedis *pStore,int iOp,...);
VEDIS_APIEXPORT int vedis_kv_delete(vedis *pStore,const void *pKey,int nKeyLen);

/* Manual Transaction Manager */
VEDIS_APIEXPORT int vedis_begin(vedis *pStore);
VEDIS_APIEXPORT int vedis_commit(vedis *pStore);
VEDIS_APIEXPORT int vedis_rollback(vedis *pStore);

/* Utility interfaces */
VEDIS_APIEXPORT int vedis_util_random_string(vedis *pStore,char *zBuf,unsigned int buf_size);
VEDIS_APIEXPORT unsigned int vedis_util_random_num(vedis *pStore);

/* Call Context Key/Value Store Interfaces */
VEDIS_APIEXPORT int vedis_context_kv_store(vedis_context *pCtx,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen);
VEDIS_APIEXPORT int vedis_context_kv_append(vedis_context *pCtx,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen);
VEDIS_APIEXPORT int vedis_context_kv_store_fmt(vedis_context *pCtx,const void *pKey,int nKeyLen,const char *zFormat,...);
VEDIS_APIEXPORT int vedis_context_kv_append_fmt(vedis_context *pCtx,const void *pKey,int nKeyLen,const char *zFormat,...);
VEDIS_APIEXPORT int vedis_context_kv_fetch(vedis_context *pCtx,const void *pKey,int nKeyLen,void *pBuf,vedis_int64 /* in|out */*pBufLen);
VEDIS_APIEXPORT int vedis_context_kv_fetch_callback(vedis_context *pCtx,const void *pKey,
	                    int nKeyLen,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);
VEDIS_APIEXPORT int vedis_context_kv_delete(vedis_context *pCtx,const void *pKey,int nKeyLen);

/* Command Execution Context Interfaces */
VEDIS_APIEXPORT int vedis_context_throw_error(vedis_context *pCtx, int iErr, const char *zErr);
VEDIS_APIEXPORT int vedis_context_throw_error_format(vedis_context *pCtx, int iErr, const char *zFormat, ...);
VEDIS_APIEXPORT unsigned int vedis_context_random_num(vedis_context *pCtx);
VEDIS_APIEXPORT int vedis_context_random_string(vedis_context *pCtx, char *zBuf, int nBuflen);
VEDIS_APIEXPORT void * vedis_context_user_data(vedis_context *pCtx);
VEDIS_APIEXPORT int    vedis_context_push_aux_data(vedis_context *pCtx, void *pUserData);
VEDIS_APIEXPORT void * vedis_context_peek_aux_data(vedis_context *pCtx);
VEDIS_APIEXPORT void * vedis_context_pop_aux_data(vedis_context *pCtx);

/* Setting The Return Value Of A Vedis Command */
VEDIS_APIEXPORT int vedis_result_int(vedis_context *pCtx, int iValue);
VEDIS_APIEXPORT int vedis_result_int64(vedis_context *pCtx, vedis_int64 iValue);
VEDIS_APIEXPORT int vedis_result_bool(vedis_context *pCtx, int iBool);
VEDIS_APIEXPORT int vedis_result_double(vedis_context *pCtx, double Value);
VEDIS_APIEXPORT int vedis_result_null(vedis_context *pCtx);
VEDIS_APIEXPORT int vedis_result_string(vedis_context *pCtx, const char *zString, int nLen);
VEDIS_APIEXPORT int vedis_result_string_format(vedis_context *pCtx, const char *zFormat, ...);
VEDIS_APIEXPORT int vedis_result_value(vedis_context *pCtx, vedis_value *pValue);

/* Extracting Vedis Commands Parameter/Return Values */
VEDIS_APIEXPORT int vedis_value_to_int(vedis_value *pValue);
VEDIS_APIEXPORT int vedis_value_to_bool(vedis_value *pValue);
VEDIS_APIEXPORT vedis_int64 vedis_value_to_int64(vedis_value *pValue);
VEDIS_APIEXPORT double vedis_value_to_double(vedis_value *pValue);
VEDIS_APIEXPORT const char * vedis_value_to_string(vedis_value *pValue, int *pLen);

/* Dynamically Typed Value Object Query Interfaces */
VEDIS_APIEXPORT int vedis_value_is_int(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_is_float(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_is_bool(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_is_string(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_is_null(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_is_numeric(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_is_scalar(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_is_array(vedis_value *pVal);

/* Populating dynamically Typed Objects  */
VEDIS_APIEXPORT int vedis_value_int(vedis_value *pVal, int iValue);
VEDIS_APIEXPORT int vedis_value_int64(vedis_value *pVal, vedis_int64 iValue);
VEDIS_APIEXPORT int vedis_value_bool(vedis_value *pVal, int iBool);
VEDIS_APIEXPORT int vedis_value_null(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_double(vedis_value *pVal, double Value);
VEDIS_APIEXPORT int vedis_value_string(vedis_value *pVal, const char *zString, int nLen);
VEDIS_APIEXPORT int vedis_value_string_format(vedis_value *pVal, const char *zFormat, ...);
VEDIS_APIEXPORT int vedis_value_reset_string_cursor(vedis_value *pVal);
VEDIS_APIEXPORT int vedis_value_release(vedis_value *pVal);

/* On-demand Object Value Allocation */
VEDIS_APIEXPORT vedis_value * vedis_context_new_scalar(vedis_context *pCtx);
VEDIS_APIEXPORT vedis_value * vedis_context_new_array(vedis_context *pCtx);
VEDIS_APIEXPORT void vedis_context_release_value(vedis_context *pCtx, vedis_value *pValue);

/* Working with Vedis Arrays */
VEDIS_APIEXPORT vedis_value * vedis_array_fetch(vedis_value *pArray,unsigned int index);
VEDIS_APIEXPORT int vedis_array_walk(vedis_value *pArray, int (*xWalk)(vedis_value *, void *), void *pUserData);
VEDIS_APIEXPORT int vedis_array_insert(vedis_value *pArray,vedis_value *pValue);
VEDIS_APIEXPORT unsigned int vedis_array_count(vedis_value *pArray);
VEDIS_APIEXPORT int vedis_array_reset(vedis_value *pArray);
VEDIS_APIEXPORT vedis_value * vedis_array_next_elem(vedis_value *pArray);

/* Global Library Management Interfaces */
VEDIS_APIEXPORT int vedis_lib_init(void);
VEDIS_APIEXPORT int vedis_lib_config(int nConfigOp, ...);
VEDIS_APIEXPORT int vedis_lib_shutdown(void);
VEDIS_APIEXPORT int vedis_lib_is_threadsafe(void);
VEDIS_APIEXPORT const char * vedis_lib_version(void);
VEDIS_APIEXPORT const char * vedis_lib_signature(void);
VEDIS_APIEXPORT const char * vedis_lib_ident(void);
VEDIS_APIEXPORT const char * vedis_lib_copyright(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* _VEDIS_H_ */
