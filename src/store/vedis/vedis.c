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
/*
 * $SymiscID: vedis.c v1.2.6 Unix|Win32/64 2013-09-15 23:42:22 stable <chm@symisc.net> $ 
 */
/* This file is an amalgamation of many separate C source files from vedis version 1.2.6
 * By combining all the individual C code files into this single large file, the entire code
 * can be compiled as a single translation unit. This allows many compilers to do optimization's
 * that would not be possible if the files were compiled separately. Performance improvements
 * are commonly seen when vedis is compiled as a single translation unit.
 *
 * This file is all you need to compile vedis. To use vedis in other programs, you need
 * this file and the "vedis.h" header file that defines the programming interface to the 
 * vedis engine.(If you do not have the "vedis.h" header file at hand, you will find
 * a copy embedded within the text of this file.Search for "Header file: <vedis.h>" to find
 * the start of the embedded vedis.h header file.) Additional code files may be needed if
 * you want a wrapper to interface vedis with your choice of programming language.
 * To get the official documentation, please visit http://vedis.symisc.net/
 */
 /*
  * Make the sure the following directive is defined in the amalgamation build.
  */
 #ifndef VEDIS_AMALGAMATION
 #define VEDIS_AMALGAMATION
 #endif /* VEDIS_AMALGAMATION */
/*
 * Embedded header file for vedis: <vedis.h>
 */
/*
 * ----------------------------------------------------------
 * File: vedis.h
 * MD5: 935b32c31005cfdaa53305ce2d582dbf
 * ----------------------------------------------------------
 */
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
/*
 * ----------------------------------------------------------
 * File: vedisInt.h
 * MD5: 9cc0cabef3741742fc403ac1a3dc0e0a
 * ----------------------------------------------------------
 */
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
 /* $SymiscID: vedisInt.h v2.1 FreeBSD 2013-09-15 01:49 devel <chm@symisc.net> $ */
#ifndef __VEDISINT_H__
#define __VEDISINT_H__
/* Internal interface definitions for Vedis. */
#ifdef VEDIS_AMALGAMATION
#ifndef VEDIS_PRIVATE
/* Marker for routines not intended for external use */
#define VEDIS_PRIVATE static
#endif /* VEDIS_PRIVATE */
#else
#define VEDIS_PRIVATE
#include "vedis.h"
#endif 
#ifndef VEDIS_PI
/* Value of PI */
#define VEDIS_PI 3.1415926535898
#endif
/*
 * Constants for the largest and smallest possible 64-bit signed integers.
 * These macros are designed to work correctly on both 32-bit and 64-bit
 * compilers.
 */
#ifndef LARGEST_INT64
#define LARGEST_INT64  (0xffffffff|(((sxi64)0x7fffffff)<<32))
#endif
#ifndef SMALLEST_INT64
#define SMALLEST_INT64 (((sxi64)-1) - LARGEST_INT64)
#endif

/* Symisc Standard types */
#if !defined(SYMISC_STD_TYPES)
#define SYMISC_STD_TYPES
#ifdef __WINNT__
/* Disable nuisance warnings on Borland compilers */
#if defined(__BORLANDC__)
#pragma warn -rch /* unreachable code */
#pragma warn -ccc /* Condition is always true or false */
#pragma warn -aus /* Assigned value is never used */
#pragma warn -csu /* Comparing signed and unsigned */
#pragma warn -spa /* Suspicious pointer arithmetic */
#endif
#endif
typedef signed char        sxi8; /* signed char */
typedef unsigned char      sxu8; /* unsigned char */
typedef signed short int   sxi16; /* 16 bits(2 bytes) signed integer */
typedef unsigned short int sxu16; /* 16 bits(2 bytes) unsigned integer */
typedef int                sxi32; /* 32 bits(4 bytes) integer */
typedef unsigned int       sxu32; /* 32 bits(4 bytes) unsigned integer */
typedef long               sxptr;
typedef unsigned long      sxuptr;
typedef long               sxlong;
typedef unsigned long      sxulong;
typedef sxi32              sxofft;
typedef sxi64              sxofft64;
typedef long double	       sxlongreal;
typedef double             sxreal;
#define SXI8_HIGH       0x7F
#define SXU8_HIGH       0xFF
#define SXI16_HIGH      0x7FFF
#define SXU16_HIGH      0xFFFF
#define SXI32_HIGH      0x7FFFFFFF
#define SXU32_HIGH      0xFFFFFFFF
#define SXI64_HIGH      0x7FFFFFFFFFFFFFFF
#define SXU64_HIGH      0xFFFFFFFFFFFFFFFF 
#if !defined(TRUE)
#define TRUE 1
#endif
#if !defined(FALSE)
#define FALSE 0
#endif
/*
 * The following macros are used to cast pointers to integers and
 * integers to pointers.
 */
#if defined(__PTRDIFF_TYPE__)  
# define SX_INT_TO_PTR(X)  ((void*)(__PTRDIFF_TYPE__)(X))
# define SX_PTR_TO_INT(X)  ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)    
# define SX_INT_TO_PTR(X)  ((void*)&((char*)0)[X])
# define SX_PTR_TO_INT(X)  ((int)(((char*)X)-(char*)0))
#else                       
# define SX_INT_TO_PTR(X)  ((void*)(X))
# define SX_PTR_TO_INT(X)  ((int)(X))
#endif
#define SXMIN(a, b)  ((a < b) ? (a) : (b))
#define SXMAX(a, b)  ((a < b) ? (b) : (a))
#endif /* SYMISC_STD_TYPES */
/* Symisc Run-time API private definitions */
#if !defined(SYMISC_PRIVATE_DEFS)
#define SYMISC_PRIVATE_DEFS

typedef sxi32 (*ProcRawStrCmp)(const SyString *, const SyString *);
#define SyStringData(RAW)	((RAW)->zString)
#define SyStringLength(RAW)	((RAW)->nByte)
#define SyStringInitFromBuf(RAW, ZBUF, NLEN){\
	(RAW)->zString 	= (const char *)ZBUF;\
	(RAW)->nByte	= (sxu32)(NLEN);\
}
#define SyStringUpdatePtr(RAW, NBYTES){\
	if( NBYTES > (RAW)->nByte ){\
		(RAW)->nByte = 0;\
	}else{\
		(RAW)->zString += NBYTES;\
		(RAW)->nByte -= NBYTES;\
	}\
}
#define SyStringDupPtr(RAW1, RAW2)\
	(RAW1)->zString = (RAW2)->zString;\
	(RAW1)->nByte = (RAW2)->nByte;

#define SyStringTrimLeadingChar(RAW, CHAR)\
	while((RAW)->nByte > 0 && (RAW)->zString[0] == CHAR ){\
			(RAW)->zString++;\
			(RAW)->nByte--;\
	}
#define SyStringTrimTrailingChar(RAW, CHAR)\
	while((RAW)->nByte > 0 && (RAW)->zString[(RAW)->nByte - 1] == CHAR){\
		(RAW)->nByte--;\
	}
#define SyStringCmp(RAW1, RAW2, xCMP)\
	(((RAW1)->nByte == (RAW2)->nByte) ? xCMP((RAW1)->zString, (RAW2)->zString, (RAW2)->nByte) : (sxi32)((RAW1)->nByte - (RAW2)->nByte))

#define SyStringCmp2(RAW1, RAW2, xCMP)\
	(((RAW1)->nByte >= (RAW2)->nByte) ? xCMP((RAW1)->zString, (RAW2)->zString, (RAW2)->nByte) : (sxi32)((RAW2)->nByte - (RAW1)->nByte))

#define SyStringCharCmp(RAW, CHAR) \
	(((RAW)->nByte == sizeof(char)) ? ((RAW)->zString[0] == CHAR ? 0 : CHAR - (RAW)->zString[0]) : ((RAW)->zString[0] == CHAR ? 0 : (RAW)->nByte - sizeof(char)))

#define SX_ADDR(PTR)    ((sxptr)PTR)
#define SX_ARRAYSIZE(X) (sizeof(X)/sizeof(X[0]))
#define SXUNUSED(P)	(P = 0)
#define	SX_EMPTY(PTR)   (PTR == 0)
#define SX_EMPTY_STR(STR) (STR == 0 || STR[0] == 0 )
typedef struct SyMemBackend SyMemBackend;
typedef struct SyBlob SyBlob;
typedef struct SySet SySet;
/* Standard function signatures */
typedef sxi32 (*ProcCmp)(const void *, const void *, sxu32);
typedef sxi32 (*ProcPatternMatch)(const char *, sxu32, const char *, sxu32, sxu32 *);
typedef sxi32 (*ProcSearch)(const void *, sxu32, const void *, sxu32, ProcCmp, sxu32 *);
typedef sxu32 (*ProcHash)(const void *, sxu32);
typedef sxi32 (*ProcHashSum)(const void *, sxu32, unsigned char *, sxu32);
typedef sxi32 (*ProcSort)(void *, sxu32, sxu32, ProcCmp);
#define MACRO_LIST_PUSH(Head, Item)\
	Item->pNext = Head;\
	Head = Item; 
#define MACRO_LD_PUSH(Head, Item)\
	if( Head == 0 ){\
		Head = Item;\
	}else{\
		Item->pNext = Head;\
		Head->pPrev = Item;\
		Head = Item;\
	}
#define MACRO_LD_REMOVE(Head, Item)\
	if( Head == Item ){\
		Head = Head->pNext;\
	}\
	if( Item->pPrev ){ Item->pPrev->pNext = Item->pNext;}\
	if( Item->pNext ){ Item->pNext->pPrev = Item->pPrev;}
/*
 * A generic dynamic set.
 */
struct SySet
{
	SyMemBackend *pAllocator; /* Memory backend */
	void *pBase;              /* Base pointer */	
	sxu32 nUsed;              /* Total number of used slots  */
	sxu32 nSize;              /* Total number of available slots */
	sxu32 eSize;              /* Size of a single slot */
	sxu32 nCursor;	          /* Loop cursor */	
	void *pUserData;          /* User private data associated with this container */
};
#define SySetBasePtr(S)           ((S)->pBase)
#define SySetBasePtrJump(S, OFFT)  (&((char *)(S)->pBase)[OFFT*(S)->eSize])
#define SySetUsed(S)              ((S)->nUsed)
#define SySetSize(S)              ((S)->nSize)
#define SySetElemSize(S)          ((S)->eSize) 
#define SySetCursor(S)            ((S)->nCursor)
#define SySetGetAllocator(S)      ((S)->pAllocator)
#define SySetSetUserData(S, DATA)  ((S)->pUserData = DATA)
#define SySetGetUserData(S)       ((S)->pUserData)
/*
 * A variable length containers for generic data.
 */
struct SyBlob
{
	SyMemBackend *pAllocator; /* Memory backend */
	void   *pBlob;	          /* Base pointer */
	sxu32  nByte;	          /* Total number of used bytes */
	sxu32  mByte;	          /* Total number of available bytes */
	sxu32  nFlags;	          /* Blob internal flags, see below */
};
#define SXBLOB_LOCKED	0x01	/* Blob is locked [i.e: Cannot auto grow] */
#define SXBLOB_STATIC	0x02	/* Not allocated from heap   */
#define SXBLOB_RDONLY   0x04    /* Read-Only data */

#define SyBlobFreeSpace(BLOB)	 ((BLOB)->mByte - (BLOB)->nByte)
#define SyBlobLength(BLOB)	     ((BLOB)->nByte)
#define SyBlobData(BLOB)	     ((BLOB)->pBlob)
#define SyBlobCurData(BLOB)	     ((void*)(&((char*)(BLOB)->pBlob)[(BLOB)->nByte]))
#define SyBlobDataAt(BLOB, OFFT)	 ((void *)(&((char *)(BLOB)->pBlob)[OFFT]))
#define SyBlobGetAllocator(BLOB) ((BLOB)->pAllocator)

#define SXMEM_POOL_INCR			3
#define SXMEM_POOL_NBUCKETS		12
#define SXMEM_BACKEND_MAGIC	0xBAC3E67D
#define SXMEM_BACKEND_CORRUPT(BACKEND)	(BACKEND == 0 || BACKEND->nMagic != SXMEM_BACKEND_MAGIC)

#define SXMEM_BACKEND_RETRY	3
/* A memory backend subsystem is defined by an instance of the following structures */
typedef union SyMemHeader SyMemHeader;
typedef struct SyMemBlock SyMemBlock;
struct SyMemBlock
{
	SyMemBlock *pNext, *pPrev; /* Chain of allocated memory blocks */
#ifdef UNTRUST
	sxu32 nGuard;             /* magic number associated with each valid block, so we
							   * can detect misuse.
							   */
#endif
};
/*
 * Header associated with each valid memory pool block.
 */
union SyMemHeader
{
	SyMemHeader *pNext; /* Next chunk of size 1 << (nBucket + SXMEM_POOL_INCR) in the list */
	sxu32 nBucket;      /* Bucket index in aPool[] */
};
struct SyMemBackend
{
	const SyMutexMethods *pMutexMethods; /* Mutex methods */
	const SyMemMethods *pMethods;  /* Memory allocation methods */
	SyMemBlock *pBlocks;           /* List of valid memory blocks */
	sxu32 nBlock;                  /* Total number of memory blocks allocated so far */
	ProcMemError xMemError;        /* Out-of memory callback */
	void *pUserData;               /* First arg to xMemError() */
	SyMutex *pMutex;               /* Per instance mutex */
	sxu32 nMagic;                  /* Sanity check against misuse */
	SyMemHeader *apPool[SXMEM_POOL_NBUCKETS+SXMEM_POOL_INCR]; /* Pool of memory chunks */
};
/* Mutex types */
#define SXMUTEX_TYPE_FAST	1
#define SXMUTEX_TYPE_RECURSIVE	2
#define SXMUTEX_TYPE_STATIC_1	3
#define SXMUTEX_TYPE_STATIC_2	4
#define SXMUTEX_TYPE_STATIC_3	5
#define SXMUTEX_TYPE_STATIC_4	6
#define SXMUTEX_TYPE_STATIC_5	7
#define SXMUTEX_TYPE_STATIC_6	8

#define SyMutexGlobalInit(METHOD){\
	if( (METHOD)->xGlobalInit ){\
	(METHOD)->xGlobalInit();\
	}\
}
#define SyMutexGlobalRelease(METHOD){\
	if( (METHOD)->xGlobalRelease ){\
	(METHOD)->xGlobalRelease();\
	}\
}
#define SyMutexNew(METHOD, TYPE)			(METHOD)->xNew(TYPE)
#define SyMutexRelease(METHOD, MUTEX){\
	if( MUTEX && (METHOD)->xRelease ){\
		(METHOD)->xRelease(MUTEX);\
	}\
}
#define SyMutexEnter(METHOD, MUTEX){\
	if( MUTEX ){\
	(METHOD)->xEnter(MUTEX);\
	}\
}
#define SyMutexTryEnter(METHOD, MUTEX){\
	if( MUTEX && (METHOD)->xTryEnter ){\
	(METHOD)->xTryEnter(MUTEX);\
	}\
}
#define SyMutexLeave(METHOD, MUTEX){\
	if( MUTEX ){\
	(METHOD)->xLeave(MUTEX);\
	}\
}
/* Comparison, byte swap, byte copy macros */
#define SX_MACRO_FAST_CMP(X1, X2, SIZE, RC){\
	register unsigned char *r1 = (unsigned char *)X1;\
	register unsigned char *r2 = (unsigned char *)X2;\
	register sxu32 LEN = SIZE;\
	for(;;){\
	  if( !LEN ){ break; }if( r1[0] != r2[0] ){ break; } r1++; r2++; LEN--;\
	  if( !LEN ){ break; }if( r1[0] != r2[0] ){ break; } r1++; r2++; LEN--;\
	  if( !LEN ){ break; }if( r1[0] != r2[0] ){ break; } r1++; r2++; LEN--;\
	  if( !LEN ){ break; }if( r1[0] != r2[0] ){ break; } r1++; r2++; LEN--;\
	}\
	RC = !LEN ? 0 : r1[0] - r2[0];\
}
#define	SX_MACRO_FAST_MEMCPY(SRC, DST, SIZ){\
	register unsigned char *xSrc = (unsigned char *)SRC;\
	register unsigned char *xDst = (unsigned char *)DST;\
	register sxu32 xLen = SIZ;\
	for(;;){\
	    if( !xLen ){ break; }xDst[0] = xSrc[0]; xDst++; xSrc++; --xLen;\
		if( !xLen ){ break; }xDst[0] = xSrc[0]; xDst++; xSrc++; --xLen;\
		if( !xLen ){ break; }xDst[0] = xSrc[0]; xDst++; xSrc++; --xLen;\
		if( !xLen ){ break; }xDst[0] = xSrc[0]; xDst++; xSrc++; --xLen;\
	}\
}
#define SX_MACRO_BYTE_SWAP(X, Y, Z){\
	register unsigned char *s = (unsigned char *)X;\
	register unsigned char *d = (unsigned char *)Y;\
	sxu32	ZLong = Z;  \
	sxi32 c; \
	for(;;){\
	  if(!ZLong){ break; } c = s[0] ; s[0] = d[0]; d[0] = (unsigned char)c; s++; d++; --ZLong;\
	  if(!ZLong){ break; } c = s[0] ; s[0] = d[0]; d[0] = (unsigned char)c; s++; d++; --ZLong;\
	  if(!ZLong){ break; } c = s[0] ; s[0] = d[0]; d[0] = (unsigned char)c; s++; d++; --ZLong;\
	  if(!ZLong){ break; } c = s[0] ; s[0] = d[0]; d[0] = (unsigned char)c; s++; d++; --ZLong;\
	}\
}
#define SX_MSEC_PER_SEC	(1000)			/* Millisec per seconds */
#define SX_USEC_PER_SEC	(1000000)		/* Microsec per seconds */
#define SX_NSEC_PER_SEC	(1000000000)	/* Nanosec per seconds */
#endif /* SYMISC_PRIVATE_DEFS */
/* Symisc Run-time API auxiliary definitions */
#if !defined(SYMISC_PRIVATE_AUX_DEFS)
#define SYMISC_PRIVATE_AUX_DEFS

typedef struct SyHashEntry_Pr SyHashEntry_Pr;
typedef struct SyHashEntry SyHashEntry;
typedef struct SyHash SyHash;
/*
 * Each public hashtable entry is represented by an instance
 * of the following structure.
 */
struct SyHashEntry
{
	const void *pKey; /* Hash key */
	sxu32 nKeyLen;    /* Key length */
	void *pUserData;  /* User private data */
};
#define SyHashEntryGetUserData(ENTRY) ((ENTRY)->pUserData)
#define SyHashEntryGetKey(ENTRY)      ((ENTRY)->pKey)
/* Each active hashtable is identified by an instance of the following structure */
struct SyHash
{
	SyMemBackend *pAllocator;         /* Memory backend */
	ProcHash xHash;                   /* Hash function */
	ProcCmp xCmp;                     /* Comparison function */
	SyHashEntry_Pr *pList, *pCurrent;  /* Linked list of hash entries user for linear traversal */
	sxu32 nEntry;                     /* Total number of entries */
	SyHashEntry_Pr **apBucket;        /* Hash buckets */
	sxu32 nBucketSize;                /* Current bucket size */
};
#define SXHASH_BUCKET_SIZE 16 /* Initial bucket size: must be a power of two */
#define SXHASH_FILL_FACTOR 3
/* Hash access macro */
#define SyHashFunc(HASH)		((HASH)->xHash)
#define SyHashCmpFunc(HASH)		((HASH)->xCmp)
#define SyHashTotalEntry(HASH)	((HASH)->nEntry)
#define SyHashGetPool(HASH)		((HASH)->pAllocator)
/*
 * An instance of the following structure define a single context
 * for an Pseudo Random Number Generator.
 *
 * Nothing in this file or anywhere else in the library does any kind of
 * encryption.  The RC4 algorithm is being used as a PRNG (pseudo-random
 * number generator) not as an encryption device.
 * This implementation is taken from the SQLite3 source tree.
 */
typedef struct SyPRNGCtx SyPRNGCtx;
struct SyPRNGCtx
{
    sxu8 i, j;				/* State variables */
    unsigned char s[256];   /* State variables */
	sxu16 nMagic;			/* Sanity check */
 };
typedef sxi32 (*ProcRandomSeed)(void *, unsigned int, void *);
/* High resolution timer.*/
typedef struct sytime sytime;
struct sytime
{
	long tm_sec;	/* seconds */
	long tm_usec;	/* microseconds */
};
/* Forward declaration */
typedef struct SyStream SyStream;
typedef struct SyToken  SyToken;
typedef struct SyLex    SyLex;
/*
 * Tokenizer callback signature.
 */
typedef sxi32 (*ProcTokenizer)(SyStream *, SyToken *, void *, void *);
/*
 * Each token in the input is represented by an instance
 * of the following structure.
 */
struct SyToken
{
	SyString sData;  /* Token text and length */
	sxu32 nType;     /* Token type */
	sxu32 nLine;     /* Token line number */
	void *pUserData; /* User private data associated with this token */
};
/*
 * During tokenization, information about the state of the input
 * stream is held in an instance of the following structure.
 */
struct SyStream
{
	const unsigned char *zInput; /* Complete text of the input */
	const unsigned char *zText; /* Current input we are processing */	
	const unsigned char *zEnd; /* End of input marker */
	sxu32  nLine; /* Total number of processed lines */
	sxu32  nIgn; /* Total number of ignored tokens */
	SySet *pSet; /* Token containers */
};
/*
 * Each lexer is represented by an instance of the following structure.
 */
struct SyLex
{
	SyStream sStream;         /* Input stream */
	ProcTokenizer xTokenizer; /* Tokenizer callback */
	void * pUserData;         /* Third argument to xTokenizer() */
	SySet *pTokenSet;         /* Token set */
};
#define SyLexTotalToken(LEX)    SySetTotalEntry(&(LEX)->aTokenSet)
#define SyLexTotalLines(LEX)    ((LEX)->sStream.nLine)
#define SyLexTotalIgnored(LEX)  ((LEX)->sStream.nIgn)
#define XLEX_IN_LEN(STREAM)     (sxu32)(STREAM->zEnd - STREAM->zText)
#endif /* SYMISC_PRIVATE_AUX_DEFS */
/*
** Notes on UTF-8 (According to SQLite3 authors):
**
**   Byte-0    Byte-1    Byte-2    Byte-3    Value
**  0xxxxxxx                                 00000000 00000000 0xxxxxxx
**  110yyyyy  10xxxxxx                       00000000 00000yyy yyxxxxxx
**  1110zzzz  10yyyyyy  10xxxxxx             00000000 zzzzyyyy yyxxxxxx
**  11110uuu  10uuzzzz  10yyyyyy  10xxxxxx   000uuuuu zzzzyyyy yyxxxxxx
**
*/
/*
** Assuming zIn points to the first byte of a UTF-8 character, 
** advance zIn to point to the first byte of the next UTF-8 character.
*/
#define SX_JMP_UTF8(zIn, zEnd)\
	while(zIn < zEnd && (((unsigned char)zIn[0] & 0xc0) == 0x80) ){ zIn++; }
#define SX_WRITE_UTF8(zOut, c) {                       \
  if( c<0x00080 ){                                     \
    *zOut++ = (sxu8)(c&0xFF);                          \
  }else if( c<0x00800 ){                               \
    *zOut++ = 0xC0 + (sxu8)((c>>6)&0x1F);              \
    *zOut++ = 0x80 + (sxu8)(c & 0x3F);                 \
  }else if( c<0x10000 ){                               \
    *zOut++ = 0xE0 + (sxu8)((c>>12)&0x0F);             \
    *zOut++ = 0x80 + (sxu8)((c>>6) & 0x3F);            \
    *zOut++ = 0x80 + (sxu8)(c & 0x3F);                 \
  }else{                                               \
    *zOut++ = 0xF0 + (sxu8)((c>>18) & 0x07);           \
    *zOut++ = 0x80 + (sxu8)((c>>12) & 0x3F);           \
    *zOut++ = 0x80 + (sxu8)((c>>6) & 0x3F);            \
    *zOut++ = 0x80 + (sxu8)(c & 0x3F);                 \
  }                                                    \
}
/* Rely on the standard ctype */
#include <ctype.h>
#define SyToUpper(c) toupper(c) 
#define SyToLower(c) tolower(c) 
#define SyisUpper(c) isupper(c)
#define SyisLower(c) islower(c)
#define SyisSpace(c) isspace(c)
#define SyisBlank(c) isspace(c)
#define SyisAlpha(c) isalpha(c)
#define SyisDigit(c) isdigit(c)
#define SyisHex(c)	 isxdigit(c)
#define SyisPrint(c) isprint(c)
#define SyisPunct(c) ispunct(c)
#define SyisSpec(c)	 iscntrl(c)
#define SyisCtrl(c)	 iscntrl(c)
#define SyisAscii(c) isascii(c)
#define SyisAlphaNum(c) isalnum(c)
#define SyisGraph(c)     isgraph(c)
#define SyDigToHex(c)    "0123456789ABCDEF"[c & 0x0F] 		
#define SyDigToInt(c)     ((c < 0xc0 && SyisDigit(c))? (c - '0') : 0 )
#define SyCharToUpper(c)  ((c < 0xc0 && SyisLower(c))? SyToUpper(c) : c)
#define SyCharToLower(c)  ((c < 0xc0 && SyisUpper(c))? SyToLower(c) : c)
/* Remove white space/NUL byte from a raw string */
#define SyStringLeftTrim(RAW)\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[0] < 0xc0 && SyisSpace((RAW)->zString[0])){\
		(RAW)->nByte--;\
		(RAW)->zString++;\
	}
#define SyStringLeftTrimSafe(RAW)\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[0] < 0xc0 && ((RAW)->zString[0] == 0 || SyisSpace((RAW)->zString[0]))){\
		(RAW)->nByte--;\
		(RAW)->zString++;\
	}
#define SyStringRightTrim(RAW)\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[(RAW)->nByte - 1] < 0xc0  && SyisSpace((RAW)->zString[(RAW)->nByte - 1])){\
		(RAW)->nByte--;\
	}
#define SyStringRightTrimSafe(RAW)\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[(RAW)->nByte - 1] < 0xc0  && \
	(( RAW)->zString[(RAW)->nByte - 1] == 0 || SyisSpace((RAW)->zString[(RAW)->nByte - 1]))){\
		(RAW)->nByte--;\
	}

#define SyStringFullTrim(RAW)\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[0] < 0xc0  && SyisSpace((RAW)->zString[0])){\
		(RAW)->nByte--;\
		(RAW)->zString++;\
	}\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[(RAW)->nByte - 1] < 0xc0  && SyisSpace((RAW)->zString[(RAW)->nByte - 1])){\
		(RAW)->nByte--;\
	}
#define SyStringFullTrimSafe(RAW)\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[0] < 0xc0  && \
          ( (RAW)->zString[0] == 0 || SyisSpace((RAW)->zString[0]))){\
		(RAW)->nByte--;\
		(RAW)->zString++;\
	}\
	while((RAW)->nByte > 0 && (unsigned char)(RAW)->zString[(RAW)->nByte - 1] < 0xc0  && \
                   ( (RAW)->zString[(RAW)->nByte - 1] == 0 || SyisSpace((RAW)->zString[(RAW)->nByte - 1]))){\
		(RAW)->nByte--;\
	}

#ifndef VEDIS_DISABLE_HASH_FUNC
/* MD5 context */
typedef struct MD5Context MD5Context;
struct MD5Context {
 sxu32 buf[4];
 sxu32 bits[2];
 unsigned char in[64];
};
/* SHA1 context */
typedef struct SHA1Context SHA1Context;
struct SHA1Context {
  unsigned int state[5];
  unsigned int count[2];
  unsigned char buffer[64];
};
#endif /* VEDIS_DISABLE_HASH_FUNC */
/*
** The following values may be passed as the second argument to
** UnqliteOsLock(). The various locks exhibit the following semantics:
**
** SHARED:    Any number of processes may hold a SHARED lock simultaneously.
** RESERVED:  A single process may hold a RESERVED lock on a file at
**            any time. Other processes may hold and obtain new SHARED locks.
** PENDING:   A single process may hold a PENDING lock on a file at
**            any one time. Existing SHARED locks may persist, but no new
**            SHARED locks may be obtained by other processes.
** EXCLUSIVE: An EXCLUSIVE lock precludes all other locks.
**
** PENDING_LOCK may not be passed directly to UnqliteOsLock(). Instead, a
** process that requests an EXCLUSIVE lock may actually obtain a PENDING
** lock. This can be upgraded to an EXCLUSIVE lock by a subsequent call to
** UnqliteOsLock().
*/
#define NO_LOCK         0
#define SHARED_LOCK     1
#define RESERVED_LOCK   2
#define PENDING_LOCK    3
#define EXCLUSIVE_LOCK  4
/*
 * Vedis Locking Strategy (Same as SQLite3)
 *
 * The following #defines specify the range of bytes used for locking.
 * SHARED_SIZE is the number of bytes available in the pool from which
 * a random byte is selected for a shared lock.  The pool of bytes for
 * shared locks begins at SHARED_FIRST. 
 *
 * The same locking strategy and byte ranges are used for Unix and Windows.
 * This leaves open the possiblity of having clients on winNT, and
 * unix all talking to the same shared file and all locking correctly.
 * To do so would require that samba (or whatever
 * tool is being used for file sharing) implements locks correctly between
 * windows and unix.  I'm guessing that isn't likely to happen, but by
 * using the same locking range we are at least open to the possibility.
 *
 * Locking in windows is mandatory.  For this reason, we cannot store
 * actual data in the bytes used for locking.  The pager never allocates
 * the pages involved in locking therefore.  SHARED_SIZE is selected so
 * that all locks will fit on a single page even at the minimum page size.
 * PENDING_BYTE defines the beginning of the locks.  By default PENDING_BYTE
 * is set high so that we don't have to allocate an unused page except
 * for very large databases.  But one should test the page skipping logic 
 * by setting PENDING_BYTE low and running the entire regression suite.
 *
 * Changing the value of PENDING_BYTE results in a subtly incompatible
 * file format.  Depending on how it is changed, you might not notice
 * the incompatibility right away, even running a full regression test.
 * The default location of PENDING_BYTE is the first byte past the
 * 1GB boundary.
 */
#define PENDING_BYTE     (0x40000000)
#define RESERVED_BYTE    (PENDING_BYTE+1)
#define SHARED_FIRST     (PENDING_BYTE+2)
#define SHARED_SIZE      510
/*
 * The default size of a disk sector in bytes.
 */
#ifndef VEDIS_DEFAULT_SECTOR_SIZE
#define VEDIS_DEFAULT_SECTOR_SIZE 512
#endif
/* Forward declaration */
typedef struct vedis_hashmap_node vedis_hashmap_node;
typedef struct vedis_hashmap vedis_hashmap;
/* Forward declaration */
typedef struct vedis_table_entry vedis_table_entry;
typedef struct vedis_table vedis_table;
typedef struct Bitvec Bitvec;
/*
 * Each open database file is managed by a separate instance
 * of the "Pager" structure.
 */
typedef struct Pager Pager;
/*
 * Memory Objects.
 * Internally, the Vedis engine manipulates nearly all vedis values
 * [i.e: string, int, float, bool, null] as vedis_values structures.
 * Each vedis_values struct may cache multiple representations (string, integer etc.)
 * of the same value.
 */
struct vedis_value
{
	union{
		vedis_real rVal;      /* Real value */
		sxi64 iVal;       /* Integer value */
		void *pOther;     /* Other values (Hashmap etc.) */
	}x;
	sxi32 iFlags;       /* Control flags (see below) */
	SyBlob sBlob;       /* Blob values (Warning: Must be last field in this structure) */
};
/* Allowed value types.
 */
#define MEMOBJ_STRING    0x001  /* Memory value is a UTF-8/Binary stream */
#define MEMOBJ_INT       0x002  /* Memory value is an integer */
#define MEMOBJ_REAL      0x004  /* Memory value is a real number */
#define MEMOBJ_BOOL      0x008  /* Memory value is a boolean */
#define MEMOBJ_NULL      0x020  /* Memory value is NULL */
#define MEMOBJ_HASHMAP   0x040  /* Memory value is a hashmap  */
/* Mask of all known types */
#define MEMOBJ_ALL (MEMOBJ_STRING|MEMOBJ_INT|MEMOBJ_REAL|MEMOBJ_BOOL|MEMOBJ_NULL|MEMOBJ_HASHMAP) 
/*
 * The following macro clear the current vedis_value type and replace
 * it with the given one.
 */
#define MemObjSetType(OBJ, TYPE) ((OBJ)->iFlags = ((OBJ)->iFlags&~MEMOBJ_ALL)|TYPE)
#define MEMOBJ_SCALAR (MEMOBJ_STRING|MEMOBJ_INT|MEMOBJ_REAL|MEMOBJ_BOOL|MEMOBJ_NULL)
/* vedis cast method signature */
typedef sxi32 (*ProcMemObjCast)(vedis_value *);
/*
 * Auxiliary data associated with each foreign command is stored
 * in a stack of the following structure.
 * Note that automatic tracked chunks are also stored in an instance
 * of this structure.
 */
typedef struct vedis_aux_data vedis_aux_data;
struct vedis_aux_data
{
	void *pAuxData; /* Aux data */
};
/*
 * Each registered vedis command is represented by an instance of the following
 * structure.
 */
typedef int (*ProcVedisCmd)(vedis_context *,int,vedis_value **);
typedef struct vedis_cmd vedis_cmd;
struct vedis_cmd
{
	SyString sName;       /* Command name */
	sxu32 nHash;          /* Hash of the command name */
	ProcVedisCmd xCmd;    /* Command implementation */
	SySet aAux;           /* Stack of auxiliary data */
	void *pUserData;      /* Command private data */
	vedis_cmd *pNext,*pPrev; /* Pointer to other commands in the chaine */
	vedis_cmd *pNextCol,*pPrevCol; /* Collision chain */
};
/*
 * The 'context' argument for an installable commands. A pointer to an
 * instance of this structure is the first argument to the routines used
 * implement the vedis commands.
 */
struct vedis_context
{
	vedis *pVedis;       /* Vedis handle */
	vedis_cmd *pCmd;     /* Executed vedis command */
	SyBlob sWorker;      /* Working buffer */
	vedis_value *pRet;   /* Return value is stored here. */
	SySet sVar;          /* Container of dynamically allocated vedis_values
						  * [i.e: Garbage collection purposes.]
						  */
};
/*
 * Command output consumer callback.
 */
typedef int (*ProcCmdConsumer)(vedis_value *,void *);
/*
 * Each datastore connection is an instance of the following structure.
 */
struct vedis
{
	SyMemBackend sMem;               /* Memory allocator subsystem */
	SyBlob sErr;                     /* Error log */
	Pager *pPager;                   /* Storage backend */
	vedis_kv_cursor *pCursor;        /* General purpose database cursor */
	vedis_cmd **apCmd;               /* Table of vedis command */
	sxu32 nSize;                     /* Table size */
	sxu32 nCmd;                      /* Total number of installed vedis commands */
	vedis_cmd *pList;                /* List of vedis command */
	vedis_table **apTable;           /* Loaded vedis tables */
	sxu32 nTableSize;                /* apTable[] size */
	sxu32 nTable;                    /* apTable[] length */
	vedis_table *pTableList;         /* List of vedis tables loaded in memory */
#if defined(VEDIS_ENABLE_THREADS)
	const SyMutexMethods *pMethods;  /* Mutex methods */
	SyMutex *pMutex;                 /* Per-handle mutex */
#endif
	ProcCmdConsumer xResultConsumer; /* Result consumer callback */
	void *pUserData;                 /* Last argument to xResultConsumer() */
	vedis_value sResult;             /* Execution result of the last executed command */
	sxi32 iFlags;                    /* Control flags (See below)  */
	vedis *pNext,*pPrev;             /* List of active handles */
	sxu32 nMagic;                    /* Sanity check against misuse */
};
#define VEDIS_FL_DISABLE_AUTO_COMMIT   0x001 /* Disable auto-commit on close */
/*
 * Vedis Token
 * The following set of constants are the tokens recognized
 * by the lexer when processing input.
 * Important: Token values MUST BE A POWER OF TWO.
 */
#define VEDIS_TK_INTEGER   0x0000001  /* Integer */
#define VEDIS_TK_REAL      0x0000002  /* Real number */
#define VEDIS_TK_NUM       (VEDIS_TK_INTEGER|VEDIS_TK_REAL) /* Numeric token, either integer or real */
#define VEDIS_TK_STREAM    0x0000004 /* UTF-8/Binary stream */
#define VEDIS_TK_SEMI      0x0000008 /* ';' semi-colon */
/* 
 * Database signature to identify a valid database image.
 */
#define VEDIS_DB_SIG "SymiscVedis"
/*
 * Database magic number (4 bytes).
 */
#define VEDIS_DB_MAGIC   0xCA1DB634
/*
 * Maximum page size in bytes.
 */
#ifdef VEDIS_MAX_PAGE_SIZE
# undef VEDIS_MAX_PAGE_SIZE
#endif
#define VEDIS_MAX_PAGE_SIZE 65536 /* 65K */
/*
 * Minimum page size in bytes.
 */
#ifdef VEDIS_MIN_PAGE_SIZE
# undef VEDIS_MIN_PAGE_SIZE
#endif
#define VEDIS_MIN_PAGE_SIZE 512
/*
 * The default size of a database page.
 */
#ifndef VEDIS_DEFAULT_PAGE_SIZE
# undef VEDIS_DEFAULT_PAGE_SIZE
#endif
# define VEDIS_DEFAULT_PAGE_SIZE 4096 /* 4K */
/*
 * These bit values are intended for use in the 3rd parameter to the [vedis_open()] interface
 * and in the 4th parameter to the xOpen method of the [vedis_vfs] object.
 */
#define VEDIS_OPEN_READONLY         0x00000001  /* Read only mode. Ok for [vedis_open] */
#define VEDIS_OPEN_READWRITE        0x00000002  /* Ok for [vedis_open] */
#define VEDIS_OPEN_CREATE           0x00000004  /* Ok for [vedis_open] */
#define VEDIS_OPEN_EXCLUSIVE        0x00000008  /* VFS only */
#define VEDIS_OPEN_TEMP_DB          0x00000010  /* VFS only */
#define VEDIS_OPEN_NOMUTEX          0x00000020  /* Ok for [vedis_open] */
#define VEDIS_OPEN_OMIT_JOURNALING  0x00000040  /* Omit journaling for this database. Ok for [vedis_open] */
#define VEDIS_OPEN_IN_MEMORY        0x00000080  /* An in memory database. Ok for [vedis_open]*/
#define VEDIS_OPEN_MMAP             0x00000100  /* Obtain a memory view of the whole file. Ok for [vedis_open] */
/*
 * Each vedis table (i.e.: Hash, Set, List, etc.) is identified by an instance
 * of the following structure.
 */
struct vedis_table_entry
{
	vedis_table *pTable; /* Table that own this entry */
	sxi32 iType;           /* Node type */
	union{
		sxi64 iKey;        /* Int key */
		SyBlob sKey;       /* Blob key */
	}xKey;
	sxi32 iFlags;          /* Control flags */
	sxu32 nHash;           /* Key hash value */
	SyBlob sData;          /* Data */
	sxu32 nId;             /* Unique ID associated with this entry */
	vedis_table_entry *pNext,*pPrev; 
	vedis_table_entry *pNextCollide,*pPrevCollide;
};
/* Allowed node types */
#define VEDIS_TABLE_ENTRY_INT_NODE    1  /* Node with an int [i.e: 64-bit integer] key */
#define VEDIS_TABLE_ENTRY_BLOB_NODE  2  /* Node with a string/BLOB key */
/*
 * Supported Vedis Data Structures.
 */
#define VEDIS_TABLE_HASH 1
#define VEDIS_TABLE_SET  2
#define VEDIS_TABLE_LIST 3
/* hashmap.c */
VEDIS_PRIVATE sxu32 vedisHashmapCount(vedis_hashmap *pMap);
VEDIS_PRIVATE sxi32 vedisHashmapWalk(
	vedis_hashmap *pMap, /* Target hashmap */
	int (*xWalk)(vedis_value *, void *), /* Walker callback */
	void *pUserData /* Last argument to xWalk() */
	);
VEDIS_PRIVATE void vedisHashmapResetLoopCursor(vedis_hashmap *pMap);
VEDIS_PRIVATE vedis_value * vedisHashmapGetNextEntry(vedis_hashmap *pMap);
VEDIS_PRIVATE vedis_hashmap * vedisNewHashmap(
	vedis *pStore,             /* Engine that trigger the hashmap creation */
	sxu32 (*xIntHash)(sxi64), /* Hash function for int keys.NULL otherwise*/
	sxu32 (*xBlobHash)(const void *, sxu32) /* Hash function for BLOB keys.NULL otherwise */
	);
VEDIS_PRIVATE void vedisHashmapRef(vedis_hashmap *pMap);
VEDIS_PRIVATE void vedisHashmapUnref(vedis_hashmap *pMap);
VEDIS_PRIVATE vedis * vedisHashmapGetEngine(vedis_hashmap *pMap);
VEDIS_PRIVATE sxi32 vedisHashmapLookup(
	vedis_hashmap *pMap,        /* Target hashmap */
	vedis_value *pKey,          /* Lookup key */
	vedis_value **ppOut /* OUT: Target node on success */
	);
VEDIS_PRIVATE sxi32 vedisHashmapInsert(
	vedis_hashmap *pMap, /* Target hashmap */
	vedis_value *pKey,   /* Lookup key */
	vedis_value *pVal    /* Node value.NULL otherwise */
	);
/* zSet.c */
VEDIS_PRIVATE void vedisTableReset(vedis_table *pTable);
VEDIS_PRIVATE int VedisRemoveTableEntry(vedis_table *pTable,vedis_table_entry *pEntry);
VEDIS_PRIVATE vedis_table_entry * vedisTableFirstEntry(vedis_table *pTable);
VEDIS_PRIVATE vedis_table_entry * vedisTableLastEntry(vedis_table *pTable);
VEDIS_PRIVATE vedis_table_entry * vedisTableNextEntry(vedis_table *pTable);
VEDIS_PRIVATE sxu32 vedisTableLength(vedis_table *pTable);
VEDIS_PRIVATE vedis_table * vedisFetchTable(vedis *pDb,vedis_value *pName,int create_new,int iType);
VEDIS_PRIVATE vedis_table_entry * vedisTableGetRecordByIndex(vedis_table *pTable,sxu32 nIndex);
VEDIS_PRIVATE vedis_table_entry * vedisTableGetRecord(vedis_table *pTable,vedis_value *pKey);
VEDIS_PRIVATE int vedisTableInsertRecord(vedis_table *pTable,vedis_value *pKey,vedis_value *pData);
VEDIS_PRIVATE int vedisTableDeleteRecord(vedis_table *pTable,vedis_value *pKey);
VEDIS_PRIVATE  vedis_table * vedisTableChain(vedis_table *pEntry);
VEDIS_PRIVATE  SyString * vedisTableName(vedis_table *pEntry);
VEDIS_PRIVATE int vedisOnCommit(void *pUserData);
/* cmd.c */
VEDIS_PRIVATE int vedisRegisterBuiltinCommands(vedis *pVedis);
/* json.c */
VEDIS_PRIVATE int vedisJsonSerialize(vedis_value *pValue,SyBlob *pOut);
/* obj.c */
VEDIS_PRIVATE void vedisMemObjInit(vedis *pVedis,vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjInitFromString(vedis *pStore, vedis_value *pObj, const SyString *pVal);
VEDIS_PRIVATE sxi32 vedisMemObjInitFromInt(vedis *pStore, vedis_value *pObj, sxi64 iVal);
VEDIS_PRIVATE vedis_value * vedisNewObjectValue(vedis *pVedis,SyToken *pToken);
VEDIS_PRIVATE vedis_value * vedisNewObjectArrayValue(vedis *pVedis);
VEDIS_PRIVATE void vedisObjectValueDestroy(vedis *pVedis,vedis_value *pValue);
VEDIS_PRIVATE SyBlob * vedisObjectValueBlob(vedis_value *pValue);
VEDIS_PRIVATE sxi32 vedisMemObjRelease(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjTryInteger(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjIsNumeric(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjToInteger(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjToReal(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjToBool(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjToString(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjToNull(vedis_value *pObj);
VEDIS_PRIVATE sxi32 vedisMemObjStore(vedis_value *pSrc, vedis_value *pDest);
/* parse.c */
VEDIS_PRIVATE int vedisProcessInput(vedis *pVedis,const char *zInput,sxu32 nByte);
VEDIS_PRIVATE SyBlob * VedisContextResultBuffer(vedis_context *pCtx);
VEDIS_PRIVATE SyBlob * VedisContextWorkingBuffer(vedis_context *pCtx);
/* api.c */
VEDIS_PRIVATE const SyMemBackend * vedisExportMemBackend(void);
VEDIS_PRIVATE int vedisKvFetchCallback(vedis *pStore,const void *pKey,int nKeyLen,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);
VEDIS_PRIVATE int vedisKvDelete(vedis *pStore,const void *pKey,int nKeyLen);
VEDIS_PRIVATE int vedisDataConsumer(
	const void *pOut,   /* Data to consume */
	unsigned int nLen,  /* Data length */
	void *pUserData     /* User private data */
	);
VEDIS_PRIVATE vedis_kv_methods * vedisFindKVStore(
	const char *zName, /* Storage engine name [i.e. Hash, B+tree, LSM, etc.] */
	sxu32 nByte        /* zName length */
	);
VEDIS_PRIVATE int vedisGetPageSize(void);
VEDIS_PRIVATE int vedisGenError(vedis *pDb,const char *zErr);
VEDIS_PRIVATE int vedisGenErrorFormat(vedis *pDb,const char *zFmt,...);
VEDIS_PRIVATE int vedisGenOutofMem(vedis *pDb);
VEDIS_PRIVATE vedis_cmd * vedisFetchCommand(vedis *pVedis,SyString *pName);
/* vfs.c [io_win.c, io_unix.c ] */
VEDIS_PRIVATE const vedis_vfs * vedisExportBuiltinVfs(void);
/* mem_kv.c */
VEDIS_PRIVATE const vedis_kv_methods * vedisExportMemKvStorage(void);
/* lhash_kv.c */
VEDIS_PRIVATE const vedis_kv_methods * vedisExportDiskKvStorage(void);
/* os.c */
VEDIS_PRIVATE int vedisOsRead(vedis_file *id, void *pBuf, vedis_int64 amt, vedis_int64 offset);
VEDIS_PRIVATE int vedisOsWrite(vedis_file *id, const void *pBuf, vedis_int64 amt, vedis_int64 offset);
VEDIS_PRIVATE int vedisOsTruncate(vedis_file *id, vedis_int64 size);
VEDIS_PRIVATE int vedisOsSync(vedis_file *id, int flags);
VEDIS_PRIVATE int vedisOsFileSize(vedis_file *id, vedis_int64 *pSize);
VEDIS_PRIVATE int vedisOsLock(vedis_file *id, int lockType);
VEDIS_PRIVATE int vedisOsUnlock(vedis_file *id, int lockType);
VEDIS_PRIVATE int vedisOsCheckReservedLock(vedis_file *id, int *pResOut);
VEDIS_PRIVATE int vedisOsSectorSize(vedis_file *id);
VEDIS_PRIVATE int vedisOsOpen(
  vedis_vfs *pVfs,
  SyMemBackend *pAlloc,
  const char *zPath, 
  vedis_file **ppOut, 
  unsigned int flags
);
VEDIS_PRIVATE int vedisOsCloseFree(SyMemBackend *pAlloc,vedis_file *pId);
VEDIS_PRIVATE int vedisOsDelete(vedis_vfs *pVfs, const char *zPath, int dirSync);
VEDIS_PRIVATE int vedisOsAccess(vedis_vfs *pVfs,const char *zPath,int flags,int *pResOut);
/* bitmap.c */
VEDIS_PRIVATE Bitvec *vedisBitvecCreate(SyMemBackend *pAlloc,pgno iSize);
VEDIS_PRIVATE int vedisBitvecTest(Bitvec *p,pgno i);
VEDIS_PRIVATE int vedisBitvecSet(Bitvec *p,pgno i);
VEDIS_PRIVATE void vedisBitvecDestroy(Bitvec *p);
/* pager.c */
VEDIS_PRIVATE int vedisInitCursor(vedis *pDb,vedis_kv_cursor **ppOut);
VEDIS_PRIVATE int vedisReleaseCursor(vedis *pDb,vedis_kv_cursor *pCur);
VEDIS_PRIVATE int vedisPagerisMemStore(vedis *pStore);
VEDIS_PRIVATE int vedisPagerSetCachesize(Pager *pPager,int mxPage);
VEDIS_PRIVATE int vedisPagerSetCommitCallback(Pager *pPager,int (*xCommit)(void *),void *pUserdata);
VEDIS_PRIVATE int vedisPagerClose(Pager *pPager);
VEDIS_PRIVATE int vedisPagerOpen(
  vedis_vfs *pVfs,       /* The virtual file system to use */
  vedis *pDb,            /* Database handle */
  const char *zFilename,   /* Name of the database file to open */
  unsigned int iFlags      /* flags controlling this file */
  );
VEDIS_PRIVATE int vedisPagerRegisterKvEngine(Pager *pPager,vedis_kv_methods *pMethods);
VEDIS_PRIVATE vedis_kv_engine * vedisPagerGetKvEngine(vedis *pDb);
VEDIS_PRIVATE int vedisPagerBegin(Pager *pPager);
VEDIS_PRIVATE int vedisPagerCommit(Pager *pPager);
VEDIS_PRIVATE int vedisPagerRollback(Pager *pPager,int bResetKvEngine);
VEDIS_PRIVATE void vedisPagerRandomString(Pager *pPager,char *zBuf,sxu32 nLen);
VEDIS_PRIVATE sxu32 vedisPagerRandomNum(Pager *pPager);
/* lib.c */
#ifdef VEDIS_ENABLE_HASH_CMD
VEDIS_PRIVATE sxi32 SyBinToHexConsumer(const void *pIn, sxu32 nLen, ProcConsumer xConsumer, void *pConsumerData);
VEDIS_PRIVATE sxu32 SyCrc32(const void *pSrc, sxu32 nLen);
VEDIS_PRIVATE void MD5Update(MD5Context *ctx, const unsigned char *buf, unsigned int len);
VEDIS_PRIVATE void MD5Final(unsigned char digest[16], MD5Context *ctx);
VEDIS_PRIVATE sxi32 MD5Init(MD5Context *pCtx);
VEDIS_PRIVATE sxi32 SyMD5Compute(const void *pIn, sxu32 nLen, unsigned char zDigest[16]);
VEDIS_PRIVATE void SHA1Init(SHA1Context *context);
VEDIS_PRIVATE void SHA1Update(SHA1Context *context, const unsigned char *data, unsigned int len);
VEDIS_PRIVATE void SHA1Final(SHA1Context *context, unsigned char digest[20]);
VEDIS_PRIVATE sxi32 SySha1Compute(const void *pIn, sxu32 nLen, unsigned char zDigest[20]);
#endif /* VEDIS_ENABLE_HASH_CMD */
VEDIS_PRIVATE sxi32 SyRandomness(SyPRNGCtx *pCtx, void *pBuf, sxu32 nLen);
VEDIS_PRIVATE sxi32 SyRandomnessInit(SyPRNGCtx *pCtx, ProcRandomSeed xSeed, void *pUserData);
#ifdef __UNIXES__
VEDIS_PRIVATE sxu32 SyBufferFormat(char *zBuf, sxu32 nLen, const char *zFormat, ...);
#endif /* __UNIXES__ */
VEDIS_PRIVATE sxu32 SyBlobFormatAp(SyBlob *pBlob, const char *zFormat, va_list ap);
VEDIS_PRIVATE sxu32 SyBlobFormat(SyBlob *pBlob, const char *zFormat, ...);
VEDIS_PRIVATE sxi32 SyLexRelease(SyLex *pLex);
VEDIS_PRIVATE sxi32 SyLexTokenizeInput(SyLex *pLex, const char *zInput, sxu32 nLen, void *pCtxData, ProcSort xSort, ProcCmp xCmp);
VEDIS_PRIVATE sxi32 SyLexInit(SyLex *pLex, SySet *pSet, ProcTokenizer xTokenizer, void *pUserData);
VEDIS_PRIVATE sxi32 SyBase64Decode(const char *zB64, sxu32 nLen, ProcConsumer xConsumer, void *pUserData);
VEDIS_PRIVATE sxi32 SyBase64Encode(const char *zSrc, sxu32 nLen, ProcConsumer xConsumer, void *pUserData);
VEDIS_PRIVATE sxu32 SyBinHash(const void *pSrc, sxu32 nLen);
VEDIS_PRIVATE sxi32 SyStrToReal(const char *zSrc, sxu32 nLen, void *pOutVal, const char **zRest);
VEDIS_PRIVATE sxi32 SyBinaryStrToInt64(const char *zSrc, sxu32 nLen, void *pOutVal, const char **zRest);
VEDIS_PRIVATE sxi32 SyOctalStrToInt64(const char *zSrc, sxu32 nLen, void *pOutVal, const char **zRest);
VEDIS_PRIVATE sxi32 SyHexStrToInt64(const char *zSrc, sxu32 nLen, void *pOutVal, const char **zRest);
VEDIS_PRIVATE sxi32 SyHexToint(sxi32 c);
VEDIS_PRIVATE sxi32 SyStrToInt64(const char *zSrc, sxu32 nLen, void *pOutVal, const char **zRest);
VEDIS_PRIVATE sxi32 SyStrIsNumeric(const char *zSrc, sxu32 nLen, sxu8 *pReal, const char **pzTail);
VEDIS_PRIVATE void *SySetPop(SySet *pSet);
VEDIS_PRIVATE void *SySetPeek(SySet *pSet);
VEDIS_PRIVATE sxi32 SySetRelease(SySet *pSet);
VEDIS_PRIVATE sxi32 SySetReset(SySet *pSet);
VEDIS_PRIVATE sxi32 SySetPut(SySet *pSet, const void *pItem);
VEDIS_PRIVATE sxi32 SySetInit(SySet *pSet, SyMemBackend *pAllocator, sxu32 ElemSize);
VEDIS_PRIVATE sxi32 SyBlobRelease(SyBlob *pBlob);
VEDIS_PRIVATE sxi32 SyBlobReset(SyBlob *pBlob);
VEDIS_PRIVATE sxi32 SyBlobDup(SyBlob *pSrc, SyBlob *pDest);
VEDIS_PRIVATE sxi32 SyBlobNullAppend(SyBlob *pBlob);
VEDIS_PRIVATE sxi32 SyBlobAppend(SyBlob *pBlob, const void *pData, sxu32 nSize);
VEDIS_PRIVATE sxi32 SyBlobInit(SyBlob *pBlob, SyMemBackend *pAllocator);
VEDIS_PRIVATE sxi32 SyBlobInitFromBuf(SyBlob *pBlob, void *pBuffer, sxu32 nSize);
VEDIS_PRIVATE void *SyMemBackendDup(SyMemBackend *pBackend, const void *pSrc, sxu32 nSize);
VEDIS_PRIVATE sxi32 SyMemBackendRelease(SyMemBackend *pBackend);
VEDIS_PRIVATE sxi32 SyMemBackendInitFromOthers(SyMemBackend *pBackend, const SyMemMethods *pMethods, ProcMemError xMemErr, void *pUserData);
VEDIS_PRIVATE sxi32 SyMemBackendInit(SyMemBackend *pBackend, ProcMemError xMemErr, void *pUserData);
VEDIS_PRIVATE sxi32 SyMemBackendInitFromParent(SyMemBackend *pBackend,const SyMemBackend *pParent);
#if 0
/* Not used in the current release of the VEDIS engine */
VEDIS_PRIVATE void *SyMemBackendPoolRealloc(SyMemBackend *pBackend, void *pOld, sxu32 nByte);
#endif
VEDIS_PRIVATE sxi32 SyMemBackendPoolFree(SyMemBackend *pBackend, void *pChunk);
VEDIS_PRIVATE void *SyMemBackendPoolAlloc(SyMemBackend *pBackend, sxu32 nByte);
VEDIS_PRIVATE sxi32 SyMemBackendFree(SyMemBackend *pBackend, void *pChunk);
VEDIS_PRIVATE void *SyMemBackendRealloc(SyMemBackend *pBackend, void *pOld, sxu32 nByte);
VEDIS_PRIVATE void *SyMemBackendAlloc(SyMemBackend *pBackend, sxu32 nByte);
VEDIS_PRIVATE sxu32 SyMemcpy(const void *pSrc, void *pDest, sxu32 nLen);
VEDIS_PRIVATE sxi32 SyMemcmp(const void *pB1, const void *pB2, sxu32 nSize);
VEDIS_PRIVATE void SyZero(void *pSrc, sxu32 nSize);
VEDIS_PRIVATE sxi32 SyStrnicmp(const char *zLeft, const char *zRight, sxu32 SLen);
VEDIS_PRIVATE sxu32 Systrcpy(char *zDest, sxu32 nDestLen, const char *zSrc, sxu32 nLen);
#if defined(__APPLE__)
VEDIS_PRIVATE sxi32 SyStrncmp(const char *zLeft, const char *zRight, sxu32 nLen);
#endif
VEDIS_PRIVATE sxu32 SyStrlen(const char *zSrc);
#if defined(VEDIS_ENABLE_THREADS)
VEDIS_PRIVATE const SyMutexMethods *SyMutexExportMethods(void);
VEDIS_PRIVATE sxi32 SyMemBackendMakeThreadSafe(SyMemBackend *pBackend, const SyMutexMethods *pMethods);
VEDIS_PRIVATE sxi32 SyMemBackendDisbaleMutexing(SyMemBackend *pBackend);
#endif
VEDIS_PRIVATE void SyBigEndianPack32(unsigned char *buf,sxu32 nb);
VEDIS_PRIVATE void SyBigEndianUnpack32(const unsigned char *buf,sxu32 *uNB);
VEDIS_PRIVATE void SyBigEndianPack16(unsigned char *buf,sxu16 nb);
VEDIS_PRIVATE void SyBigEndianUnpack16(const unsigned char *buf,sxu16 *uNB);
VEDIS_PRIVATE void SyBigEndianPack64(unsigned char *buf,sxu64 n64);
VEDIS_PRIVATE void SyBigEndianUnpack64(const unsigned char *buf,sxu64 *n64);
#if 0
VEDIS_PRIVATE sxi32 SyBlobAppendBig64(SyBlob *pBlob,sxu64 n64);
#endif
VEDIS_PRIVATE sxi32 SyBlobAppendBig32(SyBlob *pBlob,sxu32 n32);
VEDIS_PRIVATE sxi32 SyBlobAppendBig16(SyBlob *pBlob,sxu16 n16);
VEDIS_PRIVATE void SyTimeFormatToDos(Sytm *pFmt,sxu32 *pOut);
VEDIS_PRIVATE void SyDosTimeFormat(sxu32 nDosDate, Sytm *pOut);
#endif /* _VEDISINT_H_ */
/*
 * ----------------------------------------------------------
 * File: zset.c
 * MD5: 88b2fa4158316f4d34b59a22d03468d5
 * ----------------------------------------------------------
 */
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
/* $SymiscID: obj.c v1.6 Linux 2013-07-10 03:52 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* Hash, Set, List in a single data structure which support persistance for the set
 * of the HSET, HGET, ZSET, etc. command family.
 * This was taken from the PH7 engine source tree, another project developed by
 * Symisc Systems,S.U.A.R.L. Visit http://ph7.symisc.net/ for additional
 * information.
 */
struct vedis_table
{
	vedis *pStore;  /* Store that own this instance */
	SyString sName; /* Table name */
	vedis_table_entry **apBucket;  /* Hash bucket */
	vedis_table_entry *pFirst;     /* First inserted entry */
	vedis_table_entry *pLast;      /* Last inserted entry */
	vedis_table_entry *pCur;       /* Current entry */
	sxu32 nEntry;                  /* Total entries */
	sxu32 nSize;                   /* apBucket[] length */
	sxu32 (*xIntHash)(sxi64);      /* Hash function for int_keys */
	sxu32 (*xBlobHash)(const void *, sxu32); /* Hash function for blob_keys */
	sxi32 iFlags;                 /* vedisTable control flags */
	sxi64 iNextIdx;               /* Next available automatically assigned index */
	sxi32 iTableType;          /* Table type [i.e. Hash, Set, ...] */
	sxu32 nLastID;             /* Last assigned ID */
	vedis_table *pNext,*pPrev; /* Link to other tables */
	vedis_table *pNextCol,*pPrevCol; /* Collision chain */
};
/* Table control flags */
#define VEDIS_TABLE_DISK_LOAD 0x001 /* Decoding table entries from diks */
/*
 * Default hash function for int [i.e; 64-bit integer] keys.
 */
static sxu32 VedisTableIntHash(sxi64 iKey)
{
	return (sxu32)(iKey ^ (iKey << 8) ^ (iKey >> 8));
}
/*
 * Default hash function for string/BLOB keys.
 */
static sxu32 VedisTableBinHash(const void *pSrc, sxu32 nLen)
{
	register unsigned char *zIn = (unsigned char *)pSrc;
	unsigned char *zEnd;
	sxu32 nH = 5381;
	zEnd = &zIn[nLen];
	for(;;){
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
	}	
	return nH;
}
/*
 * Allocate a new hashmap node with a 64-bit integer key.
 * If something goes wrong [i.e: out of memory], this function return NULL.
 * Otherwise a fresh [vedis_table_entry] instance is returned.
 */
static vedis_table_entry * vedisTableNewIntNode(vedis_table *pTable, sxi64 iKey, sxu32 nHash,vedis_value *pValue)
{
	vedis_table_entry *pNode;
	/* Allocate a new node */
	pNode = (vedis_table_entry *)SyMemBackendPoolAlloc(&pTable->pStore->sMem, sizeof(vedis_table_entry));
	if( pNode == 0 ){
		return 0;
	}
	/* Zero the stucture */
	SyZero(pNode, sizeof(vedis_table_entry));
	/* Fill in the structure */
	pNode->pTable  = &(*pTable);
	pNode->iType = VEDIS_TABLE_ENTRY_INT_NODE;
	pNode->nHash = nHash;
	pNode->xKey.iKey = iKey;
	SyBlobInit(&pNode->sData,&pTable->pStore->sMem);
	/* Duplicate the value */
	if( pValue ){
		const char *zData;
		int nByte;
		zData = vedis_value_to_string(pValue,&nByte);
		if( nByte > 0 ){
			SyBlobAppend(&pNode->sData,zData,(sxu32)nByte);
		}
	}
	return pNode;
}
/*
 * Allocate a new hashmap node with a BLOB key.
 * If something goes wrong [i.e: out of memory], this function return NULL.
 * Otherwise a fresh [vedis_table_entry] instance is returned.
 */
static vedis_table_entry * vedisTableNewBlobNode(vedis_table *pTable, const void *pKey, sxu32 nKeyLen, sxu32 nHash,vedis_value *pValue)
{
	vedis_table_entry *pNode;
	/* Allocate a new node */
	pNode = (vedis_table_entry *)SyMemBackendPoolAlloc(&pTable->pStore->sMem, sizeof(vedis_table_entry));
	if( pNode == 0 ){
		return 0;
	}
	/* Zero the stucture */
	SyZero(pNode, sizeof(vedis_table_entry));
	/* Fill in the structure */
	pNode->pTable  = &(*pTable);
	pNode->iType = VEDIS_TABLE_ENTRY_BLOB_NODE;
	pNode->nHash = nHash;
	SyBlobInit(&pNode->xKey.sKey, &pTable->pStore->sMem);
	SyBlobAppend(&pNode->xKey.sKey, pKey, nKeyLen);
	SyBlobInit(&pNode->sData,&pTable->pStore->sMem);
	/* Duplicate the value */
	if( pValue ){
		const char *zData;
		int nByte;
		zData = vedis_value_to_string(pValue,&nByte);
		if( nByte > 0 ){
			SyBlobAppend(&pNode->sData,zData,(sxu32)nByte);
		}
	}
	return pNode;
}
/* Forward declaration */
static int vedisTableEntrySerialize(vedis_table *pTable,vedis_table_entry *pEntry);
/*
 * link a hashmap node to the given bucket index (last argument to this function).
 */
static int vedisTableNodeLink(vedis_table *pTable, vedis_table_entry *pNode, sxu32 nBucketIdx)
{
	int rc = VEDIS_OK;
	/* Link */
	if( pTable->apBucket[nBucketIdx] != 0 ){
		pNode->pNextCollide = pTable->apBucket[nBucketIdx];
		pTable->apBucket[nBucketIdx]->pPrevCollide = pNode;
	}
	pTable->apBucket[nBucketIdx] = pNode;
	/* Link to the map list */
	if( pTable->pFirst == 0 ){
		pTable->pFirst = pTable->pLast = pNode;
		/* Point to the first inserted node */
		pTable->pCur = pNode;
	}else{
		MACRO_LD_PUSH(pTable->pLast, pNode);
	}
	pNode->nId = pTable->nLastID++;
	++pTable->nEntry;
	if( !vedisPagerisMemStore(pTable->pStore) && !(pTable->iFlags & VEDIS_TABLE_DISK_LOAD)){
		rc = vedisTableEntrySerialize(pTable,pNode);
	}
	return rc;
}
/*
 * Unlink a node from the hashmap.
 * If the node count reaches zero then release the whole hash-bucket.
 */
static void vedisTableUnlinkNode(vedis_table_entry *pNode)
{
	vedis_table *pTable = pNode->pTable;	
	/* Unlink from the corresponding bucket */
	if( pNode->pPrevCollide == 0 ){
		pTable->apBucket[pNode->nHash & (pTable->nSize - 1)] = pNode->pNextCollide;
	}else{
		pNode->pPrevCollide->pNextCollide = pNode->pNextCollide;
	}
	if( pNode->pNextCollide ){
		pNode->pNextCollide->pPrevCollide = pNode->pPrevCollide;
	}
	if( pTable->pFirst == pNode ){
		pTable->pFirst = pNode->pPrev;
	}
	if( pTable->pCur == pNode ){
		/* Advance the node cursor */
		pTable->pCur = pTable->pCur->pPrev; /* Reverse link */
	}
	/* Unlink from the map list */
	MACRO_LD_REMOVE(pTable->pLast, pNode);
	/* Release the value */
	if( pNode->iType == VEDIS_TABLE_ENTRY_BLOB_NODE ){
		SyBlobRelease(&pNode->xKey.sKey);
	}
	SyBlobRelease(&pNode->sData);
	SyMemBackendPoolFree(&pTable->pStore->sMem, pNode);
	pTable->nEntry--;
	if( pTable->nEntry < 1 ){
		/* Free the hash-bucket */
		SyMemBackendFree(&pTable->pStore->sMem, pTable->apBucket);
		pTable->apBucket = 0;
		pTable->nSize = 0;
		pTable->pFirst = pTable->pLast = pTable->pCur = 0;
	}
}
#define VEDIS_TABLE_FILL_FACTOR 3
/*
 * Grow the hash-table and rehash all entries.
 */
static sxi32 vedisTableGrowBucket(vedis_table *pTable)
{
	if( pTable->nEntry >= pTable->nSize * VEDIS_TABLE_FILL_FACTOR ){
		vedis_table_entry **apOld = pTable->apBucket;
		vedis_table_entry *pEntry, **apNew;
		sxu32 nNew = pTable->nSize << 1;
		sxu32 nBucket;
		sxu32 n;
		if( nNew < 1 ){
			nNew = 16;
		}
		/* Allocate a new bucket */
		apNew = (vedis_table_entry **)SyMemBackendAlloc(&pTable->pStore->sMem, nNew * sizeof(vedis_table_entry *));
		if( apNew == 0 ){
			if( pTable->nSize < 1 ){
				return SXERR_MEM; /* Fatal */
			}
			/* Not so fatal here, simply a performance hit */
			return SXRET_OK;
		}
		/* Zero the table */
		SyZero((void *)apNew, nNew * sizeof(vedis_table_entry *));
		/* Reflect the change */
		pTable->apBucket = apNew;
		pTable->nSize = nNew;
		if( apOld == 0 ){
			/* First allocated table [i.e: no entry], return immediately */
			return SXRET_OK;
		}
		/* Rehash old entries */
		pEntry = pTable->pFirst;
		n = 0;
		for( ;; ){
			if( n >= pTable->nEntry ){
				break;
			}
			/* Clear the old collision link */
			pEntry->pNextCollide = pEntry->pPrevCollide = 0;
			/* Link to the new bucket */
			nBucket = pEntry->nHash & (nNew - 1);
			if( pTable->apBucket[nBucket] != 0 ){
				pEntry->pNextCollide = pTable->apBucket[nBucket];
				pTable->apBucket[nBucket]->pPrevCollide = pEntry;
			}
			pTable->apBucket[nBucket] = pEntry;
			/* Point to the next entry */
			pEntry = pEntry->pPrev; /* Reverse link */
			n++;
		}
		/* Free the old table */
		SyMemBackendFree(&pTable->pStore->sMem, (void *)apOld);
	}
	return SXRET_OK;
}
/*
 * Insert a 64-bit integer key and it's associated value (if any) in the given
 * hashmap.
 */
static sxi32 vedisTableInsertIntKey(vedis_table *pTable,sxi64 iKey,vedis_value *pValue)
{
	vedis_table_entry *pNode;
	sxu32 nHash;
	sxi32 rc;
	/* Hash the key */
	nHash = pTable->xIntHash(iKey);
	/* Allocate a new int node */
	pNode = vedisTableNewIntNode(&(*pTable), iKey, nHash, pValue);
	if( pNode == 0 ){
		return SXERR_MEM;
	}
	/* Make sure the bucket is big enough to hold the new entry */
	rc = vedisTableGrowBucket(&(*pTable));
	if( rc == VEDIS_OK ){
		/* Perform the insertion */
		rc = vedisTableNodeLink(&(*pTable), pNode, nHash & (pTable->nSize - 1));
	}
	if( rc != SXRET_OK ){
		SyMemBackendPoolFree(&pTable->pStore->sMem, pNode);
		return rc;
	}
	return VEDIS_OK;
}
/*
 * Insert a BLOB key and it's associated value (if any) in the given
 * hashmap.
 */
static sxi32 vedisTableInsertBlobKey(vedis_table *pTable,const void *pKey,sxu32 nKeyLen,vedis_value *pValue)
{
	vedis_table_entry *pNode;
	sxu32 nHash;
	sxi32 rc;
	/* Hash the key */
	nHash = pTable->xBlobHash(pKey, nKeyLen);
	/* Allocate a new blob node */
	pNode = vedisTableNewBlobNode(&(*pTable), pKey, nKeyLen, nHash,pValue);
	if( pNode == 0 ){
		return SXERR_MEM;
	}
	/* Make sure the bucket is big enough to hold the new entry */
	rc = vedisTableGrowBucket(&(*pTable));
	if( rc == VEDIS_OK ){
		/* Perform the insertion */
		rc = vedisTableNodeLink(&(*pTable), pNode, nHash & (pTable->nSize - 1));
	}
	if( rc != SXRET_OK ){
		SyMemBackendPoolFree(&pTable->pStore->sMem, pNode);
		return rc;
	}
	/* All done */
	return VEDIS_OK;
}
/*
 * Check if a given 64-bit integer key exists in the given hashmap.
 * Write a pointer to the target node on success. Otherwise
 * SXERR_NOTFOUND is returned on failure.
 */
static sxi32 vedisTableLookupIntKey(
	vedis_table *pMap,         /* Target hashmap */
	sxi64 iKey,                /* lookup key */
	vedis_table_entry **ppNode  /* OUT: target node on success */
	)
{
	vedis_table_entry *pNode;
	sxu32 nHash;
	if( pMap->nEntry < 1 ){
		/* Don't bother hashing, there is no entry anyway */
		return SXERR_NOTFOUND;
	}
	/* Hash the key first */
	nHash = pMap->xIntHash(iKey);
	/* Point to the appropriate bucket */
	pNode = pMap->apBucket[nHash & (pMap->nSize - 1)];
	/* Perform the lookup */
	for(;;){
		if( pNode == 0 ){
			break;
		}
		if( pNode->iType == VEDIS_TABLE_ENTRY_INT_NODE
			&& pNode->nHash == nHash
			&& pNode->xKey.iKey == iKey ){
				/* Node found */
				if( ppNode ){
					*ppNode = pNode;
				}
				return SXRET_OK;
		}
		/* Follow the collision link */
		pNode = pNode->pNextCollide;
	}
	/* No such entry */
	return SXERR_NOTFOUND;
}
/*
 * Check if a given BLOB key exists in the given hashmap.
 * Write a pointer to the target node on success. Otherwise
 * SXERR_NOTFOUND is returned on failure.
 */
static sxi32 vedisTableLookupBlobKey(
	vedis_table *pMap,          /* Target hashmap */
	const void *pKey,           /* Lookup key */
	sxu32 nKeyLen,              /* Key length in bytes */
	vedis_table_entry **ppNode   /* OUT: target node on success */
	)
{
	vedis_table_entry *pNode;
	sxu32 nHash;
	if( pMap->nEntry < 1 ){
		/* Don't bother hashing, there is no entry anyway */
		return SXERR_NOTFOUND;
	}
	/* Hash the key first */
	nHash = pMap->xBlobHash(pKey, nKeyLen);
	/* Point to the appropriate bucket */
	pNode = pMap->apBucket[nHash & (pMap->nSize - 1)];
	/* Perform the lookup */
	for(;;){
		if( pNode == 0 ){
			break;
		}
		if( pNode->iType == VEDIS_TABLE_ENTRY_BLOB_NODE 
			&& pNode->nHash == nHash
			&& SyBlobLength(&pNode->xKey.sKey) == nKeyLen 
			&& SyMemcmp(SyBlobData(&pNode->xKey.sKey), pKey, nKeyLen) == 0 ){
				/* Node found */
				if( ppNode ){
					*ppNode = pNode;
				}
				return SXRET_OK;
		}
		/* Follow the collision link */
		pNode = pNode->pNextCollide;
	}
	/* No such entry */
	return SXERR_NOTFOUND;
}
/*
 * Check if a given key exists in the given hashmap.
 * Write a pointer to the target node on success.
 * Otherwise SXERR_NOTFOUND is returned on failure.
 */
static sxi32 vedisTableLookup(
	vedis_table *pMap,          /* Target hashmap */
	vedis_value *pKey,            /* Lookup key */
	vedis_table_entry **ppNode   /* OUT: target node on success */
	)
{
	vedis_table_entry *pNode = 0; /* cc -O6 warning */
	sxi32 rc;
	if( pKey->iFlags & (MEMOBJ_STRING|MEMOBJ_HASHMAP|MEMOBJ_REAL) ){
		if( (pKey->iFlags & MEMOBJ_STRING) == 0 ){
			/* Force a string cast */
			vedisMemObjToString(&(*pKey));
		}
		if( SyBlobLength(&pKey->sBlob) > 0 ){
			/* Perform a blob lookup */
			rc = vedisTableLookupBlobKey(&(*pMap), SyBlobData(&pKey->sBlob), SyBlobLength(&pKey->sBlob), &pNode);
			goto result;
		}
	}
	/* Perform an int lookup */
	if((pKey->iFlags & MEMOBJ_INT) == 0 ){
		/* Force an integer cast */
		vedisMemObjToInteger(pKey);
	}
	/* Perform an int lookup */
	rc = vedisTableLookupIntKey(&(*pMap), pKey->x.iVal, &pNode);
result:
	if( rc == SXRET_OK ){
		/* Node found */
		if( ppNode ){
			*ppNode = pNode;
		}
		return SXRET_OK;
	}
	/* No such entry */
	return SXERR_NOTFOUND;
}
/*
 * Insert a given key and it's associated value (if any) in the given
 * hashmap.
 * If a node with the given key already exists in the database
 * then this function overwrite the old value.
 */
static sxi32 vedisTableInsert(
	vedis_table *pMap, /* Target hashmap */
	vedis_value *pKey,   /* Lookup key  */
	vedis_value *pVal    /* Node value */
	)
{
	vedis_table_entry *pNode = 0;
	sxi32 rc = SXRET_OK;
	
	if( pKey && (pKey->iFlags & (MEMOBJ_STRING|MEMOBJ_HASHMAP)) ){
		if( (pKey->iFlags & MEMOBJ_STRING) == 0 ){
			/* Force a string cast */
			vedisMemObjToString(&(*pKey));
		}
		if( SyBlobLength(&pKey->sBlob) < 1  ){
			/* Automatic index assign */
			pKey = 0;
			goto IntKey;
		}
		if( SXRET_OK == vedisTableLookupBlobKey(&(*pMap), SyBlobData(&pKey->sBlob), 
			SyBlobLength(&pKey->sBlob), &pNode) ){
				/* Overwrite the old value */
				SyBlobReset(&pNode->sData);
				if( pVal ){
					const char *zVal;
					int nByte;
					/* Get a string representation */
					zVal = vedis_value_to_string(pVal,&nByte);
					if( nByte > 0 ){
						SyBlobAppend(&pNode->sData,zVal,(sxu32)nByte);
					}
				}
				if( !vedisPagerisMemStore(pMap->pStore) ){
					rc = vedisTableEntrySerialize(pMap,pNode);
				}
				return rc;
		}else{
			/* Perform a blob-key insertion */
			rc = vedisTableInsertBlobKey(&(*pMap),SyBlobData(&pKey->sBlob),SyBlobLength(&pKey->sBlob),&(*pVal));
			if( rc != VEDIS_OK ){
				return rc;
			}
		}
		return rc;
	}
IntKey:
	if( pKey ){
		if((pKey->iFlags & MEMOBJ_INT) == 0 ){
			/* Force an integer cast */
			vedisMemObjToInteger(pKey);
		}
		if( SXRET_OK == vedisTableLookupIntKey(&(*pMap), pKey->x.iVal, &pNode) ){
			/* Overwrite the old value */
			SyBlobReset(&pNode->sData);
				if( pVal ){
					const char *zVal;
					int nByte;
					/* Get a string representation */
					zVal = vedis_value_to_string(pVal,&nByte);
					if( nByte > 0 ){
						SyBlobAppend(&pNode->sData,zVal,(sxu32)nByte);
					}
				}
				rc = VEDIS_OK;
				if( !vedisPagerisMemStore(pMap->pStore) ){
					rc = vedisTableEntrySerialize(pMap,pNode);
				}
				return rc;
		}
		/* Perform a 64-bit-int-key insertion */
		rc = vedisTableInsertIntKey(&(*pMap), pKey->x.iVal, &(*pVal));
		if( rc == SXRET_OK ){
			if( pKey->x.iVal >= pMap->iNextIdx ){
				/* Increment the automatic index */ 
				pMap->iNextIdx = pKey->x.iVal + 1;
				/* Make sure the automatic index is not reserved */
				while( SXRET_OK == vedisTableLookupIntKey(&(*pMap), pMap->iNextIdx, 0) ){
					pMap->iNextIdx++;
				}
			}
		}
	}else{
		/* Assign an automatic index */
		rc = vedisTableInsertIntKey(&(*pMap),pMap->iNextIdx,&(*pVal));
		if( rc == SXRET_OK ){
			++pMap->iNextIdx;
		}
	}
	/* Insertion result */
	return rc;
}
/*
 * Exported interfaces used by the built-in Vedis commands.
 */
/*
 * Remove the given entry from the target table.
 */
VEDIS_PRIVATE int VedisRemoveTableEntry(vedis_table *pTable,vedis_table_entry *pEntry)
{
	int rc = VEDIS_OK;
	if( !vedisPagerisMemStore(pTable->pStore) ){
		SyBlob sWorker;
		/* Remove the entry from disk */
		SyBlobInit(&sWorker,&pTable->pStore->sMem);
		/* Build the key */
		SyBlobFormat(&sWorker,"vt%z%d%u",&pTable->sName,pTable->iTableType,pEntry->nId);
		/* Perform the deletion */
		rc = vedisKvDelete(pTable->pStore,SyBlobData(&sWorker),(int)SyBlobLength(&sWorker));
		/* Cleanup */
		SyBlobRelease(&sWorker);
	}
	vedisTableUnlinkNode(pEntry);
	return rc;
}
/*
 * Fetch a record from the given table.
 */
VEDIS_PRIVATE vedis_table_entry * vedisTableGetRecord(vedis_table *pTable,vedis_value *pKey)
{
	vedis_table_entry *pEntry;
	int rc;
	/* Fetch */
	rc = vedisTableLookup(pTable,pKey,&pEntry);
	return rc == VEDIS_OK ? pEntry : 0 /* No such entry */;
}
/*
 * Only lists.
 */
VEDIS_PRIVATE vedis_table_entry * vedisTableGetRecordByIndex(vedis_table *pTable,sxu32 nIndex)
{
	vedis_table_entry *pEntry = 0; /* cc warning */
	vedis_value sKey;
	int rc;
	vedisMemObjInitFromInt(pTable->pStore,&sKey,(sxi64)nIndex);
	/* Fetch */
	rc = vedisTableLookup(pTable,&sKey,&pEntry);
	vedisMemObjRelease(&sKey);
	return rc == VEDIS_OK ? pEntry : 0 /* No such entry */;
}
/*
 * Delete a record from the given table.
 */
VEDIS_PRIVATE int vedisTableDeleteRecord(vedis_table *pTable,vedis_value *pKey)
{
	vedis_table_entry *pEntry;
	int rc;
	/* Fetch */
	rc = vedisTableLookup(pTable,pKey,&pEntry);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Perform the deletion */
	rc = VedisRemoveTableEntry(pTable,pEntry);
	return rc;
}
/*
 * Insert a record into the given table.
 */
VEDIS_PRIVATE int vedisTableInsertRecord(vedis_table *pTable,vedis_value *pKey,vedis_value *pData)
{
	int rc;
	rc = vedisTableInsert(pTable,pKey,pData);
	return rc;
}
/*
 * Return the total entries in a given table.
 */
VEDIS_PRIVATE sxu32 vedisTableLength(vedis_table *pTable)
{
	return pTable->nEntry;
}
/*
 * Point to the first entry in a given table.
 */
VEDIS_PRIVATE void vedisTableReset(vedis_table *pTable)
{
	/* Reset the loop cursor */
	pTable->pCur = pTable->pFirst;
}
/*
 * Return the entry pointed by the cursor in a given table.
 */
VEDIS_PRIVATE vedis_table_entry * vedisTableNextEntry(vedis_table *pTable)
{
	vedis_table_entry *pCur = pTable->pCur;
	if( pCur == 0 ){
		/* End of the list, return null */
		return 0;
	}
	/* Advance the node cursor */
	pTable->pCur = pCur->pPrev; /* Reverse link */
	/* Current Entry */
	return pCur;
}
/*
 * Return the last entry in a given table.
 */
VEDIS_PRIVATE vedis_table_entry * vedisTableLastEntry(vedis_table *pTable)
{
	return pTable->nEntry > 0 ? pTable->pLast : 0 /* Empty list*/;
}
/*
 * Return the first entry in a given table.
 */
VEDIS_PRIVATE vedis_table_entry * vedisTableFirstEntry(vedis_table *pTable)
{
	return pTable->nEntry > 0 ? pTable->pFirst : 0 /* Empty list*/;
}
/*
 * Install a freshly created table.
 */
static void vedisInstallTable(vedis *pStore,vedis_table *pTable,sxu32 nHash)
{
	sxu32 nBucket = nHash & (pStore->nTableSize - 1);
	/* Install in the corresponding bucket */
	pTable->pNextCol = pStore->apTable[nBucket];
	if( pStore->apTable[nBucket] ){
		pStore->apTable[nBucket]->pPrevCol = pTable;
	}
	pStore->apTable[nBucket] = pTable;
	/* Link the table */
	MACRO_LD_PUSH(pStore->pTableList,pTable);
	pStore->nTable++;
	if( (pStore->nTable >= pStore->nTableSize * 4) && pStore->nTable < 100000 ){
		/* Grow the hashtable */
		sxu32 nNewSize = pStore->nTableSize << 1;
		vedis_table *pEntry,**apNew;
		sxu32 n;
		apNew = (vedis_table **)SyMemBackendAlloc(&pStore->sMem, nNewSize * sizeof(vedis_table *));
		if( apNew ){
			sxu32 iBucket;
			/* Zero the new table */
			SyZero((void *)apNew, nNewSize * sizeof(vedis_table *));
			/* Rehash all entries */
			n = 0;
			pEntry = pStore->pTableList;
			for(;;){
				/* Loop one */
				if( n >= pStore->nTable ){
					break;
				}
				pEntry->pNextCol = pEntry->pPrevCol = 0;
				/* Install in the new bucket */
				iBucket = SyBinHash(SyStringData(&pEntry->sName),SyStringLength(&pEntry->sName)) & (nNewSize - 1);
				pEntry->pNextCol = apNew[iBucket];
				if( apNew[iBucket] ){
					apNew[iBucket]->pPrevCol = pEntry;
				}
				apNew[iBucket] = pEntry;
				/* Point to the next entry */
				pEntry = pEntry->pNext;
				n++;
			}
			/* Release the old table and reflect the change */
			SyMemBackendFree(&pStore->sMem,(void *)pStore->apTable);
			pStore->apTable = apNew;
			pStore->nTableSize  = nNewSize;
		}
	}
}
/*
 * Allocate a new table.
 */
static vedis_table * vedisNewTable(vedis *pStore,SyString *pName,int iType,sxu32 nHash)
{
	vedis_table *pTable;
	char *zPtr;
	/* Allocate a new instance */
	pTable = (vedis_table *)SyMemBackendAlloc(&pStore->sMem,sizeof(vedis_table)+pName->nByte);
	if( pTable == 0 ){
		return 0;
	}
	/* Zero the structure */
	SyZero(pTable,sizeof(vedis_table));
	/* Fill-in */
	pTable->iTableType = iType;
	pTable->pStore = pStore;
	pTable->xIntHash  = VedisTableIntHash;
	pTable->xBlobHash = VedisTableBinHash; 
	zPtr = (char *)&pTable[1];
	SyMemcpy(pName->zString,zPtr,pName->nByte);
	SyStringInitFromBuf(&pTable->sName,zPtr,pName->nByte);
	/* Install the table */
	vedisInstallTable(pStore,pTable,nHash);
	return pTable;
}
#define VEDIS_TABLE_MAGIC        0xCA10 /* Table magic number */
#define VEDIS_TABLE_ENTRY_MAGIC  0xEF32 /* Table entry magic number */
/*
 * Serialize a vedis table to disk.
 */
static int vedisTableSerialize(vedis_table *pTable)
{
	vedis *pStore = pTable->pStore;
	vedis_kv_methods *pMethods;
	vedis_kv_engine *pEngine;
	SyBlob sWorker;
	sxu32 nOfft;
	int rc;
	
	/* Start the serialization process */
	pEngine = vedisPagerGetKvEngine(pStore);
	pMethods = pEngine->pIo->pMethods;
	if( pMethods->xReplace ==  0 ){
		vedisGenErrorFormat(pStore,
			"Cannot serialize table '%z' due to a read-only KV storage engine '%s'",
			&pTable->sName,pMethods->zName
			);
		return VEDIS_READ_ONLY;
	}
	SyBlobInit(&sWorker,&pStore->sMem);
	/* Write the table header */
	SyBlobFormat(&sWorker,"vt%d%z",pTable->iTableType,&pTable->sName);
	nOfft = SyBlobLength(&sWorker);
	/* table header */
	SyBlobAppendBig16(&sWorker,VEDIS_TABLE_MAGIC); /* Magic number */
	SyBlobAppendBig32(&sWorker,pTable->nLastID);   /* Last assigned ID */
	SyBlobAppendBig32(&sWorker,pTable->nEntry);    /* Total number of records  */
	/* Write the header */
	rc = pMethods->xReplace(pEngine,SyBlobData(&sWorker),(int)nOfft,SyBlobDataAt(&sWorker,nOfft),SyBlobLength(&sWorker)-nOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* All done, clean up and return */
	SyBlobRelease(&sWorker);
	return VEDIS_OK;
}
/*
 * Serialize a vedis table entry to Disk
 */
static int vedisTableEntrySerialize(vedis_table *pTable,vedis_table_entry *pEntry)
{
	vedis *pStore = pTable->pStore;
	vedis_kv_methods *pMethods;
	vedis_kv_engine *pEngine;
	SyBlob sWorker;
	sxu32 nByte = 0;
	sxu32 nOfft;
	
	/* Start the serialization process */
	pEngine = vedisPagerGetKvEngine(pStore);
	pMethods = pEngine->pIo->pMethods;
	if( pMethods->xReplace ==  0 ){
		vedisGenErrorFormat(pStore,
			"Cannot serialize table '%z' entry due to a read-only KV storage engine '%s'",
			&pTable->sName,pMethods->zName
			);
		return VEDIS_READ_ONLY;
	}
	SyBlobInit(&sWorker,&pStore->sMem);
	/* Prepare the key */
	SyBlobFormat(&sWorker,"vt%z%d%u",&pTable->sName,pTable->iTableType,pEntry->nId);
	nOfft = SyBlobLength(&sWorker);
	/* Prepare the payload */
	SyBlobAppendBig16(&sWorker,VEDIS_TABLE_ENTRY_MAGIC); /* Magic */
	SyBlobAppendBig32(&sWorker,pEntry->nId); /* Unique ID */
	SyBlobAppend(&sWorker,(const void *)&pEntry->iType,sizeof(char)); /* Key type */
	if( pEntry->iType == VEDIS_TABLE_ENTRY_BLOB_NODE ){
		nByte = SyBlobLength(&pEntry->xKey.sKey);
	}
	SyBlobAppendBig32(&sWorker,nByte);
	SyBlobAppendBig32(&sWorker,SyBlobLength(&pEntry->sData)); /* Data length */
	if( pEntry->iType == VEDIS_TABLE_ENTRY_BLOB_NODE ){
		SyBlobDup(&pEntry->xKey.sKey,&sWorker);
	}
	SyBlobDup(&pEntry->sData,&sWorker);
	/* Perform the write process */
	pMethods->xReplace(pEngine,SyBlobData(&sWorker),(int)nOfft,SyBlobDataAt(&sWorker,nOfft),SyBlobLength(&sWorker) - nOfft);	
	/* All done, clean up and return */
	SyBlobRelease(&sWorker);
	return VEDIS_OK;
}
/*
 * On commit callback.
 */
VEDIS_PRIVATE int vedisOnCommit(void *pUserData)
{
	vedis *pStore = (vedis *)pUserData;
	vedis_table *pTable;
	sxu32 n;
	int rc;
	/* Make sure we are dealing with an on-disk data store */
	if( vedisPagerisMemStore(pStore) ){
		return VEDIS_OK;
	}
	pTable = pStore->pTableList;
	for( n = 0 ; n < pStore->nTable ; ++n ){
		/* Serialize this table */
		rc = vedisTableSerialize(pTable);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Point to the next entry */
		pTable = pTable->pNext;
	}
	return VEDIS_OK;
}
/*
 * Unserialize an on-disk table.
 */
static vedis_table * vedisTableUnserialize(
	vedis *pStore,SyString *pName,int iType,sxu32 nHash,
	const unsigned char *zBuf,sxu32 nByte,
	sxu32 *pEntry,sxu32 *pLastID
	)
{
	vedis_table *pNew;
	sxu16 iMagic;
	/* Sanity check */
	if( nByte != 2 /* Magic */ + 4 /* Last unique ID */ + 4 /* Total records */ ){
		/* Corrupt */
		return 0;
	}
	SyBigEndianUnpack16(zBuf,&iMagic);
	if( iMagic != VEDIS_TABLE_MAGIC ){
		return 0;
	}
	zBuf += 2; /* Magic */
	SyBigEndianUnpack32(zBuf,pLastID); /* Last Unique ID */
	zBuf += 4;
	SyBigEndianUnpack32(zBuf,pEntry); /* Total number of records */
	zBuf += 4;
	/* Allocate a new table */
	pNew = vedisNewTable(pStore,pName,iType,nHash);
	return pNew;
}
/*
 * Unserialize a table entry.
 */
static int vedisUnserializeEntry(vedis_table *pTable,const unsigned char *zPtr,sxu32 nByte)
{
	const unsigned char *zBuf = zPtr;
	vedis_value sKey,sData;
	sxu32 nData,nKey;
	SyString sEntry;
	sxu16 iMagic;
	sxu32 nId;
	int iType;
	
	if( nByte < 2 /* Magic */ + 4 /* Unique ID */+ 1 /* type */ + 4 /* key length */ + 4 /* data length */ ){
		return VEDIS_CORRUPT;
	}
	SyBigEndianUnpack16(zBuf,&iMagic); /* Magic */
	if( iMagic != VEDIS_TABLE_ENTRY_MAGIC ){
		return VEDIS_CORRUPT;
	}
	zBuf += 2; /* Magic */
	SyBigEndianUnpack32(zBuf,&nId);
	zBuf += 4; /* Unique ID */
	iType = (int)zBuf[0];
	if( iType != VEDIS_TABLE_ENTRY_BLOB_NODE && iType != VEDIS_TABLE_ENTRY_INT_NODE ){
		return VEDIS_CORRUPT;
	}
	zBuf++;
	SyBigEndianUnpack32(zBuf,&nKey); /* Key */
	zBuf += 4;
	SyBigEndianUnpack32(zBuf,&nData); /* Data */
	zBuf += 4;
	/* Sanity check */
	if(  (sxu32)(&zPtr[nByte] - zBuf) != nKey + nData ){
		return VEDIS_CORRUPT;
	}
	SyStringInitFromBuf(&sEntry,zBuf,nKey);
	vedisMemObjInitFromString(pTable->pStore,&sKey,&sEntry);
	zBuf += nKey;
	SyStringInitFromBuf(&sEntry,zBuf,nData);
	vedisMemObjInitFromString(pTable->pStore,&sData,&sEntry);
	/* Perform the insertion */
	if( VEDIS_OK == vedisTableInsert(pTable,nKey > 0 ? &sKey : 0,nData > 0 ? &sData : 0) ){
		/* Set the real ID */
		pTable->pLast->nId = nId;
		if( pTable->nLastID > 0 ){
			pTable->nLastID--;
		}
	}
	/* Clean up and return */
	vedisMemObjRelease(&sKey);
	vedisMemObjRelease(&sData);
	return VEDIS_OK;
}
/*
 * Fetch a table from disk and load its entries.
 */
static vedis_table * vedisTableLoadFromDisk(vedis *pStore,SyString *pName,int iType,sxu32 nHash)
{
	vedis_table *pTable;
	SyBlob sWorker;
	sxu32 nLastID;
	sxu32 nEntry;
	sxu32 nByte;
	sxu32 nOfft;
	sxu32 nId;
	sxu32 n;
	int rc;
	/* Make sure we are dealing with an on-disk data store */
	if( vedisPagerisMemStore(pStore) ){
		return 0;
	}
	/* Go fetch */
	SyBlobInit(&sWorker,&pStore->sMem);
	SyBlobFormat(&sWorker,"vt%d%z",iType,pName);
	nOfft = SyBlobLength(&sWorker);
	rc = vedisKvFetchCallback(pStore,SyBlobData(&sWorker),(int)nOfft,vedisDataConsumer,&sWorker);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	nByte = SyBlobLength(&sWorker) - nOfft;
	/* Unserialize the table */
	nEntry = 0;
	pTable = vedisTableUnserialize(pStore,pName,iType,nHash,(const unsigned char *)SyBlobDataAt(&sWorker,nOfft),nByte,&nEntry,&nLastID);
	if( pTable == 0 ){
		/* No such table */
		goto fail;
	}
	pTable->iFlags |= VEDIS_TABLE_DISK_LOAD;
	pTable->nLastID = nLastID;
	/* Unserialize table entries */
	n = nId = 0;
	for( ;; ){
		if( n >= nEntry ){
			break;
		}
		SyBlobReset(&sWorker);
		/* Read the entry */
		SyBlobFormat(&sWorker,"vt%z%d%u",&pTable->sName,pTable->iTableType,nId++);
		nOfft = SyBlobLength(&sWorker);
		rc = vedisKvFetchCallback(pStore,SyBlobData(&sWorker),nOfft,vedisDataConsumer,&sWorker);
		if( rc == VEDIS_OK ){
			/* Decode the entry */
			vedisUnserializeEntry(pTable,(const unsigned char *)SyBlobDataAt(&sWorker,nOfft),SyBlobLength(&sWorker) - nOfft);
			n++;
		}else if( rc != VEDIS_NOTFOUND ){
			break;
		}
	}
	SyBlobRelease(&sWorker);
	/* Remove stale flags */
	pTable->iFlags &= ~VEDIS_TABLE_DISK_LOAD;
	/* All done */
	return pTable;
fail:
	SyBlobRelease(&sWorker);
	/* No such table */
	return 0;
}
/*
 * Fetch a table and load its entries.
 */
VEDIS_PRIVATE vedis_table * vedisFetchTable(vedis *pDb,vedis_value *pName,int create_new,int iType)
{
	vedis_table *pTable;
	const char *zName;
	SyString sName;
	sxu32 nHash;
	int nByte;
	/* Extract table name */
	zName = vedis_value_to_string(pName,&nByte);
	if( nByte < 1 ){
		/* Invalid table name */
		vedisGenError(pDb,"Invalid table name");
		return 0;
	}
	SyStringInitFromBuf(&sName,zName,nByte);
	/* Fetch table */
	nHash = SyBinHash(sName.zString,sName.nByte);
	pTable = pDb->apTable[nHash & (pDb->nTableSize - 1)];
	for(;;){
		if( pTable == 0 ){
			break;
		}
		if( pTable->iTableType == iType && SyStringCmp(&sName,&pTable->sName,SyMemcmp) == 0 ){
			/* Table found */
			return pTable;
		}
		/* Point to the next entry */
		pTable = pTable->pNext;
	}
	/* Try to load from disk */
	pTable = vedisTableLoadFromDisk(pDb,&sName,iType,nHash);
	if( pTable ){
		return pTable;
	}
	if( !create_new ){
		/* No such table */
		return 0;
	}
	/* fall through, create a new table */
	pTable = vedisNewTable(pDb,&sName,iType,nHash);
	if( !pTable ){
		vedisGenOutofMem(pDb);
		return 0;
	}
	return pTable;
}
/*
 * Return the name of the given table.
 */
VEDIS_PRIVATE  SyString * vedisTableName(vedis_table *pEntry)
{
	return &pEntry->sName;
}
/*
 * Return the next table on the chain.
 */
VEDIS_PRIVATE  vedis_table * vedisTableChain(vedis_table *pEntry)
{
	return pEntry->pNext;
}
/*
 * ----------------------------------------------------------
 * File: parse.c
 * MD5: 90f0b67cbdc5dc75d39c2ce4f9ba3edd
 * ----------------------------------------------------------
 */
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
/* $SymiscID: parse.c v1.3 Win7 2013-07-08 05:42 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* Vedis command Lexer & parser */
/*
 * Tokenize a raw input.
 * Get a single low-level token from the input file. Update the stream pointer so that
 * it points to the first character beyond the extracted token.
 */
static sxi32 vedisTokenizeInput(SyStream *pStream,SyToken *pToken,void *pUserData,void *pCtxData)
{
	const unsigned char *zIn;
	SyString *pStr;
	sxi32 c;
	/* Ignore leading white spaces */
	while( pStream->zText < pStream->zEnd && pStream->zText[0] < 0xc0 && SyisSpace(pStream->zText[0]) ){
		/* Advance the stream cursor */
		if( pStream->zText[0] == '\n' ){
			/* Update line counter */
			pStream->nLine++;
		}
		pStream->zText++;
	}
	if( pStream->zText >= pStream->zEnd ){
		/* End of input reached */
		return SXERR_EOF;
	}
	/* Record token starting position and line */
	pToken->nLine = pStream->nLine;
	pToken->pUserData = 0;
	pStr = &pToken->sData;
	SyStringInitFromBuf(pStr, pStream->zText, 0);
	if( pStream->zText[0] == ';' ){
		pStream->zText++;
		/* A stream of semi-colons */
		while( pStream->zText < pStream->zEnd && pStream->zText[0] < 0xc0 && pStream->zText[0] == ';' ){
			pStream->zText++;
		}
		/* Mark the token */
		pToken->nType = VEDIS_TK_SEMI;
		/* Record token length */
		pStr->nByte = (sxu32)((const char *)pStream->zText-pStr->zString);
	}else if( SyisDigit(pStream->zText[0]) ){
		pStream->zText++;
		/* Decimal digit stream */
		while( pStream->zText < pStream->zEnd && pStream->zText[0] < 0xc0 && SyisDigit(pStream->zText[0]) ){
			pStream->zText++;
		}
		/* Mark the token as integer until we encounter a real number */
		pToken->nType = VEDIS_TK_INTEGER;
		if( pStream->zText < pStream->zEnd ){
			c = pStream->zText[0];
			if( c == '.' ){
				/* Real number */
				pStream->zText++;
				while( pStream->zText < pStream->zEnd && pStream->zText[0] < 0xc0 && SyisDigit(pStream->zText[0]) ){
					pStream->zText++;
				}
				if( pStream->zText < pStream->zEnd ){
					c = pStream->zText[0];
					if( c=='e' || c=='E' ){
						pStream->zText++;
						if( pStream->zText < pStream->zEnd ){
							c = pStream->zText[0];
							if( (c =='+' || c=='-') && &pStream->zText[1] < pStream->zEnd  &&
								pStream->zText[1] < 0xc0 && SyisDigit(pStream->zText[1]) ){
									pStream->zText++;
							}
							while( pStream->zText < pStream->zEnd && pStream->zText[0] < 0xc0 && SyisDigit(pStream->zText[0]) ){
								pStream->zText++;
							}
						}
					}
				}
				pToken->nType = VEDIS_TK_REAL;
			}else if( c=='e' || c=='E' ){
				SXUNUSED(pUserData); /* Prevent compiler warning */
				SXUNUSED(pCtxData);
				pStream->zText++;
				if( pStream->zText < pStream->zEnd ){
					c = pStream->zText[0];
					if( (c =='+' || c=='-') && &pStream->zText[1] < pStream->zEnd  &&
						pStream->zText[1] < 0xc0 && SyisDigit(pStream->zText[1]) ){
							pStream->zText++;
					}
					while( pStream->zText < pStream->zEnd && pStream->zText[0] < 0xc0 && SyisDigit(pStream->zText[0]) ){
						pStream->zText++;
					}
				}
				pToken->nType = VEDIS_TK_REAL;
			}else if( c == 'x' || c == 'X' ){
				/* Hex digit stream */
				pStream->zText++;
				while( pStream->zText < pStream->zEnd && pStream->zText[0] < 0xc0 && SyisHex(pStream->zText[0]) ){
					pStream->zText++;
				}
			}else if(c  == 'b' || c == 'B' ){
				/* Binary digit stream */
				pStream->zText++;
				while( pStream->zText < pStream->zEnd && (pStream->zText[0] == '0' || pStream->zText[0] == '1') ){
					pStream->zText++;
				}
			}
		}
		/* Record token length */
		pStr->nByte = (sxu32)((const char *)pStream->zText-pStr->zString);
	}else if( pStream->zText[0] == '"' || pStream->zText[0] == '\'' ){
		/* Quoted string */
		c = pStream->zText[0];
		pStream->zText++;
		pStr->zString++;
		while( pStream->zText < pStream->zEnd ){
			if( pStream->zText[0] == c  ){
				if( pStream->zText[-1] != '\\' ){
					break;
				}
			}
			if( pStream->zText[0] == '\n' ){
				pStream->nLine++;
			}
			pStream->zText++;
		}
		/* Record token length and type */
		pStr->nByte = (sxu32)((const char *)pStream->zText-pStr->zString);
		pToken->nType = VEDIS_TK_STREAM;
		/* Jump the trailing quote */
		pStream->zText++;
	}else{
		/* The following code fragment is taken verbatim from the xPP source tree.
		 * xPP is a modern embeddable macro processor with advanced features useful for
		 * application seeking for a production quality, ready to use macro processor.
		 * xPP is a widely used library developed and maintened by Symisc Systems.
		 * You can reach the xPP home page by following this link:
		 * http://symisc.net/
		 */
		/* Isolate UTF-8 or alphanumeric stream */
		if( pStream->zText[0] < 0xc0 ){
			pStream->zText++;
		}
		for(;;){
			zIn = pStream->zText;
			if( zIn[0] >= 0xc0 ){
				zIn++;
				/* UTF-8 stream */
				while( zIn < pStream->zEnd && ((zIn[0] & 0xc0) == 0x80) ){
					zIn++;
				}
			}
			/* Delimit the stream */
			while( zIn < pStream->zEnd && zIn[0] < 0xc0 && zIn[0] != ';' && !SyisSpace(zIn[0]) ){
				zIn++;
			}
			if( zIn == pStream->zText ){
				/* End of the stream */
				break;
			}
			/* Synchronize pointers */
			pStream->zText = zIn;
		}
		/* Record token length */
		pStr->nByte = (sxu32)((const char *)pStream->zText-pStr->zString);
		/* A simple identifier */
		pToken->nType = VEDIS_TK_STREAM;
	}
	/* Tell the upper-layer to save the extracted token for later processing */
	return SXRET_OK;
}
/*
 * Tokenize a raw input. 
 */
static sxi32 vedisTokenize(const char *zInput,sxu32 nLen,SySet *pOut)
{
	SyLex sLexer;
	sxi32 rc;
	/* Initialize the lexer */
	rc = SyLexInit(&sLexer, &(*pOut),vedisTokenizeInput,0);
	if( rc != SXRET_OK ){
		return rc;
	}
	/* Tokenize input */
	rc = SyLexTokenizeInput(&sLexer, zInput, nLen, 0, 0, 0);
	/* Release the lexer */
	SyLexRelease(&sLexer);
	/* Tokenization result */
	return rc;
}
/*
 * Vedis parser state is recorded in an instance of the following structure.
 */
typedef struct vedis_gen_state vedis_gen_state;
struct vedis_gen_state
{
	SyToken *pIn;  /* Token stream */
	SyToken *pEnd; /* End of the token stream */
	vedis *pVedis;    /* Vedis handle  */
};
static int vedisInitContext(vedis_context *pCtx,vedis *pVedis,vedis_cmd *pCmd)
{
	pCtx->pVedis = pVedis;
	pCtx->pCmd = pCmd;
	SyBlobInit(&pCtx->sWorker,&pVedis->sMem);
	SySetInit(&pCtx->sVar, &pVedis->sMem, sizeof(vedis_value *));
	pCtx->pRet = &pVedis->sResult;
	/* Invalidate any prior representation */
	vedisMemObjRelease(pCtx->pRet);
	return VEDIS_OK;
}
VEDIS_PRIVATE SyBlob * VedisContextResultBuffer(vedis_context *pCtx)
{
	return &pCtx->pRet->sBlob;
}
VEDIS_PRIVATE SyBlob * VedisContextWorkingBuffer(vedis_context *pCtx)
{
	return &pCtx->sWorker;
}
static void vedisReleaseContext(vedis_context *pCtx)
{
	sxu32 n;
	if( SySetUsed(&pCtx->sVar) > 0 ){
		/* Context alloacated values */
		vedis_value **apObj = (vedis_value **)SySetBasePtr(&pCtx->sVar);
		for( n = 0 ; n < SySetUsed(&pCtx->sVar) ; ++n ){
			if( apObj[n] == 0 ){
				/* Already released */
				continue;
			}
			vedisMemObjRelease(apObj[n]);
			SyMemBackendPoolFree(&pCtx->pVedis->sMem, apObj[n]);
		}
		SySetRelease(&pCtx->sVar);
	}
	SyBlobRelease(&pCtx->sWorker);
}
static void vedisObjContainerDestroy(SySet *aValues,vedis *pVedis)
{
	vedis_value **apValues = (vedis_value **)SySetBasePtr(aValues);
	sxu32 n;
	for( n = 0 ; n < SySetUsed(aValues) ; ++n ){
		vedis_value *pValue = apValues[n];
		/* Destroy the object */
		vedisObjectValueDestroy(pVedis,pValue);
	}
	SySetRelease(aValues);
}
static int vedisExec(vedis_gen_state *pGen)
{
	vedis_value *pValue;
	vedis_context sCtx;
	vedis_cmd *pCmd;
	vedis *pStore;
	SySet sValue;
	int rc;
	/* Get the target command */
	if( !(pGen->pIn->nType & VEDIS_TK_STREAM) ){
		vedisGenError(pGen->pVedis,"Invalid Vedis command");
		return SXERR_INVALID;
	}
	pStore = pGen->pVedis;
	/* Extract it */
	pCmd = vedisFetchCommand(pStore,&pGen->pIn->sData);
	if( pCmd == 0 ){
		vedisGenErrorFormat(pStore,"Unknown Vedis command: '%z'",&pGen->pIn->sData);
		return SXERR_UNKNOWN;
	}
	pGen->pIn++;
	/* Collect command arguments */
	SySetInit(&sValue,&pStore->sMem,sizeof(vedis_value *));
	while( pGen->pIn < pGen->pEnd && (pGen->pIn->nType != VEDIS_TK_SEMI /*';'*/) ){
		pValue = vedisNewObjectValue(pStore,pGen->pIn);
		if( pValue ){
			SySetPut(&sValue,(const void *)&pValue);
		}
		/* Point to the next token */
		pGen->pIn++;
	}
	/* Init the call context */
	vedisInitContext(&sCtx,pStore,pCmd);
	/* Invoke the command */
	rc = pCmd->xCmd(&sCtx,(int)SySetUsed(&sValue),(vedis_value **)SySetBasePtr(&sValue));
	if( rc == VEDIS_ABORT ){
		vedisGenErrorFormat(pGen->pVedis,"Vedis command '%z' request an operation abort",&pCmd->sName);
	}else{
		rc = VEDIS_OK;
	}
	/* Invoke any output consumer callback */
	if( pStore->xResultConsumer && rc == VEDIS_OK ){
		rc = pStore->xResultConsumer(sCtx.pRet,pStore->pUserData);
		if( rc != VEDIS_ABORT ){
			rc = VEDIS_OK;
		}
	}
	/* Cleanup */
	vedisReleaseContext(&sCtx);
	vedisObjContainerDestroy(&sValue,pGen->pVedis);
	return rc;
}

VEDIS_PRIVATE int vedisProcessInput(vedis *pVedis,const char *zInput,sxu32 nByte)
{
	SySet sToken;
	int rc;
	/* Prepare the tokenizer */
	SySetInit(&sToken,&pVedis->sMem,sizeof(SyToken));
	/* Tokenize the input */
	rc = vedisTokenize(zInput,nByte,&sToken);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	rc = VEDIS_OK;
	if( SySetUsed(&sToken) > 0 ){
		vedis_gen_state sGen;
		/* Init the parser state */
		sGen.pIn = (SyToken *)SySetBasePtr(&sToken);
		sGen.pEnd = &sGen.pIn[SySetUsed(&sToken)];
		sGen.pVedis = pVedis;
		/* Process the pipelined commands */
		for(;;){
			while( sGen.pIn < sGen.pEnd && sGen.pIn->nType == VEDIS_TK_SEMI ){
				/* Discard leading and trailing semi-colons */
				sGen.pIn++;
			}
			if( sGen.pIn >= sGen.pEnd ){
				/* End of the vedis input */
				break;
			}
			/* Execute the command if available */
			rc = vedisExec(&sGen);
			if( rc != VEDIS_OK ){
				break;
			}
		}
	}
	/* Fall through */
fail:
	/* Cleanup */
	SySetRelease(&sToken);
	return rc;
}
/*
 * ----------------------------------------------------------
 * File: pager.c
 * MD5: b4db2677f77d8b4f49a90287106a7de1
 * ----------------------------------------------------------
 */
/*
 * Symisc Vedis: An Embeddable NoSQL (Post Modern) Database Engine.
 * Copyright (C) 2012-2013, Symisc Systems http://vedis.org/
 * Version 1.1.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.org/licensing.html
 */
 /* $SymiscID: pager.c v1.1 Win7 2012-11-29 03:46 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/*
** This file implements the pager and the transaction manager for UnQLite (Mostly inspired from the SQLite3 Source tree).
**
** The Pager.eState variable stores the current 'state' of a pager. A
** pager may be in any one of the seven states shown in the following
** state diagram.
**
**                            OPEN <------+------+
**                              |         |      |
**                              V         |      |
**               +---------> READER-------+      |
**               |              |                |
**               |              V                |
**               |<-------WRITER_LOCKED--------->| 
**               |              |                |  
**               |              V                |
**               |<------WRITER_CACHEMOD-------->|
**               |              |                |
**               |              V                |
**               |<-------WRITER_DBMOD---------->|
**               |              |                |
**               |              V                |
**               +<------WRITER_FINISHED-------->+
** 
**  OPEN:
**
**    The pager starts up in this state. Nothing is guaranteed in this
**    state - the file may or may not be locked and the database size is
**    unknown. The database may not be read or written.
**
**    * No read or write transaction is active.
**    * Any lock, or no lock at all, may be held on the database file.
**    * The dbSize and dbOrigSize variables may not be trusted.
**
**  READER:
**
**    In this state all the requirements for reading the database in 
**    rollback mode are met. Unless the pager is (or recently
**    was) in exclusive-locking mode, a user-level read transaction is 
**    open. The database size is known in this state.
** 
**    * A read transaction may be active (but a write-transaction cannot).
**    * A SHARED or greater lock is held on the database file.
**    * The dbSize variable may be trusted (even if a user-level read 
**      transaction is not active). The dbOrigSize variables
**      may not be trusted at this point.
**    * Even if a read-transaction is not open, it is guaranteed that 
**      there is no hot-journal in the file-system.
**
**  WRITER_LOCKED:
**
**    The pager moves to this state from READER when a write-transaction
**    is first opened on the database. In WRITER_LOCKED state, all locks 
**    required to start a write-transaction are held, but no actual 
**    modifications to the cache or database have taken place.
**
**    In rollback mode, a RESERVED or (if the transaction was opened with 
**    EXCLUSIVE flag) EXCLUSIVE lock is obtained on the database file when
**    moving to this state, but the journal file is not written to or opened 
**    to in this state. If the transaction is committed or rolled back while 
**    in WRITER_LOCKED state, all that is required is to unlock the database 
**    file.
**
**    * A write transaction is active.
**    * If the connection is open in rollback-mode, a RESERVED or greater 
**      lock is held on the database file.
**    * The dbSize and dbOrigSize variables are all valid.
**    * The contents of the pager cache have not been modified.
**    * The journal file may or may not be open.
**    * Nothing (not even the first header) has been written to the journal.
**
**  WRITER_CACHEMOD:
**
**    A pager moves from WRITER_LOCKED state to this state when a page is
**    first modified by the upper layer. In rollback mode the journal file
**    is opened (if it is not already open) and a header written to the
**    start of it. The database file on disk has not been modified.
**
**    * A write transaction is active.
**    * A RESERVED or greater lock is held on the database file.
**    * The journal file is open and the first header has been written 
**      to it, but the header has not been synced to disk.
**    * The contents of the page cache have been modified.
**
**  WRITER_DBMOD:
**
**    The pager transitions from WRITER_CACHEMOD into WRITER_DBMOD state
**    when it modifies the contents of the database file.
**
**    * A write transaction is active.
**    * An EXCLUSIVE or greater lock is held on the database file.
**    * The journal file is open and the first header has been written 
**      and synced to disk.
**    * The contents of the page cache have been modified (and possibly
**      written to disk).
**
**  WRITER_FINISHED:
**
**    A rollback-mode pager changes to WRITER_FINISHED state from WRITER_DBMOD
**    state after the entire transaction has been successfully written into the
**    database file. In this state the transaction may be committed simply
**    by finalizing the journal file. Once in WRITER_FINISHED state, it is 
**    not possible to modify the database further. At this point, the upper 
**    layer must either commit or rollback the transaction.
**
**    * A write transaction is active.
**    * An EXCLUSIVE or greater lock is held on the database file.
**    * All writing and syncing of journal and database data has finished.
**      If no error occured, all that remains is to finalize the journal to
**      commit the transaction. If an error did occur, the caller will need
**      to rollback the transaction. 
**  
**
*/
#define PAGER_OPEN                  0
#define PAGER_READER                1
#define PAGER_WRITER_LOCKED         2
#define PAGER_WRITER_CACHEMOD       3
#define PAGER_WRITER_DBMOD          4
#define PAGER_WRITER_FINISHED       5
/*
** Journal files begin with the following magic string.  The data
** was obtained from /dev/random.  It is used only as a sanity check.
**
** NOTE: These values must be different from the one used by SQLite3
** to avoid journal file collision.
**
*/
static const unsigned char aJournalMagic[] = {
  0xc1, 0xd2, 0xfa, 0x77, 0x2b, 0x18, 0x27, 0x2a,
};
/*
** The journal header size for this pager. This is usually the same 
** size as a single disk sector. See also setSectorSize().
*/
#define JOURNAL_HDR_SZ(pPager) (pPager->iSectorSize)
/*
 * Database page handle.
 * Each raw disk page is represented in memory by an instance
 * of the following structure.
 */
typedef struct Page Page;
struct Page {
  /* Must correspond to vedis_page */
  unsigned char *zData;           /* Content of this page */
  void *pUserData;                /* Extra content */
  pgno pgno;                      /* Page number for this page */
  /**********************************************************************
  ** Elements above are public.  All that follows is private to pcache.c
  ** and should not be accessed by other modules.
  */
  Pager *pPager;                 /* The pager this page is part of */
  int flags;                     /* Page flags defined below */
  int nRef;                      /* Number of users of this page */
  Page *pNext, *pPrev;    /* A list of all pages */
  Page *pDirtyNext;             /* Next element in list of dirty pages */
  Page *pDirtyPrev;             /* Previous element in list of dirty pages */
  Page *pNextCollide,*pPrevCollide; /* Collission chain */
  Page *pNextHot,*pPrevHot;    /* Hot dirty pages chain */
};
/* Bit values for Page.flags */
#define PAGE_DIRTY             0x002  /* Page has changed */
#define PAGE_NEED_SYNC         0x004  /* fsync the rollback journal before
                                       ** writing this page to the database */
#define PAGE_DONT_WRITE        0x008  /* Dont write page content to disk */
#define PAGE_NEED_READ         0x010  /* Content is unread */
#define PAGE_IN_JOURNAL        0x020  /* Page written to the journal */
#define PAGE_HOT_DIRTY         0x040  /* Hot dirty page */
#define PAGE_DONT_MAKE_HOT     0x080  /* Dont make this page Hot. In other words,
									   * do not link it to the hot dirty list.
									   */
/*
 * Each active database pager is represented by an instance of
 * the following structure.
 */
struct Pager
{
  SyMemBackend *pAllocator;      /* Memory backend */
  vedis *pDb;                  /* DB handle that own this instance */
  vedis_kv_engine *pEngine;    /* Underlying KV storage engine */
  char *zFilename;               /* Name of the database file */
  char *zJournal;                /* Name of the journal file */
  vedis_vfs *pVfs;             /* Underlying virtual file system */
  vedis_file *pfd,*pjfd;       /* File descriptors for database and journal */
  pgno dbSize;                   /* Number of pages in the file */
  pgno dbOrigSize;               /* dbSize before the current change */
  sxi64 dbByteSize;              /* Database size in bytes */
  void *pMmap;                   /* Read-only Memory view (mmap) of the whole file if requested (VEDIS_OPEN_MMAP). */
  sxu32 nRec;                    /* Number of pages written to the journal */
  SyPRNGCtx sPrng;               /* PRNG Context */
  sxu32 cksumInit;               /* Quasi-random value added to every checksum */
  sxu32 iOpenFlags;              /* Flag passed to vedis_open() after processing */
  sxi64 iJournalOfft;            /* Journal offset we are reading from */
  int (*xBusyHandler)(void *);   /* Busy handler */
  void *pBusyHandlerArg;         /* First arg to xBusyHandler() */
  void (*xPageUnpin)(void *);    /* Page Unpin callback */
  void (*xPageReload)(void *);   /* Page Reload callback */
  int (*xCommit)(void *);        /* On commit user callback */
  void *pCommitData;             /* First arg to xCommit() */
  Bitvec *pVec;                  /* Bitmap */
  Page *pHeader;                 /* Page one of the database (Unqlite header) */
  Sytm tmCreate;                 /* Database creation time */
  SyString sKv;                  /* Underlying Key/Value storage engine name */
  int iState;                    /* Pager state */
  int iLock;                     /* Lock state */
  sxi32 iFlags;                  /* Control flags (see below) */
  int is_mem;                    /* True for an in-memory database */
  int is_rdonly;                 /* True for a read-only database */
  int no_jrnl;                   /* TRUE to omit journaling */
  int iPageSize;                 /* Page size in bytes (default 4K) */
  int iSectorSize;               /* Size of a single sector on disk */
  unsigned char *zTmpPage;       /* Temporary page */
  Page *pFirstDirty;             /* First dirty pages */
  Page *pDirty;                  /* Transient list of dirty pages */
  Page *pAll;                    /* List of all pages */
  Page *pHotDirty;               /* List of hot dirty pages */
  Page *pFirstHot;               /* First hot dirty page */
  sxu32 nHot;                    /* Total number of hot dirty pages */
  Page **apHash;                 /* Page table */
  sxu32 nSize;                   /* apHash[] size: Must be a power of two  */
  sxu32 nPage;                   /* Total number of page loaded in memory */
  sxu32 nCacheMax;               /* Maximum page to cache*/
};
/* Control flags */
#define PAGER_CTRL_COMMIT_ERR   0x001 /* Commit error */
#define PAGER_CTRL_DIRTY_COMMIT 0x002 /* Dirty commit has been applied */ 
/*
** Read a 32-bit integer from the given file descriptor. 
** All values are stored on disk as big-endian.
*/
static int ReadInt32(vedis_file *pFd,sxu32 *pOut,sxi64 iOfft)
{
	unsigned char zBuf[4];
	int rc;
	rc = vedisOsRead(pFd,zBuf,sizeof(zBuf),iOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	SyBigEndianUnpack32(zBuf,pOut);
	return VEDIS_OK;
}
/*
** Read a 64-bit integer from the given file descriptor. 
** All values are stored on disk as big-endian.
*/
static int ReadInt64(vedis_file *pFd,sxu64 *pOut,sxi64 iOfft)
{
	unsigned char zBuf[8];
	int rc;
	rc = vedisOsRead(pFd,zBuf,sizeof(zBuf),iOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	SyBigEndianUnpack64(zBuf,pOut);
	return VEDIS_OK;
}
/*
** Write a 32-bit integer into the given file descriptor.
*/
static int WriteInt32(vedis_file *pFd,sxu32 iNum,sxi64 iOfft)
{
	unsigned char zBuf[4];
	int rc;
	SyBigEndianPack32(zBuf,iNum);
	rc = vedisOsWrite(pFd,zBuf,sizeof(zBuf),iOfft);
	return rc;
}
/*
** Write a 64-bit integer into the given file descriptor.
*/
static int WriteInt64(vedis_file *pFd,sxu64 iNum,sxi64 iOfft)
{
	unsigned char zBuf[8];
	int rc;
	SyBigEndianPack64(zBuf,iNum);
	rc = vedisOsWrite(pFd,zBuf,sizeof(zBuf),iOfft);
	return rc;
}
/*
** The maximum allowed sector size. 64KiB. If the xSectorsize() method 
** returns a value larger than this, then MAX_SECTOR_SIZE is used instead.
** This could conceivably cause corruption following a power failure on
** such a system. This is currently an undocumented limit.
*/
#define MAX_SECTOR_SIZE 0x10000
/*
** Get the size of a single sector on disk.
** The sector size will be used used  to determine the size
** and alignment of journal header and within created journal files.
**
** The default sector size is set to 512.
*/
static int GetSectorSize(vedis_file *pFd)
{
	int iSectorSize = VEDIS_DEFAULT_SECTOR_SIZE;
	if( pFd ){
		iSectorSize = vedisOsSectorSize(pFd);
		if( iSectorSize < 32 ){
			iSectorSize = 512;
		}
		if( iSectorSize > MAX_SECTOR_SIZE ){
			iSectorSize = MAX_SECTOR_SIZE;
		}
	}
	return iSectorSize;
}
/* Hash function for page number  */
#define PAGE_HASH(PNUM) (PNUM)
/*
 * Fetch a page from the cache.
 */
static Page * pager_fetch_page(Pager *pPager,pgno page_num)
{
	Page *pEntry;
	if( pPager->nPage < 1 ){
		/* Don't bother hashing */
		return 0;
	}
	/* Perform the lookup */
	pEntry = pPager->apHash[PAGE_HASH(page_num) & (pPager->nSize - 1)];
	for(;;){
		if( pEntry == 0 ){
			break;
		}
		if( pEntry->pgno == page_num ){
			return pEntry;
		}
		/* Point to the next entry in the colission chain */
		pEntry = pEntry->pNextCollide;
	}
	/* No such page */
	return 0;
}
/*
 * Allocate and initialize a new page.
 */
static Page * pager_alloc_page(Pager *pPager,pgno num_page)
{
	Page *pNew;
	
	pNew = (Page *)SyMemBackendPoolAlloc(pPager->pAllocator,sizeof(Page)+pPager->iPageSize);
	if( pNew == 0 ){
		return 0;
	}
	/* Zero the structure */
	SyZero(pNew,sizeof(Page)+pPager->iPageSize);
	/* Page data */
	pNew->zData = (unsigned char *)&pNew[1];
	/* Fill in the structure */
	pNew->pPager = pPager;
	pNew->nRef = 1;
	pNew->pgno = num_page;
	return pNew;
}
/*
 * Increment the reference count of a given page.
 */
static void page_ref(Page *pPage)
{
	pPage->nRef++;
}
/*
 * Release an in-memory page after its reference count reach zero.
 */
static int pager_release_page(Pager *pPager,Page *pPage)
{
	int rc = VEDIS_OK;
	if( !(pPage->flags & PAGE_DIRTY)){
		/* Invoke the unpin callback if available */
		if( pPager->xPageUnpin && pPage->pUserData ){
			pPager->xPageUnpin(pPage->pUserData);
		}
		pPage->pUserData = 0;
		SyMemBackendPoolFree(pPager->pAllocator,pPage);
	}else{
		/* Dirty page, it will be released later when a dirty commit
		 * or the final commit have been applied.
		 */
		rc = VEDIS_LOCKED;
	}
	return rc;
}
/* Forward declaration */
static int pager_unlink_page(Pager *pPager,Page *pPage);
/*
 * Decrement the reference count of a given page.
 */
static void page_unref(Page *pPage)
{
	pPage->nRef--;
	if( pPage->nRef < 1	){
		Pager *pPager = pPage->pPager;
		if( !(pPage->flags & PAGE_DIRTY)  ){
			pager_unlink_page(pPager,pPage);
			/* Release the page */
			pager_release_page(pPager,pPage);
		}else{
			if( pPage->flags & PAGE_DONT_MAKE_HOT ){
				/* Do not add this page to the hot dirty list */
				return;
			}
			if( !(pPage->flags & PAGE_HOT_DIRTY) ){
				/* Add to the hot dirty list */
				pPage->pPrevHot = 0;
				if( pPager->pFirstHot == 0 ){
					pPager->pFirstHot = pPager->pHotDirty = pPage;
				}else{
					pPage->pNextHot = pPager->pHotDirty;
					if( pPager->pHotDirty ){
						pPager->pHotDirty->pPrevHot = pPage;
					}
					pPager->pHotDirty = pPage;
				}
				pPager->nHot++;
				pPage->flags |= PAGE_HOT_DIRTY;
			}
		}
	}
}
/*
 * Link a freshly created page to the list of active page.
 */
static int pager_link_page(Pager *pPager,Page *pPage)
{
	sxu32 nBucket;
	/* Install in the corresponding bucket */
	nBucket = PAGE_HASH(pPage->pgno) & (pPager->nSize - 1);
	pPage->pNextCollide = pPager->apHash[nBucket];
	if( pPager->apHash[nBucket] ){
		pPager->apHash[nBucket]->pPrevCollide = pPage;
	}
	pPager->apHash[nBucket] = pPage;
	/* Link to the list of active pages */
	MACRO_LD_PUSH(pPager->pAll,pPage);
	pPager->nPage++;
	if( (pPager->nPage >= pPager->nSize * 4)  && pPager->nPage < 100000 ){
		/* Grow the hashtable */
		sxu32 nNewSize = pPager->nSize << 1;
		Page *pEntry,**apNew;
		sxu32 n;
		apNew = (Page **)SyMemBackendAlloc(pPager->pAllocator, nNewSize * sizeof(Page *));
		if( apNew ){
			sxu32 iBucket;
			/* Zero the new table */
			SyZero((void *)apNew, nNewSize * sizeof(Page *));
			/* Rehash all entries */
			n = 0;
			pEntry = pPager->pAll;
			for(;;){
				/* Loop one */
				if( n >= pPager->nPage ){
					break;
				}
				pEntry->pNextCollide = pEntry->pPrevCollide = 0;
				/* Install in the new bucket */
				iBucket = PAGE_HASH(pEntry->pgno) & (nNewSize - 1);
				pEntry->pNextCollide = apNew[iBucket];
				if( apNew[iBucket] ){
					apNew[iBucket]->pPrevCollide = pEntry;
				}
				apNew[iBucket] = pEntry;
				/* Point to the next entry */
				pEntry = pEntry->pNext;
				n++;
			}
			/* Release the old table and reflect the change */
			SyMemBackendFree(pPager->pAllocator,(void *)pPager->apHash);
			pPager->apHash = apNew;
			pPager->nSize  = nNewSize;
		}
	}
	return VEDIS_OK;
}
/*
 * Unlink a page from the list of active pages.
 */
static int pager_unlink_page(Pager *pPager,Page *pPage)
{
	if( pPage->pNextCollide ){
		pPage->pNextCollide->pPrevCollide = pPage->pPrevCollide;
	}
	if( pPage->pPrevCollide ){
		pPage->pPrevCollide->pNextCollide = pPage->pNextCollide;
	}else{
		sxu32 nBucket = PAGE_HASH(pPage->pgno) & (pPager->nSize - 1);
		pPager->apHash[nBucket] = pPage->pNextCollide;
	}
	MACRO_LD_REMOVE(pPager->pAll,pPage);
	pPager->nPage--;
	return VEDIS_OK;
}
/*
 * Update the content of a cached page.
 */
static int pager_fill_page(Pager *pPager,pgno iNum,void *pContents)
{
	Page *pPage;
	/* Fetch the page from the catch */
	pPage = pager_fetch_page(pPager,iNum);
	if( pPage == 0 ){
		return SXERR_NOTFOUND;
	}
	/* Reflect the change */
	SyMemcpy(pContents,pPage->zData,pPager->iPageSize);

	return VEDIS_OK;
}
/*
 * Read the content of a page from disk.
 */
static int pager_get_page_contents(Pager *pPager,Page *pPage,int noContent)
{
	int rc = VEDIS_OK;
	if( pPager->is_mem || noContent || pPage->pgno >= pPager->dbSize ){
		/* Do not bother reading, zero the page contents only */
		SyZero(pPage->zData,pPager->iPageSize);
		return VEDIS_OK;
	}
	if( (pPager->iOpenFlags & VEDIS_OPEN_MMAP) && (pPager->pMmap /* Paranoid edition */) ){
		unsigned char *zMap = (unsigned char *)pPager->pMmap;
		pPage->zData = &zMap[pPage->pgno * pPager->iPageSize];
	}else{
		/* Read content */
		rc = vedisOsRead(pPager->pfd,pPage->zData,pPager->iPageSize,pPage->pgno * pPager->iPageSize);
	}
	return rc;
}
/*
 * Add a page to the dirty list.
 */
static void pager_page_to_dirty_list(Pager *pPager,Page *pPage)
{
	if( pPage->flags & PAGE_DIRTY ){
		/* Already set */
		return;
	}
	/* Mark the page as dirty */
	pPage->flags |= PAGE_DIRTY|PAGE_NEED_SYNC|PAGE_IN_JOURNAL;
	/* Link to the list */
	pPage->pDirtyPrev = 0;
	pPage->pDirtyNext = pPager->pDirty;
	if( pPager->pDirty ){
		pPager->pDirty->pDirtyPrev = pPage;
	}
	pPager->pDirty = pPage;
	if( pPager->pFirstDirty == 0 ){
		pPager->pFirstDirty = pPage;
	}
}
/*
 * Merge sort.
 * The merge sort implementation is based on the one used by
 * the PH7 Embeddable PHP Engine (http://ph7.symisc.net/).
 */
/*
** Inputs:
**   a:       A sorted, null-terminated linked list.  (May be null).
**   b:       A sorted, null-terminated linked list.  (May be null).
**   cmp:     A pointer to the comparison function.
**
** Return Value:
**   A pointer to the head of a sorted list containing the elements
**   of both a and b.
**
** Side effects:
**   The "next", "prev" pointers for elements in the lists a and b are
**   changed.
*/
static Page * page_merge_dirty(Page *pA, Page *pB)
{
	Page result, *pTail;
    /* Prevent compiler warning */
	result.pDirtyNext = result.pDirtyPrev = 0;
	pTail = &result;
	while( pA && pB ){
		if( pA->pgno < pB->pgno ){
			pTail->pDirtyPrev = pA;
			pA->pDirtyNext = pTail;
			pTail = pA;
			pA = pA->pDirtyPrev;
		}else{
			pTail->pDirtyPrev = pB;
			pB->pDirtyNext = pTail;
			pTail = pB;
			pB = pB->pDirtyPrev;
		}
	}
	if( pA ){
		pTail->pDirtyPrev = pA;
		pA->pDirtyNext = pTail;
	}else if( pB ){
		pTail->pDirtyPrev = pB;
		pB->pDirtyNext = pTail;
	}else{
		pTail->pDirtyPrev = pTail->pDirtyNext = 0;
	}
	return result.pDirtyPrev;
}
/*
** Inputs:
**   Map:       Input hashmap
**   cmp:       A comparison function.
**
** Return Value:
**   Sorted hashmap.
**
** Side effects:
**   The "next" pointers for elements in list are changed.
*/
#define N_SORT_BUCKET  32
static Page * pager_get_dirty_pages(Pager *pPager)
{
	Page *a[N_SORT_BUCKET], *p, *pIn;
	sxu32 i;
	if( pPager->pFirstDirty == 0 ){
		/* Don't bother sorting, the list is already empty */
		return 0;
	}
	SyZero(a, sizeof(a));
	/* Point to the first inserted entry */
	pIn = pPager->pFirstDirty;
	while( pIn ){
		p = pIn;
		pIn = p->pDirtyPrev;
		p->pDirtyPrev = 0;
		for(i=0; i<N_SORT_BUCKET-1; i++){
			if( a[i]==0 ){
				a[i] = p;
				break;
			}else{
				p = page_merge_dirty(a[i], p);
				a[i] = 0;
			}
		}
		if( i==N_SORT_BUCKET-1 ){
			/* To get here, there need to be 2^(N_SORT_BUCKET) elements in he input list.
			 * But that is impossible.
			 */
			a[i] = page_merge_dirty(a[i], p);
		}
	}
	p = a[0];
	for(i=1; i<N_SORT_BUCKET; i++){
		p = page_merge_dirty(p,a[i]);
	}
	p->pDirtyNext = 0;
	return p;
}
/*
 * See block comment above.
 */
static Page * page_merge_hot(Page *pA, Page *pB)
{
	Page result, *pTail;
    /* Prevent compiler warning */
	result.pNextHot = result.pPrevHot = 0;
	pTail = &result;
	while( pA && pB ){
		if( pA->pgno < pB->pgno ){
			pTail->pPrevHot = pA;
			pA->pNextHot = pTail;
			pTail = pA;
			pA = pA->pPrevHot;
		}else{
			pTail->pPrevHot = pB;
			pB->pNextHot = pTail;
			pTail = pB;
			pB = pB->pPrevHot;
		}
	}
	if( pA ){
		pTail->pPrevHot = pA;
		pA->pNextHot = pTail;
	}else if( pB ){
		pTail->pPrevHot = pB;
		pB->pNextHot = pTail;
	}else{
		pTail->pPrevHot = pTail->pNextHot = 0;
	}
	return result.pPrevHot;
}
/*
** Inputs:
**   Map:       Input hashmap
**   cmp:       A comparison function.
**
** Return Value:
**   Sorted hashmap.
**
** Side effects:
**   The "next" pointers for elements in list are changed.
*/
#define N_SORT_BUCKET  32
static Page * pager_get_hot_pages(Pager *pPager)
{
	Page *a[N_SORT_BUCKET], *p, *pIn;
	sxu32 i;
	if( pPager->pFirstHot == 0 ){
		/* Don't bother sorting, the list is already empty */
		return 0;
	}
	SyZero(a, sizeof(a));
	/* Point to the first inserted entry */
	pIn = pPager->pFirstHot;
	while( pIn ){
		p = pIn;
		pIn = p->pPrevHot;
		p->pPrevHot = 0;
		for(i=0; i<N_SORT_BUCKET-1; i++){
			if( a[i]==0 ){
				a[i] = p;
				break;
			}else{
				p = page_merge_hot(a[i], p);
				a[i] = 0;
			}
		}
		if( i==N_SORT_BUCKET-1 ){
			/* To get here, there need to be 2^(N_SORT_BUCKET) elements in he input list.
			 * But that is impossible.
			 */
			a[i] = page_merge_hot(a[i], p);
		}
	}
	p = a[0];
	for(i=1; i<N_SORT_BUCKET; i++){
		p = page_merge_hot(p,a[i]);
	}
	p->pNextHot = 0;
	return p;
}
/*
** The format for the journal header is as follows:
** - 8 bytes: Magic identifying journal format.
** - 4 bytes: Number of records in journal.
** - 4 bytes: Random number used for page hash.
** - 8 bytes: Initial database page count.
** - 4 bytes: Sector size used by the process that wrote this journal.
** - 4 bytes: Database page size.
** 
** Followed by (JOURNAL_HDR_SZ - 28) bytes of unused space.
*/
/*
** Open the journal file and extract its header information.
**
** If the header is read successfully, *pNRec is set to the number of
** page records following this header and *pDbSize is set to the size of the
** database before the transaction began, in pages. Also, pPager->cksumInit
** is set to the value read from the journal header. VEDIS_OK is returned
** in this case.
**
** If the journal header file appears to be corrupted, VEDIS_DONE is
** returned and *pNRec and *PDbSize are undefined.  If JOURNAL_HDR_SZ bytes
** cannot be read from the journal file an error code is returned.
*/
static int pager_read_journal_header(
  Pager *pPager,               /* Pager object */
  sxu32 *pNRec,                /* OUT: Value read from the nRec field */
  pgno  *pDbSize               /* OUT: Value of original database size field */
)
{
	sxu32 iPageSize,iSectorSize;
	unsigned char zMagic[8];
	sxi64 iHdrOfft;
	sxi64 iSize;
	int rc;
	/* Offset to start reading from */
	iHdrOfft = 0;
	/* Get the size of the journal */
	rc = vedisOsFileSize(pPager->pjfd,&iSize);
	if( rc != VEDIS_OK ){
		return VEDIS_DONE;
	}
	/* If the journal file is too small, return VEDIS_DONE. */
	if( 32 /* Minimum sector size */> iSize ){
		return VEDIS_DONE;
	}
	/* Make sure we are dealing with a valid journal */
	rc = vedisOsRead(pPager->pjfd,zMagic,sizeof(zMagic),iHdrOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( SyMemcmp(zMagic,aJournalMagic,sizeof(zMagic)) != 0 ){
		return VEDIS_DONE;
	}
	iHdrOfft += sizeof(zMagic);
	 /* Read the first three 32-bit fields of the journal header: The nRec
      ** field, the checksum-initializer and the database size at the start
      ** of the transaction. Return an error code if anything goes wrong.
      */
	rc = ReadInt32(pPager->pjfd,pNRec,iHdrOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	iHdrOfft += 4;
	rc = ReadInt32(pPager->pjfd,&pPager->cksumInit,iHdrOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	iHdrOfft += 4;
	rc = ReadInt64(pPager->pjfd,pDbSize,iHdrOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	iHdrOfft += 8;
	/* Read the page-size and sector-size journal header fields. */
	rc = ReadInt32(pPager->pjfd,&iSectorSize,iHdrOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	iHdrOfft += 4;
	rc = ReadInt32(pPager->pjfd,&iPageSize,iHdrOfft);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Check that the values read from the page-size and sector-size fields
    ** are within range. To be 'in range', both values need to be a power
    ** of two greater than or equal to 512 or 32, and not greater than their 
    ** respective compile time maximum limits.
    */
    if( iPageSize < VEDIS_MIN_PAGE_SIZE || iSectorSize<32
     || iPageSize > VEDIS_MAX_PAGE_SIZE || iSectorSize>MAX_SECTOR_SIZE
     || ((iPageSize-1)&iPageSize)!=0    || ((iSectorSize-1)&iSectorSize)!=0 
    ){
      /* If the either the page-size or sector-size in the journal-header is 
      ** invalid, then the process that wrote the journal-header must have 
      ** crashed before the header was synced. In this case stop reading 
      ** the journal file here.
      */
      return VEDIS_DONE;
    }
    /* Update the assumed sector-size to match the value used by 
    ** the process that created this journal. If this journal was
    ** created by a process other than this one, then this routine
    ** is being called from within pager_playback(). The local value
    ** of Pager.sectorSize is restored at the end of that routine.
    */
    pPager->iSectorSize = iSectorSize;
	pPager->iPageSize = iPageSize;
	/* Ready to rollback */
	pPager->iJournalOfft = JOURNAL_HDR_SZ(pPager);
	/* All done */
	return VEDIS_OK;
}
/*
 * Write the journal header in the given memory buffer.
 * The given buffer is big enough to hold the whole header.
 */
static int pager_write_journal_header(Pager *pPager,unsigned char *zBuf)
{
	unsigned char *zPtr = zBuf;
	/* 8 bytes magic number */
	SyMemcpy(aJournalMagic,zPtr,sizeof(aJournalMagic));
	zPtr += sizeof(aJournalMagic);
	/* 4 bytes: Number of records in journal. */
	SyBigEndianPack32(zPtr,0);
	zPtr += 4;
	/* 4 bytes: Random number used to compute page checksum. */
	SyBigEndianPack32(zPtr,pPager->cksumInit);
	zPtr += 4;
	/* 8 bytes: Initial database page count. */
	SyBigEndianPack64(zPtr,pPager->dbOrigSize);
	zPtr += 8;
	/* 4 bytes: Sector size used by the process that wrote this journal. */
	SyBigEndianPack32(zPtr,(sxu32)pPager->iSectorSize);
	zPtr += 4;
	/* 4 bytes: Database page size. */
	SyBigEndianPack32(zPtr,(sxu32)pPager->iPageSize);
	return VEDIS_OK;
}
/*
** Parameter aData must point to a buffer of pPager->pageSize bytes
** of data. Compute and return a checksum based ont the contents of the 
** page of data and the current value of pPager->cksumInit.
**
** This is not a real checksum. It is really just the sum of the 
** random initial value (pPager->cksumInit) and every 200th byte
** of the page data, starting with byte offset (pPager->pageSize%200).
** Each byte is interpreted as an 8-bit unsigned integer.
**
** Changing the formula used to compute this checksum results in an
** incompatible journal file format.
**
** If journal corruption occurs due to a power failure, the most likely 
** scenario is that one end or the other of the record will be changed. 
** It is much less likely that the two ends of the journal record will be
** correct and the middle be corrupt.  Thus, this "checksum" scheme,
** though fast and simple, catches the mostly likely kind of corruption.
*/
static sxu32 pager_cksum(Pager *pPager,const unsigned char *zData)
{
  sxu32 cksum = pPager->cksumInit;         /* Checksum value to return */
  int i = pPager->iPageSize-200;          /* Loop counter */
  while( i>0 ){
    cksum += zData[i];
    i -= 200;
  }
  return cksum;
}
/*
** Read a single page from the journal file opened on file descriptor
** jfd. Playback this one page. Update the offset to read from.
*/
static int pager_play_back_one_page(Pager *pPager,sxi64 *pOfft,unsigned char *zTmp)
{
	unsigned char *zData = zTmp;
	sxi64 iOfft; /* Offset to read from */
	pgno iNum;   /* Pager number */
	sxu32 ckSum; /* Sanity check */
	int rc;
	/* Offset to start reading from */
	iOfft = *pOfft;
	/* Database page number */
	rc = ReadInt64(pPager->pjfd,&iNum,iOfft);
	if( rc != VEDIS_OK ){ return rc; }
	iOfft += 8;
	/* Page data */
	rc = vedisOsRead(pPager->pjfd,zData,pPager->iPageSize,iOfft);
	if( rc != VEDIS_OK ){ return rc; }
	iOfft += pPager->iPageSize;
	/* Page cksum */
	rc = ReadInt32(pPager->pjfd,&ckSum,iOfft);
	if( rc != VEDIS_OK ){ return rc; }
	iOfft += 4;
	/* Synchronize pointers */
	*pOfft = iOfft;
	/* Make sure we are dealing with a valid page */
	if( ckSum != pager_cksum(pPager,zData) ){
		/* Ignore that page */
		return SXERR_IGNORE;
	}
	if( iNum >= pPager->dbSize ){
		/* Ignore that page */
		return VEDIS_OK;
	}
	/* playback */
	rc = vedisOsWrite(pPager->pfd,zData,pPager->iPageSize,iNum * pPager->iPageSize);
	if( rc == VEDIS_OK ){
		/* Flush the cache */
		pager_fill_page(pPager,iNum,zData);
	}
	return rc;
}
/*
** Playback the journal and thus restore the database file to
** the state it was in before we started making changes.  
**
** The journal file format is as follows: 
**
**  (1)  8 byte prefix.  A copy of aJournalMagic[].
**  (2)  4 byte big-endian integer which is the number of valid page records
**       in the journal. 
**  (3)  4 byte big-endian integer which is the initial value for the 
**       sanity checksum.
**  (4)  8 byte integer which is the number of pages to truncate the
**       database to during a rollback.
**  (5)  4 byte big-endian integer which is the sector size.  The header
**       is this many bytes in size.
**  (6)  4 byte big-endian integer which is the page size.
**  (7)  zero padding out to the next sector size.
**  (8)  Zero or more pages instances, each as follows:
**        +  4 byte page number.
**        +  pPager->pageSize bytes of data.
**        +  4 byte checksum
**
** When we speak of the journal header, we mean the first 7 items above.
** Each entry in the journal is an instance of the 8th item.
**
** Call the value from the second bullet "nRec".  nRec is the number of
** valid page entries in the journal.  In most cases, you can compute the
** value of nRec from the size of the journal file.  But if a power
** failure occurred while the journal was being written, it could be the
** case that the size of the journal file had already been increased but
** the extra entries had not yet made it safely to disk.  In such a case,
** the value of nRec computed from the file size would be too large.  For
** that reason, we always use the nRec value in the header.
**
** If the file opened as the journal file is not a well-formed
** journal file then all pages up to the first corrupted page are rolled
** back (or no pages if the journal header is corrupted). The journal file
** is then deleted and SQLITE_OK returned, just as if no corruption had
** been encountered.
**
** If an I/O or malloc() error occurs, the journal-file is not deleted
** and an error code is returned.
**
*/
static int pager_playback(Pager *pPager)
{
	unsigned char *zTmp = 0; /* cc warning */
	sxu32 n,nRec;
	sxi64 iOfft;
	int rc;
	/* Read the journal header*/
	rc = pager_read_journal_header(pPager,&nRec,&pPager->dbSize);
	if( rc != VEDIS_OK ){
		if( rc == VEDIS_DONE ){
			goto end_playback;
		}
		vedisGenErrorFormat(pPager->pDb,"IO error while reading journal file '%s' header",pPager->zJournal);
		return rc;
	}
	/* Truncate the database back to its original size */
	rc = vedisOsTruncate(pPager->pfd,pPager->iPageSize * pPager->dbSize);
	if( rc != VEDIS_OK ){
		vedisGenError(pPager->pDb,"IO error while truncating database file");
		return rc;
	}
	/* Allocate a temporary page */
	zTmp = (unsigned char *)SyMemBackendAlloc(pPager->pAllocator,(sxu32)pPager->iPageSize);
	if( zTmp == 0 ){
		vedisGenOutofMem(pPager->pDb);
		return VEDIS_NOMEM;
	}
	SyZero((void *)zTmp,(sxu32)pPager->iPageSize);
	/* Copy original pages out of the journal and back into the 
    ** database file and/or page cache.
    */
	iOfft = pPager->iJournalOfft;
	for( n = 0 ; n < nRec ; ++n ){
		rc = pager_play_back_one_page(pPager,&iOfft,zTmp);
		if( rc != VEDIS_OK ){
			if( rc != SXERR_IGNORE ){
				vedisGenError(pPager->pDb,"Page playback error");
				goto end_playback;
			}
		}
	}
end_playback:
	/* Release the temp page */
	SyMemBackendFree(pPager->pAllocator,(void *)zTmp);
	if( rc == VEDIS_OK ){
		/* Sync the database file */
		vedisOsSync(pPager->pfd,VEDIS_SYNC_FULL);
	}
	if( rc == VEDIS_DONE ){
		rc = VEDIS_OK;
	}
	/* Return to the caller */
	return rc;
}
/*
** Unlock the database file to level eLock, which must be either NO_LOCK
** or SHARED_LOCK. Regardless of whether or not the call to xUnlock()
** succeeds, set the Pager.iLock variable to match the (attempted) new lock.
**
** Except, if Pager.iLock is set to NO_LOCK when this function is
** called, do not modify it. See the comment above the #define of 
** NO_LOCK for an explanation of this.
*/
static int pager_unlock_db(Pager *pPager, int eLock)
{
  int rc = VEDIS_OK;
  if( pPager->iLock != NO_LOCK ){
    rc = vedisOsUnlock(pPager->pfd,eLock);
    pPager->iLock = eLock;
  }
  return rc;
}
/*
** Lock the database file to level eLock, which must be either SHARED_LOCK,
** RESERVED_LOCK or EXCLUSIVE_LOCK. If the caller is successful, set the
** Pager.eLock variable to the new locking state. 
**
** Except, if Pager.eLock is set to NO_LOCK when this function is 
** called, do not modify it unless the new locking state is EXCLUSIVE_LOCK. 
** See the comment above the #define of NO_LOCK for an explanation 
** of this.
*/
static int pager_lock_db(Pager *pPager, int eLock){
  int rc = VEDIS_OK;
  if( pPager->iLock < eLock || pPager->iLock == NO_LOCK ){
    rc = vedisOsLock(pPager->pfd, eLock);
    if( rc==VEDIS_OK ){
      pPager->iLock = eLock;
    }else{
		vedisGenError(pPager->pDb,
			rc == VEDIS_BUSY ? "Another process or thread hold the requested lock" : "Error while requesting database lock"
			);
	}
  }
  return rc;
}
/*
** Try to obtain a lock of type locktype on the database file. If
** a similar or greater lock is already held, this function is a no-op
** (returning VEDIS_OK immediately).
**
** Otherwise, attempt to obtain the lock using vedisOsLock(). Invoke 
** the busy callback if the lock is currently not available. Repeat 
** until the busy callback returns false or until the attempt to 
** obtain the lock succeeds.
**
** Return VEDIS_OK on success and an error code if we cannot obtain
** the lock. If the lock is obtained successfully, set the Pager.state 
** variable to locktype before returning.
*/
static int pager_wait_on_lock(Pager *pPager, int locktype){
  int rc;                              /* Return code */
  do {
    rc = pager_lock_db(pPager,locktype);
  }while( rc==VEDIS_BUSY && pPager->xBusyHandler && pPager->xBusyHandler(pPager->pBusyHandlerArg) );
  return rc;
}
/*
** This function is called after transitioning from PAGER_OPEN to
** PAGER_SHARED state. It tests if there is a hot journal present in
** the file-system for the given pager. A hot journal is one that 
** needs to be played back. According to this function, a hot-journal
** file exists if the following criteria are met:
**
**   * The journal file exists in the file system, and
**   * No process holds a RESERVED or greater lock on the database file, and
**   * The database file itself is greater than 0 bytes in size, and
**   * The first byte of the journal file exists and is not 0x00.
**
** If the current size of the database file is 0 but a journal file
** exists, that is probably an old journal left over from a prior
** database with the same name. In this case the journal file is
** just deleted using OsDelete, *pExists is set to 0 and VEDIS_OK
** is returned.
**
** If a hot-journal file is found to exist, *pExists is set to 1 and 
** VEDIS_OK returned. If no hot-journal file is present, *pExists is
** set to 0 and VEDIS_OK returned. If an IO error occurs while trying
** to determine whether or not a hot-journal file exists, the IO error
** code is returned and the value of *pExists is undefined.
*/
static int pager_has_hot_journal(Pager *pPager, int *pExists)
{
  vedis_vfs *pVfs = pPager->pVfs;
  int rc = VEDIS_OK;           /* Return code */
  int exists = 1;               /* True if a journal file is present */

  *pExists = 0;
  rc = vedisOsAccess(pVfs, pPager->zJournal, VEDIS_ACCESS_EXISTS, &exists);
  if( rc==VEDIS_OK && exists ){
    int locked = 0;             /* True if some process holds a RESERVED lock */

    /* Race condition here:  Another process might have been holding the
    ** the RESERVED lock and have a journal open at the vedisOsAccess() 
    ** call above, but then delete the journal and drop the lock before
    ** we get to the following vedisOsCheckReservedLock() call.  If that
    ** is the case, this routine might think there is a hot journal when
    ** in fact there is none.  This results in a false-positive which will
    ** be dealt with by the playback routine.
    */
    rc = vedisOsCheckReservedLock(pPager->pfd, &locked);
    if( rc==VEDIS_OK && !locked ){
      sxi64 n = 0;                    /* Size of db file in bytes */
 
      /* Check the size of the database file. If it consists of 0 pages,
      ** then delete the journal file. See the header comment above for 
      ** the reasoning here.  Delete the obsolete journal file under
      ** a RESERVED lock to avoid race conditions.
      */
      rc = vedisOsFileSize(pPager->pfd,&n);
      if( rc==VEDIS_OK ){
        if( n < 1 ){
          if( pager_lock_db(pPager, RESERVED_LOCK)==VEDIS_OK ){
            vedisOsDelete(pVfs, pPager->zJournal, 0);
			pager_unlock_db(pPager, SHARED_LOCK);
          }
        }else{
          /* The journal file exists and no other connection has a reserved
          ** or greater lock on the database file. */
			*pExists = 1;
        }
      }
    }
  }
  return rc;
}
/*
 * Rollback a journal file. (See block-comment above).
 */
static int pager_journal_rollback(Pager *pPager,int check_hot)
{
	int rc;
	if( check_hot ){
		int iExists = 0; /* cc warning */
		/* Check if the journal file exists */
		rc = pager_has_hot_journal(pPager,&iExists);
		if( rc != VEDIS_OK  ){
			/* IO error */
			return rc;
		}
		if( !iExists ){
			/* Journal file does not exists */
			return VEDIS_OK;
		}
	}
	if( pPager->is_rdonly ){
		vedisGenErrorFormat(pPager->pDb,
			"Cannot rollback journal file '%s' due to a read-only database handle",pPager->zJournal);
		return VEDIS_READ_ONLY;
	}
	/* Get an EXCLUSIVE lock on the database file. At this point it is
      ** important that a RESERVED lock is not obtained on the way to the
      ** EXCLUSIVE lock. If it were, another process might open the
      ** database file, detect the RESERVED lock, and conclude that the
      ** database is safe to read while this process is still rolling the 
      ** hot-journal back.
      ** 
      ** Because the intermediate RESERVED lock is not requested, any
      ** other process attempting to access the database file will get to 
      ** this point in the code and fail to obtain its own EXCLUSIVE lock 
      ** on the database file.
      **
      ** Unless the pager is in locking_mode=exclusive mode, the lock is
      ** downgraded to SHARED_LOCK before this function returns.
      */
	/* Open the journal file */
	rc = vedisOsOpen(pPager->pVfs,pPager->pAllocator,pPager->zJournal,&pPager->pjfd,VEDIS_OPEN_READWRITE);
	if( rc != VEDIS_OK ){
		vedisGenErrorFormat(pPager->pDb,"IO error while opening journal file: '%s'",pPager->zJournal);
		goto fail;
	}
	rc = pager_lock_db(pPager,EXCLUSIVE_LOCK);
	if( rc != VEDIS_OK ){
		vedisGenError(pPager->pDb,"Cannot acquire an exclusive lock on the database while journal rollback");
		goto fail;
	}
	/* Sync the journal file */
	vedisOsSync(pPager->pjfd,VEDIS_SYNC_NORMAL);
	/* Finally rollback the database */
	rc = pager_playback(pPager);
	/* Switch back to shared lock */
	pager_unlock_db(pPager,SHARED_LOCK);
fail:
	/* Close the journal handle */
	vedisOsCloseFree(pPager->pAllocator,pPager->pjfd);
	pPager->pjfd = 0;
	if( rc == VEDIS_OK ){
		/* Delete the journal file */
		vedisOsDelete(pPager->pVfs,pPager->zJournal,TRUE);
	}
	return rc;
}
/*
 * Write the vedis header (First page). (Big-Endian)
 */
static int pager_write_db_header(Pager *pPager)
{
	unsigned char *zRaw = pPager->pHeader->zData;
	vedis_kv_engine *pEngine = pPager->pEngine;
	sxu32 nDos;
	sxu16 nLen;
	/* Database signature */
	SyMemcpy(VEDIS_DB_SIG,zRaw,sizeof(VEDIS_DB_SIG)-1);
	zRaw += sizeof(VEDIS_DB_SIG)-1;
	/* Database magic number */
	SyBigEndianPack32(zRaw,VEDIS_DB_MAGIC);
	zRaw += 4; /* 4 byte magic number */
	/* Database creation time */
	SyZero(&pPager->tmCreate,sizeof(Sytm));
	if( pPager->pVfs->xCurrentTime ){
		pPager->pVfs->xCurrentTime(pPager->pVfs,&pPager->tmCreate);
	}
	/* DOS time format (4 bytes) */
	SyTimeFormatToDos(&pPager->tmCreate,&nDos);
	SyBigEndianPack32(zRaw,nDos);
	zRaw += 4; /* 4 byte DOS time */
	/* Sector size */
	SyBigEndianPack32(zRaw,(sxu32)pPager->iSectorSize);
	zRaw += 4; /* 4 byte sector size */
	/* Page size */
	SyBigEndianPack32(zRaw,(sxu32)pPager->iPageSize);
	zRaw += 4; /* 4 byte page size */
	/* Key value storage engine */
	nLen = (sxu16)SyStrlen(pEngine->pIo->pMethods->zName);
	SyBigEndianPack16(zRaw,nLen); /* 2 byte storage engine name */
	zRaw += 2;
	SyMemcpy((const void *)pEngine->pIo->pMethods->zName,(void *)zRaw,nLen);
	zRaw += nLen;
	/* All rest are meta-data available to the host application */
	return VEDIS_OK;
}
/*
 * Read the vedis header (first page). (Big-Endian)
 */
static int pager_extract_header(Pager *pPager,const unsigned char *zRaw,sxu32 nByte)
{
	const unsigned char *zEnd = &zRaw[nByte];
	sxu32 nDos,iMagic;
	sxu16 nLen;
	char *zKv;
	/* Database signature */
	if( SyMemcmp(VEDIS_DB_SIG,zRaw,sizeof(VEDIS_DB_SIG)-1) != 0 ){
		/* Corrupt database */
		return VEDIS_CORRUPT;
	}
	zRaw += sizeof(VEDIS_DB_SIG)-1;
	/* Database magic number */
	SyBigEndianUnpack32(zRaw,&iMagic);
	zRaw += 4; /* 4 byte magic number */
	if( iMagic != VEDIS_DB_MAGIC ){
		/* Corrupt database */
		return VEDIS_CORRUPT;
	}
	/* Database creation time */
	SyBigEndianUnpack32(zRaw,&nDos);
	zRaw += 4; /* 4 byte DOS time format */
	SyDosTimeFormat(nDos,&pPager->tmCreate);
	/* Sector size */
	SyBigEndianUnpack32(zRaw,(sxu32 *)&pPager->iSectorSize);
	zRaw += 4; /* 4 byte sector size */
	/* Page size */
	SyBigEndianUnpack32(zRaw,(sxu32 *)&pPager->iPageSize);
	zRaw += 4; /* 4 byte page size */
	/* Check that the values read from the page-size and sector-size fields
    ** are within range. To be 'in range', both values need to be a power
    ** of two greater than or equal to 512 or 32, and not greater than their 
    ** respective compile time maximum limits.
    */
    if( pPager->iPageSize<VEDIS_MIN_PAGE_SIZE || pPager->iSectorSize<32
     || pPager->iPageSize>VEDIS_MAX_PAGE_SIZE || pPager->iSectorSize>MAX_SECTOR_SIZE
     || ((pPager->iPageSize<-1)&pPager->iPageSize)!=0    || ((pPager->iSectorSize-1)&pPager->iSectorSize)!=0 
    ){
      return VEDIS_CORRUPT;
	}
	/* Key value storage engine */
	SyBigEndianUnpack16(zRaw,&nLen); /* 2 byte storage engine length */
	zRaw += 2;
	if( nLen > (sxu16)(zEnd - zRaw) ){
		nLen = (sxu16)(zEnd - zRaw);
	}
	zKv = (char *)SyMemBackendDup(pPager->pAllocator,(const char *)zRaw,nLen);
	if( zKv == 0 ){
		return VEDIS_NOMEM;
	}
	SyStringInitFromBuf(&pPager->sKv,zKv,nLen);
	return VEDIS_OK;
}
/*
 * Read the database header.
 */
static int pager_read_db_header(Pager *pPager)
{
	unsigned char zRaw[VEDIS_MIN_PAGE_SIZE]; /* Minimum page size */
	sxi64 n = 0;              /* Size of db file in bytes */
	int rc;
	/* Get the file size first */
	rc = vedisOsFileSize(pPager->pfd,&n);
	if( rc != VEDIS_OK ){
		return rc;
	}
	pPager->dbByteSize = n;
	if( n > 0 ){
		vedis_kv_methods *pMethods;
		SyString *pKv;
		pgno nPage;
		if( n < VEDIS_MIN_PAGE_SIZE ){
			/* A valid vedis database must be at least 512 bytes long */
			vedisGenError(pPager->pDb,"Malformed database image");
			return VEDIS_CORRUPT;
		}
		/* Read the database header */
		rc = vedisOsRead(pPager->pfd,zRaw,sizeof(zRaw),0);
		if( rc != VEDIS_OK ){
			vedisGenError(pPager->pDb,"IO error while reading database header");
			return rc;
		}
		/* Extract the header */
		rc = pager_extract_header(pPager,zRaw,sizeof(zRaw));
		if( rc != VEDIS_OK ){
			vedisGenError(pPager->pDb,rc == VEDIS_NOMEM ? "Unqlite is running out of memory" : "Malformed database image");
			return rc;
		}
		/* Update pager state  */
		nPage = (pgno)(n / pPager->iPageSize);
		if( nPage==0 && n>0 ){
			nPage = 1;
		}
		pPager->dbSize = nPage;
		/* Laod the target Key/Value storage engine */
		pKv = &pPager->sKv;
		pMethods = vedisFindKVStore(pKv->zString,pKv->nByte);
		if( pMethods == 0 ){
			vedisGenErrorFormat(pPager->pDb,"No such Key/Value storage engine '%z'",pKv);
			return VEDIS_NOTIMPLEMENTED;
		}
		/* Install the new KV storage engine */
		rc = vedisPagerRegisterKvEngine(pPager,pMethods);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}else{
		/* Set a default page and sector size */
		pPager->iSectorSize = GetSectorSize(pPager->pfd);
		pPager->iPageSize = vedisGetPageSize();
		SyStringInitFromBuf(&pPager->sKv,pPager->pEngine->pIo->pMethods->zName,SyStrlen(pPager->pEngine->pIo->pMethods->zName));
		pPager->dbSize = 0;
	}
	/* Allocate a temporary page size */
	pPager->zTmpPage = (unsigned char *)SyMemBackendAlloc(pPager->pAllocator,(sxu32)pPager->iPageSize);
	if( pPager->zTmpPage == 0 ){
		vedisGenOutofMem(pPager->pDb);
		return VEDIS_NOMEM;
	}
	SyZero(pPager->zTmpPage,(sxu32)pPager->iPageSize);
	return VEDIS_OK;
}
/*
 * Write the database header.
 */
static int pager_create_header(Pager *pPager)
{
	Page *pHeader;
	int rc;
	/* Allocate a new page */
	pHeader = pager_alloc_page(pPager,0);
	if( pHeader == 0 ){
		return VEDIS_NOMEM;
	}
	pPager->pHeader = pHeader;
	/* Link the page */
	pager_link_page(pPager,pHeader);
	/* Add to the dirty list */
	pager_page_to_dirty_list(pPager,pHeader);
	/* Write the database header */
	rc = pager_write_db_header(pPager);
	return rc;
}
/*
** This function is called to obtain a shared lock on the database file.
** It is illegal to call vedisPagerAcquire() until after this function
** has been successfully called. If a shared-lock is already held when
** this function is called, it is a no-op.
**
** The following operations are also performed by this function.
**
**   1) If the pager is currently in PAGER_OPEN state (no lock held
**      on the database file), then an attempt is made to obtain a
**      SHARED lock on the database file. Immediately after obtaining
**      the SHARED lock, the file-system is checked for a hot-journal,
**      which is played back if present. 
**
** If everything is successful, VEDIS_OK is returned. If an IO error 
** occurs while locking the database, checking for a hot-journal file or 
** rolling back a journal file, the IO error code is returned.
*/
static int pager_shared_lock(Pager *pPager)
{
	int rc = VEDIS_OK;
	if( pPager->iState == PAGER_OPEN ){
		vedis_kv_methods *pMethods;
		/* Open the target database */
		rc = vedisOsOpen(pPager->pVfs,pPager->pAllocator,pPager->zFilename,&pPager->pfd,pPager->iOpenFlags);
		if( rc != VEDIS_OK ){
			vedisGenErrorFormat(pPager->pDb,
				"IO error while opening the target database file: %s",pPager->zFilename
				);
			return rc;
		}
		/* Try to obtain a shared lock */
		rc = pager_wait_on_lock(pPager,SHARED_LOCK);
		if( rc == VEDIS_OK ){
			if( pPager->iLock <= SHARED_LOCK ){
				/* Rollback any hot journal */
				rc = pager_journal_rollback(pPager,1);
				if( rc != VEDIS_OK ){
					return rc;
				}
			}
			/* Read the database header */
			rc = pager_read_db_header(pPager);
			if( rc != VEDIS_OK ){
				return rc;
			}
			if(pPager->dbSize > 0 ){
				if( pPager->iOpenFlags & VEDIS_OPEN_MMAP ){
					const vedis_vfs *pVfs = vedisExportBuiltinVfs();
					/* Obtain a read-only memory view of the whole file */
					if( pVfs && pVfs->xMmap ){
						int vr;
						vr = pVfs->xMmap(pPager->zFilename,&pPager->pMmap,&pPager->dbByteSize);
						if( vr != VEDIS_OK ){
							/* Generate a warning */
							vedisGenError(pPager->pDb,"Cannot obtain a read-only memory view of the target database");
							pPager->iOpenFlags &= ~VEDIS_OPEN_MMAP;
						}
					}else{
						/* Generate a warning */
						vedisGenError(pPager->pDb,"Cannot obtain a read-only memory view of the target database");
						pPager->iOpenFlags &= ~VEDIS_OPEN_MMAP;
					}
				}
			}
			/* Update the pager state */
			pPager->iState = PAGER_READER;
			/* Invoke the xOpen methods if available */
			pMethods = pPager->pEngine->pIo->pMethods;
			if( pMethods->xOpen ){
				rc = pMethods->xOpen(pPager->pEngine,pPager->dbSize);
				if( rc != VEDIS_OK ){
					vedisGenErrorFormat(pPager->pDb,
						"xOpen() method of the underlying KV engine '%z' failed",
						&pPager->sKv
						);
					pager_unlock_db(pPager,NO_LOCK);
					pPager->iState = PAGER_OPEN;
					return rc;
				}
			}
		}else if( rc == VEDIS_BUSY ){
			vedisGenError(pPager->pDb,"Another process or thread have a reserved or exclusive lock on this database");
		}		
	}
	return rc;
}
/*
** Begin a write-transaction on the specified pager object. If a 
** write-transaction has already been opened, this function is a no-op.
*/
VEDIS_PRIVATE int vedisPagerBegin(Pager *pPager)
{
	int rc;
	/* Obtain a shared lock on the database first */
	rc = pager_shared_lock(pPager);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( pPager->iState >= PAGER_WRITER_LOCKED ){
		return VEDIS_OK;
	}
	if( pPager->is_rdonly ){
		vedisGenError(pPager->pDb,"Read-only database");
		/* Read only database */
		return VEDIS_READ_ONLY;
	}
	/* Obtain a reserved lock on the database */
	rc = pager_wait_on_lock(pPager,RESERVED_LOCK);
	if( rc == VEDIS_OK ){
		/* Create the bitvec */
		pPager->pVec = vedisBitvecCreate(pPager->pAllocator,pPager->dbSize);
		if( pPager->pVec == 0 ){
			vedisGenOutofMem(pPager->pDb);
			rc = VEDIS_NOMEM;
			goto fail;
		}
		/* Change to the WRITER_LOCK state */
		pPager->iState = PAGER_WRITER_LOCKED;
		pPager->dbOrigSize = pPager->dbSize;
		pPager->iJournalOfft = 0;
		pPager->nRec = 0;
		if( pPager->dbSize < 1 ){
			/* Write the  database header */
			rc = pager_create_header(pPager);
			if( rc != VEDIS_OK ){
				goto fail;
			}
			pPager->dbSize = 1;
		}
	}else if( rc == VEDIS_BUSY ){
		vedisGenError(pPager->pDb,"Another process or thread have a reserved lock on this database");
	}
	return rc;
fail:
	/* Downgrade to shared lock */
	pager_unlock_db(pPager,SHARED_LOCK);
	return rc;
}
/*
** This function is called at the start of every write transaction.
** There must already be a RESERVED or EXCLUSIVE lock on the database 
** file when this routine is called.
**
*/
static int vedisOpenJournal(Pager *pPager)
{
	unsigned char *zHeader;
	int rc = VEDIS_OK;
	if( pPager->is_mem || pPager->no_jrnl ){
		/* Journaling is omitted for this database */
		goto finish;
	}
	if( pPager->iState >= PAGER_WRITER_CACHEMOD ){
		/* Already opened */
		return VEDIS_OK;
	}
	/* Delete any previously journal with the same name */
	vedisOsDelete(pPager->pVfs,pPager->zJournal,1);
	/* Open the journal file */
	rc = vedisOsOpen(pPager->pVfs,pPager->pAllocator,pPager->zJournal,
		&pPager->pjfd,VEDIS_OPEN_CREATE|VEDIS_OPEN_READWRITE);
	if( rc != VEDIS_OK ){
		vedisGenErrorFormat(pPager->pDb,"IO error while opening journal file: %s",pPager->zJournal);
		return rc;
	}
	/* Write the journal header */
	zHeader = (unsigned char *)SyMemBackendAlloc(pPager->pAllocator,(sxu32)pPager->iSectorSize);
	if( zHeader == 0 ){
		rc = VEDIS_NOMEM;
		goto fail;
	}
	pager_write_journal_header(pPager,zHeader);
	/* Perform the disk write */
	rc = vedisOsWrite(pPager->pjfd,zHeader,pPager->iSectorSize,0);
	/* Offset to start writing from */
	pPager->iJournalOfft = pPager->iSectorSize;
	/* All done, journal will be synced later */
	SyMemBackendFree(pPager->pAllocator,zHeader);
finish:
	if( rc == VEDIS_OK ){
		pPager->iState = PAGER_WRITER_CACHEMOD;
		return VEDIS_OK;
	}
fail:
	/* Unlink the journal file if something goes wrong */
	vedisOsCloseFree(pPager->pAllocator,pPager->pjfd);
	vedisOsDelete(pPager->pVfs,pPager->zJournal,0);
	pPager->pjfd = 0;
	return rc;
}
/*
** Sync the journal. In other words, make sure all the pages that have
** been written to the journal have actually reached the surface of the
** disk and can be restored in the event of a hot-journal rollback.
*
* This routine try also to obtain an exlusive lock on the database.
*/
static int vedisFinalizeJournal(Pager *pPager,int *pRetry,int close_jrnl)
{
	int rc;
	*pRetry = 0;
	/* Grab the exclusive lock first */
	rc = pager_lock_db(pPager,EXCLUSIVE_LOCK);
	if( rc != VEDIS_OK ){
		/* Retry the excusive lock process */
		*pRetry = 1;
		rc = VEDIS_OK;
	}
	if( pPager->no_jrnl ){
		/* Journaling is omitted, return immediately */
		return VEDIS_OK;
	}
	/* Write the total number of database records */
	rc = WriteInt32(pPager->pjfd,pPager->nRec,8 /* sizeof(aJournalRec) */);
	if( rc != VEDIS_OK ){
		if( pPager->nRec > 0 ){
			return rc;
		}else{
			/* Not so fatal */
			rc = VEDIS_OK;
		}
	}
	/* Sync the journal and close it */
	rc = vedisOsSync(pPager->pjfd,VEDIS_SYNC_NORMAL);
	if( close_jrnl ){
		/* close the journal file */
		if( VEDIS_OK != vedisOsCloseFree(pPager->pAllocator,pPager->pjfd) ){
			if( rc != VEDIS_OK /* vedisOsSync */ ){
				return rc;
			}
		}
		pPager->pjfd = 0;
	}
	if( (*pRetry) == 1 ){
		if( pager_lock_db(pPager,EXCLUSIVE_LOCK) == VEDIS_OK ){
			/* Got exclusive lock */
			*pRetry = 0;
		}
	}
	return VEDIS_OK;
}
/*
 * Mark a single data page as writeable. The page is written into the 
 * main journal as required.
 */
static int page_write(Pager *pPager,Page *pPage)
{
	int rc;
	if( !pPager->is_mem && !pPager->no_jrnl ){
		/* Write the page to the transaction journal */
		if( pPage->pgno < pPager->dbOrigSize && !vedisBitvecTest(pPager->pVec,pPage->pgno) ){
			sxu32 cksum;
			if( pPager->nRec == SXU32_HIGH ){
				/* Journal Limit reached */
				vedisGenError(pPager->pDb,"Journal record limit reached, commit your changes");
				return VEDIS_LIMIT;
			}
			/* Write the page number */
			rc = WriteInt64(pPager->pjfd,pPage->pgno,pPager->iJournalOfft);
			if( rc != VEDIS_OK ){ return rc; }
			/* Write the raw page */
			/** CODEC */
			rc = vedisOsWrite(pPager->pjfd,pPage->zData,pPager->iPageSize,pPager->iJournalOfft + 8);
			if( rc != VEDIS_OK ){ return rc; }
			/* Compute the checksum */
			cksum = pager_cksum(pPager,pPage->zData);
			rc = WriteInt32(pPager->pjfd,cksum,pPager->iJournalOfft + 8 + pPager->iPageSize);
			if( rc != VEDIS_OK ){ return rc; }
			/* Update the journal offset */
			pPager->iJournalOfft += 8 /* page num */ + pPager->iPageSize + 4 /* cksum */;
			pPager->nRec++;
			/* Mark as journalled  */
			vedisBitvecSet(pPager->pVec,pPage->pgno);
		}
	}
	/* Add the page to the dirty list */
	pager_page_to_dirty_list(pPager,pPage);
	/* Update the database size and return. */
	if( (1 + pPage->pgno) > pPager->dbSize ){
		pPager->dbSize = 1 + pPage->pgno;
		if( pPager->dbSize == SXU64_HIGH ){
			vedisGenError(pPager->pDb,"Database maximum page limit (64-bit) reached");
			return VEDIS_LIMIT;
		}
	}	
	return VEDIS_OK;
}
/*
** The argument is the first in a linked list of dirty pages connected
** by the PgHdr.pDirty pointer. This function writes each one of the
** in-memory pages in the list to the database file. The argument may
** be NULL, representing an empty list. In this case this function is
** a no-op.
**
** The pager must hold at least a RESERVED lock when this function
** is called. Before writing anything to the database file, this lock
** is upgraded to an EXCLUSIVE lock. If the lock cannot be obtained,
** VEDIS_BUSY is returned and no data is written to the database file.
*/
static int pager_write_dirty_pages(Pager *pPager,Page *pDirty)
{
	int rc = VEDIS_OK;
	Page *pNext;
	for(;;){
		if( pDirty == 0 ){
			break;
		}
		/* Point to the next dirty page */
		pNext = pDirty->pDirtyPrev; /* Not a bug: Reverse link */
		if( (pDirty->flags & PAGE_DONT_WRITE) == 0 ){
			rc = vedisOsWrite(pPager->pfd,pDirty->zData,pPager->iPageSize,pDirty->pgno * pPager->iPageSize);
			if( rc != VEDIS_OK ){
				/* A rollback should be done */
				break;
			}
		}
		/* Remove stale flags */
		pDirty->flags &= ~(PAGE_DIRTY|PAGE_DONT_WRITE|PAGE_NEED_SYNC|PAGE_IN_JOURNAL|PAGE_HOT_DIRTY);
		if( pDirty->nRef < 1 ){
			/* Unlink the page now it is unused */
			pager_unlink_page(pPager,pDirty);
			/* Release the page */
			pager_release_page(pPager,pDirty);
		}
		/* Point to the next page */
		pDirty = pNext;
	}
	pPager->pDirty = pPager->pFirstDirty = 0;
	pPager->pHotDirty = pPager->pFirstHot = 0;
	pPager->nHot = 0;
	return rc;
}
/*
** The argument is the first in a linked list of hot dirty pages connected
** by the PgHdr.pHotDirty pointer. This function writes each one of the
** in-memory pages in the list to the database file. The argument may
** be NULL, representing an empty list. In this case this function is
** a no-op.
**
** The pager must hold at least a RESERVED lock when this function
** is called. Before writing anything to the database file, this lock
** is upgraded to an EXCLUSIVE lock. If the lock cannot be obtained,
** VEDIS_BUSY is returned and no data is written to the database file.
*/
static int pager_write_hot_dirty_pages(Pager *pPager,Page *pDirty)
{
	int rc = VEDIS_OK;
	Page *pNext;
	for(;;){
		if( pDirty == 0 ){
			break;
		}
		/* Point to the next page */
		pNext = pDirty->pPrevHot; /* Not a bug: Reverse link */
		if( (pDirty->flags & PAGE_DONT_WRITE) == 0 ){
			rc = vedisOsWrite(pPager->pfd,pDirty->zData,pPager->iPageSize,pDirty->pgno * pPager->iPageSize);
			if( rc != VEDIS_OK ){
				break;
			}
		}
		/* Remove stale flags */
		pDirty->flags &= ~(PAGE_DIRTY|PAGE_DONT_WRITE|PAGE_NEED_SYNC|PAGE_IN_JOURNAL|PAGE_HOT_DIRTY);
		/* Unlink from the list of dirty pages */
		if( pDirty->pDirtyPrev ){
			pDirty->pDirtyPrev->pDirtyNext = pDirty->pDirtyNext;
		}else{
			pPager->pDirty = pDirty->pDirtyNext;
		}
		if( pDirty->pDirtyNext ){
			pDirty->pDirtyNext->pDirtyPrev = pDirty->pDirtyPrev;
		}else{
			pPager->pFirstDirty = pDirty->pDirtyPrev;
		}
		/* Discard */
		pager_unlink_page(pPager,pDirty);
		/* Release the page */
		pager_release_page(pPager,pDirty);
		/* Next hot page */
		pDirty = pNext;
	}
	return rc;
}
/*
 * Commit a transaction: Phase one.
 */
static int pager_commit_phase1(Pager *pPager)
{
	int get_excl = 0;
	Page *pDirty;
	int rc;
	/* If no database changes have been made, return early. */
	if( pPager->iState < PAGER_WRITER_CACHEMOD ){
		return VEDIS_OK;
	}
	if( pPager->is_rdonly ){
		/* Read-Only DB */
		vedisGenError(pPager->pDb,"Read-Only database");
		return VEDIS_READ_ONLY;
	}
	/* Invoke any user commit callback */
	if( pPager->xCommit ){
		rc = pPager->xCommit(pPager->pCommitData);
		if( rc == VEDIS_ABORT ){
			vedisGenError(pPager->pDb,"User ommit callback request an operation abort");
			return VEDIS_ABORT;
		}
		/* Fall through */
		rc = VEDIS_OK;
	}
	if( pPager->is_mem ){
		/* An in-memory database */
		return VEDIS_OK;
	}
	/* Finalize the journal file */
	rc = vedisFinalizeJournal(pPager,&get_excl,1);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Get the dirty pages */
	pDirty = pager_get_dirty_pages(pPager);
	if( get_excl ){
		/* Wait one last time for the exclusive lock */
		rc = pager_wait_on_lock(pPager,EXCLUSIVE_LOCK);
		if( rc != VEDIS_OK ){
			vedisGenError(pPager->pDb,"Cannot obtain an Exclusive lock on the target database");
			return rc;
		}
	}
	if( pPager->iFlags & PAGER_CTRL_DIRTY_COMMIT ){
		/* Synce the database first if a dirty commit have been applied */
		vedisOsSync(pPager->pfd,VEDIS_SYNC_NORMAL);
	}
	/* Write the dirty pages */
	rc = pager_write_dirty_pages(pPager,pDirty);
	if( rc != VEDIS_OK ){
		/* Rollback your DB */
		pPager->iFlags |= PAGER_CTRL_COMMIT_ERR;
		pPager->pFirstDirty = pDirty;
		vedisGenError(pPager->pDb,"IO error while writing dirty pages, rollback your database");
		return rc;
	}
	/* If the file on disk is not the same size as the database image,
     * then use vedisOsTruncate to grow or shrink the file here.
     */
	if( pPager->dbSize != pPager->dbOrigSize ){
		vedisOsTruncate(pPager->pfd,pPager->iPageSize * pPager->dbSize);
	}
	/* Sync the database file */
	vedisOsSync(pPager->pfd,VEDIS_SYNC_FULL);
	/* Remove stale flags */
	pPager->iJournalOfft = 0;
	pPager->nRec = 0;
	return VEDIS_OK;
}
/*
 * Commit a transaction: Phase two.
 */
static int pager_commit_phase2(Pager *pPager)
{
	if( !pPager->is_mem ){
		if( pPager->iState == PAGER_OPEN ){
			return VEDIS_OK;
		}
		if( pPager->iState != PAGER_READER ){
			if( !pPager->no_jrnl ){
				/* Finally, unlink the journal file */
				vedisOsDelete(pPager->pVfs,pPager->zJournal,1);
			}
			/* Downgrade to shraed lock */
			pager_unlock_db(pPager,SHARED_LOCK);
			pPager->iState = PAGER_READER;
			if( pPager->pVec ){
				vedisBitvecDestroy(pPager->pVec);
				pPager->pVec = 0;
			}
		}
	}
	return VEDIS_OK;
}
/*
 * Perform a dirty commit.
 */
static int pager_dirty_commit(Pager *pPager)
{
	int get_excl = 0;
	Page *pHot;
	int rc;
	/* Finalize the journal file without closing it */
	rc = vedisFinalizeJournal(pPager,&get_excl,0);
	if( rc != VEDIS_OK ){
		/* It's not a fatal error if something goes wrong here since
		 * its not the final commit.
		 */
		return VEDIS_OK;
	}
	/* Point to the list of hot pages */
	pHot = pager_get_hot_pages(pPager);
	if( pHot == 0 ){
		return VEDIS_OK;
	}
	if( get_excl ){
		/* Wait one last time for the exclusive lock */
		rc = pager_wait_on_lock(pPager,EXCLUSIVE_LOCK);
		if( rc != VEDIS_OK ){
			/* Not so fatal, will try another time */
			return VEDIS_OK;
		}
	}
	/* Tell that a dirty commit happen */
	pPager->iFlags |= PAGER_CTRL_DIRTY_COMMIT;
	/* Write the hot pages now */
	rc = pager_write_hot_dirty_pages(pPager,pHot);
	if( rc != VEDIS_OK ){
		pPager->iFlags |= PAGER_CTRL_COMMIT_ERR;
		vedisGenError(pPager->pDb,"IO error while writing hot dirty pages, rollback your database");
		return rc;
	}
	pPager->pFirstHot = pPager->pHotDirty = 0;
	pPager->nHot = 0;
	/* No need to sync the database file here, since the journal is already
	 * open here and this is not the final commit.
	 */
	return VEDIS_OK;
}
/*
** Commit a transaction and sync the database file for the pager pPager.
**
** This routine ensures that:
**
**   * the journal is synced,
**   * all dirty pages are written to the database file, 
**   * the database file is truncated (if required), and
**   * the database file synced.
**   * the journal file is deleted.
*/
VEDIS_PRIVATE int vedisPagerCommit(Pager *pPager)
{
	int rc;
	/* Commit: Phase One */
	rc = pager_commit_phase1(pPager);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	/* Commit: Phase Two */
	rc = pager_commit_phase2(pPager);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	/* Remove stale flags */
	pPager->iFlags &= ~PAGER_CTRL_COMMIT_ERR;
	/* All done */
	return VEDIS_OK;
fail:
	/* Disable the auto-commit flag */
	pPager->pDb->iFlags |= VEDIS_FL_DISABLE_AUTO_COMMIT;
	return rc;
}
/*
 * Reset the pager to its initial state. This is caused by
 * a rollback operation.
 */
static int pager_reset_state(Pager *pPager,int bResetKvEngine)
{
	vedis_kv_engine *pEngine = pPager->pEngine;
	Page *pNext,*pPtr = pPager->pAll;
	const vedis_kv_io *pIo;
	int rc;
	/* Remove stale flags */
	pPager->iFlags &= ~(PAGER_CTRL_COMMIT_ERR|PAGER_CTRL_DIRTY_COMMIT);
	pPager->iJournalOfft = 0;
	pPager->nRec = 0;
	/* Database original size */
	pPager->dbSize = pPager->dbOrigSize;
	/* Discard all in-memory pages */
	for(;;){
		if( pPtr == 0 ){
			break;
		}
		pNext = pPtr->pNext; /* Reverse link */
		/* Remove stale flags */
		pPtr->flags &= ~(PAGE_DIRTY|PAGE_DONT_WRITE|PAGE_NEED_SYNC|PAGE_IN_JOURNAL|PAGE_HOT_DIRTY);
		/* Release the page */
		pager_release_page(pPager,pPtr);
		/* Point to the next page */
		pPtr = pNext;
	}
	pPager->pAll = 0;
	pPager->nPage = 0;
	pPager->pDirty = pPager->pFirstDirty = 0;
	pPager->pHotDirty = pPager->pFirstHot = 0;
	pPager->nHot = 0;
	if( pPager->apHash ){
		/* Zero the table */
		SyZero((void *)pPager->apHash,sizeof(Page *) * pPager->nSize);
	}
	if( pPager->pVec ){
		vedisBitvecDestroy(pPager->pVec);
		pPager->pVec = 0;
	}
	/* Switch back to shared lock */
	pager_unlock_db(pPager,SHARED_LOCK);
	pPager->iState = PAGER_READER;
	if( bResetKvEngine ){
		/* Reset the underlying KV engine */
		pIo = pEngine->pIo;
		if( pIo->pMethods->xRelease ){
			/* Call the release callback */
			pIo->pMethods->xRelease(pEngine);
		}
		/* Zero the structure */
		SyZero(pEngine,(sxu32)pIo->pMethods->szKv);
		/* Fill in */
		pEngine->pIo = pIo;
		if( pIo->pMethods->xInit ){
			/* Call the init method */
			rc = pIo->pMethods->xInit(pEngine,pPager->iPageSize);
			if( rc != VEDIS_OK ){
				return rc;
			}
		}
		if( pIo->pMethods->xOpen ){
			/* Call the xOpen method */
			rc = pIo->pMethods->xOpen(pEngine,pPager->dbSize);
			if( rc != VEDIS_OK ){
				return rc;
			}
		}
	}
	/* All done */
	return VEDIS_OK;
}
/*
** If a write transaction is open, then all changes made within the 
** transaction are reverted and the current write-transaction is closed.
** The pager falls back to PAGER_READER state if successful.
**
** Otherwise, in rollback mode, this function performs two functions:
**
**   1) It rolls back the journal file, restoring all database file and 
**      in-memory cache pages to the state they were in when the transaction
**      was opened, and
**
**   2) It finalizes the journal file, so that it is not used for hot
**      rollback at any point in the future (i.e. deletion).
**
** Finalization of the journal file (task 2) is only performed if the 
** rollback is successful.
**
*/
VEDIS_PRIVATE int vedisPagerRollback(Pager *pPager,int bResetKvEngine)
{
	int rc = VEDIS_OK;
	if( pPager->iState < PAGER_WRITER_LOCKED ){
		/* A write transaction must be opened */
		return VEDIS_OK;
	}
	if( pPager->is_mem ){
		/* As of this release 1.1.6: Transactions are not supported for in-memory databases */
		return VEDIS_OK;
	}
	if( pPager->is_rdonly ){
		/* Read-Only DB */
		vedisGenError(pPager->pDb,"Read-Only database");
		return VEDIS_READ_ONLY;
	}
	if( pPager->iState >= PAGER_WRITER_CACHEMOD ){
		if( !pPager->no_jrnl ){
			/* Close any outstanding joural file */
			if( pPager->pjfd ){
				/* Sync the journal file */
				vedisOsSync(pPager->pjfd,VEDIS_SYNC_NORMAL);
			}
			vedisOsCloseFree(pPager->pAllocator,pPager->pjfd);
			pPager->pjfd = 0;
			if( pPager->iFlags & (PAGER_CTRL_COMMIT_ERR|PAGER_CTRL_DIRTY_COMMIT) ){
				/* Perform the rollback */
				rc = pager_journal_rollback(pPager,0);
				if( rc != VEDIS_OK ){
					/* Set the auto-commit flag */
					pPager->pDb->iFlags |= VEDIS_FL_DISABLE_AUTO_COMMIT;
					return rc;
				}
			}
		}
		/* Unlink the journal file */
		vedisOsDelete(pPager->pVfs,pPager->zJournal,1);
		/* Reset the pager state */
		rc = pager_reset_state(pPager,bResetKvEngine);
		if( rc != VEDIS_OK ){
			/* Mostly an unlikely scenario */
			pPager->pDb->iFlags |= VEDIS_FL_DISABLE_AUTO_COMMIT; /* Set the auto-commit flag */
			vedisGenError(pPager->pDb,"Error while reseting pager to its initial state");
			return rc;
		}
	}else{
		/* Downgrade to shared lock */
		pager_unlock_db(pPager,SHARED_LOCK);
		pPager->iState = PAGER_READER;
	}
	return VEDIS_OK;
}
/*
 *  Mark a data page as non writeable.
 */
static int vedisPagerDontWrite(vedis_page *pMyPage)
{
	Page *pPage = (Page *)pMyPage;
	if( pPage->pgno > 0 /* Page 0 is always writeable */ ){
		pPage->flags |= PAGE_DONT_WRITE;
	}
	return VEDIS_OK;
}
/*
** Mark a data page as writeable. This routine must be called before 
** making changes to a page. The caller must check the return value 
** of this function and be careful not to change any page data unless 
** this routine returns VEDIS_OK.
*/
static int vedisPageWrite(vedis_page *pMyPage)
{
	Page *pPage = (Page *)pMyPage;
	Pager *pPager = pPage->pPager;
	int rc;
	/* Begin the write transaction */
	rc = vedisPagerBegin(pPager);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( pPager->iState == PAGER_WRITER_LOCKED ){
		/* The journal file needs to be opened. Higher level routines have already
		 ** obtained the necessary locks to begin the write-transaction, but the
		 ** rollback journal might not yet be open. Open it now if this is the case.
		 */
		rc = vedisOpenJournal(pPager);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}
	if( pPager->nHot > 127 ){
		/* Write hot dirty pages */
		rc = pager_dirty_commit(pPager);
		if( rc != VEDIS_OK ){
			/* A rollback must be done */
			vedisGenError(pPager->pDb,"Please perform a rollback");
			return rc;
		}
	}
	/* Write the page to the journal file */
	rc = page_write(pPager,pPage);
	return rc;
}
/*
** Acquire a reference to page number pgno in pager pPager (a page
** reference has type vedis_page*). If the requested reference is 
** successfully obtained, it is copied to *ppPage and VEDIS_OK returned.
**
** If the requested page is already in the cache, it is returned. 
** Otherwise, a new page object is allocated and populated with data
** read from the database file.
*/
static int vedisPagerAcquire(
  Pager *pPager,      /* The pager open on the database file */
  pgno pgno,          /* Page number to fetch */
  vedis_page **ppPage,    /* OUT: Acquired page */
  int fetchOnly,      /* Cache lookup only */
  int noContent       /* Do not bother reading content from disk if true */
)
{
	Page *pPage;
	int rc;
	/* Acquire a shared lock (if not yet done) on the database and rollback any hot-journal if present */
	rc = pager_shared_lock(pPager);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Fetch the page from the cache */
	pPage = pager_fetch_page(pPager,pgno);
	if( fetchOnly ){
		if( ppPage ){
			*ppPage = (vedis_page *)pPage;
		}
		return pPage ? VEDIS_OK : VEDIS_NOTFOUND;
	}
	if( pPage == 0 ){
		/* Allocate a new page */
		pPage = pager_alloc_page(pPager,pgno);
		if( pPage == 0 ){
			vedisGenOutofMem(pPager->pDb);
			return VEDIS_NOMEM;
		}
		/* Read page contents */
		rc = pager_get_page_contents(pPager,pPage,noContent);
		if( rc != VEDIS_OK ){
			SyMemBackendPoolFree(pPager->pAllocator,pPage);
			return rc;
		}
		/* Link the page */
		pager_link_page(pPager,pPage);
	}else{
		if( ppPage ){
			page_ref(pPage);
		}
	}
	/* All done, page is loaded in memeory */
	if( ppPage ){
		*ppPage = (vedis_page *)pPage;
	}
	return VEDIS_OK;
}
/*
 * Return true if we are dealing with an in-memory database.
 */
static int vedisInMemory(const char *zFilename)
{
	sxu32 n;
	if( SX_EMPTY_STR(zFilename) ){
		/* NULL or the empty string means an in-memory database */
		return TRUE;
	}
	n = SyStrlen(zFilename);
	if( n == sizeof(":mem:") - 1 && 
		SyStrnicmp(zFilename,":mem:",sizeof(":mem:") - 1) == 0 ){
			return TRUE;
	}
	if( n == sizeof(":memory:") - 1 && 
		SyStrnicmp(zFilename,":memory:",sizeof(":memory:") - 1) == 0 ){
			return TRUE;
	}
	return FALSE;
}
/*
 * Allocate a new KV cursor.
 */
VEDIS_PRIVATE int vedisInitCursor(vedis *pDb,vedis_kv_cursor **ppOut)
{
	vedis_kv_methods *pMethods;
	vedis_kv_cursor *pCur;
	sxu32 nByte;
	/* Storage engine methods */
	pMethods = pDb->pPager->pEngine->pIo->pMethods;
	if( pMethods->szCursor < 1 ){
		/* Implementation does not supprt cursors */
		vedisGenErrorFormat(pDb,"Storage engine '%s' does not support cursors",pMethods->zName);
		return VEDIS_NOTIMPLEMENTED;
	}
	nByte = pMethods->szCursor;
	if( nByte < sizeof(vedis_kv_cursor) ){
		nByte += sizeof(vedis_kv_cursor);
	}
	pCur = (vedis_kv_cursor *)SyMemBackendPoolAlloc(&pDb->sMem,nByte);
	if( pCur == 0 ){
		vedisGenOutofMem(pDb);
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pCur,nByte);
	/* Save the cursor */
	pCur->pStore = pDb->pPager->pEngine;
	/* Invoke the initialization callback if any */
	if( pMethods->xCursorInit ){
		pMethods->xCursorInit(pCur);
	}
	/* All done */
	*ppOut = pCur;
	return VEDIS_OK;
}
/*
 * Release a cursor.
 */
VEDIS_PRIVATE int vedisReleaseCursor(vedis *pDb,vedis_kv_cursor *pCur)
{
	vedis_kv_methods *pMethods;
	/* Storage engine methods */
	pMethods = pDb->pPager->pEngine->pIo->pMethods;
	/* Invoke the release callback if available */
	if( pMethods->xCursorRelease ){
		pMethods->xCursorRelease(pCur);
	}
	/* Finally, free the whole instance */
	SyMemBackendPoolFree(&pDb->sMem,pCur);
	return VEDIS_OK;
}
/*
 * Release the underlying KV storage engine and invoke
 * its associated callbacks if available.
 */
static void pager_release_kv_engine(Pager *pPager)
{
	vedis_kv_engine *pEngine = pPager->pEngine;
	vedis *pStorage = pPager->pDb;
	if( pStorage->pCursor ){
		/* Release the associated cursor */
		vedisReleaseCursor(pPager->pDb,pStorage->pCursor);
		pStorage->pCursor = 0;
	}
	if( pEngine->pIo->pMethods->xRelease ){
		pEngine->pIo->pMethods->xRelease(pEngine);
	}
	/* Release the whole instance */
	SyMemBackendFree(&pPager->pDb->sMem,(void *)pEngine->pIo);
	SyMemBackendFree(&pPager->pDb->sMem,(void *)pEngine);
	pPager->pEngine = 0;
}
/* Forward declaration */
static int pager_kv_io_init(Pager *pPager,vedis_kv_methods *pMethods,vedis_kv_io *pIo);
/*
 * Allocate, initialize and register a new KV storage engine
 * within this database instance.
 */
VEDIS_PRIVATE int vedisPagerRegisterKvEngine(Pager *pPager,vedis_kv_methods *pMethods)
{
	vedis *pStorage = pPager->pDb;
	vedis *pDb = pPager->pDb;
	vedis_kv_engine *pEngine;
	vedis_kv_io *pIo;
	sxu32 nByte;
	int rc;
	if( pPager->pEngine ){
		if( pMethods == pPager->pEngine->pIo->pMethods ){
			/* Ticket 1432: Same implementation */
			return VEDIS_OK;
		}
		/* Release the old KV engine */
		pager_release_kv_engine(pPager);
	}
	/* Allocate a new KV engine instance */
	nByte = (sxu32)pMethods->szKv;
	pEngine = (vedis_kv_engine *)SyMemBackendAlloc(&pDb->sMem,nByte);
	if( pEngine == 0 ){
		vedisGenOutofMem(pDb);
		return VEDIS_NOMEM;
	}
	pIo = (vedis_kv_io *)SyMemBackendAlloc(&pDb->sMem,sizeof(vedis_kv_io));
	if( pIo == 0 ){
		SyMemBackendFree(&pDb->sMem,pEngine);
		vedisGenOutofMem(pDb);
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pIo,sizeof(vedis_io_methods));
	SyZero(pEngine,nByte);
	/* Populate the IO structure */
	pager_kv_io_init(pPager,pMethods,pIo);
	pEngine->pIo = pIo;
	/* Invoke the init callback if avaialble */
	if( pMethods->xInit ){
		rc = pMethods->xInit(pEngine,vedisGetPageSize());
		if( rc != VEDIS_OK ){
			vedisGenErrorFormat(pDb,
				"xInit() method of the underlying KV engine '%z' failed",&pPager->sKv);
			goto fail;
		}
		pEngine->pIo = pIo;
	}
	pPager->pEngine = pEngine;
	/* Allocate a new cursor */
	rc = vedisInitCursor(pDb,&pStorage->pCursor);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	return VEDIS_OK;
fail:
	SyMemBackendFree(&pDb->sMem,pEngine);
	SyMemBackendFree(&pDb->sMem,pIo);
	return rc;
}
/*
 * Return the underlying KV storage engine instance.
 */
VEDIS_PRIVATE vedis_kv_engine * vedisPagerGetKvEngine(vedis *pDb)
{
	return pDb->pPager->pEngine;
}
/*
* Allocate and initialize a new Pager object. The pager should
* eventually be freed by passing it to vedisPagerClose().
*
* The zFilename argument is the path to the database file to open.
* If zFilename is NULL or ":memory:" then all information is held
* in cache. It is never written to disk.  This can be used to implement
* an in-memory database.
*/
VEDIS_PRIVATE int vedisPagerOpen(
  vedis_vfs *pVfs,       /* The virtual file system to use */
  vedis *pDb,            /* Database handle */
  const char *zFilename,   /* Name of the database file to open */
  unsigned int iFlags      /* flags controlling this file */
  )
{
	vedis_kv_methods *pMethods = 0;
	int is_mem,rd_only,no_jrnl;
	Pager *pPager;
	sxu32 nByte;
	sxu32 nLen;
	int rc;

	/* Select the appropriate KV storage subsytem  */
	if( (iFlags & VEDIS_OPEN_IN_MEMORY) || vedisInMemory(zFilename) ){
		/* An in-memory database, record that  */
		pMethods = vedisFindKVStore("mem",sizeof("mem") - 1); /* Always available */
		iFlags |= VEDIS_OPEN_IN_MEMORY;
	}else{
		/* Install the default key value storage subsystem [i.e. Linear Hash] */
		pMethods = vedisFindKVStore("hash",sizeof("hash")-1);
		if( pMethods == 0 ){
			/* Use the b+tree storage backend if the linear hash storage is not available */
			pMethods = vedisFindKVStore("btree",sizeof("btree")-1);
		}
	}
	if( pMethods == 0 ){
		/* Can't happen */
		vedisGenError(pDb,"Cannot install a default Key/Value storage engine");
		return VEDIS_NOTIMPLEMENTED;
	}
	is_mem = (iFlags & VEDIS_OPEN_IN_MEMORY) != 0;
	rd_only = (iFlags & VEDIS_OPEN_READONLY) != 0;
	no_jrnl = (iFlags & VEDIS_OPEN_OMIT_JOURNALING) != 0;
	rc = VEDIS_OK;
	if( is_mem ){
		/* Omit journaling for in-memory database */
		no_jrnl = 1;
	}
	/* Total number of bytes to allocate */
	nByte = sizeof(Pager);
	nLen = 0;
	if( !is_mem ){
		nLen = SyStrlen(zFilename);
		nByte += pVfs->mxPathname + nLen + sizeof(char) /* null termniator */;
	}
	/* Allocate */
	pPager = (Pager *)SyMemBackendAlloc(&pDb->sMem,nByte);
	if( pPager == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pPager,nByte);
	/* Fill-in the structure */
	pPager->pAllocator = &pDb->sMem;
	pPager->pDb = pDb;
	pDb->pPager = pPager;
	/* Allocate page table */
	pPager->nSize = 128; /* Must be a power of two */
	nByte = pPager->nSize * sizeof(Page *);
	pPager->apHash = (Page **)SyMemBackendAlloc(pPager->pAllocator,nByte);
	if( pPager->apHash == 0 ){
		rc = VEDIS_NOMEM;
		goto fail;
	}
	SyZero(pPager->apHash,nByte);
	pPager->is_mem = is_mem;
	pPager->no_jrnl = no_jrnl;
	pPager->is_rdonly = rd_only;
	pPager->iOpenFlags = iFlags;
	pPager->pVfs = pVfs;
	SyRandomnessInit(&pPager->sPrng,0,0);
	SyRandomness(&pPager->sPrng,(void *)&pPager->cksumInit,sizeof(sxu32));
	/* Unlimited cache size */
	pPager->nCacheMax = SXU32_HIGH;
	/* Copy filename and journal name */
	if( !is_mem ){
		pPager->zFilename = (char *)&pPager[1];
		rc = VEDIS_OK;
		if( pVfs->xFullPathname ){
			rc = pVfs->xFullPathname(pVfs,zFilename,pVfs->mxPathname + nLen,pPager->zFilename);
		}
		if( rc != VEDIS_OK ){
			/* Simple filename copy */
			SyMemcpy(zFilename,pPager->zFilename,nLen);
			pPager->zFilename[nLen] = 0;
			rc = VEDIS_OK;
		}else{
			nLen = SyStrlen(pPager->zFilename);
		}
		pPager->zJournal = (char *) SyMemBackendAlloc(pPager->pAllocator,nLen + sizeof(VEDIS_JOURNAL_FILE_SUFFIX) + sizeof(char));
		if( pPager->zJournal == 0 ){
			rc = VEDIS_NOMEM;
			goto fail;
		}
		/* Copy filename */
		SyMemcpy(pPager->zFilename,pPager->zJournal,nLen);
		/* Copy journal suffix */
		SyMemcpy(VEDIS_JOURNAL_FILE_SUFFIX,&pPager->zJournal[nLen],sizeof(VEDIS_JOURNAL_FILE_SUFFIX)-1);
		/* Append the nul terminator to the journal path */
		pPager->zJournal[nLen + ( sizeof(VEDIS_JOURNAL_FILE_SUFFIX) - 1)] = 0;
	}
	/* Finally, register the selected KV engine */
	rc = vedisPagerRegisterKvEngine(pPager,pMethods);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	/* Set the pager state */
	if( pPager->is_mem ){
		pPager->iState = PAGER_WRITER_FINISHED;
		pPager->iLock = EXCLUSIVE_LOCK;
	}else{
		pPager->iState = PAGER_OPEN;
		pPager->iLock = NO_LOCK;
	}
	/* All done, ready for processing */
	return VEDIS_OK;
fail:
	SyMemBackendFree(&pDb->sMem,pPager);
	return rc;
}
/*
 * Return TRUE if we are dealing with an in-memory database.
 */
VEDIS_PRIVATE int vedisPagerisMemStore(vedis *pStore)
{
	return pStore->pPager->is_mem;
}
/*
 * Set a cache limit. Note that, this is a simple hint, the pager is not
 * forced to honor this limit.
 */
VEDIS_PRIVATE int vedisPagerSetCachesize(Pager *pPager,int mxPage)
{
	if( mxPage < 256 ){
		return VEDIS_INVALID;
	}
	pPager->nCacheMax = mxPage;
	return VEDIS_OK;
}
/*
 * Set the user commit callback.
 */
VEDIS_PRIVATE int vedisPagerSetCommitCallback(Pager *pPager,int (*xCommit)(void *),void *pUserdata)
{
	pPager->xCommit = xCommit;
	pPager->pCommitData = pUserdata;
	return VEDIS_OK;
}
/*
 * Shutdown the page cache. Free all memory and close the database file.
 */
VEDIS_PRIVATE int vedisPagerClose(Pager *pPager)
{
	/* Release the KV engine */
	pager_release_kv_engine(pPager);
	if( pPager->iOpenFlags & VEDIS_OPEN_MMAP ){
		const vedis_vfs *pVfs = vedisExportBuiltinVfs();
		if( pVfs && pVfs->xUnmap && pPager->pMmap ){
			pVfs->xUnmap(pPager->pMmap,pPager->dbByteSize);
		}
	}
	if( !pPager->is_mem && pPager->iState > PAGER_OPEN ){
		/* Release all lock on this database handle */
		pager_unlock_db(pPager,NO_LOCK);
		/* Close the file  */
		vedisOsCloseFree(pPager->pAllocator,pPager->pfd);
	}
	if( pPager->pVec ){
		vedisBitvecDestroy(pPager->pVec);
		pPager->pVec = 0;
	}
	return VEDIS_OK;
}
/*
 * Generate a random string.
 */
VEDIS_PRIVATE void vedisPagerRandomString(Pager *pPager,char *zBuf,sxu32 nLen)
{
	static const char zBase[] = {"abcdefghijklmnopqrstuvwxyz"}; /* English Alphabet */
	sxu32 i;
	/* Generate a binary string first */
	SyRandomness(&pPager->sPrng,zBuf,nLen);
	/* Turn the binary string into english based alphabet */
	for( i = 0 ; i < nLen ; ++i ){
		 zBuf[i] = zBase[zBuf[i] % (sizeof(zBase)-1)];
	 }
}
/*
 * Generate a random number.
 */
VEDIS_PRIVATE sxu32 vedisPagerRandomNum(Pager *pPager)
{
	sxu32 iNum;
	SyRandomness(&pPager->sPrng,(void *)&iNum,sizeof(iNum));
	return iNum;
}
/* Exported KV IO Methods */
/* 
 * Refer to [vedisPagerAcquire()]
 */
static int vedisKvIoPageGet(vedis_kv_handle pHandle,pgno iNum,vedis_page **ppPage)
{
	int rc;
	rc = vedisPagerAcquire((Pager *)pHandle,iNum,ppPage,0,0);
	return rc;
}
/* 
 * Refer to [vedisPagerAcquire()]
 */
static int vedisKvIoPageLookup(vedis_kv_handle pHandle,pgno iNum,vedis_page **ppPage)
{
	int rc;
	rc = vedisPagerAcquire((Pager *)pHandle,iNum,ppPage,1,0);
	return rc;
}
/* 
 * Refer to [vedisPagerAcquire()]
 */
static int vedisKvIoNewPage(vedis_kv_handle pHandle,vedis_page **ppPage)
{
	Pager *pPager = (Pager *)pHandle;
	int rc;
	/* 
	 * Acquire a reader-lock first so that pPager->dbSize get initialized.
	 */
	rc = pager_shared_lock(pPager);
	if( rc == VEDIS_OK ){
		rc = vedisPagerAcquire(pPager,pPager->dbSize == 0 ? /* Page 0 is reserved */ 1 : pPager->dbSize ,ppPage,0,0);
	}
	return rc;
}
/* 
 * Refer to [vedisPageWrite()]
 */
static int vedisKvIopageWrite(vedis_page *pPage)
{
	int rc;
	if( pPage == 0 ){
		/* TICKET 1433-0348 */
		return VEDIS_OK;
	}
	rc = vedisPageWrite(pPage);
	return rc;
}
/* 
 * Refer to [vedisPagerDontWrite()]
 */
static int vedisKvIoPageDontWrite(vedis_page *pPage)
{
	int rc;
	if( pPage == 0 ){
		/* TICKET 1433-0348 */
		return VEDIS_OK;
	}
	rc = vedisPagerDontWrite(pPage);
	return rc;
}
/* 
 * Refer to [vedisBitvecSet()]
 */
static int vedisKvIoPageDontJournal(vedis_page *pRaw)
{
	Page *pPage = (Page *)pRaw;
	Pager *pPager;
	if( pPage == 0 ){
		/* TICKET 1433-0348 */
		return VEDIS_OK;
	}
	pPager = pPage->pPager;
	if( pPager->iState >= PAGER_WRITER_LOCKED ){
		if( !pPager->no_jrnl && pPager->pVec && !vedisBitvecTest(pPager->pVec,pPage->pgno) ){
			vedisBitvecSet(pPager->pVec,pPage->pgno);
		}
	}
	return VEDIS_OK;
}
/* 
 * Do not add a page to the hot dirty list.
 */
static int vedisKvIoPageDontMakeHot(vedis_page *pRaw)
{
	Page *pPage = (Page *)pRaw;
	
	if( pPage == 0 ){
		/* TICKET 1433-0348 */
		return VEDIS_OK;
	}
	pPage->flags |= PAGE_DONT_MAKE_HOT;
	return VEDIS_OK;
}
/* 
 * Refer to [page_ref()]
 */
static int vedisKvIopage_ref(vedis_page *pPage)
{
	if( pPage ){
		page_ref((Page *)pPage);
	}
	return VEDIS_OK;
}
/* 
 * Refer to [page_unref()]
 */
static int vedisKvIoPageUnRef(vedis_page *pPage)
{
	if( pPage ){
		page_unref((Page *)pPage);
	}
	return VEDIS_OK;
}
/* 
 * Refer to the declaration of the [Pager] structure
 */
static int vedisKvIoReadOnly(vedis_kv_handle pHandle)
{
	return ((Pager *)pHandle)->is_rdonly;
}
/* 
 * Refer to the declaration of the [Pager] structure
 */
static int vedisKvIoPageSize(vedis_kv_handle pHandle)
{
	return ((Pager *)pHandle)->iPageSize;
}
/* 
 * Refer to the declaration of the [Pager] structure
 */
static unsigned char * vedisKvIoTempPage(vedis_kv_handle pHandle)
{
	return ((Pager *)pHandle)->zTmpPage;
}
/* 
 * Set a page unpin callback.
 * Refer to the declaration of the [Pager] structure
 */
static void vedisKvIoPageUnpin(vedis_kv_handle pHandle,void (*xPageUnpin)(void *))
{
	Pager *pPager = (Pager *)pHandle;
	pPager->xPageUnpin = xPageUnpin;
}
/* 
 * Set a page reload callback.
 * Refer to the declaration of the [Pager] structure
 */
static void vedisKvIoPageReload(vedis_kv_handle pHandle,void (*xPageReload)(void *))
{
	Pager *pPager = (Pager *)pHandle;
	pPager->xPageReload = xPageReload;
}
/* 
 * Log an error.
 * Refer to the declaration of the [Pager] structure
 */
static void vedisKvIoErr(vedis_kv_handle pHandle,const char *zErr)
{
	Pager *pPager = (Pager *)pHandle;
	vedisGenError(pPager->pDb,zErr);
}
/*
 * Init an instance of the [vedis_kv_io] structure.
 */
static int pager_kv_io_init(Pager *pPager,vedis_kv_methods *pMethods,vedis_kv_io *pIo)
{
	pIo->pHandle =  pPager;
	pIo->pMethods = pMethods;
	
	pIo->xGet    = vedisKvIoPageGet;
	pIo->xLookup = vedisKvIoPageLookup;
	pIo->xNew    = vedisKvIoNewPage;
	
	pIo->xWrite     = vedisKvIopageWrite; 
	pIo->xDontWrite = vedisKvIoPageDontWrite;
	pIo->xDontJournal = vedisKvIoPageDontJournal;
	pIo->xDontMkHot = vedisKvIoPageDontMakeHot;

	pIo->xPageRef   = vedisKvIopage_ref;
	pIo->xPageUnref = vedisKvIoPageUnRef;

	pIo->xPageSize = vedisKvIoPageSize;
	pIo->xReadOnly = vedisKvIoReadOnly;

	pIo->xTmpPage =  vedisKvIoTempPage;

	pIo->xSetUnpin = vedisKvIoPageUnpin;
	pIo->xSetReload = vedisKvIoPageReload;

	pIo->xErr = vedisKvIoErr;

	return VEDIS_OK;
}
/*
 * ----------------------------------------------------------
 * File: os_win.c
 * MD5: 8f05b9895ac8989f395417dcf864fa74
 * ----------------------------------------------------------
 */
/*
 * Symisc Vedis: An Embeddable NoSQL (Post Modern) Database Engine.
 * Copyright (C) 2012-2013, Symisc Systems http://vedis.org/
 * Version 1.1.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.org/licensing.html
 */
 /* $SymiscID: os_win.c v1.2 Win7 2012-11-10 12:10 devel <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* Omit the whole layer from the build if compiling for platforms other than Windows */
#ifdef __WINNT__
/* This file contains code that is specific to windows. (Mostly SQLite3 source tree) */
#include <Windows.h>
/*
** Some microsoft compilers lack this definition.
*/
#ifndef INVALID_FILE_ATTRIBUTES
# define INVALID_FILE_ATTRIBUTES ((DWORD)-1) 
#endif
/*
** WinCE lacks native support for file locking so we have to fake it
** with some code of our own.
*/
#ifdef __WIN_CE__
typedef struct winceLock {
  int nReaders;       /* Number of reader locks obtained */
  BOOL bPending;      /* Indicates a pending lock has been obtained */
  BOOL bReserved;     /* Indicates a reserved lock has been obtained */
  BOOL bExclusive;    /* Indicates an exclusive lock has been obtained */
} winceLock;
#define AreFileApisANSI() 1
#define FormatMessageW(a,b,c,d,e,f,g) 0
#endif

/*
** The winFile structure is a subclass of vedis_file* specific to the win32
** portability layer.
*/
typedef struct winFile winFile;
struct winFile {
  const vedis_io_methods *pMethod; /*** Must be first ***/
  vedis_vfs *pVfs;      /* The VFS used to open this file */
  HANDLE h;               /* Handle for accessing the file */
  sxu8 locktype;          /* Type of lock currently held on this file */
  short sharedLockByte;   /* Randomly chosen byte used as a shared lock */
  DWORD lastErrno;        /* The Windows errno from the last I/O error */
  DWORD sectorSize;       /* Sector size of the device file is on */
  int szChunk;            /* Chunk size */
#ifdef __WIN_CE__
  WCHAR *zDeleteOnClose;  /* Name of file to delete when closing */
  HANDLE hMutex;          /* Mutex used to control access to shared lock */  
  HANDLE hShared;         /* Shared memory segment used for locking */
  winceLock local;        /* Locks obtained by this instance of winFile */
  winceLock *shared;      /* Global shared lock memory for the file  */
#endif
};
/*
** Convert a UTF-8 string to microsoft unicode (UTF-16?). 
**
** Space to hold the returned string is obtained from HeapAlloc().
*/
static WCHAR *utf8ToUnicode(const char *zFilename){
  int nChar;
  WCHAR *zWideFilename;

  nChar = MultiByteToWideChar(CP_UTF8, 0, zFilename, -1, 0, 0);
  zWideFilename = (WCHAR *)HeapAlloc(GetProcessHeap(),0,nChar*sizeof(zWideFilename[0]) );
  if( zWideFilename==0 ){
    return 0;
  }
  nChar = MultiByteToWideChar(CP_UTF8, 0, zFilename, -1, zWideFilename, nChar);
  if( nChar==0 ){
    HeapFree(GetProcessHeap(),0,zWideFilename);
    zWideFilename = 0;
  }
  return zWideFilename;
}

/*
** Convert microsoft unicode to UTF-8.  Space to hold the returned string is
** obtained from malloc().
*/
static char *unicodeToUtf8(const WCHAR *zWideFilename){
  int nByte;
  char *zFilename;

  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideFilename, -1, 0, 0, 0, 0);
  zFilename = (char *)HeapAlloc(GetProcessHeap(),0,nByte );
  if( zFilename==0 ){
    return 0;
  }
  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideFilename, -1, zFilename, nByte,
                              0, 0);
  if( nByte == 0 ){
    HeapFree(GetProcessHeap(),0,zFilename);
    zFilename = 0;
  }
  return zFilename;
}

/*
** Convert an ansi string to microsoft unicode, based on the
** current codepage settings for file apis.
** 
** Space to hold the returned string is obtained
** from malloc.
*/
static WCHAR *mbcsToUnicode(const char *zFilename){
  int nByte;
  WCHAR *zMbcsFilename;
  int codepage = AreFileApisANSI() ? CP_ACP : CP_OEMCP;

  nByte = MultiByteToWideChar(codepage, 0, zFilename, -1, 0,0)*sizeof(WCHAR);
  zMbcsFilename = (WCHAR *)HeapAlloc(GetProcessHeap(),0,nByte*sizeof(zMbcsFilename[0]) );
  if( zMbcsFilename==0 ){
    return 0;
  }
  nByte = MultiByteToWideChar(codepage, 0, zFilename, -1, zMbcsFilename, nByte);
  if( nByte==0 ){
    HeapFree(GetProcessHeap(),0,zMbcsFilename);
    zMbcsFilename = 0;
  }
  return zMbcsFilename;
}
/*
** Convert multibyte character string to UTF-8.  Space to hold the
** returned string is obtained from malloc().
*/
char *vedis_win32_mbcs_to_utf8(const char *zFilename){
  char *zFilenameUtf8;
  WCHAR *zTmpWide;

  zTmpWide = mbcsToUnicode(zFilename);
  if( zTmpWide==0 ){
    return 0;
  }
  zFilenameUtf8 = unicodeToUtf8(zTmpWide);
  HeapFree(GetProcessHeap(),0,zTmpWide);
  return zFilenameUtf8;
}
/*
** Some microsoft compilers lack this definition.
*/
#ifndef INVALID_SET_FILE_POINTER
# define INVALID_SET_FILE_POINTER ((DWORD)-1)
#endif

/*
** Move the current position of the file handle passed as the first 
** argument to offset iOffset within the file. If successful, return 0. 
** Otherwise, set pFile->lastErrno and return non-zero.
*/
static int seekWinFile(winFile *pFile, vedis_int64 iOffset){
  LONG upperBits;                 /* Most sig. 32 bits of new offset */
  LONG lowerBits;                 /* Least sig. 32 bits of new offset */
  DWORD dwRet;                    /* Value returned by SetFilePointer() */

  upperBits = (LONG)((iOffset>>32) & 0x7fffffff);
  lowerBits = (LONG)(iOffset & 0xffffffff);

  /* API oddity: If successful, SetFilePointer() returns a dword 
  ** containing the lower 32-bits of the new file-offset. Or, if it fails,
  ** it returns INVALID_SET_FILE_POINTER. However according to MSDN, 
  ** INVALID_SET_FILE_POINTER may also be a valid new offset. So to determine 
  ** whether an error has actually occured, it is also necessary to call 
  ** GetLastError().
  */
  dwRet = SetFilePointer(pFile->h, lowerBits, &upperBits, FILE_BEGIN);
  if( (dwRet==INVALID_SET_FILE_POINTER && GetLastError()!=NO_ERROR) ){
    pFile->lastErrno = GetLastError();
    return 1;
  }
  return 0;
}
/*
** Close a file.
**
** It is reported that an attempt to close a handle might sometimes
** fail.  This is a very unreasonable result, but windows is notorious
** for being unreasonable so I do not doubt that it might happen.  If
** the close fails, we pause for 100 milliseconds and try again.  As
** many as MX_CLOSE_ATTEMPT attempts to close the handle are made before
** giving up and returning an error.
*/
#define MX_CLOSE_ATTEMPT 3
static int winClose(vedis_file *id)
{
  int rc, cnt = 0;
  winFile *pFile = (winFile*)id;
  do{
    rc = CloseHandle(pFile->h);
  }while( rc==0 && ++cnt < MX_CLOSE_ATTEMPT && (Sleep(100), 1) );

  return rc ? VEDIS_OK : VEDIS_IOERR;
}
/*
** Read data from a file into a buffer.  Return VEDIS_OK if all
** bytes were read successfully and VEDIS_IOERR if anything goes
** wrong.
*/
static int winRead(
  vedis_file *id,          /* File to read from */
  void *pBuf,                /* Write content into this buffer */
  vedis_int64 amt,        /* Number of bytes to read */
  vedis_int64 offset       /* Begin reading at this offset */
){
  winFile *pFile = (winFile*)id;  /* file handle */
  DWORD nRead;                    /* Number of bytes actually read from file */

  if( seekWinFile(pFile, offset) ){
    return VEDIS_FULL;
  }
  if( !ReadFile(pFile->h, pBuf, (DWORD)amt, &nRead, 0) ){
    pFile->lastErrno = GetLastError();
    return VEDIS_IOERR;
  }
  if( nRead<(DWORD)amt ){
    /* Unread parts of the buffer must be zero-filled */
    SyZero(&((char*)pBuf)[nRead],(sxu32)(amt-nRead));
    return VEDIS_IOERR;
  }

  return VEDIS_OK;
}

/*
** Write data from a buffer into a file.  Return VEDIS_OK on success
** or some other error code on failure.
*/
static int winWrite(
  vedis_file *id,               /* File to write into */
  const void *pBuf,               /* The bytes to be written */
  vedis_int64 amt,                        /* Number of bytes to write */
  vedis_int64 offset            /* Offset into the file to begin writing at */
){
  int rc;                         /* True if error has occured, else false */
  winFile *pFile = (winFile*)id;  /* File handle */

  rc = seekWinFile(pFile, offset);
  if( rc==0 ){
    sxu8 *aRem = (sxu8 *)pBuf;        /* Data yet to be written */
    vedis_int64 nRem = amt;         /* Number of bytes yet to be written */
    DWORD nWrite;                 /* Bytes written by each WriteFile() call */

    while( nRem>0 && WriteFile(pFile->h, aRem, (DWORD)nRem, &nWrite, 0) && nWrite>0 ){
      aRem += nWrite;
      nRem -= nWrite;
    }
    if( nRem>0 ){
      pFile->lastErrno = GetLastError();
      rc = 1;
    }
  }
  if( rc ){
    if( pFile->lastErrno==ERROR_HANDLE_DISK_FULL ){
      return VEDIS_FULL;
    }
    return VEDIS_IOERR;
  }
  return VEDIS_OK;
}

/*
** Truncate an open file to a specified size
*/
static int winTruncate(vedis_file *id, vedis_int64 nByte){
  winFile *pFile = (winFile*)id;  /* File handle object */
  int rc = VEDIS_OK;             /* Return code for this function */


  /* If the user has configured a chunk-size for this file, truncate the
  ** file so that it consists of an integer number of chunks (i.e. the
  ** actual file size after the operation may be larger than the requested
  ** size).
  */
  if( pFile->szChunk ){
    nByte = ((nByte + pFile->szChunk - 1)/pFile->szChunk) * pFile->szChunk;
  }

  /* SetEndOfFile() returns non-zero when successful, or zero when it fails. */
  if( seekWinFile(pFile, nByte) ){
    rc = VEDIS_IOERR;
  }else if( 0==SetEndOfFile(pFile->h) ){
    pFile->lastErrno = GetLastError();
    rc = VEDIS_IOERR;
  }
  return rc;
}
/*
** Make sure all writes to a particular file are committed to disk.
*/
static int winSync(vedis_file *id, int flags){
  winFile *pFile = (winFile*)id;
  SXUNUSED(flags); /* MSVC warning */
  if( FlushFileBuffers(pFile->h) ){
    return VEDIS_OK;
  }else{
    pFile->lastErrno = GetLastError();
    return VEDIS_IOERR;
  }
}
/*
** Determine the current size of a file in bytes
*/
static int winFileSize(vedis_file *id, vedis_int64 *pSize){
  DWORD upperBits;
  DWORD lowerBits;
  winFile *pFile = (winFile*)id;
  DWORD error;
  lowerBits = GetFileSize(pFile->h, &upperBits);
  if(   (lowerBits == INVALID_FILE_SIZE)
     && ((error = GetLastError()) != NO_ERROR) )
  {
    pFile->lastErrno = error;
    return VEDIS_IOERR;
  }
  *pSize = (((vedis_int64)upperBits)<<32) + lowerBits;
  return VEDIS_OK;
}
/*
** LOCKFILE_FAIL_IMMEDIATELY is undefined on some Windows systems.
*/
#ifndef LOCKFILE_FAIL_IMMEDIATELY
# define LOCKFILE_FAIL_IMMEDIATELY 1
#endif

/*
** Acquire a reader lock.
*/
static int getReadLock(winFile *pFile){
  int res;
  OVERLAPPED ovlp;
  ovlp.Offset = SHARED_FIRST;
  ovlp.OffsetHigh = 0;
  ovlp.hEvent = 0;
  res = LockFileEx(pFile->h, LOCKFILE_FAIL_IMMEDIATELY,0, SHARED_SIZE, 0, &ovlp);
  if( res == 0 ){
    pFile->lastErrno = GetLastError();
  }
  return res;
}
/*
** Undo a readlock
*/
static int unlockReadLock(winFile *pFile){
  int res;
  res = UnlockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
  if( res == 0 ){
    pFile->lastErrno = GetLastError();
  }
  return res;
}
/*
** Lock the file with the lock specified by parameter locktype - one
** of the following:
**
**     (1) SHARED_LOCK
**     (2) RESERVED_LOCK
**     (3) PENDING_LOCK
**     (4) EXCLUSIVE_LOCK
**
** Sometimes when requesting one lock state, additional lock states
** are inserted in between.  The locking might fail on one of the later
** transitions leaving the lock state different from what it started but
** still short of its goal.  The following chart shows the allowed
** transitions and the inserted intermediate states:
**
**    UNLOCKED -> SHARED
**    SHARED -> RESERVED
**    SHARED -> (PENDING) -> EXCLUSIVE
**    RESERVED -> (PENDING) -> EXCLUSIVE
**    PENDING -> EXCLUSIVE
**
** This routine will only increase a lock.  The winUnlock() routine
** erases all locks at once and returns us immediately to locking level 0.
** It is not possible to lower the locking level one step at a time.  You
** must go straight to locking level 0.
*/
static int winLock(vedis_file *id, int locktype){
  int rc = VEDIS_OK;    /* Return code from subroutines */
  int res = 1;           /* Result of a windows lock call */
  int newLocktype;       /* Set pFile->locktype to this value before exiting */
  int gotPendingLock = 0;/* True if we acquired a PENDING lock this time */
  winFile *pFile = (winFile*)id;
  DWORD error = NO_ERROR;

  /* If there is already a lock of this type or more restrictive on the
  ** OsFile, do nothing.
  */
  if( pFile->locktype>=locktype ){
    return VEDIS_OK;
  }

  /* Make sure the locking sequence is correct
  assert( pFile->locktype!=NO_LOCK || locktype==SHARED_LOCK );
  assert( locktype!=PENDING_LOCK );
  assert( locktype!=RESERVED_LOCK || pFile->locktype==SHARED_LOCK );
  */
  /* Lock the PENDING_LOCK byte if we need to acquire a PENDING lock or
  ** a SHARED lock.  If we are acquiring a SHARED lock, the acquisition of
  ** the PENDING_LOCK byte is temporary.
  */
  newLocktype = pFile->locktype;
  if(   (pFile->locktype==NO_LOCK)
     || (   (locktype==EXCLUSIVE_LOCK)
         && (pFile->locktype==RESERVED_LOCK))
  ){
    int cnt = 3;
    while( cnt-->0 && (res = LockFile(pFile->h, PENDING_BYTE, 0, 1, 0))==0 ){
      /* Try 3 times to get the pending lock.  The pending lock might be
      ** held by another reader process who will release it momentarily.
	  */
      Sleep(1);
    }
    gotPendingLock = res;
    if( !res ){
      error = GetLastError();
    }
  }

  /* Acquire a shared lock
  */
  if( locktype==SHARED_LOCK && res ){
   /* assert( pFile->locktype==NO_LOCK ); */
    res = getReadLock(pFile);
    if( res ){
      newLocktype = SHARED_LOCK;
    }else{
      error = GetLastError();
    }
  }

  /* Acquire a RESERVED lock
  */
  if( locktype==RESERVED_LOCK && res ){
    /* assert( pFile->locktype==SHARED_LOCK ); */
    res = LockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    if( res ){
      newLocktype = RESERVED_LOCK;
    }else{
      error = GetLastError();
    }
  }

  /* Acquire a PENDING lock
  */
  if( locktype==EXCLUSIVE_LOCK && res ){
    newLocktype = PENDING_LOCK;
    gotPendingLock = 0;
  }

  /* Acquire an EXCLUSIVE lock
  */
  if( locktype==EXCLUSIVE_LOCK && res ){
    /* assert( pFile->locktype>=SHARED_LOCK ); */
    res = unlockReadLock(pFile);
    res = LockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
    if( res ){
      newLocktype = EXCLUSIVE_LOCK;
    }else{
      error = GetLastError();
      getReadLock(pFile);
    }
  }

  /* If we are holding a PENDING lock that ought to be released, then
  ** release it now.
  */
  if( gotPendingLock && locktype==SHARED_LOCK ){
    UnlockFile(pFile->h, PENDING_BYTE, 0, 1, 0);
  }

  /* Update the state of the lock has held in the file descriptor then
  ** return the appropriate result code.
  */
  if( res ){
    rc = VEDIS_OK;
  }else{
    pFile->lastErrno = error;
    rc = VEDIS_BUSY;
  }
  pFile->locktype = (sxu8)newLocktype;
  return rc;
}
/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, return
** non-zero, otherwise zero.
*/
static int winCheckReservedLock(vedis_file *id, int *pResOut){
  int rc;
  winFile *pFile = (winFile*)id;
  if( pFile->locktype>=RESERVED_LOCK ){
    rc = 1;
  }else{
    rc = LockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    if( rc ){
      UnlockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    }
    rc = !rc;
  }
  *pResOut = rc;
  return VEDIS_OK;
}
/*
** Lower the locking level on file descriptor id to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
**
** It is not possible for this routine to fail if the second argument
** is NO_LOCK.  If the second argument is SHARED_LOCK then this routine
** might return VEDIS_IOERR;
*/
static int winUnlock(vedis_file *id, int locktype){
  int type;
  winFile *pFile = (winFile*)id;
  int rc = VEDIS_OK;

  type = pFile->locktype;
  if( type>=EXCLUSIVE_LOCK ){
    UnlockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
    if( locktype==SHARED_LOCK && !getReadLock(pFile) ){
      /* This should never happen.  We should always be able to
      ** reacquire the read lock */
      rc = VEDIS_IOERR;
    }
  }
  if( type>=RESERVED_LOCK ){
    UnlockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
  }
  if( locktype==NO_LOCK && type>=SHARED_LOCK ){
    unlockReadLock(pFile);
  }
  if( type>=PENDING_LOCK ){
    UnlockFile(pFile->h, PENDING_BYTE, 0, 1, 0);
  }
  pFile->locktype = (sxu8)locktype;
  return rc;
}
/*
** Return the sector size in bytes of the underlying block device for
** the specified file. This is almost always 512 bytes, but may be
** larger for some devices.
**
*/
static int winSectorSize(vedis_file *id){
  return (int)(((winFile*)id)->sectorSize);
}
/*
** This vector defines all the methods that can operate on an
** vedis_file for Windows systems.
*/
static const vedis_io_methods winIoMethod = {
  1,                              /* iVersion */
  winClose,                       /* xClose */
  winRead,                        /* xRead */
  winWrite,                       /* xWrite */
  winTruncate,                    /* xTruncate */
  winSync,                        /* xSync */
  winFileSize,                    /* xFileSize */
  winLock,                        /* xLock */
  winUnlock,                      /* xUnlock */
  winCheckReservedLock,           /* xCheckReservedLock */
  winSectorSize,                  /* xSectorSize */
};
/*
 * Windows VFS Methods.
 */
/*
** Convert a UTF-8 filename into whatever form the underlying
** operating system wants filenames in.  Space to hold the result
** is obtained from malloc and must be freed by the calling
** function.
*/
static void *convertUtf8Filename(const char *zFilename)
{
  void *zConverted;
  zConverted = utf8ToUnicode(zFilename);
  /* caller will handle out of memory */
  return zConverted;
}
/*
** Delete the named file.
**
** Note that windows does not allow a file to be deleted if some other
** process has it open.  Sometimes a virus scanner or indexing program
** will open a journal file shortly after it is created in order to do
** whatever it does.  While this other process is holding the
** file open, we will be unable to delete it.  To work around this
** problem, we delay 100 milliseconds and try to delete again.  Up
** to MX_DELETION_ATTEMPTs deletion attempts are run before giving
** up and returning an error.
*/
#define MX_DELETION_ATTEMPTS 5
static int winDelete(
  vedis_vfs *pVfs,          /* Not used on win32 */
  const char *zFilename,      /* Name of file to delete */
  int syncDir                 /* Not used on win32 */
){
  int cnt = 0;
  DWORD rc;
  DWORD error = 0;
  void *zConverted;
  zConverted = convertUtf8Filename(zFilename);
  if( zConverted==0 ){
	   SXUNUSED(pVfs);
	   SXUNUSED(syncDir);
    return VEDIS_NOMEM;
  }
  do{
	  DeleteFileW((LPCWSTR)zConverted);
  }while(   (   ((rc = GetFileAttributesW((LPCWSTR)zConverted)) != INVALID_FILE_ATTRIBUTES)
	  || ((error = GetLastError()) == ERROR_ACCESS_DENIED))
	  && (++cnt < MX_DELETION_ATTEMPTS)
	  && (Sleep(100), 1)
	  );
	HeapFree(GetProcessHeap(),0,zConverted);
 
  return (   (rc == INVALID_FILE_ATTRIBUTES) 
          && (error == ERROR_FILE_NOT_FOUND)) ? VEDIS_OK : VEDIS_IOERR;
}
/*
** Check the existance and status of a file.
*/
static int winAccess(
  vedis_vfs *pVfs,         /* Not used  */
  const char *zFilename,     /* Name of file to check */
  int flags,                 /* Type of test to make on this file */
  int *pResOut               /* OUT: Result */
){
  WIN32_FILE_ATTRIBUTE_DATA sAttrData;
  DWORD attr;
  int rc = 0;
  void *zConverted;
  SXUNUSED(pVfs);

  zConverted = convertUtf8Filename(zFilename);
  if( zConverted==0 ){
    return VEDIS_NOMEM;
  }
  SyZero(&sAttrData,sizeof(sAttrData));
  if( GetFileAttributesExW((WCHAR*)zConverted,
	  GetFileExInfoStandard, 
	  &sAttrData) ){
      /* For an VEDIS_ACCESS_EXISTS query, treat a zero-length file
      ** as if it does not exist.
      */
      if(    flags==VEDIS_ACCESS_EXISTS
          && sAttrData.nFileSizeHigh==0 
          && sAttrData.nFileSizeLow==0 ){
        attr = INVALID_FILE_ATTRIBUTES;
      }else{
        attr = sAttrData.dwFileAttributes;
      }
    }else{
      if( GetLastError()!=ERROR_FILE_NOT_FOUND ){
        HeapFree(GetProcessHeap(),0,zConverted);
        return VEDIS_IOERR;
      }else{
        attr = INVALID_FILE_ATTRIBUTES;
      }
    }
  HeapFree(GetProcessHeap(),0,zConverted);
  switch( flags ){
     case VEDIS_ACCESS_READWRITE:
      rc = (attr & FILE_ATTRIBUTE_READONLY)==0;
      break;
    case VEDIS_ACCESS_READ:
    case VEDIS_ACCESS_EXISTS:
	default:
      rc = attr!=INVALID_FILE_ATTRIBUTES;
      break;
  }
  *pResOut = rc;
  return VEDIS_OK;
}
/*
** Turn a relative pathname into a full pathname.  Write the full
** pathname into zOut[].  zOut[] will be at least pVfs->mxPathname
** bytes in size.
*/
static int winFullPathname(
  vedis_vfs *pVfs,            /* Pointer to vfs object */
  const char *zRelative,        /* Possibly relative input path */
  int nFull,                    /* Size of output buffer in bytes */
  char *zFull                   /* Output buffer */
){
  int nByte;
  void *zConverted;
  WCHAR *zTemp;
  char *zOut;
  SXUNUSED(nFull);
  zConverted = convertUtf8Filename(zRelative);
  if( zConverted == 0 ){
	  return VEDIS_NOMEM;
  }
  nByte = GetFullPathNameW((WCHAR*)zConverted, 0, 0, 0) + 3;
  zTemp = (WCHAR *)HeapAlloc(GetProcessHeap(),0,nByte*sizeof(zTemp[0]) );
  if( zTemp==0 ){
	  HeapFree(GetProcessHeap(),0,zConverted);
	  return VEDIS_NOMEM;
  }
  GetFullPathNameW((WCHAR*)zConverted, nByte, zTemp, 0);
  HeapFree(GetProcessHeap(),0,zConverted);
  zOut = unicodeToUtf8(zTemp);
  HeapFree(GetProcessHeap(),0,zTemp);
  if( zOut == 0 ){
    return VEDIS_NOMEM;
  }
  Systrcpy(zFull,(sxu32)pVfs->mxPathname,zOut,0);
  HeapFree(GetProcessHeap(),0,zOut);
  return VEDIS_OK;
}
/*
** Get the sector size of the device used to store
** file.
*/
static int getSectorSize(
    vedis_vfs *pVfs,
    const char *zRelative     /* UTF-8 file name */
){
  DWORD bytesPerSector = VEDIS_DEFAULT_SECTOR_SIZE;
  char zFullpath[MAX_PATH+1];
  int rc;
  DWORD dwRet = 0;
  DWORD dwDummy;
  /*
  ** We need to get the full path name of the file
  ** to get the drive letter to look up the sector
  ** size.
  */
  rc = winFullPathname(pVfs, zRelative, MAX_PATH, zFullpath);
  if( rc == VEDIS_OK )
  {
    void *zConverted = convertUtf8Filename(zFullpath);
    if( zConverted ){
        /* trim path to just drive reference */
        WCHAR *p = (WCHAR *)zConverted;
        for(;*p;p++){
          if( *p == '\\' ){
            *p = '\0';
            break;
          }
        }
        dwRet = GetDiskFreeSpaceW((WCHAR*)zConverted,
                                  &dwDummy,
                                  &bytesPerSector,
                                  &dwDummy,
                                  &dwDummy);
		 HeapFree(GetProcessHeap(),0,zConverted);
	}
    if( !dwRet ){
      bytesPerSector = VEDIS_DEFAULT_SECTOR_SIZE;
    }
  }
  return (int) bytesPerSector; 
}
/*
** Sleep for a little while.  Return the amount of time slept.
*/
static int winSleep(vedis_vfs *pVfs, int microsec){
  Sleep((microsec+999)/1000);
  SXUNUSED(pVfs);
  return ((microsec+999)/1000)*1000;
}
/*
 * Export the current system time.
 */
static int winCurrentTime(vedis_vfs *pVfs,Sytm *pOut)
{
	SYSTEMTIME sSys;
	SXUNUSED(pVfs);
	GetSystemTime(&sSys);
	SYSTEMTIME_TO_SYTM(&sSys,pOut);
	return VEDIS_OK;
}
/*
** The idea is that this function works like a combination of
** GetLastError() and FormatMessage() on windows (or errno and
** strerror_r() on unix). After an error is returned by an OS
** function, UnQLite calls this function with zBuf pointing to
** a buffer of nBuf bytes. The OS layer should populate the
** buffer with a nul-terminated UTF-8 encoded error message
** describing the last IO error to have occurred within the calling
** thread.
**
** If the error message is too large for the supplied buffer,
** it should be truncated. The return value of xGetLastError
** is zero if the error message fits in the buffer, or non-zero
** otherwise (if the message was truncated). If non-zero is returned,
** then it is not necessary to include the nul-terminator character
** in the output buffer.
*/
static int winGetLastError(vedis_vfs *pVfs, int nBuf, char *zBuf)
{
  /* FormatMessage returns 0 on failure.  Otherwise it
  ** returns the number of TCHARs written to the output
  ** buffer, excluding the terminating null char.
  */
  DWORD error = GetLastError();
  WCHAR *zTempWide = 0;
  DWORD dwLen;
  char *zOut = 0;

  SXUNUSED(pVfs);
  dwLen = FormatMessageW(
	  FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
	  0,
	  error,
	  0,
	  (LPWSTR) &zTempWide,
	  0,
	  0
	  );
    if( dwLen > 0 ){
      /* allocate a buffer and convert to UTF8 */
      zOut = unicodeToUtf8(zTempWide);
      /* free the system buffer allocated by FormatMessage */
      LocalFree(zTempWide);
    }
	if( 0 == dwLen ){
		Systrcpy(zBuf,(sxu32)nBuf,"OS Error",sizeof("OS Error")-1);
	}else{
		/* copy a maximum of nBuf chars to output buffer */
		Systrcpy(zBuf,(sxu32)nBuf,zOut,0 /* Compute input length automatically */);
		/* free the UTF8 buffer */
		HeapFree(GetProcessHeap(),0,zOut);
	}
  return 0;
}
/*
** Open a file.
*/
static int winOpen(
  vedis_vfs *pVfs,        /* Not used */
  const char *zName,        /* Name of the file (UTF-8) */
  vedis_file *id,         /* Write the UnQLite file handle here */
  unsigned int flags                /* Open mode flags */
){
  HANDLE h;
  DWORD dwDesiredAccess;
  DWORD dwShareMode;
  DWORD dwCreationDisposition;
  DWORD dwFlagsAndAttributes = 0;
  winFile *pFile = (winFile*)id;
  void *zConverted;              /* Filename in OS encoding */
  const char *zUtf8Name = zName; /* Filename in UTF-8 encoding */
  int isExclusive  = (flags & VEDIS_OPEN_EXCLUSIVE);
  int isDelete     = (flags & VEDIS_OPEN_TEMP_DB);
  int isCreate     = (flags & VEDIS_OPEN_CREATE);
  int isReadWrite  = (flags & VEDIS_OPEN_READWRITE);

  pFile->h = INVALID_HANDLE_VALUE;
  /* Convert the filename to the system encoding. */
  zConverted = convertUtf8Filename(zUtf8Name);
  if( zConverted==0 ){
    return VEDIS_NOMEM;
  }
  if( isReadWrite ){
    dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
  }else{
    dwDesiredAccess = GENERIC_READ;
  }
  /* VEDIS_OPEN_EXCLUSIVE is used to make sure that a new file is 
  ** created.
  */
  if( isExclusive ){
    /* Creates a new file, only if it does not already exist. */
    /* If the file exists, it fails. */
    dwCreationDisposition = CREATE_NEW;
  }else if( isCreate ){
    /* Open existing file, or create if it doesn't exist */
    dwCreationDisposition = OPEN_ALWAYS;
  }else{
    /* Opens a file, only if it exists. */
    dwCreationDisposition = OPEN_EXISTING;
  }

  dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;

  if( isDelete ){
    dwFlagsAndAttributes = FILE_ATTRIBUTE_TEMPORARY
                               | FILE_ATTRIBUTE_HIDDEN
                               | FILE_FLAG_DELETE_ON_CLOSE;
  }else{
    dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
  }
  h = CreateFileW((WCHAR*)zConverted,
       dwDesiredAccess,
       dwShareMode,
       NULL,
       dwCreationDisposition,
       dwFlagsAndAttributes,
       NULL
    );
  if( h==INVALID_HANDLE_VALUE ){
    pFile->lastErrno = GetLastError();
    HeapFree(GetProcessHeap(),0,zConverted);
	return VEDIS_IOERR;
  }
  SyZero(pFile,sizeof(*pFile));
  pFile->pMethod = &winIoMethod;
  pFile->h = h;
  pFile->lastErrno = NO_ERROR;
  pFile->pVfs = pVfs;
  pFile->sectorSize = getSectorSize(pVfs, zUtf8Name);
  HeapFree(GetProcessHeap(),0,zConverted);
  return VEDIS_OK;
}
/* Open a file in a read-only mode */
static HANDLE OpenReadOnly(LPCWSTR pPath)
{
	DWORD dwType = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS;
	DWORD dwShare = FILE_SHARE_READ | FILE_SHARE_WRITE;
	DWORD dwAccess = GENERIC_READ;
	DWORD dwCreate = OPEN_EXISTING;	
	HANDLE pHandle;
	pHandle = CreateFileW(pPath, dwAccess, dwShare, 0, dwCreate, dwType, 0);
	if( pHandle == INVALID_HANDLE_VALUE){
		return 0;
	}
	return pHandle;
}
/* int (*xMmap)(const char *, void **, vedis_int64 *) */
static int winMmap(const char *zPath, void **ppMap,vedis_int64 *pSize)
{
	DWORD dwSizeLow, dwSizeHigh;
	HANDLE pHandle, pMapHandle;
	void *pConverted, *pView;

	pConverted = convertUtf8Filename(zPath);
	if( pConverted == 0 ){
		return -1;
	}
	pHandle = OpenReadOnly((LPCWSTR)pConverted);
	HeapFree(GetProcessHeap(), 0, pConverted);
	if( pHandle == 0 ){
		return -1;
	}
	/* Get the file size */
	dwSizeLow = GetFileSize(pHandle, &dwSizeHigh);
	/* Create the mapping */
	pMapHandle = CreateFileMappingW(pHandle, 0, PAGE_READONLY, dwSizeHigh, dwSizeLow, 0);
	if( pMapHandle == 0 ){
		CloseHandle(pHandle);
		return -1;
	}
	*pSize = ((vedis_int64)dwSizeHigh << 32) | dwSizeLow;
	/* Obtain the view */
	pView = MapViewOfFile(pMapHandle, FILE_MAP_READ, 0, 0, (SIZE_T)(*pSize));
	if( pView ){
		/* Let the upper layer point to the view */
		*ppMap = pView;
	}
	/* Close the handle
	 * According to MSDN it is OK the close the HANDLES.
	 */
	CloseHandle(pMapHandle);
	CloseHandle(pHandle);
	return pView ? VEDIS_OK : -1;
}
/* void (*xUnmap)(void *, vedis_int64)  */
static void winUnmap(void *pView, vedis_int64 nSize)
{
	nSize = 0; /* Compiler warning */
	UnmapViewOfFile(pView);
}
/*
 * Export the Windows Vfs.
 */
VEDIS_PRIVATE const vedis_vfs * vedisExportBuiltinVfs(void)
{
	static const vedis_vfs sWinvfs = {
		"Windows",           /* Vfs name */
		1,                   /* Vfs structure version */
		sizeof(winFile),     /* szOsFile */
		MAX_PATH,            /* mxPathName */
		winOpen,             /* xOpen */
		winDelete,           /* xDelete */
		winAccess,           /* xAccess */
		winFullPathname,     /* xFullPathname */
		0,                   /* xTmp */
		winSleep,            /* xSleep */
		winCurrentTime,      /* xCurrentTime */
		winGetLastError,     /* xGetLastError */
		winMmap,            /* xMmap */
		winUnmap            /* xUnmap */
	};
	return &sWinvfs;
}
#endif /* __WINNT__ */
/*
 * ----------------------------------------------------------
 * File: os_unix.c
 * MD5: 41ff547568152212b320903ae83fe8f8
 * ----------------------------------------------------------
 */
/*
 * Symisc Vedis: An Embeddable NoSQL (Post Modern) Database Engine.
 * Copyright (C) 2012-2013, Symisc Systems http://vedis.org/
 * Version 1.1.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.org/licensing.html
 */
 /* $SymiscID: os_unix.c v1.3 FreeBSD 2013-04-05 01:10 devel <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* 
 * Omit the whole layer from the build if compiling for platforms other than Unix (Linux, BSD, Solaris, OS X, etc.).
 * Note: Mostly SQLite3 source tree.
 */
#if defined(__UNIXES__)
/** This file contains the VFS implementation for unix-like operating systems
** include Linux, MacOSX, *BSD, QNX, VxWorks, AIX, HPUX, and others.
**
** There are actually several different VFS implementations in this file.
** The differences are in the way that file locking is done.  The default
** implementation uses Posix Advisory Locks.  Alternative implementations
** use flock(), dot-files, various proprietary locking schemas, or simply
** skip locking all together.
**
** This source file is organized into divisions where the logic for various
** subfunctions is contained within the appropriate division.  PLEASE
** KEEP THE STRUCTURE OF THIS FILE INTACT.  New code should be placed
** in the correct division and should be clearly labeled.
**
*/
/*
** standard include files.
*/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#if defined(__APPLE__) 
# include <sys/mount.h>
#endif
/*
** Allowed values of unixFile.fsFlags
*/
#define VEDIS_FSFLAGS_IS_MSDOS     0x1

/*
** Default permissions when creating a new file
*/
#ifndef VEDIS_DEFAULT_FILE_PERMISSIONS
# define VEDIS_DEFAULT_FILE_PERMISSIONS 0644
#endif
/*
 ** Default permissions when creating auto proxy dir
 */
#ifndef VEDIS_DEFAULT_PROXYDIR_PERMISSIONS
# define VEDIS_DEFAULT_PROXYDIR_PERMISSIONS 0755
#endif
/*
** Maximum supported path-length.
*/
#define MAX_PATHNAME 512
/*
** Only set the lastErrno if the error code is a real error and not 
** a normal expected return code of VEDIS_BUSY or VEDIS_OK
*/
#define IS_LOCK_ERROR(x)  ((x != VEDIS_OK) && (x != VEDIS_BUSY))
/* Forward references */
typedef struct unixInodeInfo unixInodeInfo;   /* An i-node */
typedef struct UnixUnusedFd UnixUnusedFd;     /* An unused file descriptor */
/*
** Sometimes, after a file handle is closed by SQLite, the file descriptor
** cannot be closed immediately. In these cases, instances of the following
** structure are used to store the file descriptor while waiting for an
** opportunity to either close or reuse it.
*/
struct UnixUnusedFd {
  int fd;                   /* File descriptor to close */
  int flags;                /* Flags this file descriptor was opened with */
  UnixUnusedFd *pNext;      /* Next unused file descriptor on same file */
};
/*
** The unixFile structure is subclass of vedis3_file specific to the unix
** VFS implementations.
*/
typedef struct unixFile unixFile;
struct unixFile {
  const vedis_io_methods *pMethod;  /* Always the first entry */
  unixInodeInfo *pInode;              /* Info about locks on this inode */
  int h;                              /* The file descriptor */
  int dirfd;                          /* File descriptor for the directory */
  unsigned char eFileLock;            /* The type of lock held on this fd */
  int lastErrno;                      /* The unix errno from last I/O error */
  void *lockingContext;               /* Locking style specific state */
  UnixUnusedFd *pUnused;              /* Pre-allocated UnixUnusedFd */
  int fileFlags;                      /* Miscellanous flags */
  const char *zPath;                  /* Name of the file */
  unsigned fsFlags;                   /* cached details from statfs() */
};
/*
** The following macros define bits in unixFile.fileFlags
*/
#define VEDIS_WHOLE_FILE_LOCKING  0x0001   /* Use whole-file locking */
/*
** Define various macros that are missing from some systems.
*/
#ifndef O_LARGEFILE
# define O_LARGEFILE 0
#endif
#ifndef O_NOFOLLOW
# define O_NOFOLLOW 0
#endif
#ifndef O_BINARY
# define O_BINARY 0
#endif
/*
** Helper functions to obtain and relinquish the global mutex. The
** global mutex is used to protect the unixInodeInfo and
** vxworksFileId objects used by this file, all of which may be 
** shared by multiple threads.
**
** Function unixMutexHeld() is used to assert() that the global mutex 
** is held when required. This function is only used as part of assert() 
** statements. e.g.
**
**   unixEnterMutex()
**     assert( unixMutexHeld() );
**   unixEnterLeave()
*/
static void unixEnterMutex(void){
#ifdef VEDIS_ENABLE_THREADS
	const SyMutexMethods *pMutexMethods = SyMutexExportMethods();
	if( pMutexMethods ){
		SyMutex *pMutex = pMutexMethods->xNew(SXMUTEX_TYPE_STATIC_2); /* pre-allocated, never fail */
		SyMutexEnter(pMutexMethods,pMutex);
	}
#endif /* VEDIS_ENABLE_THREADS */
}
static void unixLeaveMutex(void){
#ifdef VEDIS_ENABLE_THREADS
  const SyMutexMethods *pMutexMethods = SyMutexExportMethods();
  if( pMutexMethods ){
	 SyMutex *pMutex = pMutexMethods->xNew(SXMUTEX_TYPE_STATIC_2); /* pre-allocated, never fail */
	 SyMutexLeave(pMutexMethods,pMutex);
  }
#endif /* VEDIS_ENABLE_THREADS */
}
/*
** This routine translates a standard POSIX errno code into something
** useful to the clients of the vedis3 functions.  Specifically, it is
** intended to translate a variety of "try again" errors into VEDIS_BUSY
** and a variety of "please close the file descriptor NOW" errors into 
** VEDIS_IOERR
** 
** Errors during initialization of locks, or file system support for locks,
** should handle ENOLCK, ENOTSUP, EOPNOTSUPP separately.
*/
static int vedisErrorFromPosixError(int posixError, int vedisIOErr) {
  switch (posixError) {
  case 0: 
    return VEDIS_OK;
    
  case EAGAIN:
  case ETIMEDOUT:
  case EBUSY:
  case EINTR:
  case ENOLCK:  
    /* random NFS retry error, unless during file system support 
     * introspection, in which it actually means what it says */
    return VEDIS_BUSY;
 
  case EACCES: 
    /* EACCES is like EAGAIN during locking operations, but not any other time*/
      return VEDIS_BUSY;
    
  case EPERM: 
    return VEDIS_PERM;
    
  case EDEADLK:
    return VEDIS_IOERR;
    
#if EOPNOTSUPP!=ENOTSUP
  case EOPNOTSUPP: 
    /* something went terribly awry, unless during file system support 
     * introspection, in which it actually means what it says */
#endif
#ifdef ENOTSUP
  case ENOTSUP: 
    /* invalid fd, unless during file system support introspection, in which 
     * it actually means what it says */
#endif
  case EIO:
  case EBADF:
  case EINVAL:
  case ENOTCONN:
  case ENODEV:
  case ENXIO:
  case ENOENT:
  case ESTALE:
  case ENOSYS:
    /* these should force the client to close the file and reconnect */
    
  default: 
    return vedisIOErr;
  }
}
/******************************************************************************
*************************** Posix Advisory Locking ****************************
**
** POSIX advisory locks are broken by design.  ANSI STD 1003.1 (1996)
** section 6.5.2.2 lines 483 through 490 specify that when a process
** sets or clears a lock, that operation overrides any prior locks set
** by the same process.  It does not explicitly say so, but this implies
** that it overrides locks set by the same process using a different
** file descriptor.  Consider this test case:
**
**       int fd1 = open("./file1", O_RDWR|O_CREAT, 0644);
**       int fd2 = open("./file2", O_RDWR|O_CREAT, 0644);
**
** Suppose ./file1 and ./file2 are really the same file (because
** one is a hard or symbolic link to the other) then if you set
** an exclusive lock on fd1, then try to get an exclusive lock
** on fd2, it works.  I would have expected the second lock to
** fail since there was already a lock on the file due to fd1.
** But not so.  Since both locks came from the same process, the
** second overrides the first, even though they were on different
** file descriptors opened on different file names.
**
** This means that we cannot use POSIX locks to synchronize file access
** among competing threads of the same process.  POSIX locks will work fine
** to synchronize access for threads in separate processes, but not
** threads within the same process.
**
** To work around the problem, SQLite has to manage file locks internally
** on its own.  Whenever a new database is opened, we have to find the
** specific inode of the database file (the inode is determined by the
** st_dev and st_ino fields of the stat structure that fstat() fills in)
** and check for locks already existing on that inode.  When locks are
** created or removed, we have to look at our own internal record of the
** locks to see if another thread has previously set a lock on that same
** inode.
**
** (Aside: The use of inode numbers as unique IDs does not work on VxWorks.
** For VxWorks, we have to use the alternative unique ID system based on
** canonical filename and implemented in the previous division.)
**
** There is one locking structure
** per inode, so if the same inode is opened twice, both unixFile structures
** point to the same locking structure.  The locking structure keeps
** a reference count (so we will know when to delete it) and a "cnt"
** field that tells us its internal lock status.  cnt==0 means the
** file is unlocked.  cnt==-1 means the file has an exclusive lock.
** cnt>0 means there are cnt shared locks on the file.
**
** Any attempt to lock or unlock a file first checks the locking
** structure.  The fcntl() system call is only invoked to set a 
** POSIX lock if the internal lock structure transitions between
** a locked and an unlocked state.
**
** But wait:  there are yet more problems with POSIX advisory locks.
**
** If you close a file descriptor that points to a file that has locks,
** all locks on that file that are owned by the current process are
** released.  To work around this problem, each unixInodeInfo object
** maintains a count of the number of pending locks on that inode.
** When an attempt is made to close an unixFile, if there are
** other unixFile open on the same inode that are holding locks, the call
** to close() the file descriptor is deferred until all of the locks clear.
** The unixInodeInfo structure keeps a list of file descriptors that need to
** be closed and that list is walked (and cleared) when the last lock
** clears.
**
** Yet another problem:  LinuxThreads do not play well with posix locks.
**
** Many older versions of linux use the LinuxThreads library which is
** not posix compliant.  Under LinuxThreads, a lock created by thread
** A cannot be modified or overridden by a different thread B.
** Only thread A can modify the lock.  Locking behavior is correct
** if the appliation uses the newer Native Posix Thread Library (NPTL)
** on linux - with NPTL a lock created by thread A can override locks
** in thread B.  But there is no way to know at compile-time which
** threading library is being used.  So there is no way to know at
** compile-time whether or not thread A can override locks on thread B.
** One has to do a run-time check to discover the behavior of the
** current process.
**
*/

/*
** An instance of the following structure serves as the key used
** to locate a particular unixInodeInfo object.
*/
struct unixFileId {
  dev_t dev;                  /* Device number */
  ino_t ino;                  /* Inode number */
};
/*
** An instance of the following structure is allocated for each open
** inode.  Or, on LinuxThreads, there is one of these structures for
** each inode opened by each thread.
**
** A single inode can have multiple file descriptors, so each unixFile
** structure contains a pointer to an instance of this object and this
** object keeps a count of the number of unixFile pointing to it.
*/
struct unixInodeInfo {
  struct unixFileId fileId;       /* The lookup key */
  int nShared;                    /* Number of SHARED locks held */
  int eFileLock;                  /* One of SHARED_LOCK, RESERVED_LOCK etc. */
  int nRef;                       /* Number of pointers to this structure */
  int nLock;                      /* Number of outstanding file locks */
  UnixUnusedFd *pUnused;          /* Unused file descriptors to close */
  unixInodeInfo *pNext;           /* List of all unixInodeInfo objects */
  unixInodeInfo *pPrev;           /*    .... doubly linked */
};

static unixInodeInfo *inodeList = 0;
/*
 * Local memory allocation stuff.
 */
static void * vedis_malloc(sxu32 nByte)
{
	SyMemBackend *pAlloc;
	void *p;
	pAlloc = (SyMemBackend *)vedisExportMemBackend();
	p = SyMemBackendAlloc(pAlloc,nByte);
	return p;
}
static void vedis_free(void *p)
{
	SyMemBackend *pAlloc;
	pAlloc = (SyMemBackend *)vedisExportMemBackend();
	SyMemBackendFree(pAlloc,p);
}
/*
** Close all file descriptors accumuated in the unixInodeInfo->pUnused list.
** If all such file descriptors are closed without error, the list is
** cleared and VEDIS_OK returned.
**
** Otherwise, if an error occurs, then successfully closed file descriptor
** entries are removed from the list, and VEDIS_IOERR_CLOSE returned. 
** not deleted and VEDIS_IOERR_CLOSE returned.
*/ 
static int closePendingFds(unixFile *pFile){
  int rc = VEDIS_OK;
  unixInodeInfo *pInode = pFile->pInode;
  UnixUnusedFd *pError = 0;
  UnixUnusedFd *p;
  UnixUnusedFd *pNext;
  for(p=pInode->pUnused; p; p=pNext){
    pNext = p->pNext;
    if( close(p->fd) ){
      pFile->lastErrno = errno;
	  rc = VEDIS_IOERR;
      p->pNext = pError;
      pError = p;
    }else{
      vedis_free(p);
    }
  }
  pInode->pUnused = pError;
  return rc;
}
/*
** Release a unixInodeInfo structure previously allocated by findInodeInfo().
**
** The mutex entered using the unixEnterMutex() function must be held
** when this function is called.
*/
static void releaseInodeInfo(unixFile *pFile){
  unixInodeInfo *pInode = pFile->pInode;
  if( pInode ){
    pInode->nRef--;
    if( pInode->nRef==0 ){
      closePendingFds(pFile);
      if( pInode->pPrev ){
        pInode->pPrev->pNext = pInode->pNext;
      }else{
        inodeList = pInode->pNext;
      }
      if( pInode->pNext ){
        pInode->pNext->pPrev = pInode->pPrev;
      }
      vedis_free(pInode);
    }
  }
}
/*
** Given a file descriptor, locate the unixInodeInfo object that
** describes that file descriptor.  Create a new one if necessary.  The
** return value might be uninitialized if an error occurs.
**
** The mutex entered using the unixEnterMutex() function must be held
** when this function is called.
**
** Return an appropriate error code.
*/
static int findInodeInfo(
  unixFile *pFile,               /* Unix file with file desc used in the key */
  unixInodeInfo **ppInode        /* Return the unixInodeInfo object here */
){
  int rc;                        /* System call return code */
  int fd;                        /* The file descriptor for pFile */
  struct unixFileId fileId;      /* Lookup key for the unixInodeInfo */
  struct stat statbuf;           /* Low-level file information */
  unixInodeInfo *pInode = 0;     /* Candidate unixInodeInfo object */

  /* Get low-level information about the file that we can used to
  ** create a unique name for the file.
  */
  fd = pFile->h;
  rc = fstat(fd, &statbuf);
  if( rc!=0 ){
    pFile->lastErrno = errno;
#ifdef EOVERFLOW
	if( pFile->lastErrno==EOVERFLOW ) return VEDIS_NOTIMPLEMENTED;
#endif
    return VEDIS_IOERR;
  }

#ifdef __APPLE__
  /* On OS X on an msdos filesystem, the inode number is reported
  ** incorrectly for zero-size files.  See ticket #3260.  To work
  ** around this problem (we consider it a bug in OS X, not SQLite)
  ** we always increase the file size to 1 by writing a single byte
  ** prior to accessing the inode number.  The one byte written is
  ** an ASCII 'S' character which also happens to be the first byte
  ** in the header of every SQLite database.  In this way, if there
  ** is a race condition such that another thread has already populated
  ** the first page of the database, no damage is done.
  */
  if( statbuf.st_size==0 && (pFile->fsFlags & VEDIS_FSFLAGS_IS_MSDOS)!=0 ){
    rc = write(fd, "S", 1);
    if( rc!=1 ){
      pFile->lastErrno = errno;
      return VEDIS_IOERR;
    }
    rc = fstat(fd, &statbuf);
    if( rc!=0 ){
      pFile->lastErrno = errno;
      return VEDIS_IOERR;
    }
  }
#endif
  SyZero(&fileId,sizeof(fileId));
  fileId.dev = statbuf.st_dev;
  fileId.ino = statbuf.st_ino;
  pInode = inodeList;
  while( pInode && SyMemcmp((const void *)&fileId,(const void *)&pInode->fileId, sizeof(fileId)) ){
    pInode = pInode->pNext;
  }
  if( pInode==0 ){
    pInode = (unixInodeInfo *)vedis_malloc( sizeof(*pInode) );
    if( pInode==0 ){
      return VEDIS_NOMEM;
    }
    SyZero(pInode,sizeof(*pInode));
	SyMemcpy((const void *)&fileId,(void *)&pInode->fileId,sizeof(fileId));
    pInode->nRef = 1;
    pInode->pNext = inodeList;
    pInode->pPrev = 0;
    if( inodeList ) inodeList->pPrev = pInode;
    inodeList = pInode;
  }else{
    pInode->nRef++;
  }
  *ppInode = pInode;
  return VEDIS_OK;
}
/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, set *pResOut
** to a non-zero value otherwise *pResOut is set to zero.  The return value
** is set to VEDIS_OK unless an I/O error occurs during lock checking.
*/
static int unixCheckReservedLock(vedis_file *id, int *pResOut){
  int rc = VEDIS_OK;
  int reserved = 0;
  unixFile *pFile = (unixFile*)id;

 
  unixEnterMutex(); /* Because pFile->pInode is shared across threads */

  /* Check if a thread in this process holds such a lock */
  if( pFile->pInode->eFileLock>SHARED_LOCK ){
    reserved = 1;
  }

  /* Otherwise see if some other process holds it.
  */
  if( !reserved ){
    struct flock lock;
    lock.l_whence = SEEK_SET;
    lock.l_start = RESERVED_BYTE;
    lock.l_len = 1;
    lock.l_type = F_WRLCK;
    if (-1 == fcntl(pFile->h, F_GETLK, &lock)) {
      int tErrno = errno;
	  rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
      pFile->lastErrno = tErrno;
    } else if( lock.l_type!=F_UNLCK ){
      reserved = 1;
    }
  }
  
  unixLeaveMutex();
 
  *pResOut = reserved;
  return rc;
}
/*
** Lock the file with the lock specified by parameter eFileLock - one
** of the following:
**
**     (1) SHARED_LOCK
**     (2) RESERVED_LOCK
**     (3) PENDING_LOCK
**     (4) EXCLUSIVE_LOCK
**
** Sometimes when requesting one lock state, additional lock states
** are inserted in between.  The locking might fail on one of the later
** transitions leaving the lock state different from what it started but
** still short of its goal.  The following chart shows the allowed
** transitions and the inserted intermediate states:
**
**    UNLOCKED -> SHARED
**    SHARED -> RESERVED
**    SHARED -> (PENDING) -> EXCLUSIVE
**    RESERVED -> (PENDING) -> EXCLUSIVE
**    PENDING -> EXCLUSIVE
**
** This routine will only increase a lock.  Use the vedisOsUnlock()
** routine to lower a locking level.
*/
static int unixLock(vedis_file *id, int eFileLock){
  /* The following describes the implementation of the various locks and
  ** lock transitions in terms of the POSIX advisory shared and exclusive
  ** lock primitives (called read-locks and write-locks below, to avoid
  ** confusion with SQLite lock names). The algorithms are complicated
  ** slightly in order to be compatible with unixdows systems simultaneously
  ** accessing the same database file, in case that is ever required.
  **
  ** Symbols defined in os.h indentify the 'pending byte' and the 'reserved
  ** byte', each single bytes at well known offsets, and the 'shared byte
  ** range', a range of 510 bytes at a well known offset.
  **
  ** To obtain a SHARED lock, a read-lock is obtained on the 'pending
  ** byte'.  If this is successful, a random byte from the 'shared byte
  ** range' is read-locked and the lock on the 'pending byte' released.
  **
  ** A process may only obtain a RESERVED lock after it has a SHARED lock.
  ** A RESERVED lock is implemented by grabbing a write-lock on the
  ** 'reserved byte'. 
  **
  ** A process may only obtain a PENDING lock after it has obtained a
  ** SHARED lock. A PENDING lock is implemented by obtaining a write-lock
  ** on the 'pending byte'. This ensures that no new SHARED locks can be
  ** obtained, but existing SHARED locks are allowed to persist. A process
  ** does not have to obtain a RESERVED lock on the way to a PENDING lock.
  ** This property is used by the algorithm for rolling back a journal file
  ** after a crash.
  **
  ** An EXCLUSIVE lock, obtained after a PENDING lock is held, is
  ** implemented by obtaining a write-lock on the entire 'shared byte
  ** range'. Since all other locks require a read-lock on one of the bytes
  ** within this range, this ensures that no other locks are held on the
  ** database. 
  **
  ** The reason a single byte cannot be used instead of the 'shared byte
  ** range' is that some versions of unixdows do not support read-locks. By
  ** locking a random byte from a range, concurrent SHARED locks may exist
  ** even if the locking primitive used is always a write-lock.
  */
  int rc = VEDIS_OK;
  unixFile *pFile = (unixFile*)id;
  unixInodeInfo *pInode = pFile->pInode;
  struct flock lock;
  int s = 0;
  int tErrno = 0;

  /* If there is already a lock of this type or more restrictive on the
  ** unixFile, do nothing. Don't use the end_lock: exit path, as
  ** unixEnterMutex() hasn't been called yet.
  */
  if( pFile->eFileLock>=eFileLock ){
    return VEDIS_OK;
  }
  /* This mutex is needed because pFile->pInode is shared across threads
  */
  unixEnterMutex();
  pInode = pFile->pInode;

  /* If some thread using this PID has a lock via a different unixFile*
  ** handle that precludes the requested lock, return BUSY.
  */
  if( (pFile->eFileLock!=pInode->eFileLock && 
          (pInode->eFileLock>=PENDING_LOCK || eFileLock>SHARED_LOCK))
  ){
    rc = VEDIS_BUSY;
    goto end_lock;
  }

  /* If a SHARED lock is requested, and some thread using this PID already
  ** has a SHARED or RESERVED lock, then increment reference counts and
  ** return VEDIS_OK.
  */
  if( eFileLock==SHARED_LOCK && 
      (pInode->eFileLock==SHARED_LOCK || pInode->eFileLock==RESERVED_LOCK) ){
    pFile->eFileLock = SHARED_LOCK;
    pInode->nShared++;
    pInode->nLock++;
    goto end_lock;
  }
  /* A PENDING lock is needed before acquiring a SHARED lock and before
  ** acquiring an EXCLUSIVE lock.  For the SHARED lock, the PENDING will
  ** be released.
  */
  lock.l_len = 1L;
  lock.l_whence = SEEK_SET;
  if( eFileLock==SHARED_LOCK 
      || (eFileLock==EXCLUSIVE_LOCK && pFile->eFileLock<PENDING_LOCK)
  ){
    lock.l_type = (eFileLock==SHARED_LOCK?F_RDLCK:F_WRLCK);
    lock.l_start = PENDING_BYTE;
    s = fcntl(pFile->h, F_SETLK, &lock);
    if( s==(-1) ){
      tErrno = errno;
      rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
      goto end_lock;
    }
  }
  /* If control gets to this point, then actually go ahead and make
  ** operating system calls for the specified lock.
  */
  if( eFileLock==SHARED_LOCK ){
    /* Now get the read-lock */
    lock.l_start = SHARED_FIRST;
    lock.l_len = SHARED_SIZE;
    if( (s = fcntl(pFile->h, F_SETLK, &lock))==(-1) ){
      tErrno = errno;
    }
    /* Drop the temporary PENDING lock */
    lock.l_start = PENDING_BYTE;
    lock.l_len = 1L;
    lock.l_type = F_UNLCK;
    if( fcntl(pFile->h, F_SETLK, &lock)!=0 ){
      if( s != -1 ){
        /* This could happen with a network mount */
        tErrno = errno; 
        rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR); 
        if( IS_LOCK_ERROR(rc) ){
          pFile->lastErrno = tErrno;
        }
        goto end_lock;
      }
    }
    if( s==(-1) ){
		rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
    }else{
      pFile->eFileLock = SHARED_LOCK;
      pInode->nLock++;
      pInode->nShared = 1;
    }
  }else if( eFileLock==EXCLUSIVE_LOCK && pInode->nShared>1 ){
    /* We are trying for an exclusive lock but another thread in this
    ** same process is still holding a shared lock. */
    rc = VEDIS_BUSY;
  }else{
    /* The request was for a RESERVED or EXCLUSIVE lock.  It is
    ** assumed that there is a SHARED or greater lock on the file
    ** already.
    */
    lock.l_type = F_WRLCK;
    switch( eFileLock ){
      case RESERVED_LOCK:
        lock.l_start = RESERVED_BYTE;
        break;
      case EXCLUSIVE_LOCK:
        lock.l_start = SHARED_FIRST;
        lock.l_len = SHARED_SIZE;
        break;
      default:
		  /* Can't happen */
        break;
    }
    s = fcntl(pFile->h, F_SETLK, &lock);
    if( s==(-1) ){
      tErrno = errno;
      rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
    }
  }
  if( rc==VEDIS_OK ){
    pFile->eFileLock = eFileLock;
    pInode->eFileLock = eFileLock;
  }else if( eFileLock==EXCLUSIVE_LOCK ){
    pFile->eFileLock = PENDING_LOCK;
    pInode->eFileLock = PENDING_LOCK;
  }
end_lock:
  unixLeaveMutex();
  return rc;
}
/*
** Add the file descriptor used by file handle pFile to the corresponding
** pUnused list.
*/
static void setPendingFd(unixFile *pFile){
  unixInodeInfo *pInode = pFile->pInode;
  UnixUnusedFd *p = pFile->pUnused;
  p->pNext = pInode->pUnused;
  pInode->pUnused = p;
  pFile->h = -1;
  pFile->pUnused = 0;
}
/*
** Lower the locking level on file descriptor pFile to eFileLock.  eFileLock
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
** 
** If handleNFSUnlock is true, then on downgrading an EXCLUSIVE_LOCK to SHARED
** the byte range is divided into 2 parts and the first part is unlocked then
** set to a read lock, then the other part is simply unlocked.  This works 
** around a bug in BSD NFS lockd (also seen on MacOSX 10.3+) that fails to 
** remove the write lock on a region when a read lock is set.
*/
static int _posixUnlock(vedis_file *id, int eFileLock, int handleNFSUnlock){
  unixFile *pFile = (unixFile*)id;
  unixInodeInfo *pInode;
  struct flock lock;
  int rc = VEDIS_OK;
  int h;
  int tErrno;                      /* Error code from system call errors */

   if( pFile->eFileLock<=eFileLock ){
    return VEDIS_OK;
  }
  unixEnterMutex();
  
  h = pFile->h;
  pInode = pFile->pInode;
  
  if( pFile->eFileLock>SHARED_LOCK ){
    /* downgrading to a shared lock on NFS involves clearing the write lock
    ** before establishing the readlock - to avoid a race condition we downgrade
    ** the lock in 2 blocks, so that part of the range will be covered by a 
    ** write lock until the rest is covered by a read lock:
    **  1:   [WWWWW]
    **  2:   [....W]
    **  3:   [RRRRW]
    **  4:   [RRRR.]
    */
    if( eFileLock==SHARED_LOCK ){
      if( handleNFSUnlock ){
        off_t divSize = SHARED_SIZE - 1;
        
        lock.l_type = F_UNLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = SHARED_FIRST;
        lock.l_len = divSize;
        if( fcntl(h, F_SETLK, &lock)==(-1) ){
          tErrno = errno;
		  rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
          if( IS_LOCK_ERROR(rc) ){
            pFile->lastErrno = tErrno;
          }
          goto end_unlock;
        }
        lock.l_type = F_RDLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = SHARED_FIRST;
        lock.l_len = divSize;
        if( fcntl(h, F_SETLK, &lock)==(-1) ){
          tErrno = errno;
		  rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
          if( IS_LOCK_ERROR(rc) ){
            pFile->lastErrno = tErrno;
          }
          goto end_unlock;
        }
        lock.l_type = F_UNLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = SHARED_FIRST+divSize;
        lock.l_len = SHARED_SIZE-divSize;
        if( fcntl(h, F_SETLK, &lock)==(-1) ){
          tErrno = errno;
		  rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
          if( IS_LOCK_ERROR(rc) ){
            pFile->lastErrno = tErrno;
          }
          goto end_unlock;
        }
      }else{
        lock.l_type = F_RDLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = SHARED_FIRST;
        lock.l_len = SHARED_SIZE;
        if( fcntl(h, F_SETLK, &lock)==(-1) ){
          tErrno = errno;
		  rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
          if( IS_LOCK_ERROR(rc) ){
            pFile->lastErrno = tErrno;
          }
          goto end_unlock;
        }
      }
    }
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = PENDING_BYTE;
    lock.l_len = 2L;
    if( fcntl(h, F_SETLK, &lock)!=(-1) ){
      pInode->eFileLock = SHARED_LOCK;
    }else{
      tErrno = errno;
	  rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
      goto end_unlock;
    }
  }
  if( eFileLock==NO_LOCK ){
    /* Decrement the shared lock counter.  Release the lock using an
    ** OS call only when all threads in this same process have released
    ** the lock.
    */
    pInode->nShared--;
    if( pInode->nShared==0 ){
      lock.l_type = F_UNLCK;
      lock.l_whence = SEEK_SET;
      lock.l_start = lock.l_len = 0L;
      
      if( fcntl(h, F_SETLK, &lock)!=(-1) ){
        pInode->eFileLock = NO_LOCK;
      }else{
        tErrno = errno;
		rc = vedisErrorFromPosixError(tErrno, VEDIS_LOCKERR);
        if( IS_LOCK_ERROR(rc) ){
          pFile->lastErrno = tErrno;
        }
        pInode->eFileLock = NO_LOCK;
        pFile->eFileLock = NO_LOCK;
      }
    }

    /* Decrement the count of locks against this same file.  When the
    ** count reaches zero, close any other file descriptors whose close
    ** was deferred because of outstanding locks.
    */
    pInode->nLock--;
 
    if( pInode->nLock==0 ){
      int rc2 = closePendingFds(pFile);
      if( rc==VEDIS_OK ){
        rc = rc2;
      }
    }
  }
	
end_unlock:

  unixLeaveMutex();
  
  if( rc==VEDIS_OK ) pFile->eFileLock = eFileLock;
  return rc;
}
/*
** Lower the locking level on file descriptor pFile to eFileLock.  eFileLock
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
*/
static int unixUnlock(vedis_file *id, int eFileLock){
  return _posixUnlock(id, eFileLock, 0);
}
/*
** This function performs the parts of the "close file" operation 
** common to all locking schemes. It closes the directory and file
** handles, if they are valid, and sets all fields of the unixFile
** structure to 0.
**
*/
static int closeUnixFile(vedis_file *id){
  unixFile *pFile = (unixFile*)id;
  if( pFile ){
    if( pFile->dirfd>=0 ){
      int err = close(pFile->dirfd);
      if( err ){
        pFile->lastErrno = errno;
        return VEDIS_IOERR;
      }else{
        pFile->dirfd=-1;
      }
    }
    if( pFile->h>=0 ){
      int err = close(pFile->h);
      if( err ){
        pFile->lastErrno = errno;
        return VEDIS_IOERR;
      }
    }
    vedis_free(pFile->pUnused);
    SyZero(pFile,sizeof(unixFile));
  }
  return VEDIS_OK;
}
/*
** Close a file.
*/
static int unixClose(vedis_file *id){
  int rc = VEDIS_OK;
  if( id ){
    unixFile *pFile = (unixFile *)id;
    unixUnlock(id, NO_LOCK);
    unixEnterMutex();
    if( pFile->pInode && pFile->pInode->nLock ){
      /* If there are outstanding locks, do not actually close the file just
      ** yet because that would clear those locks.  Instead, add the file
      ** descriptor to pInode->pUnused list.  It will be automatically closed 
      ** when the last lock is cleared.
      */
      setPendingFd(pFile);
    }
    releaseInodeInfo(pFile);
    rc = closeUnixFile(id);
    unixLeaveMutex();
  }
  return rc;
}
/************** End of the posix advisory lock implementation *****************
******************************************************************************/
/*
**
** The next division contains implementations for all methods of the 
** vedis_file object other than the locking methods.  The locking
** methods were defined in divisions above (one locking method per
** division).  Those methods that are common to all locking modes
** are gather together into this division.
*/
/*
** Seek to the offset passed as the second argument, then read cnt 
** bytes into pBuf. Return the number of bytes actually read.
**
** NB:  If you define USE_PREAD or USE_PREAD64, then it might also
** be necessary to define _XOPEN_SOURCE to be 500.  This varies from
** one system to another.  Since SQLite does not define USE_PREAD
** any form by default, we will not attempt to define _XOPEN_SOURCE.
** See tickets #2741 and #2681.
**
** To avoid stomping the errno value on a failed read the lastErrno value
** is set before returning.
*/
static int seekAndRead(unixFile *id, vedis_int64 offset, void *pBuf, int cnt){
  int got;
#if (!defined(USE_PREAD) && !defined(USE_PREAD64))
  vedis_int64 newOffset;
#endif
 
#if defined(USE_PREAD)
  got = pread(id->h, pBuf, cnt, offset);
#elif defined(USE_PREAD64)
  got = pread64(id->h, pBuf, cnt, offset);
#else
  newOffset = lseek(id->h, offset, SEEK_SET);
  
  if( newOffset!=offset ){
    if( newOffset == -1 ){
      ((unixFile*)id)->lastErrno = errno;
    }else{
      ((unixFile*)id)->lastErrno = 0;			
    }
    return -1;
  }
  got = read(id->h, pBuf, cnt);
#endif
  if( got<0 ){
    ((unixFile*)id)->lastErrno = errno;
  }
  return got;
}
/*
** Read data from a file into a buffer.  Return VEDIS_OK if all
** bytes were read successfully and VEDIS_IOERR if anything goes
** wrong.
*/
static int unixRead(
  vedis_file *id, 
  void *pBuf, 
  vedis_int64 amt,
  vedis_int64 offset
){
  unixFile *pFile = (unixFile *)id;
  int got;
  
  got = seekAndRead(pFile, offset, pBuf, (int)amt);
  if( got==(int)amt ){
    return VEDIS_OK;
  }else if( got<0 ){
    /* lastErrno set by seekAndRead */
    return VEDIS_IOERR;
  }else{
    pFile->lastErrno = 0; /* not a system error */
    /* Unread parts of the buffer must be zero-filled */
    SyZero(&((char*)pBuf)[got],(sxu32)amt-got);
    return VEDIS_IOERR;
  }
}
/*
** Seek to the offset in id->offset then read cnt bytes into pBuf.
** Return the number of bytes actually read.  Update the offset.
**
** To avoid stomping the errno value on a failed write the lastErrno value
** is set before returning.
*/
static int seekAndWrite(unixFile *id, vedis_int64 offset, const void *pBuf, vedis_int64 cnt){
  int got;
#if (!defined(USE_PREAD) && !defined(USE_PREAD64))
  vedis_int64 newOffset;
#endif
  
#if defined(USE_PREAD)
  got = pwrite(id->h, pBuf, cnt, offset);
#elif defined(USE_PREAD64)
  got = pwrite64(id->h, pBuf, cnt, offset);
#else
  newOffset = lseek(id->h, offset, SEEK_SET);
  if( newOffset!=offset ){
    if( newOffset == -1 ){
      ((unixFile*)id)->lastErrno = errno;
    }else{
      ((unixFile*)id)->lastErrno = 0;			
    }
    return -1;
  }
  got = write(id->h, pBuf, cnt);
#endif
  if( got<0 ){
    ((unixFile*)id)->lastErrno = errno;
  }
  return got;
}
/*
** Write data from a buffer into a file.  Return VEDIS_OK on success
** or some other error code on failure.
*/
static int unixWrite(
  vedis_file *id, 
  const void *pBuf, 
  vedis_int64 amt,
  vedis_int64 offset 
){
  unixFile *pFile = (unixFile*)id;
  int wrote = 0;

  while( amt>0 && (wrote = seekAndWrite(pFile, offset, pBuf, amt))>0 ){
    amt -= wrote;
    offset += wrote;
    pBuf = &((char*)pBuf)[wrote];
  }
  
  if( amt>0 ){
    if( wrote<0 ){
      /* lastErrno set by seekAndWrite */
      return VEDIS_IOERR;
    }else{
      pFile->lastErrno = 0; /* not a system error */
      return VEDIS_FULL;
    }
  }
  return VEDIS_OK;
}
/*
** We do not trust systems to provide a working fdatasync().  Some do.
** Others do no.  To be safe, we will stick with the (slower) fsync().
** If you know that your system does support fdatasync() correctly,
** then simply compile with -Dfdatasync=fdatasync
*/
#if !defined(fdatasync) && !defined(__linux__)
# define fdatasync fsync
#endif

/*
** Define HAVE_FULLFSYNC to 0 or 1 depending on whether or not
** the F_FULLFSYNC macro is defined.  F_FULLFSYNC is currently
** only available on Mac OS X.  But that could change.
*/
#ifdef F_FULLFSYNC
# define HAVE_FULLFSYNC 1
#else
# define HAVE_FULLFSYNC 0
#endif
/*
** The fsync() system call does not work as advertised on many
** unix systems.  The following procedure is an attempt to make
** it work better.
**
**
** SQLite sets the dataOnly flag if the size of the file is unchanged.
** The idea behind dataOnly is that it should only write the file content
** to disk, not the inode.  We only set dataOnly if the file size is 
** unchanged since the file size is part of the inode.  However, 
** Ted Ts'o tells us that fdatasync() will also write the inode if the
** file size has changed.  The only real difference between fdatasync()
** and fsync(), Ted tells us, is that fdatasync() will not flush the
** inode if the mtime or owner or other inode attributes have changed.
** We only care about the file size, not the other file attributes, so
** as far as SQLite is concerned, an fdatasync() is always adequate.
** So, we always use fdatasync() if it is available, regardless of
** the value of the dataOnly flag.
*/
static int full_fsync(int fd, int fullSync, int dataOnly){
  int rc;
#if HAVE_FULLFSYNC
  SXUNUSED(dataOnly);
#else
  SXUNUSED(fullSync);
  SXUNUSED(dataOnly);
#endif

  /* If we compiled with the VEDIS_NO_SYNC flag, then syncing is a
  ** no-op
  */
#if HAVE_FULLFSYNC
  if( fullSync ){
    rc = fcntl(fd, F_FULLFSYNC, 0);
  }else{
    rc = 1;
  }
  /* If the FULLFSYNC failed, fall back to attempting an fsync().
  ** It shouldn't be possible for fullfsync to fail on the local 
  ** file system (on OSX), so failure indicates that FULLFSYNC
  ** isn't supported for this file system. So, attempt an fsync 
  ** and (for now) ignore the overhead of a superfluous fcntl call.  
  ** It'd be better to detect fullfsync support once and avoid 
  ** the fcntl call every time sync is called.
  */
  if( rc ) rc = fsync(fd);

#elif defined(__APPLE__)
  /* fdatasync() on HFS+ doesn't yet flush the file size if it changed correctly
  ** so currently we default to the macro that redefines fdatasync to fsync
  */
  rc = fsync(fd);
#else 
  rc = fdatasync(fd);
#endif /* ifdef VEDIS_NO_SYNC elif HAVE_FULLFSYNC */
  if( rc!= -1 ){
    rc = 0;
  }
  return rc;
}
/*
** Make sure all writes to a particular file are committed to disk.
**
** If dataOnly==0 then both the file itself and its metadata (file
** size, access time, etc) are synced.  If dataOnly!=0 then only the
** file data is synced.
**
** Under Unix, also make sure that the directory entry for the file
** has been created by fsync-ing the directory that contains the file.
** If we do not do this and we encounter a power failure, the directory
** entry for the journal might not exist after we reboot.  The next
** SQLite to access the file will not know that the journal exists (because
** the directory entry for the journal was never created) and the transaction
** will not roll back - possibly leading to database corruption.
*/
static int unixSync(vedis_file *id, int flags){
  int rc;
  unixFile *pFile = (unixFile*)id;

  int isDataOnly = (flags&VEDIS_SYNC_DATAONLY);
  int isFullsync = (flags&0x0F)==VEDIS_SYNC_FULL;

  rc = full_fsync(pFile->h, isFullsync, isDataOnly);

  if( rc ){
    pFile->lastErrno = errno;
    return VEDIS_IOERR;
  }
  if( pFile->dirfd>=0 ){
    int err;
#ifndef VEDIS_DISABLE_DIRSYNC
    /* The directory sync is only attempted if full_fsync is
    ** turned off or unavailable.  If a full_fsync occurred above,
    ** then the directory sync is superfluous.
    */
    if( (!HAVE_FULLFSYNC || !isFullsync) && full_fsync(pFile->dirfd,0,0) ){
       /*
       ** We have received multiple reports of fsync() returning
       ** errors when applied to directories on certain file systems.
       ** A failed directory sync is not a big deal.  So it seems
       ** better to ignore the error.  Ticket #1657
       */
       /* pFile->lastErrno = errno; */
       /* return VEDIS_IOERR; */
    }
#endif
    err = close(pFile->dirfd); /* Only need to sync once, so close the */
    if( err==0 ){              /* directory when we are done */
      pFile->dirfd = -1;
    }else{
      pFile->lastErrno = errno;
      rc = VEDIS_IOERR;
    }
  }
  return rc;
}
/*
** Truncate an open file to a specified size
*/
static int unixTruncate(vedis_file *id, sxi64 nByte){
  unixFile *pFile = (unixFile *)id;
  int rc;

  rc = ftruncate(pFile->h, (off_t)nByte);
  if( rc ){
    pFile->lastErrno = errno;
    return VEDIS_IOERR;
  }else{
    return VEDIS_OK;
  }
}
/*
** Determine the current size of a file in bytes
*/
static int unixFileSize(vedis_file *id,sxi64 *pSize){
  int rc;
  struct stat buf;
  
  rc = fstat(((unixFile*)id)->h, &buf);
  
  if( rc!=0 ){
    ((unixFile*)id)->lastErrno = errno;
    return VEDIS_IOERR;
  }
  *pSize = buf.st_size;

  /* When opening a zero-size database, the findInodeInfo() procedure
  ** writes a single byte into that file in order to work around a bug
  ** in the OS-X msdos filesystem.  In order to avoid problems with upper
  ** layers, we need to report this file size as zero even though it is
  ** really 1.   Ticket #3260.
  */
  if( *pSize==1 ) *pSize = 0;

  return VEDIS_OK;
}
/*
** Return the sector size in bytes of the underlying block device for
** the specified file. This is almost always 512 bytes, but may be
** larger for some devices.
**
** SQLite code assumes this function cannot fail. It also assumes that
** if two files are created in the same file-system directory (i.e.
** a database and its journal file) that the sector size will be the
** same for both.
*/
static int unixSectorSize(vedis_file *NotUsed){
  SXUNUSED(NotUsed);
  return VEDIS_DEFAULT_SECTOR_SIZE;
}
/*
** This vector defines all the methods that can operate on an
** vedis_file for Windows systems.
*/
static const vedis_io_methods unixIoMethod = {
  1,                              /* iVersion */
  unixClose,                       /* xClose */
  unixRead,                        /* xRead */
  unixWrite,                       /* xWrite */
  unixTruncate,                    /* xTruncate */
  unixSync,                        /* xSync */
  unixFileSize,                    /* xFileSize */
  unixLock,                        /* xLock */
  unixUnlock,                      /* xUnlock */
  unixCheckReservedLock,           /* xCheckReservedLock */
  unixSectorSize,                  /* xSectorSize */
};
/****************************************************************************
**************************** vedis_vfs methods ****************************
**
** This division contains the implementation of methods on the
** vedis_vfs object.
*/
/*
** Initialize the contents of the unixFile structure pointed to by pId.
*/
static int fillInUnixFile(
  vedis_vfs *pVfs,      /* Pointer to vfs object */
  int h,                  /* Open file descriptor of file being opened */
  int dirfd,              /* Directory file descriptor */
  vedis_file *pId,      /* Write to the unixFile structure here */
  const char *zFilename,  /* Name of the file being opened */
  int noLock,             /* Omit locking if true */
  int isDelete            /* Delete on close if true */
){
  const vedis_io_methods *pLockingStyle = &unixIoMethod;
  unixFile *pNew = (unixFile *)pId;
  int rc = VEDIS_OK;

  /* Parameter isDelete is only used on vxworks. Express this explicitly 
  ** here to prevent compiler warnings about unused parameters.
  */
  SXUNUSED(isDelete);
  SXUNUSED(noLock);
  SXUNUSED(pVfs);

  pNew->h = h;
  pNew->dirfd = dirfd;
  pNew->fileFlags = 0;
  pNew->zPath = zFilename;
  
  unixEnterMutex();
  rc = findInodeInfo(pNew, &pNew->pInode);
  if( rc!=VEDIS_OK ){
      /* If an error occured in findInodeInfo(), close the file descriptor
      ** immediately, before releasing the mutex. findInodeInfo() may fail
      ** in two scenarios:
      **
      **   (a) A call to fstat() failed.
      **   (b) A malloc failed.
      **
      ** Scenario (b) may only occur if the process is holding no other
      ** file descriptors open on the same file. If there were other file
      ** descriptors on this file, then no malloc would be required by
      ** findInodeInfo(). If this is the case, it is quite safe to close
      ** handle h - as it is guaranteed that no posix locks will be released
      ** by doing so.
      **
      ** If scenario (a) caused the error then things are not so safe. The
      ** implicit assumption here is that if fstat() fails, things are in
      ** such bad shape that dropping a lock or two doesn't matter much.
      */
      close(h);
      h = -1;
  }
  unixLeaveMutex();
  
  pNew->lastErrno = 0;
  if( rc!=VEDIS_OK ){
    if( dirfd>=0 ) close(dirfd); /* silent leak if fail, already in error */
    if( h>=0 ) close(h);
  }else{
    pNew->pMethod = pLockingStyle;
  }
  return rc;
}
/*
** Open a file descriptor to the directory containing file zFilename.
** If successful, *pFd is set to the opened file descriptor and
** VEDIS_OK is returned. If an error occurs, either VEDIS_NOMEM
** or VEDIS_CANTOPEN is returned and *pFd is set to an undefined
** value.
**
** If VEDIS_OK is returned, the caller is responsible for closing
** the file descriptor *pFd using close().
*/
static int openDirectory(const char *zFilename, int *pFd){
  sxu32 ii;
  int fd = -1;
  char zDirname[MAX_PATHNAME+1];
  sxu32 n;
  n = Systrcpy(zDirname,sizeof(zDirname),zFilename,0);
  for(ii=n; ii>1 && zDirname[ii]!='/'; ii--);
  if( ii>0 ){
    zDirname[ii] = '\0';
    fd = open(zDirname, O_RDONLY|O_BINARY, 0);
    if( fd>=0 ){
#ifdef FD_CLOEXEC
      fcntl(fd, F_SETFD, fcntl(fd, F_GETFD, 0) | FD_CLOEXEC);
#endif
    }
  }
  *pFd = fd;
  return (fd>=0?VEDIS_OK: VEDIS_IOERR );
}
/*
** Search for an unused file descriptor that was opened on the database 
** file (not a journal or master-journal file) identified by pathname
** zPath with VEDIS_OPEN_XXX flags matching those passed as the second
** argument to this function.
**
** Such a file descriptor may exist if a database connection was closed
** but the associated file descriptor could not be closed because some
** other file descriptor open on the same file is holding a file-lock.
** Refer to comments in the unixClose() function and the lengthy comment
** describing "Posix Advisory Locking" at the start of this file for 
** further details. Also, ticket #4018.
**
** If a suitable file descriptor is found, then it is returned. If no
** such file descriptor is located, -1 is returned.
*/
static UnixUnusedFd *findReusableFd(const char *zPath, int flags){
  UnixUnusedFd *pUnused = 0;
  struct stat sStat;                   /* Results of stat() call */
  /* A stat() call may fail for various reasons. If this happens, it is
  ** almost certain that an open() call on the same path will also fail.
  ** For this reason, if an error occurs in the stat() call here, it is
  ** ignored and -1 is returned. The caller will try to open a new file
  ** descriptor on the same path, fail, and return an error to SQLite.
  **
  ** Even if a subsequent open() call does succeed, the consequences of
  ** not searching for a resusable file descriptor are not dire.  */
  if( 0==stat(zPath, &sStat) ){
    unixInodeInfo *pInode;

    unixEnterMutex();
    pInode = inodeList;
    while( pInode && (pInode->fileId.dev!=sStat.st_dev
                     || pInode->fileId.ino!=sStat.st_ino) ){
       pInode = pInode->pNext;
    }
    if( pInode ){
      UnixUnusedFd **pp;
      for(pp=&pInode->pUnused; *pp && (*pp)->flags!=flags; pp=&((*pp)->pNext));
      pUnused = *pp;
      if( pUnused ){
        *pp = pUnused->pNext;
      }
    }
    unixLeaveMutex();
  }
  return pUnused;
}
/*
** This function is called by unixOpen() to determine the unix permissions
** to create new files with. If no error occurs, then VEDIS_OK is returned
** and a value suitable for passing as the third argument to open(2) is
** written to *pMode. If an IO error occurs, an SQLite error code is 
** returned and the value of *pMode is not modified.
**
** If the file being opened is a temporary file, it is always created with
** the octal permissions 0600 (read/writable by owner only). If the file
** is a database or master journal file, it is created with the permissions 
** mask VEDIS_DEFAULT_FILE_PERMISSIONS.
**
** Finally, if the file being opened is a WAL or regular journal file, then 
** this function queries the file-system for the permissions on the 
** corresponding database file and sets *pMode to this value. Whenever 
** possible, WAL and journal files are created using the same permissions 
** as the associated database file.
*/
static int findCreateFileMode(
  const char *zPath,              /* Path of file (possibly) being created */
  int flags,                      /* Flags passed as 4th argument to xOpen() */
  mode_t *pMode                   /* OUT: Permissions to open file with */
){
  int rc = VEDIS_OK;             /* Return Code */
  if( flags & VEDIS_OPEN_TEMP_DB ){
    *pMode = 0600;
     SXUNUSED(zPath);
  }else{
    *pMode = VEDIS_DEFAULT_FILE_PERMISSIONS;
  }
  return rc;
}
/*
** Open the file zPath.
** 
** Previously, the SQLite OS layer used three functions in place of this
** one:
**
**     vedisOsOpenReadWrite();
**     vedisOsOpenReadOnly();
**     vedisOsOpenExclusive();
**
** These calls correspond to the following combinations of flags:
**
**     ReadWrite() ->     (READWRITE | CREATE)
**     ReadOnly()  ->     (READONLY) 
**     OpenExclusive() -> (READWRITE | CREATE | EXCLUSIVE)
**
** The old OpenExclusive() accepted a boolean argument - "delFlag". If
** true, the file was configured to be automatically deleted when the
** file handle closed. To achieve the same effect using this new 
** interface, add the DELETEONCLOSE flag to those specified above for 
** OpenExclusive().
*/
static int unixOpen(
  vedis_vfs *pVfs,           /* The VFS for which this is the xOpen method */
  const char *zPath,           /* Pathname of file to be opened */
  vedis_file *pFile,         /* The file descriptor to be filled in */
  unsigned int flags           /* Input flags to control the opening */
){
  unixFile *p = (unixFile *)pFile;
  int fd = -1;                   /* File descriptor returned by open() */
  int dirfd = -1;                /* Directory file descriptor */
  int openFlags = 0;             /* Flags to pass to open() */
  int noLock;                    /* True to omit locking primitives */
  int rc = VEDIS_OK;            /* Function Return Code */
  UnixUnusedFd *pUnused;
  int isExclusive  = (flags & VEDIS_OPEN_EXCLUSIVE);
  int isDelete     = (flags & VEDIS_OPEN_TEMP_DB);
  int isCreate     = (flags & VEDIS_OPEN_CREATE);
  int isReadonly   = (flags & VEDIS_OPEN_READONLY);
  int isReadWrite  = (flags & VEDIS_OPEN_READWRITE);
  /* If creating a master or main-file journal, this function will open
  ** a file-descriptor on the directory too. The first time unixSync()
  ** is called the directory file descriptor will be fsync()ed and close()d.
  */
  int isOpenDirectory = isCreate ;
  const char *zName = zPath;

  SyZero(p,sizeof(unixFile));
  
  pUnused = findReusableFd(zName, flags);
  if( pUnused ){
	  fd = pUnused->fd;
  }else{
	  pUnused = vedis_malloc(sizeof(*pUnused));
      if( !pUnused ){
        return VEDIS_NOMEM;
      }
  }
  p->pUnused = pUnused;
  
  /* Determine the value of the flags parameter passed to POSIX function
  ** open(). These must be calculated even if open() is not called, as
  ** they may be stored as part of the file handle and used by the 
  ** 'conch file' locking functions later on.  */
  if( isReadonly )  openFlags |= O_RDONLY;
  if( isReadWrite ) openFlags |= O_RDWR;
  if( isCreate )    openFlags |= O_CREAT;
  if( isExclusive ) openFlags |= (O_EXCL|O_NOFOLLOW);
  openFlags |= (O_LARGEFILE|O_BINARY);

  if( fd<0 ){
    mode_t openMode;              /* Permissions to create file with */
    rc = findCreateFileMode(zName, flags, &openMode);
    if( rc!=VEDIS_OK ){
      return rc;
    }
    fd = open(zName, openFlags, openMode);
    if( fd<0 ){
	  rc = VEDIS_IOERR;
      goto open_finished;
    }
  }
  
  if( p->pUnused ){
    p->pUnused->fd = fd;
    p->pUnused->flags = flags;
  }

  if( isDelete ){
    unlink(zName);
  }

  if( isOpenDirectory ){
    rc = openDirectory(zPath, &dirfd);
    if( rc!=VEDIS_OK ){
      /* It is safe to close fd at this point, because it is guaranteed not
      ** to be open on a database file. If it were open on a database file,
      ** it would not be safe to close as this would release any locks held
      ** on the file by this process.  */
      close(fd);             /* silently leak if fail, already in error */
      goto open_finished;
    }
  }

#ifdef FD_CLOEXEC
  fcntl(fd, F_SETFD, fcntl(fd, F_GETFD, 0) | FD_CLOEXEC);
#endif

  noLock = 0;

#if defined(__APPLE__) 
  struct statfs fsInfo;
  if( fstatfs(fd, &fsInfo) == -1 ){
    ((unixFile*)pFile)->lastErrno = errno;
    if( dirfd>=0 ) close(dirfd); /* silently leak if fail, in error */
    close(fd); /* silently leak if fail, in error */
    return VEDIS_IOERR;
  }
  if (0 == SyStrncmp("msdos", fsInfo.f_fstypename, 5)) {
    ((unixFile*)pFile)->fsFlags |= VEDIS_FSFLAGS_IS_MSDOS;
  }
#endif
  
  rc = fillInUnixFile(pVfs, fd, dirfd, pFile, zPath, noLock, isDelete);
open_finished:
  if( rc!=VEDIS_OK ){
    vedis_free(p->pUnused);
  }
  return rc;
}
/*
** Delete the file at zPath. If the dirSync argument is true, fsync()
** the directory after deleting the file.
*/
static int unixDelete(
  vedis_vfs *NotUsed,     /* VFS containing this as the xDelete method */
  const char *zPath,        /* Name of file to be deleted */
  int dirSync               /* If true, fsync() directory after deleting file */
){
  int rc = VEDIS_OK;
  SXUNUSED(NotUsed);
  
  if( unlink(zPath)==(-1) && errno!=ENOENT ){
	  return VEDIS_IOERR;
  }
#ifndef VEDIS_DISABLE_DIRSYNC
  if( dirSync ){
    int fd;
    rc = openDirectory(zPath, &fd);
    if( rc==VEDIS_OK ){
      if( fsync(fd) )
      {
        rc = VEDIS_IOERR;
      }
      if( close(fd) && !rc ){
        rc = VEDIS_IOERR;
      }
    }
  }
#endif
  return rc;
}
/*
** Sleep for a little while.  Return the amount of time slept.
** The argument is the number of microseconds we want to sleep.
** The return value is the number of microseconds of sleep actually
** requested from the underlying operating system, a number which
** might be greater than or equal to the argument, but not less
** than the argument.
*/
static int unixSleep(vedis_vfs *NotUsed, int microseconds)
{
#if defined(HAVE_USLEEP) && HAVE_USLEEP
  usleep(microseconds);
  SXUNUSED(NotUsed);
  return microseconds;
#else
  int seconds = (microseconds+999999)/1000000;
  SXUNUSED(NotUsed);
  sleep(seconds);
  return seconds*1000000;
#endif
}
/*
 * Export the current system time.
 */
static int unixCurrentTime(vedis_vfs *pVfs,Sytm *pOut)
{
	struct tm *pTm;
	time_t tt;
	SXUNUSED(pVfs);
	time(&tt);
	pTm = gmtime(&tt);
	if( pTm ){ /* Yes, it can fail */
		STRUCT_TM_TO_SYTM(pTm,pOut);
	}
	return VEDIS_OK;
}
/*
** Test the existance of or access permissions of file zPath. The
** test performed depends on the value of flags:
**
**     VEDIS_ACCESS_EXISTS: Return 1 if the file exists
**     VEDIS_ACCESS_READWRITE: Return 1 if the file is read and writable.
**     VEDIS_ACCESS_READONLY: Return 1 if the file is readable.
**
** Otherwise return 0.
*/
static int unixAccess(
  vedis_vfs *NotUsed,   /* The VFS containing this xAccess method */
  const char *zPath,      /* Path of the file to examine */
  int flags,              /* What do we want to learn about the zPath file? */
  int *pResOut            /* Write result boolean here */
){
  int amode = 0;
  SXUNUSED(NotUsed);
  switch( flags ){
    case VEDIS_ACCESS_EXISTS:
      amode = F_OK;
      break;
    case VEDIS_ACCESS_READWRITE:
      amode = W_OK|R_OK;
      break;
    case VEDIS_ACCESS_READ:
      amode = R_OK;
      break;
    default:
		/* Can't happen */
      break;
  }
  *pResOut = (access(zPath, amode)==0);
  if( flags==VEDIS_ACCESS_EXISTS && *pResOut ){
    struct stat buf;
    if( 0==stat(zPath, &buf) && buf.st_size==0 ){
      *pResOut = 0;
    }
  }
  return VEDIS_OK;
}
/*
** Turn a relative pathname into a full pathname. The relative path
** is stored as a nul-terminated string in the buffer pointed to by
** zPath. 
**
** zOut points to a buffer of at least vedis_vfs.mxPathname bytes 
** (in this case, MAX_PATHNAME bytes). The full-path is written to
** this buffer before returning.
*/
static int unixFullPathname(
  vedis_vfs *pVfs,            /* Pointer to vfs object */
  const char *zPath,            /* Possibly relative input path */
  int nOut,                     /* Size of output buffer in bytes */
  char *zOut                    /* Output buffer */
){
  if( zPath[0]=='/' ){
	  Systrcpy(zOut,(sxu32)nOut,zPath,0);
	  SXUNUSED(pVfs);
  }else{
    sxu32 nCwd;
	zOut[nOut-1] = '\0';
    if( getcwd(zOut, nOut-1)==0 ){
		return VEDIS_IOERR;
    }
    nCwd = SyStrlen(zOut);
    SyBufferFormat(&zOut[nCwd],(sxu32)nOut-nCwd,"/%s",zPath);
  }
  return VEDIS_OK;
}
/* int (*xMmap)(const char *, void **, vedis_int64 *) */
static int UnixMmap(const char *zPath, void **ppMap, vedis_int64 *pSize)
{
	struct stat st;
	void *pMap;
	int fd;
	int rc;
	/* Open the file in a read-only mode */
	fd = open(zPath, O_RDONLY);
	if( fd < 0 ){
		return -1;
	}
	/* stat the handle */
	fstat(fd, &st);
	/* Obtain a memory view of the whole file */
	pMap = mmap(0, st.st_size, PROT_READ, MAP_PRIVATE|MAP_FILE, fd, 0);
	rc = VEDIS_OK;
	if( pMap == MAP_FAILED ){
		rc = -1;
	}else{
		/* Point to the memory view */
		*ppMap = pMap;
		*pSize = (vedis_int64)st.st_size;
	}
	close(fd);
	return rc;
}
/* void (*xUnmap)(void *, vedis_int64)  */
static void UnixUnmap(void *pView, vedis_int64 nSize)
{
	munmap(pView, (size_t)nSize);
}
/*
 * Export the Unix Vfs.
 */
VEDIS_PRIVATE const vedis_vfs * vedisExportBuiltinVfs(void)
{
	static const vedis_vfs sUnixvfs = {
		"Unix",              /* Vfs name */
		1,                   /* Vfs structure version */
		sizeof(unixFile),    /* szOsFile */
		MAX_PATHNAME,        /* mxPathName */
		unixOpen,            /* xOpen */
		unixDelete,          /* xDelete */
		unixAccess,          /* xAccess */
		unixFullPathname,    /* xFullPathname */
		0,                   /* xTmp */
		unixSleep,           /* xSleep */
		unixCurrentTime,     /* xCurrentTime */
		0,                   /* xGetLastError */
		UnixMmap,            /* xMmap */
		UnixUnmap            /* xUnmap */
	};
	return &sUnixvfs;
}

#endif /* __UNIXES__ */

/*
 * ----------------------------------------------------------
 * File: os.c
 * MD5: b04f646dc8cef1afabca3f8c053f648b
 * ----------------------------------------------------------
 */
/*
 * Symisc Vedis: An Embeddable NoSQL (Post Modern) Database Engine.
 * Copyright (C) 2012-2013, Symisc Systems http://vedis.org/
 * Version 1.1.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.org/licensing.html
 */
 /* $SymiscID: os.c v1.0 FreeBSD 2012-11-12 21:27 devel <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* OS interfaces abstraction layers: Mostly SQLite3 source tree */
/*
** The following routines are convenience wrappers around methods
** of the vedis_file object.  This is mostly just syntactic sugar. All
** of this would be completely automatic if UnQLite were coded using
** C++ instead of plain old C.
*/
VEDIS_PRIVATE int vedisOsRead(vedis_file *id, void *pBuf, vedis_int64 amt, vedis_int64 offset)
{
  return id->pMethods->xRead(id, pBuf, amt, offset);
}
VEDIS_PRIVATE int vedisOsWrite(vedis_file *id, const void *pBuf, vedis_int64 amt, vedis_int64 offset)
{
  return id->pMethods->xWrite(id, pBuf, amt, offset);
}
VEDIS_PRIVATE int vedisOsTruncate(vedis_file *id, vedis_int64 size)
{
  return id->pMethods->xTruncate(id, size);
}
VEDIS_PRIVATE int vedisOsSync(vedis_file *id, int flags)
{
  return id->pMethods->xSync(id, flags);
}
VEDIS_PRIVATE int vedisOsFileSize(vedis_file *id, vedis_int64 *pSize)
{
  return id->pMethods->xFileSize(id, pSize);
}
VEDIS_PRIVATE int vedisOsLock(vedis_file *id, int lockType)
{
  return id->pMethods->xLock(id, lockType);
}
VEDIS_PRIVATE int vedisOsUnlock(vedis_file *id, int lockType)
{
  return id->pMethods->xUnlock(id, lockType);
}
VEDIS_PRIVATE int vedisOsCheckReservedLock(vedis_file *id, int *pResOut)
{
  return id->pMethods->xCheckReservedLock(id, pResOut);
}
VEDIS_PRIVATE int vedisOsSectorSize(vedis_file *id)
{
  if( id->pMethods->xSectorSize ){
	  return id->pMethods->xSectorSize(id);
  }
  return  VEDIS_DEFAULT_SECTOR_SIZE;
}
/*
** The next group of routines are convenience wrappers around the
** VFS methods.
*/
VEDIS_PRIVATE int vedisOsOpen(
  vedis_vfs *pVfs,
  SyMemBackend *pAlloc,
  const char *zPath, 
  vedis_file **ppOut, 
  unsigned int flags 
)
{
	vedis_file *pFile;
	int rc;
	*ppOut = 0;
	if( zPath == 0 ){
		/* May happen if dealing with an in-memory database */
		return SXERR_EMPTY;
	}
	/* Allocate a new instance */
	pFile = (vedis_file *)SyMemBackendAlloc(pAlloc,sizeof(vedis_file)+pVfs->szOsFile);
	if( pFile == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pFile,sizeof(vedis_file)+pVfs->szOsFile);
	/* Invoke the xOpen method of the underlying VFS */
	rc = pVfs->xOpen(pVfs, zPath, pFile, flags);
	if( rc != VEDIS_OK ){
		SyMemBackendFree(pAlloc,pFile);
		pFile = 0;
	}
	*ppOut = pFile;
	return rc;
}
VEDIS_PRIVATE int vedisOsCloseFree(SyMemBackend *pAlloc,vedis_file *pId)
{
	int rc = VEDIS_OK;
	if( pId ){
		rc = pId->pMethods->xClose(pId);
		SyMemBackendFree(pAlloc,pId);
	}
	return rc;
}
VEDIS_PRIVATE int vedisOsDelete(vedis_vfs *pVfs, const char *zPath, int dirSync){
  return pVfs->xDelete(pVfs, zPath, dirSync);
}
VEDIS_PRIVATE int vedisOsAccess(
  vedis_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  return pVfs->xAccess(pVfs, zPath, flags, pResOut);
}
/*
 * ----------------------------------------------------------
 * File: obj.c
 * MD5: 5d0b5f8c634519f435585ccae3a25ef7
 * ----------------------------------------------------------
 */
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
/* $SymiscID: obj.c v1.6 Linux 2013-07-10 03:52 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* This file manage low-level stuff related to indexed memory objects [i.e: vedis_value] */
/*
 * Notes on memory objects [i.e: vedis_value].
 * Internally, the VEDIS engine manipulates nearly all VEDIS values
 * [i.e: string, int, float, resource, object, bool, null..] as vedis_values structures.
 * Each vedis_values struct may cache multiple representations (string, 
 * integer etc.) of the same value.
 */
/*
 * Convert a 64-bit IEEE double into a 64-bit signed integer.
 * If the double is too large, return 0x8000000000000000.
 *
 * Most systems appear to do this simply by assigning ariables and without
 * the extra range tests.
 * But there are reports that windows throws an expection if the floating 
 * point value is out of range.
 */
static sxi64 MemObjRealToInt(vedis_value *pObj)
{
#ifdef VEDIS_OMIT_FLOATING_POINT
	/* Real and 64bit integer are the same when floating point arithmetic
	 * is omitted from the build.
	 */
	return pObj->x.rVal;
#else
 /*
  ** Many compilers we encounter do not define constants for the
  ** minimum and maximum 64-bit integers, or they define them
  ** inconsistently.  And many do not understand the "LL" notation.
  ** So we define our own static constants here using nothing
  ** larger than a 32-bit integer constant.
  */
  static const sxi64 maxInt = LARGEST_INT64;
  static const sxi64 minInt = SMALLEST_INT64;
  vedis_real r = pObj->x.rVal;
  if( r<(vedis_real)minInt ){
    return minInt;
  }else if( r>(vedis_real)maxInt ){
    /* minInt is correct here - not maxInt.  It turns out that assigning
    ** a very large positive number to an integer results in a very large
    ** negative integer.  This makes no sense, but it is what x86 hardware
    ** does so for compatibility we will do the same in software. */
    return minInt;
  }else{
    return (sxi64)r;
  }
#endif
}
/*
 * Convert a raw token value typically a stream of digit [i.e: hex, octal, binary or decimal] 
 * to a 64-bit integer.
 */
VEDIS_PRIVATE sxi64 vedisTokenValueToInt64(SyString *pVal)
{
	sxi64 iVal = 0;
	if( pVal->nByte <= 0 ){
		return 0;
	}
	if( pVal->zString[0] == '0' ){
		sxi32 c;
		if( pVal->nByte == sizeof(char) ){
			return 0;
		}
		c = pVal->zString[1];
		if( c  == 'x' || c == 'X' ){
			/* Hex digit stream */
			SyHexStrToInt64(pVal->zString, pVal->nByte, (void *)&iVal, 0);
		}else if( c == 'b' || c == 'B' ){
			/* Binary digit stream */
			SyBinaryStrToInt64(pVal->zString, pVal->nByte, (void *)&iVal, 0);
		}else{
			/* Octal digit stream */
			SyOctalStrToInt64(pVal->zString, pVal->nByte, (void *)&iVal, 0);
		}
	}else{
		/* Decimal digit stream */
		SyStrToInt64(pVal->zString, pVal->nByte, (void *)&iVal, 0);
	}
	return iVal;
}
/*
 * Return some kind of 64-bit integer value which is the best we can
 * do at representing the value that pObj describes as a string
 * representation.
 */
static sxi64 MemObjStringToInt(vedis_value *pObj)
{
	SyString sVal;
	SyStringInitFromBuf(&sVal, SyBlobData(&pObj->sBlob), SyBlobLength(&pObj->sBlob));
	return vedisTokenValueToInt64(&sVal);	
}
/*
 * Return some kind of integer value which is the best we can
 * do at representing the value that pObj describes as an integer.
 * If pObj is an integer, then the value is exact. If pObj is
 * a floating-point then  the value returned is the integer part.
 * If pObj is a string, then we make an attempt to convert it into
 * a integer and return that. 
 * If pObj represents a NULL value, return 0.
 */
static sxi64 MemObjIntValue(vedis_value *pObj)
{
	sxi32 iFlags;
	iFlags = pObj->iFlags;
	if (iFlags & MEMOBJ_REAL ){
		return MemObjRealToInt(&(*pObj));
	}else if( iFlags & (MEMOBJ_INT|MEMOBJ_BOOL) ){
		return pObj->x.iVal;
	}else if (iFlags & MEMOBJ_STRING) {
		return MemObjStringToInt(&(*pObj));
	}else if( iFlags & MEMOBJ_NULL ){
		return 0;
	}else if( iFlags & MEMOBJ_HASHMAP ){
		vedis_hashmap *pMap = (vedis_hashmap *)pObj->x.pOther;
		sxu32 n = vedisHashmapCount(pMap);
		vedisHashmapUnref(pMap);
		/* Return total number of entries in the hashmap */
		return n; 
	}
	/* CANT HAPPEN */
	return 0;
}
/*
 * Return some kind of real value which is the best we can
 * do at representing the value that pObj describes as a real.
 * If pObj is a real, then the value is exact.If pObj is an
 * integer then the integer  is promoted to real and that value
 * is returned.
 * If pObj is a string, then we make an attempt to convert it
 * into a real and return that. 
 * If pObj represents a NULL value, return 0.0
 */
static vedis_real MemObjRealValue(vedis_value *pObj)
{
	sxi32 iFlags;
	iFlags = pObj->iFlags;
	if( iFlags & MEMOBJ_REAL ){
		return pObj->x.rVal;
	}else if (iFlags & (MEMOBJ_INT|MEMOBJ_BOOL) ){
		return (vedis_real)pObj->x.iVal;
	}else if (iFlags & MEMOBJ_STRING){
		SyString sString;
#ifdef VEDIS_OMIT_FLOATING_POINT
		vedis_real rVal = 0;
#else
		vedis_real rVal = 0.0;
#endif
		SyStringInitFromBuf(&sString, SyBlobData(&pObj->sBlob), SyBlobLength(&pObj->sBlob));
		if( SyBlobLength(&pObj->sBlob) > 0 ){
			/* Convert as much as we can */
#ifdef VEDIS_OMIT_FLOATING_POINT
			rVal = MemObjStringToInt(&(*pObj));
#else
			SyStrToReal(sString.zString, sString.nByte, (void *)&rVal, 0);
#endif
		}
		return rVal;
	}else if( iFlags & MEMOBJ_NULL ){
#ifdef VEDIS_OMIT_FLOATING_POINT
		return 0;
#else
		return 0.0;
#endif
	}else if( iFlags & MEMOBJ_HASHMAP ){
		/* Return the total number of entries in the hashmap */
		vedis_hashmap *pMap = (vedis_hashmap *)pObj->x.pOther;
		vedis_real n = (vedis_real)vedisHashmapCount(pMap);
		vedisHashmapUnref(pMap);
		return n;
	}
	/* NOT REACHED  */
	return 0;
}
/* 
 * Return the string representation of a given vedis_value.
 * This function never fail and always return SXRET_OK.
 */
static sxi32 MemObjStringValue(SyBlob *pOut,vedis_value *pObj)
{
	if( pObj->iFlags & MEMOBJ_REAL ){
		SyBlobFormat(&(*pOut), "%.15g", pObj->x.rVal);
	}else if( pObj->iFlags & MEMOBJ_INT ){
		SyBlobFormat(&(*pOut), "%qd", pObj->x.iVal);
		/* %qd (BSD quad) is equivalent to %lld in the libc printf */
	}else if( pObj->iFlags & MEMOBJ_BOOL ){
		if( pObj->x.iVal ){
			SyBlobAppend(&(*pOut),"true", sizeof("true")-1);
		}else{
			SyBlobAppend(&(*pOut),"false", sizeof("false")-1);
		}
	}else if( pObj->iFlags & MEMOBJ_HASHMAP ){
		/* Serialize JSON object or array */
		vedisJsonSerialize(pObj,pOut);
		vedisHashmapUnref((vedis_hashmap *)pObj->x.pOther);
	}
	return SXRET_OK;
}
/*
 * Return some kind of boolean value which is the best we can do
 * at representing the value that pObj describes as a boolean.
 * When converting to boolean, the following values are considered FALSE:
 * NULL
 * the boolean FALSE itself.
 * the integer 0 (zero).
 * the real 0.0 (zero).
 * the empty string, a stream of zero [i.e: "0", "00", "000", ...] and the string
 * "false".
 * an array with zero elements. 
 */
static sxi32 MemObjBooleanValue(vedis_value *pObj)
{
	sxi32 iFlags;	
	iFlags = pObj->iFlags;
	if (iFlags & MEMOBJ_REAL ){
#ifdef VEDIS_OMIT_FLOATING_POINT
		return pObj->x.rVal ? 1 : 0;
#else
		return pObj->x.rVal != 0.0 ? 1 : 0;
#endif
	}else if( iFlags & MEMOBJ_INT ){
		return pObj->x.iVal ? 1 : 0;
	}else if (iFlags & MEMOBJ_STRING) {
		SyString sString;
		SyStringInitFromBuf(&sString, SyBlobData(&pObj->sBlob), SyBlobLength(&pObj->sBlob));
		if( sString.nByte == 0 ){
			/* Empty string */
			return 0;
		}else if( (sString.nByte == sizeof("true") - 1 && SyStrnicmp(sString.zString, "true", sizeof("true")-1) == 0) ||
			(sString.nByte == sizeof("on") - 1 && SyStrnicmp(sString.zString, "on", sizeof("on")-1) == 0) ||
			(sString.nByte == sizeof("yes") - 1 && SyStrnicmp(sString.zString, "yes", sizeof("yes")-1) == 0) ){
				return 1;
		}else if( sString.nByte == sizeof("false") - 1 && SyStrnicmp(sString.zString, "false", sizeof("false")-1) == 0 ){
			return 0;
		}else{
			const char *zIn, *zEnd;
			zIn = sString.zString;
			zEnd = &zIn[sString.nByte];
			while( zIn < zEnd && zIn[0] == '0' ){
				zIn++;
			}
			return zIn >= zEnd ? 0 : 1;
		}
	}else if( iFlags & MEMOBJ_NULL ){
		return 0;
	}else if( iFlags & MEMOBJ_HASHMAP ){
		vedis_hashmap *pMap = (vedis_hashmap *)pObj->x.pOther;
		sxu32 n = vedisHashmapCount(pMap);
		vedisHashmapUnref(pMap);
		return n > 0 ? TRUE : FALSE;
	}
	/* NOT REACHED */
	return 0;
}
/*
 * If the vedis_value is of type real, try to make it an integer also.
 */
static sxi32 MemObjTryIntger(vedis_value *pObj)
{
	sxi64 iVal = MemObjRealToInt(&(*pObj));
  /* Only mark the value as an integer if
  **
  **    (1) the round-trip conversion real->int->real is a no-op, and
  **    (2) The integer is neither the largest nor the smallest
  **        possible integer
  **
  ** The second and third terms in the following conditional enforces
  ** the second condition under the assumption that addition overflow causes
  ** values to wrap around.  On x86 hardware, the third term is always
  ** true and could be omitted.  But we leave it in because other
  ** architectures might behave differently.
  */
	if( pObj->x.rVal ==(vedis_real)iVal && iVal>SMALLEST_INT64 && iVal<LARGEST_INT64 ){
		pObj->x.iVal = iVal; 
		pObj->iFlags = MEMOBJ_INT;
	}
	return SXRET_OK;
}
/*
 * Check whether the vedis_value is numeric [i.e: int/float/bool] or looks
 * like a numeric number [i.e: if the vedis_value is of type string.].
 * Return TRUE if numeric.FALSE otherwise.
 */
VEDIS_PRIVATE sxi32 vedisMemObjIsNumeric(vedis_value *pObj)
{
	if( pObj->iFlags & ( MEMOBJ_BOOL|MEMOBJ_INT|MEMOBJ_REAL) ){
		return TRUE;
	}else if( pObj->iFlags & (MEMOBJ_NULL|MEMOBJ_HASHMAP) ){
		return FALSE;
	}else if( pObj->iFlags & MEMOBJ_STRING ){
		SyString sStr;
		sxi32 rc;
		SyStringInitFromBuf(&sStr, SyBlobData(&pObj->sBlob), SyBlobLength(&pObj->sBlob));
		if( sStr.nByte <= 0 ){
			/* Empty string */
			return FALSE;
		}
		/* Check if the string representation looks like a numeric number */
		rc = SyStrIsNumeric(sStr.zString, sStr.nByte, 0, 0);
		return rc == SXRET_OK ? TRUE : FALSE;
	}
	/* NOT REACHED */
	return FALSE;
}
/*
 * Convert a vedis_value to type integer.Invalidate any prior representations.
 */
VEDIS_PRIVATE sxi32 vedisMemObjToInteger(vedis_value *pObj)
{
	if( (pObj->iFlags & MEMOBJ_INT) == 0 ){
		/* Preform the conversion */
		pObj->x.iVal = MemObjIntValue(&(*pObj));
		/* Invalidate any prior representations */
		SyBlobRelease(&pObj->sBlob);
		MemObjSetType(pObj, MEMOBJ_INT);
	}
	return SXRET_OK;
}
/*
 * Try a get an integer representation of the given vedis_value.
 * If the vedis_value is not of type real, this function is a no-op.
 */
VEDIS_PRIVATE sxi32 vedisMemObjTryInteger(vedis_value *pObj)
{
	if( pObj->iFlags & MEMOBJ_REAL ){
		/* Work only with reals */
		MemObjTryIntger(&(*pObj));
	}
	return SXRET_OK;
}
/*
 * Convert a vedis_value to type real (Try to get an integer representation also).
 * Invalidate any prior representations
 */
VEDIS_PRIVATE sxi32 vedisMemObjToReal(vedis_value *pObj)
{
	if((pObj->iFlags & MEMOBJ_REAL) == 0 ){
		/* Preform the conversion */
		pObj->x.rVal = MemObjRealValue(&(*pObj));
		/* Invalidate any prior representations */
		SyBlobRelease(&pObj->sBlob);
		MemObjSetType(pObj, MEMOBJ_REAL);
	}
	return SXRET_OK;
}
/*
 * Convert a vedis_value to type boolean.Invalidate any prior representations.
 */
VEDIS_PRIVATE sxi32 vedisMemObjToBool(vedis_value *pObj)
{
	if( (pObj->iFlags & MEMOBJ_BOOL) == 0 ){
		/* Preform the conversion */
		pObj->x.iVal = MemObjBooleanValue(&(*pObj));
		/* Invalidate any prior representations */
		SyBlobRelease(&pObj->sBlob);
		MemObjSetType(pObj, MEMOBJ_BOOL);
	}
	return SXRET_OK;
}
/*
 * Convert a vedis_value to type string. Prior representations are NOT invalidated.
 */
VEDIS_PRIVATE sxi32 vedisMemObjToString(vedis_value *pObj)
{
	sxi32 rc = SXRET_OK;
	if( (pObj->iFlags & MEMOBJ_STRING) == 0 ){
		/* Perform the conversion */
		SyBlobReset(&pObj->sBlob); /* Reset the internal buffer */
		rc = MemObjStringValue(&pObj->sBlob, &(*pObj));
		MemObjSetType(pObj, MEMOBJ_STRING);
	}
	return rc;
}
/*
 * Nullify a vedis_value.In other words invalidate any prior
 * representation.
 */
VEDIS_PRIVATE sxi32 vedisMemObjToNull(vedis_value *pObj)
{
	return vedisMemObjRelease(pObj);
}
/*
 * Invalidate any prior representation of a given vedis_value.
 */
VEDIS_PRIVATE sxi32 vedisMemObjRelease(vedis_value *pObj)
{
	if( (pObj->iFlags & MEMOBJ_NULL) == 0 ){
		if( pObj->iFlags & MEMOBJ_HASHMAP ){
			vedisHashmapUnref((vedis_hashmap *)pObj->x.pOther);
		}
		/* Release the internal buffer */
		SyBlobRelease(&pObj->sBlob);
		/* Invalidate any prior representation */
		pObj->iFlags = MEMOBJ_NULL;
	}
	return SXRET_OK;
}
/*
 * Duplicate the contents of a vedis_value.
 */
VEDIS_PRIVATE sxi32 vedisMemObjStore(vedis_value *pSrc,vedis_value *pDest)
{
	vedis_hashmap *pMap = 0;
	sxi32 rc;
	if( pSrc->iFlags & MEMOBJ_HASHMAP ){
		/* Increment reference count */
		vedisHashmapRef((vedis_hashmap *)pSrc->x.pOther);
	}
	if( pDest->iFlags & MEMOBJ_HASHMAP ){
		pMap = (vedis_hashmap *)pDest->x.pOther;
	}
	SyMemcpy((const void *)&(*pSrc), &(*pDest), sizeof(vedis_value)-sizeof(SyBlob));
	rc = SXRET_OK;
	if( SyBlobLength(&pSrc->sBlob) > 0 ){
		SyBlobReset(&pDest->sBlob);
		rc = SyBlobDup(&pSrc->sBlob, &pDest->sBlob);
	}else{
		if( SyBlobLength(&pDest->sBlob) > 0 ){
			SyBlobRelease(&pDest->sBlob);
		}
	}
	if( pMap ){
		vedisHashmapUnref(pMap);
	}
	return rc;
}
VEDIS_PRIVATE void vedisMemObjInit(vedis *pVedis,vedis_value *pObj)
{
	/* Zero the structure */
	SyZero(pObj,sizeof(vedis_value));
	/* Init */
	SyBlobInit(&pObj->sBlob,&pVedis->sMem);
	/* Set the NULL type */
	pObj->iFlags = MEMOBJ_NULL;
}
/*
 * Initialize a vedis_value to the integer type.
 */
VEDIS_PRIVATE sxi32 vedisMemObjInitFromInt(vedis *pStore, vedis_value *pObj, sxi64 iVal)
{
	/* Zero the structure */
	SyZero(pObj, sizeof(vedis_value));
	/* Initialize fields */
	SyBlobInit(&pObj->sBlob, &pStore->sMem);
	/* Set the desired type */
	pObj->x.iVal = iVal;
	pObj->iFlags = MEMOBJ_INT;
	return SXRET_OK;
}
/*
 * Initialize a vedis_value to the string type.
 */
VEDIS_PRIVATE sxi32 vedisMemObjInitFromString(vedis *pStore, vedis_value *pObj, const SyString *pVal)
{
	/* Zero the structure */
	SyZero(pObj, sizeof(vedis_value));
	/* Initialize fields */
	SyBlobInit(&pObj->sBlob, &pStore->sMem);
	if( pVal && pVal->nByte > 0){
		/* Append contents */
		SyBlobAppend(&pObj->sBlob, (const void *)pVal->zString, pVal->nByte);
	}
	/* Set the desired type */
	pObj->iFlags = MEMOBJ_STRING;
	return SXRET_OK;
}
VEDIS_PRIVATE vedis_value * vedisNewObjectValue(vedis *pVedis,SyToken *pToken)
{
	vedis_value *pObj;
	/* Allocate a new instance */
	pObj = (vedis_value *)SyMemBackendPoolAlloc(&pVedis->sMem,sizeof(vedis_value));
	if( pObj == 0 ){
		return 0;
	}
	if( pToken ){
		SyString *pValue = &pToken->sData;
		/* Switch to the appropriate type */
		vedisMemObjInitFromString(pVedis,pObj,pValue);
		if( pToken->nType & VEDIS_TK_INTEGER ){
			vedisMemObjToInteger(pObj);
		}else if( pToken->nType & VEDIS_TK_REAL ){
			vedisMemObjToReal(pObj);
		}
	}else{
		/* Default to nil */
		vedisMemObjInit(pVedis,pObj);
	}
	return pObj;
}
VEDIS_PRIVATE vedis_value * vedisNewObjectArrayValue(vedis *pVedis)
{
	vedis_hashmap *pMap;
	vedis_value *pObj;
	/* Allocate a new instance */
	pObj = (vedis_value *)SyMemBackendPoolAlloc(&pVedis->sMem,sizeof(vedis_value));
	if( pObj == 0 ){
		return 0;
	}
	vedisMemObjInit(pVedis,pObj);
	/* Allocate a new hashmap instance */
	pMap = vedisNewHashmap(pVedis,0,0); 
	if( pMap == 0 ){
		/* Discard */
		SyMemBackendPoolFree(&pVedis->sMem,pObj);
		return 0;
	}
	/* Set the array type */
	MemObjSetType(pObj, MEMOBJ_HASHMAP);
	pObj->x.pOther = pMap;
	return pObj;
}
VEDIS_PRIVATE void vedisObjectValueDestroy(vedis *pVedis,vedis_value *pValue)
{
	/* Invalidate any prior representation */
	vedisMemObjRelease(pValue);
	/* Discard */
	SyMemBackendPoolFree(&pVedis->sMem,pValue);
}
VEDIS_PRIVATE SyBlob * vedisObjectValueBlob(vedis_value *pValue)
{
	return &pValue->sBlob;
}
/*
 * ----------------------------------------------------------
 * File: mem_kv.c
 * MD5: 1ca85d6c931aac2bd2a40b799a91125a
 * ----------------------------------------------------------
 */
/*
 * Symisc Vedis: An Embeddable NoSQL (Post Modern) Database Engine.
 * Copyright (C) 2012-2013, Symisc Systems http://vedis.org/
 * Version 1.1.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.org/licensing.html
 */
 /* $SymiscID: mem_kv.c v1.7 Win7 2012-11-28 01:41 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* 
 * This file implements an in-memory key value storage engine for Vedis.
 * Note that this storage engine does not support transactions.
 *
 * Normaly, I (chm@symisc.net) planned to implement a red-black tree
 * which is suitable for this kind of operation, but due to the lack
 * of time, I decided to implement a tunned hashtable which everybody
 * know works very well for this kind of operation.
 * Again, I insist on a red-black tree implementation for future version
 * of Unqlite.
 */
/* Forward declaration */
typedef struct mem_hash_kv_engine mem_hash_kv_engine;
/*
 * Each record is storead in an instance of the following structure.
 */
typedef struct mem_hash_record mem_hash_record;
struct mem_hash_record
{
	mem_hash_kv_engine *pEngine;    /* Storage engine */
	sxu32 nHash;                    /* Hash of the key */
	const void *pKey;               /* Key */
	sxu32 nKeyLen;                  /* Key size (Max 1GB) */
	const void *pData;              /* Data */
	sxu32 nDataLen;                 /* Data length (Max 4GB) */
	mem_hash_record *pNext,*pPrev;  /* Link to other records */
	mem_hash_record *pNextHash,*pPrevHash; /* Collision link */
};
/*
 * Each in-memory KV engine is represented by an instance
 * of the following structure.
 */
struct mem_hash_kv_engine
{
	const vedis_kv_io *pIo; /* IO methods: MUST be first */
	/* Private data */
	SyMemBackend sAlloc;        /* Private memory allocator */
	ProcHash    xHash;          /* Default hash function */
	ProcCmp     xCmp;           /* Default comparison function */
	sxu32 nRecord;              /* Total number of records  */
	sxu32 nBucket;              /* Bucket size: Must be a power of two */
	mem_hash_record **apBucket; /* Hash bucket */
	mem_hash_record *pFirst;    /* First inserted entry */
	mem_hash_record *pLast;     /* Last inserted entry */
};
/*
 * Allocate a new hash record.
 */
static mem_hash_record * MemHashNewRecord(
	mem_hash_kv_engine *pEngine,
	const void *pKey,int nKey,
	const void *pData,vedis_int64 nData,
	sxu32 nHash
	)
{
	SyMemBackend *pAlloc = &pEngine->sAlloc;
	mem_hash_record *pRecord;
	void *pDupData;
	sxu32 nByte;
	char *zPtr;
	
	/* Total number of bytes to alloc */
	nByte = sizeof(mem_hash_record) + nKey;
	/* Allocate a new instance */
	pRecord = (mem_hash_record *)SyMemBackendAlloc(pAlloc,nByte);
	if( pRecord == 0 ){
		return 0;
	}
	pDupData = (void *)SyMemBackendAlloc(pAlloc,(sxu32)nData);
	if( pDupData == 0 ){
		SyMemBackendFree(pAlloc,pRecord);
		return 0;
	}
	zPtr = (char *)pRecord;
	zPtr += sizeof(mem_hash_record);
	/* Zero the structure */
	SyZero(pRecord,sizeof(mem_hash_record));
	/* Fill in the structure */
	pRecord->pEngine = pEngine;
	pRecord->nDataLen = (sxu32)nData;
	pRecord->nKeyLen = (sxu32)nKey;
	pRecord->nHash = nHash;
	SyMemcpy(pKey,zPtr,pRecord->nKeyLen);
	pRecord->pKey = (const void *)zPtr;
	SyMemcpy(pData,pDupData,pRecord->nDataLen);
	pRecord->pData = pDupData;
	/* All done */
	return pRecord;
}
/*
 * Install a given record in the hashtable.
 */
static void MemHashLinkRecord(mem_hash_kv_engine *pEngine,mem_hash_record *pRecord)
{
	sxu32 nBucket = pRecord->nHash & (pEngine->nBucket - 1);
	pRecord->pNextHash = pEngine->apBucket[nBucket];
	if( pEngine->apBucket[nBucket] ){
		pEngine->apBucket[nBucket]->pPrevHash = pRecord;
	}
	pEngine->apBucket[nBucket] = pRecord;
	if( pEngine->pFirst == 0 ){
		pEngine->pFirst = pEngine->pLast = pRecord;
	}else{
		MACRO_LD_PUSH(pEngine->pLast,pRecord);
	}
	pEngine->nRecord++;
}
/*
 * Unlink a given record from the hashtable.
 */
static void MemHashUnlinkRecord(mem_hash_kv_engine *pEngine,mem_hash_record *pEntry)
{
	sxu32 nBucket = pEntry->nHash & (pEngine->nBucket - 1);
	SyMemBackend *pAlloc = &pEngine->sAlloc;
	if( pEntry->pPrevHash == 0 ){
		pEngine->apBucket[nBucket] = pEntry->pNextHash;
	}else{
		pEntry->pPrevHash->pNextHash = pEntry->pNextHash;
	}
	if( pEntry->pNextHash ){
		pEntry->pNextHash->pPrevHash = pEntry->pPrevHash;
	}
	MACRO_LD_REMOVE(pEngine->pLast,pEntry);
	if( pEntry == pEngine->pFirst ){
		pEngine->pFirst = pEntry->pPrev;
	}
	pEngine->nRecord--;
	/* Release the entry */
	SyMemBackendFree(pAlloc,(void *)pEntry->pData);
	SyMemBackendFree(pAlloc,pEntry); /* Key is also stored here */
}
/*
 * Perform a lookup for a given entry.
 */
static mem_hash_record * MemHashGetEntry(
	mem_hash_kv_engine *pEngine,
	const void *pKey,int nKeyLen
	)
{
	mem_hash_record *pEntry;
	sxu32 nHash,nBucket;
	/* Hash the entry */
	nHash = pEngine->xHash(pKey,(sxu32)nKeyLen);
	nBucket = nHash & (pEngine->nBucket - 1);
	pEntry = pEngine->apBucket[nBucket];
	for(;;){
		if( pEntry == 0 ){
			break;
		}
		if( pEntry->nHash == nHash && pEntry->nKeyLen == (sxu32)nKeyLen && 
			pEngine->xCmp(pEntry->pKey,pKey,pEntry->nKeyLen) == 0 ){
				return pEntry;
		}
		pEntry = pEntry->pNextHash;
	}
	/* No such entry */
	return 0;
}
/*
 * Rehash all the entries in the given table.
 */
static int MemHashGrowTable(mem_hash_kv_engine *pEngine)
{
	sxu32 nNewSize = pEngine->nBucket << 1;
	mem_hash_record *pEntry;
	mem_hash_record **apNew;
	sxu32 n,iBucket;
	/* Allocate a new larger table */
	apNew = (mem_hash_record **)SyMemBackendAlloc(&pEngine->sAlloc, nNewSize * sizeof(mem_hash_record *));
	if( apNew == 0 ){
		/* Not so fatal, simply a performance hit */
		return VEDIS_OK;
	}
	/* Zero the new table */
	SyZero((void *)apNew, nNewSize * sizeof(mem_hash_record *));
	/* Rehash all entries */
	n = 0;
	pEntry = pEngine->pLast;
	for(;;){
		
		/* Loop one */
		if( n >= pEngine->nRecord ){
			break;
		}
		pEntry->pNextHash = pEntry->pPrevHash = 0;
		/* Install in the new bucket */
		iBucket = pEntry->nHash & (nNewSize - 1);
		pEntry->pNextHash = apNew[iBucket];
		if( apNew[iBucket] ){
			apNew[iBucket]->pPrevHash = pEntry;
		}
		apNew[iBucket] = pEntry;
		/* Point to the next entry */
		pEntry = pEntry->pNext;
		n++;

		/* Loop two */
		if( n >= pEngine->nRecord ){
			break;
		}
		pEntry->pNextHash = pEntry->pPrevHash = 0;
		/* Install in the new bucket */
		iBucket = pEntry->nHash & (nNewSize - 1);
		pEntry->pNextHash = apNew[iBucket];
		if( apNew[iBucket] ){
			apNew[iBucket]->pPrevHash = pEntry;
		}
		apNew[iBucket] = pEntry;
		/* Point to the next entry */
		pEntry = pEntry->pNext;
		n++;

		/* Loop three */
		if( n >= pEngine->nRecord ){
			break;
		}
		pEntry->pNextHash = pEntry->pPrevHash = 0;
		/* Install in the new bucket */
		iBucket = pEntry->nHash & (nNewSize - 1);
		pEntry->pNextHash = apNew[iBucket];
		if( apNew[iBucket] ){
			apNew[iBucket]->pPrevHash = pEntry;
		}
		apNew[iBucket] = pEntry;
		/* Point to the next entry */
		pEntry = pEntry->pNext;
		n++;

		/* Loop four */
		if( n >= pEngine->nRecord ){
			break;
		}
		pEntry->pNextHash = pEntry->pPrevHash = 0;
		/* Install in the new bucket */
		iBucket = pEntry->nHash & (nNewSize - 1);
		pEntry->pNextHash = apNew[iBucket];
		if( apNew[iBucket] ){
			apNew[iBucket]->pPrevHash = pEntry;
		}
		apNew[iBucket] = pEntry;
		/* Point to the next entry */
		pEntry = pEntry->pNext;
		n++;
	}
	/* Release the old table and reflect the change */
	SyMemBackendFree(&pEngine->sAlloc,(void *)pEngine->apBucket);
	pEngine->apBucket = apNew;
	pEngine->nBucket  = nNewSize;
	return VEDIS_OK;
}
/*
 * Exported Interfaces.
 */
/*
 * Each public cursor is identified by an instance of this structure.
 */
typedef struct mem_hash_cursor mem_hash_cursor;
struct mem_hash_cursor
{
	vedis_kv_engine *pStore; /* Must be first */
	/* Private fields */
	mem_hash_record *pCur;     /* Current hash record */
};
/*
 * Initialize the cursor.
 */
static void MemHashInitCursor(vedis_kv_cursor *pCursor)
{
	 mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pCursor->pStore;
	 mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	 /* Point to the first inserted entry */
	 pMem->pCur = pEngine->pFirst;
}
/*
 * Point to the first entry.
 */
static int MemHashCursorFirst(vedis_kv_cursor *pCursor)
{
	 mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pCursor->pStore;
	 mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	 pMem->pCur = pEngine->pFirst;
	 return VEDIS_OK;
}
/*
 * Point to the last entry.
 */
static int MemHashCursorLast(vedis_kv_cursor *pCursor)
{
	 mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pCursor->pStore;
	 mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	 pMem->pCur = pEngine->pLast;
	 return VEDIS_OK;
}
/*
 * is a Valid Cursor.
 */
static int MemHashCursorValid(vedis_kv_cursor *pCursor)
{
	 mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	 return pMem->pCur != 0 ? 1 : 0;
}
/*
 * Point to the next entry.
 */
static int MemHashCursorNext(vedis_kv_cursor *pCursor)
{
	 mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	 if( pMem->pCur == 0){
		 return VEDIS_EOF;
	 }
	 pMem->pCur = pMem->pCur->pPrev; /* Reverse link: Not a Bug */
	 return VEDIS_OK;
}
/*
 * Point to the previous entry.
 */
static int MemHashCursorPrev(vedis_kv_cursor *pCursor)
{
	 mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	 if( pMem->pCur == 0){
		 return VEDIS_EOF;
	 }
	 pMem->pCur = pMem->pCur->pNext; /* Reverse link: Not a Bug */
	 return VEDIS_OK;
}
/*
 * Return key length.
 */
static int MemHashCursorKeyLength(vedis_kv_cursor *pCursor,int *pLen)
{
	mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	if( pMem->pCur == 0){
		 return VEDIS_EOF;
	}
	*pLen = (int)pMem->pCur->nKeyLen;
	return VEDIS_OK;
}
/*
 * Return data length.
 */
static int MemHashCursorDataLength(vedis_kv_cursor *pCursor,vedis_int64 *pLen)
{
	mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	if( pMem->pCur == 0 ){
		 return VEDIS_EOF;
	}
	*pLen = pMem->pCur->nDataLen;
	return VEDIS_OK;
}
/*
 * Consume the key.
 */
static int MemHashCursorKey(vedis_kv_cursor *pCursor,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData)
{
	mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	int rc;
	if( pMem->pCur == 0){
		 return VEDIS_EOF;
	}
	/* Invoke the callback */
	rc = xConsumer(pMem->pCur->pKey,pMem->pCur->nKeyLen,pUserData);
	/* Callback result */
	return rc;
}
/*
 * Consume the data.
 */
static int MemHashCursorData(vedis_kv_cursor *pCursor,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData)
{
	mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	int rc;
	if( pMem->pCur == 0){
		 return VEDIS_EOF;
	}
	/* Invoke the callback */
	rc = xConsumer(pMem->pCur->pData,pMem->pCur->nDataLen,pUserData);
	/* Callback result */
	return rc;
}
/*
 * Reset the cursor.
 */
static void MemHashCursorReset(vedis_kv_cursor *pCursor)
{
	mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	pMem->pCur = ((mem_hash_kv_engine *)pCursor->pStore)->pFirst;
}
/*
 * Remove a particular record.
 */
static int MemHashCursorDelete(vedis_kv_cursor *pCursor)
{
	mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	mem_hash_record *pNext;
	if( pMem->pCur == 0 ){
		/* Cursor does not point to anything */
		return VEDIS_NOTFOUND;
	}
	pNext = pMem->pCur->pPrev;
	/* Perform the deletion */
	MemHashUnlinkRecord(pMem->pCur->pEngine,pMem->pCur);
	/* Point to the next entry */
	pMem->pCur = pNext;
	return VEDIS_OK;
}
/*
 * Find a particular record.
 */
static int MemHashCursorSeek(vedis_kv_cursor *pCursor,const void *pKey,int nByte,int iPos)
{
	mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pCursor->pStore;
	mem_hash_cursor *pMem = (mem_hash_cursor *)pCursor;
	/* Perform the lookup */
	pMem->pCur = MemHashGetEntry(pEngine,pKey,nByte);
	if( pMem->pCur == 0 ){
		if( iPos != VEDIS_CURSOR_MATCH_EXACT ){
			/* noop; */
		}
		/* No such record */
		return VEDIS_NOTFOUND;
	}
	return VEDIS_OK;
}
/*
 * Builtin hash function.
 */
static sxu32 MemHashFunc(const void *pSrc,sxu32 nLen)
{
	register unsigned char *zIn = (unsigned char *)pSrc;
	unsigned char *zEnd;
	sxu32 nH = 5381;
	zEnd = &zIn[nLen];
	for(;;){
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
	}	
	return nH;
}
/* Default bucket size */
#define MEM_HASH_BUCKET_SIZE 64
/* Default fill factor */
#define MEM_HASH_FILL_FACTOR 4 /* or 3 */
/*
 * Initialize the in-memory storage engine.
 */
static int MemHashInit(vedis_kv_engine *pKvEngine,int iPageSize)
{
	mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pKvEngine;
	/* Note that this instance is already zeroed */	
	/* Memory backend */
	SyMemBackendInitFromParent(&pEngine->sAlloc,vedisExportMemBackend());
#if defined(VEDIS_ENABLE_THREADS)
	/* Already protected by the upper layers */
	SyMemBackendDisbaleMutexing(&pEngine->sAlloc);
#endif
	/* Default hash & comparison function */
	pEngine->xHash = MemHashFunc;
	pEngine->xCmp = SyMemcmp;
	/* Allocate a new bucket */
	pEngine->apBucket = (mem_hash_record **)SyMemBackendAlloc(&pEngine->sAlloc,MEM_HASH_BUCKET_SIZE * sizeof(mem_hash_record *));
	if( pEngine->apBucket == 0 ){
		SXUNUSED(iPageSize); /* cc warning */
		return VEDIS_NOMEM;
	}
	/* Zero the bucket */
	SyZero(pEngine->apBucket,MEM_HASH_BUCKET_SIZE * sizeof(mem_hash_record *));
	pEngine->nRecord = 0;
	pEngine->nBucket = MEM_HASH_BUCKET_SIZE;
	return VEDIS_OK;
}
/*
 * Release the in-memory storage engine.
 */
static void MemHashRelease(vedis_kv_engine *pKvEngine)
{
	mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pKvEngine;
	/* Release the private memory backend */
	SyMemBackendRelease(&pEngine->sAlloc);
}
/*
 * Configure the in-memory storage engine.
 */
static int MemHashConfigure(vedis_kv_engine *pKvEngine,int iOp,va_list ap)
{
	mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pKvEngine;
	int rc = VEDIS_OK;
	switch(iOp){
	case VEDIS_KV_CONFIG_HASH_FUNC:{
		/* Use a default hash function */
		if( pEngine->nRecord > 0 ){
			rc = VEDIS_LOCKED;
		}else{
			ProcHash xHash = va_arg(ap,ProcHash);
			if( xHash ){
				pEngine->xHash = xHash;
			}
		}
		break;
									 }
	case VEDIS_KV_CONFIG_CMP_FUNC: {
		/* Default comparison function */
		ProcCmp xCmp = va_arg(ap,ProcCmp);
		if( xCmp ){
			pEngine->xCmp = xCmp;
		}
		break;
									 }
	default:
		/* Unknown configuration option */
		rc = VEDIS_UNKNOWN;
	}
	return rc;
}
/*
 * Replace method.
 */
static int MemHashReplace(
	  vedis_kv_engine *pKv,
	  const void *pKey,int nKeyLen,
	  const void *pData,vedis_int64 nDataLen
	  )
{
	mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pKv;
	mem_hash_record *pRecord;
	if( nDataLen > SXU32_HIGH ){
		/* Database limit */
		pEngine->pIo->xErr(pEngine->pIo->pHandle,"Record size limit reached");
		return VEDIS_LIMIT;
	}
	/* Fetch the record first */
	pRecord = MemHashGetEntry(pEngine,pKey,nKeyLen);
	if( pRecord == 0 ){
		/* Allocate a new record */
		pRecord = MemHashNewRecord(pEngine,
			pKey,nKeyLen,
			pData,nDataLen,
			pEngine->xHash(pKey,nKeyLen)
			);
		if( pRecord == 0 ){
			return VEDIS_NOMEM;
		}
		/* Link the entry */
		MemHashLinkRecord(pEngine,pRecord);
		if( (pEngine->nRecord >= pEngine->nBucket * MEM_HASH_FILL_FACTOR) && pEngine->nRecord < 100000 ){
			/* Rehash the table */
			MemHashGrowTable(pEngine);
		}
	}else{
		sxu32 nData = (sxu32)nDataLen;
		void *pNew;
		/* Replace an existing record */
		if( nData == pRecord->nDataLen ){
			/* No need to free the old chunk */
			pNew = (void *)pRecord->pData;
		}else{
			pNew = SyMemBackendAlloc(&pEngine->sAlloc,nData);
			if( pNew == 0 ){
				return VEDIS_NOMEM;
			}
			/* Release the old data */
			SyMemBackendFree(&pEngine->sAlloc,(void *)pRecord->pData);
		}
		/* Reflect the change */
		pRecord->nDataLen = nData;
		SyMemcpy(pData,pNew,nData);
		pRecord->pData = pNew;
	}
	return VEDIS_OK;
}
/*
 * Append method.
 */
static int MemHashAppend(
	  vedis_kv_engine *pKv,
	  const void *pKey,int nKeyLen,
	  const void *pData,vedis_int64 nDataLen
	  )
{
	mem_hash_kv_engine *pEngine = (mem_hash_kv_engine *)pKv;
	mem_hash_record *pRecord;
	if( nDataLen > SXU32_HIGH ){
		/* Database limit */
		pEngine->pIo->xErr(pEngine->pIo->pHandle,"Record size limit reached");
		return VEDIS_LIMIT;
	}
	/* Fetch the record first */
	pRecord = MemHashGetEntry(pEngine,pKey,nKeyLen);
	if( pRecord == 0 ){
		/* Allocate a new record */
		pRecord = MemHashNewRecord(pEngine,
			pKey,nKeyLen,
			pData,nDataLen,
			pEngine->xHash(pKey,nKeyLen)
			);
		if( pRecord == 0 ){
			return VEDIS_NOMEM;
		}
		/* Link the entry */
		MemHashLinkRecord(pEngine,pRecord);
		if( pEngine->nRecord * MEM_HASH_FILL_FACTOR >= pEngine->nBucket && pEngine->nRecord < 100000 ){
			/* Rehash the table */
			MemHashGrowTable(pEngine);
		}
	}else{
		vedis_int64 nNew = pRecord->nDataLen + nDataLen;
		void *pOld = (void *)pRecord->pData;
		sxu32 nData;
		char *zNew;
		/* Append data to the existing record */
		if( nNew > SXU32_HIGH ){
			/* Overflow */
			pEngine->pIo->xErr(pEngine->pIo->pHandle,"Append operation will cause data overflow");	
			return VEDIS_LIMIT;
		}
		nData = (sxu32)nNew;
		/* Allocate bigger chunk */
		zNew = (char *)SyMemBackendRealloc(&pEngine->sAlloc,pOld,nData);
		if( zNew == 0 ){
			return VEDIS_NOMEM;
		}
		/* Reflect the change */
		SyMemcpy(pData,&zNew[pRecord->nDataLen],(sxu32)nDataLen);
		pRecord->pData = (const void *)zNew;
		pRecord->nDataLen = nData;
	}
	return VEDIS_OK;
}
/*
 * Export the in-memory storage engine.
 */
VEDIS_PRIVATE const vedis_kv_methods * vedisExportMemKvStorage(void)
{
	static const vedis_kv_methods sMemStore = {
		"mem",                      /* zName */
		sizeof(mem_hash_kv_engine), /* szKv */
		sizeof(mem_hash_cursor),    /* szCursor */
		1,                          /* iVersion */
		MemHashInit,                /* xInit */
		MemHashRelease,             /* xRelease */
		MemHashConfigure,           /* xConfig */
		0,                          /* xOpen */
		MemHashReplace,             /* xReplace */
		MemHashAppend,              /* xAppend */
		MemHashInitCursor,          /* xCursorInit */
		MemHashCursorSeek,          /* xSeek */
		MemHashCursorFirst,         /* xFirst */
		MemHashCursorLast,          /* xLast */
		MemHashCursorValid,         /* xValid */
		MemHashCursorNext,          /* xNext */
		MemHashCursorPrev,          /* xPrev */
		MemHashCursorDelete,        /* xDelete */
		MemHashCursorKeyLength,     /* xKeyLength */
		MemHashCursorKey,           /* xKey */
		MemHashCursorDataLength,    /* xDataLength */
		MemHashCursorData,          /* xData */
		MemHashCursorReset,         /* xReset */
		0        /* xRelease */                        
	};
	return &sMemStore;
}
/*
 * ----------------------------------------------------------
 * File: lib.c
 * MD5: 5c88bf62c65357b1845e119f319d9721
 * ----------------------------------------------------------
 */
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
 /* $SymiscID: lib.c v5.1 Win7 2012-08-08 04:19 stable <chm@symisc.net> $ */
/*
 * Symisc Run-Time API: A modern thread safe replacement of the standard libc
 * Copyright (C) Symisc Systems 2007-2012, http://www.symisc.net/
 *
 * The Symisc Run-Time API is an independent project developed by symisc systems
 * internally as a secure replacement of the standard libc.
 * The library is re-entrant, thread-safe and platform independent.
 */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
#if defined(__WINNT__)
#include <Windows.h>
#else
#include <stdlib.h>
#endif
#if defined(VEDIS_ENABLE_THREADS)
/* SyRunTimeApi: sxmutex.c */
#if defined(__WINNT__)
struct SyMutex
{
	CRITICAL_SECTION sMutex;
	sxu32 nType; /* Mutex type, one of SXMUTEX_TYPE_* */
};
/* Preallocated static mutex */
static SyMutex aStaticMutexes[] = {
		{{0}, SXMUTEX_TYPE_STATIC_1}, 
		{{0}, SXMUTEX_TYPE_STATIC_2}, 
		{{0}, SXMUTEX_TYPE_STATIC_3}, 
		{{0}, SXMUTEX_TYPE_STATIC_4}, 
		{{0}, SXMUTEX_TYPE_STATIC_5}, 
		{{0}, SXMUTEX_TYPE_STATIC_6}
};
static BOOL winMutexInit = FALSE;
static LONG winMutexLock = 0;

static sxi32 WinMutexGlobaInit(void)
{
	LONG rc;
	rc = InterlockedCompareExchange(&winMutexLock, 1, 0);
	if ( rc == 0 ){
		sxu32 n;
		for( n = 0 ; n  < SX_ARRAYSIZE(aStaticMutexes) ; ++n ){
			InitializeCriticalSection(&aStaticMutexes[n].sMutex);
		}
		winMutexInit = TRUE;
	}else{
		/* Someone else is doing this for us */
		while( winMutexInit == FALSE ){
			Sleep(1);
		}
	}
	return SXRET_OK;
}
static void WinMutexGlobalRelease(void)
{
	LONG rc;
	rc = InterlockedCompareExchange(&winMutexLock, 0, 1);
	if( rc == 1 ){
		/* The first to decrement to zero does the actual global release */
		if( winMutexInit == TRUE ){
			sxu32 n;
			for( n = 0 ; n < SX_ARRAYSIZE(aStaticMutexes) ; ++n ){
				DeleteCriticalSection(&aStaticMutexes[n].sMutex);
			}
			winMutexInit = FALSE;
		}
	}
}
static SyMutex * WinMutexNew(int nType)
{
	SyMutex *pMutex = 0;
	if( nType == SXMUTEX_TYPE_FAST || nType == SXMUTEX_TYPE_RECURSIVE ){
		/* Allocate a new mutex */
		pMutex = (SyMutex *)HeapAlloc(GetProcessHeap(), 0, sizeof(SyMutex));
		if( pMutex == 0 ){
			return 0;
		}
		InitializeCriticalSection(&pMutex->sMutex);
	}else{
		/* Use a pre-allocated static mutex */
		if( nType > SXMUTEX_TYPE_STATIC_6 ){
			nType = SXMUTEX_TYPE_STATIC_6;
		}
		pMutex = &aStaticMutexes[nType - 3];
	}
	pMutex->nType = nType;
	return pMutex;
}
static void WinMutexRelease(SyMutex *pMutex)
{
	if( pMutex->nType == SXMUTEX_TYPE_FAST || pMutex->nType == SXMUTEX_TYPE_RECURSIVE ){
		DeleteCriticalSection(&pMutex->sMutex);
		HeapFree(GetProcessHeap(), 0, pMutex);
	}
}
static void WinMutexEnter(SyMutex *pMutex)
{
	EnterCriticalSection(&pMutex->sMutex);
}
static sxi32 WinMutexTryEnter(SyMutex *pMutex)
{
#ifdef _WIN32_WINNT
	BOOL rc;
	/* Only WindowsNT platforms */
	rc = TryEnterCriticalSection(&pMutex->sMutex);
	if( rc ){
		return SXRET_OK;
	}else{
		return SXERR_BUSY;
	}
#else
	return SXERR_NOTIMPLEMENTED;
#endif
}
static void WinMutexLeave(SyMutex *pMutex)
{
	LeaveCriticalSection(&pMutex->sMutex);
}
/* Export Windows mutex interfaces */
static const SyMutexMethods sWinMutexMethods = {
	WinMutexGlobaInit,  /* xGlobalInit() */
	WinMutexGlobalRelease, /* xGlobalRelease() */
	WinMutexNew,     /* xNew() */
	WinMutexRelease, /* xRelease() */
	WinMutexEnter,   /* xEnter() */
	WinMutexTryEnter, /* xTryEnter() */
	WinMutexLeave     /* xLeave() */
};
VEDIS_PRIVATE const SyMutexMethods * SyMutexExportMethods(void)
{
	return &sWinMutexMethods;
}
#elif defined(__UNIXES__)
#include <pthread.h>
struct SyMutex
{
	pthread_mutex_t sMutex;
	sxu32 nType;
};
static SyMutex * UnixMutexNew(int nType)
{
	static SyMutex aStaticMutexes[] = {
		{PTHREAD_MUTEX_INITIALIZER, SXMUTEX_TYPE_STATIC_1}, 
		{PTHREAD_MUTEX_INITIALIZER, SXMUTEX_TYPE_STATIC_2}, 
		{PTHREAD_MUTEX_INITIALIZER, SXMUTEX_TYPE_STATIC_3}, 
		{PTHREAD_MUTEX_INITIALIZER, SXMUTEX_TYPE_STATIC_4}, 
		{PTHREAD_MUTEX_INITIALIZER, SXMUTEX_TYPE_STATIC_5}, 
		{PTHREAD_MUTEX_INITIALIZER, SXMUTEX_TYPE_STATIC_6}
	};
	SyMutex *pMutex;
	
	if( nType == SXMUTEX_TYPE_FAST || nType == SXMUTEX_TYPE_RECURSIVE ){
		pthread_mutexattr_t sRecursiveAttr;
  		/* Allocate a new mutex */
  		pMutex = (SyMutex *)malloc(sizeof(SyMutex));
  		if( pMutex == 0 ){
  			return 0;
  		}
  		if( nType == SXMUTEX_TYPE_RECURSIVE ){
  			pthread_mutexattr_init(&sRecursiveAttr);
  			pthread_mutexattr_settype(&sRecursiveAttr, PTHREAD_MUTEX_RECURSIVE);
  		}
  		pthread_mutex_init(&pMutex->sMutex, nType == SXMUTEX_TYPE_RECURSIVE ? &sRecursiveAttr : 0 );
		if(	nType == SXMUTEX_TYPE_RECURSIVE ){
   			pthread_mutexattr_destroy(&sRecursiveAttr);
		}
	}else{
		/* Use a pre-allocated static mutex */
		if( nType > SXMUTEX_TYPE_STATIC_6 ){
			nType = SXMUTEX_TYPE_STATIC_6;
		}
		pMutex = &aStaticMutexes[nType - 3];
	}
  pMutex->nType = nType;
  
  return pMutex;
}
static void UnixMutexRelease(SyMutex *pMutex)
{
	if( pMutex->nType == SXMUTEX_TYPE_FAST || pMutex->nType == SXMUTEX_TYPE_RECURSIVE ){
		pthread_mutex_destroy(&pMutex->sMutex);
		free(pMutex);
	}
}
static void UnixMutexEnter(SyMutex *pMutex)
{
	pthread_mutex_lock(&pMutex->sMutex);
}
static void UnixMutexLeave(SyMutex *pMutex)
{
	pthread_mutex_unlock(&pMutex->sMutex);
}
/* Export pthread mutex interfaces */
static const SyMutexMethods sPthreadMutexMethods = {
	0, /* xGlobalInit() */
	0, /* xGlobalRelease() */
	UnixMutexNew,      /* xNew() */
	UnixMutexRelease,  /* xRelease() */
	UnixMutexEnter,    /* xEnter() */
	0,                 /* xTryEnter() */
	UnixMutexLeave     /* xLeave() */
};
VEDIS_PRIVATE const SyMutexMethods * SyMutexExportMethods(void)
{
	return &sPthreadMutexMethods;
}
#else
/* Host application must register their own mutex subsystem if the target
 * platform is not an UNIX-like or windows systems.
 */
struct SyMutex
{
	sxu32 nType;
};
static SyMutex * DummyMutexNew(int nType)
{
	static SyMutex sMutex;
	SXUNUSED(nType);
	return &sMutex;
}
static void DummyMutexRelease(SyMutex *pMutex)
{
	SXUNUSED(pMutex);
}
static void DummyMutexEnter(SyMutex *pMutex)
{
	SXUNUSED(pMutex);
}
static void DummyMutexLeave(SyMutex *pMutex)
{
	SXUNUSED(pMutex);
}
/* Export the dummy mutex interfaces */
static const SyMutexMethods sDummyMutexMethods = {
	0, /* xGlobalInit() */
	0, /* xGlobalRelease() */
	DummyMutexNew,      /* xNew() */
	DummyMutexRelease,  /* xRelease() */
	DummyMutexEnter,    /* xEnter() */
	0,                  /* xTryEnter() */
	DummyMutexLeave     /* xLeave() */
};
VEDIS_PRIVATE const SyMutexMethods * SyMutexExportMethods(void)
{
	return &sDummyMutexMethods;
}
#endif /* __WINNT__ */
#endif /* VEDIS_ENABLE_THREADS */
static void * SyOSHeapAlloc(sxu32 nByte)
{
	void *pNew;
#if defined(__WINNT__)
	pNew = HeapAlloc(GetProcessHeap(), 0, nByte);
#else
	pNew = malloc((size_t)nByte);
#endif
	return pNew;
}
static void * SyOSHeapRealloc(void *pOld, sxu32 nByte)
{
	void *pNew;
#if defined(__WINNT__)
	pNew = HeapReAlloc(GetProcessHeap(), 0, pOld, nByte);
#else
	pNew = realloc(pOld, (size_t)nByte);
#endif
	return pNew;	
}
static void SyOSHeapFree(void *pPtr)
{
#if defined(__WINNT__)
	HeapFree(GetProcessHeap(), 0, pPtr);
#else
	free(pPtr);
#endif
}
/* SyRunTimeApi:sxstr.c */
VEDIS_PRIVATE sxu32 SyStrlen(const char *zSrc)
{
	register const char *zIn = zSrc;
#if defined(UNTRUST)
	if( zIn == 0 ){
		return 0;
	}
#endif
	for(;;){
		if( !zIn[0] ){ break; } zIn++;
		if( !zIn[0] ){ break; } zIn++;
		if( !zIn[0] ){ break; } zIn++;
		if( !zIn[0] ){ break; } zIn++;	
	}
	return (sxu32)(zIn - zSrc);
}
#if defined(__APPLE__)
VEDIS_PRIVATE sxi32 SyStrncmp(const char *zLeft, const char *zRight, sxu32 nLen)
{
	const unsigned char *zP = (const unsigned char *)zLeft;
	const unsigned char *zQ = (const unsigned char *)zRight;

	if( SX_EMPTY_STR(zP) || SX_EMPTY_STR(zQ)  ){
			return SX_EMPTY_STR(zP) ? (SX_EMPTY_STR(zQ) ? 0 : -1) :1;
	}
	if( nLen <= 0 ){
		return 0;
	}
	for(;;){
		if( nLen <= 0 ){ return 0; } if( zP[0] == 0 || zQ[0] == 0 || zP[0] != zQ[0] ){ break; } zP++; zQ++; nLen--;
		if( nLen <= 0 ){ return 0; } if( zP[0] == 0 || zQ[0] == 0 || zP[0] != zQ[0] ){ break; } zP++; zQ++; nLen--;
		if( nLen <= 0 ){ return 0; } if( zP[0] == 0 || zQ[0] == 0 || zP[0] != zQ[0] ){ break; } zP++; zQ++; nLen--;
		if( nLen <= 0 ){ return 0; } if( zP[0] == 0 || zQ[0] == 0 || zP[0] != zQ[0] ){ break; } zP++; zQ++; nLen--;
	}
	return (sxi32)(zP[0] - zQ[0]);
}	
#endif
VEDIS_PRIVATE sxi32 SyStrnicmp(const char *zLeft, const char *zRight, sxu32 SLen)
{
  	register unsigned char *p = (unsigned char *)zLeft;
	register unsigned char *q = (unsigned char *)zRight;
	
	if( SX_EMPTY_STR(p) || SX_EMPTY_STR(q) ){
		return SX_EMPTY_STR(p)? SX_EMPTY_STR(q) ? 0 : -1 :1;
	}
	for(;;){
		if( !SLen ){ return 0; }if( !*p || !*q || SyCharToLower(*p) != SyCharToLower(*q) ){ break; }p++;q++;--SLen;
		if( !SLen ){ return 0; }if( !*p || !*q || SyCharToLower(*p) != SyCharToLower(*q) ){ break; }p++;q++;--SLen;
		if( !SLen ){ return 0; }if( !*p || !*q || SyCharToLower(*p) != SyCharToLower(*q) ){ break; }p++;q++;--SLen;
		if( !SLen ){ return 0; }if( !*p || !*q || SyCharToLower(*p) != SyCharToLower(*q) ){ break; }p++;q++;--SLen;
		
	}
	return (sxi32)(SyCharToLower(p[0]) - SyCharToLower(q[0]));
}
VEDIS_PRIVATE sxu32 Systrcpy(char *zDest, sxu32 nDestLen, const char *zSrc, sxu32 nLen)
{
	unsigned char *zBuf = (unsigned char *)zDest;
	unsigned char *zIn = (unsigned char *)zSrc;
	unsigned char *zEnd;
#if defined(UNTRUST)
	if( zSrc == (const char *)zDest ){
			return 0;
	}
#endif
	if( nLen <= 0 ){
		nLen = SyStrlen(zSrc);
	}
	zEnd = &zBuf[nDestLen - 1]; /* reserve a room for the null terminator */
	for(;;){
		if( zBuf >= zEnd || nLen == 0 ){ break;} zBuf[0] = zIn[0]; zIn++; zBuf++; nLen--;
		if( zBuf >= zEnd || nLen == 0 ){ break;} zBuf[0] = zIn[0]; zIn++; zBuf++; nLen--;
		if( zBuf >= zEnd || nLen == 0 ){ break;} zBuf[0] = zIn[0]; zIn++; zBuf++; nLen--;
		if( zBuf >= zEnd || nLen == 0 ){ break;} zBuf[0] = zIn[0]; zIn++; zBuf++; nLen--;
	}
	zBuf[0] = 0;
	return (sxu32)(zBuf-(unsigned char *)zDest);
}
/* SyRunTimeApi:sxmem.c */
VEDIS_PRIVATE void SyZero(void *pSrc, sxu32 nSize)
{
	register unsigned char *zSrc = (unsigned char *)pSrc;
	unsigned char *zEnd;
#if defined(UNTRUST)
	if( zSrc == 0 || nSize <= 0 ){
		return ;
	}
#endif
	zEnd = &zSrc[nSize];
	for(;;){
		if( zSrc >= zEnd ){break;} zSrc[0] = 0; zSrc++;
		if( zSrc >= zEnd ){break;} zSrc[0] = 0; zSrc++;
		if( zSrc >= zEnd ){break;} zSrc[0] = 0; zSrc++;
		if( zSrc >= zEnd ){break;} zSrc[0] = 0; zSrc++;
	}
}
VEDIS_PRIVATE sxi32 SyMemcmp(const void *pB1, const void *pB2, sxu32 nSize)
{
	sxi32 rc;
	if( nSize <= 0 ){
		return 0;
	}
	if( pB1 == 0 || pB2 == 0 ){
		return pB1 != 0 ? 1 : (pB2 == 0 ? 0 : -1);
	}
	SX_MACRO_FAST_CMP(pB1, pB2, nSize, rc);
	return rc;
}
VEDIS_PRIVATE sxu32 SyMemcpy(const void *pSrc, void *pDest, sxu32 nLen)
{
	if( pSrc == 0 || pDest == 0 ){
		return 0;
	}
	if( pSrc == (const void *)pDest ){
		return nLen;
	}
	SX_MACRO_FAST_MEMCPY(pSrc, pDest, nLen);
	return nLen;
}
static void * MemOSAlloc(sxu32 nBytes)
{
	sxu32 *pChunk;
	pChunk = (sxu32 *)SyOSHeapAlloc(nBytes + sizeof(sxu32));
	if( pChunk == 0 ){
		return 0;
	}
	pChunk[0] = nBytes;
	return (void *)&pChunk[1];
}
static void * MemOSRealloc(void *pOld, sxu32 nBytes)
{
	sxu32 *pOldChunk;
	sxu32 *pChunk;
	pOldChunk = (sxu32 *)(((char *)pOld)-sizeof(sxu32));
	if( pOldChunk[0] >= nBytes ){
		return pOld;
	}
	pChunk = (sxu32 *)SyOSHeapRealloc(pOldChunk, nBytes + sizeof(sxu32));
	if( pChunk == 0 ){
		return 0;
	}
	pChunk[0] = nBytes;
	return (void *)&pChunk[1];
}
static void MemOSFree(void *pBlock)
{
	void *pChunk;
	pChunk = (void *)(((char *)pBlock)-sizeof(sxu32));
	SyOSHeapFree(pChunk);
}
static sxu32 MemOSChunkSize(void *pBlock)
{
	sxu32 *pChunk;
	pChunk = (sxu32 *)(((char *)pBlock)-sizeof(sxu32));
	return pChunk[0];
}
/* Export OS allocation methods */
static const SyMemMethods sOSAllocMethods = {
	MemOSAlloc, 
	MemOSRealloc, 
	MemOSFree, 
	MemOSChunkSize, 
	0, 
	0, 
	0
};
static void * MemBackendAlloc(SyMemBackend *pBackend, sxu32 nByte)
{
	SyMemBlock *pBlock;
	sxi32 nRetry = 0;

	/* Append an extra block so we can tracks allocated chunks and avoid memory
	 * leaks.
	 */
	nByte += sizeof(SyMemBlock);
	for(;;){
		pBlock = (SyMemBlock *)pBackend->pMethods->xAlloc(nByte);
		if( pBlock != 0 || pBackend->xMemError == 0 || nRetry > SXMEM_BACKEND_RETRY 
			|| SXERR_RETRY != pBackend->xMemError(pBackend->pUserData) ){
				break;
		}
		nRetry++;
	}
	if( pBlock  == 0 ){
		return 0;
	}
	pBlock->pNext = pBlock->pPrev = 0;
	/* Link to the list of already tracked blocks */
	MACRO_LD_PUSH(pBackend->pBlocks, pBlock);
#if defined(UNTRUST)
	pBlock->nGuard = SXMEM_BACKEND_MAGIC;
#endif
	pBackend->nBlock++;
	return (void *)&pBlock[1];
}
VEDIS_PRIVATE void * SyMemBackendAlloc(SyMemBackend *pBackend, sxu32 nByte)
{
	void *pChunk;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) ){
		return 0;
	}
#endif
	if( pBackend->pMutexMethods ){
		SyMutexEnter(pBackend->pMutexMethods, pBackend->pMutex);
	}
	pChunk = MemBackendAlloc(&(*pBackend), nByte);
	if( pBackend->pMutexMethods ){
		SyMutexLeave(pBackend->pMutexMethods, pBackend->pMutex);
	}
	return pChunk;
}
static void * MemBackendRealloc(SyMemBackend *pBackend, void * pOld, sxu32 nByte)
{
	SyMemBlock *pBlock, *pNew, *pPrev, *pNext;
	sxu32 nRetry = 0;

	if( pOld == 0 ){
		return MemBackendAlloc(&(*pBackend), nByte);
	}
	pBlock = (SyMemBlock *)(((char *)pOld) - sizeof(SyMemBlock));
#if defined(UNTRUST)
	if( pBlock->nGuard != SXMEM_BACKEND_MAGIC ){
		return 0;
	}
#endif
	nByte += sizeof(SyMemBlock);
	pPrev = pBlock->pPrev;
	pNext = pBlock->pNext;
	for(;;){
		pNew = (SyMemBlock *)pBackend->pMethods->xRealloc(pBlock, nByte);
		if( pNew != 0 || pBackend->xMemError == 0 || nRetry > SXMEM_BACKEND_RETRY ||
			SXERR_RETRY != pBackend->xMemError(pBackend->pUserData) ){
				break;
		}
		nRetry++;
	}
	if( pNew == 0 ){
		return 0;
	}
	if( pNew != pBlock ){
		if( pPrev == 0 ){
			pBackend->pBlocks = pNew;
		}else{
			pPrev->pNext = pNew;
		}
		if( pNext ){
			pNext->pPrev = pNew;
		}
#if defined(UNTRUST)
		pNew->nGuard = SXMEM_BACKEND_MAGIC;
#endif
	}
	return (void *)&pNew[1];
}
VEDIS_PRIVATE void * SyMemBackendRealloc(SyMemBackend *pBackend, void * pOld, sxu32 nByte)
{
	void *pChunk;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend)  ){
		return 0;
	}
#endif
	if( pBackend->pMutexMethods ){
		SyMutexEnter(pBackend->pMutexMethods, pBackend->pMutex);
	}
	pChunk = MemBackendRealloc(&(*pBackend), pOld, nByte);
	if( pBackend->pMutexMethods ){
		SyMutexLeave(pBackend->pMutexMethods, pBackend->pMutex);
	}
	return pChunk;
}
static sxi32 MemBackendFree(SyMemBackend *pBackend, void * pChunk)
{
	SyMemBlock *pBlock;
	pBlock = (SyMemBlock *)(((char *)pChunk) - sizeof(SyMemBlock));
#if defined(UNTRUST)
	if( pBlock->nGuard != SXMEM_BACKEND_MAGIC ){
		return SXERR_CORRUPT;
	}
#endif
	/* Unlink from the list of active blocks */
	if( pBackend->nBlock > 0 ){
		/* Release the block */
#if defined(UNTRUST)
		/* Mark as stale block */
		pBlock->nGuard = 0x635B;
#endif
		MACRO_LD_REMOVE(pBackend->pBlocks, pBlock);
		pBackend->nBlock--;
		pBackend->pMethods->xFree(pBlock);
	}
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyMemBackendFree(SyMemBackend *pBackend, void * pChunk)
{
	sxi32 rc;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) ){
		return SXERR_CORRUPT;
	}
#endif
	if( pChunk == 0 ){
		return SXRET_OK;
	}
	if( pBackend->pMutexMethods ){
		SyMutexEnter(pBackend->pMutexMethods, pBackend->pMutex);
	}
	rc = MemBackendFree(&(*pBackend), pChunk);
	if( pBackend->pMutexMethods ){
		SyMutexLeave(pBackend->pMutexMethods, pBackend->pMutex);
	}
	return rc;
}
#if defined(VEDIS_ENABLE_THREADS)
VEDIS_PRIVATE sxi32 SyMemBackendMakeThreadSafe(SyMemBackend *pBackend, const SyMutexMethods *pMethods)
{
	SyMutex *pMutex;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) || pMethods == 0 || pMethods->xNew == 0){
		return SXERR_CORRUPT;
	}
#endif
	pMutex = pMethods->xNew(SXMUTEX_TYPE_FAST);
	if( pMutex == 0 ){
		return SXERR_OS;
	}
	/* Attach the mutex to the memory backend */
	pBackend->pMutex = pMutex;
	pBackend->pMutexMethods = pMethods;
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyMemBackendDisbaleMutexing(SyMemBackend *pBackend)
{
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) ){
		return SXERR_CORRUPT;
	}
#endif
	if( pBackend->pMutex == 0 ){
		/* There is no mutex subsystem at all */
		return SXRET_OK;
	}
	SyMutexRelease(pBackend->pMutexMethods, pBackend->pMutex);
	pBackend->pMutexMethods = 0;
	pBackend->pMutex = 0; 
	return SXRET_OK;
}
#endif
/*
 * Memory pool allocator
 */
#define SXMEM_POOL_MAGIC		0xDEAD
#define SXMEM_POOL_MAXALLOC		(1<<(SXMEM_POOL_NBUCKETS+SXMEM_POOL_INCR)) 
#define SXMEM_POOL_MINALLOC		(1<<(SXMEM_POOL_INCR))
static sxi32 MemPoolBucketAlloc(SyMemBackend *pBackend, sxu32 nBucket)
{
	char *zBucket, *zBucketEnd;
	SyMemHeader *pHeader;
	sxu32 nBucketSize;
	
	/* Allocate one big block first */
	zBucket = (char *)MemBackendAlloc(&(*pBackend), SXMEM_POOL_MAXALLOC);
	if( zBucket == 0 ){
		return SXERR_MEM;
	}
	zBucketEnd = &zBucket[SXMEM_POOL_MAXALLOC];
	/* Divide the big block into mini bucket pool */
	nBucketSize = 1 << (nBucket + SXMEM_POOL_INCR);
	pBackend->apPool[nBucket] = pHeader = (SyMemHeader *)zBucket;
	for(;;){
		if( &zBucket[nBucketSize] >= zBucketEnd ){
			break;
		}
		pHeader->pNext = (SyMemHeader *)&zBucket[nBucketSize];
		/* Advance the cursor to the next available chunk */
		pHeader = pHeader->pNext;
		zBucket += nBucketSize;	
	}
	pHeader->pNext = 0;
	
	return SXRET_OK;
}
static void * MemBackendPoolAlloc(SyMemBackend *pBackend, sxu32 nByte)
{
	SyMemHeader *pBucket, *pNext;
	sxu32 nBucketSize;
	sxu32 nBucket;

	if( nByte + sizeof(SyMemHeader) >= SXMEM_POOL_MAXALLOC ){
		/* Allocate a big chunk directly */
		pBucket = (SyMemHeader *)MemBackendAlloc(&(*pBackend), nByte+sizeof(SyMemHeader));
		if( pBucket == 0 ){
			return 0;
		}
		/* Record as big block */
		pBucket->nBucket = (sxu32)(SXMEM_POOL_MAGIC << 16) | SXU16_HIGH;
		return (void *)(pBucket+1);
	}
	/* Locate the appropriate bucket */
	nBucket = 0;
	nBucketSize = SXMEM_POOL_MINALLOC;
	while( nByte + sizeof(SyMemHeader) > nBucketSize  ){
		nBucketSize <<= 1;
		nBucket++;
	}
	pBucket = pBackend->apPool[nBucket];
	if( pBucket == 0 ){
		sxi32 rc;
		rc = MemPoolBucketAlloc(&(*pBackend), nBucket);
		if( rc != SXRET_OK ){
			return 0;
		}
		pBucket = pBackend->apPool[nBucket];
	}
	/* Remove from the free list */
	pNext = pBucket->pNext;
	pBackend->apPool[nBucket] = pNext;
	/* Record bucket&magic number */
	pBucket->nBucket = (SXMEM_POOL_MAGIC << 16) | nBucket;
	return (void *)&pBucket[1];
}
VEDIS_PRIVATE void * SyMemBackendPoolAlloc(SyMemBackend *pBackend, sxu32 nByte)
{
	void *pChunk;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) ){
		return 0;
	}
#endif
	if( pBackend->pMutexMethods ){
		SyMutexEnter(pBackend->pMutexMethods, pBackend->pMutex);
	}
	pChunk = MemBackendPoolAlloc(&(*pBackend), nByte);
	if( pBackend->pMutexMethods ){
		SyMutexLeave(pBackend->pMutexMethods, pBackend->pMutex);
	}
	return pChunk;
}
static sxi32 MemBackendPoolFree(SyMemBackend *pBackend, void * pChunk)
{
	SyMemHeader *pHeader;
	sxu32 nBucket;
	/* Get the corresponding bucket */
	pHeader = (SyMemHeader *)(((char *)pChunk) - sizeof(SyMemHeader));
	/* Sanity check to avoid misuse */
	if( (pHeader->nBucket >> 16) != SXMEM_POOL_MAGIC ){
		return SXERR_CORRUPT;
	}
	nBucket = pHeader->nBucket & 0xFFFF;
	if( nBucket == SXU16_HIGH ){
		/* Free the big block */
		MemBackendFree(&(*pBackend), pHeader);
	}else{
		/* Return to the free list */
		pHeader->pNext = pBackend->apPool[nBucket & 0x0f];
		pBackend->apPool[nBucket & 0x0f] = pHeader;
	}
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyMemBackendPoolFree(SyMemBackend *pBackend, void * pChunk)
{
	sxi32 rc;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) || pChunk == 0 ){
		return SXERR_CORRUPT;
	}
#endif
	if( pBackend->pMutexMethods ){
		SyMutexEnter(pBackend->pMutexMethods, pBackend->pMutex);
	}
	rc = MemBackendPoolFree(&(*pBackend), pChunk);
	if( pBackend->pMutexMethods ){
		SyMutexLeave(pBackend->pMutexMethods, pBackend->pMutex);
	}
	return rc;
}
#if 0
static void * MemBackendPoolRealloc(SyMemBackend *pBackend, void * pOld, sxu32 nByte)
{
	sxu32 nBucket, nBucketSize;
	SyMemHeader *pHeader;
	void * pNew;

	if( pOld == 0 ){
		/* Allocate a new pool */
		pNew = MemBackendPoolAlloc(&(*pBackend), nByte);
		return pNew;
	}
	/* Get the corresponding bucket */
	pHeader = (SyMemHeader *)(((char *)pOld) - sizeof(SyMemHeader));
	/* Sanity check to avoid misuse */
	if( (pHeader->nBucket >> 16) != SXMEM_POOL_MAGIC ){
		return 0;
	}
	nBucket = pHeader->nBucket & 0xFFFF;
	if( nBucket == SXU16_HIGH ){
		/* Big block */
		return MemBackendRealloc(&(*pBackend), pHeader, nByte);
	}
	nBucketSize = 1 << (nBucket + SXMEM_POOL_INCR);
	if( nBucketSize >= nByte + sizeof(SyMemHeader) ){
		/* The old bucket can honor the requested size */
		return pOld;
	}
	/* Allocate a new pool */
	pNew = MemBackendPoolAlloc(&(*pBackend), nByte);
	if( pNew == 0 ){
		return 0;
	}
	/* Copy the old data into the new block */
	SyMemcpy(pOld, pNew, nBucketSize);
	/* Free the stale block */
	MemBackendPoolFree(&(*pBackend), pOld);
	return pNew;
}
VEDIS_PRIVATE void * SyMemBackendPoolRealloc(SyMemBackend *pBackend, void * pOld, sxu32 nByte)
{
	void *pChunk;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) ){
		return 0;
	}
#endif
	if( pBackend->pMutexMethods ){
		SyMutexEnter(pBackend->pMutexMethods, pBackend->pMutex);
	}
	pChunk = MemBackendPoolRealloc(&(*pBackend), pOld, nByte);
	if( pBackend->pMutexMethods ){
		SyMutexLeave(pBackend->pMutexMethods, pBackend->pMutex);
	}
	return pChunk;
}
#endif
VEDIS_PRIVATE sxi32 SyMemBackendInit(SyMemBackend *pBackend, ProcMemError xMemErr, void * pUserData)
{
#if defined(UNTRUST)
	if( pBackend == 0 ){
		return SXERR_EMPTY;
	}
#endif
	/* Zero the allocator first */
	SyZero(&(*pBackend), sizeof(SyMemBackend));
	pBackend->xMemError = xMemErr;
	pBackend->pUserData = pUserData;
	/* Switch to the OS memory allocator */
	pBackend->pMethods = &sOSAllocMethods;
	if( pBackend->pMethods->xInit ){
		/* Initialize the backend  */
		if( SXRET_OK != pBackend->pMethods->xInit(pBackend->pMethods->pUserData) ){
			return SXERR_ABORT;
		}
	}
#if defined(UNTRUST)
	pBackend->nMagic = SXMEM_BACKEND_MAGIC;
#endif
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyMemBackendInitFromOthers(SyMemBackend *pBackend, const SyMemMethods *pMethods, ProcMemError xMemErr, void * pUserData)
{
#if defined(UNTRUST)
	if( pBackend == 0 || pMethods == 0){
		return SXERR_EMPTY;
	}
#endif
	if( pMethods->xAlloc == 0 || pMethods->xRealloc == 0 || pMethods->xFree == 0 || pMethods->xChunkSize == 0 ){
		/* mandatory methods are missing */
		return SXERR_INVALID;
	}
	/* Zero the allocator first */
	SyZero(&(*pBackend), sizeof(SyMemBackend));
	pBackend->xMemError = xMemErr;
	pBackend->pUserData = pUserData;
	/* Switch to the host application memory allocator */
	pBackend->pMethods = pMethods;
	if( pBackend->pMethods->xInit ){
		/* Initialize the backend  */
		if( SXRET_OK != pBackend->pMethods->xInit(pBackend->pMethods->pUserData) ){
			return SXERR_ABORT;
		}
	}
#if defined(UNTRUST)
	pBackend->nMagic = SXMEM_BACKEND_MAGIC;
#endif
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyMemBackendInitFromParent(SyMemBackend *pBackend,const SyMemBackend *pParent)
{
	sxu8 bInheritMutex;
#if defined(UNTRUST)
	if( pBackend == 0 || SXMEM_BACKEND_CORRUPT(pParent) ){
		return SXERR_CORRUPT;
	}
#endif
	/* Zero the allocator first */
	SyZero(&(*pBackend), sizeof(SyMemBackend));
	pBackend->pMethods  = pParent->pMethods;
	pBackend->xMemError = pParent->xMemError;
	pBackend->pUserData = pParent->pUserData;
	bInheritMutex = pParent->pMutexMethods ? TRUE : FALSE;
	if( bInheritMutex ){
		pBackend->pMutexMethods = pParent->pMutexMethods;
		/* Create a private mutex */
		pBackend->pMutex = pBackend->pMutexMethods->xNew(SXMUTEX_TYPE_FAST);
		if( pBackend->pMutex ==  0){
			return SXERR_OS;
		}
	}
#if defined(UNTRUST)
	pBackend->nMagic = SXMEM_BACKEND_MAGIC;
#endif
	return SXRET_OK;
}
static sxi32 MemBackendRelease(SyMemBackend *pBackend)
{
	SyMemBlock *pBlock, *pNext;

	pBlock = pBackend->pBlocks;
	for(;;){
		if( pBackend->nBlock == 0 ){
			break;
		}
		pNext  = pBlock->pNext;
		pBackend->pMethods->xFree(pBlock);
		pBlock = pNext;
		pBackend->nBlock--;
		/* LOOP ONE */
		if( pBackend->nBlock == 0 ){
			break;
		}
		pNext  = pBlock->pNext;
		pBackend->pMethods->xFree(pBlock);
		pBlock = pNext;
		pBackend->nBlock--;
		/* LOOP TWO */
		if( pBackend->nBlock == 0 ){
			break;
		}
		pNext  = pBlock->pNext;
		pBackend->pMethods->xFree(pBlock);
		pBlock = pNext;
		pBackend->nBlock--;
		/* LOOP THREE */
		if( pBackend->nBlock == 0 ){
			break;
		}
		pNext  = pBlock->pNext;
		pBackend->pMethods->xFree(pBlock);
		pBlock = pNext;
		pBackend->nBlock--;
		/* LOOP FOUR */
	}
	if( pBackend->pMethods->xRelease ){
		pBackend->pMethods->xRelease(pBackend->pMethods->pUserData);
	}
	pBackend->pMethods = 0;
	pBackend->pBlocks  = 0;
#if defined(UNTRUST)
	pBackend->nMagic = 0x2626;
#endif
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyMemBackendRelease(SyMemBackend *pBackend)
{
	sxi32 rc;
#if defined(UNTRUST)
	if( SXMEM_BACKEND_CORRUPT(pBackend) ){
		return SXERR_INVALID;
	}
#endif
	if( pBackend->pMutexMethods ){
		SyMutexEnter(pBackend->pMutexMethods, pBackend->pMutex);
	}
	rc = MemBackendRelease(&(*pBackend));
	if( pBackend->pMutexMethods ){
		SyMutexLeave(pBackend->pMutexMethods, pBackend->pMutex);
		SyMutexRelease(pBackend->pMutexMethods, pBackend->pMutex);
	}
	return rc;
}
VEDIS_PRIVATE void * SyMemBackendDup(SyMemBackend *pBackend, const void *pSrc, sxu32 nSize)
{
	void *pNew;
#if defined(UNTRUST)
	if( pSrc == 0 || nSize <= 0 ){
		return 0;
	}
#endif
	pNew = SyMemBackendAlloc(&(*pBackend), nSize);
	if( pNew ){
		SyMemcpy(pSrc, pNew, nSize);
	}
	return pNew;
}
VEDIS_PRIVATE sxi32 SyBlobInitFromBuf(SyBlob *pBlob, void *pBuffer, sxu32 nSize)
{
#if defined(UNTRUST)
	if( pBlob == 0 || pBuffer == 0 || nSize < 1 ){
		return SXERR_EMPTY;
	}
#endif
	pBlob->pBlob = pBuffer;
	pBlob->mByte = nSize;
	pBlob->nByte = 0;
	pBlob->pAllocator = 0;
	pBlob->nFlags = SXBLOB_LOCKED|SXBLOB_STATIC;
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyBlobInit(SyBlob *pBlob, SyMemBackend *pAllocator)
{
#if defined(UNTRUST)
	if( pBlob == 0  ){
		return SXERR_EMPTY;
	}
#endif
	pBlob->pBlob = 0;
	pBlob->mByte = pBlob->nByte	= 0;
	pBlob->pAllocator = &(*pAllocator);
	pBlob->nFlags = 0;
	return SXRET_OK;
}
#ifndef SXBLOB_MIN_GROWTH
#define SXBLOB_MIN_GROWTH 16
#endif
static sxi32 BlobPrepareGrow(SyBlob *pBlob, sxu32 *pByte)
{
	sxu32 nByte;
	void *pNew;
	nByte = *pByte;
	if( pBlob->nFlags & (SXBLOB_LOCKED|SXBLOB_STATIC) ){
		if ( SyBlobFreeSpace(pBlob) < nByte ){
			*pByte = SyBlobFreeSpace(pBlob);
			if( (*pByte) == 0 ){
				return SXERR_SHORT;
			}
		}
		return SXRET_OK;
	}
	if( pBlob->nFlags & SXBLOB_RDONLY ){
		/* Make a copy of the read-only item */
		if( pBlob->nByte > 0 ){
			pNew = SyMemBackendDup(pBlob->pAllocator, pBlob->pBlob, pBlob->nByte);
			if( pNew == 0 ){
				return SXERR_MEM;
			}
			pBlob->pBlob = pNew;
			pBlob->mByte = pBlob->nByte;
		}else{
			pBlob->pBlob = 0;
			pBlob->mByte = 0;
		}
		/* Remove the read-only flag */
		pBlob->nFlags &= ~SXBLOB_RDONLY;
	}
	if( SyBlobFreeSpace(pBlob) >= nByte ){
		return SXRET_OK;
	}
	if( pBlob->mByte > 0 ){
		nByte = nByte + pBlob->mByte * 2 + SXBLOB_MIN_GROWTH;
	}else if ( nByte < SXBLOB_MIN_GROWTH ){
		nByte = SXBLOB_MIN_GROWTH;
	}
	pNew = SyMemBackendRealloc(pBlob->pAllocator, pBlob->pBlob, nByte);
	if( pNew == 0 ){
		return SXERR_MEM;
	}
	pBlob->pBlob = pNew;
	pBlob->mByte = nByte;
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyBlobAppend(SyBlob *pBlob, const void *pData, sxu32 nSize)
{
	sxu8 *zBlob;
	sxi32 rc;
	if( nSize < 1 ){
		return SXRET_OK;
	}
	rc = BlobPrepareGrow(&(*pBlob), &nSize);
	if( SXRET_OK != rc ){
		return rc;
	}
	if( pData ){
		zBlob = (sxu8 *)pBlob->pBlob ;
		zBlob = &zBlob[pBlob->nByte];
		pBlob->nByte += nSize;
		SX_MACRO_FAST_MEMCPY(pData, zBlob, nSize);
	}
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyBlobNullAppend(SyBlob *pBlob)
{
	sxi32 rc;
	sxu32 n;
	n = pBlob->nByte;
	rc = SyBlobAppend(&(*pBlob), (const void *)"\0", sizeof(char));
	if (rc == SXRET_OK ){
		pBlob->nByte = n;
	}
	return rc;
}
VEDIS_PRIVATE sxi32 SyBlobDup(SyBlob *pSrc, SyBlob *pDest)
{
	sxi32 rc = SXRET_OK;
	if( pSrc->nByte > 0 ){
		rc = SyBlobAppend(&(*pDest), pSrc->pBlob, pSrc->nByte);
	}
	return rc;
}
VEDIS_PRIVATE sxi32 SyBlobReset(SyBlob *pBlob)
{
	pBlob->nByte = 0;
	if( pBlob->nFlags & SXBLOB_RDONLY ){
		/* Read-only (Not malloced chunk) */
		pBlob->pBlob = 0;
		pBlob->mByte = 0;
		pBlob->nFlags &= ~SXBLOB_RDONLY;
	}
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyBlobRelease(SyBlob *pBlob)
{
	if( (pBlob->nFlags & (SXBLOB_STATIC|SXBLOB_RDONLY)) == 0 && pBlob->mByte > 0 ){
		SyMemBackendFree(pBlob->pAllocator, pBlob->pBlob);
	}
	pBlob->pBlob = 0;
	pBlob->nByte = pBlob->mByte = 0;
	pBlob->nFlags = 0;
	return SXRET_OK;
}
/* SyRunTimeApi:sxds.c */
VEDIS_PRIVATE sxi32 SySetInit(SySet *pSet, SyMemBackend *pAllocator, sxu32 ElemSize)
{
	pSet->nSize = 0 ;
	pSet->nUsed = 0;
	pSet->nCursor = 0;
	pSet->eSize = ElemSize;
	pSet->pAllocator = pAllocator;
	pSet->pBase =  0;
	pSet->pUserData = 0;
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SySetPut(SySet *pSet, const void *pItem)
{
	unsigned char *zbase;
	if( pSet->nUsed >= pSet->nSize ){
		void *pNew;
		if( pSet->pAllocator == 0 ){
			return  SXERR_LOCKED;
		}
		if( pSet->nSize <= 0 ){
			pSet->nSize = 4;
		}
		pNew = SyMemBackendRealloc(pSet->pAllocator, pSet->pBase, pSet->eSize * pSet->nSize * 2);
		if( pNew == 0 ){
			return SXERR_MEM;
		}
		pSet->pBase = pNew;
		pSet->nSize <<= 1;
	}
	zbase = (unsigned char *)pSet->pBase;
	SX_MACRO_FAST_MEMCPY(pItem, &zbase[pSet->nUsed * pSet->eSize], pSet->eSize);
	pSet->nUsed++;	
	return SXRET_OK;
} 
VEDIS_PRIVATE sxi32 SySetReset(SySet *pSet)
{
	pSet->nUsed   = 0;
	pSet->nCursor = 0;
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SySetRelease(SySet *pSet)
{
	sxi32 rc = SXRET_OK;
	if( pSet->pAllocator && pSet->pBase ){
		rc = SyMemBackendFree(pSet->pAllocator, pSet->pBase);
	}
	pSet->pBase = 0;
	pSet->nUsed = 0;
	pSet->nCursor = 0;
	return rc;
}
VEDIS_PRIVATE void * SySetPeek(SySet *pSet)
{
	const char *zBase;
	if( pSet->nUsed <= 0 ){
		return 0;
	}
	zBase = (const char *)pSet->pBase;
	return (void *)&zBase[(pSet->nUsed - 1) * pSet->eSize]; 
}
VEDIS_PRIVATE void * SySetPop(SySet *pSet)
{
	const char *zBase;
	void *pData;
	if( pSet->nUsed <= 0 ){
		return 0;
	}
	zBase = (const char *)pSet->pBase;
	pSet->nUsed--;
	pData =  (void *)&zBase[pSet->nUsed * pSet->eSize]; 
	return pData;
}
/* SyRunTimeApi:sxutils.c */
VEDIS_PRIVATE sxi32 SyStrIsNumeric(const char *zSrc, sxu32 nLen, sxu8 *pReal, const char  **pzTail)
{
	const char *zCur, *zEnd;
#ifdef UNTRUST
	if( SX_EMPTY_STR(zSrc) ){
		return SXERR_EMPTY;
	}
#endif
	zEnd = &zSrc[nLen];
	/* Jump leading white spaces */
	while( zSrc < zEnd && (unsigned char)zSrc[0] < 0xc0  && SyisSpace(zSrc[0]) ){
		zSrc++;
	}
	if( zSrc < zEnd && (zSrc[0] == '+' || zSrc[0] == '-') ){
		zSrc++;
	}
	zCur = zSrc;
	if( pReal ){
		*pReal = FALSE;
	}
	for(;;){
		if( zSrc >= zEnd || (unsigned char)zSrc[0] >= 0xc0 || !SyisDigit(zSrc[0]) ){ break; } zSrc++;
		if( zSrc >= zEnd || (unsigned char)zSrc[0] >= 0xc0 || !SyisDigit(zSrc[0]) ){ break; } zSrc++;
		if( zSrc >= zEnd || (unsigned char)zSrc[0] >= 0xc0 || !SyisDigit(zSrc[0]) ){ break; } zSrc++;
		if( zSrc >= zEnd || (unsigned char)zSrc[0] >= 0xc0 || !SyisDigit(zSrc[0]) ){ break; } zSrc++;
	};
	if( zSrc < zEnd && zSrc > zCur ){
		int c = zSrc[0];
		if( c == '.' ){
			zSrc++;
			if( pReal ){
				*pReal = TRUE;
			}
			if( pzTail ){
				while( zSrc < zEnd && (unsigned char)zSrc[0] < 0xc0 && SyisDigit(zSrc[0]) ){
					zSrc++;
				}
				if( zSrc < zEnd && (zSrc[0] == 'e' || zSrc[0] == 'E') ){
					zSrc++;
					if( zSrc < zEnd && (zSrc[0] == '+' || zSrc[0] == '-') ){
						zSrc++;
					}
					while( zSrc < zEnd && (unsigned char)zSrc[0] < 0xc0 && SyisDigit(zSrc[0]) ){
						zSrc++;
					}
				}
			}
		}else if( c == 'e' || c == 'E' ){
			zSrc++;
			if( pReal ){
				*pReal = TRUE;
			}
			if( pzTail ){
				if( zSrc < zEnd && (zSrc[0] == '+' || zSrc[0] == '-') ){
					zSrc++;
				}
				while( zSrc < zEnd && (unsigned char)zSrc[0] < 0xc0 && SyisDigit(zSrc[0]) ){
					zSrc++;
				}
			}
		}
	}
	if( pzTail ){
		/* Point to the non numeric part */
		*pzTail = zSrc;
	}
	return zSrc > zCur ? SXRET_OK /* String prefix is numeric */ : SXERR_INVALID /* Not a digit stream */;
}
#define SXINT32_MIN_STR		"2147483648"
#define SXINT32_MAX_STR		"2147483647"
#define SXINT64_MIN_STR		"9223372036854775808"
#define SXINT64_MAX_STR		"9223372036854775807"
VEDIS_PRIVATE sxi32 SyStrToInt64(const char *zSrc, sxu32 nLen, void * pOutVal, const char **zRest)
{
	int isNeg = FALSE;
	const char *zEnd;
	sxi64 nVal;
	sxi16 i;
#if defined(UNTRUST)
	if( SX_EMPTY_STR(zSrc) ){
		if( pOutVal ){
			*(sxi32 *)pOutVal = 0;
		}
		return SXERR_EMPTY;
	}
#endif
	zEnd = &zSrc[nLen];
	while(zSrc < zEnd && SyisSpace(zSrc[0]) ){
		zSrc++;
	}
	if( zSrc < zEnd && ( zSrc[0] == '-' || zSrc[0] == '+' ) ){
		isNeg = (zSrc[0] == '-') ? TRUE :FALSE;
		zSrc++;
	}
	/* Skip leading zero */
	while(zSrc < zEnd && zSrc[0] == '0' ){
		zSrc++;
	}
	i = 19;
	if( (sxu32)(zEnd-zSrc) >= 19 ){
		i = SyMemcmp(zSrc, isNeg ? SXINT64_MIN_STR : SXINT64_MAX_STR, 19) <= 0 ? 19 : 18 ;
	}
	nVal = 0;
	for(;;){
		if(zSrc >= zEnd || !i || !SyisDigit(zSrc[0])){ break; } nVal = nVal * 10 + ( zSrc[0] - '0' ) ; --i ; zSrc++;
		if(zSrc >= zEnd || !i || !SyisDigit(zSrc[0])){ break; } nVal = nVal * 10 + ( zSrc[0] - '0' ) ; --i ; zSrc++;
		if(zSrc >= zEnd || !i || !SyisDigit(zSrc[0])){ break; } nVal = nVal * 10 + ( zSrc[0] - '0' ) ; --i ; zSrc++;
		if(zSrc >= zEnd || !i || !SyisDigit(zSrc[0])){ break; } nVal = nVal * 10 + ( zSrc[0] - '0' ) ; --i ; zSrc++;
	}
	/* Skip trailing spaces */
	while(zSrc < zEnd && SyisSpace(zSrc[0])){
		zSrc++;
	}
	if( zRest ){
		*zRest = (char *)zSrc;
	}	
	if( pOutVal ){
		if( isNeg == TRUE && nVal != 0 ){
			nVal = -nVal;
		}
		*(sxi64 *)pOutVal = nVal;
	}
	return (zSrc >= zEnd) ? SXRET_OK : SXERR_SYNTAX;
}
VEDIS_PRIVATE sxi32 SyHexToint(sxi32 c)
{
	switch(c){
	case '0': return 0;
	case '1': return 1;
	case '2': return 2;
	case '3': return 3;
	case '4': return 4;
	case '5': return 5;
	case '6': return 6;
	case '7': return 7;
	case '8': return 8;
	case '9': return 9;
	case 'A': case 'a': return 10;
	case 'B': case 'b': return 11;
	case 'C': case 'c': return 12;
	case 'D': case 'd': return 13;
	case 'E': case 'e': return 14;
	case 'F': case 'f': return 15;
	}
	return -1; 	
}
VEDIS_PRIVATE sxi32 SyHexStrToInt64(const char *zSrc, sxu32 nLen, void * pOutVal, const char **zRest)
{
	const char *zIn, *zEnd;
	int isNeg = FALSE;
	sxi64 nVal = 0;
#if defined(UNTRUST)
	if( SX_EMPTY_STR(zSrc) ){
		if( pOutVal ){
			*(sxi32 *)pOutVal = 0;
		}
		return SXERR_EMPTY;
	}
#endif
	zEnd = &zSrc[nLen];
	while( zSrc < zEnd && SyisSpace(zSrc[0]) ){
		zSrc++;
	}
	if( zSrc < zEnd && ( *zSrc == '-' || *zSrc == '+' ) ){
		isNeg = (zSrc[0] == '-') ? TRUE :FALSE;
		zSrc++;
	}
	if( zSrc < &zEnd[-2] && zSrc[0] == '0' && (zSrc[1] == 'x' || zSrc[1] == 'X') ){
		/* Bypass hex prefix */
		zSrc += sizeof(char) * 2;
	}	
	/* Skip leading zero */
	while(zSrc < zEnd && zSrc[0] == '0' ){
		zSrc++;
	}
	zIn = zSrc;
	for(;;){
		if(zSrc >= zEnd || !SyisHex(zSrc[0]) || (int)(zSrc-zIn) > 15) break; nVal = nVal * 16 + SyHexToint(zSrc[0]);  zSrc++ ;
		if(zSrc >= zEnd || !SyisHex(zSrc[0]) || (int)(zSrc-zIn) > 15) break; nVal = nVal * 16 + SyHexToint(zSrc[0]);  zSrc++ ;
		if(zSrc >= zEnd || !SyisHex(zSrc[0]) || (int)(zSrc-zIn) > 15) break; nVal = nVal * 16 + SyHexToint(zSrc[0]);  zSrc++ ;
		if(zSrc >= zEnd || !SyisHex(zSrc[0]) || (int)(zSrc-zIn) > 15) break; nVal = nVal * 16 + SyHexToint(zSrc[0]);  zSrc++ ;
	}
	while( zSrc < zEnd && SyisSpace(zSrc[0]) ){
		zSrc++;
	}	
	if( zRest ){
		*zRest = zSrc;
	}
	if( pOutVal ){
		if( isNeg == TRUE && nVal != 0 ){
			nVal = -nVal;
		}
		*(sxi64 *)pOutVal = nVal;
	}
	return zSrc >= zEnd ? SXRET_OK : SXERR_SYNTAX;
}
VEDIS_PRIVATE sxi32 SyOctalStrToInt64(const char *zSrc, sxu32 nLen, void * pOutVal, const char **zRest)
{
	const char *zIn, *zEnd;
	int isNeg = FALSE;
	sxi64 nVal = 0;
	int c;
#if defined(UNTRUST)
	if( SX_EMPTY_STR(zSrc) ){
		if( pOutVal ){
			*(sxi32 *)pOutVal = 0;
		}
		return SXERR_EMPTY;
	}
#endif
	zEnd = &zSrc[nLen];
	while(zSrc < zEnd && SyisSpace(zSrc[0]) ){
		zSrc++;
	}
	if( zSrc < zEnd && ( zSrc[0] == '-' || zSrc[0] == '+' ) ){
		isNeg = (zSrc[0] == '-') ? TRUE :FALSE;
		zSrc++;
	}
	/* Skip leading zero */
	while(zSrc < zEnd && zSrc[0] == '0' ){
		zSrc++; 
	}
	zIn = zSrc;
	for(;;){
		if(zSrc >= zEnd || !SyisDigit(zSrc[0])){ break; } if( (c=zSrc[0]-'0') > 7 || (int)(zSrc-zIn) > 20){ break;} nVal = nVal * 8 +  c; zSrc++;
		if(zSrc >= zEnd || !SyisDigit(zSrc[0])){ break; } if( (c=zSrc[0]-'0') > 7 || (int)(zSrc-zIn) > 20){ break;} nVal = nVal * 8 +  c; zSrc++;
		if(zSrc >= zEnd || !SyisDigit(zSrc[0])){ break; } if( (c=zSrc[0]-'0') > 7 || (int)(zSrc-zIn) > 20){ break;} nVal = nVal * 8 +  c; zSrc++;
		if(zSrc >= zEnd || !SyisDigit(zSrc[0])){ break; } if( (c=zSrc[0]-'0') > 7 || (int)(zSrc-zIn) > 20){ break;} nVal = nVal * 8 +  c; zSrc++;
	}
	/* Skip trailing spaces */
	while(zSrc < zEnd && SyisSpace(zSrc[0])){
		zSrc++;
	}
	if( zRest ){
		*zRest = zSrc;
	}	
	if( pOutVal ){
		if( isNeg == TRUE && nVal != 0 ){
			nVal = -nVal;
		}
		*(sxi64 *)pOutVal = nVal;
	}
	return (zSrc >= zEnd) ? SXRET_OK : SXERR_SYNTAX;
}
VEDIS_PRIVATE sxi32 SyBinaryStrToInt64(const char *zSrc, sxu32 nLen, void * pOutVal, const char **zRest)
{
	const char *zIn, *zEnd;
	int isNeg = FALSE;
	sxi64 nVal = 0;
	int c;
#if defined(UNTRUST)
	if( SX_EMPTY_STR(zSrc) ){
		if( pOutVal ){
			*(sxi32 *)pOutVal = 0;
		}
		return SXERR_EMPTY;
	}
#endif
	zEnd = &zSrc[nLen];
	while(zSrc < zEnd && SyisSpace(zSrc[0]) ){
		zSrc++;
	}
	if( zSrc < zEnd && ( zSrc[0] == '-' || zSrc[0] == '+' ) ){
		isNeg = (zSrc[0] == '-') ? TRUE :FALSE;
		zSrc++;
	}
	if( zSrc < &zEnd[-2] && zSrc[0] == '0' && (zSrc[1] == 'b' || zSrc[1] == 'B') ){
		/* Bypass binary prefix */
		zSrc += sizeof(char) * 2;
	}
	/* Skip leading zero */
	while(zSrc < zEnd && zSrc[0] == '0' ){
		zSrc++; 
	}
	zIn = zSrc;
	for(;;){
		if(zSrc >= zEnd || (zSrc[0] != '1' && zSrc[0] != '0') || (int)(zSrc-zIn) > 62){ break; } c = zSrc[0] - '0'; nVal = (nVal << 1) + c; zSrc++;
		if(zSrc >= zEnd || (zSrc[0] != '1' && zSrc[0] != '0') || (int)(zSrc-zIn) > 62){ break; } c = zSrc[0] - '0'; nVal = (nVal << 1) + c; zSrc++;
		if(zSrc >= zEnd || (zSrc[0] != '1' && zSrc[0] != '0') || (int)(zSrc-zIn) > 62){ break; } c = zSrc[0] - '0'; nVal = (nVal << 1) + c; zSrc++;
		if(zSrc >= zEnd || (zSrc[0] != '1' && zSrc[0] != '0') || (int)(zSrc-zIn) > 62){ break; } c = zSrc[0] - '0'; nVal = (nVal << 1) + c; zSrc++;
	}
	/* Skip trailing spaces */
	while(zSrc < zEnd && SyisSpace(zSrc[0])){
		zSrc++;
	}
	if( zRest ){
		*zRest = zSrc;
	}	
	if( pOutVal ){
		if( isNeg == TRUE && nVal != 0 ){
			nVal = -nVal;
		}
		*(sxi64 *)pOutVal = nVal;
	}
	return (zSrc >= zEnd) ? SXRET_OK : SXERR_SYNTAX;
}
VEDIS_PRIVATE sxi32 SyStrToReal(const char *zSrc, sxu32 nLen, void * pOutVal, const char **zRest)
{
#define SXDBL_DIG        15
#define SXDBL_MAX_EXP    308
#define SXDBL_MIN_EXP_PLUS	307
	static const sxreal aTab[] = {
	10, 
	1.0e2, 
	1.0e4, 
	1.0e8, 
	1.0e16, 
	1.0e32, 
	1.0e64, 
	1.0e128, 
	1.0e256
	};
	sxu8 neg = FALSE;
	sxreal Val = 0.0;
	const char *zEnd;
	sxi32 Lim, exp;
	sxreal *p = 0;
#ifdef UNTRUST
	if( SX_EMPTY_STR(zSrc)  ){
		if( pOutVal ){
			*(sxreal *)pOutVal = 0.0;
		}
		return SXERR_EMPTY;
	}
#endif
	zEnd = &zSrc[nLen];
	while( zSrc < zEnd && SyisSpace(zSrc[0]) ){
		zSrc++; 
	}
	if( zSrc < zEnd && (zSrc[0] == '-' || zSrc[0] == '+' ) ){
		neg =  zSrc[0] == '-' ? TRUE : FALSE ;
		zSrc++;
	}
	Lim = SXDBL_DIG ;
	for(;;){
		if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; zSrc++ ; --Lim;
		if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; zSrc++ ; --Lim;
		if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; zSrc++ ; --Lim;
		if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; zSrc++ ; --Lim;
	}
	if( zSrc < zEnd && ( zSrc[0] == '.' || zSrc[0] == ',' ) ){
		sxreal dec = 1.0;
		zSrc++;
		for(;;){
			if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; dec *= 10.0; zSrc++ ;--Lim;
			if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; dec *= 10.0; zSrc++ ;--Lim;
			if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; dec *= 10.0; zSrc++ ;--Lim;
			if(zSrc >= zEnd||!Lim||!SyisDigit(zSrc[0])) break ; Val = Val * 10.0 + (zSrc[0] - '0') ; dec *= 10.0; zSrc++ ;--Lim;
		}
		Val /= dec;
	}
	if( neg == TRUE && Val != 0.0 ) {
		Val = -Val ; 
	}
	if( Lim <= 0 ){
		/* jump overflow digit */
		while( zSrc < zEnd ){
			if( zSrc[0] == 'e' || zSrc[0] == 'E' ){
				break;  
			}
			zSrc++;
		}
	}
	neg = FALSE;
	if( zSrc < zEnd && ( zSrc[0] == 'e' || zSrc[0] == 'E' ) ){
		zSrc++;
		if( zSrc < zEnd && ( zSrc[0] == '-' || zSrc[0] == '+') ){
			neg = zSrc[0] == '-' ? TRUE : FALSE ;
			zSrc++;
		}
		exp = 0;
		while( zSrc < zEnd && SyisDigit(zSrc[0]) && exp < SXDBL_MAX_EXP ){
			exp = exp * 10 + (zSrc[0] - '0');
			zSrc++;
		}
		if( neg  ){
			if( exp > SXDBL_MIN_EXP_PLUS ) exp = SXDBL_MIN_EXP_PLUS ;
		}else if ( exp > SXDBL_MAX_EXP ){
			exp = SXDBL_MAX_EXP; 
		}		
		for( p = (sxreal *)aTab ; exp ; exp >>= 1 , p++ ){
			if( exp & 01 ){
				if( neg ){
					Val /= *p ;
				}else{
					Val *= *p;
				}
			}
		}
	}
	while( zSrc < zEnd && SyisSpace(zSrc[0]) ){
		zSrc++;
	}
	if( zRest ){
		*zRest = zSrc; 
	}
	if( pOutVal ){
		*(sxreal *)pOutVal = Val;
	}
	return zSrc >= zEnd ? SXRET_OK : SXERR_SYNTAX;
}
/* SyRunTimeApi:sxlib.c  */
VEDIS_PRIVATE sxu32 SyBinHash(const void *pSrc, sxu32 nLen)
{
	register unsigned char *zIn = (unsigned char *)pSrc;
	unsigned char *zEnd;
	sxu32 nH = 5381;
	zEnd = &zIn[nLen];
	for(;;){
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
	}	
	return nH;
}
VEDIS_PRIVATE sxi32 SyBase64Encode(const char *zSrc, sxu32 nLen, ProcConsumer xConsumer, void *pUserData)
{
	static const unsigned char zBase64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	unsigned char *zIn = (unsigned char *)zSrc;
	unsigned char z64[4];
	sxu32 i;
	sxi32 rc;
#if defined(UNTRUST)
	if( SX_EMPTY_STR(zSrc) || xConsumer == 0){
		return SXERR_EMPTY;
	}
#endif
	for(i = 0; i + 2 < nLen; i += 3){
		z64[0] = zBase64[(zIn[i] >> 2) & 0x3F];
		z64[1] = zBase64[( ((zIn[i] & 0x03) << 4)   | (zIn[i+1] >> 4)) & 0x3F]; 
		z64[2] = zBase64[( ((zIn[i+1] & 0x0F) << 2) | (zIn[i + 2] >> 6) ) & 0x3F];
		z64[3] = zBase64[ zIn[i + 2] & 0x3F];
		
		rc = xConsumer((const void *)z64, sizeof(z64), pUserData);
		if( rc != SXRET_OK ){return SXERR_ABORT;}

	}	
	if ( i+1 < nLen ){
		z64[0] = zBase64[(zIn[i] >> 2) & 0x3F];
		z64[1] = zBase64[( ((zIn[i] & 0x03) << 4)   | (zIn[i+1] >> 4)) & 0x3F]; 
		z64[2] = zBase64[(zIn[i+1] & 0x0F) << 2 ];
		z64[3] = '=';
		
		rc = xConsumer((const void *)z64, sizeof(z64), pUserData);
		if( rc != SXRET_OK ){return SXERR_ABORT;}

	}else if( i < nLen ){
		z64[0] = zBase64[(zIn[i] >> 2) & 0x3F];
		z64[1]   = zBase64[(zIn[i] & 0x03) << 4];
		z64[2] = '=';
		z64[3] = '=';
		
		rc = xConsumer((const void *)z64, sizeof(z64), pUserData);
		if( rc != SXRET_OK ){return SXERR_ABORT;}
	}

	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyBase64Decode(const char *zB64, sxu32 nLen, ProcConsumer xConsumer, void *pUserData)
{
	static const sxu32 aBase64Trans[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 62, 0, 0, 0, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 
	5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 0, 0, 0, 0, 0, 0, 26, 27, 
	28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 0, 0, 
	0, 0, 0
	};
	sxu32 n, w, x, y, z;
	sxi32 rc;
	unsigned char zOut[10];
#if defined(UNTRUST)
	if( SX_EMPTY_STR(zB64) || xConsumer == 0 ){
		return SXERR_EMPTY;
	}
#endif
	while(nLen > 0 && zB64[nLen - 1] == '=' ){
		nLen--;
	}
	for( n = 0 ; n+3<nLen ; n += 4){
		w = aBase64Trans[zB64[n] & 0x7F];
		x = aBase64Trans[zB64[n+1] & 0x7F];
		y = aBase64Trans[zB64[n+2] & 0x7F];
		z = aBase64Trans[zB64[n+3] & 0x7F];
		zOut[0] = ((w<<2) & 0xFC) | ((x>>4) & 0x03);
		zOut[1] = ((x<<4) & 0xF0) | ((y>>2) & 0x0F);
		zOut[2] = ((y<<6) & 0xC0) | (z & 0x3F);

		rc = xConsumer((const void *)zOut, sizeof(unsigned char)*3, pUserData);
		if( rc != SXRET_OK ){ return SXERR_ABORT;}
	}
	if( n+2 < nLen ){
		w = aBase64Trans[zB64[n] & 0x7F];
		x = aBase64Trans[zB64[n+1] & 0x7F];
		y = aBase64Trans[zB64[n+2] & 0x7F];

		zOut[0] = ((w<<2) & 0xFC) | ((x>>4) & 0x03);
		zOut[1] = ((x<<4) & 0xF0) | ((y>>2) & 0x0F);

		rc = xConsumer((const void *)zOut, sizeof(unsigned char)*2, pUserData);
		if( rc != SXRET_OK ){ return SXERR_ABORT;}
	}else if( n+1 < nLen ){
		w = aBase64Trans[zB64[n] & 0x7F];
		x = aBase64Trans[zB64[n+1] & 0x7F];

		zOut[0] = ((w<<2) & 0xFC) | ((x>>4) & 0x03);

		rc = xConsumer((const void *)zOut, sizeof(unsigned char)*1, pUserData);
		if( rc != SXRET_OK ){ return SXERR_ABORT;}
	}
	return SXRET_OK;
}

#define INVALID_LEXER(LEX)	(  LEX == 0  || LEX->xTokenizer == 0 )
VEDIS_PRIVATE sxi32 SyLexInit(SyLex *pLex, SySet *pSet, ProcTokenizer xTokenizer, void *pUserData)
{
	SyStream *pStream;
#if defined (UNTRUST)
	if ( pLex == 0 || xTokenizer == 0 ){
		return SXERR_CORRUPT;
	}
#endif
	pLex->pTokenSet = 0;
	/* Initialize lexer fields */
	if( pSet ){
		if ( SySetElemSize(pSet) != sizeof(SyToken) ){
			return SXERR_INVALID;
		}
		pLex->pTokenSet = pSet;
	}
	pStream = &pLex->sStream;
	pLex->xTokenizer = xTokenizer;
	pLex->pUserData = pUserData;
	
	pStream->nLine = 1;
	pStream->nIgn  = 0;
	pStream->zText = pStream->zEnd = 0;
	pStream->pSet  = pSet;
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyLexTokenizeInput(SyLex *pLex, const char *zInput, sxu32 nLen, void *pCtxData, ProcSort xSort, ProcCmp xCmp)
{
	const unsigned char *zCur;
	SyStream *pStream;
	SyToken sToken;
	sxi32 rc;
#if defined (UNTRUST)
	if ( INVALID_LEXER(pLex) || zInput == 0 ){
		return SXERR_CORRUPT;
	}
#endif
	pStream = &pLex->sStream;
	/* Point to the head of the input */
	pStream->zText = pStream->zInput = (const unsigned char *)zInput;
	/* Point to the end of the input */
	pStream->zEnd = &pStream->zInput[nLen];
	for(;;){
		if( pStream->zText >= pStream->zEnd ){
			/* End of the input reached */
			break;
		}
		zCur = pStream->zText;
		/* Call the tokenizer callback */
		rc = pLex->xTokenizer(pStream, &sToken, pLex->pUserData, pCtxData);
		if( rc != SXRET_OK && rc != SXERR_CONTINUE ){
			/* Tokenizer callback request an operation abort */
			if( rc == SXERR_ABORT ){
				return SXERR_ABORT;
			}
			break;
		}
		if( rc == SXERR_CONTINUE ){
			/* Request to ignore this token */
			pStream->nIgn++;
		}else if( pLex->pTokenSet  ){
			/* Put the token in the set */
			rc = SySetPut(pLex->pTokenSet, (const void *)&sToken);
			if( rc != SXRET_OK ){
				break;
			}
		}
		if( zCur >= pStream->zText ){
			/* Automatic advance of the stream cursor */
			pStream->zText = &zCur[1];
		}
	}
	if( xSort &&  pLex->pTokenSet ){
		SyToken *aToken = (SyToken *)SySetBasePtr(pLex->pTokenSet);
		/* Sort the extrated tokens */
		if( xCmp == 0 ){
			/* Use a default comparison function */
			xCmp = SyMemcmp;
		}
		xSort(aToken, SySetUsed(pLex->pTokenSet), sizeof(SyToken), xCmp);
	}
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyLexRelease(SyLex *pLex)
{
	sxi32 rc = SXRET_OK;
#if defined (UNTRUST)
	if ( INVALID_LEXER(pLex) ){
		return SXERR_CORRUPT;
	}
#else
	SXUNUSED(pLex); /* Prevent compiler warning */
#endif
	return rc;
}
/* SyRunTimeApi: sxfmt.c */
#define SXFMT_BUFSIZ 1024 /* Conversion buffer size */
/*
** Conversion types fall into various categories as defined by the
** following enumeration.
*/
#define SXFMT_RADIX       1 /* Integer types.%d, %x, %o, and so forth */
#define SXFMT_FLOAT       2 /* Floating point.%f */
#define SXFMT_EXP         3 /* Exponentional notation.%e and %E */
#define SXFMT_GENERIC     4 /* Floating or exponential, depending on exponent.%g */
#define SXFMT_SIZE        5 /* Total number of characters processed so far.%n */
#define SXFMT_STRING      6 /* Strings.%s */
#define SXFMT_PERCENT     7 /* Percent symbol.%% */
#define SXFMT_CHARX       8 /* Characters.%c */
#define SXFMT_ERROR       9 /* Used to indicate no such conversion type */
/* Extension by Symisc Systems */
#define SXFMT_RAWSTR     13 /* %z Pointer to raw string (SyString *) */
#define SXFMT_UNUSED     15 
/*
** Allowed values for SyFmtInfo.flags
*/
#define SXFLAG_SIGNED	0x01
#define SXFLAG_UNSIGNED 0x02
/* Allowed values for SyFmtConsumer.nType */
#define SXFMT_CONS_PROC		1	/* Consumer is a procedure */
#define SXFMT_CONS_STR		2	/* Consumer is a managed string */
#define SXFMT_CONS_FILE		5	/* Consumer is an open File */
#define SXFMT_CONS_BLOB		6	/* Consumer is a BLOB */
/*
** Each builtin conversion character (ex: the 'd' in "%d") is described
** by an instance of the following structure
*/
typedef struct SyFmtInfo SyFmtInfo;
struct SyFmtInfo
{
  char fmttype;  /* The format field code letter [i.e: 'd', 's', 'x'] */
  sxu8 base;     /* The base for radix conversion */
  int flags;    /* One or more of SXFLAG_ constants below */
  sxu8 type;     /* Conversion paradigm */
  char *charset; /* The character set for conversion */
  char *prefix;  /* Prefix on non-zero values in alt format */
};
typedef struct SyFmtConsumer SyFmtConsumer;
struct SyFmtConsumer
{
	sxu32 nLen; /* Total output length */
	sxi32 nType; /* Type of the consumer see below */
	sxi32 rc;	/* Consumer return value;Abort processing if rc != SXRET_OK */
 union{
	struct{	
	ProcConsumer xUserConsumer;
	void *pUserData;
	}sFunc;  
	SyBlob *pBlob;
 }uConsumer;	
}; 
#ifndef SX_OMIT_FLOATINGPOINT
static int getdigit(sxlongreal *val, int *cnt)
{
  sxlongreal d;
  int digit;

  if( (*cnt)++ >= 16 ){
	  return '0';
  }
  digit = (int)*val;
  d = digit;
   *val = (*val - d)*10.0;
  return digit + '0' ;
}
#endif /* SX_OMIT_FLOATINGPOINT */
/*
 * The following routine was taken from the SQLITE2 source tree and was
 * extended by Symisc Systems to fit its need.
 * Status: Public Domain
 */
static sxi32 InternFormat(ProcConsumer xConsumer, void *pUserData, const char *zFormat, va_list ap)
{
	/*
	 * The following table is searched linearly, so it is good to put the most frequently
	 * used conversion types first.
	 */
static const SyFmtInfo aFmt[] = {
  {  'd', 10, SXFLAG_SIGNED, SXFMT_RADIX, "0123456789", 0    }, 
  {  's',  0, 0, SXFMT_STRING,     0,                  0    }, 
  {  'c',  0, 0, SXFMT_CHARX,      0,                  0    }, 
  {  'x', 16, 0, SXFMT_RADIX,      "0123456789abcdef", "x0" }, 
  {  'X', 16, 0, SXFMT_RADIX,      "0123456789ABCDEF", "X0" }, 
         /* -- Extensions by Symisc Systems -- */
  {  'z',  0, 0, SXFMT_RAWSTR,     0,                   0   }, /* Pointer to a raw string (SyString *) */
  {  'B',  2, 0, SXFMT_RADIX,      "01",                "b0"}, 
         /* -- End of Extensions -- */
  {  'o',  8, 0, SXFMT_RADIX,      "01234567",         "0"  }, 
  {  'u', 10, 0, SXFMT_RADIX,      "0123456789",       0    }, 
#ifndef SX_OMIT_FLOATINGPOINT
  {  'f',  0, SXFLAG_SIGNED, SXFMT_FLOAT,       0,     0    }, 
  {  'e',  0, SXFLAG_SIGNED, SXFMT_EXP,        "e",    0    }, 
  {  'E',  0, SXFLAG_SIGNED, SXFMT_EXP,        "E",    0    }, 
  {  'g',  0, SXFLAG_SIGNED, SXFMT_GENERIC,    "e",    0    }, 
  {  'G',  0, SXFLAG_SIGNED, SXFMT_GENERIC,    "E",    0    }, 
#endif
  {  'i', 10, SXFLAG_SIGNED, SXFMT_RADIX, "0123456789", 0    }, 
  {  'n',  0, 0, SXFMT_SIZE,       0,                  0    }, 
  {  '%',  0, 0, SXFMT_PERCENT,    0,                  0    }, 
  {  'p', 10, 0, SXFMT_RADIX,      "0123456789",       0    }
};
  int c;                     /* Next character in the format string */
  char *bufpt;               /* Pointer to the conversion buffer */
  int precision;             /* Precision of the current field */
  int length;                /* Length of the field */
  int idx;                   /* A general purpose loop counter */
  int width;                 /* Width of the current field */
  sxu8 flag_leftjustify;   /* True if "-" flag is present */
  sxu8 flag_plussign;      /* True if "+" flag is present */
  sxu8 flag_blanksign;     /* True if " " flag is present */
  sxu8 flag_alternateform; /* True if "#" flag is present */
  sxu8 flag_zeropad;       /* True if field width constant starts with zero */
  sxu8 flag_long;          /* True if "l" flag is present */
  sxi64 longvalue;         /* Value for integer types */
  const SyFmtInfo *infop;  /* Pointer to the appropriate info structure */
  char buf[SXFMT_BUFSIZ];  /* Conversion buffer */
  char prefix;             /* Prefix character."+" or "-" or " " or '\0'.*/
  sxu8 errorflag = 0;      /* True if an error is encountered */
  sxu8 xtype;              /* Conversion paradigm */
  char *zExtra;    
  static char spaces[] = "                                                  ";
#define etSPACESIZE ((int)sizeof(spaces)-1)
#ifndef SX_OMIT_FLOATINGPOINT
  sxlongreal realvalue;    /* Value for real types */
  int  exp;                /* exponent of real numbers */
  double rounder;          /* Used for rounding floating point values */
  sxu8 flag_dp;            /* True if decimal point should be shown */
  sxu8 flag_rtz;           /* True if trailing zeros should be removed */
  sxu8 flag_exp;           /* True to force display of the exponent */
  int nsd;                 /* Number of significant digits returned */
#endif
  int rc;

  length = 0;
  bufpt = 0;
  for(; (c=(*zFormat))!=0; ++zFormat){
    if( c!='%' ){
      unsigned int amt;
      bufpt = (char *)zFormat;
      amt = 1;
      while( (c=(*++zFormat))!='%' && c!=0 ) amt++;
	  rc = xConsumer((const void *)bufpt, amt, pUserData);
	  if( rc != SXRET_OK ){
		  return SXERR_ABORT; /* Consumer routine request an operation abort */
	  }
      if( c==0 ){
		  return errorflag > 0 ? SXERR_FORMAT : SXRET_OK;
	  }
    }
    if( (c=(*++zFormat))==0 ){
      errorflag = 1;
	  rc = xConsumer("%", sizeof("%")-1, pUserData);
	  if( rc != SXRET_OK ){
		  return SXERR_ABORT; /* Consumer routine request an operation abort */
	  }
      return errorflag > 0 ? SXERR_FORMAT : SXRET_OK;
    }
    /* Find out what flags are present */
    flag_leftjustify = flag_plussign = flag_blanksign = 
     flag_alternateform = flag_zeropad = 0;
    do{
      switch( c ){
        case '-':   flag_leftjustify = 1;     c = 0;   break;
        case '+':   flag_plussign = 1;        c = 0;   break;
        case ' ':   flag_blanksign = 1;       c = 0;   break;
        case '#':   flag_alternateform = 1;   c = 0;   break;
        case '0':   flag_zeropad = 1;         c = 0;   break;
        default:                                       break;
      }
    }while( c==0 && (c=(*++zFormat))!=0 );
    /* Get the field width */
    width = 0;
    if( c=='*' ){
      width = va_arg(ap, int);
      if( width<0 ){
        flag_leftjustify = 1;
        width = -width;
      }
      c = *++zFormat;
    }else{
      while( c>='0' && c<='9' ){
        width = width*10 + c - '0';
        c = *++zFormat;
      }
    }
    if( width > SXFMT_BUFSIZ-10 ){
      width = SXFMT_BUFSIZ-10;
    }
    /* Get the precision */
	precision = -1;
    if( c=='.' ){
      precision = 0;
      c = *++zFormat;
      if( c=='*' ){
        precision = va_arg(ap, int);
        if( precision<0 ) precision = -precision;
        c = *++zFormat;
      }else{
        while( c>='0' && c<='9' ){
          precision = precision*10 + c - '0';
          c = *++zFormat;
        }
      }
    }
    /* Get the conversion type modifier */
	flag_long = 0;
    if( c=='l' || c == 'q' /* BSD quad (expect a 64-bit integer) */ ){
      flag_long = (c == 'q') ? 2 : 1;
      c = *++zFormat;
	  if( c == 'l' ){
		  /* Standard printf emulation 'lld' (expect a 64bit integer) */
		  flag_long = 2;
	  }
    }
    /* Fetch the info entry for the field */
    infop = 0;
    xtype = SXFMT_ERROR;
	for(idx=0; idx< (int)SX_ARRAYSIZE(aFmt); idx++){
      if( c==aFmt[idx].fmttype ){
        infop = &aFmt[idx];
		xtype = infop->type;
        break;
      }
    }
    zExtra = 0;

    /*
    ** At this point, variables are initialized as follows:
    **
    **   flag_alternateform          TRUE if a '#' is present.
    **   flag_plussign               TRUE if a '+' is present.
    **   flag_leftjustify            TRUE if a '-' is present or if the
    **                               field width was negative.
    **   flag_zeropad                TRUE if the width began with 0.
    **   flag_long                   TRUE if the letter 'l' (ell) or 'q'(BSD quad) prefixed
    **                               the conversion character.
    **   flag_blanksign              TRUE if a ' ' is present.
    **   width                       The specified field width.This is
    **                               always non-negative.Zero is the default.
    **   precision                   The specified precision.The default
    **                               is -1.
    **   xtype                       The object of the conversion.
    **   infop                       Pointer to the appropriate info struct.
    */
    switch( xtype ){
      case SXFMT_RADIX:
        if( flag_long > 0 ){
			if( flag_long > 1 ){
				/* BSD quad: expect a 64-bit integer */
				longvalue = va_arg(ap, sxi64);
			}else{
				longvalue = va_arg(ap, sxlong);
			}
		}else{
			if( infop->flags & SXFLAG_SIGNED ){
				longvalue = va_arg(ap, sxi32);
			}else{
				longvalue = va_arg(ap, sxu32);
			}
		}
		/* Limit the precision to prevent overflowing buf[] during conversion */
      if( precision>SXFMT_BUFSIZ-40 ) precision = SXFMT_BUFSIZ-40;
#if 1
        /* For the format %#x, the value zero is printed "0" not "0x0".
        ** I think this is stupid.*/
        if( longvalue==0 ) flag_alternateform = 0;
#else
        /* More sensible: turn off the prefix for octal (to prevent "00"), 
        ** but leave the prefix for hex.*/
        if( longvalue==0 && infop->base==8 ) flag_alternateform = 0;
#endif
        if( infop->flags & SXFLAG_SIGNED ){
          if( longvalue<0 ){ 
            longvalue = -longvalue;
			/* Ticket 1433-003 */
			if( longvalue < 0 ){
				/* Overflow */
				longvalue= 0x7FFFFFFFFFFFFFFF;
			}
            prefix = '-';
          }else if( flag_plussign )  prefix = '+';
          else if( flag_blanksign )  prefix = ' ';
          else                       prefix = 0;
        }else{
			if( longvalue<0 ){
				longvalue = -longvalue;
				/* Ticket 1433-003 */
				if( longvalue < 0 ){
					/* Overflow */
					longvalue= 0x7FFFFFFFFFFFFFFF;
				}
			}
			prefix = 0;
		}
        if( flag_zeropad && precision<width-(prefix!=0) ){
          precision = width-(prefix!=0);
        }
        bufpt = &buf[SXFMT_BUFSIZ-1];
        {
          register char *cset;      /* Use registers for speed */
          register int base;
          cset = infop->charset;
          base = infop->base;
          do{                                           /* Convert to ascii */
            *(--bufpt) = cset[longvalue%base];
            longvalue = longvalue/base;
          }while( longvalue>0 );
        }
        length = &buf[SXFMT_BUFSIZ-1]-bufpt;
        for(idx=precision-length; idx>0; idx--){
          *(--bufpt) = '0';                             /* Zero pad */
        }
        if( prefix ) *(--bufpt) = prefix;               /* Add sign */
        if( flag_alternateform && infop->prefix ){      /* Add "0" or "0x" */
          char *pre, x;
          pre = infop->prefix;
          if( *bufpt!=pre[0] ){
            for(pre=infop->prefix; (x=(*pre))!=0; pre++) *(--bufpt) = x;
          }
        }
        length = &buf[SXFMT_BUFSIZ-1]-bufpt;
        break;
      case SXFMT_FLOAT:
      case SXFMT_EXP:
      case SXFMT_GENERIC:
#ifndef SX_OMIT_FLOATINGPOINT
		realvalue = va_arg(ap, double);
        if( precision<0 ) precision = 6;         /* Set default precision */
        if( precision>SXFMT_BUFSIZ-40) precision = SXFMT_BUFSIZ-40;
        if( realvalue<0.0 ){
          realvalue = -realvalue;
          prefix = '-';
        }else{
          if( flag_plussign )          prefix = '+';
          else if( flag_blanksign )    prefix = ' ';
          else                         prefix = 0;
        }
        if( infop->type==SXFMT_GENERIC && precision>0 ) precision--;
        rounder = 0.0;
#if 0
        /* Rounding works like BSD when the constant 0.4999 is used.Wierd! */
        for(idx=precision, rounder=0.4999; idx>0; idx--, rounder*=0.1);
#else
        /* It makes more sense to use 0.5 */
        for(idx=precision, rounder=0.5; idx>0; idx--, rounder*=0.1);
#endif
        if( infop->type==SXFMT_FLOAT ) realvalue += rounder;
        /* Normalize realvalue to within 10.0 > realvalue >= 1.0 */
        exp = 0;
        if( realvalue>0.0 ){
          while( realvalue>=1e8 && exp<=350 ){ realvalue *= 1e-8; exp+=8; }
          while( realvalue>=10.0 && exp<=350 ){ realvalue *= 0.1; exp++; }
          while( realvalue<1e-8 && exp>=-350 ){ realvalue *= 1e8; exp-=8; }
          while( realvalue<1.0 && exp>=-350 ){ realvalue *= 10.0; exp--; }
          if( exp>350 || exp<-350 ){
            bufpt = "NaN";
            length = 3;
            break;
          }
        }
        bufpt = buf;
        /*
        ** If the field type is etGENERIC, then convert to either etEXP
        ** or etFLOAT, as appropriate.
        */
        flag_exp = xtype==SXFMT_EXP;
        if( xtype!=SXFMT_FLOAT ){
          realvalue += rounder;
          if( realvalue>=10.0 ){ realvalue *= 0.1; exp++; }
        }
        if( xtype==SXFMT_GENERIC ){
          flag_rtz = !flag_alternateform;
          if( exp<-4 || exp>precision ){
            xtype = SXFMT_EXP;
          }else{
            precision = precision - exp;
            xtype = SXFMT_FLOAT;
          }
        }else{
          flag_rtz = 0;
        }
        /*
        ** The "exp+precision" test causes output to be of type etEXP if
        ** the precision is too large to fit in buf[].
        */
        nsd = 0;
        if( xtype==SXFMT_FLOAT && exp+precision<SXFMT_BUFSIZ-30 ){
          flag_dp = (precision>0 || flag_alternateform);
          if( prefix ) *(bufpt++) = prefix;         /* Sign */
          if( exp<0 )  *(bufpt++) = '0';            /* Digits before "." */
          else for(; exp>=0; exp--) *(bufpt++) = (char)getdigit(&realvalue, &nsd);
          if( flag_dp ) *(bufpt++) = '.';           /* The decimal point */
          for(exp++; exp<0 && precision>0; precision--, exp++){
            *(bufpt++) = '0';
          }
          while( (precision--)>0 ) *(bufpt++) = (char)getdigit(&realvalue, &nsd);
          *(bufpt--) = 0;                           /* Null terminate */
          if( flag_rtz && flag_dp ){     /* Remove trailing zeros and "." */
            while( bufpt>=buf && *bufpt=='0' ) *(bufpt--) = 0;
            if( bufpt>=buf && *bufpt=='.' ) *(bufpt--) = 0;
          }
          bufpt++;                            /* point to next free slot */
        }else{    /* etEXP or etGENERIC */
          flag_dp = (precision>0 || flag_alternateform);
          if( prefix ) *(bufpt++) = prefix;   /* Sign */
          *(bufpt++) = (char)getdigit(&realvalue, &nsd);  /* First digit */
          if( flag_dp ) *(bufpt++) = '.';     /* Decimal point */
          while( (precision--)>0 ) *(bufpt++) = (char)getdigit(&realvalue, &nsd);
          bufpt--;                            /* point to last digit */
          if( flag_rtz && flag_dp ){          /* Remove tail zeros */
            while( bufpt>=buf && *bufpt=='0' ) *(bufpt--) = 0;
            if( bufpt>=buf && *bufpt=='.' ) *(bufpt--) = 0;
          }
          bufpt++;                            /* point to next free slot */
          if( exp || flag_exp ){
            *(bufpt++) = infop->charset[0];
            if( exp<0 ){ *(bufpt++) = '-'; exp = -exp; } /* sign of exp */
            else       { *(bufpt++) = '+'; }
            if( exp>=100 ){
              *(bufpt++) = (char)((exp/100)+'0');                /* 100's digit */
              exp %= 100;
            }
            *(bufpt++) = (char)(exp/10+'0');                     /* 10's digit */
            *(bufpt++) = (char)(exp%10+'0');                     /* 1's digit */
          }
        }
        /* The converted number is in buf[] and zero terminated.Output it.
        ** Note that the number is in the usual order, not reversed as with
        ** integer conversions.*/
        length = bufpt-buf;
        bufpt = buf;

        /* Special case:  Add leading zeros if the flag_zeropad flag is
        ** set and we are not left justified */
        if( flag_zeropad && !flag_leftjustify && length < width){
          int i;
          int nPad = width - length;
          for(i=width; i>=nPad; i--){
            bufpt[i] = bufpt[i-nPad];
          }
          i = prefix!=0;
          while( nPad-- ) bufpt[i++] = '0';
          length = width;
        }
#else
         bufpt = " ";
		 length = (int)sizeof(" ") - 1;
#endif /* SX_OMIT_FLOATINGPOINT */
        break;
      case SXFMT_SIZE:{
		 int *pSize = va_arg(ap, int *);
		 *pSize = ((SyFmtConsumer *)pUserData)->nLen;
		 length = width = 0;
					  }
        break;
      case SXFMT_PERCENT:
        buf[0] = '%';
        bufpt = buf;
        length = 1;
        break;
      case SXFMT_CHARX:
        c = va_arg(ap, int);
		buf[0] = (char)c;
		/* Limit the precision to prevent overflowing buf[] during conversion */
		if( precision>SXFMT_BUFSIZ-40 ) precision = SXFMT_BUFSIZ-40;
        if( precision>=0 ){
          for(idx=1; idx<precision; idx++) buf[idx] = (char)c;
          length = precision;
        }else{
          length =1;
        }
        bufpt = buf;
        break;
      case SXFMT_STRING:
        bufpt = va_arg(ap, char*);
        if( bufpt==0 ){
          bufpt = " ";
		  length = (int)sizeof(" ")-1;
		  break;
        }
		length = precision;
		if( precision < 0 ){
			/* Symisc extension */
			length = (int)SyStrlen(bufpt);
		}
        if( precision>=0 && precision<length ) length = precision;
        break;
	case SXFMT_RAWSTR:{
		/* Symisc extension */
		SyString *pStr = va_arg(ap, SyString *);
		if( pStr == 0 || pStr->zString == 0 ){
			 bufpt = " ";
		     length = (int)sizeof(char);
		     break;
		}
		bufpt = (char *)pStr->zString;
		length = (int)pStr->nByte;
		break;
					  }
      case SXFMT_ERROR:
        buf[0] = '?';
        bufpt = buf;
		length = (int)sizeof(char);
        if( c==0 ) zFormat--;
        break;
    }/* End switch over the format type */
    /*
    ** The text of the conversion is pointed to by "bufpt" and is
    ** "length" characters long.The field width is "width".Do
    ** the output.
    */
    if( !flag_leftjustify ){
      register int nspace;
      nspace = width-length;
      if( nspace>0 ){
        while( nspace>=etSPACESIZE ){
			rc = xConsumer(spaces, etSPACESIZE, pUserData);
			if( rc != SXRET_OK ){
				return SXERR_ABORT; /* Consumer routine request an operation abort */
			}
			nspace -= etSPACESIZE;
        }
        if( nspace>0 ){
			rc = xConsumer(spaces, (unsigned int)nspace, pUserData);
			if( rc != SXRET_OK ){
				return SXERR_ABORT; /* Consumer routine request an operation abort */
			}
		}
      }
    }
    if( length>0 ){
		rc = xConsumer(bufpt, (unsigned int)length, pUserData);
		if( rc != SXRET_OK ){
		  return SXERR_ABORT; /* Consumer routine request an operation abort */
		}
    }
    if( flag_leftjustify ){
      register int nspace;
      nspace = width-length;
      if( nspace>0 ){
        while( nspace>=etSPACESIZE ){
			rc = xConsumer(spaces, etSPACESIZE, pUserData);
			if( rc != SXRET_OK ){
				return SXERR_ABORT; /* Consumer routine request an operation abort */
			}
			nspace -= etSPACESIZE;
        }
        if( nspace>0 ){
			rc = xConsumer(spaces, (unsigned int)nspace, pUserData);
			if( rc != SXRET_OK ){
				return SXERR_ABORT; /* Consumer routine request an operation abort */
			}
		}
      }
    }
  }/* End for loop over the format string */
  return errorflag ? SXERR_FORMAT : SXRET_OK;
} 
static sxi32 FormatConsumer(const void *pSrc, unsigned int nLen, void *pData)
{
	SyFmtConsumer *pConsumer = (SyFmtConsumer *)pData;
	sxi32 rc = SXERR_ABORT;
	switch(pConsumer->nType){
	case SXFMT_CONS_PROC:
			/* User callback */
			rc = pConsumer->uConsumer.sFunc.xUserConsumer(pSrc, nLen, pConsumer->uConsumer.sFunc.pUserData);
			break;
	case SXFMT_CONS_BLOB:
			/* Blob consumer */
			rc = SyBlobAppend(pConsumer->uConsumer.pBlob, pSrc, (sxu32)nLen);
			break;
		default: 
			/* Unknown consumer */
			break;
	}
	/* Update total number of bytes consumed so far */
	pConsumer->nLen += nLen;
	pConsumer->rc = rc;
	return rc;	
}
static sxi32 FormatMount(sxi32 nType, void *pConsumer, ProcConsumer xUserCons, void *pUserData, sxu32 *pOutLen, const char *zFormat, va_list ap)
{
	SyFmtConsumer sCons;
	sCons.nType = nType;
	sCons.rc = SXRET_OK;
	sCons.nLen = 0;
	if( pOutLen ){
		*pOutLen = 0;
	}
	switch(nType){
	case SXFMT_CONS_PROC:
#if defined(UNTRUST)
			if( xUserCons == 0 ){
				return SXERR_EMPTY;
			}
#endif
			sCons.uConsumer.sFunc.xUserConsumer = xUserCons;
			sCons.uConsumer.sFunc.pUserData	    = pUserData;
		break;
		case SXFMT_CONS_BLOB:
			sCons.uConsumer.pBlob = (SyBlob *)pConsumer;
			break;
		default: 
			return SXERR_UNKNOWN;
	}
	InternFormat(FormatConsumer, &sCons, zFormat, ap); 
	if( pOutLen ){
		*pOutLen = sCons.nLen;
	}
	return sCons.rc;
}
VEDIS_PRIVATE sxu32 SyBlobFormat(SyBlob *pBlob, const char *zFormat, ...)
{
	va_list ap;
	sxu32 n;
#if defined(UNTRUST)	
	if( SX_EMPTY_STR(zFormat) ){
		return 0;
	}
#endif			
	va_start(ap, zFormat);
	FormatMount(SXFMT_CONS_BLOB, &(*pBlob), 0, 0, &n, zFormat, ap);
	va_end(ap);
	return n;
}
VEDIS_PRIVATE sxu32 SyBlobFormatAp(SyBlob *pBlob, const char *zFormat, va_list ap)
{
	sxu32 n = 0; /* cc warning */
#if defined(UNTRUST)	
	if( SX_EMPTY_STR(zFormat) ){
		return 0;
	}
#endif	
	FormatMount(SXFMT_CONS_BLOB, &(*pBlob), 0, 0, &n, zFormat, ap);
	return n;
}
#ifdef __UNIXES__
VEDIS_PRIVATE sxu32 SyBufferFormat(char *zBuf, sxu32 nLen, const char *zFormat, ...)
{
	SyBlob sBlob;
	va_list ap;
	sxu32 n;
#if defined(UNTRUST)	
	if( SX_EMPTY_STR(zFormat) ){
		return 0;
	}
#endif	
	if( SXRET_OK != SyBlobInitFromBuf(&sBlob, zBuf, nLen - 1) ){
		return 0;
	}		
	va_start(ap, zFormat);
	FormatMount(SXFMT_CONS_BLOB, &sBlob, 0, 0, 0, zFormat, ap);
	va_end(ap);
	n = SyBlobLength(&sBlob);
	/* Append the null terminator */
	sBlob.mByte++;
	SyBlobAppend(&sBlob, "\0", sizeof(char));
	return n;
}
#endif /* __UNIXES__ */ 
/*
 * Psuedo Random Number Generator (PRNG)
 * @authors: SQLite authors <http://www.sqlite.org/>
 * @status: Public Domain
 * NOTE:
 *  Nothing in this file or anywhere else in the library does any kind of
 *  encryption.The RC4 algorithm is being used as a PRNG (pseudo-random
 *  number generator) not as an encryption device.
 */
#define SXPRNG_MAGIC	0x13C4
#ifdef __UNIXES__
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#endif
static sxi32 SyOSUtilRandomSeed(void *pBuf, sxu32 nLen, void *pUnused)
{
	char *zBuf = (char *)pBuf;
#ifdef __WINNT__
	DWORD nProcessID; /* Yes, keep it uninitialized when compiling using the MinGW32 builds tools */
#elif defined(__UNIXES__)
	pid_t pid;
	int fd;
#else
	char zGarbage[128]; /* Yes, keep this buffer uninitialized */
#endif
	SXUNUSED(pUnused);
#ifdef __WINNT__
#ifndef __MINGW32__
	nProcessID = GetProcessId(GetCurrentProcess());
#endif
	SyMemcpy((const void *)&nProcessID, zBuf, SXMIN(nLen, sizeof(DWORD)));
	if( (sxu32)(&zBuf[nLen] - &zBuf[sizeof(DWORD)]) >= sizeof(SYSTEMTIME)  ){
		GetSystemTime((LPSYSTEMTIME)&zBuf[sizeof(DWORD)]);
	}
#elif defined(__UNIXES__)
	fd = open("/dev/urandom", O_RDONLY);
	if (fd >= 0 ){
		if( read(fd, zBuf, nLen) > 0 ){
			return SXRET_OK;
		}
		/* FALL THRU */
	}
	pid = getpid();
	SyMemcpy((const void *)&pid, zBuf, SXMIN(nLen, sizeof(pid_t)));
	if( &zBuf[nLen] - &zBuf[sizeof(pid_t)] >= (int)sizeof(struct timeval)  ){
		gettimeofday((struct timeval *)&zBuf[sizeof(pid_t)], 0);
	}
#else
	/* Fill with uninitialized data */
	SyMemcpy(zGarbage, zBuf, SXMIN(nLen, sizeof(zGarbage)));
#endif
	return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyRandomnessInit(SyPRNGCtx *pCtx, ProcRandomSeed xSeed, void * pUserData)
{
	char zSeed[256];
	sxu8 t;
	sxi32 rc;
	sxu32 i;
	if( pCtx->nMagic == SXPRNG_MAGIC ){
		return SXRET_OK; /* Already initialized */
	}
 /* Initialize the state of the random number generator once, 
  ** the first time this routine is called.The seed value does
  ** not need to contain a lot of randomness since we are not
  ** trying to do secure encryption or anything like that...
  */	
	if( xSeed == 0 ){
		xSeed = SyOSUtilRandomSeed;
	}
	rc = xSeed(zSeed, sizeof(zSeed), pUserData);
	if( rc != SXRET_OK ){
		return rc;
	}
	pCtx->i = pCtx->j = 0;
	for(i=0; i < SX_ARRAYSIZE(pCtx->s) ; i++){
		pCtx->s[i] = (unsigned char)i;
    }
    for(i=0; i < sizeof(zSeed) ; i++){
      pCtx->j += pCtx->s[i] + zSeed[i];
      t = pCtx->s[pCtx->j];
      pCtx->s[pCtx->j] = pCtx->s[i];
      pCtx->s[i] = t;
    }
	pCtx->nMagic = SXPRNG_MAGIC;
	
	return SXRET_OK;
}
/*
 * Get a single 8-bit random value using the RC4 PRNG.
 */
static sxu8 randomByte(SyPRNGCtx *pCtx)
{
  sxu8 t;
  
  /* Generate and return single random byte */
  pCtx->i++;
  t = pCtx->s[pCtx->i];
  pCtx->j += t;
  pCtx->s[pCtx->i] = pCtx->s[pCtx->j];
  pCtx->s[pCtx->j] = t;
  t += pCtx->s[pCtx->i];
  return pCtx->s[t];
}
VEDIS_PRIVATE sxi32 SyRandomness(SyPRNGCtx *pCtx, void *pBuf, sxu32 nLen)
{
	unsigned char *zBuf = (unsigned char *)pBuf;
	unsigned char *zEnd = &zBuf[nLen];
#if defined(UNTRUST)
	if( pCtx == 0 || pBuf == 0 || nLen <= 0 ){
		return SXERR_EMPTY;
	}
#endif
	if(pCtx->nMagic != SXPRNG_MAGIC ){
		return SXERR_CORRUPT;
	}
	for(;;){
		if( zBuf >= zEnd ){break;}	zBuf[0] = randomByte(pCtx);	zBuf++;	
		if( zBuf >= zEnd ){break;}	zBuf[0] = randomByte(pCtx);	zBuf++;	
		if( zBuf >= zEnd ){break;}	zBuf[0] = randomByte(pCtx);	zBuf++;	
		if( zBuf >= zEnd ){break;}	zBuf[0] = randomByte(pCtx);	zBuf++;	
	}
	return SXRET_OK;  
}
#ifdef VEDIS_ENABLE_HASH_CMD
/* SyRunTimeApi: sxhash.c */
/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent, 
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */
#define SX_MD5_BINSZ	16
#define SX_MD5_HEXSZ	32
/*
 * Note: this code is harmless on little-endian machines.
 */
static void byteReverse (unsigned char *buf, unsigned longs)
{
	sxu32 t;
        do {
                t = (sxu32)((unsigned)buf[3]<<8 | buf[2]) << 16 |
                            ((unsigned)buf[1]<<8 | buf[0]);
                *(sxu32*)buf = t;
                buf += 4;
        } while (--longs);
}
/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#ifdef F1
#undef F1
#endif
#ifdef F2
#undef F2
#endif
#ifdef F3
#undef F3
#endif
#ifdef F4
#undef F4
#endif

#define F1(x, y, z) (z ^ (x & (y ^ z)))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) (x ^ y ^ z)
#define F4(x, y, z) (y ^ (x | ~z))

/* This is the central step in the MD5 algorithm.*/
#define SX_MD5STEP(f, w, x, y, z, data, s) \
        ( w += f(x, y, z) + data,  w = w<<s | w>>(32-s),  w += x )

/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */
static void MD5Transform(sxu32 buf[4], const sxu32 in[16])
{
	register sxu32 a, b, c, d;

        a = buf[0];
        b = buf[1];
        c = buf[2];
        d = buf[3];

        SX_MD5STEP(F1, a, b, c, d, in[ 0]+0xd76aa478,  7);
        SX_MD5STEP(F1, d, a, b, c, in[ 1]+0xe8c7b756, 12);
        SX_MD5STEP(F1, c, d, a, b, in[ 2]+0x242070db, 17);
        SX_MD5STEP(F1, b, c, d, a, in[ 3]+0xc1bdceee, 22);
        SX_MD5STEP(F1, a, b, c, d, in[ 4]+0xf57c0faf,  7);
        SX_MD5STEP(F1, d, a, b, c, in[ 5]+0x4787c62a, 12);
        SX_MD5STEP(F1, c, d, a, b, in[ 6]+0xa8304613, 17);
        SX_MD5STEP(F1, b, c, d, a, in[ 7]+0xfd469501, 22);
        SX_MD5STEP(F1, a, b, c, d, in[ 8]+0x698098d8,  7);
        SX_MD5STEP(F1, d, a, b, c, in[ 9]+0x8b44f7af, 12);
        SX_MD5STEP(F1, c, d, a, b, in[10]+0xffff5bb1, 17);
        SX_MD5STEP(F1, b, c, d, a, in[11]+0x895cd7be, 22);
        SX_MD5STEP(F1, a, b, c, d, in[12]+0x6b901122,  7);
        SX_MD5STEP(F1, d, a, b, c, in[13]+0xfd987193, 12);
        SX_MD5STEP(F1, c, d, a, b, in[14]+0xa679438e, 17);
        SX_MD5STEP(F1, b, c, d, a, in[15]+0x49b40821, 22);

        SX_MD5STEP(F2, a, b, c, d, in[ 1]+0xf61e2562,  5);
        SX_MD5STEP(F2, d, a, b, c, in[ 6]+0xc040b340,  9);
        SX_MD5STEP(F2, c, d, a, b, in[11]+0x265e5a51, 14);
        SX_MD5STEP(F2, b, c, d, a, in[ 0]+0xe9b6c7aa, 20);
        SX_MD5STEP(F2, a, b, c, d, in[ 5]+0xd62f105d,  5);
        SX_MD5STEP(F2, d, a, b, c, in[10]+0x02441453,  9);
        SX_MD5STEP(F2, c, d, a, b, in[15]+0xd8a1e681, 14);
        SX_MD5STEP(F2, b, c, d, a, in[ 4]+0xe7d3fbc8, 20);
        SX_MD5STEP(F2, a, b, c, d, in[ 9]+0x21e1cde6,  5);
        SX_MD5STEP(F2, d, a, b, c, in[14]+0xc33707d6,  9);
        SX_MD5STEP(F2, c, d, a, b, in[ 3]+0xf4d50d87, 14);
        SX_MD5STEP(F2, b, c, d, a, in[ 8]+0x455a14ed, 20);
        SX_MD5STEP(F2, a, b, c, d, in[13]+0xa9e3e905,  5);
        SX_MD5STEP(F2, d, a, b, c, in[ 2]+0xfcefa3f8,  9);
        SX_MD5STEP(F2, c, d, a, b, in[ 7]+0x676f02d9, 14);
        SX_MD5STEP(F2, b, c, d, a, in[12]+0x8d2a4c8a, 20);

        SX_MD5STEP(F3, a, b, c, d, in[ 5]+0xfffa3942,  4);
        SX_MD5STEP(F3, d, a, b, c, in[ 8]+0x8771f681, 11);
        SX_MD5STEP(F3, c, d, a, b, in[11]+0x6d9d6122, 16);
        SX_MD5STEP(F3, b, c, d, a, in[14]+0xfde5380c, 23);
        SX_MD5STEP(F3, a, b, c, d, in[ 1]+0xa4beea44,  4);
        SX_MD5STEP(F3, d, a, b, c, in[ 4]+0x4bdecfa9, 11);
        SX_MD5STEP(F3, c, d, a, b, in[ 7]+0xf6bb4b60, 16);
        SX_MD5STEP(F3, b, c, d, a, in[10]+0xbebfbc70, 23);
        SX_MD5STEP(F3, a, b, c, d, in[13]+0x289b7ec6,  4);
        SX_MD5STEP(F3, d, a, b, c, in[ 0]+0xeaa127fa, 11);
        SX_MD5STEP(F3, c, d, a, b, in[ 3]+0xd4ef3085, 16);
        SX_MD5STEP(F3, b, c, d, a, in[ 6]+0x04881d05, 23);
        SX_MD5STEP(F3, a, b, c, d, in[ 9]+0xd9d4d039,  4);
        SX_MD5STEP(F3, d, a, b, c, in[12]+0xe6db99e5, 11);
        SX_MD5STEP(F3, c, d, a, b, in[15]+0x1fa27cf8, 16);
        SX_MD5STEP(F3, b, c, d, a, in[ 2]+0xc4ac5665, 23);

        SX_MD5STEP(F4, a, b, c, d, in[ 0]+0xf4292244,  6);
        SX_MD5STEP(F4, d, a, b, c, in[ 7]+0x432aff97, 10);
        SX_MD5STEP(F4, c, d, a, b, in[14]+0xab9423a7, 15);
        SX_MD5STEP(F4, b, c, d, a, in[ 5]+0xfc93a039, 21);
        SX_MD5STEP(F4, a, b, c, d, in[12]+0x655b59c3,  6);
        SX_MD5STEP(F4, d, a, b, c, in[ 3]+0x8f0ccc92, 10);
        SX_MD5STEP(F4, c, d, a, b, in[10]+0xffeff47d, 15);
        SX_MD5STEP(F4, b, c, d, a, in[ 1]+0x85845dd1, 21);
        SX_MD5STEP(F4, a, b, c, d, in[ 8]+0x6fa87e4f,  6);
        SX_MD5STEP(F4, d, a, b, c, in[15]+0xfe2ce6e0, 10);
        SX_MD5STEP(F4, c, d, a, b, in[ 6]+0xa3014314, 15);
        SX_MD5STEP(F4, b, c, d, a, in[13]+0x4e0811a1, 21);
        SX_MD5STEP(F4, a, b, c, d, in[ 4]+0xf7537e82,  6);
        SX_MD5STEP(F4, d, a, b, c, in[11]+0xbd3af235, 10);
        SX_MD5STEP(F4, c, d, a, b, in[ 2]+0x2ad7d2bb, 15);
        SX_MD5STEP(F4, b, c, d, a, in[ 9]+0xeb86d391, 21);

        buf[0] += a;
        buf[1] += b;
        buf[2] += c;
        buf[3] += d;
}
/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */
VEDIS_PRIVATE void MD5Update(MD5Context *ctx, const unsigned char *buf, unsigned int len)
{
	sxu32 t;

        /* Update bitcount */
        t = ctx->bits[0];
        if ((ctx->bits[0] = t + ((sxu32)len << 3)) < t)
                ctx->bits[1]++; /* Carry from low to high */
        ctx->bits[1] += len >> 29;
        t = (t >> 3) & 0x3f;    /* Bytes already in shsInfo->data */
        /* Handle any leading odd-sized chunks */
        if ( t ) {
                unsigned char *p = (unsigned char *)ctx->in + t;

                t = 64-t;
                if (len < t) {
                        SyMemcpy(buf, p, len);
                        return;
                }
                SyMemcpy(buf, p, t);
                byteReverse(ctx->in, 16);
                MD5Transform(ctx->buf, (sxu32*)ctx->in);
                buf += t;
                len -= t;
        }
        /* Process data in 64-byte chunks */
        while (len >= 64) {
                SyMemcpy(buf, ctx->in, 64);
                byteReverse(ctx->in, 16);
                MD5Transform(ctx->buf, (sxu32*)ctx->in);
                buf += 64;
                len -= 64;
        }
        /* Handle any remaining bytes of data.*/
        SyMemcpy(buf, ctx->in, len);
}
/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern 
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
VEDIS_PRIVATE void MD5Final(unsigned char digest[16], MD5Context *ctx){
        unsigned count;
        unsigned char *p;

        /* Compute number of bytes mod 64 */
        count = (ctx->bits[0] >> 3) & 0x3F;

        /* Set the first char of padding to 0x80.This is safe since there is
           always at least one byte free */
        p = ctx->in + count;
        *p++ = 0x80;

        /* Bytes of padding needed to make 64 bytes */
        count = 64 - 1 - count;

        /* Pad out to 56 mod 64 */
        if (count < 8) {
                /* Two lots of padding:  Pad the first block to 64 bytes */
               SyZero(p, count);
                byteReverse(ctx->in, 16);
                MD5Transform(ctx->buf, (sxu32*)ctx->in);

                /* Now fill the next block with 56 bytes */
                SyZero(ctx->in, 56);
        } else {
                /* Pad block to 56 bytes */
                SyZero(p, count-8);
        }
        byteReverse(ctx->in, 14);

        /* Append length in bits and transform */
        ((sxu32*)ctx->in)[ 14 ] = ctx->bits[0];
        ((sxu32*)ctx->in)[ 15 ] = ctx->bits[1];

        MD5Transform(ctx->buf, (sxu32*)ctx->in);
        byteReverse((unsigned char *)ctx->buf, 4);
        SyMemcpy(ctx->buf, digest, 0x10);
        SyZero(ctx, sizeof(ctx));    /* In case it's sensitive */
}
#undef F1
#undef F2
#undef F3
#undef F4
VEDIS_PRIVATE sxi32 MD5Init(MD5Context *pCtx)
{	
	pCtx->buf[0] = 0x67452301;
    pCtx->buf[1] = 0xefcdab89;
    pCtx->buf[2] = 0x98badcfe;
    pCtx->buf[3] = 0x10325476;
    pCtx->bits[0] = 0;
    pCtx->bits[1] = 0;
   
   return SXRET_OK;
}
VEDIS_PRIVATE sxi32 SyMD5Compute(const void *pIn, sxu32 nLen, unsigned char zDigest[16])
{
	MD5Context sCtx;
	MD5Init(&sCtx);
	MD5Update(&sCtx, (const unsigned char *)pIn, nLen);
	MD5Final(zDigest, &sCtx);	
	return SXRET_OK;
}
/*
 * SHA-1 in C
 * By Steve Reid <steve@edmweb.com>
 * Status: Public Domain
 */
/*
 * blk0() and blk() perform the initial expand.
 * I got the idea of expanding during the round function from SSLeay
 *
 * blk0le() for little-endian and blk0be() for big-endian.
 */
#if __GNUC__ && (defined(__i386__) || defined(__x86_64__))
/*
 * GCC by itself only generates left rotates.  Use right rotates if
 * possible to be kinder to dinky implementations with iterative rotate
 * instructions.
 */
#define SHA_ROT(op, x, k) \
        ({ unsigned int y; asm(op " %1, %0" : "=r" (y) : "I" (k), "0" (x)); y; })
#define rol(x, k) SHA_ROT("roll", x, k)
#define ror(x, k) SHA_ROT("rorl", x, k)

#else
/* Generic C equivalent */
#define SHA_ROT(x, l, r) ((x) << (l) | (x) >> (r))
#define rol(x, k) SHA_ROT(x, k, 32-(k))
#define ror(x, k) SHA_ROT(x, 32-(k), k)
#endif

#define blk0le(i) (block[i] = (ror(block[i], 8)&0xFF00FF00) \
    |(rol(block[i], 8)&0x00FF00FF))
#define blk0be(i) block[i]
#define blk(i) (block[i&15] = rol(block[(i+13)&15]^block[(i+8)&15] \
    ^block[(i+2)&15]^block[i&15], 1))

/*
 * (R0+R1), R2, R3, R4 are the different operations (rounds) used in SHA1
 *
 * Rl0() for little-endian and Rb0() for big-endian.  Endianness is 
 * determined at run-time.
 */
#define Rl0(v, w, x, y, z, i) \
    z+=((w&(x^y))^y)+blk0le(i)+0x5A827999+rol(v, 5);w=ror(w, 2);
#define Rb0(v, w, x, y, z, i) \
    z+=((w&(x^y))^y)+blk0be(i)+0x5A827999+rol(v, 5);w=ror(w, 2);
#define R1(v, w, x, y, z, i) \
    z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v, 5);w=ror(w, 2);
#define R2(v, w, x, y, z, i) \
    z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v, 5);w=ror(w, 2);
#define R3(v, w, x, y, z, i) \
    z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v, 5);w=ror(w, 2);
#define R4(v, w, x, y, z, i) \
    z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v, 5);w=ror(w, 2);

/*
 * Hash a single 512-bit block. This is the core of the algorithm.
 */
#define a qq[0]
#define b qq[1]
#define c qq[2]
#define d qq[3]
#define e qq[4]

static void SHA1Transform(unsigned int state[5], const unsigned char buffer[64])
{
  unsigned int qq[5]; /* a, b, c, d, e; */
  static int one = 1;
  unsigned int block[16];
  SyMemcpy(buffer, (void *)block, 64);
  SyMemcpy(state, qq, 5*sizeof(unsigned int));

  /* Copy context->state[] to working vars */
  /*
  a = state[0];
  b = state[1];
  c = state[2];
  d = state[3];
  e = state[4];
  */

  /* 4 rounds of 20 operations each. Loop unrolled. */
  if( 1 == *(unsigned char*)&one ){
    Rl0(a, b, c, d, e, 0); Rl0(e, a, b, c, d, 1); Rl0(d, e, a, b, c, 2); Rl0(c, d, e, a, b, 3);
    Rl0(b, c, d, e, a, 4); Rl0(a, b, c, d, e, 5); Rl0(e, a, b, c, d, 6); Rl0(d, e, a, b, c, 7);
    Rl0(c, d, e, a, b, 8); Rl0(b, c, d, e, a, 9); Rl0(a, b, c, d, e, 10); Rl0(e, a, b, c, d, 11);
    Rl0(d, e, a, b, c, 12); Rl0(c, d, e, a, b, 13); Rl0(b, c, d, e, a, 14); Rl0(a, b, c, d, e, 15);
  }else{
    Rb0(a, b, c, d, e, 0); Rb0(e, a, b, c, d, 1); Rb0(d, e, a, b, c, 2); Rb0(c, d, e, a, b, 3);
    Rb0(b, c, d, e, a, 4); Rb0(a, b, c, d, e, 5); Rb0(e, a, b, c, d, 6); Rb0(d, e, a, b, c, 7);
    Rb0(c, d, e, a, b, 8); Rb0(b, c, d, e, a, 9); Rb0(a, b, c, d, e, 10); Rb0(e, a, b, c, d, 11);
    Rb0(d, e, a, b, c, 12); Rb0(c, d, e, a, b, 13); Rb0(b, c, d, e, a, 14); Rb0(a, b, c, d, e, 15);
  }
  R1(e, a, b, c, d, 16); R1(d, e, a, b, c, 17); R1(c, d, e, a, b, 18); R1(b, c, d, e, a, 19);
  R2(a, b, c, d, e, 20); R2(e, a, b, c, d, 21); R2(d, e, a, b, c, 22); R2(c, d, e, a, b, 23);
  R2(b, c, d, e, a, 24); R2(a, b, c, d, e, 25); R2(e, a, b, c, d, 26); R2(d, e, a, b, c, 27);
  R2(c, d, e, a, b, 28); R2(b, c, d, e, a, 29); R2(a, b, c, d, e, 30); R2(e, a, b, c, d, 31);
  R2(d, e, a, b, c, 32); R2(c, d, e, a, b, 33); R2(b, c, d, e, a, 34); R2(a, b, c, d, e, 35);
  R2(e, a, b, c, d, 36); R2(d, e, a, b, c, 37); R2(c, d, e, a, b, 38); R2(b, c, d, e, a, 39);
  R3(a, b, c, d, e, 40); R3(e, a, b, c, d, 41); R3(d, e, a, b, c, 42); R3(c, d, e, a, b, 43);
  R3(b, c, d, e, a, 44); R3(a, b, c, d, e, 45); R3(e, a, b, c, d, 46); R3(d, e, a, b, c, 47);
  R3(c, d, e, a, b, 48); R3(b, c, d, e, a, 49); R3(a, b, c, d, e, 50); R3(e, a, b, c, d, 51);
  R3(d, e, a, b, c, 52); R3(c, d, e, a, b, 53); R3(b, c, d, e, a, 54); R3(a, b, c, d, e, 55);
  R3(e, a, b, c, d, 56); R3(d, e, a, b, c, 57); R3(c, d, e, a, b, 58); R3(b, c, d, e, a, 59);
  R4(a, b, c, d, e, 60); R4(e, a, b, c, d, 61); R4(d, e, a, b, c, 62); R4(c, d, e, a, b, 63);
  R4(b, c, d, e, a, 64); R4(a, b, c, d, e, 65); R4(e, a, b, c, d, 66); R4(d, e, a, b, c, 67);
  R4(c, d, e, a, b, 68); R4(b, c, d, e, a, 69); R4(a, b, c, d, e, 70); R4(e, a, b, c, d, 71);
  R4(d, e, a, b, c, 72); R4(c, d, e, a, b, 73); R4(b, c, d, e, a, 74); R4(a, b, c, d, e, 75);
  R4(e, a, b, c, d, 76); R4(d, e, a, b, c, 77); R4(c, d, e, a, b, 78); R4(b, c, d, e, a, 79);

  /* Add the working vars back into context.state[] */
  state[0] += a;
  state[1] += b;
  state[2] += c;
  state[3] += d;
  state[4] += e;
}
#undef a
#undef b
#undef c
#undef d
#undef e
/*
 * SHA1Init - Initialize new context
 */
VEDIS_PRIVATE void SHA1Init(SHA1Context *context){
    /* SHA1 initialization constants */
    context->state[0] = 0x67452301;
    context->state[1] = 0xEFCDAB89;
    context->state[2] = 0x98BADCFE;
    context->state[3] = 0x10325476;
    context->state[4] = 0xC3D2E1F0;
    context->count[0] = context->count[1] = 0;
}
/*
 * Run your data through this.
 */
VEDIS_PRIVATE void SHA1Update(SHA1Context *context, const unsigned char *data, unsigned int len){
    unsigned int i, j;

    j = context->count[0];
    if ((context->count[0] += len << 3) < j)
	context->count[1] += (len>>29)+1;
    j = (j >> 3) & 63;
    if ((j + len) > 63) {
		(void)SyMemcpy(data, &context->buffer[j],  (i = 64-j));
	SHA1Transform(context->state, context->buffer);
	for ( ; i + 63 < len; i += 64)
	    SHA1Transform(context->state, &data[i]);
	j = 0;
    } else {
	i = 0;
    }
	(void)SyMemcpy(&data[i], &context->buffer[j], len - i);
}
/*
 * Add padding and return the message digest.
 */
VEDIS_PRIVATE void SHA1Final(SHA1Context *context, unsigned char digest[20]){
    unsigned int i;
    unsigned char finalcount[8];

    for (i = 0; i < 8; i++) {
	finalcount[i] = (unsigned char)((context->count[(i >= 4 ? 0 : 1)]
	 >> ((3-(i & 3)) * 8) ) & 255);	 /* Endian independent */
    }
    SHA1Update(context, (const unsigned char *)"\200", 1);
    while ((context->count[0] & 504) != 448)
	SHA1Update(context, (const unsigned char *)"\0", 1);
    SHA1Update(context, finalcount, 8);  /* Should cause a SHA1Transform() */

    if (digest) {
	for (i = 0; i < 20; i++)
	    digest[i] = (unsigned char)
		((context->state[i>>2] >> ((3-(i & 3)) * 8) ) & 255);
    }
}
#undef Rl0
#undef Rb0
#undef R1
#undef R2
#undef R3
#undef R4

VEDIS_PRIVATE sxi32 SySha1Compute(const void *pIn, sxu32 nLen, unsigned char zDigest[20])
{
	SHA1Context sCtx;
	SHA1Init(&sCtx);
	SHA1Update(&sCtx, (const unsigned char *)pIn, nLen);
	SHA1Final(&sCtx, zDigest);
	return SXRET_OK;
}
static const sxu32 crc32_table[] = {
	0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 
	0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3, 
	0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988, 
	0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 
	0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de, 
	0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7, 
	0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 
	0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5, 
	0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172, 
	0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 
	0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940, 
	0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59, 
	0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 
	0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f, 
	0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924, 
	0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 
	0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a, 
	0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433, 
	0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 
	0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01, 
	0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e, 
	0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 
	0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c, 
	0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65, 
	0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 
	0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb, 
	0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0, 
	0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 
	0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086, 
	0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f, 
	0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 
	0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad, 
	0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a, 
	0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 
	0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8, 
	0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1, 
	0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 
	0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7, 
	0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc, 
	0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 
	0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252, 
	0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b, 
	0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 
	0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79, 
	0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236, 
	0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 
	0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04, 
	0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d, 
	0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 
	0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713, 
	0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 
	0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 
	0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e, 
	0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777, 
	0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 
	0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45, 
	0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2, 
	0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 
	0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0, 
	0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9, 
	0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 
	0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf, 
	0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94, 
	0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d, 
};
#define CRC32C(c, d) (c = ( crc32_table[(c ^ (d)) & 0xFF] ^ (c>>8) ) )
static sxu32 SyCrc32Update(sxu32 crc32, const void *pSrc, sxu32 nLen)
{
	register unsigned char *zIn = (unsigned char *)pSrc;
	unsigned char *zEnd;
	if( zIn == 0 ){
		return crc32;
	}
	zEnd = &zIn[nLen];
	for(;;){
		if(zIn >= zEnd ){ break; } CRC32C(crc32, zIn[0]); zIn++;
		if(zIn >= zEnd ){ break; } CRC32C(crc32, zIn[0]); zIn++;
		if(zIn >= zEnd ){ break; } CRC32C(crc32, zIn[0]); zIn++;
		if(zIn >= zEnd ){ break; } CRC32C(crc32, zIn[0]); zIn++;
	}
		
	return crc32;
}
VEDIS_PRIVATE sxu32 SyCrc32(const void *pSrc, sxu32 nLen)
{
	return SyCrc32Update(SXU32_HIGH, pSrc, nLen);
}
VEDIS_PRIVATE sxi32 SyBinToHexConsumer(const void *pIn, sxu32 nLen, ProcConsumer xConsumer, void *pConsumerData)
{
	static const unsigned char zHexTab[] = "0123456789abcdef";
	const unsigned char *zIn, *zEnd;
	unsigned char zOut[3];
	sxi32 rc;
#if defined(UNTRUST)
	if( pIn == 0 || xConsumer == 0 ){
		return SXERR_EMPTY;
	}
#endif
	zIn   = (const unsigned char *)pIn;
	zEnd  = &zIn[nLen];
	for(;;){
		if( zIn >= zEnd  ){
			break;
		}
		zOut[0] = zHexTab[zIn[0] >> 4];  zOut[1] = zHexTab[zIn[0] & 0x0F];
		rc = xConsumer((const void *)zOut, sizeof(char)*2, pConsumerData);
		if( rc != SXRET_OK ){
			return rc;
		}
		zIn++; 
	}
	return SXRET_OK;
}
#endif /* VEDIS_ENABLE_HASH_CMD */
VEDIS_PRIVATE void SyBigEndianPack32(unsigned char *buf,sxu32 nb)
{
	buf[3] = nb & 0xFF ; nb >>=8;
	buf[2] = nb & 0xFF ; nb >>=8;
	buf[1] = nb & 0xFF ; nb >>=8;
	buf[0] = (unsigned char)nb ;
}
VEDIS_PRIVATE void SyBigEndianUnpack32(const unsigned char *buf,sxu32 *uNB)
{
	*uNB = buf[3] + (buf[2] << 8) + (buf[1] << 16) + (buf[0] << 24);
}
VEDIS_PRIVATE void SyBigEndianPack16(unsigned char *buf,sxu16 nb)
{
	buf[1] = nb & 0xFF ; nb >>=8;
	buf[0] = (unsigned char)nb ;
}
VEDIS_PRIVATE void SyBigEndianUnpack16(const unsigned char *buf,sxu16 *uNB)
{
	*uNB = buf[1] + (buf[0] << 8);
}
VEDIS_PRIVATE void SyBigEndianPack64(unsigned char *buf,sxu64 n64)
{
	buf[7] = n64 & 0xFF; n64 >>=8;
	buf[6] = n64 & 0xFF; n64 >>=8;
	buf[5] = n64 & 0xFF; n64 >>=8;
	buf[4] = n64 & 0xFF; n64 >>=8;
	buf[3] = n64 & 0xFF; n64 >>=8;
	buf[2] = n64 & 0xFF; n64 >>=8;
	buf[1] = n64 & 0xFF; n64 >>=8;
	buf[0] = (sxu8)n64 ; 
}
VEDIS_PRIVATE void SyBigEndianUnpack64(const unsigned char *buf,sxu64 *n64)
{
	sxu32 u1,u2;
	u1 = buf[7] + (buf[6] << 8) + (buf[5] << 16) + (buf[4] << 24);
	u2 = buf[3] + (buf[2] << 8) + (buf[1] << 16) + (buf[0] << 24);
	*n64 = (((sxu64)u2) << 32) | u1;
}
#if 0
VEDIS_PRIVATE sxi32 SyBlobAppendBig64(SyBlob *pBlob,sxu64 n64)
{
	unsigned char zBuf[8];
	sxi32 rc;
	SyBigEndianPack64(zBuf,n64);
	rc = SyBlobAppend(pBlob,(const void *)zBuf,sizeof(zBuf));
	return rc;
}
#endif
VEDIS_PRIVATE sxi32 SyBlobAppendBig32(SyBlob *pBlob,sxu32 n32)
{
	unsigned char zBuf[4];
	sxi32 rc;
	SyBigEndianPack32(zBuf,n32);
	rc = SyBlobAppend(pBlob,(const void *)zBuf,sizeof(zBuf));
	return rc;
}
VEDIS_PRIVATE sxi32 SyBlobAppendBig16(SyBlob *pBlob,sxu16 n16)
{
	unsigned char zBuf[2];
	sxi32 rc;
	SyBigEndianPack16(zBuf,n16);
	rc = SyBlobAppend(pBlob,(const void *)zBuf,sizeof(zBuf));
	return rc;
}
VEDIS_PRIVATE void SyTimeFormatToDos(Sytm *pFmt,sxu32 *pOut)
{
	sxi32 nDate,nTime;
	nDate = ((pFmt->tm_year - 1980) << 9) + (pFmt->tm_mon << 5) + pFmt->tm_mday;
	nTime = (pFmt->tm_hour << 11) + (pFmt->tm_min << 5)+ (pFmt->tm_sec >> 1);
	*pOut = (nDate << 16) | nTime;
}
VEDIS_PRIVATE void SyDosTimeFormat(sxu32 nDosDate, Sytm *pOut)
{
	sxu16 nDate;
	sxu16 nTime;
	nDate = nDosDate >> 16;
	nTime = nDosDate & 0xFFFF;
	pOut->tm_isdst  = 0;
	pOut->tm_year 	= 1980 + (nDate >> 9);
	pOut->tm_mon	= (nDate % (1<<9))>>5;
	pOut->tm_mday	= (nDate % (1<<9))&0x1F;
	pOut->tm_hour	= nTime >> 11;
	pOut->tm_min	= (nTime % (1<<11)) >> 5;
	pOut->tm_sec	= ((nTime % (1<<11))& 0x1F )<<1;
}
/*
 * ----------------------------------------------------------
 * File: lhash_kv.c
 * MD5: 8d719faf8d557b1132dd0f52e2f5560b
 * ----------------------------------------------------------
 */
/*
 * Symisc Vedis: An Embeddable NoSQL (Post Modern) Database Engine.
 * Copyright (C) 2012-2013, Symisc Systems http://vedis.org/
 * Version 1.1.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.org/licensing.html
 */
 /* $SymiscID: lhash_kv.c v1.7 Solaris 2013-01-14 12:56 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* 
 * This file implements disk based hashtable using the linear hashing algorithm.
 * This implementation is the one decribed in the paper:
 *  LINEAR HASHING : A NEW TOOL FOR FILE AND TABLE ADDRESSING. Witold Litwin. I. N. Ft. I. A.. 78 150 Le Chesnay, France.
 * Plus a smart extension called Virtual Bucket Table. (contact devel@symisc.net for additional information).
 */
/* Magic number identifying a valid storage image */
#define L_HASH_MAGIC 0xDE671CEF
/*
 * Magic word to hash to identify a valid hash function.
 */
#define L_HASH_WORD "chm@symisc"
/*
 * Cell size on disk. 
 */
#define L_HASH_CELL_SZ (4/*Hash*/+4/*Key*/+8/*Data*/+2/* Offset of the next cell */+8/*Overflow*/)
/*
 * Primary page (not overflow pages) header size on disk.
 */
#define L_HASH_PAGE_HDR_SZ (2/* Cell offset*/+2/* Free block offset*/+8/*Slave page number*/)
/*
 * The maximum amount of payload (in bytes) that can be stored locally for
 * a database entry.  If the entry contains more data than this, the
 * extra goes onto overflow pages.
*/
#define L_HASH_MX_PAYLOAD(PageSize)  (PageSize-(L_HASH_PAGE_HDR_SZ+L_HASH_CELL_SZ))
/*
 * Maxium free space on a single page.
 */
#define L_HASH_MX_FREE_SPACE(PageSize) (PageSize - (L_HASH_PAGE_HDR_SZ))
/*
** The maximum number of bytes of payload allowed on a single overflow page.
*/
#define L_HASH_OVERFLOW_SIZE(PageSize) (PageSize-8)
/* Forward declaration */
typedef struct lhash_kv_engine lhash_kv_engine;
typedef struct lhpage lhpage;
/*
 * Each record in the database is identified either in-memory or in
 * disk by an instance of the following structure.
 */
typedef struct lhcell lhcell;
struct lhcell
{
	/* Disk-data (Big-Endian) */
	sxu32 nHash;   /* Hash of the key: 4 bytes */
	sxu32 nKey;    /* Key length: 4 bytes */
	sxu64 nData;   /* Data length: 8 bytes */
	sxu16 iNext;   /* Offset of the next cell: 2 bytes */
	pgno iOvfl;    /* Overflow page number if any: 8 bytes */
	/* In-memory data only */
	lhpage *pPage;     /* Page this cell belongs */
	sxu16 iStart;      /* Offset of this cell */
	pgno iDataPage;    /* Data page number when overflow */
	sxu16 iDataOfft;   /* Offset of the data in iDataPage */
	SyBlob sKey;       /* Record key for fast lookup (Kept in-memory if < 256KB ) */
	lhcell *pNext,*pPrev;         /* Linked list of the loaded memory cells */
	lhcell *pNextCol,*pPrevCol;   /* Collison chain  */
};
/*
** Each database page has a header that is an instance of this
** structure.
*/
typedef struct lhphdr lhphdr;
struct lhphdr 
{
  sxu16 iOfft; /* Offset of the first cell */
  sxu16 iFree; /* Offset of the first free block*/
  pgno iSlave; /* Slave page number */
};
/*
 * Each loaded primary disk page is represented in-memory using
 * an instance of the following structure.
 */
struct lhpage
{
	lhash_kv_engine *pHash;  /* KV Storage engine that own this page */
	vedis_page *pRaw;      /* Raw page contents */
	lhphdr sHdr;             /* Processed page header */
	lhcell **apCell;         /* Cell buckets */
	lhcell *pList,*pFirst;   /* Linked list of cells */
	sxu32 nCell;             /* Total number of cells */
	sxu32 nCellSize;         /* apCell[] size */
	lhpage *pMaster;         /* Master page in case we are dealing with a slave page */
	lhpage *pSlave;          /* List of slave pages */
	lhpage *pNextSlave;      /* Next slave page on the list */
	sxi32 iSlave;            /* Total number of slave pages */
	sxu16 nFree;             /* Amount of free space available in the page */
};
/*
 * A Bucket map record which is used to map logical bucket number to real
 * bucket number is represented by an instance of the following structure.
 */
typedef struct lhash_bmap_rec lhash_bmap_rec;
struct lhash_bmap_rec
{
	pgno iLogic;                   /* Logical bucket number */
	pgno iReal;                    /* Real bucket number */
	lhash_bmap_rec *pNext,*pPrev;  /* Link to other bucket map */     
	lhash_bmap_rec *pNextCol,*pPrevCol; /* Collision links */
};
typedef struct lhash_bmap_page lhash_bmap_page;
struct lhash_bmap_page
{
	pgno iNum;   /* Page number where this entry is stored */
	sxu16 iPtr;  /* Offset to start reading/writing from */
	sxu32 nRec;  /* Total number of records in this page */
	pgno iNext;  /* Next map page */
};
/*
 * An in memory linear hash implemenation is represented by in an isntance
 * of the following structure.
 */
struct lhash_kv_engine
{
	const vedis_kv_io *pIo;     /* IO methods: Must be first */
	/* Private fields */
	SyMemBackend sAllocator;      /* Private memory backend */
	ProcHash xHash;               /* Default hash function */
	ProcCmp xCmp;                 /* Default comparison function */
	vedis_page *pHeader;        /* Page one to identify a valid implementation */
	lhash_bmap_rec **apMap;       /* Buckets map records */
	sxu32 nBuckRec;               /* Total number of bucket map records */
	sxu32 nBuckSize;              /* apMap[] size  */
	lhash_bmap_rec *pList;        /* List of bucket map records */
	lhash_bmap_rec *pFirst;       /* First record*/
	lhash_bmap_page sPageMap;     /* Primary bucket map */
	int iPageSize;                /* Page size */
	pgno nFreeList;               /* List of free pages */
	pgno split_bucket;            /* Current split bucket: MUST BE A POWER OF TWO */
	pgno max_split_bucket;        /* Maximum split bucket: MUST BE A POWER OF TWO */
	pgno nmax_split_nucket;       /* Next maximum split bucket (1 << nMsb): In-memory only */
	sxu32 nMagic;                 /* Magic number to identify a valid linear hash disk database */
};
/*
 * Given a logical bucket number, return the record associated with it.
 */
static lhash_bmap_rec * lhMapFindBucket(lhash_kv_engine *pEngine,pgno iLogic)
{
	lhash_bmap_rec *pRec;
	if( pEngine->nBuckRec < 1 ){
		/* Don't bother */
		return 0;
	}
	pRec = pEngine->apMap[iLogic & (pEngine->nBuckSize - 1)];
	for(;;){
		if( pRec == 0 ){
			break;
		}
		if( pRec->iLogic == iLogic ){
			return pRec;
		}
		/* Point to the next entry */
		pRec = pRec->pNextCol;
	}
	/* No such record */
	return 0;
}
/*
 * Install a new bucket map record.
 */
static int lhMapInstallBucket(lhash_kv_engine *pEngine,pgno iLogic,pgno iReal)
{
	lhash_bmap_rec *pRec;
	sxu32 iBucket;
	/* Allocate a new instance */
	pRec = (lhash_bmap_rec *)SyMemBackendPoolAlloc(&pEngine->sAllocator,sizeof(lhash_bmap_rec));
	if( pRec == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pRec,sizeof(lhash_bmap_rec));
	/* Fill in the structure */
	pRec->iLogic = iLogic;
	pRec->iReal = iReal;
	iBucket = iLogic & (pEngine->nBuckSize - 1);
	pRec->pNextCol = pEngine->apMap[iBucket];
	if( pEngine->apMap[iBucket] ){
		pEngine->apMap[iBucket]->pPrevCol = pRec;
	}
	pEngine->apMap[iBucket] = pRec;
	/* Link */
	if( pEngine->pFirst == 0 ){
		pEngine->pFirst = pEngine->pList = pRec;
	}else{
		MACRO_LD_PUSH(pEngine->pList,pRec);
	}
	pEngine->nBuckRec++;
	if( (pEngine->nBuckRec >= pEngine->nBuckSize * 3) && pEngine->nBuckRec < 100000 ){
		/* Allocate a new larger table */
		sxu32 nNewSize = pEngine->nBuckSize << 1;
		lhash_bmap_rec *pEntry;
		lhash_bmap_rec **apNew;
		sxu32 n;
		
		apNew = (lhash_bmap_rec **)SyMemBackendAlloc(&pEngine->sAllocator, nNewSize * sizeof(lhash_bmap_rec *));
		if( apNew ){
			/* Zero the new table */
			SyZero((void *)apNew, nNewSize * sizeof(lhash_bmap_rec *));
			/* Rehash all entries */
			n = 0;
			pEntry = pEngine->pList;
			for(;;){
				/* Loop one */
				if( n >= pEngine->nBuckRec ){
					break;
				}
				pEntry->pNextCol = pEntry->pPrevCol = 0;
				/* Install in the new bucket */
				iBucket = pEntry->iLogic & (nNewSize - 1);
				pEntry->pNextCol = apNew[iBucket];
				if( apNew[iBucket] ){
					apNew[iBucket]->pPrevCol = pEntry;
				}
				apNew[iBucket] = pEntry;
				/* Point to the next entry */
				pEntry = pEntry->pNext;
				n++;
			}
			/* Release the old table and reflect the change */
			SyMemBackendFree(&pEngine->sAllocator,(void *)pEngine->apMap);
			pEngine->apMap = apNew;
			pEngine->nBuckSize  = nNewSize;
		}
	}
	return VEDIS_OK;
}
/*
 * Process a raw bucket map record.
 */
static int lhMapLoadPage(lhash_kv_engine *pEngine,lhash_bmap_page *pMap,const unsigned char *zRaw)
{
	const unsigned char *zEnd = &zRaw[pEngine->iPageSize];
	const unsigned char *zPtr = zRaw;
	pgno iLogic,iReal;
	sxu32 n;
	int rc;
	if( pMap->iPtr == 0 ){
		/* Read the map header */
		SyBigEndianUnpack64(zRaw,&pMap->iNext);
		zRaw += 8;
		SyBigEndianUnpack32(zRaw,&pMap->nRec);
		zRaw += 4;
	}else{
		/* Mostly page one of the database */
		zRaw += pMap->iPtr;
	}
	/* Start processing */
	for( n = 0; n < pMap->nRec ; ++n ){
		if( zRaw >= zEnd ){
			break;
		}
		/* Extract the logical and real bucket number */
		SyBigEndianUnpack64(zRaw,&iLogic);
		zRaw += 8;
		SyBigEndianUnpack64(zRaw,&iReal);
		zRaw += 8;
		/* Install the record in the map */
		rc = lhMapInstallBucket(pEngine,iLogic,iReal);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}
	pMap->iPtr = (sxu16)(zRaw-zPtr);
	/* All done */
	return VEDIS_OK;
}
/* 
 * Allocate a new cell instance.
 */
static lhcell * lhNewCell(lhash_kv_engine *pEngine,lhpage *pPage)
{
	lhcell *pCell;
	pCell = (lhcell *)SyMemBackendPoolAlloc(&pEngine->sAllocator,sizeof(lhcell));
	if( pCell == 0 ){
		return 0;
	}
	/* Zero the structure */
	SyZero(pCell,sizeof(lhcell));
	/* Fill in the structure */
	SyBlobInit(&pCell->sKey,&pEngine->sAllocator);
	pCell->pPage = pPage;
	return pCell;
}
/*
 * Discard a cell from the page table.
 */
static void lhCellDiscard(lhcell *pCell)
{
	lhpage *pPage = pCell->pPage->pMaster;	
	
	if( pCell->pPrevCol ){
		pCell->pPrevCol->pNextCol = pCell->pNextCol;
	}else{
		pPage->apCell[pCell->nHash & (pPage->nCellSize - 1)] = pCell->pNextCol;
	}
	if( pCell->pNextCol ){
		pCell->pNextCol->pPrevCol = pCell->pPrevCol;
	}
	MACRO_LD_REMOVE(pPage->pList,pCell);
	if( pCell == pPage->pFirst ){
		pPage->pFirst = pCell->pPrev;
	}
	pPage->nCell--;
	/* Release the cell */
	SyBlobRelease(&pCell->sKey);
	SyMemBackendPoolFree(&pPage->pHash->sAllocator,pCell);
}
/*
 * Install a cell in the page table.
 */
static int lhInstallCell(lhcell *pCell)
{
	lhpage *pPage = pCell->pPage->pMaster;
	sxu32 iBucket;
	if( pPage->nCell < 1 ){
		sxu32 nTableSize = 32; /* Must be a power of two */
		lhcell **apTable;
		/* Allocate a new cell table */
		apTable = (lhcell **)SyMemBackendAlloc(&pPage->pHash->sAllocator, nTableSize * sizeof(lhcell *));
		if( apTable == 0 ){
			return VEDIS_NOMEM;
		}
		/* Zero the new table */
		SyZero((void *)apTable, nTableSize * sizeof(lhcell *));
		/* Install it */
		pPage->apCell = apTable;
		pPage->nCellSize = nTableSize;
	}
	iBucket = pCell->nHash & (pPage->nCellSize - 1);
	pCell->pNextCol = pPage->apCell[iBucket];
	if( pPage->apCell[iBucket] ){
		pPage->apCell[iBucket]->pPrevCol = pCell;
	}
	pPage->apCell[iBucket] = pCell;
	if( pPage->pFirst == 0 ){
		pPage->pFirst = pPage->pList = pCell;
	}else{
		MACRO_LD_PUSH(pPage->pList,pCell);
	}
	pPage->nCell++;
	if( (pPage->nCell >= pPage->nCellSize * 3) && pPage->nCell < 100000 ){
		/* Allocate a new larger table */
		sxu32 nNewSize = pPage->nCellSize << 1;
		lhcell *pEntry;
		lhcell **apNew;
		sxu32 n;
		
		apNew = (lhcell **)SyMemBackendAlloc(&pPage->pHash->sAllocator, nNewSize * sizeof(lhcell *));
		if( apNew ){
			/* Zero the new table */
			SyZero((void *)apNew, nNewSize * sizeof(lhcell *));
			/* Rehash all entries */
			n = 0;
			pEntry = pPage->pList;
			for(;;){
				/* Loop one */
				if( n >= pPage->nCell ){
					break;
				}
				pEntry->pNextCol = pEntry->pPrevCol = 0;
				/* Install in the new bucket */
				iBucket = pEntry->nHash & (nNewSize - 1);
				pEntry->pNextCol = apNew[iBucket];
				if( apNew[iBucket]  ){
					apNew[iBucket]->pPrevCol = pEntry;
				}
				apNew[iBucket] = pEntry;
				/* Point to the next entry */
				pEntry = pEntry->pNext;
				n++;
			}
			/* Release the old table and reflect the change */
			SyMemBackendFree(&pPage->pHash->sAllocator,(void *)pPage->apCell);
			pPage->apCell = apNew;
			pPage->nCellSize  = nNewSize;
		}
	}
	return VEDIS_OK;
}
/*
 * Private data of lhKeyCmp().
 */
struct lhash_key_cmp
{
	const char *zIn;  /* Start of the stream */
	const char *zEnd; /* End of the stream */
	ProcCmp xCmp;     /* Comparison function */
};
/*
 * Comparsion callback for large key > 256 KB
 */
static int lhKeyCmp(const void *pData,sxu32 nLen,void *pUserData)
{
	struct lhash_key_cmp *pCmp = (struct lhash_key_cmp *)pUserData;
	int rc;
	if( pCmp->zIn >= pCmp->zEnd ){
		if( nLen > 0 ){
			return VEDIS_ABORT;
		}
		return VEDIS_OK;
	}
	/* Perform the comparison */
	rc = pCmp->xCmp((const void *)pCmp->zIn,pData,nLen);
	if( rc != 0 ){
		/* Abort comparison */
		return VEDIS_ABORT;
	}
	/* Advance the cursor */
	pCmp->zIn += nLen;
	return VEDIS_OK;
}
/* Forward declaration */
static int lhConsumeCellkey(lhcell *pCell,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData,int offt_only);
/*
 * given a key, return the cell associated with it on success. NULL otherwise.
 */
static lhcell * lhFindCell(
	lhpage *pPage,    /* Target page */
	const void *pKey, /* Lookup key */
	sxu32 nByte,      /* Key length */
	sxu32 nHash       /* Hash of the key */
	)
{
	lhcell *pEntry;
	if( pPage->nCell < 1 ){
		/* Don't bother hashing */
		return 0;
	}
	/* Point to the corresponding bucket */
	pEntry = pPage->apCell[nHash & (pPage->nCellSize - 1)];
	for(;;){
		if( pEntry == 0 ){
			break;
		}
		if( pEntry->nHash == nHash && pEntry->nKey == nByte ){
			if( SyBlobLength(&pEntry->sKey) < 1 ){
				/* Large key (> 256 KB) are not kept in-memory */
				struct lhash_key_cmp sCmp;
				int rc;
				/* Fill-in the structure */
				sCmp.zIn = (const char *)pKey;
				sCmp.zEnd = &sCmp.zIn[nByte];
				sCmp.xCmp = pPage->pHash->xCmp;
				/* Fetch the key from disk and perform the comparison */
				rc = lhConsumeCellkey(pEntry,lhKeyCmp,&sCmp,0);
				if( rc == VEDIS_OK ){
					/* Cell found */
					return pEntry;
				}
			}else if ( pPage->pHash->xCmp(pKey,SyBlobData(&pEntry->sKey),nByte) == 0 ){
				/* Cell found */
				return pEntry;
			}
		}
		/* Point to the next entry */
		pEntry = pEntry->pNextCol;
	}
	/* No such entry */
	return 0;
}
/*
 * Parse a raw cell fetched from disk.
 */
static int lhParseOneCell(lhpage *pPage,const unsigned char *zRaw,const unsigned char *zEnd,lhcell **ppOut)
{
	sxu16 iNext,iOfft;
	sxu32 iHash,nKey;
	lhcell *pCell;
	sxu64 nData;
	int rc;
	/* Offset this cell is stored */
	iOfft = (sxu16)(zRaw - (const unsigned char *)pPage->pRaw->zData);
	/* 4 byte hash number */
	SyBigEndianUnpack32(zRaw,&iHash);
	zRaw += 4;	
	/* 4 byte key length  */
	SyBigEndianUnpack32(zRaw,&nKey);
	zRaw += 4;	
	/* 8 byte data length */
	SyBigEndianUnpack64(zRaw,&nData);
	zRaw += 8;
	/* 2 byte offset of the next cell */
	SyBigEndianUnpack16(zRaw,&iNext);
	/* Perform a sanity check */
	if( iNext > 0 && &pPage->pRaw->zData[iNext] >= zEnd ){
		return VEDIS_CORRUPT;
	}
	zRaw += 2;
	pCell = lhNewCell(pPage->pHash,pPage);
	if( pCell == 0 ){
		return VEDIS_NOMEM;
	}
	/* Fill in the structure */
	pCell->iNext = iNext;
	pCell->nKey  = nKey;
	pCell->nData = nData;
	pCell->nHash = iHash;
	/* Overflow page if any */
	SyBigEndianUnpack64(zRaw,&pCell->iOvfl);
	zRaw += 8;
	/* Cell offset */
	pCell->iStart = iOfft;
	/* Consume the key */
	rc = lhConsumeCellkey(pCell,vedisDataConsumer,&pCell->sKey,pCell->nKey > 262144 /* 256 KB */? 1 : 0);
	if( rc != VEDIS_OK ){
		/* TICKET: 14-32-chm@symisc.net: Key too large for memory */
		SyBlobRelease(&pCell->sKey);
	}
	/* Finally install the cell */
	rc = lhInstallCell(pCell);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( ppOut ){
		*ppOut = pCell;
	}
	return VEDIS_OK;
}
/*
 * Compute the total number of free space on a given page.
 */
static int lhPageFreeSpace(lhpage *pPage)
{
	const unsigned char *zEnd,*zRaw = pPage->pRaw->zData;
	lhphdr *pHdr = &pPage->sHdr;
	sxu16 iNext,iAmount;
	sxu16 nFree = 0;
	if( pHdr->iFree < 1 ){
		/* Don't bother processing, the page is full */
		pPage->nFree = 0;
		return VEDIS_OK;
	}
	/* Point to first free block */
	zEnd = &zRaw[pPage->pHash->iPageSize];
	zRaw += pHdr->iFree;
	for(;;){
		/* Offset of the next free block */
		SyBigEndianUnpack16(zRaw,&iNext);
		zRaw += 2;
		/* Available space on this block */
		SyBigEndianUnpack16(zRaw,&iAmount);
		nFree += iAmount;
		if( iNext < 1 ){
			/* No more free blocks */
			break;
		}
		/* Point to the next free block*/
		zRaw = &pPage->pRaw->zData[iNext];
		if( zRaw >= zEnd ){
			/* Corrupt page */
			return VEDIS_CORRUPT;
		}
	}
	/* Save the amount of free space */
	pPage->nFree = nFree;
	return VEDIS_OK;
}
/*
 * Given a primary page, load all its cell.
 */
static int lhLoadCells(lhpage *pPage)
{
	const unsigned char *zEnd,*zRaw = pPage->pRaw->zData;
	lhphdr *pHdr = &pPage->sHdr;
	lhcell *pCell = 0; /* cc warning */
	int rc;
	/* Calculate the amount of free space available first */
	rc = lhPageFreeSpace(pPage);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( pHdr->iOfft < 1 ){
		/* Don't bother processing, the page is empty */
		return VEDIS_OK;
	}
	/* Point to first cell */
	zRaw += pHdr->iOfft;
	zEnd = &zRaw[pPage->pHash->iPageSize];
	for(;;){
		/* Parse a single cell */
		rc = lhParseOneCell(pPage,zRaw,zEnd,&pCell);
		if( rc != VEDIS_OK ){
			return rc;
		}
		if( pCell->iNext < 1 ){
			/* No more cells */
			break;
		}
		/* Point to the next cell */
		zRaw = &pPage->pRaw->zData[pCell->iNext];
		if( zRaw >= zEnd ){
			/* Corrupt page */
			return VEDIS_CORRUPT;
		}
	}
	/* All done */
	return VEDIS_OK;
}
/*
 * Given a page, parse its raw headers.
 */
static int lhParsePageHeader(lhpage *pPage)
{
	const unsigned char *zRaw = pPage->pRaw->zData;
	lhphdr *pHdr = &pPage->sHdr;
	/* Offset of the first cell */
	SyBigEndianUnpack16(zRaw,&pHdr->iOfft);
	zRaw += 2;
	/* Offset of the first free block */
	SyBigEndianUnpack16(zRaw,&pHdr->iFree);
	zRaw += 2;
	/* Slave page number */
	SyBigEndianUnpack64(zRaw,&pHdr->iSlave);
	/* All done */
	return VEDIS_OK;
}
/*
 * Allocate a new page instance.
 */
static lhpage * lhNewPage(
	lhash_kv_engine *pEngine, /* KV store which own this instance */
	vedis_page *pRaw,       /* Raw page contents */
	lhpage *pMaster           /* Master page in case we are dealing with a slave page */
	)
{
	lhpage *pPage;
	/* Allocate a new instance */
	pPage = (lhpage *)SyMemBackendPoolAlloc(&pEngine->sAllocator,sizeof(lhpage));
	if( pPage == 0 ){
		return 0;
	}
	/* Zero the structure */
	SyZero(pPage,sizeof(lhpage));
	/* Fill-in the structure */
	pPage->pHash = pEngine;
	pPage->pRaw = pRaw;
	pPage->pMaster = pMaster ? pMaster /* Slave page */ : pPage /* Master page */ ;
	if( pPage->pMaster != pPage ){
		/* Slave page, attach it to its master */
		pPage->pNextSlave = pMaster->pSlave;
		pMaster->pSlave = pPage;
		pMaster->iSlave++;
	}
	/* Save this instance for future fast lookup */
	pRaw->pUserData = pPage;
	/* All done */
	return pPage;
}
/*
 * Load a primary and its associated slave pages from disk.
 */
static int lhLoadPage(lhash_kv_engine *pEngine,pgno pnum,lhpage *pMaster,lhpage **ppOut,int iNest)
{
	vedis_page *pRaw;
	lhpage *pPage = 0; /* cc warning */
	int rc;
	/* Aquire the page from the pager first */
	rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,pnum,&pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( pRaw->pUserData ){
		/* The page is already parsed and loaded in memory. Point to it */
		pPage = (lhpage *)pRaw->pUserData;
	}else{
		/* Allocate a new page */
		pPage = lhNewPage(pEngine,pRaw,pMaster);
		if( pPage == 0 ){
			return VEDIS_NOMEM;
		}
		/* Process the page */
		rc = lhParsePageHeader(pPage);
		if( rc == VEDIS_OK ){
			/* Load cells */
			rc = lhLoadCells(pPage);
		}
		if( rc != VEDIS_OK ){
			pEngine->pIo->xPageUnref(pPage->pRaw); /* pPage will be released inside this call */
			return rc;
		}
		if( pPage->sHdr.iSlave > 0 && iNest < 128 ){
			if( pMaster == 0 ){
				pMaster = pPage;
			}
			/* Slave page. Not a fatal error if something goes wrong here */
			lhLoadPage(pEngine,pPage->sHdr.iSlave,pMaster,0,iNest++);
		}
	}
	if( ppOut ){
		*ppOut = pPage;
	}
	return VEDIS_OK;
}
/*
 * Given a cell, Consume its key by invoking the given callback for each extracted chunk.
 */
static int lhConsumeCellkey(
	lhcell *pCell, /* Target cell */
	int (*xConsumer)(const void *,unsigned int,void *), /* Consumer callback */
	void *pUserData, /* Last argument to xConsumer() */
	int offt_only
	)
{
	lhpage *pPage = pCell->pPage;
	const unsigned char *zRaw = pPage->pRaw->zData;
	const unsigned char *zPayload;
	int rc;
	/* Point to the payload area */
	zPayload = &zRaw[pCell->iStart];
	if( pCell->iOvfl == 0 ){
		/* Best scenario, consume the key directly without any overflow page */
		zPayload += L_HASH_CELL_SZ;
		rc = xConsumer((const void *)zPayload,pCell->nKey,pUserData);
		if( rc != VEDIS_OK ){
			rc = VEDIS_ABORT;
		}
	}else{
		lhash_kv_engine *pEngine = pPage->pHash;
		sxu32 nByte,nData = pCell->nKey;
		vedis_page *pOvfl;
		int data_offset = 0;
		pgno iOvfl;
		/* Overflow page */
		iOvfl = pCell->iOvfl;
		/* Total usable bytes in an overflow page */
		nByte = L_HASH_OVERFLOW_SIZE(pEngine->iPageSize);
		for(;;){
			if( iOvfl == 0 || nData < 1 ){
				/* no more overflow page */
				break;
			}
			/* Point to the overflow page */
			rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,iOvfl,&pOvfl);
			if( rc != VEDIS_OK ){
				return rc;
			}
			zPayload = &pOvfl->zData[8];
			/* Point to the raw content */
			if( !data_offset ){
				/* Get the data page and offset */
				SyBigEndianUnpack64(zPayload,&pCell->iDataPage);
				zPayload += 8;
				SyBigEndianUnpack16(zPayload,&pCell->iDataOfft);
				zPayload += 2;
				if( offt_only ){
					/* Key too large, grab the data offset and return */
					pEngine->pIo->xPageUnref(pOvfl);
					return VEDIS_OK;
				}
				data_offset = 1;
			}
			/* Consume the key */
			if( nData <= nByte ){
				rc = xConsumer((const void *)zPayload,nData,pUserData);
				if( rc != VEDIS_OK ){
					pEngine->pIo->xPageUnref(pOvfl);
					return VEDIS_ABORT;
				}
				nData = 0;
			}else{
				rc = xConsumer((const void *)zPayload,nByte,pUserData);
				if( rc != VEDIS_OK ){
					pEngine->pIo->xPageUnref(pOvfl);
					return VEDIS_ABORT;
				}
				nData -= nByte;
			}
			/* Next overflow page in the chain */
			SyBigEndianUnpack64(pOvfl->zData,&iOvfl);
			/* Unref the page */
			pEngine->pIo->xPageUnref(pOvfl);
		}
		rc = VEDIS_OK;
	}
	return rc;
}
/*
 * Given a cell, Consume its data by invoking the given callback for each extracted chunk.
 */
static int lhConsumeCellData(
	lhcell *pCell, /* Target cell */
	int (*xConsumer)(const void *,unsigned int,void *), /* Data consumer callback */
	void *pUserData /* Last argument to xConsumer() */
	)
{
	lhpage *pPage = pCell->pPage;
	const unsigned char *zRaw = pPage->pRaw->zData;
	const unsigned char *zPayload;
	int rc;
	/* Point to the payload area */
	zPayload = &zRaw[pCell->iStart];
	if( pCell->iOvfl == 0 ){
		/* Best scenario, consume the data directly without any overflow page */
		zPayload += L_HASH_CELL_SZ + pCell->nKey;
		rc = xConsumer((const void *)zPayload,(sxu32)pCell->nData,pUserData);
		if( rc != VEDIS_OK ){
			rc = VEDIS_ABORT;
		}
	}else{
		lhash_kv_engine *pEngine = pPage->pHash;
		sxu64 nData = pCell->nData;
		vedis_page *pOvfl;
		int fix_offset = 0;
		sxu32 nByte;
		pgno iOvfl;
		/* Overflow page where data is stored */
		iOvfl = pCell->iDataPage;
		for(;;){
			if( iOvfl == 0 || nData < 1 ){
				/* no more overflow page */
				break;
			}
			/* Point to the overflow page */
			rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,iOvfl,&pOvfl);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Point to the raw content */
			zPayload = pOvfl->zData;
			if( !fix_offset ){
				/* Point to the data */
				zPayload += pCell->iDataOfft;
				nByte = pEngine->iPageSize - pCell->iDataOfft;
				fix_offset = 1;
			}else{
				zPayload += 8;
				/* Total usable bytes in an overflow page */
				nByte = L_HASH_OVERFLOW_SIZE(pEngine->iPageSize);
			}
			/* Consume the data */
			if( nData <= (sxu64)nByte ){
				rc = xConsumer((const void *)zPayload,(unsigned int)nData,pUserData);
				if( rc != VEDIS_OK ){
					pEngine->pIo->xPageUnref(pOvfl);
					return VEDIS_ABORT;
				}
				nData = 0;
			}else{
				if( nByte > 0 ){
					rc = xConsumer((const void *)zPayload,nByte,pUserData);
					if( rc != VEDIS_OK ){
						pEngine->pIo->xPageUnref(pOvfl);
						return VEDIS_ABORT;
					}
					nData -= nByte;
				}
			}
			/* Next overflow page in the chain */
			SyBigEndianUnpack64(pOvfl->zData,&iOvfl);
			/* Unref the page */
			pEngine->pIo->xPageUnref(pOvfl);
		}
		rc = VEDIS_OK;
	}
	return rc;
}
/*
 * Read the linear hash header (Page one of the database).
 */
static int lhash_read_header(lhash_kv_engine *pEngine,vedis_page *pHeader)
{
	const unsigned char *zRaw = pHeader->zData;
	lhash_bmap_page *pMap;
	sxu32 nHash;
	int rc;
	pEngine->pHeader = pHeader;
	/* 4 byte magic number */
	SyBigEndianUnpack32(zRaw,&pEngine->nMagic);
	zRaw += 4;
	if( pEngine->nMagic != L_HASH_MAGIC ){
		/* Corrupt implementation */
		return VEDIS_CORRUPT;
	}
	/* 4 byte hash value to identify a valid hash function */
	SyBigEndianUnpack32(zRaw,&nHash);
	zRaw += 4;
	/* Sanity check */
	if( pEngine->xHash(L_HASH_WORD,sizeof(L_HASH_WORD)-1) != nHash ){
		/* Different hash function */
		pEngine->pIo->xErr(pEngine->pIo->pHandle,"Invalid hash function");
		return VEDIS_INVALID;
	}
	/* List of free pages */
	SyBigEndianUnpack64(zRaw,&pEngine->nFreeList);
	zRaw += 8;
	/* Current split bucket */
	SyBigEndianUnpack64(zRaw,&pEngine->split_bucket);
	zRaw += 8;
	/* Maximum split bucket */
	SyBigEndianUnpack64(zRaw,&pEngine->max_split_bucket);
	zRaw += 8;
	/* Next generation */
	pEngine->nmax_split_nucket = pEngine->max_split_bucket << 1;
	/* Initialiaze the bucket map */
	pMap = &pEngine->sPageMap;
	/* Fill in the structure */
	pMap->iNum = pHeader->pgno;
	/* Next page in the bucket map */
	SyBigEndianUnpack64(zRaw,&pMap->iNext);
	zRaw += 8;
	/* Total number of records in the bucket map (This page only) */
	SyBigEndianUnpack32(zRaw,&pMap->nRec);
	zRaw += 4;
	pMap->iPtr = (sxu16)(zRaw - pHeader->zData);
	/* Load the map in memory */
	rc = lhMapLoadPage(pEngine,pMap,pHeader->zData);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Load the bucket map chain if any */
	for(;;){
		pgno iNext = pMap->iNext;
		vedis_page *pPage;
		if( iNext == 0 ){
			/* No more map pages */
			break;
		}
		/* Point to the target page */
		rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,iNext,&pPage);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Fill in the structure */
		pMap->iNum = iNext;
		pMap->iPtr = 0;
		/* Load the map in memory */
		rc = lhMapLoadPage(pEngine,pMap,pPage->zData);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}
	/* All done */
	return VEDIS_OK;
}
/*
 * Perform a record lookup.
 */
static int lhRecordLookup(
	lhash_kv_engine *pEngine, /* KV storage engine */
	const void *pKey,         /* Lookup key */
	sxu32 nByte,              /* Key length */
	lhcell **ppCell           /* OUT: Target cell on success */
	)
{
	lhash_bmap_rec *pRec;
	lhpage *pPage;
	lhcell *pCell;
	pgno iBucket;
	sxu32 nHash;
	int rc;
	/* Acquire the first page (hash Header) so that everything gets loaded autmatically */
	rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,1,0);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Compute the hash of the key first */
	nHash = pEngine->xHash(pKey,nByte);
	/* Extract the logical (i.e. not real) page number */
	iBucket = nHash & (pEngine->nmax_split_nucket - 1);
	if( iBucket >= (pEngine->split_bucket + pEngine->max_split_bucket) ){
		/* Low mask */
		iBucket = nHash & (pEngine->max_split_bucket - 1);
	}
	/* Map the logical bucket number to real page number */
	pRec = lhMapFindBucket(pEngine,iBucket);
	if( pRec == 0 ){
		/* No such entry */
		return VEDIS_NOTFOUND;
	}
	/* Load the master page and it's slave page in-memory  */
	rc = lhLoadPage(pEngine,pRec->iReal,0,&pPage,0);
	if( rc != VEDIS_OK ){
		/* IO error, unlikely scenario */
		return rc;
	}
	/* Lookup for the cell */
	pCell = lhFindCell(pPage,pKey,nByte,nHash);
	if( pCell == 0 ){
		/* No such entry */
		return VEDIS_NOTFOUND;
	}
	if( ppCell ){
		*ppCell = pCell;
	}
	return VEDIS_OK;
}
/*
 * Acquire a new page either from the free list or ask the pager
 * for a new one.
 */
static int lhAcquirePage(lhash_kv_engine *pEngine,vedis_page **ppOut)
{
	vedis_page *pPage;
	int rc;
	if( pEngine->nFreeList != 0 ){
		/* Acquire one from the free list */
		rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,pEngine->nFreeList,&pPage);
		if( rc == VEDIS_OK ){
			/* Point to the next free page */
			SyBigEndianUnpack64(pPage->zData,&pEngine->nFreeList);
			/* Update the database header */
			rc = pEngine->pIo->xWrite(pEngine->pHeader);
			if( rc != VEDIS_OK ){
				return rc;
			}
			SyBigEndianPack64(&pEngine->pHeader->zData[4/*Magic*/+4/*Hash*/],pEngine->nFreeList);
			/* Tell the pager do not journal this page */
			pEngine->pIo->xDontJournal(pPage);
			/* Return to the caller */
			*ppOut = pPage;
			/* All done */
			return VEDIS_OK;
		}
	}
	/* Acquire a new page */
	rc = pEngine->pIo->xNew(pEngine->pIo->pHandle,&pPage);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Point to the target page */
	*ppOut = pPage;
	return VEDIS_OK;
}
/*
 * Write a bucket map record to disk.
 */
static int lhMapWriteRecord(lhash_kv_engine *pEngine,pgno iLogic,pgno iReal)
{
	lhash_bmap_page *pMap = &pEngine->sPageMap;
	vedis_page *pPage = 0;
	int rc;
	if( pMap->iPtr > (pEngine->iPageSize - 16) /* 8 byte logical bucket number + 8 byte real bucket number */ ){
		vedis_page *pOld;
		/* Point to the old page */
		rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,pMap->iNum,&pOld);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Acquire a new page */
		rc = lhAcquirePage(pEngine,&pPage);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Reflect the change  */
		pMap->iNext = 0;
		pMap->iNum = pPage->pgno;
		pMap->nRec = 0;
		pMap->iPtr = 8/* Next page number */+4/* Total records in the map*/;
		/* Link this page */
		rc = pEngine->pIo->xWrite(pOld);
		if( rc != VEDIS_OK ){
			return rc;
		}
		if( pOld->pgno == pEngine->pHeader->pgno ){
			/* First page (Hash header) */
			SyBigEndianPack64(&pOld->zData[4/*magic*/+4/*hash*/+8/* Free page */+8/*current split bucket*/+8/*Maximum split bucket*/],pPage->pgno);
		}else{
			/* Link the new page */
			SyBigEndianPack64(pOld->zData,pPage->pgno);
			/* Unref */
			pEngine->pIo->xPageUnref(pOld);
		}
		/* Assume the last bucket map page */
		rc = pEngine->pIo->xWrite(pPage);
		if( rc != VEDIS_OK ){
			return rc;
		}
		SyBigEndianPack64(pPage->zData,0); /* Next bucket map page on the list */
	}
	if( pPage == 0){
		/* Point to the current map page */
		rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,pMap->iNum,&pPage);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}
	/* Make page writable */
	rc = pEngine->pIo->xWrite(pPage);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Write the data */
	SyBigEndianPack64(&pPage->zData[pMap->iPtr],iLogic);
	pMap->iPtr += 8;
	SyBigEndianPack64(&pPage->zData[pMap->iPtr],iReal);
	pMap->iPtr += 8;
	/* Install the bucket map */
	rc = lhMapInstallBucket(pEngine,iLogic,iReal);
	if( rc == VEDIS_OK ){
		/* Total number of records */
		pMap->nRec++;
		if( pPage->pgno == pEngine->pHeader->pgno ){
			/* Page one: Always writable */
			SyBigEndianPack32(
				&pPage->zData[4/*magic*/+4/*hash*/+8/* Free page */+8/*current split bucket*/+8/*Maximum split bucket*/+8/*Next map page*/],
				pMap->nRec);
		}else{
			/* Make page writable */
			rc = pEngine->pIo->xWrite(pPage);
			if( rc != VEDIS_OK ){
				return rc;
			}
			SyBigEndianPack32(&pPage->zData[8],pMap->nRec);
		}
	}
	return rc;
}
/*
 * Defragment a page.
 */
static int lhPageDefragment(lhpage *pPage)
{
	lhash_kv_engine *pEngine = pPage->pHash;
	unsigned char *zTmp,*zPtr,*zEnd,*zPayload;
	lhcell *pCell;
	/* Get a temporary page from the pager. This opertaion never fail */
	zTmp = pEngine->pIo->xTmpPage(pEngine->pIo->pHandle);
	/* Move the target cells to the begining */
	pCell = pPage->pList;
	/* Write the slave page number */
	SyBigEndianPack64(&zTmp[2/*Offset of the first cell */+2/*Offset of the first free block */],pPage->sHdr.iSlave);
	zPtr = &zTmp[L_HASH_PAGE_HDR_SZ]; /* Offset to start writing from */
	zEnd = &zTmp[pEngine->iPageSize];
	pPage->sHdr.iOfft = 0; /* Offset of the first cell */
	for(;;){
		if( pCell == 0 ){
			/* No more cells */
			break;
		}
		if( pCell->pPage->pRaw->pgno == pPage->pRaw->pgno ){
			/* Cell payload if locally stored */
			zPayload = 0;
			if( pCell->iOvfl == 0 ){
				zPayload = &pCell->pPage->pRaw->zData[pCell->iStart + L_HASH_CELL_SZ];
			}
			/* Move the cell */
			pCell->iNext = pPage->sHdr.iOfft;
			pCell->iStart = (sxu16)(zPtr - zTmp); /* Offset where this cell start */
			pPage->sHdr.iOfft = pCell->iStart;
			/* Write the cell header */
			/* 4 byte hash number */
			SyBigEndianPack32(zPtr,pCell->nHash);
			zPtr += 4;
			/* 4 byte ley length */
			SyBigEndianPack32(zPtr,pCell->nKey);
			zPtr += 4;
			/* 8 byte data length */
			SyBigEndianPack64(zPtr,pCell->nData);
			zPtr += 8;
			/* 2 byte offset of the next cell */
			SyBigEndianPack16(zPtr,pCell->iNext);
			zPtr += 2;
			/* 8 byte overflow page number */
			SyBigEndianPack64(zPtr,pCell->iOvfl);
			zPtr += 8;
			if( zPayload ){
				/* Local payload */
				SyMemcpy((const void *)zPayload,zPtr,(sxu32)(pCell->nKey + pCell->nData));
				zPtr += pCell->nKey + pCell->nData;
			}
			if( zPtr >= zEnd ){
				/* Can't happen */
				break;
			}
		}
		/* Point to the next page */
		pCell = pCell->pNext;
	}
	/* Mark the free block */
	pPage->nFree = (sxu16)(zEnd - zPtr); /* Block length */
	if( pPage->nFree > 3 ){
		pPage->sHdr.iFree = (sxu16)(zPtr - zTmp); /* Offset of the free block */
		/* Mark the block */
		SyBigEndianPack16(zPtr,0); /* Offset of the next free block */
		SyBigEndianPack16(&zPtr[2],pPage->nFree); /* Block length */
	}else{
		/* Block of length less than 4 bytes are simply discarded */
		pPage->nFree = 0;
		pPage->sHdr.iFree = 0;
	}
	/* Reflect the change */
	SyBigEndianPack16(zTmp,pPage->sHdr.iOfft);     /* Offset of the first cell */
	SyBigEndianPack16(&zTmp[2],pPage->sHdr.iFree); /* Offset of the first free block */
	SyMemcpy((const void *)zTmp,pPage->pRaw->zData,pEngine->iPageSize);
	/* All done */
	return VEDIS_OK;
}
/*
** Allocate nByte bytes of space on a page.
**
** Return the index into pPage->pRaw->zData[] of the first byte of
** the new allocation. Or return 0 if there is not enough free
** space on the page to satisfy the allocation request.
**
** If the page contains nBytes of free space but does not contain
** nBytes of contiguous free space, then this routine automatically
** calls defragementPage() to consolidate all free space before 
** allocating the new chunk.
*/
static int lhAllocateSpace(lhpage *pPage,sxu64 nAmount,sxu16 *pOfft)
{
	const unsigned char *zEnd,*zPtr;
	sxu16 iNext,iBlksz,nByte;
	unsigned char *zPrev;
	int rc;
	if( (sxu64)pPage->nFree < nAmount ){
		/* Don't bother looking for a free chunk */
		return VEDIS_FULL;
	}
	if( pPage->nCell < 10 && ((int)nAmount >= (pPage->pHash->iPageSize / 2)) ){
		/* Big chunk need an overflow page for its data */
		return VEDIS_FULL;
	}
	zPtr = &pPage->pRaw->zData[pPage->sHdr.iFree];
	zEnd = &pPage->pRaw->zData[pPage->pHash->iPageSize];
	nByte = (sxu16)nAmount;
	zPrev = 0;
	iBlksz = 0; /* cc warning */
	/* Perform the lookup */
	for(;;){
		if( zPtr >= zEnd ){
			return VEDIS_FULL;
		}
		/* Offset of the next free block */
		SyBigEndianUnpack16(zPtr,&iNext);
		/* Block size */
		SyBigEndianUnpack16(&zPtr[2],&iBlksz);
		if( iBlksz >= nByte ){
			/* Got one */
			break;
		}
		zPrev = (unsigned char *)zPtr;
		if( iNext == 0 ){
			/* No more free blocks, defragment the page */
			rc = lhPageDefragment(pPage);
			if( rc == VEDIS_OK && pPage->nFree >= nByte) {
				/* Free blocks are merged together */
				iNext = 0;
				zPtr = &pPage->pRaw->zData[pPage->sHdr.iFree];
				iBlksz = pPage->nFree;
				zPrev = 0;
				break;
			}else{
				return VEDIS_FULL;
			}
		}
		/* Point to the next free block */
		zPtr = &pPage->pRaw->zData[iNext];
	}
	/* Acquire writer lock on this page */
	rc = pPage->pHash->pIo->xWrite(pPage->pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Save block offset */
	*pOfft = (sxu16)(zPtr - pPage->pRaw->zData);
	/* Fix pointers */
	if( iBlksz >= nByte && (iBlksz - nByte) > 3 ){
		unsigned char *zBlock = &pPage->pRaw->zData[(*pOfft) + nByte];
		/* Create a new block */
		zPtr = zBlock;
		SyBigEndianPack16(zBlock,iNext); /* Offset of the next block */
		SyBigEndianPack16(&zBlock[2],iBlksz-nByte); /* Block size*/
		/* Offset of the new block */
		iNext = (sxu16)(zPtr - pPage->pRaw->zData);
	}
	/* Fix offsets */
	if( zPrev ){
		SyBigEndianPack16(zPrev,iNext);
	}else{
		/* First block */
		pPage->sHdr.iFree = iNext;
		/* Reflect on the page header */
		SyBigEndianPack16(&pPage->pRaw->zData[2/* Offset of the first cell1*/],iNext);
	}
	/* All done */
	pPage->nFree -= nByte;
	return VEDIS_OK;
}
/*
 * Write the cell header into the corresponding offset.
 */
static int lhCellWriteHeader(lhcell *pCell)
{
	lhpage *pPage = pCell->pPage;
	unsigned char *zRaw = pPage->pRaw->zData;
	/* Seek to the desired location */
	zRaw += pCell->iStart;
	/* 4 byte hash number */
	SyBigEndianPack32(zRaw,pCell->nHash);
	zRaw += 4;
	/* 4 byte key length */
	SyBigEndianPack32(zRaw,pCell->nKey);
	zRaw += 4;
	/* 8 byte data length */
	SyBigEndianPack64(zRaw,pCell->nData);
	zRaw += 8;
	/* 2 byte offset of the next cell */
	pCell->iNext = pPage->sHdr.iOfft;
	SyBigEndianPack16(zRaw,pCell->iNext);
	zRaw += 2;
	/* 8 byte overflow page number */
	SyBigEndianPack64(zRaw,pCell->iOvfl);
	/* Update the page header */
	pPage->sHdr.iOfft = pCell->iStart;
	/* pEngine->pIo->xWrite() has been successfully called on this page */
	SyBigEndianPack16(pPage->pRaw->zData,pCell->iStart);
	/* All done */
	return VEDIS_OK;
}
/*
 * Write local payload.
 */
static int lhCellWriteLocalPayload(lhcell *pCell,
	const void *pKey,sxu32 nKeylen,
	const void *pData,vedis_int64 nDatalen
	)
{
	/* A writer lock have been acquired on this page */
	lhpage *pPage = pCell->pPage;
	unsigned char *zRaw = pPage->pRaw->zData;
	/* Seek to the desired location */
	zRaw += pCell->iStart + L_HASH_CELL_SZ;
	/* Write the key */
	SyMemcpy(pKey,(void *)zRaw,nKeylen);
	zRaw += nKeylen;
	if( nDatalen > 0 ){
		/* Write the Data */
		SyMemcpy(pData,(void *)zRaw,(sxu32)nDatalen);
	}
	return VEDIS_OK;
}
/*
 * Allocate as much overflow page we need to store the cell payload.
 */
static int lhCellWriteOvflPayload(lhcell *pCell,const void *pKey,sxu32 nKeylen,...)
{
	lhpage *pPage = pCell->pPage;
	lhash_kv_engine *pEngine = pPage->pHash;
	vedis_page *pOvfl,*pFirst,*pNew;
	const unsigned char *zPtr,*zEnd;
	unsigned char *zRaw,*zRawEnd;
	sxu32 nAvail;
	va_list ap;
	int rc;
	/* Acquire a new overflow page */
	rc = lhAcquirePage(pEngine,&pOvfl);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Acquire a writer lock */
	rc = pEngine->pIo->xWrite(pOvfl);
	if( rc != VEDIS_OK ){
		return rc;
	}
	pFirst = pOvfl;
	/* Link */
	pCell->iOvfl = pOvfl->pgno;
	/* Update the cell header */
	SyBigEndianPack64(&pPage->pRaw->zData[pCell->iStart + 4/*Hash*/ + 4/*Key*/ + 8/*Data*/ + 2 /*Next cell*/],pCell->iOvfl);
	/* Start the write process */
	zPtr = (const unsigned char *)pKey;
	zEnd = &zPtr[nKeylen];
	SyBigEndianPack64(pOvfl->zData,0); /* Next overflow page on the chain */
	zRaw = &pOvfl->zData[8/* Next ovfl page*/ + 8 /* Data page */ + 2 /* Data offset*/];
	zRawEnd = &pOvfl->zData[pEngine->iPageSize];
	pNew = pOvfl;
	/* Write the key */
	for(;;){
		if( zPtr >= zEnd ){
			break;
		}
		if( zRaw >= zRawEnd ){
			/* Acquire a new page */
			rc = lhAcquirePage(pEngine,&pNew);
			if( rc != VEDIS_OK ){
				return rc;
			}
			rc = pEngine->pIo->xWrite(pNew);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Link */
			SyBigEndianPack64(pOvfl->zData,pNew->pgno);
			pEngine->pIo->xPageUnref(pOvfl);
			SyBigEndianPack64(pNew->zData,0); /* Next overflow page on the chain */
			pOvfl = pNew;
			zRaw = &pNew->zData[8];
			zRawEnd = &pNew->zData[pEngine->iPageSize];
		}
		nAvail = (sxu32)(zRawEnd-zRaw);
		nKeylen = (sxu32)(zEnd-zPtr);
		if( nKeylen > nAvail ){
			nKeylen = nAvail;
		}
		SyMemcpy((const void *)zPtr,(void *)zRaw,nKeylen);
		/* Synchronize pointers */
		zPtr += nKeylen;
		zRaw += nKeylen;
	}
	rc = VEDIS_OK;
	va_start(ap,nKeylen);
	pCell->iDataPage = pNew->pgno;
	pCell->iDataOfft = (sxu16)(zRaw-pNew->zData);
	/* Write the data page and its offset */
	SyBigEndianPack64(&pFirst->zData[8/*Next ovfl*/],pCell->iDataPage);
	SyBigEndianPack16(&pFirst->zData[8/*Next ovfl*/+8/*Data page*/],pCell->iDataOfft);
	/* Write data */
	for(;;){
		const void *pData;
		sxu32 nDatalen;
		sxu64 nData;
		pData = va_arg(ap,const void *);
		nData = va_arg(ap,sxu64);
		if( pData == 0 ){
			/* No more chunks */
			break;
		}
		/* Write this chunk */
		zPtr = (const unsigned char *)pData;
		zEnd = &zPtr[nData];
		for(;;){
			if( zPtr >= zEnd ){
				break;
			}
			if( zRaw >= zRawEnd ){
				/* Acquire a new page */
				rc = lhAcquirePage(pEngine,&pNew);
				if( rc != VEDIS_OK ){
					va_end(ap);
					return rc;
				}
				rc = pEngine->pIo->xWrite(pNew);
				if( rc != VEDIS_OK ){
					va_end(ap);
					return rc;
				}
				/* Link */
				SyBigEndianPack64(pOvfl->zData,pNew->pgno);
				pEngine->pIo->xPageUnref(pOvfl);
				SyBigEndianPack64(pNew->zData,0); /* Next overflow page on the chain */
				pOvfl = pNew;
				zRaw = &pNew->zData[8];
				zRawEnd = &pNew->zData[pEngine->iPageSize];
			}
			nAvail = (sxu32)(zRawEnd-zRaw);
			nDatalen = (sxu32)(zEnd-zPtr);
			if( nDatalen > nAvail ){
				nDatalen = nAvail;
			}
			SyMemcpy((const void *)zPtr,(void *)zRaw,nDatalen);
			/* Synchronize pointers */
			zPtr += nDatalen;
			zRaw += nDatalen;
		}
	}
	/* Unref the overflow page */
	pEngine->pIo->xPageUnref(pOvfl);
	va_end(ap);
	return VEDIS_OK;
}
/*
 * Restore a page to the free list.
 */
static int lhRestorePage(lhash_kv_engine *pEngine,vedis_page *pPage)
{
	int rc;
	rc = pEngine->pIo->xWrite(pEngine->pHeader);
	if( rc != VEDIS_OK ){
		return rc;
	}
	rc = pEngine->pIo->xWrite(pPage);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Link to the list of free page */
	SyBigEndianPack64(pPage->zData,pEngine->nFreeList);
	pEngine->nFreeList = pPage->pgno;
	SyBigEndianPack64(&pEngine->pHeader->zData[4/*Magic*/+4/*Hash*/],pEngine->nFreeList);
	/* All done */
	return VEDIS_OK;
}
/*
 * Restore cell space and mark it as a free block.
 */
static int lhRestoreSpace(lhpage *pPage,sxu16 iOfft,sxu16 nByte)
{
	unsigned char *zRaw;
	if( nByte < 4 ){
		/* At least 4 bytes of freespace must form a valid block */
		return VEDIS_OK;
	}
	/* pEngine->pIo->xWrite() has been successfully called on this page */
	zRaw = &pPage->pRaw->zData[iOfft];
	/* Mark as a free block */
	SyBigEndianPack16(zRaw,pPage->sHdr.iFree); /* Offset of the next free block */
	zRaw += 2;
	SyBigEndianPack16(zRaw,nByte);
	/* Link */
	SyBigEndianPack16(&pPage->pRaw->zData[2/* offset of the first cell */],iOfft);
	pPage->sHdr.iFree = iOfft;
	pPage->nFree += nByte;
	return VEDIS_OK;
}
/* Forward declaration */
static lhcell * lhFindSibeling(lhcell *pCell);
/*
 * Unlink a cell.
 */
static int lhUnlinkCell(lhcell *pCell)
{
	lhash_kv_engine *pEngine = pCell->pPage->pHash;
	lhpage *pPage = pCell->pPage;
	sxu16 nByte = L_HASH_CELL_SZ;
	lhcell *pPrev;
	int rc;
	rc = pEngine->pIo->xWrite(pPage->pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Bring the link */
	pPrev = lhFindSibeling(pCell);
	if( pPrev ){
		pPrev->iNext = pCell->iNext;
		/* Fix offsets in the page header */
		SyBigEndianPack16(&pPage->pRaw->zData[pPrev->iStart + 4/*Hash*/+4/*Key*/+8/*Data*/],pCell->iNext);
	}else{
		/* First entry on this page (either master or slave) */
		pPage->sHdr.iOfft = pCell->iNext;
		/* Update the page header */
		SyBigEndianPack16(pPage->pRaw->zData,pCell->iNext);
	}
	/* Restore cell space */
	if( pCell->iOvfl == 0 ){
		nByte += (sxu16)(pCell->nData + pCell->nKey);
	}
	lhRestoreSpace(pPage,pCell->iStart,nByte);
	/* Discard the cell from the in-memory hashtable */
	lhCellDiscard(pCell);
	return VEDIS_OK;
}
/*
 * Remove a cell and its paylod (key + data).
 */
static int lhRecordRemove(lhcell *pCell)
{
	lhash_kv_engine *pEngine = pCell->pPage->pHash;
	int rc;
	if( pCell->iOvfl > 0){
		/* Discard overflow pages */
		vedis_page *pOvfl;
		pgno iNext = pCell->iOvfl;
		for(;;){
			/* Point to the overflow page */
			rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,iNext,&pOvfl);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Next page on the chain */
			SyBigEndianUnpack64(pOvfl->zData,&iNext);
			/* Restore the page to the free list */
			rc = lhRestorePage(pEngine,pOvfl);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Unref */
			pEngine->pIo->xPageUnref(pOvfl);
			if( iNext == 0 ){
				break;
			}
		}
	}
	/* Unlink the cell */
	rc = lhUnlinkCell(pCell);
	return rc;
}
/*
 * Find cell sibeling.
 */
static lhcell * lhFindSibeling(lhcell *pCell)
{
	lhpage *pPage = pCell->pPage->pMaster;
	lhcell *pEntry;
	pEntry = pPage->pFirst; 
	while( pEntry ){
		if( pEntry->pPage == pCell->pPage && pEntry->iNext == pCell->iStart ){
			/* Sibeling found */
			return pEntry;
		}
		/* Point to the previous entry */
		pEntry = pEntry->pPrev; 
	}
	/* Last inserted cell */
	return 0;
}
/*
 * Move a cell to a new location with its new data.
 */
static int lhMoveLocalCell(
	lhcell *pCell,
	sxu16 iOfft,
	const void *pData,
	vedis_int64 nData
	)
{
	sxu16 iKeyOfft = pCell->iStart + L_HASH_CELL_SZ;
	lhpage *pPage = pCell->pPage;
	lhcell *pSibeling;
	pSibeling = lhFindSibeling(pCell);
	if( pSibeling ){
		/* Fix link */
		SyBigEndianPack16(&pPage->pRaw->zData[pSibeling->iStart + 4/*Hash*/+4/*Key*/+8/*Data*/],pCell->iNext);
		pSibeling->iNext = pCell->iNext;
	}else{
		/* First cell, update page header only */
		SyBigEndianPack16(pPage->pRaw->zData,pCell->iNext);
		pPage->sHdr.iOfft = pCell->iNext;
	}
	/* Set the new offset */
	pCell->iStart = iOfft;
	pCell->nData = (sxu64)nData;
	/* Write the cell payload */
	lhCellWriteLocalPayload(pCell,(const void *)&pPage->pRaw->zData[iKeyOfft],pCell->nKey,pData,nData);
	/* Finally write the cell header */
	lhCellWriteHeader(pCell);
	/* All done */
	return VEDIS_OK;
}
/*
 * Overwrite an existing record.
 */
static int lhRecordOverwrite(
	lhcell *pCell,
	const void *pData,vedis_int64 nByte
	)
{
	lhash_kv_engine *pEngine = pCell->pPage->pHash;
	unsigned char *zRaw,*zRawEnd,*zPayload;
	const unsigned char *zPtr,*zEnd;
	vedis_page *pOvfl,*pOld,*pNew;
	lhpage *pPage = pCell->pPage;
	sxu32 nAvail;
	pgno iOvfl;
	int rc;
	/* Acquire a writer lock on this page */
	rc = pEngine->pIo->xWrite(pPage->pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( pCell->iOvfl == 0 ){
		/* Local payload, try to deal with the free space issues */
		zPayload = &pPage->pRaw->zData[pCell->iStart + L_HASH_CELL_SZ + pCell->nKey];
		if( pCell->nData == (sxu64)nByte ){
			/* Best scenario, simply a memcpy operation */
			SyMemcpy(pData,(void *)zPayload,(sxu32)nByte);
		}else if( (sxu64)nByte < pCell->nData ){
			/* Shorter data, not so ugly */
			SyMemcpy(pData,(void *)zPayload,(sxu32)nByte);
			/* Update the cell header */
			SyBigEndianPack64(&pPage->pRaw->zData[pCell->iStart + 4 /* Hash */ + 4 /* Key */],nByte);
			/* Restore freespace */
			lhRestoreSpace(pPage,(sxu16)(pCell->iStart + L_HASH_CELL_SZ + pCell->nKey + nByte),(sxu16)(pCell->nData - nByte));
			/* New data size */
			pCell->nData = (sxu64)nByte;
		}else{
			sxu16 iOfft = 0; /* cc warning */
			/* Check if another chunk is available for this cell */
			rc = lhAllocateSpace(pPage,L_HASH_CELL_SZ + pCell->nKey + nByte,&iOfft);
			if( rc != VEDIS_OK ){
				/* Transfer the payload to an overflow page */
				rc = lhCellWriteOvflPayload(pCell,&pPage->pRaw->zData[pCell->iStart + L_HASH_CELL_SZ],pCell->nKey,pData,nByte,(const void *)0);
				if( rc != VEDIS_OK ){
					return rc;
				}
				/* Update the cell header */
				SyBigEndianPack64(&pPage->pRaw->zData[pCell->iStart + 4 /* Hash */ + 4 /* Key */],(sxu64)nByte);
				/* Restore freespace */
				lhRestoreSpace(pPage,(sxu16)(pCell->iStart + L_HASH_CELL_SZ),(sxu16)(pCell->nKey + pCell->nData));
				/* New data size */
				pCell->nData = (sxu64)nByte;
			}else{
				sxu16 iOldOfft = pCell->iStart;
				sxu32 iOld = (sxu32)pCell->nData;
				/* Space is available, transfer the cell */
				lhMoveLocalCell(pCell,iOfft,pData,nByte);
				/* Restore cell space */
				lhRestoreSpace(pPage,iOldOfft,(sxu16)(L_HASH_CELL_SZ + pCell->nKey + iOld));
			}
		}
		return VEDIS_OK;
	}
	/* Point to the overflow page */
	rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,pCell->iDataPage,&pOvfl);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Relase all old overflow pages first */
	SyBigEndianUnpack64(pOvfl->zData,&iOvfl);
	pOld = pOvfl;
	for(;;){
		if( iOvfl == 0 ){
			/* No more overflow pages on the chain */
			break;
		}
		/* Point to the target page */
		if( VEDIS_OK != pEngine->pIo->xGet(pEngine->pIo->pHandle,iOvfl,&pOld) ){
			/* Not so fatal if something goes wrong here */
			break;
		}
		/* Next overflow page to be released */
		SyBigEndianUnpack64(pOld->zData,&iOvfl);
		if( pOld != pOvfl ){ /* xx: chm is maniac */
			/* Restore the page to the free list */
			lhRestorePage(pEngine,pOld);
			/* Unref */
			pEngine->pIo->xPageUnref(pOld);
		}
	}
	/* Point to the data offset */
	zRaw = &pOvfl->zData[pCell->iDataOfft];
	zRawEnd = &pOvfl->zData[pEngine->iPageSize];
	/* The data to be stored */
	zPtr = (const unsigned char *)pData;
	zEnd = &zPtr[nByte];
	/* Start the overwrite process */
	/* Acquire a writer lock */
	rc = pEngine->pIo->xWrite(pOvfl);
	if( rc != VEDIS_OK ){
		return rc;
	}
	SyBigEndianPack64(pOvfl->zData,0);
	for(;;){
		sxu32 nLen;
		if( zPtr >= zEnd ){
			break;
		}
		if( zRaw >= zRawEnd ){
			/* Acquire a new page */
			rc = lhAcquirePage(pEngine,&pNew);
			if( rc != VEDIS_OK ){
				return rc;
			}
			rc = pEngine->pIo->xWrite(pNew);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Link */
			SyBigEndianPack64(pOvfl->zData,pNew->pgno);
			pEngine->pIo->xPageUnref(pOvfl);
			SyBigEndianPack64(pNew->zData,0); /* Next overflow page on the chain */
			pOvfl = pNew;
			zRaw = &pNew->zData[8];
			zRawEnd = &pNew->zData[pEngine->iPageSize];
		}
		nAvail = (sxu32)(zRawEnd-zRaw);
		nLen = (sxu32)(zEnd-zPtr);
		if( nLen > nAvail ){
			nLen = nAvail;
		}
		SyMemcpy((const void *)zPtr,(void *)zRaw,nLen);
		/* Synchronize pointers */
		zPtr += nLen;
		zRaw += nLen;
	}
	/* Unref the last overflow page */
	pEngine->pIo->xPageUnref(pOvfl);
	/* Finally, update the cell header */
	pCell->nData = (sxu64)nByte;
	SyBigEndianPack64(&pPage->pRaw->zData[pCell->iStart + 4 /* Hash */ + 4 /* Key */],pCell->nData);
	/* All done */
	return VEDIS_OK;
}
/*
 * Append data to an existing record.
 */
static int lhRecordAppend(
	lhcell *pCell,
	const void *pData,vedis_int64 nByte
	)
{
	lhash_kv_engine *pEngine = pCell->pPage->pHash;
	const unsigned char *zPtr,*zEnd;
	lhpage *pPage = pCell->pPage;
	unsigned char *zRaw,*zRawEnd;
	vedis_page *pOvfl,*pNew;
	sxu64 nDatalen;
	sxu32 nAvail;
	pgno iOvfl;
	int rc;
	if( pCell->nData + nByte < pCell->nData ){
		/* Overflow */
		pEngine->pIo->xErr(pEngine->pIo->pHandle,"Append operation will cause data overflow");
		return VEDIS_LIMIT;
	}
	/* Acquire a writer lock on this page */
	rc = pEngine->pIo->xWrite(pPage->pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( pCell->iOvfl == 0 ){
		sxu16 iOfft = 0; /* cc warning */
		/* Local payload, check for a bigger place */
		rc = lhAllocateSpace(pPage,L_HASH_CELL_SZ + pCell->nKey + pCell->nData + nByte,&iOfft);
		if( rc != VEDIS_OK ){
			/* Transfer the payload to an overflow page */
			rc = lhCellWriteOvflPayload(pCell,
				&pPage->pRaw->zData[pCell->iStart + L_HASH_CELL_SZ],pCell->nKey,
				(const void *)&pPage->pRaw->zData[pCell->iStart + L_HASH_CELL_SZ + pCell->nKey],pCell->nData,
				pData,nByte,
				(const void *)0
				);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Update the cell header */
			SyBigEndianPack64(&pPage->pRaw->zData[pCell->iStart + 4 /* Hash */ + 4 /* Key */],pCell->nData + nByte);
			/* Restore freespace */
			lhRestoreSpace(pPage,(sxu16)(pCell->iStart + L_HASH_CELL_SZ),(sxu16)(pCell->nKey + pCell->nData));
			/* New data size */
			pCell->nData += nByte;
		}else{
			sxu16 iOldOfft = pCell->iStart;
			sxu32 iOld = (sxu32)pCell->nData;
			SyBlob sWorker;
			SyBlobInit(&sWorker,&pEngine->sAllocator);
			/* Copy the old data */
			rc = SyBlobAppend(&sWorker,(const void *)&pPage->pRaw->zData[pCell->iStart + L_HASH_CELL_SZ + pCell->nKey],(sxu32)pCell->nData);
			if( rc == SXRET_OK ){
				/* Append the new data */
				rc = SyBlobAppend(&sWorker,pData,(sxu32)nByte);
			}
			if( rc != VEDIS_OK ){
				SyBlobRelease(&sWorker);
				return rc;
			}
			/* Space is available, transfer the cell */
			lhMoveLocalCell(pCell,iOfft,SyBlobData(&sWorker),(vedis_int64)SyBlobLength(&sWorker));
			/* Restore cell space */
			lhRestoreSpace(pPage,iOldOfft,(sxu16)(L_HASH_CELL_SZ + pCell->nKey + iOld));
			/* All done */
			SyBlobRelease(&sWorker);
		}
		return VEDIS_OK;
	}
	/* Point to the overflow page which hold the data */
	rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,pCell->iDataPage,&pOvfl);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Next overflow page in the chain */
	SyBigEndianUnpack64(pOvfl->zData,&iOvfl);
	/* Point to the end of the chunk */
	zRaw = &pOvfl->zData[pCell->iDataOfft];
	zRawEnd = &pOvfl->zData[pEngine->iPageSize];
	nDatalen = pCell->nData;
	nAvail = (sxu32)(zRawEnd - zRaw);
	for(;;){
		if( zRaw >= zRawEnd ){
			if( iOvfl == 0 ){
				/* Cant happen */
				pEngine->pIo->xErr(pEngine->pIo->pHandle,"Corrupt overflow page");
				return VEDIS_CORRUPT;
			}
			rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,iOvfl,&pNew);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Next overflow page on the chain */
			SyBigEndianUnpack64(pNew->zData,&iOvfl);
			/* Unref the previous overflow page */
			pEngine->pIo->xPageUnref(pOvfl);
			/* Point to the new chunk */
			zRaw = &pNew->zData[8];
			zRawEnd = &pNew->zData[pCell->pPage->pHash->iPageSize];
			nAvail = L_HASH_OVERFLOW_SIZE(pCell->pPage->pHash->iPageSize);
			pOvfl = pNew;
		}
		if( (sxu64)nAvail > nDatalen ){
			zRaw += nDatalen;
			break;
		}else{
			nDatalen -= nAvail;
		}
		zRaw += nAvail;
	}
	/* Start the append process */
	zPtr = (const unsigned char *)pData;
	zEnd = &zPtr[nByte];
	/* Acquire a writer lock */
	rc = pEngine->pIo->xWrite(pOvfl);
	if( rc != VEDIS_OK ){
		return rc;
	}
	for(;;){
		sxu32 nLen;
		if( zPtr >= zEnd ){
			break;
		}
		if( zRaw >= zRawEnd ){
			/* Acquire a new page */
			rc = lhAcquirePage(pEngine,&pNew);
			if( rc != VEDIS_OK ){
				return rc;
			}
			rc = pEngine->pIo->xWrite(pNew);
			if( rc != VEDIS_OK ){
				return rc;
			}
			/* Link */
			SyBigEndianPack64(pOvfl->zData,pNew->pgno);
			pEngine->pIo->xPageUnref(pOvfl);
			SyBigEndianPack64(pNew->zData,0); /* Next overflow page on the chain */
			pOvfl = pNew;
			zRaw = &pNew->zData[8];
			zRawEnd = &pNew->zData[pEngine->iPageSize];
		}
		nAvail = (sxu32)(zRawEnd-zRaw);
		nLen = (sxu32)(zEnd-zPtr);
		if( nLen > nAvail ){
			nLen = nAvail;
		}
		SyMemcpy((const void *)zPtr,(void *)zRaw,nLen);
		/* Synchronize pointers */
		zPtr += nLen;
		zRaw += nLen;
	}
	/* Unref the last overflow page */
	pEngine->pIo->xPageUnref(pOvfl);
	/* Finally, update the cell header */
	pCell->nData += nByte;
	SyBigEndianPack64(&pPage->pRaw->zData[pCell->iStart + 4 /* Hash */ + 4 /* Key */],pCell->nData);
	/* All done */
	return VEDIS_OK;
}
/*
 * A write privilege have been acquired on this page.
 * Mark it as an empty page (No cells).
 */
static int lhSetEmptyPage(lhpage *pPage)
{
	unsigned char *zRaw = pPage->pRaw->zData;
	lhphdr *pHeader = &pPage->sHdr;
	sxu16 nByte;
	int rc;
	/* Acquire a writer lock */
	rc = pPage->pHash->pIo->xWrite(pPage->pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Offset of the first cell */
	SyBigEndianPack16(zRaw,0);
	zRaw += 2;
	/* Offset of the first free block */
	pHeader->iFree = L_HASH_PAGE_HDR_SZ;
	SyBigEndianPack16(zRaw,L_HASH_PAGE_HDR_SZ);
	zRaw += 2;
	/* Slave page number */
	SyBigEndianPack64(zRaw,0);
	zRaw += 8;
	/* Fill the free block */
	SyBigEndianPack16(zRaw,0); /* Offset of the next free block */
	zRaw += 2;
	nByte = (sxu16)L_HASH_MX_FREE_SPACE(pPage->pHash->iPageSize);
	SyBigEndianPack16(zRaw,nByte);
	pPage->nFree = nByte;
	/* Do not add this page to the hot dirty list */
	pPage->pHash->pIo->xDontMkHot(pPage->pRaw);
	return VEDIS_OK;
}
/* Forward declaration */
static int lhSlaveStore(
	lhpage *pPage,
	const void *pKey,sxu32 nKeyLen,
	const void *pData,vedis_int64 nDataLen,
	sxu32 nHash
	);
/*
 * Store a cell and its payload in a given page.
 */
static int lhStoreCell(
	lhpage *pPage, /* Target page */
	const void *pKey,sxu32 nKeyLen, /* Payload: Key */
	const void *pData,vedis_int64 nDataLen, /* Payload: Data */
	sxu32 nHash, /* Hash of the key */
	int auto_append /* Auto append a slave page if full */
	)
{
	lhash_kv_engine *pEngine = pPage->pHash;
	int iNeedOvfl = 0; /* Need overflow page for this cell and its payload*/
	lhcell *pCell;
	sxu16 nOfft;
	int rc;
	/* Acquire a writer lock on this page first */
	rc = pEngine->pIo->xWrite(pPage->pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Check for a free block  */
	rc = lhAllocateSpace(pPage,L_HASH_CELL_SZ+nKeyLen+nDataLen,&nOfft);
	if( rc != VEDIS_OK ){
		/* Check for a free block to hold a single cell only (without payload) */
		rc = lhAllocateSpace(pPage,L_HASH_CELL_SZ,&nOfft);
		if( rc != VEDIS_OK ){
			if( !auto_append ){
				/* A split must be done */
				return VEDIS_FULL;
			}else{
				/* Store this record in a slave page */
				rc = lhSlaveStore(pPage,pKey,nKeyLen,pData,nDataLen,nHash);
				return rc;
			}
		}
		iNeedOvfl = 1;
	}
	/* Allocate a new cell instance */
	pCell = lhNewCell(pEngine,pPage);
	if( pCell == 0 ){
		pEngine->pIo->xErr(pEngine->pIo->pHandle,"KV store is running out of memory");
		return VEDIS_NOMEM;
	}
	/* Fill-in the structure */
	pCell->iStart = nOfft;
	pCell->nKey = nKeyLen;
	pCell->nData = (sxu64)nDataLen;
	pCell->nHash = nHash;
	if( nKeyLen < 262144 /* 256 KB */ ){
		/* Keep the key in-memory for fast lookup */
		SyBlobAppend(&pCell->sKey,pKey,nKeyLen);
	}
	/* Link the cell */
	rc = lhInstallCell(pCell);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Write the payload */
	if( iNeedOvfl ){
		rc = lhCellWriteOvflPayload(pCell,pKey,nKeyLen,pData,nDataLen,(const void *)0);
		if( rc != VEDIS_OK ){
			lhCellDiscard(pCell);
			return rc;
		}
	}else{
		lhCellWriteLocalPayload(pCell,pKey,nKeyLen,pData,nDataLen);
	}
	/* Finally, Write the cell header */
	lhCellWriteHeader(pCell);
	/* All done */
	return VEDIS_OK;
}
/*
 * Find a slave page capable of hosting the given amount.
 */
static int lhFindSlavePage(lhpage *pPage,sxu64 nAmount,sxu16 *pOfft,lhpage **ppSlave)
{
	lhash_kv_engine *pEngine = pPage->pHash;
	lhpage *pMaster = pPage->pMaster;
	lhpage *pSlave = pMaster->pSlave;
	vedis_page *pRaw;
	lhpage *pNew;
	sxu16 iOfft;
	sxi32 i;
	int rc;
	/* Look for an already attached slave page */
	for( i = 0 ; i < pMaster->iSlave ; ++i ){
		/* Find a free chunk big enough */
		rc = lhAllocateSpace(pSlave,L_HASH_CELL_SZ+nAmount,&iOfft);
		if( rc != VEDIS_OK ){
			/* A space for cell header only */
			rc = lhAllocateSpace(pSlave,L_HASH_CELL_SZ,&iOfft);
		}
		if( rc == VEDIS_OK ){
			/* All done */
			if( pOfft ){
				*pOfft = iOfft;
			}
			*ppSlave = pSlave;
			return VEDIS_OK;
		}
		/* Point to the next slave page */
		pSlave = pSlave->pNextSlave;
	}
	/* Acquire a new slave page */
	rc = lhAcquirePage(pEngine,&pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Last slave page */
	pSlave = pMaster->pSlave;
	if( pSlave == 0 ){
		/* First slave page */
		pSlave = pMaster;
	}
	/* Initialize the page */
	pNew = lhNewPage(pEngine,pRaw,pMaster);
	if( pNew == 0 ){
		return VEDIS_NOMEM;
	}
	/* Mark as an empty page */
	rc = lhSetEmptyPage(pNew);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	if( pOfft ){
		/* Look for a free block */
		if( VEDIS_OK != lhAllocateSpace(pNew,L_HASH_CELL_SZ+nAmount,&iOfft) ){
			/* Cell header only */
			lhAllocateSpace(pNew,L_HASH_CELL_SZ,&iOfft); /* Never fail */
		}	
		*pOfft = iOfft;
	}
	/* Link this page to the previous slave page */
	rc = pEngine->pIo->xWrite(pSlave->pRaw);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	/* Reflect in the page header */
	SyBigEndianPack64(&pSlave->pRaw->zData[2/*Cell offset*/+2/*Free block offset*/],pRaw->pgno);
	pSlave->sHdr.iSlave = pRaw->pgno;
	/* All done */
	*ppSlave = pNew;
	return VEDIS_OK;
fail:
	pEngine->pIo->xPageUnref(pNew->pRaw); /* pNew will be released in this call */
	return rc;

}
/*
 * Perform a store operation in a slave page.
 */
static int lhSlaveStore(
	lhpage *pPage, /* Master page */
	const void *pKey,sxu32 nKeyLen, /* Payload: key */
	const void *pData,vedis_int64 nDataLen, /* Payload: data */
	sxu32 nHash /* Hash of the key */
	)
{
	lhpage *pSlave;
	int rc;
	/* Find a slave page */
	rc = lhFindSlavePage(pPage,nKeyLen + nDataLen,0,&pSlave);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Perform the insertion in the slave page */
	rc = lhStoreCell(pSlave,pKey,nKeyLen,pData,nDataLen,nHash,1);
	return rc;
}
/*
 * Transfer a cell to a new page (either a master or slave).
 */
static int lhTransferCell(lhcell *pTarget,lhpage *pPage)
{
	lhcell *pCell;
	sxu16 nOfft;
	int rc;
	/* Check for a free block to hold a single cell only */
	rc = lhAllocateSpace(pPage,L_HASH_CELL_SZ,&nOfft);
	if( rc != VEDIS_OK ){
		/* Store in a slave page */
		rc = lhFindSlavePage(pPage,L_HASH_CELL_SZ,&nOfft,&pPage);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}
	/* Allocate a new cell instance */
	pCell = lhNewCell(pPage->pHash,pPage);
	if( pCell == 0 ){
		return VEDIS_NOMEM;
	}
	/* Fill-in the structure */
	pCell->iStart = nOfft;
	pCell->nData  = pTarget->nData;
	pCell->nKey   = pTarget->nKey;
	pCell->iOvfl  = pTarget->iOvfl;
	pCell->iDataOfft = pTarget->iDataOfft;
	pCell->iDataPage = pTarget->iDataPage;
	pCell->nHash = pTarget->nHash;
	SyBlobDup(&pTarget->sKey,&pCell->sKey);
	/* Link the cell */
	rc = lhInstallCell(pCell);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Finally, Write the cell header */
	lhCellWriteHeader(pCell);
	/* All done */
	return VEDIS_OK;
}
/*
 * Perform a page split.
 */
static int lhPageSplit(
	lhpage *pOld,      /* Page to be split */
	lhpage *pNew,      /* New page */
	pgno split_bucket, /* Current split bucket */
	pgno high_mask     /* High mask (Max split bucket - 1) */
	)
{
	lhcell *pCell,*pNext;
	SyBlob sWorker;
	pgno iBucket;
	int rc; 
	SyBlobInit(&sWorker,&pOld->pHash->sAllocator);
	/* Perform the split */
	pCell = pOld->pList;
	for( ;; ){
		if( pCell == 0 ){
			/* No more cells */
			break;
		}
		/* Obtain the new logical bucket */
		iBucket = pCell->nHash & high_mask;
		pNext =  pCell->pNext;
		if( iBucket != split_bucket){
			rc = VEDIS_OK;
			if( pCell->iOvfl ){
				/* Transfer the cell only */
				rc = lhTransferCell(pCell,pNew);
			}else{
				/* Transfer the cell and its payload */
				SyBlobReset(&sWorker);
				if( SyBlobLength(&pCell->sKey) < 1 ){
					/* Consume the key */
					rc = lhConsumeCellkey(pCell,vedisDataConsumer,&pCell->sKey,0);
					if( rc != VEDIS_OK ){
						goto fail;
					}
				}
				/* Consume the data (Very small data < 65k) */
				rc = lhConsumeCellData(pCell,vedisDataConsumer,&sWorker);
				if( rc != VEDIS_OK ){
					goto fail;
				}
				/* Perform the transfer */
				rc = lhStoreCell(
					pNew,
					SyBlobData(&pCell->sKey),(int)SyBlobLength(&pCell->sKey),
					SyBlobData(&sWorker),SyBlobLength(&sWorker),
					pCell->nHash,
					1
					);
			}
			if( rc != VEDIS_OK ){
				goto fail;
			}
			/* Discard the cell from the old page */
			lhUnlinkCell(pCell);
		}
		/* Point to the next cell */
		pCell = pNext;
	}
	/* All done */
	rc = VEDIS_OK;
fail:
	SyBlobRelease(&sWorker);
	return rc;
}
/*
 * Perform the infamous linear hash split operation.
 */
static int lhSplit(lhpage *pTarget,int *pRetry)
{
	lhash_kv_engine *pEngine = pTarget->pHash;
	lhash_bmap_rec *pRec;
	lhpage *pOld,*pNew;
	vedis_page *pRaw;
	int rc;
	/* Get the real page number of the bucket to split */
	pRec = lhMapFindBucket(pEngine,pEngine->split_bucket);
	if( pRec == 0 ){
		/* Can't happen */
		return VEDIS_CORRUPT;
	}
	/* Load the page to be split */
	rc = lhLoadPage(pEngine,pRec->iReal,0,&pOld,0);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Request a new page */
	rc = lhAcquirePage(pEngine,&pRaw);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Initialize the page */
	pNew = lhNewPage(pEngine,pRaw,0);
	if( pNew == 0 ){
		return VEDIS_NOMEM;
	}
	/* Mark as an empty page */
	rc = lhSetEmptyPage(pNew);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	/* Install and write the logical map record */
	rc = lhMapWriteRecord(pEngine,
		pEngine->split_bucket + pEngine->max_split_bucket,
		pRaw->pgno
		);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	if( pTarget->pRaw->pgno == pOld->pRaw->pgno ){
		*pRetry = 1;
	}
	/* Perform the split */
	rc = lhPageSplit(pOld,pNew,pEngine->split_bucket,pEngine->nmax_split_nucket - 1);
	if( rc != VEDIS_OK ){
		goto fail;
	}
	/* Update the database header */
	pEngine->split_bucket++;
	/* Acquire a writer lock on the first page */
	rc = pEngine->pIo->xWrite(pEngine->pHeader);
	if( rc != VEDIS_OK ){
		return rc;
	}
	if( pEngine->split_bucket >= pEngine->max_split_bucket ){
		/* Increment the generation number */
		pEngine->split_bucket = 0;
		pEngine->max_split_bucket = pEngine->nmax_split_nucket;
		pEngine->nmax_split_nucket <<= 1;
		if( !pEngine->nmax_split_nucket ){
			/* If this happen to your installation, please tell us <chm@symisc.net> */
			pEngine->pIo->xErr(pEngine->pIo->pHandle,"Database page (64-bit integer) limit reached");
			return VEDIS_LIMIT;
		}
		/* Reflect in the page header */
		SyBigEndianPack64(&pEngine->pHeader->zData[4/*Magic*/+4/*Hash*/+8/*Free list*/],pEngine->split_bucket);
		SyBigEndianPack64(&pEngine->pHeader->zData[4/*Magic*/+4/*Hash*/+8/*Free list*/+8/*Split bucket*/],pEngine->max_split_bucket);
	}else{
		/* Modify only the split bucket */
		SyBigEndianPack64(&pEngine->pHeader->zData[4/*Magic*/+4/*Hash*/+8/*Free list*/],pEngine->split_bucket);
	}
	/* All done */
	return VEDIS_OK;
fail:
	pEngine->pIo->xPageUnref(pNew->pRaw);
	return rc;
}
/*
 * Store a record in the target page.
 */
static int lhRecordInstall(
	  lhpage *pPage, /* Target page */
	  sxu32 nHash,   /* Hash of the key */
	  const void *pKey,sxu32 nKeyLen,          /* Payload: Key */
	  const void *pData,vedis_int64 nDataLen /* Payload: Data */
	  )
{
	int rc;
	rc = lhStoreCell(pPage,pKey,nKeyLen,pData,nDataLen,nHash,0);
	if( rc == VEDIS_FULL ){
		int do_retry = 0;
		/* Split */
		rc = lhSplit(pPage,&do_retry);
		if( rc == VEDIS_OK ){
			if( do_retry ){
				/* Re-calculate logical bucket number */
				return SXERR_RETRY;
			}
			/* Perform the store */
			rc = lhStoreCell(pPage,pKey,nKeyLen,pData,nDataLen,nHash,1);
		}
	}
	return rc;
}
/*
 * Insert a record (Either overwrite or append operation) in our database.
 */
static int lh_record_insert(
	  vedis_kv_engine *pKv,         /* KV store */
	  const void *pKey,sxu32 nKeyLen, /* Payload: Key */
	  const void *pData,vedis_int64 nDataLen, /* Payload: data */
	  int is_append /* True for an append operation */
	  )
{
	lhash_kv_engine *pEngine = (lhash_kv_engine *)pKv;
	lhash_bmap_rec *pRec;
	vedis_page *pRaw;
	lhpage *pPage;
	lhcell *pCell;
	pgno iBucket;
	sxu32 nHash;
	int iCnt;
	int rc;

	/* Acquire the first page (DB hash Header) so that everything gets loaded autmatically */
	rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,1,0);
	if( rc != VEDIS_OK ){
		return rc;
	}
	iCnt = 0;
	/* Compute the hash of the key first */
	nHash = pEngine->xHash(pKey,(sxu32)nKeyLen);
retry:
	/* Extract the logical bucket number */
	iBucket = nHash & (pEngine->nmax_split_nucket - 1);
	if( iBucket >= pEngine->split_bucket + pEngine->max_split_bucket ){
		/* Low mask */
		iBucket = nHash & (pEngine->max_split_bucket - 1);
	}
	/* Map the logical bucket number to real page number */
	pRec = lhMapFindBucket(pEngine,iBucket);
	if( pRec == 0 ){
		/* Request a new page */
		rc = lhAcquirePage(pEngine,&pRaw);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Initialize the page */
		pPage = lhNewPage(pEngine,pRaw,0);
		if( pPage == 0 ){
			return VEDIS_NOMEM;
		}
		/* Mark as an empty page */
		rc = lhSetEmptyPage(pPage);
		if( rc != VEDIS_OK ){
			pEngine->pIo->xPageUnref(pRaw); /* pPage will be released during this call */
			return rc;
		}
		/* Store the cell */
		rc = lhStoreCell(pPage,pKey,nKeyLen,pData,nDataLen,nHash,1);
		if( rc == VEDIS_OK ){
			/* Install and write the logical map record */
			rc = lhMapWriteRecord(pEngine,iBucket,pRaw->pgno);
		}
		pEngine->pIo->xPageUnref(pRaw);
		return rc;
	}else{
		/* Load the page */
		rc = lhLoadPage(pEngine,pRec->iReal,0,&pPage,0);
		if( rc != VEDIS_OK ){
			/* IO error, unlikely scenario */
			return rc;
		}
		/* Do not add this page to the hot dirty list */
		pEngine->pIo->xDontMkHot(pPage->pRaw);
		/* Lookup for the cell */
		pCell = lhFindCell(pPage,pKey,(sxu32)nKeyLen,nHash);
		if( pCell == 0 ){
			/* Create the record */
			rc = lhRecordInstall(pPage,nHash,pKey,nKeyLen,pData,nDataLen);
			if( rc == SXERR_RETRY && iCnt++ < 2 ){
				rc = VEDIS_OK;
				goto retry;
			}
		}else{
			if( is_append ){
				/* Append operation */
				rc = lhRecordAppend(pCell,pData,nDataLen);
			}else{
				/* Overwrite old value */
				rc = lhRecordOverwrite(pCell,pData,nDataLen);
			}
		}
		pEngine->pIo->xPageUnref(pPage->pRaw);
	}
	return rc;
}
/*
 * Replace method.
 */
static int lhash_kv_replace(
	  vedis_kv_engine *pKv,
	  const void *pKey,int nKeyLen,
	  const void *pData,vedis_int64 nDataLen
	  )
{
	int rc;
	rc = lh_record_insert(pKv,pKey,(sxu32)nKeyLen,pData,nDataLen,0);
	return rc;
}
/*
 * Append method.
 */
static int lhash_kv_append(
	  vedis_kv_engine *pKv,
	  const void *pKey,int nKeyLen,
	  const void *pData,vedis_int64 nDataLen
	  )
{
	int rc;
	rc = lh_record_insert(pKv,pKey,(sxu32)nKeyLen,pData,nDataLen,1);
	return rc;
}
/*
 * Write the hash header (Page one).
 */
static int lhash_write_header(lhash_kv_engine *pEngine,vedis_page *pHeader)
{
	unsigned char *zRaw = pHeader->zData;
	lhash_bmap_page *pMap;

	pEngine->pHeader = pHeader;
	/* 4 byte magic number */
	SyBigEndianPack32(zRaw,pEngine->nMagic);
	zRaw += 4;
	/* 4 byte hash value to identify a valid hash function */
	SyBigEndianPack32(zRaw,pEngine->xHash(L_HASH_WORD,sizeof(L_HASH_WORD)-1));
	zRaw += 4;
	/* List of free pages: Empty */
	SyBigEndianPack64(zRaw,0);
	zRaw += 8;
	/* Current split bucket */
	SyBigEndianPack64(zRaw,pEngine->split_bucket);
	zRaw += 8;
	/* Maximum split bucket */
	SyBigEndianPack64(zRaw,pEngine->max_split_bucket);
	zRaw += 8;
	/* Initialiaze the bucket map */
	pMap = &pEngine->sPageMap;
	/* Fill in the structure */
	pMap->iNum = pHeader->pgno;
	/* Next page in the bucket map */
	SyBigEndianPack64(zRaw,0);
	zRaw += 8;
	/* Total number of records in the bucket map */
	SyBigEndianPack32(zRaw,0);
	zRaw += 4;
	pMap->iPtr = (sxu16)(zRaw - pHeader->zData);
	/* All done */
	return VEDIS_OK;
 }
/*
 * Exported: xOpen() method.
 */
static int lhash_kv_open(vedis_kv_engine *pEngine,pgno dbSize)
{
	lhash_kv_engine *pHash = (lhash_kv_engine *)pEngine;
	vedis_page *pHeader;
	int rc;
	if( dbSize < 1 ){
		/* A new database, create the header */
		rc = pEngine->pIo->xNew(pEngine->pIo->pHandle,&pHeader);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Acquire a writer lock */
		rc = pEngine->pIo->xWrite(pHeader);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Write the hash header */
		rc = lhash_write_header(pHash,pHeader);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}else{
		/* Acquire the page one of the database */
		rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,1,&pHeader);
		if( rc != VEDIS_OK ){
			return rc;
		}
		/* Read the database header */
		rc = lhash_read_header(pHash,pHeader);
		if( rc != VEDIS_OK ){
			return rc;
		}
	}
	return VEDIS_OK;
}
/*
 * Release a master or slave page. (xUnpin callback).
 */
static void lhash_page_release(void *pUserData)
{
	lhpage *pPage = (lhpage *)pUserData;
	lhash_kv_engine *pEngine = pPage->pHash;
	lhcell *pNext,*pCell = pPage->pList;
	vedis_page *pRaw = pPage->pRaw;
	sxu32 n;
	/* Drop in-memory cells */
	for( n = 0 ; n < pPage->nCell ; ++n ){
		pNext = pCell->pNext;
		SyBlobRelease(&pCell->sKey);
		/* Release the cell instance */
		SyMemBackendPoolFree(&pEngine->sAllocator,(void *)pCell);
		/* Point to the next entry */
		pCell = pNext;
	}
	if( pPage->apCell ){
		/* Release the cell table */
		SyMemBackendFree(&pEngine->sAllocator,(void *)pPage->apCell);
	}
	/* Finally, release the whole page */
	SyMemBackendPoolFree(&pEngine->sAllocator,pPage);
	pRaw->pUserData = 0;
}
/*
 * Default hash function (DJB).
 */
static sxu32 lhash_bin_hash(const void *pSrc,sxu32 nLen)
{
	register unsigned char *zIn = (unsigned char *)pSrc;
	unsigned char *zEnd;
	sxu32 nH = 5381;
	if( nLen > 2048 /* 2K */ ){
		nLen = 2048;
	}
	zEnd = &zIn[nLen];
	for(;;){
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
	}	
	return nH;
}
/*
 * Exported: xInit() method.
 * Initialize the Key value storage engine.
 */
static int lhash_kv_init(vedis_kv_engine *pEngine,int iPageSize)
{
	lhash_kv_engine *pHash = (lhash_kv_engine *)pEngine;
	int rc;

	/* This structure is always zeroed, go to the initialization directly */
	SyMemBackendInitFromParent(&pHash->sAllocator,vedisExportMemBackend());
#if defined(VEDIS_ENABLE_THREADS)
	/* Already protected by the upper layers */
	SyMemBackendDisbaleMutexing(&pHash->sAllocator);
#endif
	pHash->iPageSize = iPageSize;
	/* Default hash function */
	pHash->xHash = lhash_bin_hash;
	/* Default comparison function */
	pHash->xCmp = SyMemcmp;
	/* Allocate a new record map */
	pHash->nBuckSize = 32;
	pHash->apMap = (lhash_bmap_rec **)SyMemBackendAlloc(&pHash->sAllocator,pHash->nBuckSize *sizeof(lhash_bmap_rec *));
	if( pHash->apMap == 0 ){
		rc = VEDIS_NOMEM;
		goto err;
	}
	/* Zero the table */
	SyZero(pHash->apMap,pHash->nBuckSize * sizeof(lhash_bmap_rec *));
	/* Linear hashing components */
	pHash->split_bucket = 0; /* Logical not real bucket number */
	pHash->max_split_bucket = 1;
	pHash->nmax_split_nucket = 2;
	pHash->nMagic = L_HASH_MAGIC;
	/* Install the cache unpin and reload callbacks */
	pHash->pIo->xSetUnpin(pHash->pIo->pHandle,lhash_page_release);
	pHash->pIo->xSetReload(pHash->pIo->pHandle,lhash_page_release);
	return VEDIS_OK;
err:
	SyMemBackendRelease(&pHash->sAllocator);
	return rc;
}
/*
 * Exported: xRelease() method.
 * Release the Key value storage engine.
 */
static void lhash_kv_release(vedis_kv_engine *pEngine)
{
	lhash_kv_engine *pHash = (lhash_kv_engine *)pEngine;
	/* Release the private memory backend */
	SyMemBackendRelease(&pHash->sAllocator);
}
/*
 *  Exported: xConfig() method.
 *  Configure the linear hash KV store.
 */
static int lhash_kv_config(vedis_kv_engine *pEngine,int op,va_list ap)
{
	lhash_kv_engine *pHash = (lhash_kv_engine *)pEngine;
	int rc = VEDIS_OK;
	switch(op){
	case VEDIS_KV_CONFIG_HASH_FUNC: {
		/* Default hash function */
		if( pHash->nBuckRec > 0 ){
			/* Locked operation */
			rc = VEDIS_LOCKED;
		}else{
			ProcHash xHash = va_arg(ap,ProcHash);
			if( xHash ){
				pHash->xHash = xHash;
			}
		}
		break;
									  }
	case VEDIS_KV_CONFIG_CMP_FUNC: {
		/* Default comparison function */
		ProcCmp xCmp = va_arg(ap,ProcCmp);
		if( xCmp ){
			pHash->xCmp  = xCmp;
		}
		break;
									 }
	default:
		/* Unknown OP */
		rc = VEDIS_UNKNOWN;
		break;
	}
	return rc;
}
/*
 * Each public cursor is identified by an instance of this structure.
 */
typedef struct lhash_kv_cursor lhash_kv_cursor;
struct lhash_kv_cursor
{
	vedis_kv_engine *pStore; /* Must be first */
	/* Private fields */
	int iState;           /* Current state of the cursor */
	int is_first;         /* True to read the database header */
	lhcell *pCell;        /* Current cell we are processing */
	vedis_page *pRaw;   /* Raw disk page */
	lhash_bmap_rec *pRec; /* Logical to real bucket map */
};
/* 
 * Possible state of the cursor
 */
#define L_HASH_CURSOR_STATE_NEXT_PAGE 1 /* Next page in the list */
#define L_HASH_CURSOR_STATE_CELL      2 /* Processing Cell */
#define L_HASH_CURSOR_STATE_DONE      3 /* Cursor does not point to anything */
/*
 * Initialize the cursor.
 */
static void lhInitCursor(vedis_kv_cursor *pPtr)
{
	 lhash_kv_engine *pEngine = (lhash_kv_engine *)pPtr->pStore;
	 lhash_kv_cursor *pCur = (lhash_kv_cursor *)pPtr;
	 /* Init */
	 pCur->iState = L_HASH_CURSOR_STATE_NEXT_PAGE;
	 pCur->pCell = 0;
	 pCur->pRec = pEngine->pFirst;
	 pCur->pRaw = 0;
	 pCur->is_first = 1;
}
/*
 * Point to the next page on the database.
 */
static int lhCursorNextPage(lhash_kv_cursor *pPtr)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pPtr;
	lhash_bmap_rec *pRec;
	lhpage *pPage;
	int rc;
	for(;;){
		pRec = pCur->pRec;
		if( pRec == 0 ){
			pCur->iState = L_HASH_CURSOR_STATE_DONE;
			return VEDIS_DONE;
		}
		if( pPtr->iState == L_HASH_CURSOR_STATE_CELL && pPtr->pRaw ){
			/* Unref this page */
			pCur->pStore->pIo->xPageUnref(pPtr->pRaw);
			pPtr->pRaw = 0;
		}
		/* Advance the map cursor */
		pCur->pRec = pRec->pPrev; /* Not a bug, reverse link */
		/* Load the next page on the list */
		rc = lhLoadPage((lhash_kv_engine *)pCur->pStore,pRec->iReal,0,&pPage,0);
		if( rc != VEDIS_OK ){
			return rc;
		}
		if( pPage->pList ){
			/* Reflect the change */
			pCur->pCell = pPage->pList;
			pCur->iState = L_HASH_CURSOR_STATE_CELL;
			pCur->pRaw = pPage->pRaw;
			break;
		}
		/* Empty page, discard this page and continue */
		pPage->pHash->pIo->xPageUnref(pPage->pRaw);
	}
	return VEDIS_OK;
}
/*
 * Point to the previous page on the database.
 */
static int lhCursorPrevPage(lhash_kv_cursor *pPtr)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pPtr;
	lhash_bmap_rec *pRec;
	lhpage *pPage;
	int rc;
	for(;;){
		pRec = pCur->pRec;
		if( pRec == 0 ){
			pCur->iState = L_HASH_CURSOR_STATE_DONE;
			return VEDIS_DONE;
		}
		if( pPtr->iState == L_HASH_CURSOR_STATE_CELL && pPtr->pRaw ){
			/* Unref this page */
			pCur->pStore->pIo->xPageUnref(pPtr->pRaw);
			pPtr->pRaw = 0;
		}
		/* Advance the map cursor */
		pCur->pRec = pRec->pNext; /* Not a bug, reverse link */
		/* Load the previous page on the list */
		rc = lhLoadPage((lhash_kv_engine *)pCur->pStore,pRec->iReal,0,&pPage,0);
		if( rc != VEDIS_OK ){
			return rc;
		}
		if( pPage->pFirst ){
			/* Reflect the change */
			pCur->pCell = pPage->pFirst;
			pCur->iState = L_HASH_CURSOR_STATE_CELL;
			pCur->pRaw = pPage->pRaw;
			break;
		}
		/* Discard this page and continue */
		pPage->pHash->pIo->xPageUnref(pPage->pRaw);
	}
	return VEDIS_OK;
}
/*
 * Is a valid cursor.
 */
static int lhCursorValid(vedis_kv_cursor *pPtr)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pPtr;
	return (pCur->iState == L_HASH_CURSOR_STATE_CELL) && pCur->pCell;
}
/*
 * Point to the first record.
 */
static int lhCursorFirst(vedis_kv_cursor *pCursor)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhash_kv_engine *pEngine = (lhash_kv_engine *)pCursor->pStore;
	int rc;
	if( pCur->is_first ){
		/* Read the database header first */
		rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,1,0);
		if( rc != VEDIS_OK ){
			return rc;
		}
		pCur->is_first = 0;
	}
	/* Point to the first map record */
	pCur->pRec = pEngine->pFirst;
	/* Load the cells */
	rc = lhCursorNextPage(pCur);
	return rc;
}
/*
 * Point to the last record.
 */
static int lhCursorLast(vedis_kv_cursor *pCursor)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhash_kv_engine *pEngine = (lhash_kv_engine *)pCursor->pStore;
	int rc;
	if( pCur->is_first ){
		/* Read the database header first */
		rc = pEngine->pIo->xGet(pEngine->pIo->pHandle,1,0);
		if( rc != VEDIS_OK ){
			return rc;
		}
		pCur->is_first = 0;
	}
	/* Point to the last map record */
	pCur->pRec = pEngine->pList;
	/* Load the cells */
	rc = lhCursorPrevPage(pCur);
	return rc;
}
/*
 * Reset the cursor.
 */
static void lhCursorReset(vedis_kv_cursor *pCursor)
{
	lhCursorFirst(pCursor);
}
/*
 * Point to the next record.
 */
static int lhCursorNext(vedis_kv_cursor *pCursor)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhcell *pCell;
	int rc;
	if( pCur->iState != L_HASH_CURSOR_STATE_CELL || pCur->pCell == 0 ){
		/* Load the cells of the next page  */
		rc = lhCursorNextPage(pCur);
		return rc;
	}
	pCell = pCur->pCell;
	pCur->pCell = pCell->pNext;
	if( pCur->pCell == 0 ){
		/* Load the cells of the next page  */
		rc = lhCursorNextPage(pCur);
		return rc;
	}
	return VEDIS_OK;
}
/*
 * Point to the previous record.
 */
static int lhCursorPrev(vedis_kv_cursor *pCursor)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhcell *pCell;
	int rc;
	if( pCur->iState != L_HASH_CURSOR_STATE_CELL || pCur->pCell == 0 ){
		/* Load the cells of the previous page  */
		rc = lhCursorPrevPage(pCur);
		return rc;
	}
	pCell = pCur->pCell;
	pCur->pCell = pCell->pPrev;
	if( pCur->pCell == 0 ){
		/* Load the cells of the previous page  */
		rc = lhCursorPrevPage(pCur);
		return rc;
	}
	return VEDIS_OK;
}
/*
 * Return key length.
 */
static int lhCursorKeyLength(vedis_kv_cursor *pCursor,int *pLen)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhcell *pCell;
	
	if( pCur->iState != L_HASH_CURSOR_STATE_CELL || pCur->pCell == 0 ){
		/* Invalid state */
		return VEDIS_INVALID;
	}
	/* Point to the target cell */
	pCell = pCur->pCell;
	/* Return key length */
	*pLen = (int)pCell->nKey;
	return VEDIS_OK;
}
/*
 * Return data length.
 */
static int lhCursorDataLength(vedis_kv_cursor *pCursor,vedis_int64 *pLen)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhcell *pCell;
	
	if( pCur->iState != L_HASH_CURSOR_STATE_CELL || pCur->pCell == 0 ){
		/* Invalid state */
		return VEDIS_INVALID;
	}
	/* Point to the target cell */
	pCell = pCur->pCell;
	/* Return data length */
	*pLen = (vedis_int64)pCell->nData;
	return VEDIS_OK;
}
/*
 * Consume the key.
 */
static int lhCursorKey(vedis_kv_cursor *pCursor,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhcell *pCell;
	int rc;
	if( pCur->iState != L_HASH_CURSOR_STATE_CELL || pCur->pCell == 0 ){
		/* Invalid state */
		return VEDIS_INVALID;
	}
	/* Point to the target cell */
	pCell = pCur->pCell;
	if( SyBlobLength(&pCell->sKey) > 0 ){
		/* Consume the key directly */
		rc = xConsumer(SyBlobData(&pCell->sKey),SyBlobLength(&pCell->sKey),pUserData);
	}else{
		/* Very large key */
		rc = lhConsumeCellkey(pCell,xConsumer,pUserData,0);
	}
	return rc;
}
/*
 * Consume the data.
 */
static int lhCursorData(vedis_kv_cursor *pCursor,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhcell *pCell;
	int rc;
	if( pCur->iState != L_HASH_CURSOR_STATE_CELL || pCur->pCell == 0 ){
		/* Invalid state */
		return VEDIS_INVALID;
	}
	/* Point to the target cell */
	pCell = pCur->pCell;
	/* Consume the data */
	rc = lhConsumeCellData(pCell,xConsumer,pUserData);
	return rc;
}
/*
 * Find a partiuclar record.
 */
static int lhCursorSeek(vedis_kv_cursor *pCursor,const void *pKey,int nByte,int iPos)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	int rc;
	/* Perform a lookup */
	rc = lhRecordLookup((lhash_kv_engine *)pCur->pStore,pKey,nByte,&pCur->pCell);
	if( rc != VEDIS_OK ){
		SXUNUSED(iPos);
		pCur->pCell = 0;
		pCur->iState = L_HASH_CURSOR_STATE_DONE;
		return rc;
	}
	pCur->iState = L_HASH_CURSOR_STATE_CELL;
	return VEDIS_OK;
}
/*
 * Remove a particular record.
 */
static int lhCursorDelete(vedis_kv_cursor *pCursor)
{
	lhash_kv_cursor *pCur = (lhash_kv_cursor *)pCursor;
	lhcell *pCell;
	int rc;
	if( pCur->iState != L_HASH_CURSOR_STATE_CELL || pCur->pCell == 0 ){
		/* Invalid state */
		return VEDIS_INVALID;
	}
	/* Point to the target cell  */
	pCell = pCur->pCell;
	/* Point to the next entry */
	pCur->pCell = pCell->pNext;
	/* Perform the deletion */
	rc = lhRecordRemove(pCell);
	return rc;
}
/*
 * Export the linear-hash storage engine.
 */
VEDIS_PRIVATE const vedis_kv_methods * vedisExportDiskKvStorage(void)
{
	static const vedis_kv_methods sDiskStore = {
		"hash",                     /* zName */
		sizeof(lhash_kv_engine),    /* szKv */
		sizeof(lhash_kv_cursor),    /* szCursor */
		1,                          /* iVersion */
		lhash_kv_init,              /* xInit */
		lhash_kv_release,           /* xRelease */
		lhash_kv_config,            /* xConfig */
		lhash_kv_open,              /* xOpen */
		lhash_kv_replace,           /* xReplace */
		lhash_kv_append,            /* xAppend */
		lhInitCursor,               /* xCursorInit */
		lhCursorSeek,               /* xSeek */
		lhCursorFirst,              /* xFirst */
		lhCursorLast,               /* xLast */
		lhCursorValid,              /* xValid */
		lhCursorNext,               /* xNext */
		lhCursorPrev,               /* xPrev */
		lhCursorDelete,             /* xDelete */
		lhCursorKeyLength,          /* xKeyLength */
		lhCursorKey,                /* xKey */
		lhCursorDataLength,         /* xDataLength */
		lhCursorData,               /* xData */
		lhCursorReset,              /* xReset */
		0                           /* xRelease */                        
	};
	return &sDiskStore;
}
/*
 * ----------------------------------------------------------
 * File: json.c
 * MD5: a4aef01e657e37d9ace4729b9205976c
 * ----------------------------------------------------------
 */
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
 /* $SymiscID: json.c v1.0 FreeBSD 2012-12-16 00:28 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* This file deals with JSON serialization, decoding and stuff like that. */
/*
 * Section: 
 *  JSON encoding/decoding routines.
 * Authors:
 *  Symisc Systems, devel@symisc.net.
 *  Copyright (C) Symisc Systems, http://vedis.symisc.net
 * Status:
 *    Devel.
 */
/* Forward reference */
static int VmJsonArrayEncode(vedis_value *pValue, void *pUserData);
/* 
 * JSON encoder state is stored in an instance 
 * of the following structure.
 */
typedef struct json_private_data json_private_data;
struct json_private_data
{
	SyBlob *pOut;      /* Output consumer buffer */
	int isFirst;       /* True if first encoded entry */
	int iFlags;        /* JSON encoding flags */
	int nRecCount;     /* Recursion count */
};
/*
 * Returns the JSON representation of a value.In other word perform a JSON encoding operation.
 * According to wikipedia
 * JSON's basic types are:
 *   Number (double precision floating-point format in JavaScript, generally depends on implementation)
 *   String (double-quoted Unicode, with backslash escaping)
 *   Boolean (true or false)
 *   Array (an ordered sequence of values, comma-separated and enclosed in square brackets; the values
 *    do not need to be of the same type)
 *   Object (an unordered collection of key:value pairs with the ':' character separating the key 
 *     and the value, comma-separated and enclosed in curly braces; the keys must be strings and should
 *     be distinct from each other)
 *   null (empty)
 * Non-significant white space may be added freely around the "structural characters"
 * (i.e. the brackets "[{]}", colon ":" and comma ",").
 */
static sxi32 VmJsonEncode(
	vedis_value *pIn,          /* Encode this value */
	json_private_data *pData /* Context data */
	){
		SyBlob *pOut = pData->pOut;
		int nByte;
		if( vedis_value_is_null(pIn) ){
			/* null */
			SyBlobAppend(pOut, "null", sizeof("null")-1);
		}else if( vedis_value_is_bool(pIn) ){
			int iBool = vedis_value_to_bool(pIn);
			sxu32 iLen;
			/* true/false */
			iLen = iBool ? sizeof("true") : sizeof("false");
			SyBlobAppend(pOut, iBool ? "true" : "false", iLen-1);
		}else if(  vedis_value_is_numeric(pIn) && !vedis_value_is_string(pIn) ){
			const char *zNum;
			/* Get a string representation of the number */
			zNum = vedis_value_to_string(pIn, &nByte);
			SyBlobAppend(pOut,zNum,nByte);
		}else if( vedis_value_is_string(pIn) ){
				const char *zIn, *zEnd;
				int c;
				/* Encode the string */
				zIn = vedis_value_to_string(pIn, &nByte);
				zEnd = &zIn[nByte];
				/* Append the double quote */
				SyBlobAppend(pOut,"\"", sizeof(char));
				for(;;){
					if( zIn >= zEnd ){
						/* No more input to process */
						break;
					}
					c = zIn[0];
					/* Advance the stream cursor */
					zIn++;
					if( c == '"' || c == '\\' ){
						/* Unescape the character */
						SyBlobAppend(pOut,"\\", sizeof(char));
					}
					/* Append character verbatim */
					SyBlobAppend(pOut,(const char *)&c,sizeof(char));
				}
				/* Append the double quote */
				SyBlobAppend(pOut,"\"",sizeof(char));
		}else if( vedis_value_is_array(pIn) ){
			/* Encode the array/object */
			pData->isFirst = 1;
			/* Append the square bracket or curly braces */
			SyBlobAppend(pOut,"[",sizeof(char));
			/* Iterate over array entries */
			vedis_array_walk(pIn, VmJsonArrayEncode, pData);
			/* Append the closing square bracket or curly braces */
			SyBlobAppend(pOut,"]",sizeof(char));
		}else{
			/* Can't happen */
			SyBlobAppend(pOut,"null",sizeof("null")-1);
		}
		/* All done */
		return VEDIS_OK;
}
/*
 * The following walker callback is invoked each time we need
 * to encode an array to JSON.
 */
static int VmJsonArrayEncode(vedis_value *pValue, void *pUserData)
{
	json_private_data *pJson = (json_private_data *)pUserData;
	if( pJson->nRecCount > 31 ){
		/* Recursion limit reached, return immediately */
		return VEDIS_OK;
	}
	if( !pJson->isFirst ){
		/* Append the colon first */
		SyBlobAppend(pJson->pOut,",",(int)sizeof(char));
	}
	/* Encode the value */
	pJson->nRecCount++;
	VmJsonEncode(pValue, pJson);
	pJson->nRecCount--;
	pJson->isFirst = 0;
	return VEDIS_OK;
}
#if 0
/*
 * The following walker callback is invoked each time we need to encode
 * a object instance [i.e: Object in the VEDIS jargon] to JSON.
 */
static int VmJsonObjectEncode(vedis_value *pKey,vedis_value *pValue,void *pUserData)
{
	json_private_data *pJson = (json_private_data *)pUserData;
	const char *zKey;
	int nByte;
	if( pJson->nRecCount > 31 ){
		/* Recursion limit reached, return immediately */
		return VEDIS_OK;
	}
	if( !pJson->isFirst ){
		/* Append the colon first */
		SyBlobAppend(pJson->pOut,",",sizeof(char));
	}
	/* Extract a string representation of the key */
	zKey = vedis_value_to_string(pKey, &nByte);
	/* Append the key and the double colon */
	if( nByte > 0 ){
		SyBlobAppend(pJson->pOut,"\"",sizeof(char));
		SyBlobAppend(pJson->pOut,zKey,(sxu32)nByte);
		SyBlobAppend(pJson->pOut,"\"",sizeof(char));
	}else{
		/* Can't happen */
		SyBlobAppend(pJson->pOut,"null",sizeof("null")-1);
	}
	SyBlobAppend(pJson->pOut,":",sizeof(char));
	/* Encode the value */
	pJson->nRecCount++;
	VmJsonEncode(pValue, pJson);
	pJson->nRecCount--;
	pJson->isFirst = 0;
	return VEDIS_OK;
}
#endif
/*
 *  Returns a string containing the JSON representation of value. 
 *  In other words, perform the serialization of the given JSON object.
 */
VEDIS_PRIVATE int vedisJsonSerialize(vedis_value *pValue,SyBlob *pOut)
{
	json_private_data sJson;
	/* Prepare the JSON data */
	sJson.nRecCount = 0;
	sJson.pOut = pOut;
	sJson.isFirst = 1;
	sJson.iFlags = 0;
	/* Perform the encoding operation */
	VmJsonEncode(pValue, &sJson);
	/* All done */
	return VEDIS_OK;
}
/*
 * ----------------------------------------------------------
 * File: hashmap.c
 * MD5: 8b3d7bf394c07e7c5442c871607e1f96
 * ----------------------------------------------------------
 */
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
/* $SymiscID: hashmap.c v1.2 FreeBSD 2013-07-20 06:09 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/*
 * Each hashmap entry [i.e: array(4, 5, 6)] is recorded in an instance
 * of the following structure.
 */
struct vedis_hashmap_node
{
	vedis_hashmap *pMap;     /* Hashmap that own this instance */
	sxi32 iType;           /* Node type */
	union{
		sxi64 iKey;        /* Int key */
		SyBlob sKey;       /* Blob key */
	}xKey;
	sxi32 iFlags;          /* Control flags */
	sxu32 nHash;           /* Key hash value */
	vedis_value sValue;    /* Node value */
	vedis_hashmap_node *pNext, *pPrev;               /* Link to other entries [i.e: linear traversal] */
	vedis_hashmap_node *pNextCollide, *pPrevCollide; /* Collision chain */
};
/* 
 * Each active hashmap is represented by an instance of the following structure.
 */
struct vedis_hashmap
{
	vedis  *pStore;                  /* Store that own this instance */
	vedis_hashmap_node **apBucket;  /* Hash bucket */
	vedis_hashmap_node *pFirst;     /* First inserted entry */
	vedis_hashmap_node *pLast;      /* Last inserted entry */
	vedis_hashmap_node *pCur;       /* Current entry */
	sxu32 nSize;                  /* Bucket size */
	sxu32 nEntry;                 /* Total number of inserted entries */
	sxu32 (*xIntHash)(sxi64);     /* Hash function for int_keys */
	sxu32 (*xBlobHash)(const void *, sxu32); /* Hash function for blob_keys */
	sxi32 iFlags;                 /* Hashmap control flags */
	sxi64 iNextIdx;               /* Next available automatically assigned index */
	sxi32 iRef;                   /* Reference count */
};
/* Allowed node types */
#define HASHMAP_INT_NODE   1  /* Node with an int [i.e: 64-bit integer] key */
#define HASHMAP_BLOB_NODE  2  /* Node with a string/BLOB key */
/*
 * Default hash function for int [i.e; 64-bit integer] keys.
 */
static sxu32 IntHash(sxi64 iKey)
{
	return (sxu32)(iKey ^ (iKey << 8) ^ (iKey >> 8));
}
/*
 * Default hash function for string/BLOB keys.
 */
static sxu32 BinHash(const void *pSrc, sxu32 nLen)
{
	register unsigned char *zIn = (unsigned char *)pSrc;
	unsigned char *zEnd;
	sxu32 nH = 5381;
	zEnd = &zIn[nLen];
	for(;;){
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
		if( zIn >= zEnd ){ break; } nH = nH * 33 + zIn[0] ; zIn++;
	}	
	return nH;
}
/*
 * Return the total number of entries in a given hashmap.
 */
VEDIS_PRIVATE sxu32 vedisHashmapCount(vedis_hashmap *pMap)
{
	return pMap->nEntry;
}
/*
 * Allocate a new hashmap node with a 64-bit integer key.
 * If something goes wrong [i.e: out of memory], this function return NULL.
 * Otherwise a fresh [vedis_hashmap_node] instance is returned.
 */
static vedis_hashmap_node * HashmapNewIntNode(vedis_hashmap *pMap, sxi64 iKey, sxu32 nHash,vedis_value *pValue)
{
	vedis_hashmap_node *pNode;
	/* Allocate a new node */
	pNode = (vedis_hashmap_node *)SyMemBackendPoolAlloc(&pMap->pStore->sMem, sizeof(vedis_hashmap_node));
	if( pNode == 0 ){
		return 0;
	}
	/* Zero the stucture */
	SyZero(pNode, sizeof(vedis_hashmap_node));
	/* Fill in the structure */
	pNode->pMap  = &(*pMap);
	pNode->iType = HASHMAP_INT_NODE;
	pNode->nHash = nHash;
	pNode->xKey.iKey = iKey;
	/* Duplicate the value */
	vedisMemObjInit(pMap->pStore,&pNode->sValue);
	if( pValue ){
		vedisMemObjStore(pValue,&pNode->sValue);
	}
	return pNode;
}
/*
 * Allocate a new hashmap node with a BLOB key.
 * If something goes wrong [i.e: out of memory], this function return NULL.
 * Otherwise a fresh [vedis_hashmap_node] instance is returned.
 */
static vedis_hashmap_node * HashmapNewBlobNode(vedis_hashmap *pMap, const void *pKey, sxu32 nKeyLen, sxu32 nHash,vedis_value *pValue)
{
	vedis_hashmap_node *pNode;
	/* Allocate a new node */
	pNode = (vedis_hashmap_node *)SyMemBackendPoolAlloc(&pMap->pStore->sMem, sizeof(vedis_hashmap_node));
	if( pNode == 0 ){
		return 0;
	}
	/* Zero the stucture */
	SyZero(pNode, sizeof(vedis_hashmap_node));
	/* Fill in the structure */
	pNode->pMap  = &(*pMap);
	pNode->iType = HASHMAP_BLOB_NODE;
	pNode->nHash = nHash;
	SyBlobInit(&pNode->xKey.sKey, &pMap->pStore->sMem);
	SyBlobAppend(&pNode->xKey.sKey, pKey, nKeyLen);
	/* Duplicate the value */
	vedisMemObjInit(pMap->pStore,&pNode->sValue);
	if( pValue ){
		vedisMemObjStore(pValue,&pNode->sValue);
	}
	return pNode;
}
/*
 * link a hashmap node to the given bucket index (last argument to this function).
 */
static void HashmapNodeLink(vedis_hashmap *pMap, vedis_hashmap_node *pNode, sxu32 nBucketIdx)
{
	/* Link */
	if( pMap->apBucket[nBucketIdx] != 0 ){
		pNode->pNextCollide = pMap->apBucket[nBucketIdx];
		pMap->apBucket[nBucketIdx]->pPrevCollide = pNode;
	}
	pMap->apBucket[nBucketIdx] = pNode;
	/* Link to the map list */
	if( pMap->pFirst == 0 ){
		pMap->pFirst = pMap->pLast = pNode;
		/* Point to the first inserted node */
		pMap->pCur = pNode;
	}else{
		MACRO_LD_PUSH(pMap->pLast, pNode);
	}
	++pMap->nEntry;
}
#define HASHMAP_FILL_FACTOR 3
/*
 * Grow the hash-table and rehash all entries.
 */
static sxi32 HashmapGrowBucket(vedis_hashmap *pMap)
{
	if( pMap->nEntry >= pMap->nSize * HASHMAP_FILL_FACTOR ){
		vedis_hashmap_node **apOld = pMap->apBucket;
		vedis_hashmap_node *pEntry, **apNew;
		sxu32 nNew = pMap->nSize << 1;
		sxu32 nBucket;
		sxu32 n;
		if( nNew < 1 ){
			nNew = 16;
		}
		/* Allocate a new bucket */
		apNew = (vedis_hashmap_node **)SyMemBackendAlloc(&pMap->pStore->sMem, nNew * sizeof(vedis_hashmap_node *));
		if( apNew == 0 ){
			if( pMap->nSize < 1 ){
				return SXERR_MEM; /* Fatal */
			}
			/* Not so fatal here, simply a performance hit */
			return SXRET_OK;
		}
		/* Zero the table */
		SyZero((void *)apNew, nNew * sizeof(vedis_hashmap_node *));
		/* Reflect the change */
		pMap->apBucket = apNew;
		pMap->nSize = nNew;
		if( apOld == 0 ){
			/* First allocated table [i.e: no entry], return immediately */
			return SXRET_OK;
		}
		/* Rehash old entries */
		pEntry = pMap->pFirst;
		n = 0;
		for( ;; ){
			if( n >= pMap->nEntry ){
				break;
			}
			/* Clear the old collision link */
			pEntry->pNextCollide = pEntry->pPrevCollide = 0;
			/* Link to the new bucket */
			nBucket = pEntry->nHash & (nNew - 1);
			if( pMap->apBucket[nBucket] != 0 ){
				pEntry->pNextCollide = pMap->apBucket[nBucket];
				pMap->apBucket[nBucket]->pPrevCollide = pEntry;
			}
			pMap->apBucket[nBucket] = pEntry;
			/* Point to the next entry */
			pEntry = pEntry->pPrev; /* Reverse link */
			n++;
		}
		/* Free the old table */
		SyMemBackendFree(&pMap->pStore->sMem, (void *)apOld);
	}
	return SXRET_OK;
}
/*
 * Insert a 64-bit integer key and it's associated value (if any) in the given
 * hashmap.
 */
static sxi32 HashmapInsertIntKey(vedis_hashmap *pMap,sxi64 iKey,vedis_value *pValue)
{
	vedis_hashmap_node *pNode;
	sxu32 nHash;
	sxi32 rc;
	
	/* Hash the key */
	nHash = pMap->xIntHash(iKey);
	/* Allocate a new int node */
	pNode = HashmapNewIntNode(&(*pMap), iKey, nHash, pValue);
	if( pNode == 0 ){
		return SXERR_MEM;
	}
	/* Make sure the bucket is big enough to hold the new entry */
	rc = HashmapGrowBucket(&(*pMap));
	if( rc != SXRET_OK ){
		SyMemBackendPoolFree(&pMap->pStore->sMem, pNode);
		return rc;
	}
	/* Perform the insertion */
	HashmapNodeLink(&(*pMap), pNode, nHash & (pMap->nSize - 1));
	/* All done */
	return SXRET_OK;
}
/*
 * Insert a BLOB key and it's associated value (if any) in the given
 * hashmap.
 */
static sxi32 HashmapInsertBlobKey(vedis_hashmap *pMap,const void *pKey,sxu32 nKeyLen,vedis_value *pValue)
{
	vedis_hashmap_node *pNode;
	sxu32 nHash;
	sxi32 rc;
	
	/* Hash the key */
	nHash = pMap->xBlobHash(pKey, nKeyLen);
	/* Allocate a new blob node */
	pNode = HashmapNewBlobNode(&(*pMap), pKey, nKeyLen, nHash,pValue);
	if( pNode == 0 ){
		return SXERR_MEM;
	}
	/* Make sure the bucket is big enough to hold the new entry */
	rc = HashmapGrowBucket(&(*pMap));
	if( rc != SXRET_OK ){
		SyMemBackendPoolFree(&pMap->pStore->sMem, pNode);
		return rc;
	}
	/* Perform the insertion */
	HashmapNodeLink(&(*pMap), pNode, nHash & (pMap->nSize - 1));
	/* All done */
	return SXRET_OK;
}
/*
 * Check if a given 64-bit integer key exists in the given hashmap.
 * Write a pointer to the target node on success. Otherwise
 * SXERR_NOTFOUND is returned on failure.
 */
static sxi32 HashmapLookupIntKey(
	vedis_hashmap *pMap,         /* Target hashmap */
	sxi64 iKey,                /* lookup key */
	vedis_hashmap_node **ppNode  /* OUT: target node on success */
	)
{
	vedis_hashmap_node *pNode;
	sxu32 nHash;
	if( pMap->nEntry < 1 ){
		/* Don't bother hashing, there is no entry anyway */
		return SXERR_NOTFOUND;
	}
	/* Hash the key first */
	nHash = pMap->xIntHash(iKey);
	/* Point to the appropriate bucket */
	pNode = pMap->apBucket[nHash & (pMap->nSize - 1)];
	/* Perform the lookup */
	for(;;){
		if( pNode == 0 ){
			break;
		}
		if( pNode->iType == HASHMAP_INT_NODE
			&& pNode->nHash == nHash
			&& pNode->xKey.iKey == iKey ){
				/* Node found */
				if( ppNode ){
					*ppNode = pNode;
				}
				return SXRET_OK;
		}
		/* Follow the collision link */
		pNode = pNode->pNextCollide;
	}
	/* No such entry */
	return SXERR_NOTFOUND;
}
/*
 * Check if a given BLOB key exists in the given hashmap.
 * Write a pointer to the target node on success. Otherwise
 * SXERR_NOTFOUND is returned on failure.
 */
static sxi32 HashmapLookupBlobKey(
	vedis_hashmap *pMap,          /* Target hashmap */
	const void *pKey,           /* Lookup key */
	sxu32 nKeyLen,              /* Key length in bytes */
	vedis_hashmap_node **ppNode   /* OUT: target node on success */
	)
{
	vedis_hashmap_node *pNode;
	sxu32 nHash;
	if( pMap->nEntry < 1 ){
		/* Don't bother hashing, there is no entry anyway */
		return SXERR_NOTFOUND;
	}
	/* Hash the key first */
	nHash = pMap->xBlobHash(pKey, nKeyLen);
	/* Point to the appropriate bucket */
	pNode = pMap->apBucket[nHash & (pMap->nSize - 1)];
	/* Perform the lookup */
	for(;;){
		if( pNode == 0 ){
			break;
		}
		if( pNode->iType == HASHMAP_BLOB_NODE 
			&& pNode->nHash == nHash
			&& SyBlobLength(&pNode->xKey.sKey) == nKeyLen 
			&& SyMemcmp(SyBlobData(&pNode->xKey.sKey), pKey, nKeyLen) == 0 ){
				/* Node found */
				if( ppNode ){
					*ppNode = pNode;
				}
				return SXRET_OK;
		}
		/* Follow the collision link */
		pNode = pNode->pNextCollide;
	}
	/* No such entry */
	return SXERR_NOTFOUND;
}
/*
 * Check if a given key exists in the given hashmap.
 * Write a pointer to the target node on success.
 * Otherwise SXERR_NOTFOUND is returned on failure.
 */
static sxi32 HashmapLookup(
	vedis_hashmap *pMap,          /* Target hashmap */
	vedis_value *pKey,            /* Lookup key */
	vedis_hashmap_node **ppNode   /* OUT: target node on success */
	)
{
	vedis_hashmap_node *pNode = 0; /* cc -O6 warning */
	sxi32 rc;
	if( pKey->iFlags & (MEMOBJ_STRING|MEMOBJ_HASHMAP) ){
		if( (pKey->iFlags & MEMOBJ_STRING) == 0 ){
			/* Force a string cast */
			vedisMemObjToString(&(*pKey));
		}
		if( SyBlobLength(&pKey->sBlob) > 0 ){
			/* Perform a blob lookup */
			rc = HashmapLookupBlobKey(&(*pMap), SyBlobData(&pKey->sBlob), SyBlobLength(&pKey->sBlob), &pNode);
			goto result;
		}
	}
	/* Perform an int lookup */
	if((pKey->iFlags & MEMOBJ_INT) == 0 ){
		/* Force an integer cast */
		vedisMemObjToInteger(pKey);
	}
	/* Perform an int lookup */
	rc = HashmapLookupIntKey(&(*pMap), pKey->x.iVal, &pNode);
result:
	if( rc == SXRET_OK ){
		/* Node found */
		if( ppNode ){
			*ppNode = pNode;
		}
		return SXRET_OK;
	}
	/* No such entry */
	return SXERR_NOTFOUND;
}
/*
 * Check if the given BLOB key looks like a decimal number. 
 * Retrurn TRUE on success.FALSE otherwise.
 */
static int HashmapIsIntKey(SyBlob *pKey)
{
	const char *zIn  = (const char *)SyBlobData(pKey);
	const char *zEnd = &zIn[SyBlobLength(pKey)];
	if( (int)(zEnd-zIn) > 1 && zIn[0] == '0' ){
		/* Octal not decimal number */
		return FALSE;
	}
	if( (zIn[0] == '-' || zIn[0] == '+') && &zIn[1] < zEnd ){
		zIn++;
	}
	for(;;){
		if( zIn >= zEnd ){
			return TRUE;
		}
		if( (unsigned char)zIn[0] >= 0xc0 /* UTF-8 stream */  || !SyisDigit(zIn[0]) ){
			break;
		}
		zIn++;
	}
	/* Key does not look like a decimal number */
	return FALSE;
}
/*
 * Insert a given key and it's associated value (if any) in the given
 * hashmap.
 * If a node with the given key already exists in the database
 * then this function overwrite the old value.
 */
static sxi32 HashmapInsert(
	vedis_hashmap *pMap, /* Target hashmap */
	vedis_value *pKey,   /* Lookup key  */
	vedis_value *pVal    /* Node value */
	)
{
	vedis_hashmap_node *pNode = 0;
	sxi32 rc = SXRET_OK;
	
	if( pKey && (pKey->iFlags & (MEMOBJ_STRING|MEMOBJ_HASHMAP)) ){
		if( (pKey->iFlags & MEMOBJ_STRING) == 0 ){
			/* Force a string cast */
			vedisMemObjToString(&(*pKey));
		}
		if( SyBlobLength(&pKey->sBlob) < 1 || HashmapIsIntKey(&pKey->sBlob) ){
			if(SyBlobLength(&pKey->sBlob) < 1){
				/* Automatic index assign */
				pKey = 0;
			}
			goto IntKey;
		}
		if( SXRET_OK == HashmapLookupBlobKey(&(*pMap), SyBlobData(&pKey->sBlob), 
			SyBlobLength(&pKey->sBlob), &pNode) ){
				/* Overwrite the old value */
				if( pVal ){
					vedisMemObjStore(pVal,&pNode->sValue);
				}else{
					/* Nullify the entry */
					vedisMemObjToNull(&pNode->sValue);
				}
				return SXRET_OK;
		}
		/* Perform a blob-key insertion */
		rc = HashmapInsertBlobKey(&(*pMap),SyBlobData(&pKey->sBlob),SyBlobLength(&pKey->sBlob),&(*pVal));
		return rc;
	}
IntKey:
	if( pKey ){
		if((pKey->iFlags & MEMOBJ_INT) == 0 ){
			/* Force an integer cast */
			vedisMemObjToInteger(pKey);
		}
		if( SXRET_OK == HashmapLookupIntKey(&(*pMap), pKey->x.iVal, &pNode) ){
			/* Overwrite the old value */
			if( pVal ){
				vedisMemObjStore(pVal,&pNode->sValue);
			}else{
				/* Nullify the entry */
				vedisMemObjToNull(&pNode->sValue);
			}
			return SXRET_OK;
		}
		/* Perform a 64-bit-int-key insertion */
		rc = HashmapInsertIntKey(&(*pMap), pKey->x.iVal, &(*pVal));
		if( rc == SXRET_OK ){
			if( pKey->x.iVal >= pMap->iNextIdx ){
				/* Increment the automatic index */ 
				pMap->iNextIdx = pKey->x.iVal + 1;
				/* Make sure the automatic index is not reserved */
				while( SXRET_OK == HashmapLookupIntKey(&(*pMap), pMap->iNextIdx, 0) ){
					pMap->iNextIdx++;
				}
			}
		}
	}else{
		/* Assign an automatic index */
		rc = HashmapInsertIntKey(&(*pMap),pMap->iNextIdx,&(*pVal));
		if( rc == SXRET_OK ){
			++pMap->iNextIdx;
		}
	}
	/* Insertion result */
	return rc;
}
/*
 * Allocate a new hashmap.
 * Return a pointer to the freshly allocated hashmap on success.NULL otherwise.
 */
VEDIS_PRIVATE vedis_hashmap * vedisNewHashmap(
	vedis *pStore,             /* Engine that trigger the hashmap creation */
	sxu32 (*xIntHash)(sxi64), /* Hash function for int keys.NULL otherwise*/
	sxu32 (*xBlobHash)(const void *, sxu32) /* Hash function for BLOB keys.NULL otherwise */
	)
{
	vedis_hashmap *pMap;
	/* Allocate a new instance */
	pMap = (vedis_hashmap *)SyMemBackendPoolAlloc(&pStore->sMem, sizeof(vedis_hashmap));
	if( pMap == 0 ){
		return 0;
	}
	/* Zero the structure */
	SyZero(pMap, sizeof(vedis_hashmap));
	/* Fill in the structure */
	pMap->pStore = &(*pStore);
	pMap->iRef = 1;
	/* pMap->iFlags = 0; */
	/* Default hash functions */
	pMap->xIntHash  = xIntHash ? xIntHash : IntHash;
	pMap->xBlobHash = xBlobHash ? xBlobHash : BinHash;
	return pMap;
}
/*
 * Increment the reference count of a given hashmap.
 */
VEDIS_PRIVATE void vedisHashmapRef(vedis_hashmap *pMap)
{
	pMap->iRef++;
}
/*
 * Release a hashmap.
 */
static sxi32 vedisHashmapRelease(vedis_hashmap *pMap)
{
	vedis_hashmap_node *pEntry, *pNext;
	vedis *pStore = pMap->pStore;
	sxu32 n;
	/* Start the release process */
	n = 0;
	pEntry = pMap->pFirst;
	for(;;){
		if( n >= pMap->nEntry ){
			break;
		}
		pNext = pEntry->pPrev; /* Reverse link */
		/* Release the vedis_value */
		vedisMemObjRelease(&pEntry->sValue);
		/* Release the node */
		if( pEntry->iType == HASHMAP_BLOB_NODE ){
			SyBlobRelease(&pEntry->xKey.sKey);
		}
		SyMemBackendPoolFree(&pStore->sMem, pEntry);
		/* Point to the next entry */
		pEntry = pNext;
		n++;
	}
	if( pMap->nEntry > 0 ){
		/* Release the hash bucket */
		SyMemBackendFree(&pStore->sMem, pMap->apBucket);
	}
	/* Free the whole instance */
	SyMemBackendPoolFree(&pStore->sMem, pMap);
	return SXRET_OK;
}
/*
 * Decrement the reference count of a given hashmap.
 * If the count reaches zero which mean no more variables
 * are pointing to this hashmap, then release the whole instance.
 */
VEDIS_PRIVATE void vedisHashmapUnref(vedis_hashmap *pMap)
{
	pMap->iRef--;
	if( pMap->iRef < 1 ){
		vedisHashmapRelease(pMap);
	}
}
VEDIS_PRIVATE vedis * vedisHashmapGetEngine(vedis_hashmap *pMap)
{
	return pMap->pStore;
}
/*
 * Check if a given key exists in the given hashmap.
 * Write a pointer to the target node on success.
 * Otherwise SXERR_NOTFOUND is returned on failure.
 */
VEDIS_PRIVATE sxi32 vedisHashmapLookup(
	vedis_hashmap *pMap,        /* Target hashmap */
	vedis_value *pKey,          /* Lookup key */
	vedis_value **ppOut /* OUT: Target node on success */
	)
{
	vedis_hashmap_node *pNode;
	sxi32 rc;
	if( pMap->nEntry < 1 ){
		/* TICKET 1433-25: Don't bother hashing, the hashmap is empty anyway.
		 */
		return SXERR_NOTFOUND;
	}
	rc = HashmapLookup(&(*pMap), &(*pKey),&pNode);
	if( rc != SXRET_OK ){
		return rc;
	}
	if( ppOut ){
		/* Point to the node value */
		*ppOut = &pNode->sValue;
	}
	return VEDIS_OK;
}
/*
 * Insert a given key and it's associated value (if any) in the given
 * hashmap.
 * If a node with the given key already exists in the database
 * then this function overwrite the old value.
 */
VEDIS_PRIVATE sxi32 vedisHashmapInsert(
	vedis_hashmap *pMap, /* Target hashmap */
	vedis_value *pKey,   /* Lookup key */
	vedis_value *pVal    /* Node value.NULL otherwise */
	)
{
	sxi32 rc;
	rc = HashmapInsert(&(*pMap), &(*pKey), &(*pVal));
	return rc;
}
/*
 * Iterate throw hashmap entries and invoke the given callback [i.e: xWalk()] for each 
 * retrieved entry.
 * If the callback wishes to abort processing [i.e: it's invocation] it must return
 * a value different from VEDIS_OK.
 * Refer to [vedis_array_walk()] for more information.
 */
VEDIS_PRIVATE sxi32 vedisHashmapWalk(
	vedis_hashmap *pMap, /* Target hashmap */
	int (*xWalk)(vedis_value *, void *), /* Walker callback */
	void *pUserData /* Last argument to xWalk() */
	)
{
	vedis_hashmap_node *pEntry;
	sxi32 rc;
	sxu32 n;
	/* Initialize walker parameter */
	rc = SXRET_OK;
	n = pMap->nEntry;
	pEntry = pMap->pFirst;
	/* Start the iteration process */
	for(;;){
		if( n < 1 ){
			break;
		}
		/* Invoke the user callback */
		rc = xWalk(&pEntry->sValue,pUserData);
		if( rc != VEDIS_OK ){
			/* Callback request an operation abort */
			return SXERR_ABORT;
		}
		/* Point to the next entry */
		pEntry = pEntry->pPrev; /* Reverse link */
		n--;
	}
	/* All done */
	return SXRET_OK;
}
/*
 * Reset the node cursor of a given hashmap.
 */
VEDIS_PRIVATE void vedisHashmapResetLoopCursor(vedis_hashmap *pMap)
{
	/* Reset the loop cursor */
	pMap->pCur = pMap->pFirst;
}
/*
 * Return a pointer to the node currently pointed by the node cursor.
 * If the cursor reaches the end of the list, then this function
 * return NULL.
 * Note that the node cursor is automatically advanced by this function.
 */
VEDIS_PRIVATE vedis_value * vedisHashmapGetNextEntry(vedis_hashmap *pMap)
{
	vedis_hashmap_node *pCur = pMap->pCur;
	if( pCur == 0 ){
		/* End of the list, return null */
		return 0;
	}
	/* Advance the node cursor */
	pMap->pCur = pCur->pPrev; /* Reverse link */
	/* Entry value */
	return &pCur->sValue;
}
/*
 * ----------------------------------------------------------
 * File: cmd.c
 * MD5: 9f423624c51655b52e412da8cc3bb222
 * ----------------------------------------------------------
 */
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
/* $SymiscID: cmd.c v1.2 FreeBSD 2013-07-10 04:45 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* Implementation of the vedis commands  */
/*
 *  Command: DEL key [key ...]
 * Description:
 *   Removes the specified keys. A key is ignored if it does not exist.
 * Return:
 * Integer: The number of keys that were removed.
 */
static int vedis_cmd_del(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int nDel = 0; 
	int rc;
	int i;
	/* Delete the given keys */
	for( i = 0 ; i < argc; ++i ){
		const char *zValue;
		int nByte;
		/* String representation of the key */
		zValue = vedis_value_to_string(argv[i],&nByte);
		/* Delete the key */
		rc = vedis_context_kv_delete(pCtx,(const void *)zValue,nByte);
		if( rc == VEDIS_OK ){
			nDel++;
		}
	}
	/* Total number of removed keys */
	vedis_result_int(pCtx,nDel);
	return VEDIS_OK;
}
/*
 *  Command:  EXISTS key
 * Description:
 *   Check key existance.
 * Return:
 *   bool: TRUE if key exists. FALSE otherwise.
 */
static int vedis_cmd_exists(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int rc = VEDIS_NOTFOUND;
	if( argc > 0 ){
		const char *zKey;
		int nByte;
		/* Target key */
		zKey = vedis_value_to_string(argv[0],&nByte);
		/* Fetch */
		rc = vedis_context_kv_fetch_callback(pCtx,zKey,nByte,0,0);
	}
	/* Result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:   APPEND key value
 * Description:
 *   If key already exists and is a string, this command appends the value
 *   at the end of the string. If key does not exist it is created and set 
 *   as an empty string, so APPEND will be similar to SET in this special case.
 * Return:
 *   Integer: the length of the string after the append operation.
 */
static int vedis_cmd_append(vedis_context *pCtx,int argc,vedis_value **argv)
{
	const char *zKey,*zValue;
	vedis_int64 nTot;
	int nKey,nByte;
	int rc;
	if( argc < 2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/value pair");
		/* Return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Target key */
	zKey  = vedis_value_to_string(argv[0],&nKey);
	zValue = vedis_value_to_string(argv[1],&nByte);
	if( nByte > 0 ){
		/* Append */
		rc = vedis_context_kv_append(pCtx,zKey,nKey,zValue,nByte);
		if( rc != VEDIS_OK ){
			vedis_result_int(pCtx,0);
			return VEDIS_ABORT;
		}
	}
	/* New length */
	nTot = nByte;
	vedis_context_kv_fetch(pCtx,zKey,nKey,0,&nTot);
	vedis_result_int64(pCtx,nTot);
	return VEDIS_OK;
}
/*
 *  Command:   STRLEN key
 * Description:
 *   Returns the length of the string value stored at key. An error is returned when key
 *   holds a non-string value.
 * Return:
 *   Integer: The length of the string at key, or 0 when key does not exist.
 */
static int vedis_cmd_strlen(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_int64 nByte = 0;
	if( argc > 0 ){
		const char *zKey;
		int nKey;
		/* Target key */
		zKey  = vedis_value_to_string(argv[0],&nKey);
		vedis_context_kv_fetch(pCtx,zKey,nKey,0,&nByte);
	}
	vedis_result_int64(pCtx,nByte);
	return VEDIS_OK;
}
/*
 * Fetch Key value from the underlying database.
 */
static int vedisFetchValue(vedis_context *pCtx,vedis_value *pArg,SyBlob *pOut)
{
	const char *zKey;
	int nByte;
	int rc;
	/* Target key */
	zKey = vedis_value_to_string(pArg,&nByte);
	/* Fetch the value */
	rc = vedis_context_kv_fetch_callback(pCtx,zKey,nByte,pOut ? vedisDataConsumer : 0,pOut);
	return rc;
}
/*
 *  Command:   GET key
 * Description:
 *   Get the value of key. If the key does not exist the special value nil is returned.
 * Return:
 *   the value of key, or nil when key does not exist.
 */
static int vedis_cmd_get(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int rc;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the record */
	rc = vedisFetchValue(pCtx,argv[0],VedisContextResultBuffer(pCtx));
	if( rc == VEDIS_OK ){
		vedis_result_string(pCtx,0,0);
	}else{
		/* No such record */
		vedis_result_null(pCtx);
	}
	return VEDIS_OK;
}
/*
 *  Command:   COPY old_key new_key
 * Description:
 *   Copy key values.
 * Return:
 *   Boolean: TRUE on success. FALSE otherwise.
 */
static int vedis_cmd_copy(vedis_context *pCtx,int argc,vedis_value **argv)
{
	const char *zNew;
	SyBlob *pWorker;
	int nByte,rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing old_key/new_key pair");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	pWorker = VedisContextWorkingBuffer(pCtx);
	SyBlobReset(pWorker);
	/* Fetch the record */
	rc = vedisFetchValue(pCtx,argv[0],pWorker);
	if( rc != VEDIS_OK ){
		/* No such record, return FALSE */
		vedis_result_bool(pCtx,0);
	}
	/* Duplicate the record */
	zNew = vedis_value_to_string(argv[1],&nByte);
	rc = vedis_context_kv_store(pCtx,zNew,nByte,SyBlobData(pWorker),(vedis_int64)SyBlobLength(pWorker));
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:   MOVE old_key new_key
 * Description:
 *   Move key values.
 * Return:
 *   Boolean: TRUE on success. FALSE otherwise.
 */
static int vedis_cmd_move(vedis_context *pCtx,int argc,vedis_value **argv)
{
	const char *zNew;
	SyBlob *pWorker;
	int nByte,rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing old_key/new_key pair");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	pWorker = VedisContextWorkingBuffer(pCtx);
	SyBlobReset(pWorker);
	/* Fetch the record */
	rc = vedisFetchValue(pCtx,argv[0],pWorker);
	if( rc != VEDIS_OK ){
		/* No such record, return FALSE */
		vedis_result_bool(pCtx,0);
	}
	/* Duplicate the record */
	zNew = vedis_value_to_string(argv[1],&nByte);
	rc = vedis_context_kv_store(pCtx,zNew,nByte,SyBlobData(pWorker),(vedis_int64)SyBlobLength(pWorker));
	if( rc == VEDIS_OK ){
		const char *zOld;
		/* Discard the old record */
		zOld = vedis_value_to_string(argv[0],&nByte);
		rc = vedis_context_kv_delete(pCtx,zOld,nByte);
	}
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:   MGET key [key ...]
 * Description:
 *   Returns the values of all specified keys. For every key that
 *   does not hold a string value or does not exist, the special value
 *   nil is returned. Because of this, the operation never fails.
 * Return:
 *   Array: Multiple values.
 */
static int vedis_cmd_mget(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pArray,*pScalar;
	SyBlob *pWorker;
	int i,rc;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new array and a working buffer */
	pArray = vedis_context_new_array(pCtx);
	pScalar = vedis_context_new_scalar(pCtx);
	if( pArray == 0 || pScalar == 0){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		/* pScalar and pArray will be automaticallay desotroyed */
		return VEDIS_OK;
	}
	vedis_value_string(pScalar,0,0);
	pWorker = vedisObjectValueBlob(pScalar);
	for( i = 0 ; i < argc; ++i ){
		/* Fetch the record */
		SyBlobReset(pWorker);
		rc = vedisFetchValue(pCtx,argv[i],pWorker);
		/* Populate our array */
		vedis_array_insert(pArray,rc == VEDIS_OK ? pScalar /* Will make its own copy */ : 0 /* null */);
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	return VEDIS_OK;
}
static int VedisStoreValue(vedis_context *pCtx,vedis_value *pKey,vedis_value *pData)
{
	const char *zKey,*zData;
	int nKey,nData;
	int rc;
	/* Extract the key and data */
	zKey  = vedis_value_to_string(pKey,&nKey);
	zData = vedis_value_to_string(pData,&nData);
	/* Perform the store operation */
	rc = vedis_context_kv_store(pCtx,zKey,nKey,zData,(vedis_int64)nData);
	return rc;
}
/*
 *  Command:   SET key value
 * Description:
 *   Set key to hold the string value. If key already holds a value, it is overwritten,
 *   regardless of its type. Any previous time to live associated with the key is
 *   discarded on successful SET operation.
 * Return:
 *   bool: TRUE on success, FALSE otherwise.
 */
static int vedis_cmd_set(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/value pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Perform the store operation */
	rc = VedisStoreValue(pCtx,argv[0],argv[1]);
	/* Store result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:  SETNX key value
 * Description:
 *   Set key to hold string value if key does not exist. In that case, it is equal to SET.
 *   When key already holds a value, no operation is performed. SETNX is short for
 *  "SET if N ot e X ists".
 * Return:
 *   bool: TRUE on success, FALSE otherwise.
 */
static int vedis_cmd_setnx(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/value pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the key */
	rc = vedisFetchValue(pCtx,argv[0],0);
	if( rc == VEDIS_OK ){
		/* Key exists, return FALSE */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Perform the store operation */
	rc = VedisStoreValue(pCtx,argv[0],argv[1]);
	/* Store result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:   MSET key value [key value]
 * Description:
 *   Sets the given keys to their respective values. MSET replaces existing values
 *   with new values, just as regular SET. See MSETNX if you don't want to overwrite
 *   existing values.
 *   MSET is atomic, so all given keys are set at once. It is not possible for clients
 *   to see that some of the keys were updated while others are unchanged.
 * Return:
 *   bool: TRUE on success, FALSE otherwise.
 */
static int vedis_cmd_mset(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int i,rc = VEDIS_OK;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/value pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	for( i = 0 ; i + 1 < argc ; i += 2 ){
		/* Perform the store operation */
		rc = VedisStoreValue(pCtx,argv[i],argv[i + 1]);
		if( rc != VEDIS_OK ){
			break;
		}
	}
	/* Store result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:   MSETNX key value [key value]
 * Description:
 *   Sets the given keys to their respective values. MSETNX replaces existing values
 *   with new values only if the key does not exits, just as regular SETNX. 
 *   MSET is atomic, so all given keys are set at once. It is not possible for clients
 *   to see that some of the keys were updated while others are unchanged.
 * Return:
 *   bool: TRUE on success, FALSE otherwise.
 */
static int vedis_cmd_msetnx(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int i,rc = VEDIS_OK;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/value pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	for( i = 0 ; i + 1 < argc ; i += 2 ){
		/* Fetch the key first */
		rc = vedisFetchValue(pCtx,argv[i],0);
		if( rc == VEDIS_OK ){
			/* Key exists, ignore */
			continue;
		}
		/* Perform the store operation */
		rc = VedisStoreValue(pCtx,argv[i],argv[i + 1]);
		if( rc != VEDIS_OK ){
			break;
		}
	}
	/* Store result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:    GETSET key value
 * Description:
 *   Atomically sets key to value and returns the old value stored at key.
 *   Returns an error when key exists but does not hold a string value.
 * Return:
 *   the old value stored at key, or nil when key does not exist.
 */
static int vedis_cmd_getset(vedis_context *pCtx,int argc,vedis_value **argv)
{
	SyBlob *pWorker;
	int rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/value pair");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Working buffer */
	pWorker = VedisContextWorkingBuffer(pCtx);
	SyBlobReset(pWorker);
	/* Fetch the key first */
	rc = vedisFetchValue(pCtx,argv[0],pWorker);
	if( rc != VEDIS_OK ){
		/* Key does not exists, return null */
		vedis_result_null(pCtx);
	}else{
		/* old value */
		vedis_result_string(pCtx,(const char *)SyBlobData(pWorker),(int)SyBlobLength(pWorker));
	}
	/* Perform the store operation */
	VedisStoreValue(pCtx,argv[0],argv[1]);	
	return VEDIS_OK;
}
/*
 * Increment/Decrement a vedis record. 
 */
static int vedisValueIncrementBy(vedis_context *pCtx,vedis_value *pKey,int nIncrement,int decr_op)
{
	vedis_int64 iVal = 0;
	vedis_value *pScalar;
	SyBlob *pWorker;
	int rc;
	pWorker = VedisContextWorkingBuffer(pCtx);
	SyBlobReset(pWorker);
	/* Fetch the value */
	rc = vedisFetchValue(pCtx,pKey,pWorker);
	if( rc == VEDIS_OK && SyBlobLength(pWorker) > 0 ){
		/* Cast to an integer */
		SyStrToInt64((const char *)SyBlobData(pWorker),SyBlobLength(pWorker),(void *)&iVal,0);
	}
	if( decr_op ){
		/* Decrement the number */
		iVal -= nIncrement;
	}else{
		/* Increment the number */
		iVal += nIncrement;
	}
	/* Store the result */
	vedis_result_int64(pCtx,iVal);
	/* Update the database */
	pScalar = vedis_context_new_scalar(pCtx);
	if( pScalar ==  0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		rc = VEDIS_NOMEM;
	}else{
		vedis_value_int64(pScalar,iVal);
		/* Update the database */
		rc = VedisStoreValue(pCtx,pKey,pScalar);
		/* cleanup */
		vedis_context_release_value(pCtx,pScalar);
	}
	return rc;
}
/*
 *  Command:     INCR key
 * Description:
 *   Increments the number stored at key by one. If the key does not exist,
 *   it is set to 0 before performing the operation. An error is returned if
 *   the key contains a value of the wrong type or contains a string that can
 *   not be represented as integer. This operation is limited to 64 bit signed integers.
 * Return:
 *   the value of key after the increment
 */
static int vedis_cmd_incr(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int rc;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Increment */
	rc = vedisValueIncrementBy(pCtx,argv[0],1,0);
	return rc;
}
/*
 *  Command:     DECR key
 * Description:
 *   Decrement the number stored at key by one. If the key does not exist,
 *   it is set to 0 before performing the operation. An error is returned if
 *   the key contains a value of the wrong type or contains a string that can
 *   not be represented as integer. This operation is limited to 64 bit signed integers.
 * Return:
 *   the value of key after the decrement
 */
static int vedis_cmd_decr(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int rc;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* decrement */
	rc = vedisValueIncrementBy(pCtx,argv[0],1,1);
	return rc;
}
/*
 *  Command:   INCRBY key increment 
 * Description:
 *   Increments the number stored at key by increment. If the key does not exist, it
 *   is set to 0 before performing the operation. An error is returned if the key
 *   contains a value of the wrong type or contains a string that can not be represented
 *   as integer. This operation is limited to 64 bit signed integers.
 * Return:
 *   the value of key after the increment
 */
static int vedis_cmd_incrby(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int iIncr;
	int rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/increment");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Number to increment by */
	iIncr = vedis_value_to_int(argv[1]);
	/* Increment */
	rc = vedisValueIncrementBy(pCtx,argv[0],iIncr,0);
	return rc;
}
/*
 *  Command:   DECRBY key increment 
 * Description:
 *   Decrements the number stored at key by decrement. If the key does not exist, it
 *   is set to 0 before performing the operation. An error is returned if the key
 *   contains a value of the wrong type or contains a string that can not be represented
 *   as integer. This operation is limited to 64 bit signed integers.
 * Return:
 *   the value of key after the decrement
 */
static int vedis_cmd_decrby(vedis_context *pCtx,int argc,vedis_value **argv)
{
	int iDecr;
	int rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/decrement");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Number to decrement by */
	iDecr = vedis_value_to_int(argv[1]);
	/* Increment */
	rc = vedisValueIncrementBy(pCtx,argv[0],iDecr,1);
	return rc;
}
/*
 * Fetch a key from the given vedis Table.
 */
static vedis_table_entry * vedisGetEntryFromTable(vedis_context *pCtx,vedis_value *pTable,vedis_value *pKey)
{
	vedis *pVedis = (vedis *)vedis_context_user_data(pCtx);
	vedis_table_entry *pEntry;
	vedis_table *pHash;
	/* Fetch the table first */
	pHash = vedisFetchTable(pVedis,pTable,0,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table */
		return 0;
	}
	/* Try to fetch the field */
	pEntry = vedisTableGetRecord(pHash,pKey);
	return pEntry;
}
#define VEDIS_ENTRY_BLOB(ENTRY) (ENTRY->iType == VEDIS_TABLE_ENTRY_BLOB_NODE)
static void vedisEntryKey(vedis_table_entry *pEntry,SyString *pOut)
{
	SyStringInitFromBuf(pOut,SyBlobData(&pEntry->xKey.sKey),SyBlobLength(&pEntry->xKey.sKey));
}
/*
 *  Command:    HGET key field 
 * Description:
 *   Returns the value associated with field in the hash stored at key.
 * Return:
 *   the value associated with field, or nil when field is not present in the hash
 *   or key does not exist.
 */
static int vedis_cmd_hget(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/field pair");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Go fetch */
	pEntry = vedisGetEntryFromTable(pCtx,argv[0],argv[1]);
	if( pEntry == 0 ){
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Return the payload */
	vedis_result_string(pCtx,(const char *)SyBlobData(&pEntry->sData),(int)SyBlobLength(&pEntry->sData));
	return VEDIS_OK;
}
/*
 *  Command:     HMGET key field [field ...]  
 * Description:
 *   Returns the values associated with the specified fields in the hash stored at key.
 *   For every field that does not exist in the hash, a nil value is returned.
 *   Because a non-existing keys are treated as empty hashes, running HMGET against
 *   a non-existing key will return a list of nil values.
 * Return:
 *   array of values associated with the given fields, in the same order as they are requested.
 */
static int vedis_cmd_hmget(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pScalar,*pArray;
	vedis_table_entry *pEntry;
	vedis_table *pHash;
	int i;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/field pair");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pHash = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Perform the requested operation */
	for( i = 1 ; i < argc ; ++i ){
		/* Fetch the record */
		pEntry = vedisTableGetRecord(pHash,argv[i]);
		if( pEntry == 0 ){
			/* Insert null */
			vedis_value_null(pScalar);
		}else{
			/* Populate the scalar with the data */
			vedis_value_reset_string_cursor(pScalar);
			vedis_value_string(pScalar,(const char *)SyBlobData(&pEntry->sData),(int)SyBlobLength(&pEntry->sData));
		}
		/* Perform the insertion */
		vedis_array_insert(pArray,pScalar); /* Will make its own copy */
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:      HKEYS key  
 * Description:
 *   Returns all field names in the hash stored at key.
 * Return:
 *   array of fields in the hash, or null on failure.
 */
static int vedis_cmd_hkeys(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pScalar,*pArray;
	vedis_table_entry *pEntry;
	vedis_table *pHash;
	
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pHash = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Perform the requested operation */
	vedisTableReset(pHash);
	while( (pEntry = vedisTableNextEntry(pHash)) != 0 ){
		if( VEDIS_ENTRY_BLOB(pEntry) ){
			SyString sKey;
			vedisEntryKey(pEntry,&sKey);
			/* Populate the scalar with the data */
			vedis_value_reset_string_cursor(pScalar);
			vedis_value_string(pScalar,sKey.zString,(int)sKey.nByte);
			/* Perform the insertion */
			vedis_array_insert(pArray,pScalar); /* Will make its own copy */
		}
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:       HVALS key  
 * Description:
 *   Returns all values in the hash stored at key.
 * Return:
 *   array of values in the hash, or an empty list when key does not exist.
 */
static int vedis_cmd_hvals(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pScalar,*pArray;
	vedis_table_entry *pEntry;
	vedis_table *pHash;
	
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pHash = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Perform the requested operation */
	vedisTableReset(pHash);
	while( (pEntry = vedisTableNextEntry(pHash)) != 0 ){
		/* Populate the scalar with the data */
		vedis_value_reset_string_cursor(pScalar);
		vedis_value_string(pScalar,(const char *)SyBlobData(&pEntry->sData),(int)SyBlobLength(&pEntry->sData));		
		/* Perform the insertion */
		vedis_array_insert(pArray,pScalar); /* Will make its own copy */
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:      HGETALL key 
 * Description:
 *   Returns all fields and values of the hash stored at key. In the returned value,
 *   every field name is followed by its value, so the length of the reply is twice
 *   the size of the hash.
 * Return:
 *   array of fields and their values stored in the hash, or an empty list when key does not exist.
 */
static int vedis_cmd_hgetall(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pScalar,*pArray;
	vedis_table_entry *pEntry;
	vedis_table *pHash;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pHash = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Perform the requested operation */
	vedisTableReset(pHash);
	while( (pEntry = vedisTableNextEntry(pHash)) != 0 ){
		if( VEDIS_ENTRY_BLOB(pEntry) ){
			SyString sKey;
			vedisEntryKey(pEntry,&sKey);
			/* Populate the scalar with the key */
			vedis_value_reset_string_cursor(pScalar);
			vedis_value_string(pScalar,sKey.zString,(int)sKey.nByte);
			/* Insert the key */
			vedis_array_insert(pArray,pScalar); /* Will make its own copy of pScalar */
			/* Populate the scalar with the data */
			vedis_value_reset_string_cursor(pScalar);
			vedis_value_string(pScalar,(const char *)SyBlobData(&pEntry->sData),(int)SyBlobLength(&pEntry->sData));
			/* Perform the insertion */
			vedis_array_insert(pArray,pScalar); /* Will make its own copy */
		}
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:    HEXISTS key field 
 * Description:
 *   Returns if field is an existing field in the hash stored at key.
 * Return:
 *   boolean: TRUE on success, FALSE otherwise.
 */
static int vedis_cmd_hexists(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/field pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Go fetch */
	pEntry = vedisGetEntryFromTable(pCtx,argv[0],argv[1]);
	if( pEntry == 0 ){
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Return true */
	vedis_result_bool(pCtx,1);
	return VEDIS_OK;
}
/*
 *  Command:     HDEL key field [field ...] 
 * Description:
 *   Removes the specified fields from the hash stored at key. Specified fields
 *   that do not exist within this hash are ignored. If key does not exist, it is treated
 *   as an empty hash and this command returns 0.
 * Return:
 *   integer: Total number of fields removed.
 */
static int vedis_cmd_hdel(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pHash;
	int nDel = 0;
	int i,rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/field pair");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pHash = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Perform the deletion */
	for( i = 1 ; i < argc ; ++i ){
		rc = vedisTableDeleteRecord(pHash,argv[i]);
		if( rc == VEDIS_OK ){
			nDel++;
		}
	}
	/* Total number of deleted records */
	vedis_result_int(pCtx,nDel);
	return VEDIS_OK;
}
/*
 *  Command:     HLEN key 
 * Description:
 *   Returns the number of fields contained in the hash stored at key.
 * Return:
 *    number of fields in the hash, or 0 when key does not exist.
 */
static int vedis_cmd_hlen(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pHash;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pHash = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	vedis_result_int(pCtx,(int)vedisTableLength(pHash));
	return VEDIS_OK;
}
/*
 *  Command:     HSET key field value  
 * Description:
 *   Sets field in the hash stored at key to value. If key does not exist, a new key holding
 *   a hash is created. If field already exists in the hash, it is overwritten.
 * Return:
 *   boolean: TRUE on success. FALSE on failure.
 */
static int vedis_cmd_hset(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis *pVedis = (vedis *)vedis_context_user_data(pCtx);
	vedis_table *pHash;
	int rc;

	if( argc < 3 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key field/value pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table first */
	pHash = vedisFetchTable(pVedis,argv[0],1,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return FALSE */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Perform the insertion  */
	rc = vedisTableInsertRecord(pHash,argv[1],argv[2]);
	/* Insertion result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:      HMSET key field value [field value ...]  
 * Description:
 *   Sets the specified fields to their respective values in the hash stored at key.
 *   This command overwrites any existing fields in the hash. If key does not exist,
 *   a new key holding a hash is created.
 * Return:
 *   Integer: Total number of inserted fields.
 */
static int vedis_cmd_hmset(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pHash;
	int i,rc,cnt = 0;
	if( argc < 3 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key field/value pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	pHash = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],1,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	rc = VEDIS_OK;
	for( i = 1 ; i + 1 < argc ; i += 2 ){
		rc = vedisTableInsertRecord(pHash,argv[i],argv[i+1]);
		if( rc == VEDIS_OK ){
			cnt++;
		}
	}
	/* Insertion result */
	vedis_result_int(pCtx,cnt);
	return VEDIS_OK;
}
/*
 *  Command:      HSETNX key field value   
 * Description:
 *   Sets field in the hash stored at key to value, only if field does not yet exist.
 *   If key does not exist, a new key holding a hash is created. If field already exists,
 *   this operation has no effect.
 * Return:
 *   boolean: TRUE on success. FALSE on failure.
 */
static int vedis_cmd_hsetnx(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis *pVedis = (vedis *)vedis_context_user_data(pCtx);
	vedis_table_entry *pEntry;
	vedis_table *pHash;
	int rc;
	
	if( argc < 3 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key field/value pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table first */
	pHash = vedisFetchTable(pVedis,argv[0],1,VEDIS_TABLE_HASH);
	if( pHash == 0 ){
		/* No such table, return FALSE */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the record */
	pEntry = vedisTableGetRecord(pHash,argv[1]);
	if( pEntry ){
		/* Record exists, return FALSE */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Safely, erform the insertion  */
	rc = vedisTableInsertRecord(pHash,argv[1],argv[2]);
	/* Insertion result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command:     TABLE_LIST  
 * Description:
 *   Return a array holding the list of loaded vedis tables (i.e. Hashes, Sets, List) in memory.
 * Return:
 *   array of loaded tables.
 */
static int vedis_cmd_table_list(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	vedis_value *pScalar,*pArray;
	vedis_table *pEntry;
	sxu32 n;
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		SXUNUSED(argc); /* cc warning */
		SXUNUSED(argv);
		return VEDIS_OK;
	}
	/* Point to the first entry */
	pEntry = pStore->pTableList;
	for( n = 0 ; n < pStore->nTable ; ++n ){
		SyString *pName = vedisTableName(pEntry);
		/* Populate the scalar with the data */
		vedis_value_reset_string_cursor(pScalar);
		vedis_value_string(pScalar,pName->zString,(int)pName->nByte);
		/* Perform the insertion */
		vedis_array_insert(pArray,pScalar); /* Will make its own copy */
		/* Point to the next loaded table */
		pEntry = vedisTableChain(pEntry);
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:    SADD key member [member ...]  
 * Description:
 *   Add the specified members to the set stored at key. Specified members that
 *   are already a member of this set are ignored. If key does not exist, a new
 *   set is created before adding the specified members. An error is returned when
 *   the value stored at key is not a set.
 * Return:
 *   Intger: number of item succesfully stored.
 */
static int vedis_cmd_sadd(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis *pVedis = (vedis *)vedis_context_user_data(pCtx);
	vedis_table *pSet;
	int nStore = 0;
	int i,rc;
	
	if( argc < 2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/member pair");
		/* return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table first */
	pSet = vedisFetchTable(pVedis,argv[0],1,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Perform the insertion  */
	for( i = 1 ; i < argc ; ++i ){
		rc = vedisTableInsertRecord(pSet,argv[i],0/* No data */);
		if( rc == VEDIS_OK ){
			nStore++;
		}
	}
	/* Total number of items stored  */
	vedis_result_int(pCtx,nStore);
	return VEDIS_OK;
}
/*
 *  Command:    SCARD key 
 * Description:
 *   Returns the set cardinality (number of elements) of the set stored at key.
 * Return:
 *    number of fields in the set, or 0 when key does not exist.
 */
static int vedis_cmd_scard(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pSet;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	vedis_result_int(pCtx,(int)vedisTableLength(pSet));
	return VEDIS_OK;
}
/*
 *  Command:    SISMEMBER key member 
 * Description:
 *   Returns if member is a member of the set stored at key.
 * Return:
 *   boolean: TRUE on success, FALSE otherwise.
 */
static int vedis_cmd_sismember(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	vedis_table *pSet;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/member pair");
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table first */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Go fetch */
	pEntry = vedisTableGetRecord(pSet,argv[1]);
	if( pEntry == 0 ){
		/* return false */
		vedis_result_bool(pCtx,0);
		return VEDIS_OK;
	}
	/* Return true */
	vedis_result_bool(pCtx,1);
	return VEDIS_OK;
}
/*
 *  Command:   SPOP key 
 * Description:
 *   Removes and returns the last record from the set value stored at key.
 * Return:
 *   the removed element, or nil when key does not exist or is empty.
 */
static int vedis_cmd_spop(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	vedis_table *pSet;
	if( argc < 1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table first */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Extract the last entry */
	pEntry = vedisTableLastEntry(pSet);
	if( pEntry == 0 ){
		/* Empty table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	if ( VEDIS_ENTRY_BLOB(pEntry) ){
		SyString sKey;
		vedisEntryKey(pEntry,&sKey);
		/* Return its key */
		vedis_result_string(pCtx,sKey.zString,(int)sKey.nByte);
	}
	/* Discard this element */
	VedisRemoveTableEntry(pSet,pEntry);
	return VEDIS_OK;
}
/*
 *  Command:   SPEEK key 
 * Description:
 *   Returns the last record from the set value stored at key.
 * Return:
 *   the last element, or nil when key does not exist.
 */
static int vedis_cmd_speek(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	vedis_table *pSet;
	if( argc < 1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table first */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Extract the last entry */
	pEntry = vedisTableLastEntry(pSet);
	if( pEntry == 0 ){
		/* Empty table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	if ( VEDIS_ENTRY_BLOB(pEntry) ){
		SyString sKey;
		vedisEntryKey(pEntry,&sKey);
		/* Return its key */
		vedis_result_string(pCtx,sKey.zString,(int)sKey.nByte);
	}
	return VEDIS_OK;
}
/*
 *  Command:   STOP key 
 * Description:
 *   Returns the first record from the set value stored at key.
 * Return:
 *   the last element, or nil when key does not exist.
 */
static int vedis_cmd_stop(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	vedis_table *pSet;
	if( argc < 1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table first */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Extract the first entry */
	pEntry = vedisTableFirstEntry(pSet);
	if( pEntry == 0 ){
		/* Empty table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	if ( VEDIS_ENTRY_BLOB(pEntry) ){
		SyString sKey;
		vedisEntryKey(pEntry,&sKey);
		/* Return its key */
		vedis_result_string(pCtx,sKey.zString,(int)sKey.nByte);
	}
	return VEDIS_OK;
}
/*
 *  Command:     SREM key member [member ...]  
 * Description:
 *   Remove the specified members from the set stored at key.
 *   Specified members that are not a member of this set are ignored.
 *   If key does not exist, it is treated as an empty set and this command returns 0.
 * Return:
 *   Integer: the number of members that were removed from the set, not including non existing members.
 */
static int vedis_cmd_srem(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pSet;
	int nDel = 0;
	int i,rc;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/member pair");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Perform the deletion */
	for( i = 1 ; i < argc ; ++i ){
		rc = vedisTableDeleteRecord(pSet,argv[i]);
		if( rc == VEDIS_OK ){
			nDel++;
		}
	}
	/* Total number of deleted records */
	vedis_result_int(pCtx,nDel);
	return VEDIS_OK;
}
/*
 *  Command:    SMEMBERS key  
 * Description:
 *   Returns all the members of the set value stored at key.
 * Return:
 *   array of all elements of the set.
 */
static int vedis_cmd_smembers(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pScalar,*pArray;
	vedis_table_entry *pEntry;
	vedis_table *pSet;
	
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Perform the requested operation */
	vedisTableReset(pSet);
	while( (pEntry = vedisTableNextEntry(pSet)) != 0 ){
		if( VEDIS_ENTRY_BLOB(pEntry) ){
			SyString sKey;
			vedisEntryKey(pEntry,&sKey);
			/* Populate the scalar with the key */
			vedis_value_reset_string_cursor(pScalar);
			vedis_value_string(pScalar,sKey.zString,(int)sKey.nByte);
			/* Insert the key */
			vedis_array_insert(pArray,pScalar); /* Will make its own copy of pScalar */
		}
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:    SDIFF key [key ...] 
 * Description:
 *   Returns the members of the set resulting from the difference between the first set
 *   and all the successive sets.
 * Return:
 *   array of Keys that do not exist are considered to be empty sets.
 */
static int vedis_cmd_sdiff(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pScalar,*pArray;
	vedis_table_entry *pEntry;
	vedis_table *pSrc;
	int i;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pSrc = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSrc == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Perform the requested operation */
	vedisTableReset(pSrc);
	while( (pEntry = vedisTableNextEntry(pSrc)) != 0 ){
		if( VEDIS_ENTRY_BLOB(pEntry) ){
			SyString sKey;
			vedisEntryKey(pEntry,&sKey);
			/* Populate the scalar with the key */
			vedis_value_reset_string_cursor(pScalar);
			vedis_value_string(pScalar,sKey.zString,(int)sKey.nByte);
			/* Perform the diff */
			for( i = 1 ; i < argc ; ++i ){
				vedis_table *pTarget = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[i],0,VEDIS_TABLE_SET);
				vedis_table_entry *pEntry;
				if( pTarget == 0 ){
					/* No such set */
					continue;
				}
				/* Perform the lokup */
				pEntry = vedisTableGetRecord(pTarget,pScalar);
				if( pEntry ){
					/* Entry found */
					break;
				}
			}
			if( i >= argc ){
				/* Perform the insertion */
				vedis_array_insert(pArray,pScalar);
			}

		}
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:    SINTER key [key ...] 
 * Description:
 *   Returns the members of the set resulting from the intersection of all the given sets.
 * Return:
 *   array of Keys that do not exist are considered to be empty sets.
 */
static int vedis_cmd_sinter(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_value *pScalar,*pArray;
	vedis_table_entry *pEntry;
	vedis_table *pSrc;
	int i;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pSrc = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSrc == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Perform the requested operation */
	vedisTableReset(pSrc);
	while( (pEntry = vedisTableNextEntry(pSrc)) != 0 ){
		if( VEDIS_ENTRY_BLOB(pEntry) ){
			SyString sKey;
			vedisEntryKey(pEntry,&sKey);
			/* Populate the scalar with the key */
			vedis_value_reset_string_cursor(pScalar);
			vedis_value_string(pScalar,sKey.zString,(int)sKey.nByte);
			/* Perform the intersection */
			for( i = 1 ; i < argc ; ++i ){
				vedis_table *pTarget = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[i],0,VEDIS_TABLE_SET);
				vedis_table_entry *pEntry;
				if( pTarget == 0 ){
					/* No such set */
					continue;
				}
				/* Perform the lokup */
				pEntry = vedisTableGetRecord(pTarget,pScalar);
				if( !pEntry ){
					/* no such entry */
					break;
				}
			}
			if( i >= argc ){
				/* Perform the insertion */
				vedis_array_insert(pArray,pScalar);
			}

		}
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	vedis_context_release_value(pCtx,pScalar);
	/* pArray will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command:     SLEN key 
 * Description:
 *   Returns the number of fields contained in the set stored at key.
 * Return:
 *    number of fields in the set, or 0 when key does not exist.
 */
static int vedis_cmd_slen(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pSet;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pSet = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_SET);
	if( pSet == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	vedis_result_int(pCtx,(int)vedisTableLength(pSet));
	return VEDIS_OK;
}
/*
 *  Command:   LINDEX key index  
 * Description:
 *   Returns the element at index index in the list stored at key.
 *   The index is zero-based, so 0 means the first element, 1 the second
 *   element and so on. Negative indices can be used to designate elements
 *   starting at the tail of the list. Here, -1 means the last element, -2 means
 *   the penultimate and so forth.
 *   When the value at key is not a list, an error is returned.
 * Return:
 *   the requested element, or nil when index is out of range.
 */
static int vedis_cmd_lindex(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	vedis_table *pList;
	sxu32 nReal;
	int iIndex;
	
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/index pair");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pList = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_LIST);
	if( pList == 0 ){
		/* No such table, return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Index */
	iIndex = vedis_value_to_int(argv[1]);
	if( iIndex < 0 ){
		iIndex = -iIndex;
		nReal = vedisTableLength(pList) - iIndex;
	}else{
		nReal = (sxu32)iIndex;
	}
	/* Go fetch */
	pEntry = vedisTableGetRecordByIndex(pList,nReal); /* This will handle out of range indexes */
	if( pEntry == 0 ){
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Return data */
	vedis_result_string(pCtx,(const char *)SyBlobData(&pEntry->sData),(int)SyBlobLength(&pEntry->sData));
	return VEDIS_OK;
}
/*
 *  Command:    LLEN key  
 * Description:
 *   Returns the number of fields contained in the list stored at key.
 * Return:
 *    number of fields in the list, or 0 when key does not exist.
 */
static int vedis_cmd_llen(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pList;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pList = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_LIST);
	if( pList == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	vedis_result_int(pCtx,(int)vedisTableLength(pList));
	return VEDIS_OK;
}
/*
 *  Command:    LPOP key  
 * Description:
 *   Removes and returns the first element of the list stored at key.
 * Return:
 *    the value of the first element, or nil when key does not exist.
 */
static int vedis_cmd_lpop(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table_entry *pEntry;
	vedis_table *pList;
	if( argc <  1 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key");
		/* return null */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pList = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],0,VEDIS_TABLE_LIST);
	if( pList == 0 ){
		/* No such table, return zero */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Point to the first element */
	pEntry = vedisTableFirstEntry(pList);
	if( pEntry == 0 ){
		/* No such entry */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	vedis_result_string(pCtx,(const char *)SyBlobData(&pEntry->sData),(int)SyBlobLength(&pEntry->sData));
	/* Discard item */
	VedisRemoveTableEntry(pList,pEntry);
	return VEDIS_OK;
}
/*
 *  Command:   LPUSH key value [value ...]  
 * Description:
 *   Insert all the specified values at the head of the list stored at key. If key does
 *   not exist, it is created as empty list before performing the push operations.
 *   It is possible to push multiple elements using a single command call just specifying
 *   multiple arguments at the end of the command. Elements are inserted one after the other
 *   to the head of the list, from the leftmost element to the rightmost element.
 *   So for instance the command LPUSH mylist a b c will result into a list containing
 *  c as first element, b as second element and a as third element.
 * Return:
 *    the length of the list after the push operations.
 */
static int vedis_cmd_lpush(vedis_context *pCtx,int argc,vedis_value **argv)
{
	vedis_table *pList;
	int i;
	if( argc <  2 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Missing key/value pair");
		/* return 0 */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Fetch the table  */
	pList = vedisFetchTable((vedis *)vedis_context_user_data(pCtx),argv[0],1,VEDIS_TABLE_LIST);
	if( pList == 0 ){
		/* No such table, return zero */
		vedis_result_int(pCtx,0);
		return VEDIS_OK;
	}
	/* Perform the insertion */
	for( i = 1 ; i < argc; ++i ){
		vedisTableInsertRecord(pList,0/*Assign an automatic key*/,argv[i]);
	}
	/* Total number of inserted elements */
	vedis_result_int(pCtx,(int)vedisTableLength(pList));
	return VEDIS_OK;
}
/*
 *  Command: RAND [min] [max]
 * Description:
 *  Generate a random (unsigned 32-bit) integer.
 * Parameter
 *  min
 *    The lowest value to return (default: 0)
 *  max
 *   The highest value to return (default: GETRANDMAX)
 * Return
 *   Unsigned integer: A pseudo random value between min (or 0) and max (or GETRANDMAX, inclusive).
 */
static int vedis_cmd_rand(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	sxu32 iNum;
	/* Generate the random number */
	iNum = vedisPagerRandomNum(pStore->pPager);
	if( nArg > 1 ){
		sxu32 iMin, iMax;
		iMin = (sxu32)vedis_value_to_int(apArg[0]);
		iMax = (sxu32)vedis_value_to_int(apArg[1]);
		if( iMin < iMax ){
			sxu32 iDiv = iMax+1-iMin;
			if( iDiv > 0 ){
				iNum = (iNum % iDiv)+iMin;
			}
		}else if(iMax > 0 ){
			iNum %= iMax;
		}
	}
	/* Return the generated number */
	vedis_result_int64(pCtx, (vedis_int64)iNum);
	return VEDIS_OK;
}
/*
 *  Command: GETRANDMAX
 * Description:
 *   Show largest possible random value
 * Return
 *  Unsigned Integer: The largest possible random value returned by rand() which is in
 *  this implementation 0xFFFFFFFF.
 */
static int vedis_cmd_getrandmax(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	SXUNUSED(nArg); /* cc warning */
	SXUNUSED(apArg);
	vedis_result_int64(pCtx, SXU32_HIGH);
	return VEDIS_OK;
}
/*
 *  Command: RANDSTR [len]
 * Description:
 *  Generate a random string (English alphabet).
 * Parameter
 *  len
 *    Length of the desired string (default: 16, Min: 1, Max: 1024)
 * Return
 *   String: A pseudo random string.
 */
static int vedis_cmd_rand_str(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	char zString[1024];
	int iLen = 0x10;
	if( nArg > 0 ){
		/* Get the desired length */
		iLen = vedis_value_to_int(apArg[0]);
		if( iLen < 1 || iLen > 1024 ){
			/* Default length */
			iLen = 0x10;
		}
	}
	/* Generate the random string */
	vedisPagerRandomString(pStore->pPager, zString, iLen);
	/* Return the generated string */
	vedis_result_string(pCtx, zString, iLen); /* Will make it's own copy */
	return VEDIS_OK;
}
/*
 * Output consumer callback for the standard Symisc routines.
 * [i.e: SyBase64Encode(), SyBase64Decode(), SyUriEncode(), ...].
 */
static int base64Consumer(const void *pData, unsigned int nLen, void *pUserData)
{
	/* Store in the call context result buffer */
	vedis_result_string((vedis_context *)pUserData, (const char *)pData, (int)nLen);
	return SXRET_OK;
}
/*
 *  Command: BASE64 data
 * Description:
 *  Encode data with MIME base64
 * Parameter
 *  data
 *    Data to encode
 * Return
 *  String: MIME base64 encoded input on sucess. False otherwise.
 */
static int vedis_cmd_base64_encode(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	const char *zIn;
	int nLen;
	if( nArg < 1 ){
		/* Missing arguments, return FALSE */
		vedis_result_bool(pCtx, 0);
		return VEDIS_OK;
	}
	/* Extract the input string */
	zIn = vedis_value_to_string(apArg[0], &nLen);
	if( nLen < 1 ){
		/* Nothing to process, return FALSE */
		vedis_result_bool(pCtx, 0);
		return VEDIS_OK;
	}
	/* Perform the BASE64 encoding */
	SyBase64Encode(zIn, (sxu32)nLen, base64Consumer, pCtx);
	return VEDIS_OK;
}
/*
 * Command: BASE64_DEC
 *  Decode MIME base64 based input
 * Parameter
 *  data
 *    Encoded data.
 * Return
 *  String: Returns the original data or FALSE on failure.
 */
static int vedis_cmd_base64_decode(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	const char *zIn;
	int nLen;
	if( nArg < 1 ){
		/* Missing arguments, return FALSE */
		vedis_result_bool(pCtx, 0);
		return VEDIS_OK;
	}
	/* Extract the input string */
	zIn = vedis_value_to_string(apArg[0], &nLen);
	if( nLen < 1 ){
		/* Nothing to process, return FALSE */
		vedis_result_bool(pCtx, 0);
		return VEDIS_OK;
	}
	/* Perform the BASE64 decoding */
	SyBase64Decode(zIn, (sxu32)nLen, base64Consumer, pCtx);
	return VEDIS_OK;
}
/*
 * Command: SOUNDEX string
 *  Calculate the soundex key of a string.
 * Parameters
 *  string
 *   The input string.
 * Return
 *  String: Returns the soundex key as a string.
 * Note:
 *  This implementation is based on the one found in the SQLite3
 * source tree.
 */
static int vedis_cmd_soundex(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	const unsigned char *zIn;
	char zResult[8];
	int i, j;
	static const unsigned char iCode[] = {
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0, 
		1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0, 
		0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0, 
		1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0, 
	};
	if( nArg < 1 ){
		/* Missing arguments, return the empty string */
		vedis_result_string(pCtx, "", 0);
		return VEDIS_OK;
	}
	zIn = (unsigned char *)vedis_value_to_string(apArg[0], 0);
	for(i=0; zIn[i] && zIn[i] < 0xc0 && !SyisAlpha(zIn[i]); i++){}
	if( zIn[i] ){
		unsigned char prevcode = iCode[zIn[i]&0x7f];
		zResult[0] = (char)SyToUpper(zIn[i]);
		for(j=1; j<4 && zIn[i]; i++){
			int code = iCode[zIn[i]&0x7f];
			if( code>0 ){
				if( code!=prevcode ){
					prevcode = (unsigned char)code;
					zResult[j++] = (char)code + '0';
				}
			}else{
				prevcode = 0;
			}
		}
		while( j<4 ){
			zResult[j++] = '0';
		}
		vedis_result_string(pCtx, zResult, 4);
	}else{
	  vedis_result_string(pCtx, "?000", 4);
	}
	return VEDIS_OK;
}
/*
 * Command: SIZE_FMT int_size
 *  Return a smart string represenation of the given size [i.e: 64-bit integer]
 *  Example:
 *     size_format(1*1024*1024*1024);// 1GB
 *     size_format(512*1024*1024); // 512 MB
 *     size_format(file_size(/path/to/my/file_8192)); //8KB
 * Parameter
 *  size
 *    Entity size in bytes.
 * Return
 *   String: Formatted string representation of the given size.
 */
static int vedis_cmd_size_format(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	/*Kilo*/ /*Mega*/ /*Giga*/ /*Tera*/ /*Peta*/ /*Exa*/ /*Zeta*/
	static const char zUnit[] = {"KMGTPEZ"};
	sxi32 nRest, i_32;
	vedis_int64 iSize;
	int c = -1; /* index in zUnit[] */

	if( nArg < 1 ){
		/* Missing argument, return the empty string */
		vedis_result_string(pCtx, "", 0);
		return VEDIS_OK;
	}
	/* Extract the given size */
	iSize = vedis_value_to_int64(apArg[0]);
	if( iSize < 100 /* Bytes */ ){
		/* Don't bother formatting, return immediately */
		vedis_result_string(pCtx, "0.1 KB", (int)sizeof("0.1 KB")-1);
		return VEDIS_OK;
	}
	for(;;){
		nRest = (sxi32)(iSize & 0x3FF); 
		iSize >>= 10;
		c++;
		if( (iSize & (~0 ^ 1023)) == 0 ){
			break;
		}
	}
	nRest /= 100;
	if( nRest > 9 ){
		nRest = 9;
	}
	if( iSize > 999 ){
		c++;
		nRest = 9;
		iSize = 0;
	}
	i_32 = (sxi32)iSize;
	/* Format */
	vedis_result_string_format(pCtx, "%d.%d %cB", i_32, nRest, zUnit[c]);
	return VEDIS_OK;
}
#ifdef VEDIS_ENABLE_HASH_CMD
/*
 * Binary to hex consumer callback.
 * This callback is the default consumer used by the hash functions
 * [i.e: md5(), sha1(), ... ] defined below.
 */
static int HashConsumer(const void *pData, unsigned int nLen, void *pUserData)
{
	/* Append hex chunk verbatim */
	vedis_result_string((vedis_context *)pUserData, (const char *)pData, (int)nLen);
	return SXRET_OK;
}
/*
 * Command:  MD5 string
 *   Calculate the md5 hash of a string.
 * Parameter
 *  string
 *   Input string
 * Return
 *  String: MD5 Hash as a 32-character hexadecimal string.
 */
static int vedis_cmd_md5(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	unsigned char zDigest[16];
	const void *pIn;
	int nLen;
	if( nArg < 1 ){
		/* Missing arguments, return the empty string */
		vedis_result_string(pCtx, "", 0);
		return VEDIS_OK;
	}
	/* Extract the input string */
	pIn = (const void *)vedis_value_to_string(apArg[0], &nLen);
	if( nLen < 1 ){
		/* Empty string */
		vedis_result_string(pCtx, "", 0);
		return VEDIS_OK;
	}
	/* Compute the MD5 digest */
	SyMD5Compute(pIn, (sxu32)nLen, zDigest);
	/* Perform a binary to hex conversion */
	SyBinToHexConsumer((const void *)zDigest, sizeof(zDigest), HashConsumer, pCtx);
	return VEDIS_OK;
}
/*
 * Command: SHA1 string 
 *   Calculate the sha1 hash of a string.
 * Parameter
 *  string
 *   Input string
 * Return
 *  String: SHA1 Hash as a 40-character hexadecimal string.
 */
static int vedis_cmd_sha1(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	unsigned char zDigest[20];
	const void *pIn;
	int nLen;
	if( nArg < 1 ){
		/* Missing arguments, return the empty string */
		vedis_result_string(pCtx, "", 0);
		return VEDIS_OK;
	}
	/* Extract the input string */
	pIn = (const void *)vedis_value_to_string(apArg[0], &nLen);
	if( nLen < 1 ){
		/* Empty string */
		vedis_result_string(pCtx, "", 0);
		return VEDIS_OK;
	}
	/* Compute the SHA1 digest */
	SySha1Compute(pIn, (sxu32)nLen, zDigest);
	/* Perform a binary to hex conversion */
	SyBinToHexConsumer((const void *)zDigest, sizeof(zDigest), HashConsumer, pCtx);
	return VEDIS_OK;
}
/*
 * Command: CRC32 string
 *   Calculates the crc32 polynomial of a strin.
 * Parameter
 *  $str
 *   Input string
 * Return
 *  64-bit Integer: CRC32 checksum of the given input (64-bit integer).
 */
static int vedis_cmd_crc32(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	const void *pIn;
	sxu32 nCRC;
	int nLen;
	if( nArg < 1 ){
		/* Missing arguments, return 0 */
		vedis_result_int(pCtx, 0);
		return VEDIS_OK;
	}
	/* Extract the input string */
	pIn = (const void *)vedis_value_to_string(apArg[0], &nLen);
	if( nLen < 1 ){
		/* Empty string */
		vedis_result_int(pCtx, 0);
		return VEDIS_OK;
	}
	/* Calculate the sum */
	nCRC = SyCrc32(pIn, (sxu32)nLen);
	/* Return the CRC32 as 64-bit integer */
	vedis_result_int64(pCtx, (vedis_int64)nCRC^ 0xFFFFFFFF);
	return VEDIS_OK;
}
#endif /* VEDIS_ENABLE_HASH_CMD */
/*
 * Parse a CSV string and invoke the supplied callback for each processed xhunk.
 */
static sxi32 vedisProcessCsv(
	const char *zInput, /* Raw input */
	int nByte,  /* Input length */
	int delim,  /* Delimiter */
	int encl,   /* Enclosure */
	int escape,  /* Escape character */
	sxi32 (*xConsumer)(const char *, int, void *), /* User callback */
	void *pUserData /* Last argument to xConsumer() */
	)
{
	const char *zEnd = &zInput[nByte];
	const char *zIn = zInput;
	const char *zPtr;
	int isEnc;
	/* Start processing */
	for(;;){
		if( zIn >= zEnd ){
			/* No more input to process */
			break;
		}
		isEnc = 0;
		zPtr = zIn;
		/* Find the first delimiter */
		while( zIn < zEnd ){
			if( zIn[0] == delim && !isEnc){
				/* Delimiter found, break imediately */
				break;
			}else if( zIn[0] == encl ){
				/* Inside enclosure? */
				isEnc = !isEnc;
			}else if( zIn[0] == escape ){
				/* Escape sequence */
				zIn++;
			}
			/* Advance the cursor */
			zIn++;
		}
		if( zIn > zPtr ){
			int nByte = (int)(zIn-zPtr);
			sxi32 rc;
			/* Invoke the supllied callback */
			if( zPtr[0] == encl ){
				zPtr++;
				nByte-=2;
			}
			if( nByte > 0 ){
				rc = xConsumer(zPtr, nByte, pUserData);
				if( rc == SXERR_ABORT ){
					/* User callback request an operation abort */
					break;
				}
			}
		}
		/* Ignore trailing delimiter */
		while( zIn < zEnd && zIn[0] == delim ){
			zIn++;
		}
	}
	return SXRET_OK;
}
/*
 * Default consumer callback for the CSV parsing routine defined above.
 * All the processed input is insereted into an array passed as the last
 * argument to this callback.
 */
static sxi32 vedisCsvConsumer(const char *zToken, int nTokenLen, void *pUserData)
{
	vedis_value *pArray = (vedis_value *)pUserData;
	vedis_value sEntry;
	SyString sToken;
	/* Insert the token in the given array */
	SyStringInitFromBuf(&sToken, zToken, nTokenLen);
	/* Remove trailing and leading white spcaces and null bytes */
	SyStringFullTrimSafe(&sToken);
	if( sToken.nByte < 1){
		return SXRET_OK;
	}
	vedisMemObjInitFromString(vedisHashmapGetEngine((vedis_hashmap *)pArray->x.pOther), &sEntry, &sToken);
	vedis_array_insert(pArray, &sEntry);
	vedisMemObjRelease(&sEntry);
	return SXRET_OK;
}
/*
 * Command: GETCSV input
 *  Parse a CSV string into a array.
 * Parameters
 *  $input
 *   The string to parse.
 *  $delimiter
 *   Set the field delimiter (one character only).
 *  $enclosure
 *   Set the field enclosure character (one character only).
 *  $escape
 *   Set the escape character (one character only). Defaults as a backslash (\)
 * Return
 *  Array: An indexed array containing the CSV fields or NULL on failure.
 */
static int vedis_cmd_str_getcsv(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	const char *zInput, *zPtr;
	vedis_value *pArray;
	int delim  = ',';   /* Delimiter */
	int encl   = '"' ;  /* Enclosure */
	int escape = '\\';  /* Escape character */
	int nLen;
	if( nArg < 1 || !vedis_value_is_string(apArg[0]) ){
		/* Missing/Invalid arguments, return NULL */
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Extract the raw input */
	zInput = vedis_value_to_string(apArg[0], &nLen);
	if( nArg > 1 ){
		int i;
		if( vedis_value_is_string(apArg[1]) ){
			/* Extract the delimiter */
			zPtr = vedis_value_to_string(apArg[1], &i);
			if( i > 0 ){
				delim = zPtr[0];
			}
		}
		if( nArg > 2 ){
			if( vedis_value_is_string(apArg[2]) ){
				/* Extract the enclosure */
				zPtr = vedis_value_to_string(apArg[2], &i);
				if( i > 0 ){
					encl = zPtr[0];
				}
			}
			if( nArg > 3 ){
				if( vedis_value_is_string(apArg[3]) ){
					/* Extract the escape character */
					zPtr = vedis_value_to_string(apArg[3], &i);
					if( i > 0 ){
						escape = zPtr[0];
					}
				}
			}
		}
	}
	/* Create our array */
	pArray = vedis_context_new_array(pCtx);
	if( pArray == 0 ){
		vedis_context_throw_error(pCtx, VEDIS_CTX_ERR, "VEDIS is running out of memory");
		vedis_result_null(pCtx);
		return VEDIS_OK;
	}
	/* Parse the raw input */
	vedisProcessCsv(zInput, nLen, delim, encl, escape, vedisCsvConsumer, pArray);
	/* Return the freshly created array */
	vedis_result_value(pCtx, pArray);
	return VEDIS_OK;
}
/*
 * Extract a tag name from a raw HTML input and insert it in the given
 * container.
 * Refer to [strip_tags()].
 */
static sxi32 AddTag(SySet *pSet, const char *zTag, int nByte)
{
	const char *zEnd = &zTag[nByte];
	const char *zPtr;
	SyString sEntry;
	/* Strip tags */
	for(;;){
		while( zTag < zEnd && (zTag[0] == '<' || zTag[0] == '/' || zTag[0] == '?'
			|| zTag[0] == '!' || zTag[0] == '-' || ((unsigned char)zTag[0] < 0xc0 && SyisSpace(zTag[0]))) ){
				zTag++;
		}
		if( zTag >= zEnd ){
			break;
		}
		zPtr = zTag;
		/* Delimit the tag */
		while(zTag < zEnd ){
			if( (unsigned char)zTag[0] >= 0xc0 ){
				/* UTF-8 stream */
				zTag++;
				SX_JMP_UTF8(zTag, zEnd);
			}else if( !SyisAlphaNum(zTag[0]) ){
				break;
			}else{
				zTag++;
			}
		}
		if( zTag > zPtr ){
			/* Perform the insertion */
			SyStringInitFromBuf(&sEntry, zPtr, (int)(zTag-zPtr));
			SyStringFullTrim(&sEntry);
			SySetPut(pSet, (const void *)&sEntry);
		}
		/* Jump the trailing '>' */
		zTag++;
	}
	return SXRET_OK;
}
/*
 * Check if the given HTML tag name is present in the given container.
 * Return SXRET_OK if present.SXERR_NOTFOUND otherwise.
 * Refer to [strip_tags()].
 */
static sxi32 FindTag(SySet *pSet, const char *zTag, int nByte)
{
	if( SySetUsed(pSet) > 0 ){
		const char *zCur, *zEnd = &zTag[nByte];
		SyString sTag;
		while( zTag < zEnd &&  (zTag[0] == '<' || zTag[0] == '/' || zTag[0] == '?' ||
			((unsigned char)zTag[0] < 0xc0 && SyisSpace(zTag[0]))) ){
			zTag++;
		}
		/* Delimit the tag */
		zCur = zTag;
		while(zTag < zEnd ){
			if( (unsigned char)zTag[0] >= 0xc0 ){
				/* UTF-8 stream */
				zTag++;
				SX_JMP_UTF8(zTag, zEnd);
			}else if( !SyisAlphaNum(zTag[0]) ){
				break;
			}else{
				zTag++;
			}
		}
		SyStringInitFromBuf(&sTag, zCur, zTag-zCur);
		/* Trim leading white spaces and null bytes */
		SyStringLeftTrimSafe(&sTag);
		if( sTag.nByte > 0 ){
			SyString *aEntry, *pEntry;
			sxi32 rc;
			sxu32 n;
			/* Perform the lookup */
			aEntry = (SyString *)SySetBasePtr(pSet);
			for( n = 0 ; n < SySetUsed(pSet) ; ++n ){
				pEntry = &aEntry[n];
				/* Do the comparison */
				rc = SyStringCmp(pEntry, &sTag, SyStrnicmp);
				if( !rc ){
					return SXRET_OK;
				}
			}
		}
	}
	/* No such tag */
	return SXERR_NOTFOUND;
}
/*
 * This function tries to return a string [i.e: in the call context result buffer]
 * with all NUL bytes, HTML and VEDIS tags stripped from a given string.
 * Refer to [strip_tags()].
 */
static sxi32 vedisStripTagsFromString(vedis_context *pCtx, const char *zIn, int nByte, const char *zTaglist, int nTaglen)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	const char *zEnd = &zIn[nByte];
	const char *zPtr, *zTag;
	SySet sSet;
	/* initialize the set of allowed tags */
	SySetInit(&sSet, &pStore->sMem, sizeof(SyString));
	if( nTaglen > 0 ){
		/* Set of allowed tags */
		AddTag(&sSet, zTaglist, nTaglen);
	}
	/* Set the empty string */
	vedis_result_string(pCtx, "", 0);
	/* Start processing */
	for(;;){
		if(zIn >= zEnd){
			/* No more input to process */
			break;
		}
		zPtr = zIn;
		/* Find a tag */
		while( zIn < zEnd && zIn[0] != '<' && zIn[0] != 0 /* NUL byte */ ){
			zIn++;
		}
		if( zIn > zPtr ){
			/* Consume raw input */
			vedis_result_string(pCtx, zPtr, (int)(zIn-zPtr));
		}
		/* Ignore trailing null bytes */
		while( zIn < zEnd && zIn[0] == 0 ){
			zIn++;
		}
		if(zIn >= zEnd){
			/* No more input to process */
			break;
		}
		if( zIn[0] == '<' ){
			sxi32 rc;
			zTag = zIn++;
			/* Delimit the tag */
			while( zIn < zEnd && zIn[0] != '>' ){
				zIn++;
			}
			if( zIn < zEnd ){
				zIn++; /* Ignore the trailing closing tag */
			}
			/* Query the set */
			rc = FindTag(&sSet, zTag, (int)(zIn-zTag));
			if( rc == SXRET_OK ){
				/* Keep the tag */
				vedis_result_string(pCtx, zTag, (int)(zIn-zTag));
			}
		}
	}
	/* Cleanup */
	SySetRelease(&sSet);
	return SXRET_OK;
}
/*
 * Command: STRIP_TAG string [allowable_tags]
 *   Strip HTML tags from a string.
 * Parameters
 * str
 *  The input string.
 * allowable_tags
 *  You can use the optional second parameter to specify tags which should not be stripped. 
 * Return
 *  String: Returns the stripped string.
 */
static int vedis_cmd_strip_tags(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	const char *zTaglist = 0;
	const char *zString;
	int nTaglen = 0;
	int nLen;
	if( nArg < 1 || !vedis_value_is_string(apArg[0]) ){
		/* Missing/Invalid arguments, return the empty string */
		vedis_result_string(pCtx, "", 0);
		return VEDIS_OK;
	}
	/* Point to the raw string */
	zString = vedis_value_to_string(apArg[0], &nLen);
	if( nArg > 1 && vedis_value_is_string(apArg[1]) ){
		/* Allowed tag */
		zTaglist = vedis_value_to_string(apArg[1], &nTaglen);
	}
	/* Process input */
	vedisStripTagsFromString(pCtx, zString, nLen, zTaglist, nTaglen);
	return VEDIS_OK;
}
/*
 * Command:  STR_SPLIT string, [$split_length = 1 ]
 *  Split a string into an indexed array.
 * Parameters
 * $str
 *  The input string.
 * $split_length
 *  Maximum length of the chunk.
 * Return
 *  Array: If the optional split_length parameter is specified, the returned array
 *  will be broken down into chunks with each being split_length in length, otherwise
 *  each chunk will be one character in length. FALSE is returned if split_length is less than 1.
 *  If the split_length length exceeds the length of string, the entire string is returned 
 *  as the first (and only) array element.
 */
static int vedis_cmd_str_split(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	const char *zString, *zEnd;
	vedis_value *pArray, *pValue;
	int split_len;
	int nLen;
	if( nArg < 1 ){
		/* Missing arguments, return FALSE */
		vedis_result_bool(pCtx, 0);
		return VEDIS_OK;
	}
	/* Point to the target string */
	zString = vedis_value_to_string(apArg[0], &nLen);
	if( nLen < 1 ){
		/* Nothing to process, return FALSE */
		vedis_result_bool(pCtx, 0);
		return VEDIS_OK;
	}
	split_len = (int)sizeof(char);
	if( nArg > 1 ){
		/* Split length */
		split_len = vedis_value_to_int(apArg[1]);
		if( split_len < 1 ){
			/* Invalid length, return FALSE */
			vedis_result_bool(pCtx, 0);
			return VEDIS_OK;
		}
		if( split_len > nLen ){
			split_len = nLen;
		}
	}
	/* Create the array and the scalar value */
	pArray = vedis_context_new_array(pCtx);
	/*Chunk value */
	pValue = vedis_context_new_scalar(pCtx);
	if( pValue == 0 || pArray == 0 ){
		/* Return FALSE */
		vedis_result_bool(pCtx, 0);
		return VEDIS_OK;
	}
	/* Point to the end of the string */
	zEnd = &zString[nLen];
	/* Perform the requested operation */
	for(;;){
		int nMax;
		if( zString >= zEnd ){
			/* No more input to process */
			break;
		}
		nMax = (int)(zEnd-zString);
		if( nMax < split_len ){
			split_len = nMax;
		}
		/* Copy the current chunk */
		vedis_value_string(pValue, zString, split_len);
		/* Insert it */
		vedis_array_insert(pArray, pValue); /* Will make it's own copy */
		/* reset the string cursor */
		vedis_value_reset_string_cursor(pValue);
		/* Update position */
		zString += split_len;
	}
	/* 
	 * Return the array.
	 * Don't worry about freeing memory, everything will be automatically released
	 * upon we return from this function.
	 */
	vedis_result_value(pCtx, pArray);
	return VEDIS_OK;
}
#ifdef __WINNT__
#include <Windows.h>
#else
#include <time.h>
#endif
/*
 *  Command: TIME
 *   Expand the current time (GMT).
 * Return:
 *  String: Formatted time (HH:MM:SS)
 */
static int vedis_cmd_time(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	Sytm sTm;
#ifdef __WINNT__
	SYSTEMTIME sOS;
	GetSystemTime(&sOS);
	SYSTEMTIME_TO_SYTM(&sOS, &sTm);
#else
	struct tm *pTm;
	time_t t;
	time(&t);
	pTm = gmtime(&t);
	STRUCT_TM_TO_SYTM(pTm, &sTm);
#endif
	SXUNUSED(nArg); /* cc warning */
	SXUNUSED(apArg);
	/* Expand */
	vedis_result_string_format(pCtx, "%02d:%02d:%02d", sTm.tm_hour, sTm.tm_min, sTm.tm_sec);
	return VEDIS_OK;
}
/*
 *  Command: DATE
 *   Expand the current date in the ISO-8601 format
 * Return:
 *  String: ISO-8601 date format.
 */
static int vedis_cmd_date(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	Sytm sTm;
#ifdef __WINNT__
	SYSTEMTIME sOS;
	GetSystemTime(&sOS);
	SYSTEMTIME_TO_SYTM(&sOS, &sTm);
#else
	struct tm *pTm;
	time_t t;
	time(&t);
	pTm = gmtime(&t);
	STRUCT_TM_TO_SYTM(pTm, &sTm);
#endif
	SXUNUSED(nArg); /* cc warning */
	SXUNUSED(apArg);
	/* Expand */
	vedis_result_string_format(pCtx, "%04d-%02d-%02d", sTm.tm_year, sTm.tm_mon+1, sTm.tm_mday);
	return VEDIS_OK;
}
#if defined(__UNIXES__)
#include <sys/utsname.h>
#endif
/*
 *  Command: OS
 *   Expand the name of the host Operating System
 * Return:
 *  String: OS name.
 */
static int vedis_cmd_os(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
#if defined(__WINNT__)
	const char *zName = "Microsoft Windows";
	OSVERSIONINFOW sVer;
	sVer.dwOSVersionInfoSize = sizeof(sVer);
	if( TRUE != GetVersionExW(&sVer)){
		vedis_result_string(pCtx, zName, -1);
		return VEDIS_OK;
	}
	if( sVer.dwPlatformId == VER_PLATFORM_WIN32_NT ){
		if( sVer.dwMajorVersion <= 4 ){
			zName = "Microsoft Windows NT";
		}else if( sVer.dwMajorVersion == 5 ){
			switch(sVer.dwMinorVersion){
				case 0:	zName = "Microsoft Windows 2000"; break;
				case 1: zName = "Microsoft Windows XP";   break;
				case 2: zName = "Microsoft Windows Server 2003"; break;
			}
		}else if( sVer.dwMajorVersion == 6){
				switch(sVer.dwMinorVersion){
					case 0: zName = "Microsoft Windows Vista"; break;
					case 1: zName = "Microsoft Windows 7"; break;
					case 2: zName = "Microsoft Windows 8"; break;
					default: break;
				}
		}
	}
	vedis_result_string_format(pCtx, "%s localhost %u.%u build %u x86", 
			zName, 
			sVer.dwMajorVersion, sVer.dwMinorVersion, sVer.dwBuildNumber
			);
#elif defined(__UNIXES__)
	struct utsname sInfo;
	if( uname(&sInfo) != 0 ){
		vedis_result_string(pCtx, "Unix", (int)sizeof("Unix")-1);
	}else{
		vedis_result_string(pCtx, sInfo.sysname, -1);
	}
#else
	vedis_result_string(pCtx,"Host OS", (int)sizeof("Host OS")-1);
#endif
	SXUNUSED(nArg); /* cc warning */
	SXUNUSED(apArg);
	return VEDIS_OK;
}
/*
 *  Command: VEDIS
 *   Expand the vedis signature and copyright notice.
 * Return:
 *  String: Vedis signature and copyright notice.
 */
static int vedis_cmd_credits(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	SXUNUSED(nArg); /* cc warning */
	SXUNUSED(apArg);
	/* Expand */
	vedis_result_string(pCtx,VEDIS_SIG " " VEDIS_COPYRIGHT,-1);
	return VEDIS_OK;
}
/*
 *  Command: ECHO,PRINT
 *   Return the given argument.
 * Return:
 *  String: Given argument.
 */
static int vedis_cmd_echo(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	if( nArg > 0 ){
		/* Expand */
		vedis_result_value(pCtx,apArg[0]);
	}
	return VEDIS_OK;
}
/*
 *  Command: ABORT
 *   Throw an error message and abort execution.
 * Return:
 *  nil
 */
static int vedis_cmd_abort(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"User request an operation abort");
	SXUNUSED(nArg); /* cc wanring */
	SXUNUSED(apArg);
	return VEDIS_ABORT; /* Abort execution */
}
/*
 *  Command: CMD_LIST
 *   Return an indexed array holding the list of installed vedis commands.
 * Return:
 *  Array: Array of installed vedis commands.
 */
static int vedis_cmd_c_list(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	vedis_value *pArray,*pScalar;
	vedis_cmd *pCmd;
	sxu32 n;
	
	/* Allocate a new scalar and array */
	pScalar = vedis_context_new_scalar(pCtx);
	pArray = vedis_context_new_array(pCtx);
	if( pScalar == 0 || pArray == 0 ){
		vedis_context_throw_error(pCtx,VEDIS_CTX_ERR,"Out of memory");
		/* return null */
		vedis_result_null(pCtx);
		SXUNUSED(nArg); /* cc warning */
		SXUNUSED(apArg);
		return VEDIS_OK;
	}
	pCmd = pStore->pList;
	for( n = 0 ; n < pStore->nCmd; ++n ){
		vedis_value_reset_string_cursor(pScalar);
		/* Copy the command name */
		vedis_value_string(pScalar,SyStringData(&pCmd->sName),(int)SyStringLength(&pCmd->sName));
		/* Perform the insertion */
		vedis_array_insert(pArray,pScalar);
		/* Point to the next entry */
		pCmd = pCmd->pNext;
	}
	/* Return our array */
	vedis_result_value(pCtx,pArray);
	/* pScalar will be automatically destroyed */
	return VEDIS_OK;
}
/*
 *  Command: COMMIT
 *   Commit an active write transaction.
 * Return:
 *  Boolean: TRUE on success. FALSE otherwise.
 */
static int vedis_cmd_commit(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	int rc;
	SXUNUSED(nArg); /*cc warning */
	SXUNUSED(apArg);
	rc = vedisPagerCommit(pStore->pPager);
	/* Result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command: ROLLBACK
 *   Rollback an active write transaction.
 * Return:
 *  Boolean: TRUE on success. FALSE otherwise.
 */
static int vedis_cmd_rollback(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	int rc;
	SXUNUSED(nArg); /*cc warning */
	SXUNUSED(apArg);
	rc = vedisPagerRollback(pStore->pPager,TRUE);
	/* Result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 *  Command: BEGIN
 *   Start a write transaction.
 * Return:
 *  Boolean: TRUE on success. FALSE otherwise.
 */
static int vedis_cmd_begin(vedis_context *pCtx, int nArg, vedis_value **apArg)
{
	vedis *pStore = (vedis *)vedis_context_user_data(pCtx);
	int rc;
	SXUNUSED(nArg); /*cc warning */
	SXUNUSED(apArg);
	rc = vedisPagerBegin(pStore->pPager);
	/* Result */
	vedis_result_bool(pCtx,rc == VEDIS_OK);
	return VEDIS_OK;
}
/*
 * Register the built-in Vedis command defined above.
 */
VEDIS_PRIVATE int vedisRegisterBuiltinCommands(vedis *pVedis)
{
	static const struct vedis_built_command {
		const char *zName; /* Command name */
		ProcVedisCmd xCmd; /* Implementation of the command */
	}aCmd[] = {
		{ "DEL",       vedis_cmd_del },
		{ "REMOVE",    vedis_cmd_del },
		{ "EXISTS",    vedis_cmd_exists },
		{ "APPEND",    vedis_cmd_append },
		{ "STRLEN",    vedis_cmd_strlen },
		{ "GET",       vedis_cmd_get    },
		{ "COPY",      vedis_cmd_copy   },
		{ "MOVE",      vedis_cmd_move   },
		{ "MGET",      vedis_cmd_mget   },
		{ "SET",       vedis_cmd_set    },
		{ "SETNX",     vedis_cmd_setnx  },
		{ "MSET",      vedis_cmd_mset   },
		{ "MSETNX",    vedis_cmd_msetnx },
		{ "GETSET",    vedis_cmd_getset },
		{ "INCR",      vedis_cmd_incr   },
		{ "DECR",      vedis_cmd_decr   },
		{ "INCRBY",    vedis_cmd_incrby },
		{ "DECRBY",    vedis_cmd_decrby },
		{ "HGET",      vedis_cmd_hget   },
		{ "HEXISTS",   vedis_cmd_hexists},
		{ "HDEL",      vedis_cmd_hdel   },
		{ "HLEN",      vedis_cmd_hlen   },
		{ "HMGET",     vedis_cmd_hmget  },
		{ "HKEYS",     vedis_cmd_hkeys  },
		{ "HVALS",     vedis_cmd_hvals  }, 
		{ "HGETALL",   vedis_cmd_hgetall },
		{ "HSET",      vedis_cmd_hset   },
		{ "HMSET",     vedis_cmd_hmset  },
		{ "HSETNX",    vedis_cmd_hsetnx },
		{ "SADD",      vedis_cmd_sadd   },
		{ "SCARD",     vedis_cmd_scard  },
		{ "SISMEMBER", vedis_cmd_sismember },
		{ "SPOP",      vedis_cmd_spop   },
		{ "SPEEK",     vedis_cmd_speek  },
		{ "STOP",      vedis_cmd_stop   },
		{ "SREM",      vedis_cmd_srem   },
		{ "SMEMBERS",  vedis_cmd_smembers },
		{ "SDIFF",     vedis_cmd_sdiff  },
		{ "SINTER",    vedis_cmd_sinter },
		{ "SLEN",      vedis_cmd_slen   },
		{ "LINDEX",    vedis_cmd_lindex },
		{ "LLEN",      vedis_cmd_llen   },
		{ "LPOP",      vedis_cmd_lpop   },
		{ "LPUSH",     vedis_cmd_lpush  },
		{ "RAND",      vedis_cmd_rand   },
		{ "GETRANDMAX", vedis_cmd_getrandmax },
		{ "RANDSTR",    vedis_cmd_rand_str },
		{ "BASE64",     vedis_cmd_base64_encode },
		{ "BASE64_DEC", vedis_cmd_base64_decode },
		{ "SOUNDEX",    vedis_cmd_soundex  },
		{ "SIZE_FMT",   vedis_cmd_size_format },
#ifdef VEDIS_ENABLE_HASH_CMD
		{ "MD5",        vedis_cmd_md5 },
		{ "SHA1",       vedis_cmd_sha1 },
		{ "CRC32",      vedis_cmd_crc32 },
#endif /* VEDIS_ENABLE_HASH_CMD */
		{ "GETCSV",     vedis_cmd_str_getcsv },
		{ "STRIP_TAG",  vedis_cmd_strip_tags },
		{ "STR_SPLIT",  vedis_cmd_str_split  },
		{ "TIME",       vedis_cmd_time       },
		{ "DATE",       vedis_cmd_date       },
		{ "OS",         vedis_cmd_os         },
		{ "ECHO",       vedis_cmd_echo       },
		{ "PRINT",      vedis_cmd_echo       },
		{ "ABORT",      vedis_cmd_abort      },
		{ "CMD_LIST",   vedis_cmd_c_list     },
		{ "TABLE_LIST", vedis_cmd_table_list },
		{ "VEDIS",      vedis_cmd_credits    },
		{ "COMMIT",     vedis_cmd_commit     },
		{ "ROLLBACK",   vedis_cmd_rollback   },
		{ "BEGIN",      vedis_cmd_begin      },
	};
	int rc = VEDIS_OK;
	sxu32 n;
	for( n = 0 ; n < SX_ARRAYSIZE(aCmd); ++n ){
		/* Create the command */
		rc = vedis_register_command(pVedis,aCmd[n].zName,aCmd[n].xCmd,pVedis);
	}
	return rc;
}
/*
 * ----------------------------------------------------------
 * File: bitvec.c
 * MD5: dfd57c382edf589956568a2527d13e36
 * ----------------------------------------------------------
 */
/*
 * Symisc Vedis: An Embeddable NoSQL (Post Modern) Database Engine.
 * Copyright (C) 2012-2013, Symisc Systems http://vedis.org/
 * Version 1.1.6
 * For information on licensing, redistribution of this file, and for a DISCLAIMER OF ALL WARRANTIES
 * please contact Symisc Systems via:
 *       legal@symisc.net
 *       licensing@symisc.net
 *       contact@symisc.net
 * or visit:
 *      http://vedis.org/licensing.html
 */
 /* $SymiscID: bitvec.c v1.0 Win7 2013-02-27 15:16 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif

/** This file implements an object that represents a dynmaic
** bitmap.
**
** A bitmap is used to record which pages of a database file have been
** journalled during a transaction, or which pages have the "dont-write"
** property.  Usually only a few pages are meet either condition.
** So the bitmap is usually sparse and has low cardinality.
*/
/*
 * Actually, this is not a bitmap but a simple hashtable where page 
 * number (64-bit unsigned integers) are used as the lookup keys.
 */
typedef struct bitvec_rec bitvec_rec;
struct bitvec_rec
{
	pgno iPage;                  /* Page number */
	bitvec_rec *pNext,*pNextCol; /* Collison link */
};
struct Bitvec
{
	SyMemBackend *pAlloc; /* Memory allocator */
	sxu32 nRec;           /* Total number of records */
	sxu32 nSize;          /* Table size */
	bitvec_rec **apRec;   /* Record table */
	bitvec_rec *pList;    /* List of records */
};
/* 
 * Allocate a new bitvec instance.
*/
VEDIS_PRIVATE Bitvec * vedisBitvecCreate(SyMemBackend *pAlloc,pgno iSize)
{
	bitvec_rec **apNew;
	Bitvec *p;
	
	p = (Bitvec *)SyMemBackendAlloc(pAlloc,sizeof(*p) );
	if( p == 0 ){
		SXUNUSED(iSize); /* cc warning */
		return 0;
	}
	/* Zero the structure */
	SyZero(p,sizeof(Bitvec));
	/* Allocate a new table */
	p->nSize = 64; /* Must be a power of two */
	apNew = (bitvec_rec **)SyMemBackendAlloc(pAlloc,p->nSize * sizeof(bitvec_rec *));
	if( apNew == 0 ){
		SyMemBackendFree(pAlloc,p);
		return 0;
	}
	/* Zero the new table */
	SyZero((void *)apNew,p->nSize * sizeof(bitvec_rec *));
	/* Fill-in */
	p->apRec = apNew;
	p->pAlloc = pAlloc;
	return p;
}
/*
 * Check if the given page number is already installed in the table.
 * Return true if installed. False otherwise.
 */
VEDIS_PRIVATE int vedisBitvecTest(Bitvec *p,pgno i)
{  
	bitvec_rec *pRec;
	/* Point to the desired bucket */
	pRec = p->apRec[i & (p->nSize - 1)];
	for(;;){
		if( pRec == 0 ){ break; }
		if( pRec->iPage == i ){
			/* Page found */
			return 1;
		}
		/* Point to the next entry */
		pRec = pRec->pNextCol;

		if( pRec == 0 ){ break; }
		if( pRec->iPage == i ){
			/* Page found */
			return 1;
		}
		/* Point to the next entry */
		pRec = pRec->pNextCol;


		if( pRec == 0 ){ break; }
		if( pRec->iPage == i ){
			/* Page found */
			return 1;
		}
		/* Point to the next entry */
		pRec = pRec->pNextCol;


		if( pRec == 0 ){ break; }
		if( pRec->iPage == i ){
			/* Page found */
			return 1;
		}
		/* Point to the next entry */
		pRec = pRec->pNextCol;
	}
	/* No such entry */
	return 0;
}
/*
 * Install a given page number in our bitmap (Actually, our hashtable).
 */
VEDIS_PRIVATE int vedisBitvecSet(Bitvec *p,pgno i)
{
	bitvec_rec *pRec;
	sxi32 iBuck;
	/* Allocate a new instance */
	pRec = (bitvec_rec *)SyMemBackendPoolAlloc(p->pAlloc,sizeof(bitvec_rec));
	if( pRec == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pRec,sizeof(bitvec_rec));
	/* Fill-in */
	pRec->iPage = i;
	iBuck = i & (p->nSize - 1);
	pRec->pNextCol = p->apRec[iBuck];
	p->apRec[iBuck] = pRec;
	pRec->pNext = p->pList;
	p->pList = pRec;
	p->nRec++;
	if( p->nRec >= (p->nSize * 3) && p->nRec < 100000 ){
		/* Grow the hashtable */
		sxu32 nNewSize = p->nSize << 1;
		bitvec_rec *pEntry,**apNew;
		sxu32 n;
		apNew = (bitvec_rec **)SyMemBackendAlloc(p->pAlloc, nNewSize * sizeof(bitvec_rec *));
		if( apNew ){
			sxu32 iBucket;
			/* Zero the new table */
			SyZero((void *)apNew, nNewSize * sizeof(bitvec_rec *));
			/* Rehash all entries */
			n = 0;
			pEntry = p->pList;
			for(;;){
				/* Loop one */
				if( n >= p->nRec ){
					break;
				}
				pEntry->pNextCol = 0;
				/* Install in the new bucket */
				iBucket = pEntry->iPage & (nNewSize - 1);
				pEntry->pNextCol = apNew[iBucket];
				apNew[iBucket] = pEntry;
				/* Point to the next entry */
				pEntry = pEntry->pNext;
				n++;
			}
			/* Release the old table and reflect the change */
			SyMemBackendFree(p->pAlloc,(void *)p->apRec);
			p->apRec = apNew;
			p->nSize  = nNewSize;
		}
	}
	return VEDIS_OK;
}
/*
 * Destroy a bitvec instance. Reclaim all memory used.
 */
VEDIS_PRIVATE void vedisBitvecDestroy(Bitvec *p)
{
	bitvec_rec *pNext,*pRec = p->pList;
	SyMemBackend *pAlloc = p->pAlloc;
	
	for(;;){
		if( p->nRec < 1 ){
			break;
		}
		pNext = pRec->pNext;
		SyMemBackendPoolFree(pAlloc,(void *)pRec);
		pRec = pNext;
		p->nRec--;

		if( p->nRec < 1 ){
			break;
		}
		pNext = pRec->pNext;
		SyMemBackendPoolFree(pAlloc,(void *)pRec);
		pRec = pNext;
		p->nRec--;


		if( p->nRec < 1 ){
			break;
		}
		pNext = pRec->pNext;
		SyMemBackendPoolFree(pAlloc,(void *)pRec);
		pRec = pNext;
		p->nRec--;


		if( p->nRec < 1 ){
			break;
		}
		pNext = pRec->pNext;
		SyMemBackendPoolFree(pAlloc,(void *)pRec);
		pRec = pNext;
		p->nRec--;
	}
	SyMemBackendFree(pAlloc,(void *)p->apRec);
	SyMemBackendFree(pAlloc,p);
}
/*
 * ----------------------------------------------------------
 * File: api.c
 * MD5: 8fb4f708f4ac6f1e2b766bbdc73137f1
 * ----------------------------------------------------------
 */
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
/* $SymiscID: api.c v2.0 FreeBSD 2012-11-08 23:07 stable <chm@symisc.net> $ */
#ifndef VEDIS_AMALGAMATION
#include "vedisInt.h"
#endif
/* This file implement the public interfaces presented to host-applications.
 * Routines in other files are for internal use by Vedis and should not be
 * accessed by users of the library.
 */
#define VEDIS_DB_MISUSE(DB) (DB == 0 || DB->nMagic != VEDIS_DB_MAGIC)
/* If another thread have released a working instance, the following macros
 * evaluates to true. These macros are only used when the library
 * is built with threading support enabled.
 */
#define VEDIS_THRD_DB_RELEASE(DB) (DB->nMagic != VEDIS_DB_MAGIC)
/* IMPLEMENTATION: vedis@embedded@symisc 115-04-1213 */
/*
 * All global variables are collected in the structure named "sVedisMPGlobal".
 * That way it is clear in the code when we are using static variable because
 * its name start with sVedisMPGlobal.
 */
static struct vedisGlobal_Data
{
	SyMemBackend sAllocator;                /* Global low level memory allocator */
#if defined(VEDIS_ENABLE_THREADS)
	const SyMutexMethods *pMutexMethods;   /* Mutex methods */
	SyMutex *pMutex;                       /* Global mutex */
	sxu32 nThreadingLevel;                 /* Threading level: 0 == Single threaded/1 == Multi-Threaded 
										    * The threading level can be set using the [vedis_lib_config()]
											* interface with a configuration verb set to
											* VEDIS_LIB_CONFIG_THREAD_LEVEL_SINGLE or 
											* VEDIS_LIB_CONFIG_THREAD_LEVEL_MULTI
											*/
#endif
	SySet kv_storage;                       /* Installed KV storage engines */
	int iPageSize;                          /* Default Page size */
	vedis_vfs *pVfs;                        /* Underlying virtual file system (Vfs) */
	sxi32 nStore;                           /* Total number of active Vedis engines */
	vedis *pStore;                          /* List of active engines */
	sxu32 nMagic;                           /* Sanity check against library misuse */
}sVedisMPGlobal = {
	{0, 0, 0, 0, 0, 0, 0, 0, {0}}, 
#if defined(VEDIS_ENABLE_THREADS)
	0, 
	0, 
	0, 
#endif
	{0, 0, 0, 0, 0, 0, 0 },
	VEDIS_DEFAULT_PAGE_SIZE,
	0, 
	0, 
	0, 
	0
};
#define VEDIS_LIB_MAGIC  0xAB1495DB
#define VEDIS_LIB_MISUSE (sVedisMPGlobal.nMagic != VEDIS_LIB_MAGIC)
/*
 * Supported threading level.
 * These options have meaning only when the library is compiled with multi-threading
 * support. That is, the VEDIS_ENABLE_THREADS compile time directive must be defined
 * when Vedis is built.
 * VEDIS_THREAD_LEVEL_SINGLE:
 *  In this mode, mutexing is disabled and the library can only be used by a single thread.
 * VEDIS_THREAD_LEVEL_MULTI
 *  In this mode, all mutexes including the recursive mutexes on [vedis] objects
 *  are enabled so that the application is free to share the same engine
 *  between different threads at the same time.
 */
#define VEDIS_THREAD_LEVEL_SINGLE 1 
#define VEDIS_THREAD_LEVEL_MULTI  2
/*
 * Find a Key Value storage engine from the set of installed engines.
 * Return a pointer to the storage engine methods on success. NULL on failure.
 */
VEDIS_PRIVATE vedis_kv_methods * vedisFindKVStore(
	const char *zName, /* Storage engine name [i.e. Hash, B+tree, LSM, etc.] */
	sxu32 nByte /* zName length */
	)
{
	vedis_kv_methods **apStore,*pEntry;
	sxu32 n,nMax;
	/* Point to the set of installed engines */
	apStore = (vedis_kv_methods **)SySetBasePtr(&sVedisMPGlobal.kv_storage);
	nMax = SySetUsed(&sVedisMPGlobal.kv_storage);
	for( n = 0 ; n < nMax; ++n ){
		pEntry = apStore[n];
		if( nByte == SyStrlen(pEntry->zName) && SyStrnicmp(pEntry->zName,zName,nByte) == 0 ){
			/* Storage engine found */
			return pEntry;
		}
	}
	/* No such entry, return NULL */
	return 0;
}
/*
 * Configure the Vedis library.
 * Return VEDIS_OK on success. Any other return value indicates failure.
 * Refer to [vedis_lib_config()].
 */
static sxi32 vedisCoreConfigure(sxi32 nOp, va_list ap)
{
	int rc = VEDIS_OK;
	switch(nOp){
	    case VEDIS_LIB_CONFIG_PAGE_SIZE: {
			/* Default page size: Must be a power of two */
			int iPage = va_arg(ap,int);
			if( iPage >= VEDIS_MIN_PAGE_SIZE && iPage <= VEDIS_MAX_PAGE_SIZE ){
				if( !(iPage & (iPage - 1)) ){
					sVedisMPGlobal.iPageSize = iPage;
				}else{
					/* Invalid page size */
					rc = VEDIS_INVALID;
				}
			}else{
				/* Invalid page size */
				rc = VEDIS_INVALID;
			}
			break;
										   }
	    case VEDIS_LIB_CONFIG_STORAGE_ENGINE: {
			/* Install a key value storage engine */
			vedis_kv_methods *pMethods = va_arg(ap,vedis_kv_methods *);
			/* Make sure we are delaing with a valid methods */
			if( pMethods == 0 || SX_EMPTY_STR(pMethods->zName) || pMethods->xSeek == 0 || pMethods->xData == 0
				|| pMethods->xKey == 0 || pMethods->xDataLength == 0 || pMethods->xKeyLength == 0 
				|| pMethods->szKv < (int)sizeof(vedis_kv_engine) ){
					rc = VEDIS_INVALID;
					break;
			}
			/* Install it */
			rc = SySetPut(&sVedisMPGlobal.kv_storage,(const void *)&pMethods);
			break;
												}
	    case VEDIS_LIB_CONFIG_VFS:{
			/* Install a virtual file system */
			vedis_vfs *pVfs = va_arg(ap,vedis_vfs *);
			if( pVfs ){
			 sVedisMPGlobal.pVfs = pVfs;
			}
			break;
								}
		case VEDIS_LIB_CONFIG_USER_MALLOC: {
			/* Use an alternative low-level memory allocation routines */
			const SyMemMethods *pMethods = va_arg(ap, const SyMemMethods *);
			/* Save the memory failure callback (if available) */
			ProcMemError xMemErr = sVedisMPGlobal.sAllocator.xMemError;
			void *pMemErr = sVedisMPGlobal.sAllocator.pUserData;
			if( pMethods == 0 ){
				/* Use the built-in memory allocation subsystem */
				rc = SyMemBackendInit(&sVedisMPGlobal.sAllocator, xMemErr, pMemErr);
			}else{
				rc = SyMemBackendInitFromOthers(&sVedisMPGlobal.sAllocator, pMethods, xMemErr, pMemErr);
			}
			break;
										  }
		case VEDIS_LIB_CONFIG_MEM_ERR_CALLBACK: {
			/* Memory failure callback */
			ProcMemError xMemErr = va_arg(ap, ProcMemError);
			void *pUserData = va_arg(ap, void *);
			sVedisMPGlobal.sAllocator.xMemError = xMemErr;
			sVedisMPGlobal.sAllocator.pUserData = pUserData;
			break;
												 }	  
		case VEDIS_LIB_CONFIG_USER_MUTEX: {
#if defined(VEDIS_ENABLE_THREADS)
			/* Use an alternative low-level mutex subsystem */
			const SyMutexMethods *pMethods = va_arg(ap, const SyMutexMethods *);
#if defined (UNTRUST)
			if( pMethods == 0 ){
				rc = VEDIS_CORRUPT;
			}
#endif
			/* Sanity check */
			if( pMethods->xEnter == 0 || pMethods->xLeave == 0 || pMethods->xNew == 0){
				/* At least three criticial callbacks xEnter(), xLeave() and xNew() must be supplied */
				rc = VEDIS_CORRUPT;
				break;
			}
			if( sVedisMPGlobal.pMutexMethods ){
				/* Overwrite the previous mutex subsystem */
				SyMutexRelease(sVedisMPGlobal.pMutexMethods, sVedisMPGlobal.pMutex);
				if( sVedisMPGlobal.pMutexMethods->xGlobalRelease ){
					sVedisMPGlobal.pMutexMethods->xGlobalRelease();
				}
				sVedisMPGlobal.pMutex = 0;
			}
			/* Initialize and install the new mutex subsystem */
			if( pMethods->xGlobalInit ){
				rc = pMethods->xGlobalInit();
				if ( rc != VEDIS_OK ){
					break;
				}
			}
			/* Create the global mutex */
			sVedisMPGlobal.pMutex = pMethods->xNew(SXMUTEX_TYPE_FAST);
			if( sVedisMPGlobal.pMutex == 0 ){
				/*
				 * If the supplied mutex subsystem is so sick that we are unable to
				 * create a single mutex, there is no much we can do here.
				 */
				if( pMethods->xGlobalRelease ){
					pMethods->xGlobalRelease();
				}
				rc = VEDIS_CORRUPT;
				break;
			}
			sVedisMPGlobal.pMutexMethods = pMethods;			
			if( sVedisMPGlobal.nThreadingLevel == 0 ){
				/* Set a default threading level */
				sVedisMPGlobal.nThreadingLevel = VEDIS_THREAD_LEVEL_MULTI; 
			}
#endif
			break;
										   }
		case VEDIS_LIB_CONFIG_THREAD_LEVEL_SINGLE:
#if defined(VEDIS_ENABLE_THREADS)
			/* Single thread mode (Only one thread is allowed to play with the library) */
			sVedisMPGlobal.nThreadingLevel = VEDIS_THREAD_LEVEL_SINGLE;
#endif
			break;
		case VEDIS_LIB_CONFIG_THREAD_LEVEL_MULTI:
#if defined(VEDIS_ENABLE_THREADS)
			/* Multi-threading mode (library is thread safe and engines and virtual machines
			 * may be shared between multiple threads).
			 */
			sVedisMPGlobal.nThreadingLevel = VEDIS_THREAD_LEVEL_MULTI;
#endif
			break;
		default:
			/* Unknown configuration option */
			rc = VEDIS_CORRUPT;
			break;
	}
	return rc;
}
/*
 * [CAPIREF: vedis_lib_config()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_lib_config(int nConfigOp,...)
{
	va_list ap;
	int rc;
	if( sVedisMPGlobal.nMagic == VEDIS_LIB_MAGIC ){
		/* Library is already initialized, this operation is forbidden */
		return VEDIS_LOCKED;
	}
	va_start(ap,nConfigOp);
	rc = vedisCoreConfigure(nConfigOp,ap);
	va_end(ap);
	return rc;
}
/*
 * Global library initialization
 * Refer to [vedis_lib_init()]
 * This routine must be called to initialize the memory allocation subsystem, the mutex 
 * subsystem prior to doing any serious work with the library. The first thread to call
 * this routine does the initialization process and set the magic number so no body later
 * can re-initialize the library. If subsequent threads call this  routine before the first
 * thread have finished the initialization process, then the subsequent threads must block 
 * until the initialization process is done.
 */
static sxi32 vedisCoreInitialize(void)
{
	const vedis_kv_methods *pMethods;
	const vedis_vfs *pVfs; /* Built-in vfs */
#if defined(VEDIS_ENABLE_THREADS)
	const SyMutexMethods *pMutexMethods = 0;
	SyMutex *pMaster = 0;
#endif
	int rc;
	/*
	 * If the library is already initialized, then a call to this routine
	 * is a no-op.
	 */
	if( sVedisMPGlobal.nMagic == VEDIS_LIB_MAGIC ){
		return VEDIS_OK; /* Already initialized */
	}
#if defined(VEDIS_ENABLE_THREADS)
	if( sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_SINGLE ){
		pMutexMethods = sVedisMPGlobal.pMutexMethods;
		if( pMutexMethods == 0 ){
			/* Use the built-in mutex subsystem */
			pMutexMethods = SyMutexExportMethods();
			if( pMutexMethods == 0 ){
				return VEDIS_CORRUPT; /* Can't happen */
			}
			/* Install the mutex subsystem */
			rc = vedis_lib_config(VEDIS_LIB_CONFIG_USER_MUTEX, pMutexMethods);
			if( rc != VEDIS_OK ){
				return rc;
			}
		}
		/* Obtain a static mutex so we can initialize the library without calling malloc() */
		pMaster = SyMutexNew(pMutexMethods, SXMUTEX_TYPE_STATIC_1);
		if( pMaster == 0 ){
			return VEDIS_CORRUPT; /* Can't happen */
		}
	}
	/* Lock the master mutex */
	rc = VEDIS_OK;
	SyMutexEnter(pMutexMethods, pMaster); /* NO-OP if sVedisMPGlobal.nThreadingLevel == VEDIS_THREAD_LEVEL_SINGLE */
	if( sVedisMPGlobal.nMagic != VEDIS_LIB_MAGIC ){
#endif
		if( sVedisMPGlobal.sAllocator.pMethods == 0 ){
			/* Install a memory subsystem */
			rc = vedis_lib_config(VEDIS_LIB_CONFIG_USER_MALLOC, 0); /* zero mean use the built-in memory backend */
			if( rc != VEDIS_OK ){
				/* If we are unable to initialize the memory backend, there is no much we can do here.*/
				goto End;
			}
		}
#if defined(VEDIS_ENABLE_THREADS)
		if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE ){
			/* Protect the memory allocation subsystem */
			rc = SyMemBackendMakeThreadSafe(&sVedisMPGlobal.sAllocator, sVedisMPGlobal.pMutexMethods);
			if( rc != VEDIS_OK ){
				goto End;
			}
		}
#endif
		/* Point to the built-in vfs */
		pVfs = vedisExportBuiltinVfs();
		if( sVedisMPGlobal.pVfs == 0 ){
			/* Install it */
			vedis_lib_config(VEDIS_LIB_CONFIG_VFS, pVfs);
		}
		SySetInit(&sVedisMPGlobal.kv_storage,&sVedisMPGlobal.sAllocator,sizeof(vedis_kv_methods *));
		/* Install the built-in Key Value storage engines */
		pMethods = vedisExportMemKvStorage(); /* In-memory storage */
		vedis_lib_config(VEDIS_LIB_CONFIG_STORAGE_ENGINE,pMethods);
		/* Default disk key/value storage engine */
		pMethods = vedisExportDiskKvStorage(); /* Disk storage */
		vedis_lib_config(VEDIS_LIB_CONFIG_STORAGE_ENGINE,pMethods);
		/* Default page size */
		if( sVedisMPGlobal.iPageSize < VEDIS_MIN_PAGE_SIZE ){
			vedis_lib_config(VEDIS_LIB_CONFIG_PAGE_SIZE,VEDIS_DEFAULT_PAGE_SIZE);
		}
		/* Our library is initialized, set the magic number */
		sVedisMPGlobal.nMagic = VEDIS_LIB_MAGIC;
		rc = VEDIS_OK;
#if defined(VEDIS_ENABLE_THREADS)
	} /* sVedisMPGlobal.nMagic != VEDIS_LIB_MAGIC */
#endif
End:
#if defined(VEDIS_ENABLE_THREADS)
	/* Unlock the master mutex */
	SyMutexLeave(pMutexMethods, pMaster); /* NO-OP if sVedisMPGlobal.nThreadingLevel == VEDIS_THREAD_LEVEL_SINGLE */
#endif
	return rc;
}
/*
 * Release a single instance of a Vedis engine.
 */
static int vedisEngineRelease(vedis *pStore)
{
	int rc = VEDIS_OK;
	if( (pStore->iFlags & VEDIS_FL_DISABLE_AUTO_COMMIT) == 0 ){
		/* Commit any outstanding transaction */
		rc = vedisPagerCommit(pStore->pPager);
		if( rc != VEDIS_OK ){
			/* Rollback the transaction */
			rc = vedisPagerRollback(pStore->pPager,FALSE);
		}
	}else{
		/* Rollback any outstanding transaction */
		rc = vedisPagerRollback(pStore->pPager,FALSE);
	}
	/* Close the pager */
	vedisPagerClose(pStore->pPager);
	/* Set a dummy magic number */
	pStore->nMagic = 0x7250;
	/* Release the whole memory subsystem */
	SyMemBackendRelease(&pStore->sMem);
	/* Commit or rollback result */
	return rc;
}
/*
 * Release all resources consumed by the library.
 * Note: This call is not thread safe. Refer to [vedis_lib_shutdown()].
 */
static void vedisCoreShutdown(void)
{
	vedis *pStore, *pNext;
	/* Release all active databases handles */
	pStore = sVedisMPGlobal.pStore;
	for(;;){
		if( sVedisMPGlobal.nStore < 1 ){
			break;
		}
		pNext = pStore->pNext;
		vedisEngineRelease(pStore); 
		pStore = pNext;
		sVedisMPGlobal.nStore--;
	}
	/* Release the storage methods container */
	SySetRelease(&sVedisMPGlobal.kv_storage);
#if defined(VEDIS_ENABLE_THREADS)
	/* Release the mutex subsystem */
	if( sVedisMPGlobal.pMutexMethods ){
		if( sVedisMPGlobal.pMutex ){
			SyMutexRelease(sVedisMPGlobal.pMutexMethods, sVedisMPGlobal.pMutex);
			sVedisMPGlobal.pMutex = 0;
		}
		if( sVedisMPGlobal.pMutexMethods->xGlobalRelease ){
			sVedisMPGlobal.pMutexMethods->xGlobalRelease();
		}
		sVedisMPGlobal.pMutexMethods = 0;
	}
	sVedisMPGlobal.nThreadingLevel = 0;
#endif
	if( sVedisMPGlobal.sAllocator.pMethods ){
		/* Release the memory backend */
		SyMemBackendRelease(&sVedisMPGlobal.sAllocator);
	}
	sVedisMPGlobal.nMagic = 0x1764;
}
/*
 * [CAPIREF: vedis_lib_init()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_lib_init(void)
{
	int rc;
	rc = vedisCoreInitialize();
	return rc;
}
/*
 * [CAPIREF: vedis_lib_shutdown()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_lib_shutdown(void)
{
	if( sVedisMPGlobal.nMagic != VEDIS_LIB_MAGIC ){
		/* Already shut */
		return VEDIS_OK;
	}
	vedisCoreShutdown();
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_lib_is_threadsafe()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_lib_is_threadsafe(void)
{
	if( sVedisMPGlobal.nMagic != VEDIS_LIB_MAGIC ){
		return 0;
	}
#if defined(VEDIS_ENABLE_THREADS)
		if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE ){
			/* Muli-threading support is enabled */
			return 1;
		}else{
			/* Single-threading */
			return 0;
		}
#else
	return 0;
#endif
}
/*
 *
 * [CAPIREF: vedis_lib_version()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
const char * vedis_lib_version(void)
{
	return VEDIS_VERSION;
}
/*
 *
 * [CAPIREF: vedis_lib_signature()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
const char * vedis_lib_signature(void)
{
	return VEDIS_SIG;
}
/*
 *
 * [CAPIREF: vedis_lib_ident()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
const char * vedis_lib_ident(void)
{
	return VEDIS_IDENT;
}
/*
 *
 * [CAPIREF: vedis_lib_copyright()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
const char * vedis_lib_copyright(void)
{
	return VEDIS_COPYRIGHT;
}
/*
 * Remove harmfull and/or stale flags passed to the [vedis_open()] interface.
 */
static unsigned int vedisSanityzeFlag(unsigned int iFlags)
{
	iFlags &= ~VEDIS_OPEN_EXCLUSIVE; /* Reserved flag */
	if( !iFlags ){
		/* Default flags */
		iFlags |= VEDIS_OPEN_CREATE;
	}
	if( iFlags & VEDIS_OPEN_TEMP_DB ){
		/* Omit journaling for temporary database */
		iFlags |= VEDIS_OPEN_OMIT_JOURNALING|VEDIS_OPEN_CREATE;
	}
	if( (iFlags & (VEDIS_OPEN_READONLY|VEDIS_OPEN_READWRITE)) == 0 ){
		/* Auto-append the R+W flag */
		iFlags |= VEDIS_OPEN_READWRITE;
	}
	if( iFlags & VEDIS_OPEN_CREATE ){
		iFlags &= ~(VEDIS_OPEN_MMAP|VEDIS_OPEN_READONLY);
		/* Auto-append the R+W flag */
		iFlags |= VEDIS_OPEN_READWRITE;
	}else{
		if( iFlags & VEDIS_OPEN_READONLY ){
			iFlags &= ~VEDIS_OPEN_READWRITE;
		}else if( iFlags & VEDIS_OPEN_READWRITE ){
			iFlags &= ~VEDIS_OPEN_MMAP;
		}
	}
	return iFlags;
}
/*
 * This routine does the work of initializing a engine on behalf
 * of [vedis_open()].
 */
static int vedisInitDatabase(
	vedis *pStore,            /* Database handle */
	SyMemBackend *pParent,   /* Master memory backend */
	const char *zFilename,   /* Target database */
	unsigned int iFlags      /* Open flags */
	)
{
	vedis_cmd **apTable;
	int rc;
	/* Initialiaze the memory subsystem */
	SyMemBackendInitFromParent(&pStore->sMem,pParent);
#if defined(VEDIS_ENABLE_THREADS)
	/* No need for internal mutexes */
	SyMemBackendDisbaleMutexing(&pStore->sMem);
#endif
	SyBlobInit(&pStore->sErr,&pStore->sMem);	
	/* Sanityze flags */
	iFlags = vedisSanityzeFlag(iFlags);
	/* Init the pager and the transaction manager */
	rc = vedisPagerOpen(sVedisMPGlobal.pVfs,pStore,zFilename,iFlags);
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Allocate the command table */
	apTable = (vedis_cmd **)SyMemBackendAlloc(&pStore->sMem,sizeof(vedis_cmd *) * 64);
	if( apTable == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the table */
	SyZero((void *)apTable,sizeof(vedis_cmd *) * 64);
	pStore->apCmd = apTable;
	pStore->nSize = 64;
	/* Allocate table bucket */
	pStore->apTable = (vedis_table **)SyMemBackendAlloc(&pStore->sMem,sizeof(vedis_table *) * 32);
	if( pStore->apTable == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the table */
	SyZero((void *)pStore->apTable,sizeof(vedis_table *) * 32);
	pStore->nTableSize = 32;
	/* Execution result */
	vedisMemObjInit(pStore,&pStore->sResult);
	return VEDIS_OK;
}
/*
 * Return the default page size.
 */
VEDIS_PRIVATE int vedisGetPageSize(void)
{
	int iSize =  sVedisMPGlobal.iPageSize;
	if( iSize < VEDIS_MIN_PAGE_SIZE || iSize > VEDIS_MAX_PAGE_SIZE ){
		iSize = VEDIS_DEFAULT_PAGE_SIZE;
	}
	return iSize;
}
/*
 * Generate an error message.
 */
VEDIS_PRIVATE int vedisGenError(vedis *pStore,const char *zErr)
{
	int rc;
	/* Append the error message */
	rc = SyBlobAppend(&pStore->sErr,(const void *)zErr,SyStrlen(zErr));
	/* Append a new line */
	SyBlobAppend(&pStore->sErr,(const void *)"\n",sizeof(char));
	return rc;
}
/*
 * Generate an error message (Printf like).
 */
VEDIS_PRIVATE int vedisGenErrorFormat(vedis *pStore,const char *zFmt,...)
{
	va_list ap;
	int rc;
	va_start(ap,zFmt);
	rc = SyBlobFormatAp(&pStore->sErr,zFmt,ap);
	va_end(ap);
	/* Append a new line */
	SyBlobAppend(&pStore->sErr,(const void *)"\n",sizeof(char));
	return rc;
}
/*
 * Generate an error message (Out of memory).
 */
VEDIS_PRIVATE int vedisGenOutofMem(vedis *pStore)
{
	int rc;
	rc = vedisGenError(pStore,"Vedis is running out of memory");
	return rc;
}
/*
 * Configure a working Vedis instance.
 */
static int vedisConfigure(vedis *pStore,int nOp,va_list ap)
{
	int rc = VEDIS_OK;
	switch(nOp){
	case VEDIS_CONFIG_MAX_PAGE_CACHE: {
		int max_page = va_arg(ap,int);
		/* Maximum number of page to cache (Simple hint). */
		rc = vedisPagerSetCachesize(pStore->pPager,max_page);
		break;
										}
	case VEDIS_CONFIG_ERR_LOG: {
		/* Database error log if any */
		const char **pzPtr = va_arg(ap, const char **);
		int *pLen = va_arg(ap, int *);
		if( pzPtr == 0 ){
			rc = VEDIS_CORRUPT;
			break;
		}
		/* NULL terminate the error-log buffer */
		SyBlobNullAppend(&pStore->sErr);
		/* Point to the error-log buffer */
		*pzPtr = (const char *)SyBlobData(&pStore->sErr);
		if( pLen ){
			if( SyBlobLength(&pStore->sErr) > 1 /* NULL '\0' terminator */ ){
				*pLen = (int)SyBlobLength(&pStore->sErr);
			}else{
				*pLen = 0;
			}
		}
		break;
								 }
	case VEDIS_CONFIG_DISABLE_AUTO_COMMIT:{
		/* Disable auto-commit */
		pStore->iFlags |= VEDIS_FL_DISABLE_AUTO_COMMIT;
		break;
											}
	case VEDIS_CONFIG_GET_KV_NAME: {
		/* Name of the underlying KV storage engine */
		const char **pzPtr = va_arg(ap,const char **);
		if( pzPtr ){
			vedis_kv_engine *pEngine;
			pEngine = vedisPagerGetKvEngine(pStore);
			/* Point to the name */
			*pzPtr = pEngine->pIo->pMethods->zName;
		}
		break;
									 }
	case VEDIS_CONFIG_DUP_EXEC_VALUE:{
		/* Duplicate execution value */
		vedis_value **ppOut = va_arg(ap,vedis_value **);
		if( ppOut ){
			vedis_value *pObj = vedisNewObjectValue(pStore,0);
			if( pObj == 0 ){
				*ppOut = 0;
				rc = VEDIS_NOMEM;
				break;
			}
			/* Duplicate */
			vedisMemObjStore(&pStore->sResult,pObj);
			*ppOut = pObj;
		}
		break;
									 }
	case VEDIS_CONFIG_RELEASE_DUP_VALUE: {
		/* Release a duplicated vedis_value */
		vedis_value *pIn = va_arg(ap,vedis_value *);
		if( pIn ){
			vedisObjectValueDestroy(pStore,pIn);
		}
		break;
										 }
	case VEDIS_CONFIG_OUTPUT_CONSUMER: {
		/* Output consumer callback */
		ProcCmdConsumer xCons = va_arg(ap,ProcCmdConsumer);
		void *pUserData = va_arg(ap,void *);
		pStore->xResultConsumer = xCons;
		pStore->pUserData = pUserData;
		break;
									   }
	default:
		/* Unknown configuration option */
		rc = VEDIS_UNKNOWN;
		break;
	}
	return rc;
}
/*
 * Export the global (master) memory allocator to submodules.
 */
VEDIS_PRIVATE const SyMemBackend * vedisExportMemBackend(void)
{
	return &sVedisMPGlobal.sAllocator;
}
/*
 * Default data consumer callback. That is, all retrieved is redirected to this
 * routine which store the output in an internal blob.
 */
VEDIS_PRIVATE int vedisDataConsumer(
	const void *pOut,   /* Data to consume */
	unsigned int nLen,  /* Data length */
	void *pUserData     /* User private data */
	)
{
	 sxi32 rc;
	 /* Store the output in an internal BLOB */
	 rc = SyBlobAppend((SyBlob *)pUserData, pOut, nLen);
	 return rc;
}
/*
 * Fetch an installed vedis command.
 */
VEDIS_PRIVATE vedis_cmd * vedisFetchCommand(vedis *pVedis,SyString *pName)
{
	vedis_cmd *pCmd;
	sxu32 nH;
	if( pVedis->nCmd < 1 ){
		/* Don't bother hashing */
		return 0;
	}
	/* Hash the name */
	nH = SyBinHash(pName->zString,pName->nByte);
	/* Point to the corresponding bucket */
	pCmd = pVedis->apCmd[nH & (pVedis->nSize - 1)];
	/* Perform the lookup */
	for(;;){
		if( pCmd == 0 ){
			break;
		}
		if( pCmd->nHash == nH && SyStringCmp(&pCmd->sName,pName,SyMemcmp) == 0 ){
			/* Got command */
			return pCmd;
		}
		/* Point to the next item */
		pCmd = pCmd->pNextCol;
	}
	/* No such command */
	return 0;
}
/*
 * Install a vedis command.
 */
static int vedisInstallCommand(vedis *pVedis,const char *zName,ProcVedisCmd xCmd,void *pUserData)
{
	SyMemBackend *pAlloc = &sVedisMPGlobal.sAllocator;
	vedis_cmd *pCmd;
	SyString sName;
	sxu32 nBucket;
	char *zDup;
	sxu32 nLen;
	/* Check for an existing command with the same name */
	nLen = SyStrlen(zName);
	SyStringInitFromBuf(&sName,zName,nLen);
	pCmd = vedisFetchCommand(pVedis,&sName);
	if( pCmd ){
		/* Already installed */
		pCmd->xCmd = xCmd;
		pCmd->pUserData = pUserData;
		SySetReset(&pCmd->aAux);
		return VEDIS_OK;
	}
	/* Allocate a new instance */
	pCmd = (vedis_cmd *)SyMemBackendAlloc(pAlloc,sizeof(vedis_cmd)+nLen);
	if( pCmd == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pCmd,sizeof(vedis_cmd));
	/* Fill-in */
	SySetInit(&pCmd->aAux,&pVedis->sMem,sizeof(vedis_aux_data));
	pCmd->nHash = SyBinHash(zName,nLen);
	pCmd->xCmd = xCmd;
	pCmd->pUserData = pUserData;
	zDup = (char *)&pCmd[1];
	SyMemcpy(zName,zDup,nLen);
	SyStringInitFromBuf(&pCmd->sName,zDup,nLen);
	/* Install the command */
	MACRO_LD_PUSH(pVedis->pList,pCmd);
	pVedis->nCmd++;
	nBucket = pCmd->nHash & (pVedis->nSize - 1);
	pCmd->pNextCol = pVedis->apCmd[nBucket];
	if( pVedis->apCmd[nBucket] ){
		pVedis->apCmd[nBucket]->pPrevCol = pCmd;
	}
	pVedis->apCmd[nBucket] = pCmd;
	if( (pVedis->nCmd >= pVedis->nSize * 3) && pVedis->nCmd < 100000 ){
		/* Rehash */
		sxu32 nNewSize = pVedis->nSize << 1;
		vedis_cmd *pEntry;
		vedis_cmd **apNew;
		sxu32 n;
		/* Allocate a new larger table */
		apNew = (vedis_cmd **)SyMemBackendAlloc(&pVedis->sMem, nNewSize * sizeof(vedis_cmd *));
		if( apNew ){
			/* Zero the new table */
			SyZero((void *)apNew, nNewSize * sizeof(vedis_cmd *));
			/* Rehash all entries */
			n = 0;
			pEntry = pVedis->pList;
			for(;;){
				/* Loop one */
				if( n >= pVedis->nCmd ){
					break;
				}
				pEntry->pNextCol = pEntry->pPrevCol = 0;
				/* Install in the new bucket */
				nBucket = pEntry->nHash & (nNewSize - 1);
				pEntry->pNextCol = apNew[nBucket];
				if( apNew[nBucket] ){
					apNew[nBucket]->pPrevCol = pEntry;
				}
				apNew[nBucket] = pEntry;
				/* Point to the next entry */
				pEntry = pEntry->pNext;
				n++;
			}
			/* Release the old table and reflect the change */
			SyMemBackendFree(&pVedis->sMem,(void *)pVedis->apCmd);
			pVedis->apCmd = apNew;
			pVedis->nSize  = nNewSize;
		}
	}
	return VEDIS_OK;
}
/*
 * Remove a vedis command.
 */
static int vedisRemoveCommand(vedis *pVedis,const char *zCmd)
{
	vedis_cmd *pCmd;
	SyString sName;
	SyStringInitFromBuf(&sName,zCmd,SyStrlen(zCmd));
	/* Fetch the command first */
	pCmd = vedisFetchCommand(pVedis,&sName);
	if( pCmd == 0 ){
		/* No such command */
		return VEDIS_NOTFOUND;
	}
	/* Unlink */
	if( pCmd->pNextCol ){
		pCmd->pNextCol->pPrevCol = pCmd->pPrevCol;
	}
	if( pCmd->pPrevCol ){
		pCmd->pPrevCol->pNextCol = pCmd->pNextCol;
	}else{
		sxu32 nBucket;
		nBucket = pCmd->nHash & (pVedis->nSize - 1);
		pVedis->apCmd[nBucket] = pCmd->pNextCol;
	}
	MACRO_LD_REMOVE(pVedis->pList,pCmd);
	pVedis->nCmd--;
	/* Release */
	SyMemBackendFree(&sVedisMPGlobal.sAllocator,pCmd);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_open()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_open(vedis **ppStore,const char *zStorage)
{
	vedis *pHandle;
	int rc;
#if defined(UNTRUST)
	if( ppStore == 0 ){
		return VEDIS_CORRUPT;
	}
#endif
	*ppStore = 0;
	/* One-time automatic library initialization */
	rc = vedisCoreInitialize();
	if( rc != VEDIS_OK ){
		return rc;
	}
	/* Allocate a new engine instance */
	pHandle = (vedis *)SyMemBackendPoolAlloc(&sVedisMPGlobal.sAllocator, sizeof(vedis));
	if( pHandle == 0 ){
		return VEDIS_NOMEM;
	}
	/* Zero the structure */
	SyZero(pHandle,sizeof(vedis));
	/* Init the database */
	rc = vedisInitDatabase(pHandle,&sVedisMPGlobal.sAllocator,zStorage,0);
	if( rc != VEDIS_OK ){
		goto Release;
	}
	/* Set the magic number to identify a valid DB handle */
	 pHandle->nMagic = VEDIS_DB_MAGIC;
	 /* Register built-in vedis commands */
	vedisRegisterBuiltinCommands(pHandle);
	/* Install the commit callback */
	vedisPagerSetCommitCallback(pHandle->pPager,vedisOnCommit,pHandle);
#if defined(VEDIS_ENABLE_THREADS)
	if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE ){
		 /* Associate a recursive mutex with this instance */
		 pHandle->pMutex = SyMutexNew(sVedisMPGlobal.pMutexMethods, SXMUTEX_TYPE_RECURSIVE);
		 if( pHandle->pMutex == 0 ){
			 rc = VEDIS_NOMEM;
			 goto Release;
		 }
	 }
#endif
	/* Link to the list of active engines */
#if defined(VEDIS_ENABLE_THREADS)
	/* Enter the global mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, sVedisMPGlobal.pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel == VEDIS_THREAD_LEVEL_SINGLE */
#endif
	 MACRO_LD_PUSH(sVedisMPGlobal.pStore,pHandle);
	 sVedisMPGlobal.nStore++;
#if defined(VEDIS_ENABLE_THREADS)
	/* Leave the global mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods, sVedisMPGlobal.pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel == VEDIS_THREAD_LEVEL_SINGLE */
#endif
	/* Make the handle available to the caller */
	*ppStore = pHandle;
	return VEDIS_OK;
Release:
	SyMemBackendRelease(&pHandle->sMem);
	SyMemBackendPoolFree(&sVedisMPGlobal.sAllocator,pHandle);
	return rc;
}
/*
 * [CAPIREF: vedis_config()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_config(vedis *pStore,int nConfigOp,...)
{
	va_list ap;
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 va_start(ap, nConfigOp);
	 rc = vedisConfigure(&(*pStore),nConfigOp, ap);
	 va_end(ap);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * [CAPIREF: vedis_close()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_close(vedis *pStore)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	/* Release the engine */
	rc = vedisEngineRelease(pStore);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 /* Release DB mutex */
	 SyMutexRelease(sVedisMPGlobal.pMutexMethods, pStore->pMutex) /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
#if defined(VEDIS_ENABLE_THREADS)
	/* Enter the global mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, sVedisMPGlobal.pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel == VEDIS_THREAD_LEVEL_SINGLE */
#endif
	/* Unlink from the list of active engines */
	 MACRO_LD_REMOVE(sVedisMPGlobal.pStore, pStore);
	sVedisMPGlobal.nStore--;
#if defined(VEDIS_ENABLE_THREADS)
	/* Leave the global mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods, sVedisMPGlobal.pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel == VEDIS_THREAD_LEVEL_SINGLE */
#endif
	/* Release the memory chunk allocated to this handle */
	SyMemBackendPoolFree(&sVedisMPGlobal.sAllocator,pStore);
	return rc;
}
/*
 * [CAPIREF: vedis_exec()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_exec(vedis *pStore,const char *zCmd,int nLen)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Tokenize, parse and execute */
	 rc = vedisProcessInput(pStore,zCmd,nLen < 0 ? /* Assume a null terminated string */ SyStrlen(zCmd) : (sxu32)nLen);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	 /* Execution result */
	return rc;
}
/*
 * [CAPIREF: vedis_exec_fmt()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_exec_fmt(vedis *pStore,const char *zFmt,...)
{
	SyBlob sWorker;
	va_list ap;
	int rc;
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 SyBlobInit(&sWorker,&pStore->sMem);
	 va_start(ap,zFmt);
	 SyBlobFormatAp(&sWorker,zFmt,ap);
	 va_end(ap);
	 /* Execute */
	 rc = vedisProcessInput(pStore,(const char *)SyBlobData(&sWorker),SyBlobLength(&sWorker));
	 /* Cleanup */
	 SyBlobRelease(&sWorker);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	 /* Execution result */
	return rc;
}
/*
 * [CAPIREF: vedis_exec_result()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_exec_result(vedis *pStore,vedis_value **ppOut)
{
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 if(ppOut ){
		 *ppOut = &pStore->sResult;
	 }
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_register_command()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_register_command(vedis *pStore,const char *zName,int (*xCmd)(vedis_context *,int,vedis_value **),void *pUserdata)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) || xCmd == 0){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Install the command */
	 rc = vedisInstallCommand(pStore,zName,xCmd,pUserdata);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * [CAPIREF: vedis_delete_command()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_delete_command(vedis *pStore,const char *zName)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Delete the command */
	 rc = vedisRemoveCommand(pStore,zName);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * [CAPIREF: vedis_context_throw_error()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_throw_error(vedis_context *pCtx, int iErr, const char *zErr)
{
	if( zErr ){
		SyBlob *pErr = &pCtx->pVedis->sErr;
		const char *zErrType = "-Error-";
		/* Severity */
		switch(iErr){
		case VEDIS_CTX_WARNING: zErrType = "-Warning-"; break;
		case VEDIS_CTX_NOTICE:  zErrType = "-Notice-";  break;
		default: break;
		}
		/* Generate the error message */
		SyBlobFormat(pErr,"%z: %s %s\n",&pCtx->pCmd->sName,zErrType,zErr);
	}
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_context_throw_error_format()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_throw_error_format(vedis_context *pCtx, int iErr, const char *zFormat, ...)
{
	SyBlob *pErr = &pCtx->pVedis->sErr;
	const char *zErr = "-Error-";
	va_list ap;
	
	if( zFormat == 0){
		return VEDIS_OK;
	}
	/* Severity */
	switch(iErr){
	case VEDIS_CTX_WARNING: zErr = "-Warning-"; break;
	case VEDIS_CTX_NOTICE:  zErr = "-Notice-";  break;
	default: break;
	}
	/* Generate the error message */
	SyBlobFormat(pErr,"%z: %s",&pCtx->pCmd->sName,zErr);
	va_start(ap, zFormat);
	SyBlobFormatAp(pErr,zFormat,ap);
	va_end(ap);
	SyBlobAppend(pErr,(const void *)"\n",sizeof(char));
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_context_random_num()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
unsigned int vedis_context_random_num(vedis_context *pCtx)
{
	sxu32 n;
	n = vedisPagerRandomNum(pCtx->pVedis->pPager);
	return n;
}
/*
 * [CAPIREF: vedis_context_random_string()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_random_string(vedis_context *pCtx, char *zBuf, int nBuflen)
{
	if( nBuflen < 3 ){
		return VEDIS_CORRUPT;
	}
	vedisPagerRandomString(pCtx->pVedis->pPager, zBuf, nBuflen);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_context_user_data()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
void * vedis_context_user_data(vedis_context *pCtx)
{
	return pCtx->pCmd->pUserData;
}
/*
 * [CAPIREF: vedis_context_push_aux_data()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_push_aux_data(vedis_context *pCtx, void *pUserData)
{
	vedis_aux_data sAux;
	int rc;
	sAux.pAuxData = pUserData;
	rc = SySetPut(&pCtx->pCmd->aAux, (const void *)&sAux);
	return rc;
}
/*
 * [CAPIREF: vedis_context_peek_aux_data()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
void * vedis_context_peek_aux_data(vedis_context *pCtx)
{
	vedis_aux_data *pAux;
	pAux = (vedis_aux_data *)SySetPeek(&pCtx->pCmd->aAux);
	return pAux ? pAux->pAuxData : 0;
}
/*
 * [CAPIREF: vedis_context_pop_aux_data()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
void * vedis_context_pop_aux_data(vedis_context *pCtx)
{
	vedis_aux_data *pAux;
	pAux = (vedis_aux_data *)SySetPop(&pCtx->pCmd->aAux);
	return pAux ? pAux->pAuxData : 0;
}
/*
 * [CAPIREF: vedis_context_new_scalar()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
vedis_value * vedis_context_new_scalar(vedis_context *pCtx)
{
	vedis_value *pVal;
	pVal = vedisNewObjectValue(pCtx->pVedis,0);
	if( pVal ){
		/* Record value address so it can be freed automatically
		 * when the calling function returns. 
		 */
		SySetPut(&pCtx->sVar, (const void *)&pVal);
	}
	return pVal;
}
/*
 * [CAPIREF: vedis_context_new_array()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
vedis_value * vedis_context_new_array(vedis_context *pCtx)
{
	vedis_value *pVal;
	pVal = vedisNewObjectArrayValue(pCtx->pVedis);
	if( pVal ){
		/* Record value address so it can be freed automatically
		 * when the calling function returns. 
		 */
		SySetPut(&pCtx->sVar, (const void *)&pVal);
	}
	return pVal;
}
/*
 * [CAPIREF: vedis_context_release_value()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
void vedis_context_release_value(vedis_context *pCtx, vedis_value *pValue)
{
	if( pValue == 0 ){
		/* NULL value is a harmless operation */
		return;
	}
	if( SySetUsed(&pCtx->sVar) > 0 ){
		vedis_value **apObj = (vedis_value **)SySetBasePtr(&pCtx->sVar);
		sxu32 n;
		for( n = 0 ; n < SySetUsed(&pCtx->sVar) ; ++n ){
			if( apObj[n] == pValue ){
				vedisObjectValueDestroy(pCtx->pVedis,pValue);
				/* Mark as released */
				apObj[n] = 0;
				break;
			}
		}
	}
}
/*
 * [CAPIREF: vedis_array_walk()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_array_walk(vedis_value *pArray, int (*xWalk)(vedis_value *, void *), void *pUserData)
{
	int rc;
	if( xWalk == 0 ){
		return VEDIS_CORRUPT;
	}
	/* Make sure we are dealing with a valid hashmap */
	if( (pArray->iFlags & MEMOBJ_HASHMAP) == 0 ){
		return VEDIS_CORRUPT;
	}
	/* Start the walk process */
	rc = vedisHashmapWalk((vedis_hashmap *)pArray->x.pOther, xWalk, pUserData);
	return rc != VEDIS_OK ? VEDIS_ABORT /* User callback request an operation abort*/ : VEDIS_OK;
}
/*
 * [CAPIREF: vedis_array_reset()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_array_reset(vedis_value *pArray)
{
	/* Make sure we are dealing with a valid hashmap */
	if( (pArray->iFlags & MEMOBJ_HASHMAP) == 0 ){
		return 0;
	}
	vedisHashmapResetLoopCursor((vedis_hashmap *)pArray->x.pOther);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_array_fetch()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
vedis_value * vedis_array_fetch(vedis_value *pArray,unsigned int index)
{
	vedis_value *pValue = 0; /* cc warning */
	vedis_hashmap *pMap;
	vedis_value skey;
	int rc;
	/* Make sure we are dealing with a valid hashmap */
	if( (pArray->iFlags & MEMOBJ_HASHMAP) == 0 ){
		return 0;
	}
	pMap = (vedis_hashmap *)pArray->x.pOther;
	/* Convert the key to a vedis_value  */
	vedisMemObjInitFromInt(vedisHashmapGetEngine(pMap),&skey,(vedis_int64)index);
	/* Perform the lookup */
	rc = vedisHashmapLookup(pMap,&skey,&pValue);
	vedisMemObjRelease(&skey);
	if( rc != VEDIS_OK ){
		/* No such entry */
		return 0;
	}
	return pValue;
}
/*
 * [CAPIREF: vedis_array_insert()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_array_insert(vedis_value *pArray,vedis_value *pValue)
{
	int rc;
	/* Make sure we are dealing with a valid hashmap */
	if( (pArray->iFlags & MEMOBJ_HASHMAP) == 0 ){
		return VEDIS_CORRUPT;
	}
	/* Perform the insertion */
	rc = vedisHashmapInsert((vedis_hashmap *)pArray->x.pOther, 0 /* Assign an automatic index */, &(*pValue));
	return rc;
}
/*
 * [CAPIREF: vedis_array_next_elem()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
vedis_value * vedis_array_next_elem(vedis_value *pArray)
{
	vedis_value *pValue;
	/* Make sure we are dealing with a valid hashmap */
	if( (pArray->iFlags & MEMOBJ_HASHMAP) == 0 ){
		return 0;
	}
	/* Extract the current element */
	pValue = vedisHashmapGetNextEntry((vedis_hashmap *)pArray->x.pOther);
	return pValue;
}
/*
 * [CAPIREF: vedis_array_count()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
unsigned int vedis_array_count(vedis_value *pArray)
{
	/* Make sure we are dealing with a valid hashmap */
	if( (pArray->iFlags & MEMOBJ_HASHMAP) == 0 ){
		return 0;
	}
	return vedisHashmapCount((vedis_hashmap *)pArray->x.pOther);
}
/*
 * [CAPIREF: vedis_result_int()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_int(vedis_context *pCtx, int iValue)
{
	return vedis_value_int(pCtx->pRet, iValue);
}
/*
 * [CAPIREF: vedis_result_int64()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_int64(vedis_context *pCtx, vedis_int64 iValue)
{
	return vedis_value_int64(pCtx->pRet, iValue);
}
/*
 * [CAPIREF: vedis_result_bool()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_bool(vedis_context *pCtx, int iBool)
{
	return vedis_value_bool(pCtx->pRet, iBool);
}
/*
 * [CAPIREF: vedis_result_double()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_double(vedis_context *pCtx, double Value)
{
	return vedis_value_double(pCtx->pRet, Value);
}
/*
 * [CAPIREF: vedis_result_null()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_null(vedis_context *pCtx)
{
	/* Invalidate any prior representation and set the NULL flag */
	vedisMemObjRelease(pCtx->pRet);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_result_string()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_string(vedis_context *pCtx, const char *zString, int nLen)
{
	return vedis_value_string(pCtx->pRet, zString, nLen);
}
/*
 * [CAPIREF: vedis_result_string_format()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_string_format(vedis_context *pCtx, const char *zFormat, ...)
{
	vedis_value *p;
	va_list ap;
	int rc;
	p = pCtx->pRet;
	if( (p->iFlags & MEMOBJ_STRING) == 0 ){
		/* Invalidate any prior representation */
		vedisMemObjRelease(p);
		MemObjSetType(p, MEMOBJ_STRING);
	}
	/* Format the given string */
	va_start(ap, zFormat);
	rc = SyBlobFormatAp(&p->sBlob, zFormat, ap);
	va_end(ap);
	return rc;
}
/*
 * [CAPIREF: vedis_result_value()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_result_value(vedis_context *pCtx, vedis_value *pValue)
{
	int rc = VEDIS_OK;
	if( pValue == 0 ){
		vedisMemObjRelease(pCtx->pRet);
	}else{
		rc = vedisMemObjStore(pValue, pCtx->pRet);
	}
	return rc;
}
/*
 * [CAPIREF: vedis_value_to_int()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_to_int(vedis_value *pValue)
{
	int rc;
	rc = vedisMemObjToInteger(pValue);
	if( rc != VEDIS_OK ){
		return 0;
	}
	return (int)pValue->x.iVal;
}
/*
 * [CAPIREF: vedis_value_to_bool()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_to_bool(vedis_value *pValue)
{
	int rc;
	rc = vedisMemObjToBool(pValue);
	if( rc != VEDIS_OK ){
		return 0;
	}
	return (int)pValue->x.iVal;
}
/*
 * [CAPIREF: vedis_value_to_int64()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
vedis_int64 vedis_value_to_int64(vedis_value *pValue)
{
	int rc;
	rc = vedisMemObjToInteger(pValue);
	if( rc != VEDIS_OK ){
		return 0;
	}
	return pValue->x.iVal;
}
/*
 * [CAPIREF: vedis_value_to_double()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
double vedis_value_to_double(vedis_value *pValue)
{
	int rc;
	rc = vedisMemObjToReal(pValue);
	if( rc != VEDIS_OK ){
		return (double)0;
	}
	return (double)pValue->x.rVal;
}
/*
 * [CAPIREF: vedis_value_to_string()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
const char * vedis_value_to_string(vedis_value *pValue, int *pLen)
{
	vedisMemObjToString(pValue);
	if( SyBlobLength(&pValue->sBlob) > 0 ){
		SyBlobNullAppend(&pValue->sBlob);
		if( pLen ){
			*pLen = (int)SyBlobLength(&pValue->sBlob);
		}
		return (const char *)SyBlobData(&pValue->sBlob);
	}else{
		/* Return the empty string */
		if( pLen ){
			*pLen = 0;
		}
		return "";
	}
}
/*
 * [CAPIREF: vedis_value_int()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_int(vedis_value *pVal, int iValue)
{
	/* Invalidate any prior representation */
	vedisMemObjRelease(pVal);
	pVal->x.iVal = (vedis_int64)iValue;
	MemObjSetType(pVal, MEMOBJ_INT);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_int64()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_int64(vedis_value *pVal, vedis_int64 iValue)
{
	/* Invalidate any prior representation */
	vedisMemObjRelease(pVal);
	pVal->x.iVal = iValue;
	MemObjSetType(pVal, MEMOBJ_INT);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_bool()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_bool(vedis_value *pVal, int iBool)
{
	/* Invalidate any prior representation */
	vedisMemObjRelease(pVal);
	pVal->x.iVal = iBool ? 1 : 0;
	MemObjSetType(pVal, MEMOBJ_BOOL);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_null()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_null(vedis_value *pVal)
{
	/* Invalidate any prior representation and set the NULL flag */
	vedisMemObjRelease(pVal);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_double()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_double(vedis_value *pVal, double Value)
{
	/* Invalidate any prior representation */
	vedisMemObjRelease(pVal);
	pVal->x.rVal = (vedis_real)Value;
	MemObjSetType(pVal, MEMOBJ_REAL);
	/* Try to get an integer representation also */
	vedisMemObjTryInteger(pVal);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_string()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_string(vedis_value *pVal, const char *zString, int nLen)
{
	if((pVal->iFlags & MEMOBJ_STRING) == 0 ){
		/* Invalidate any prior representation */
		vedisMemObjRelease(pVal);
		MemObjSetType(pVal, MEMOBJ_STRING);
	}
	if( zString ){
		if( nLen < 0 ){
			/* Compute length automatically */
			nLen = (int)SyStrlen(zString);
		}
		SyBlobAppend(&pVal->sBlob, (const void *)zString, (sxu32)nLen);
	}
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_string_format()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_string_format(vedis_value *pVal, const char *zFormat, ...)
{
	va_list ap;
	int rc;
	if((pVal->iFlags & MEMOBJ_STRING) == 0 ){
		/* Invalidate any prior representation */
		vedisMemObjRelease(pVal);
		MemObjSetType(pVal, MEMOBJ_STRING);
	}
	va_start(ap, zFormat);
	rc = SyBlobFormatAp(&pVal->sBlob, zFormat, ap);
	va_end(ap);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_reset_string_cursor()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_reset_string_cursor(vedis_value *pVal)
{
	/* Reset the string cursor */
	SyBlobReset(&pVal->sBlob);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_release()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_release(vedis_value *pVal)
{
	vedisMemObjRelease(pVal);
	return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_value_is_int()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_int(vedis_value *pVal)
{
	return (pVal->iFlags & MEMOBJ_INT) ? TRUE : FALSE;
}
/*
 * [CAPIREF: vedis_value_is_float()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_float(vedis_value *pVal)
{
	return (pVal->iFlags & MEMOBJ_REAL) ? TRUE : FALSE;
}
/*
 * [CAPIREF: vedis_value_is_bool()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_bool(vedis_value *pVal)
{
	return (pVal->iFlags & MEMOBJ_BOOL) ? TRUE : FALSE;
}
/*
 * [CAPIREF: vedis_value_is_string()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_string(vedis_value *pVal)
{
	return (pVal->iFlags & MEMOBJ_STRING) ? TRUE : FALSE;
}
/*
 * [CAPIREF: vedis_value_is_null()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_null(vedis_value *pVal)
{
	return (pVal->iFlags & MEMOBJ_NULL) ? TRUE : FALSE;
}
/*
 * [CAPIREF: vedis_value_is_numeric()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_numeric(vedis_value *pVal)
{
	int rc;
	rc = vedisMemObjIsNumeric(pVal);
	return rc;
}
/*
 * [CAPIREF: vedis_value_is_scalar()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_scalar(vedis_value *pVal)
{
	return (pVal->iFlags & MEMOBJ_SCALAR) ? TRUE : FALSE;
}
/*
 * [CAPIREF: vedis_value_is_json_array()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_value_is_array(vedis_value *pVal)
{
	return (pVal->iFlags & MEMOBJ_HASHMAP) ? TRUE : FALSE;
}
/*
 * Refer to [vedis_kv_store()].
 */
static int vedisKvStore(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen)
{
	vedis_kv_engine *pEngine;
	int rc;
	/* Point to the underlying storage engine */
	 pEngine = vedisPagerGetKvEngine(pStore);
	 if( pEngine->pIo->pMethods->xReplace == 0 ){
		 /* Storage engine does not implement such method */
		 vedisGenError(pStore,"xReplace() method not implemented in the underlying storage engine");
		 rc = VEDIS_NOTIMPLEMENTED;
	 }else{
		 if( nKeyLen < 0 ){
			 /* Assume a null terminated string and compute its length */
			 nKeyLen = SyStrlen((const char *)pKey);
		 }
		 if( !nKeyLen ){
			 vedisGenError(pStore,"Empty key");
			 rc = VEDIS_EMPTY;
		 }else{
			 /* Perform the requested operation */
			 rc = pEngine->pIo->pMethods->xReplace(pEngine,pKey,nKeyLen,pData,nDataLen);
		 }
	 }
	 return rc;
}
/*
 * [CAPIREF: vedis_kv_store()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_store(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 rc = vedisKvStore(pStore,pKey,nKeyLen,pData,nDataLen);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * [CAPIREF: vedis_kv_store_fmt()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_store_fmt(vedis *pStore,const void *pKey,int nKeyLen,const char *zFormat,...)
{
	SyBlob sWorker; /* Working buffer */
	va_list ap;
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 SyBlobInit(&sWorker,&pStore->sMem);
	 /* Format the data */
	 va_start(ap,zFormat);
	 SyBlobFormatAp(&sWorker,zFormat,ap);
	 va_end(ap);
	 /* Perform the requested operation */
	 rc = vedisKvStore(pStore,pKey,nKeyLen,SyBlobData(&sWorker),SyBlobLength(&sWorker));
	 /* Clean up */
	 SyBlobRelease(&sWorker);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * Refer to [vedis_kv_append()].
 */
static int vedisKvAppend(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen)
{
	vedis_kv_engine *pEngine;
	int rc;
	/* Point to the underlying storage engine */
	 pEngine = vedisPagerGetKvEngine(pStore);
	 if( pEngine->pIo->pMethods->xAppend == 0 ){
		 /* Storage engine does not implement such method */
		 vedisGenError(pStore,"xAppend() method not implemented in the underlying storage engine");
		 rc = VEDIS_NOTIMPLEMENTED;
	 }else{
		 if( nKeyLen < 0 ){
			 /* Assume a null terminated string and compute its length */
			 nKeyLen = SyStrlen((const char *)pKey);
		 }
		 if( !nKeyLen ){
			 vedisGenError(pStore,"Empty key");
			 rc = VEDIS_EMPTY;
		 }else{
			 /* Perform the requested operation */
			 rc = pEngine->pIo->pMethods->xAppend(pEngine,pKey,nKeyLen,pData,nDataLen);
		 }
	 }
	 return rc;
}
/*
 * [CAPIREF: vedis_kv_append()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_append(vedis *pStore,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 rc = vedisKvAppend(pStore,pKey,nKeyLen,pData,nDataLen);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * [CAPIREF: vedis_kv_append_fmt()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_append_fmt(vedis *pStore,const void *pKey,int nKeyLen,const char *zFormat,...)
{
	SyBlob sWorker; /* Working buffer */
	va_list ap;
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 SyBlobInit(&sWorker,&pStore->sMem);
	 /* Format the data */
	 va_start(ap,zFormat);
	 SyBlobFormatAp(&sWorker,zFormat,ap);
	 va_end(ap);
	 /* Perform the requested operation */
	 rc = vedisKvAppend(pStore,pKey,nKeyLen,SyBlobData(&sWorker),SyBlobLength(&sWorker));
	 /* Clean up */
	 SyBlobRelease(&sWorker);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * Refer to [vedis_kv_fetch()].
 */
static int vedisKvFetch(vedis *pStore,const void *pKey,int nKeyLen,void *pBuf,vedis_int64 *pBufLen)
{
	vedis_kv_methods *pMethods;
	vedis_kv_engine *pEngine;
	vedis_kv_cursor *pCur;
	int rc;
	/* Point to the underlying storage engine */
	 pEngine = vedisPagerGetKvEngine(pStore);
	 pMethods = pEngine->pIo->pMethods;
	 pCur = pStore->pCursor;
	 if( nKeyLen < 0 ){
		 /* Assume a null terminated string and compute its length */
		 nKeyLen = SyStrlen((const char *)pKey);
	 }
	 if( !nKeyLen ){
		  vedisGenError(pStore,"Empty key");
		  rc = VEDIS_EMPTY;
	 }else{
		  /* Seek to the record position */
		  rc = pMethods->xSeek(pCur,pKey,nKeyLen,VEDIS_CURSOR_MATCH_EXACT);
	 }
	 if( rc == VEDIS_OK ){
		 if( pBuf == 0 ){
			 /* Data length only */
			 rc = pMethods->xDataLength(pCur,pBufLen);
		 }else{
			 SyBlob sBlob;
			 /* Initialize the data consumer */
			 SyBlobInitFromBuf(&sBlob,pBuf,(sxu32)*pBufLen);
			 /* Consume the data */
			 rc = pMethods->xData(pCur,vedisDataConsumer,&sBlob);
			 /* Data length */
			 *pBufLen = (vedis_int64)SyBlobLength(&sBlob);
			 /* Cleanup */
			 SyBlobRelease(&sBlob);
		 }
	 }
	 return rc;
}
/*
 * [CAPIREF: vedis_kv_fetch()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_fetch(vedis *pStore,const void *pKey,int nKeyLen,void *pBuf,vedis_int64 *pBufLen)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 rc = vedisKvFetch(pStore,pKey,nKeyLen,pBuf,pBufLen);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * Refer to [vedis_kv_fetch_callback()].
 */
VEDIS_PRIVATE int vedisKvFetchCallback(vedis *pStore,const void *pKey,int nKeyLen,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData)
{
	vedis_kv_methods *pMethods;
	vedis_kv_engine *pEngine;
	vedis_kv_cursor *pCur;
	int rc;
	/* Point to the underlying storage engine */
	 pEngine = vedisPagerGetKvEngine(pStore);
	 pMethods = pEngine->pIo->pMethods;
	 pCur = pStore->pCursor;
	 if( nKeyLen < 0 ){
		 /* Assume a null terminated string and compute its length */
		 nKeyLen = SyStrlen((const char *)pKey);
	 }
	 if( !nKeyLen ){
		 vedisGenError(pStore,"Empty key");
		 rc = VEDIS_EMPTY;
	 }else{
		 /* Seek to the record position */
		 rc = pMethods->xSeek(pCur,pKey,nKeyLen,VEDIS_CURSOR_MATCH_EXACT);
	 }
	 if( rc == VEDIS_OK && xConsumer ){
		 /* Consume the data directly */
		 rc = pMethods->xData(pCur,xConsumer,pUserData);	 
	 }
	return rc;
}
/*
 * [CAPIREF: vedis_kv_fetch_callback()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_fetch_callback(vedis *pStore,const void *pKey,int nKeyLen,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 rc = vedisKvFetchCallback(pStore,pKey,nKeyLen,xConsumer,pUserData);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * Refer to [vedis_kv_delete()].
 */
VEDIS_PRIVATE int vedisKvDelete(vedis *pStore,const void *pKey,int nKeyLen)
{
	vedis_kv_methods *pMethods;
	vedis_kv_engine *pEngine;
	vedis_kv_cursor *pCur;
	int rc;
	/* Point to the underlying storage engine */
	 pEngine = vedisPagerGetKvEngine(pStore);
	 pMethods = pEngine->pIo->pMethods;
	 pCur = pStore->pCursor;
	 if( pMethods->xDelete == 0 ){
		 /* Storage engine does not implement such method */
		 vedisGenError(pStore,"xDelete() method not implemented in the underlying storage engine");
		 rc = VEDIS_NOTIMPLEMENTED;
	 }else{
		 if( nKeyLen < 0 ){
			 /* Assume a null terminated string and compute its length */
			 nKeyLen = SyStrlen((const char *)pKey);
		 }
		 if( !nKeyLen ){
			 vedisGenError(pStore,"Empty key");
			 rc = VEDIS_EMPTY;
		 }else{
			 /* Seek to the record position */
			 rc = pMethods->xSeek(pCur,pKey,nKeyLen,VEDIS_CURSOR_MATCH_EXACT);
		 }
		 if( rc == VEDIS_OK ){
			 /* Exact match found, delete the entry */
			 rc = pMethods->xDelete(pCur);
		 }
	 }
	return rc;
}
/*
 * [CAPIREF: vedis_kv_config()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_config(vedis *pStore,int iOp,...)
{
	vedis_kv_engine *pEngine;
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Point to the underlying storage engine */
	 pEngine = vedisPagerGetKvEngine(pStore);
	 if( pEngine->pIo->pMethods->xConfig == 0 ){
		 /* Storage engine does not implements such method */
		 vedisGenError(pStore,"xConfig() method not implemented in the underlying storage engine");
		 rc = VEDIS_NOTIMPLEMENTED;
	 }else{
		 va_list ap;
		 /* Configure the storage engine */
		 va_start(ap,iOp);
		 rc = pEngine->pIo->pMethods->xConfig(pEngine,iOp,ap);
		 va_end(ap);
	 }
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * [CAPIREF: vedis_kv_delete()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_kv_delete(vedis *pStore,const void *pKey,int nKeyLen)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 rc = vedisKvDelete(pStore,pKey,nKeyLen);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	return rc;
}
/*
 * [CAPIREF: vedis_context_kv_store()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_kv_store(vedis_context *pCtx,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen)
{
	int rc;
	rc = vedisKvStore(pCtx->pVedis,pKey,nKeyLen,pData,nDataLen);
	return rc;
}
/*
 * [CAPIREF: vedis_context_kv_append()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_kv_append(vedis_context *pCtx,const void *pKey,int nKeyLen,const void *pData,vedis_int64 nDataLen)
{
	int rc;
	rc = vedisKvAppend(pCtx->pVedis,pKey,nKeyLen,pData,nDataLen);
	return rc;
}
/*
 * [CAPIREF: vedis_context_kv_store_fmt()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_kv_store_fmt(vedis_context *pCtx,const void *pKey,int nKeyLen,const char *zFormat,...)
{
	SyBlob sWorker; /* Working buffer */
	va_list ap;
	int rc;
	SyBlobInit(&sWorker,&pCtx->pVedis->sMem);
	/* Format the data */
	va_start(ap,zFormat);
	SyBlobFormatAp(&sWorker,zFormat,ap);
	va_end(ap);
	/* Perform the requested operation */
	rc = vedisKvStore(pCtx->pVedis,pKey,nKeyLen,SyBlobData(&sWorker),SyBlobLength(&sWorker));
	/* Clean up */
	SyBlobRelease(&sWorker);
	return rc;
}
/*
 * [CAPIREF: vedis_context_kv_append_fmt()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_kv_append_fmt(vedis_context *pCtx,const void *pKey,int nKeyLen,const char *zFormat,...)
{
	SyBlob sWorker; /* Working buffer */
	va_list ap;
	int rc;
	SyBlobInit(&sWorker,&pCtx->pVedis->sMem);
	/* Format the data */
	va_start(ap,zFormat);
	SyBlobFormatAp(&sWorker,zFormat,ap);
	va_end(ap);
	/* Perform the requested operation */
	rc = vedisKvAppend(pCtx->pVedis,pKey,nKeyLen,SyBlobData(&sWorker),SyBlobLength(&sWorker));
	/* Clean up */
	SyBlobRelease(&sWorker);
	return rc;
}
/*
 * [CAPIREF: vedis_context_kv_fetch()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_kv_fetch(vedis_context *pCtx,const void *pKey,int nKeyLen,void *pBuf,vedis_int64 /* in|out */*pBufLen)
{
	int rc;
	rc = vedisKvFetch(pCtx->pVedis,pKey,nKeyLen,pBuf,pBufLen);
	return rc;
}
/*
 * [CAPIREF: vedis_context_kv_fetch_callback()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_kv_fetch_callback(vedis_context *pCtx,const void *pKey,
	                    int nKeyLen,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData)
{
	int rc;
	rc = vedisKvFetchCallback(pCtx->pVedis,pKey,nKeyLen,xConsumer,pUserData);
	return rc;
}
/*
 * [CAPIREF: vedis_context_kv_delete()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_context_kv_delete(vedis_context *pCtx,const void *pKey,int nKeyLen)
{
	int rc;
	rc = vedisKvDelete(pCtx->pVedis,pKey,nKeyLen);
	return rc;
}
/*
 * [CAPIREF: vedis_begin()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_begin(vedis *pStore)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Begin the write transaction */
	 rc = vedisPagerBegin(pStore->pPager);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	 return rc;
}
/*
 * [CAPIREF: vedis_commit()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_commit(vedis *pStore)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Commit the transaction */
	 rc = vedisPagerCommit(pStore->pPager);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	 return rc;
}
/*
 * [CAPIREF: vedis_rollback()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
int vedis_rollback(vedis *pStore)
{
	int rc;
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Rollback the transaction */
	 rc = vedisPagerRollback(pStore->pPager,TRUE);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	 return rc;
}
/*
 * [CAPIREF: vedis_util_random_string()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
 int vedis_util_random_string(vedis *pStore,char *zBuf,unsigned int buf_size)
{
	if( VEDIS_DB_MISUSE(pStore) ){
		return VEDIS_CORRUPT;
	}
	if( zBuf == 0 || buf_size < 3 ){
		/* Buffer must be long enough to hold three bytes */
		return VEDIS_INVALID;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return VEDIS_ABORT; /* Another thread have released this instance */
	 }
#endif
	 /* Generate the random string */
	 vedisPagerRandomString(pStore->pPager,zBuf,buf_size);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	 return VEDIS_OK;
}
/*
 * [CAPIREF: vedis_util_random_num()]
 * Please refer to the official documentation for function purpose and expected parameters.
 */
 unsigned int vedis_util_random_num(vedis *pStore)
{
	sxu32 iNum;
	if( VEDIS_DB_MISUSE(pStore) ){
		return 0;
	}
#if defined(VEDIS_ENABLE_THREADS)
	 /* Acquire DB mutex */
	 SyMutexEnter(sVedisMPGlobal.pMutexMethods, pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
	 if( sVedisMPGlobal.nThreadingLevel > VEDIS_THREAD_LEVEL_SINGLE && 
		 VEDIS_THRD_DB_RELEASE(pStore) ){
			 return 0; /* Another thread have released this instance */
	 }
#endif
	 /* Generate the random number */
	 iNum = vedisPagerRandomNum(pStore->pPager);
#if defined(VEDIS_ENABLE_THREADS)
	 /* Leave DB mutex */
	 SyMutexLeave(sVedisMPGlobal.pMutexMethods,pStore->pMutex); /* NO-OP if sVedisMPGlobal.nThreadingLevel != VEDIS_THREAD_LEVEL_MULTI */
#endif
	 return iNum;
}
/* END-OF-IMPLEMENTATION: vedis@embedded@symisc 34-09-46 */
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
