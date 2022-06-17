/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef D1C30CB2_2CF5_410C_8658_A58E1248D0E5
#define D1C30CB2_2CF5_410C_8658_A58E1248D0E5


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/time.h>
#include <dirent.h>
#include <sys/types.h>
#include <regex.h>
#include <assert.h>
#include <errno.h>

#define TSDB_CODE_SUCCESS 0

int32_t* taosGetErrno();
#define terrno                              (*taosGetErrno())

#ifdef _ISOC11_SOURCE
  #define threadlocal _Thread_local
#elif defined(__APPLE__)
  #define threadlocal __thread
#elif defined(__GNUC__) && !defined(threadlocal)
  #define threadlocal __thread
#else
  #define threadlocal __declspec( thread )
#endif


#if defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE
#endif

#if defined(__GNUC__)
#define UNUSED_PARAM(x) _UNUSED##x __attribute__((unused))
#define UNUSED_FUNC __attribute__((unused))
#else
#define UNUSED_PARAM(x) x
#define UNUSED_FUNC
#endif

#define POINTER_SHIFT(p, b) ((void *)((char *)(p) + (b)))
#define POINTER_DISTANCE(p1, p2) ((char *)(p1) - (char *)(p2)) 

#define TSDB_TIME_PRECISION_MILLI 0
#define TSDB_TIME_PRECISION_MICRO 1
#define TSDB_TIME_PRECISION_NANO 2

#define TSDB_FILENAME_LEN 128

#define TSDB_DATA_NCHAR_NULL 0xFFFFFFFF
#define TSDB_DATA_BINARY_NULL 0xFF

#define BENCH_TEST_LOOP 2000000000

#define VV_MM_TT (sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(int8_t) + sizeof(uint16_t))
#define VV_DIRECT 10

#define tstrncpy(dst, src, size)       \
	do                                 \
	{                                  \
		strncpy((dst), (src), (size)); \
		(dst)[(size)-1] = 0;           \
	} while (0)

static FORCE_INLINE int64_t taosGetTimestampMs();

static FORCE_INLINE int64_t taosGetTimestampMs()
{
	struct timeval systemTime;
	gettimeofday(&systemTime, NULL);
	return (int64_t)systemTime.tv_sec * 1000L + (int64_t)systemTime.tv_usec / 1000;
}
static FORCE_INLINE int64_t taosGetTimestampUs()
{
	struct timeval systemTime;
	gettimeofday(&systemTime, NULL);
	return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

typedef struct
{
  uint64_t  suid;
  uint64_t  uid;
} STableId;

#ifdef TSKEY32
#define TSKEY int32_t;
#else
#define TSKEY int64_t
#endif

typedef struct {
  uint32_t magic;
  uint32_t len;
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
  uint32_t offset;
  uint64_t size;
  uint64_t tombSize;
  uint32_t fver;
} SDFInfo;

typedef struct {
  int  level;
  int  id;
  char rname[TSDB_FILENAME_LEN];  // REL name
  char aname[TSDB_FILENAME_LEN];  // ABS name
} TFILE;

typedef struct {
  SDFInfo info;
  TFILE   f;
  int     fd;
  uint8_t state;
} SDFile;

#define TFILE_LEVEL(pf) ((pf)->level)
#define TFILE_ID(pf) ((pf)->id)
#define TFILE_NAME(pf) ((pf)->aname)
#define TFILE_REL_NAME(pf) ((pf)->rname)

#define ASSERT(x) assert(x)

typedef enum
{
  CHECK_TYPE_UNDEFINED = 0,
  CHECK_LIST_DATA_OF_VID_UID_TID = 1,
  CHECK_LIST_DATA_OF_VID = 2,
  CHECK_LIST_DATA_OF_VNODES = 3,
  CHECK_TYPE_MAX,
} ECheckType;

#define TD_EQ 0x1
#define TD_GT 0x2
#define TD_LT 0x4
#define TD_GE (TD_EQ | TD_GT)
#define TD_LE (TD_EQ | TD_LT)

typedef uint16_t col_id_t;

#endif /* D1C30CB2_2CF5_410C_8658_A58E1248D0E5 */
