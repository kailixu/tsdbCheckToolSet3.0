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

#ifndef _TS_TSDB_FILE_H_
#define _TS_TSDB_FILE_H_
#include "tcommon.h"
#include "osFile.h"

#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F
#define TSDB_FILE_INIT_MAGIC 0xFFFFFFFF
#define TSDB_IVLD_FID INT_MIN
#define TSDB_FILE_STATE_OK 0
#define TSDB_FILE_STATE_BAD 1

#define TSDB_FILE_INFO(tf) (&((tf)->info))
#define TSDB_FILE_F(tf) (&((tf)->f))
#define TSDB_FILE_FD(tf) ((tf)->fd)
#define TSDB_FILE_FULL_NAME(tf) TFILE_NAME(TSDB_FILE_F(tf))
#define TSDB_FILE_OPENED(tf) (TSDB_FILE_FD(tf) >= 0)
#define TSDB_FILE_CLOSED(tf) (!TSDB_FILE_OPENED(tf))
#define TSDB_FILE_SET_CLOSED(f) (TSDB_FILE_FD(f) = -1)
#define TSDB_FILE_LEVEL(tf) TFILE_LEVEL(TSDB_FILE_F(tf))
#define TSDB_FILE_ID(tf) TFILE_ID(TSDB_FILE_F(tf))
#define TSDB_FILE_FSYNC(tf) taosFsync(TSDB_FILE_FD(tf))
#define TSDB_FILE_STATE(tf) ((tf)->state)
#define TSDB_FILE_SET_STATE(tf, s) ((tf)->state = (s))
#define TSDB_FILE_IS_OK(tf) (TSDB_FILE_STATE(tf) == TSDB_FILE_STATE_OK)
#define TSDB_FILE_IS_BAD(tf) (TSDB_FILE_STATE(tf) == TSDB_FILE_STATE_BAD)
#define ASSERT_TSDB_FSET_NFILES_VALID(s)                              \
  do {                                                                \
    uint8_t nDFiles = tsdbGetNFiles(s);                               \
    ASSERT((nDFiles >= TSDB_FILE_MIN) && (nDFiles <= TSDB_FILE_MAX)); \
  } while (0)
typedef enum {
  TSDB_FILE_HEAD = 0,
  TSDB_FILE_DATA,
  TSDB_FILE_LAST,
  TSDB_FILE_SMAD,  // sma for .data
  TSDB_FILE_SMAL,  // sma for .last
  TSDB_FILE_MAX,
  TSDB_FILE_META
} TSDB_FILE_T;

#define TSDB_FILE_MIN 3U  // min valid number of files in one DFileSet(.head/.data/.last)

// =============== SMFile
typedef struct {
  int64_t  size;
  int64_t  tombSize;
  int64_t  nRecords;
  int64_t  nDels;
  uint32_t magic;
} SMFInfo;

typedef struct {
  SMFInfo info;
  TFILE   f;
  int     fd;
  uint8_t state;
} SMFile;


// // =============== SDFile
// typedef struct {
//   uint32_t magic;
//   uint32_t len;
//   uint32_t totalBlocks;
//   uint32_t totalSubBlocks;
//   uint32_t offset;
//   uint64_t size;
//   uint64_t tombSize;
//   uint32_t fver;
// } SDFInfo;

// typedef struct {
//   SDFInfo info;
//   TFILE   f;
//   int     fd;
//   uint8_t state;
// } SDFile;


int   tsdbEncodeSDFile(void** buf, SDFile* pDFile);
int   tsdbLoadDFileHeader(SDFile* pDFile, SDFInfo* pInfo);
int   tsdbParseDFilename(const char* fname, int* vid, int* fid, TSDB_FILE_T* ftype, uint32_t* version);

static FORCE_INLINE void tsdbSetDFileInfo(SDFile* pDFile, SDFInfo* pInfo) { pDFile->info = *pInfo; }

static FORCE_INLINE int tsdbOpenDFile(SDFile* pDFile, int flags) {
  ASSERT(!TSDB_FILE_OPENED(pDFile));

  pDFile->fd = open(TSDB_FILE_FULL_NAME(pDFile), flags);
  if (pDFile->fd < 0) {
    return -1;
  }

  return 0;
}

static FORCE_INLINE void tsdbCloseDFile(SDFile* pDFile) {
  if (TSDB_FILE_OPENED(pDFile)) {
    close(pDFile->fd);
    TSDB_FILE_SET_CLOSED(pDFile);
  }
}

static FORCE_INLINE int64_t tsdbSeekDFile(SDFile* pDFile, int64_t offset, int whence) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t loffset = taosLSeek(TSDB_FILE_FD(pDFile), offset, whence);
  if (loffset < 0) {
    return -1;
  }

  return loffset;
}


static FORCE_INLINE int64_t tsdbReadDFile(SDFile* pDFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nread = taosRead(pDFile->fd, buf, nbyte);
  if (nread < 0) {
    return -1;
  }

  return nread;
}

#endif /* _TS_TSDB_FILE_H_ */