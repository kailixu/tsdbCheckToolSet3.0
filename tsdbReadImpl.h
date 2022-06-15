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

#ifndef _TD_TSDB_READ_IMPL_H_
#define _TD_TSDB_READ_IMPL_H_

#include "tcommon.h"
#include "t_array.h"
#include "tchecksum.h"
#include "tsdbFS.h"
#include "tsdb.h"
#include "osMemory.h"

typedef struct SReadH SReadH;

typedef struct
{
  uint64_t suid;
  uint64_t uid;
  uint32_t len;
  uint32_t offset;
  uint32_t hasLast : 2;
  uint32_t numOfBlocks : 30;
  TSDBKEY maxKey;
} SBlockIdx;

typedef enum
{
  TSDB_SBLK_VER_0 = 0,
  TSDB_SBLK_VER_MAX,
} ESBlockVer;

#define SBlockVerLatest TSDB_SBLK_VER_0

typedef struct
{
  uint8_t last : 1;
  uint8_t hasDupKey : 1; // 0: no dup TS key, 1: has dup TS key(since supporting Multi-Version)
  uint8_t blkVer : 6;
  uint8_t numOfSubBlocks;
  col_id_t numOfCols;   // not including timestamp column
  uint32_t len;         // data block length
  uint32_t keyLen : 20; // key column length, keyOffset = offset+sizeof(SBlockData)+sizeof(SBlockCol)*numOfCols
  uint32_t algorithm : 4;
  uint32_t reserve : 8;
  col_id_t numOfBSma;
  uint16_t numOfRows;
  int64_t offset;
  uint64_t aggrStat : 1;
  uint64_t aggrOffset : 63;
  TSDBKEY minKey;
  TSDBKEY maxKey;
} SBlock;

typedef struct
{
  int32_t delimiter; // For recovery usage
  uint64_t suid;
  uint64_t uid;
  SBlock blocks[];
} SBlockInfo;

typedef struct
{
  int16_t colId;
  uint16_t type : 6;
  uint16_t blen : 10; // 0 no bitmap if all rows are NORM, > 0 bitmap length
  uint32_t len;       // data length + bitmap length
  uint32_t offset;
} SBlockCol;

typedef struct
{
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SAggrBlkCol;

typedef struct
{
  int32_t delimiter; // For recovery usage
  int32_t numOfCols; // For recovery usage
  uint64_t uid;      // For recovery usage
  SBlockCol cols[];
} SBlockData;

typedef void SAggrBlkData; // SBlockCol cols[];

struct SReadH
{
  int32_t repoId;
  STableId tableId;
  SDFile *headFile;   // FSET to read fd
  SArray *aBlkIdx;    // SBlockIdx array
  SBlockIdx *pBlkIdx; // current reading table SBlockIdx
  int cidx;
  SBlockInfo *pBlkInfo; // SBlockInfoV#
  void *pBuf;           // buffer
  void *pCBuf;          // compression buffer
  void *pExBuf;         // extra buffer
};

#define TSDB_READ_REPO(rh) ((rh)->pRepo)
#define TSDB_READ_REPO_ID(rh) ((rh)->repoId)
#define TSDB_READ_FSET(rh) (&((rh)->rSet))
#define TSDB_READ_TABLE(rh) ((rh)->pTable)
#define TSDB_READ_HEAD_FILE(rh) ((rh)->headFile)

#define TSDB_READ_BUF(rh) ((rh)->pBuf)
#define TSDB_READ_COMP_BUF(rh) ((rh)->pCBuf)
#define TSDB_READ_EXBUF(rh) ((rh)->pExBuf)

#define TSDB_BLOCK_STATIS_SIZE(ncols, blkVer) (sizeof(SBlockData) + sizeof(SBlockCol) * (ncols) + sizeof(TSCKSUM))

static FORCE_INLINE size_t tsdbBlockStatisSize(int nCols, uint32_t blkVer)
{
  switch (blkVer)
  {
  case TSDB_SBLK_VER_0:
  default:
    return TSDB_BLOCK_STATIS_SIZE(nCols, 0);
  }
}

#define TSDB_BLOCK_AGGR_SIZE(ncols, blkVer) (sizeof(SAggrBlkCol) * (ncols) + sizeof(TSCKSUM))

static FORCE_INLINE size_t tsdbBlockAggrSize(int nCols, uint32_t blkVer)
{
  switch (blkVer)
  {
  case TSDB_SBLK_VER_0:
  default:
    return TSDB_BLOCK_AGGR_SIZE(nCols, 0);
  }
}

int tsdbInitReadH(SReadH *pReadh);
void tsdbDestroyReadH(SReadH *pReadh);
int tsdbSetAndOpenReadFSet(SReadH *pReadh);
void tsdbCloseAndUnsetFSet(SReadH *pReadh);
int tsdbLoadBlockIdx(SReadH *pReadh);
int tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget);
int tsdbEncodeSBlockIdx(void **buf, SBlockIdx *pIdx);
void *tsdbDecodeSBlockIdx(void *buf, SBlockIdx *pIdx);
int tsdbSetReadTable(SReadH *pReadh, uint64_t suid, uint64_t uid);
void tsdbResetReadFile(SReadH *pReadh);

static FORCE_INLINE int tsdbMakeRoom(void **ppBuf, size_t size)
{
  void *pBuf = *ppBuf;
  size_t tsize = taosTSizeof(pBuf);

  if (tsize < size)
  {
    if (tsize == 0)
      tsize = 1024;

    while (tsize < size)
    {
      tsize *= 2;
    }

    *ppBuf = taosTRealloc(pBuf, tsize);
    if (*ppBuf == NULL)
    {
      return -1;
    }
  }

  return 0;
}

#endif /*_TD_TSDB_READ_IMPL_H_*/