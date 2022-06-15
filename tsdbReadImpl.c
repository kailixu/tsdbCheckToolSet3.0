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

#include "tsdbReadImpl.h"
#include "tchecksum.h"
#include "tcrc32c.h"
#include "tcoding.h"
#include "osMemory.h"


#define TSDB_KEY_COL_OFFSET 0

int tsdbInitReadH(SReadH *pReadh) {
  ASSERT(pReadh != NULL);

  memset((void *)pReadh, 0, sizeof(*pReadh));

  pReadh->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pReadh->aBlkIdx == NULL)
  {
    printf("vgId:%d OOM during init pReadh->aBlkIdx\n", TSDB_READ_REPO_ID(pReadh));
    return -1;
  }

  return 0;
}

void tsdbDestroyReadH(SReadH *pReadh) {
  if (pReadh == NULL) return;
  pReadh->pExBuf = taosTZfree(pReadh->pExBuf);
  pReadh->pCBuf = taosTZfree(pReadh->pCBuf);
  pReadh->pBuf = taosTZfree(pReadh->pBuf);
  pReadh->pBlkInfo = taosTZfree(pReadh->pBlkInfo);
  pReadh->cidx = 0;
  pReadh->pBlkIdx = NULL;
  pReadh->aBlkIdx = taosArrayDestroy(pReadh->aBlkIdx);
  tsdbCloseDFile(pReadh->headFile);
}


int tsdbLoadBlockIdx(SReadH *pReadh) {
  SDFile   *pHeadf = TSDB_READ_HEAD_FILE(pReadh);
  SBlockIdx blkIdx;

  ASSERT(taosArrayGetSize(pReadh->aBlkIdx) == 0);

  // No data at all, just return
  if (pHeadf->info.offset <= 0) return 0;

  if (tsdbSeekDFile(pHeadf, pHeadf->info.offset, SEEK_SET) < 0) {
    printf("vgId:%d, failed to load SBlockIdx part while seek file %s since %d, offset:%u len :%u\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), terrno, pHeadf->info.offset,
              pHeadf->info.len);
    return -1;
  }

  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadh)), pHeadf->info.len) < 0) return -1;

  int64_t nread = tsdbReadDFile(pHeadf, TSDB_READ_BUF(pReadh), pHeadf->info.len);
  if (nread < 0) {
    printf("vgId:%d, failed to load SBlockIdx part while read file %s since %d, offset:%u len :%u\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), terrno, pHeadf->info.offset,
              pHeadf->info.len);
    return -1;
  }

  if (nread < pHeadf->info.len) {
    printf("vgId:%d, SBlockIdx part in file %s is corrupted, offset:%u expected bytes:%u read bytes: %" PRId64 "\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pHeadf->info.offset, pHeadf->info.len, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)TSDB_READ_BUF(pReadh), pHeadf->info.len)) {
    printf("vgId:%d, SBlockIdx part in file %s is corrupted since wrong checksum, offset:%u len :%u\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pHeadf->info.offset, pHeadf->info.len);
    return -1;
  }

  void *ptr = TSDB_READ_BUF(pReadh);
  int   tsize = 0;
  while (POINTER_DISTANCE(ptr, TSDB_READ_BUF(pReadh)) < (pHeadf->info.len - sizeof(TSCKSUM))) {
    ptr = tsdbDecodeSBlockIdx(ptr, &blkIdx);
    ASSERT(ptr != NULL);

    if (taosArrayPush(pReadh->aBlkIdx, (void *)(&blkIdx)) == NULL) {
      printf("vgId:%d, taos array push blkIdx OOM for %s \n",
             TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf));
      return -1;
    }

    tsize++;
    // ASSERT(tsize == 1 || ((SBlockIdx *)taosArrayGet(pReadh->aBlkIdx, tsize - 2))->tid <
    //                          ((SBlockIdx *)taosArrayGet(pReadh->aBlkIdx, tsize - 1))->tid);
  }

  return 0;
}


int tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget) {
  ASSERT(pReadh->pBlkIdx != NULL);

  SDFile    *pHeadf = TSDB_READ_HEAD_FILE(pReadh);
  SBlockIdx *pBlkIdx = pReadh->pBlkIdx;

  if (tsdbSeekDFile(pHeadf, pBlkIdx->offset, SEEK_SET) < 0) {
    printf("vgId:%d, failed to load SBlockInfo part while seek file %s since %d, offset:%u len:%u\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), terrno, pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (tsdbMakeRoom((void **)(&(pReadh->pBlkInfo)), pBlkIdx->len) < 0) return -1;

  int64_t nread = tsdbReadDFile(pHeadf, (void *)(pReadh->pBlkInfo), pBlkIdx->len);
  if (nread < 0) {
    printf("vgId:%d, failed to load SBlockInfo part while read file %s since %d, offset:%u len :%u\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), terrno, pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (nread < pBlkIdx->len) {
    printf("vgId:%d, SBlockInfo part in file %s is corrupted, offset:%u expected bytes:%u read bytes:%" PRId64 "\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->offset, pBlkIdx->len, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)(pReadh->pBlkInfo), pBlkIdx->len)) {
    printf("vgId:%d, SBlockInfo part in file %s is corrupted since wrong checksum, offset:%u len :%u\n",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  } else {
    SBlockInfo *pBlkInfo = pReadh->pBlkInfo;
    SBlock *pBlk = NULL;
    int         nBlks = (pBlkIdx->len - sizeof(SBlockInfo)) / sizeof(SBlock);
    for (int n = 0; n < nBlks; ++n) {
      pBlk = &pBlkInfo->blocks[n];
      if (pBlk->numOfSubBlocks >= 1) {
        printf("prop:vgId:%d, file %s , offset:%u len :%u has %d rows\n", TSDB_READ_REPO_ID(pReadh),
                  TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->offset, pBlkIdx->len, pBlk->numOfRows);
      }
    }
  }

  ASSERT(pBlkIdx->suid == pReadh->pBlkInfo->suid && pBlkIdx->uid == pReadh->pBlkInfo->uid);

  if (pTarget) {
    memcpy(pTarget, (void *)(pReadh->pBlkInfo), pBlkIdx->len);
  }

  return 0;
}

static int32_t tsdbBlockIdxCmprFn(const void *p1, const void *p2) {
  SBlockIdx *pBlockIdx1 = (SBlockIdx *)p1;
  SBlockIdx *pBlockIdx2 = (SBlockIdx *)p2;

  if (pBlockIdx1->suid < pBlockIdx2->suid) {
    return -1;
  } else if (pBlockIdx1->suid > pBlockIdx2->suid) {
    return 1;
  }

  if (pBlockIdx1->uid < pBlockIdx2->uid) {
    return -1;
  } else if (pBlockIdx1->uid > pBlockIdx2->uid) {
    return 1;
  }

  return 0;
}

int tsdbSetReadTable(SReadH *pReadh, 	uint64_t suid, uint64_t uid) {
  pReadh->tableId.suid = suid;
  pReadh->tableId.uid = uid;

  uint8_t *p = taosArraySearch(pReadh->aBlkIdx, &(SBlockIdx){.suid = suid, .uid = uid},
                               tsdbBlockIdxCmprFn, TD_EQ);
  if (p == NULL) {
    pReadh->pBlkIdx = NULL;
  } else {
    pReadh->pBlkIdx = (SBlockIdx *)p;
  }

  return 0;
}




int tsdbEncodeSBlockIdx(void **buf, SBlockIdx *pIdx) {
  int tlen = 0;

  tlen += taosEncodeFixedU64(buf, pIdx->suid);
  tlen += taosEncodeFixedU64(buf, pIdx->uid);
  tlen += taosEncodeVariantU32(buf, pIdx->len);
  tlen += taosEncodeVariantU32(buf, pIdx->offset);
  tlen += taosEncodeFixedU8(buf, pIdx->hasLast);
  tlen += taosEncodeVariantU32(buf, pIdx->numOfBlocks);
  tlen += taosEncodeFixedU64(buf, pIdx->maxKey.ts);

  return tlen;
}

void *tsdbDecodeSBlockIdx(void *buf, SBlockIdx *pIdx) {
  uint8_t  hasLast = 0;
  uint32_t numOfBlocks = 0;
  uint64_t value = 0;

  // if ((buf = taosDecodeVariantI32(buf, &(pIdx->tid))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->suid = (int64_t)value;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->uid = (int64_t)value;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->len))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->offset))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU8(buf, &(hasLast))) == NULL) return NULL;
  pIdx->hasLast = hasLast;
  if ((buf = taosDecodeVariantU32(buf, &(numOfBlocks))) == NULL) return NULL;
  pIdx->numOfBlocks = numOfBlocks;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->maxKey.ts = (TSKEY)value;

  return buf;
}

static void tsdbResetReadTable(SReadH *pReadh) {
  pReadh->cidx = 0;
  pReadh->pBlkIdx = NULL;
}

void tsdbResetReadFile(SReadH *pReadh) {
  tsdbResetReadTable(pReadh);
  taosArrayClear(pReadh->aBlkIdx);
  tsdbCloseDFile(pReadh->headFile);
}