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

#include "tsdbFS.h"
#include "tchecksum.h"
#include "tcrc32c.h"
#include "tcoding.h"
#include "tsdbFile.h"



static threadlocal int32_t tsErrno;
int32_t* taosGetErrno() {
  return &tsErrno;
}

static const char *TSDB_FNAME_SUFFIX[] = {
    "head",  // TSDB_FILE_HEAD
    "data",  // TSDB_FILE_DATA
    "last",  // TSDB_FILE_LAST
    "smad",  // TSDB_FILE_SMA_DATA(Small Materialized Aggregate for .data File)
    "smal",  // TSDB_FILE_SMA_LAST(Small Materialized Aggregate for .last File)
    "",      // TSDB_FILE_MAX
    "meta",  // TSDB_FILE_META
};

static int   tsdbEncodeDFInfo(void **buf, SDFInfo *pInfo);
static void *tsdbDecodeDFInfo(void *buf, SDFInfo *pInfo, TSDB_FVER_TYPE sfver);
static int   tsdbRollBackDFile(SDFile *pDFile);


// ============== Operations on SDFile


int tsdbLoadDFileHeader(SDFile *pDFile, SDFInfo *pInfo) {
  char     buf[TSDB_FILE_HEAD_SIZE] = "\0";
  // uint32_t _version;

  ASSERT(TSDB_FILE_OPENED(pDFile));

  if (tsdbSeekDFile(pDFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  if (tsdbReadDFile(pDFile, buf, TSDB_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    return -1;
  }

  void *pBuf = buf;
  pBuf = tsdbDecodeDFInfo(pBuf, pInfo, TSDB_LATEST_FVER);  // only make sure the parameter sfver > 0
  return 0;
}

static int tsdbEncodeDFInfo(void **buf, SDFInfo *pInfo) {
  int tlen = 0;
  tlen += taosEncodeFixedU32(buf, pInfo->fver);
  tlen += taosEncodeFixedU32(buf, pInfo->magic);
  tlen += taosEncodeFixedU32(buf, pInfo->len);
  tlen += taosEncodeFixedU32(buf, pInfo->totalBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->totalSubBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->offset);
  tlen += taosEncodeFixedU64(buf, pInfo->size);
  tlen += taosEncodeFixedU64(buf, pInfo->tombSize);

  return tlen;
}

static void *tsdbDecodeDFInfo(void *buf, SDFInfo *pInfo, TSDB_FVER_TYPE sfver) {
  if (sfver > TSDB_FS_VER_0) {
    buf = taosDecodeFixedU32(buf, &(pInfo->fver));
  } else {
    pInfo->fver = TSDB_FS_VER_0;  // default value
  }
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->len));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalSubBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->offset));
  buf = taosDecodeFixedU64(buf, &(pInfo->size));
  buf = taosDecodeFixedU64(buf, &(pInfo->tombSize));

  return buf;
}
