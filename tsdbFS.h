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

#ifndef _TD_TSDB_FS_H_
#define _TD_TSDB_FS_H_
#include "tcommon.h"
#include "tsdbFile.h"
/**
 * 1. The fileset .head/.data/.last use the same fver 0 before 2021.10.10.
 * 2. .head fver is 1 when extract aggregate block data from .data/.last file and save to separate .smad/.smal file
 * since 2021.10.10
 * // TODO update date and add release version.
 */
typedef enum {
  TSDB_FS_VER_0 = 0,
  TSDB_FS_VER_1,
} ETsdbFsVer;

#define TSDB_FVER_TYPE uint32_t
#define TSDB_LATEST_FVER TSDB_FS_VER_1     // latest version for DFile
#define TSDB_LATEST_SFS_VER TSDB_FS_VER_1  // latest version for 'current' file

static FORCE_INLINE uint32_t tsdbGetDFSVersion(TSDB_FILE_T fType) {  // latest version for DFile
  switch (fType) {
    case TSDB_FILE_HEAD:
      return TSDB_FS_VER_1;
    default:
      return TSDB_FS_VER_0;
  }
}


// ================== CURRENT file header info
typedef struct {
  uint32_t version;  // Current file system version (relating to code)
  uint32_t len;      // Encode content length (including checksum)
} SFSHeader;

#endif /* _TD_TSDB_FS_H_ */
