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

#include "tcommon.h"
#include "t_array.h"
#include "tsdbFile.h"
#include "tsdbReadImpl.h"

STableId gTableId = {0};
int32_t gVgId = -1;
int32_t gCheckType = CHECK_TYPE_UNDEFINED;
char gUidDir[4000] = {0};

char gConfigDir[4000] = {0};
char gDataDir[4000] = {0};

static int32_t dataStatisCheckType1(SReadH *pReadH, uint64_t suid, uint64_t uid, int32_t vgId, uint32_t *nRows);
static int32_t dataStatisCheckType2(SReadH *pReadH, int32_t vgId, uint32_t *nRows);
static int32_t getDFile(SReadH *pReadH, const char *fullName);
static int32_t tsdbGetRowsOfBlockInfo(SReadH *pReadH, uint32_t *nRows);

int32_t checkParams(int32_t argc, char *argv[])
{
	char *pEnd = NULL;
	for (int32_t i = 1; i < argc; ++i)
	{
		if (strcmp(argv[i], "-check") == 0)
		{
			if (i < argc - 1)
			{
				char *checkType = argv[++i];
				if (strspn(checkType, "0123456789") != strlen(checkType))
				{
					printf("invalid parameter for '-check'\n");
					exit(EXIT_FAILURE);
				}
				else
				{
					gCheckType = atoi(checkType);
				}
			}
			else
			{
				printf("'-check' requires a parameter\n");
				exit(EXIT_FAILURE);
			}
		}
		else if (strcmp(argv[i], "-c") == 0)
		{
			if (i < argc - 1)
			{
				if (strlen(argv[++i]) >= TSDB_FILENAME_LEN)
				{
					printf("config file path overflow");
					exit(EXIT_FAILURE);
				}
				tstrncpy(gConfigDir, argv[i], TSDB_FILENAME_LEN);
			}
			else
			{
				printf("'-c' requires a parameter, default:%s\n", gConfigDir);
				exit(EXIT_FAILURE);
			}
		}
		else if (strcmp(argv[i], "-d") == 0)
		{
			if (i < argc - 1)
			{
				if (strlen(argv[++i]) >= TSDB_FILENAME_LEN)
				{
					printf("data file path overflow");
					exit(EXIT_FAILURE);
				}
				tstrncpy(gDataDir, argv[i], TSDB_FILENAME_LEN);
			}
			else
			{
				printf("'-c' requires a parameter, default:%s\n", gDataDir);
				exit(EXIT_FAILURE);
			}
		}
		else if (strcmp(argv[i], "-suid") == 0)
		{
			if (i < argc - 1)
			{
				char *suid = argv[++i];
				if (strspn(suid, "0123456789") != strlen(suid))
				{
					printf("invalid parameter for '-suid'\n");
					exit(EXIT_FAILURE);
				}
				else
				{
					gTableId.suid = strtoull(suid, &pEnd, 10);
				}
			}
			else
			{
				printf("'-suid' requires a parameter\n");
				exit(EXIT_FAILURE);
			}
		}
		else if (strcmp(argv[i], "-uid") == 0)
		{
			if (i < argc - 1)
			{
				char *uid = argv[++i];
				if (strspn(uid, "0123456789") != strlen(uid))
				{
					printf("invalid parameter for '-uid'\n");
					exit(EXIT_FAILURE);
				}
				else
				{
					gTableId.uid = strtoull(uid, &pEnd, 10);
				}
			}
			else
			{
				printf("'-uid' requires a parameter\n");
				exit(EXIT_FAILURE);
			}
		}
		else if (strcmp(argv[i], "-vid") == 0)
		{
			if (i < argc - 1)
			{
				char *vgId = argv[++i];
				if (strspn(vgId, "0123456789") != strlen(vgId))
				{
					printf("invalid parameter for '-vid'\n");
					exit(EXIT_FAILURE);
				}
				else
				{
					gVgId = atol(vgId);
				}
			}
			else
			{
				printf("'-vid' requires a parameter\n");
				exit(EXIT_FAILURE);
			}
		}
	}

	if (gConfigDir[0] == 0)
	{
		strcpy(gConfigDir, "/etc/taos");
	}

	if (gDataDir[0] == 0)
	{
		strcpy(gDataDir, "/var/lib/taos");
	}

	return 0;
}

static int tsdbFetchDir(const char *dataDir, SArray **fArray)
{
	//   const char * pattern = "^v[0-9]+f[0-9]+\\.(head|data|last|smad|smal)(-ver[0-9]+)?$";
	const char *pattern = "^v[0-9]+f[0-9]+\\.(head)(-ver[0-9]+)?$";
	regex_t regex;
	struct dirent *ptr = NULL;

	// Resource allocation and init
	regcomp(&regex, pattern, REG_EXTENDED);

	*fArray = taosArrayInit(1024, TSDB_FILENAME_LEN);
	if (*fArray == NULL)
	{
		printf("%" PRId64 ":%s failed to fetch TFileSet Array from %s since OOM\n", taosGetTimestampMs(), __func__, dataDir);
		regfree(&regex);
		return -1;
	}

	DIR *dir = opendir(dataDir);
	if (dir == NULL)
	{
		printf("%" PRId64 ":%s failed to open dir %s\n", taosGetTimestampMs(), __func__, dataDir);
		regfree(&regex);
		taosArrayDestroy(*fArray);
		return -1;
	}

	while ((ptr = readdir(dir)) != NULL)
	{
		if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0)
		{
			continue;
		}
		int code = regexec(&regex, ptr->d_name, 0, NULL, 0);
		if (code == 0)
		{
			printf("%" PRId64 ":%s fetch head file: %s\n", taosGetTimestampMs(), __func__, ptr->d_name);
			if (taosArrayPush(*fArray, (void *)ptr->d_name) == NULL)
			{
				printf("%" PRId64 ":%s failed to push file from %s since OOM\n", taosGetTimestampMs(), __func__, dataDir);
				closedir(dir);
				taosArrayDestroy(*fArray);
				regfree(&regex);
				return -1;
			}
		}
		else if (code == REG_NOMATCH)
		{
			// NOTHING TODO
		}
		else
		{
			// Has other error
			printf("%" PRId64 ":%s failed to fetch TFileSet Array while run regexec since %d\n", taosGetTimestampMs(), __func__, code);
			closedir(dir);
			regfree(&regex);
			taosArrayDestroy(*fArray);
			return -1;
		}
	}

	closedir(dir);
	regfree(&regex);

	return 0;
}

static int32_t checkType1_SuidUid(uint64_t suid, uint64_t uid, int32_t vgId)
{
	printf("%" PRId64 ":%s vgId:%d, suid:%" PRIu64 ", uid:%" PRIu64 "\n", taosGetTimestampMs(), __func__, vgId, suid, uid);
	if (suid <= 0 || uid <= 0 || vgId < 2)
	{
		printf("%" PRId64 ":%s invalid parameters.\n", taosGetTimestampMs(), __func__);
		return -1;
	}
	if (vgId < 2)
	{
		printf("scan all vnode dirs as vgId not specified\n");
	}
	char tsdbDir[4096] = {0};
	snprintf(tsdbDir, 4096, "%s/vnode/vnode%d/tsdb/data", gDataDir, vgId);
	printf("%" PRId64 ":%s tsdbDir = %s\n", taosGetTimestampMs(), __func__, tsdbDir);

	SArray *fArray = NULL;
	size_t fArraySize = 0;

	if (tsdbFetchDir(tsdbDir, &fArray) < 0)
	{
		printf("%" PRId64 ":%s failed to fetch head files from %s\n", taosGetTimestampMs(), __func__, tsdbDir);
		return -1;
	}

	if ((fArraySize = taosArrayGetSize(fArray)) <= 0)
	{
		taosArrayDestroy(fArray);
		printf("%" PRId64 ":%s size of head files from %s is %" PRIu32 "\n", taosGetTimestampMs(), __func__, tsdbDir, (uint32_t)fArraySize);
		return 0;
	}

	SReadH readh = {0};
	SReadH *pReadH = &readh;
	tsdbInitReadH(pReadH);
	SDFile headFile = {0};
	pReadH->headFile = &headFile;
	tsdbCloseDFile(pReadH->headFile);
	uint32_t nTotalRows = 0;

	for (size_t index = 0; index < fArraySize; ++index)
	{
		uint32_t nRows = 0;
		char bname[TSDB_FILENAME_LEN] = "\0";
		char fullName[4228] = "\0";
		tstrncpy(bname, (char *)taosArrayGet(fArray, index), TSDB_FILENAME_LEN);
		snprintf(fullName, 4228, "%s/%s", tsdbDir, bname);
		printf("%" PRId64 ":%s from %s head is %s\n", taosGetTimestampMs(), __func__, tsdbDir, fullName);

		getDFile(pReadH, fullName);
		dataStatisCheckType1(pReadH, suid, uid, vgId, &nRows);
		tsdbResetReadFile(pReadH);
		nTotalRows += nRows;
	}

	printf("%" PRId64 ":%s vgId:%d, suid:%" PRIu64 ", uid:%" PRIu64 ", totalRows is %" PRIu32 " in %s\n", taosGetTimestampMs(), __func__,
		   vgId, suid, uid, nTotalRows, tsdbDir);
	tsdbDestroyReadH(pReadH);
	taosArrayDestroy(fArray);

	return TSDB_CODE_SUCCESS;
}

static int32_t checkType2_Vid(int32_t vgId)
{
	printf("%" PRId64 ":%s vgId:%d\n", taosGetTimestampMs(), __func__, vgId);
	if (vgId < 2)
	{
		printf("%" PRId64 ":%s invalid parameters.\n", taosGetTimestampMs(), __func__);
		return -1;
	}

	char tsdbDir[4096] = {0};
	snprintf(tsdbDir, 4096, "%s/vnode/vnode%d/tsdb/data", gDataDir, vgId);
	printf("%" PRId64 ":%s tsdbDir = %s\n", taosGetTimestampMs(), __func__, tsdbDir);

	SArray *fArray = NULL;
	size_t fArraySize = 0;

	if (tsdbFetchDir(tsdbDir, &fArray) < 0)
	{
		printf("%" PRId64 ":%s failed to fetch head files from %s\n", taosGetTimestampMs(), __func__, tsdbDir);
		return -1;
	}

	if ((fArraySize = taosArrayGetSize(fArray)) <= 0)
	{
		taosArrayDestroy(fArray);
		printf("%" PRId64 ":%s size of head files from %s is %" PRIu32 "\n", taosGetTimestampMs(), __func__, tsdbDir, (uint32_t)fArraySize);
		return 0;
	}

	SReadH readh = {0};
	SReadH *pReadH = &readh;
	tsdbInitReadH(pReadH);
	SDFile headFile = {0};
	pReadH->headFile = &headFile;
	tsdbCloseDFile(pReadH->headFile);
	uint32_t nTotalRows = 0;

	for (size_t index = 0; index < fArraySize; ++index)
	{
		uint32_t nRows = 0;
		char bname[TSDB_FILENAME_LEN] = "\0";
		char fullName[4228] = "\0";
		tstrncpy(bname, (char *)taosArrayGet(fArray, index), TSDB_FILENAME_LEN);
		snprintf(fullName, 4228, "%s/%s", tsdbDir, bname);
		printf("%" PRId64 ":%s from %s head is %s\n", taosGetTimestampMs(), __func__, tsdbDir, fullName);

		getDFile(pReadH, fullName);
		dataStatisCheckType2(pReadH, vgId, &nRows);
		tsdbResetReadFile(pReadH);
		nTotalRows += nRows;
	}

	printf("%" PRId64 ":%s vgId:%d, totalRows is %" PRIu32 " in %s\n", taosGetTimestampMs(), __func__,
		   vgId, nTotalRows, tsdbDir);
	tsdbDestroyReadH(pReadH);
	taosArrayDestroy(fArray);

	return TSDB_CODE_SUCCESS;
}

static int32_t getDFile(SReadH *pReadH, const char *fullName)
{
	SDFile *pHeadFile = pReadH->headFile;
	tstrncpy(TSDB_FILE_FULL_NAME(pHeadFile), fullName, TSDB_FILENAME_LEN);

	if (tsdbOpenDFile(pHeadFile, O_RDONLY) < 0)
	{
		printf("vgId:%d failed to open DFile %s since %d\n", TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pHeadFile),
			   terrno);
		return -1;
	}

	if (tsdbLoadDFileHeader(pHeadFile, &(pHeadFile->info)) < 0)
	{
		printf("vgId:%d failed to load DFile %s header since %d\n", TSDB_READ_REPO_ID(pReadH), TSDB_FILE_FULL_NAME(pHeadFile),
			   terrno);
		return -1;
	}
	return 0;
}

static int32_t dataStatisCheckType1(SReadH *pReadH, uint64_t suid, uint64_t uid, int32_t vgId, uint32_t *nRows)
{
	SDFile *pHeadFile = pReadH->headFile;
	if (tsdbLoadBlockIdx(pReadH) < 0)
	{
		printf("%" PRId64 ":%s failed to load blk Idx from %s\n", taosGetTimestampMs(), __func__, TSDB_FILE_FULL_NAME(pHeadFile));
		return -1;
	}

	tsdbSetReadTable(pReadH, suid, uid);

	if (pReadH->pBlkIdx == NULL)
	{
		printf("%" PRId64 ":%s no blkIdx found from %s\n", taosGetTimestampMs(), __func__, TSDB_FILE_FULL_NAME(pHeadFile));
		return -1;
	}
	if (tsdbLoadBlockInfo(pReadH, NULL) < 0)
	{
		printf("%" PRId64 ":%s failed to load blk info from %s\n", taosGetTimestampMs(), __func__, TSDB_FILE_FULL_NAME(pHeadFile));
		return -1;
	}

	tsdbGetRowsOfBlockInfo(pReadH, nRows);

	if (*nRows > 0)
	{
		printf("%" PRId64 ":%s vgId:%d, suid:%" PRIu64 ", uid:%" PRIu64 ", nRows is %" PRIu32 " in %s\n", taosGetTimestampMs(), __func__,
			   vgId, suid, uid, *nRows, TSDB_FILE_FULL_NAME(pHeadFile));
	}

	return 0;
}

static int32_t dataStatisCheckType2(SReadH *pReadH, int32_t vgId, uint32_t *nRows)
{
	SDFile *pHeadFile = pReadH->headFile;
	if (tsdbLoadBlockIdx(pReadH) < 0)
	{
		printf("%" PRId64 ":%s failed to load blk Idx from %s\n", taosGetTimestampMs(), __func__, TSDB_FILE_FULL_NAME(pHeadFile));
		return -1;
	}

	size_t blkIdxSize = taosArrayGetSize(pReadH->aBlkIdx);

	printf("%" PRId64 ":%s loaded %" PRIu64 " blk Idx from %s\n", taosGetTimestampMs(), __func__, blkIdxSize, TSDB_FILE_FULL_NAME(pHeadFile));

	uint32_t nTableNum = 0;
	for (int32_t idx = 0; idx < blkIdxSize; ++idx)
	{
		SBlockIdx *pBlkIdx = taosArrayGet(pReadH->aBlkIdx, idx);
		// printf("%" PRId64 ":%s idx[%d] from %s ======\n", taosGetTimestampMs(), __func__, idx, TSDB_FILE_FULL_NAME(pHeadFile));

		tsdbSetReadTable(pReadH, pBlkIdx->suid, pBlkIdx->uid);

		if (pReadH->pBlkIdx == NULL)
		{
			printf("%" PRId64 ":%s idx[%d] no blkIdx found for vid:%d suid:%" PRIu64 " uid:%" PRIu64 " from %s\n", taosGetTimestampMs(), __func__, idx, vgId,
				   pBlkIdx->suid, pBlkIdx->uid, TSDB_FILE_FULL_NAME(pHeadFile));
			continue;
		}

		++nTableNum;

		if (tsdbLoadBlockInfo(pReadH, NULL) < 0)
		{
			printf("%" PRId64 ":%s idx[%d] failed to load blk info  for vid:%d suid:%" PRIu64 "  uid:%" PRIu64 " from %s\n", taosGetTimestampMs(), __func__, idx, vgId,
				   pBlkIdx->suid, pBlkIdx->uid, TSDB_FILE_FULL_NAME(pHeadFile));
			return -1;
		}
		uint32_t rows = 0;
		tsdbGetRowsOfBlockInfo(pReadH, &rows);
		printf("%" PRId64 ":%s idx[%d] vgId:%d, suid:%" PRIu64 ", uid:%" PRIu64 ", nRows is %" PRIu32 " in %s\n", taosGetTimestampMs(), __func__, idx,
			   vgId, pBlkIdx->suid, pBlkIdx->uid, rows, TSDB_FILE_FULL_NAME(pHeadFile));
		*nRows += rows;
	}

	if (*nRows > 0)
	{
		printf("%" PRId64 ":%s vgId:%d, nTables %" PRIu32 ", nRows %" PRIu32 " in %s\n", taosGetTimestampMs(), __func__,
			   vgId, nTableNum, *nRows, TSDB_FILE_FULL_NAME(pHeadFile));
	}

	return 0;
}

static int32_t tsdbGetRowsOfBlockInfo(SReadH *pReadH, uint32_t *nRows)
{
	SBlockInfo *pBlkInfo = pReadH->pBlkInfo;
	SBlockIdx *pBlkIdx = pReadH->pBlkIdx;

	*nRows = 0;

	for (int i = 0; i < pBlkIdx->numOfBlocks; ++i)
	{
		SBlock *pBlock = pBlkInfo->blocks + i;
		SBlock *iBlock = pBlock;
		if (pBlock->numOfSubBlocks > 1)
		{
			iBlock = (SBlock *)POINTER_SHIFT(pBlkInfo, pBlock->offset);
		}

		*nRows += iBlock->numOfRows;

		for (int i = 1; i < pBlock->numOfSubBlocks; i++)
		{
			iBlock++;
			*nRows += iBlock->numOfRows;
		}
	}

	return 0;
}

/**
 * @brief
 * Usage samples:
 *  01)  program -check 1 -d /data1 -uid 12345 -tid 133 -vid 3 // designated data dir /data1
 *  02)  program -check 1 -uid 12345 -tid 133 -vid 3 // default data dir /var/lib/taos
 * @param argc
 * @param argv
 * @return int
 */
int main(int32_t argc, char *argv[])
{
	printf("%" PRId64 ":%s ====== informal tsdb data check tools(count nRows in dataDir by uid/tid/vid)\n", taosGetTimestampMs(), __func__);
	printf("%" PRId64 ":%s ====== e.g. program -check 1 -d /dataDir -suid 123 -uid 123 -vid 3\n", taosGetTimestampMs(), __func__);
	printf("%" PRId64 ":%s ====== e.g. program -check 2 -d /dataDir -vid 3\n", taosGetTimestampMs(), __func__);
	printf("%" PRId64 ":%s ====== start\n", taosGetTimestampMs(), __func__);
	if (checkParams(argc, argv) != 0)
	{
		printf("exit as invalid paramerters\n");
		exit(EXIT_FAILURE);
	}

	int code = TSDB_CODE_SUCCESS;

	switch (gCheckType)
	{
	case CHECK_LIST_DATA_OF_VID_UID_TID:
		code = checkType1_SuidUid(gTableId.suid, gTableId.uid, gVgId);
		break;
	case CHECK_LIST_DATA_OF_VID:
		code = checkType2_Vid(gVgId);
		break;
	default:
		printf("%" PRId64 ":%s ====== WARN - check type %d not support yet\n", taosGetTimestampMs(), __func__, gCheckType);
		exit(EXIT_FAILURE);
		break;
	}
	printf("%" PRId64 ":%s ====== code %d\n", taosGetTimestampMs(), __func__, code);
	printf("%" PRId64 ":%s ====== end ========\n", taosGetTimestampMs(), __func__);
}
