#! /bin/bash

objects = tsdb_check_data.o t_array.o tcrc32c.o tsdbFile.o tsdbReadImpl.o osFile.o osMemory.o talgo.o
tsdbCheck: $(objects)
	gcc -o tsdbCheck $(objects)
#GPP_MAKE_CMD="g++ -g -std=c++11 -Wall a.cpp -ltaos"

#GCC_MAKE_CMD="gcc -g -Wall -D_M_X64 a.c -ltaos -lpthread -lm -lrt"
#GCC_MAKE_CMD="gcc -g -Wall -D_M_X64 a.c -lpthread -lm -lrt"

GCC_ARGS=gcc -g -D_M_X64 -c
#GCC_ARGS=gcc -g -Wall -D_M_X64 -c
#GNU_CMD=${GPP_MAKE_CMD}
#GNU_CMD=${GCC_MAKE_CMD}

#echo "${GNU_CMD}"
#${GNU_CMD}

#GCC_MAKE_CMD="gcc -g -Wall -D_M_X64 a.c -lpthread -lm -lrt"

tsdb_check_data.o: tsdb_check_data.c tcommon.h
	$(GCC_ARGS) tsdb_check_data.c

talgo.o : talgo.c talgo.h
	$(GCC_ARGS) talgo.c

t_array.o : t_array.c t_array.h tcommon.h
	$(GCC_ARGS) t_array.c

osMemory.o: osMemory.c osMemory.h
	$(GCC_ARGS) osMemory.c

osFile.o : osFile.c osFile.h tcommon.h
	$(GCC_ARGS) osFile.c

tsdbReadImpl.o: tsdbReadImpl.c tsdbReadImpl.h tcommon.h tcoding.h tsdbFile.h tsdbFS.h tcrc32c.h osMemory.h tsdb.h
	$(GCC_ARGS) tsdbReadImpl.c

tcrc32c.o : tcrc32c.c tcrc32c.h
	$(GCC_ARGS) tcrc32c.c

tsdbFile.o : tsdbFile.h tsdbFS.h tcommon.h tcoding.h tsdbFS.h tcrc32c.h osMemory.h osFile.h
	$(GCC_ARGS) tsdbFile.c

.PHONY :clean
clean:
	rm tsdbCheck $(objects)
