// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// For fallocate(2)
#define _GNU_SOURCE

#include <fcntl.h>
#include <linux/falloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char** argv) {
  if (argc != 5) {
    fprintf(stderr, "usage: %s <path> <start block> <end block> <stride>\n", argv[0]);
    fprintf(stderr, "\n");
    fprintf(stderr, "Punches holes in an existing file designated by <path>.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Holes are punched on a per-filesystem block basis, "
                    "beginning at <start block> and ending at <end block> "
                    "(exclusive). Blocks can be skipped with <stride>; a value "
                    "of 1 means every block in the range will be punched.\n");
    exit(1);
  }

  int start_block = atoi(argv[2]);
  int end_block = atoi(argv[3]);
  int stride = atoi(argv[4]);

  int fd = open(argv[1], O_WRONLY, 0644);
  if (fd < 0) {
    perror("open");
    return fd;
  }

  struct stat sbuf;
  int ret = fstat(fd, &sbuf);
  if (ret < 0) {
    perror("fstat");
    return ret;
  }

  int block_num;
  for (block_num = start_block; block_num < end_block; block_num += stride) {
    ret = fallocate(fd,
                    FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
                    block_num * sbuf.st_blksize,
                    sbuf.st_blksize);
    if (ret < 0) {
      perror("fallocate");
      return ret;
    }
  }

  fsync(fd);
  close(fd);
}
