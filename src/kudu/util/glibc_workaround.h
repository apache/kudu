// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// See glibc_workaround.cc for details on the glibc bug that this
// file works around.

namespace kudu {
// This function does nothing. However, calling it forces glibc_workaround.o
// to be linked into a binary.
void ForceLinkingGlibcWorkaround();
}

