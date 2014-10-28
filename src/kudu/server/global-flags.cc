// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gflags/gflags.h>

DEFINE_bool(enable_process_lifetime_heap_profiling, false, "(Advanced) Enables heap "
    "profiling for the lifetime of the process. Profile output will be stored in the "
    "directory specified by -heap_profile_dir. Enabling this option will disable the "
    "on-demand/remote server profile handlers.");

DEFINE_string(heap_profile_dir, "", "Output directory to store heap profiles. If not set "
    " profiles are stored in the current working directory.");
