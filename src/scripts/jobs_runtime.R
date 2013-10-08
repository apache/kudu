# Copyright (c) 2013, Cloudera, inc.
# All rights reserved.

# How to invoke:
#  jobs_runtime.R <tsvfile> <testname>
# This script takes in input a TSV file with the following header:
# workload        avg_runtime     build_number
# It generates a png where x is the build number, y is the avg_runtime
# and each workload is a different line. The test name is used to generate
# the output file's name.
# R needs to be installed with the graphic libraries

library(Cairo)
library(ggplot2)

newpng <- function(filename = "img.png", width = 800, height = 500) {
    CairoPNG(filename, width, height)
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("usage: jobs_runtime.R <filename> <testname>")
}
filename = args[1]
testname = args[2]

newpng(paste(testname, "-jobs-runtime.png", sep = ""))

d <- read.table(file=filename, header=T)

print(ggplot(d, aes(x = build_number, y = avg_runtime, group=workload, color=workload)) + geom_line() + ggtitle(testname))
