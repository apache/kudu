# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
# All rights reserved.

# How to invoke:
#  jobs_runtime.R <tsvfile> <testname>
# This script takes in input a TSV file with the following header:
# workload        runtime     build_number
# It generates a png where x is the build number, y is the runtime
# and each workload is a different line. The test name is used to generate
# the output file's name.
# R needs to be installed with the graphic libraries

library(Cairo)
library(ggplot2)

newpng <- function(filename = "img.png", width = 1500, height = 500) {
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

print(ggplot(d, aes(x = build_number, y = runtime, color = workload)) +
             stat_summary(aes(group = workload), fun.y=median, geom = "line") +
             geom_boxplot(aes(group = interaction(workload, build_number)), position = "identity", outlier.size = 1.7, outlier.colour = "gray32") +
             ggtitle(testname))
