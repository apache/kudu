# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

library(ggplot2)
library(scales)

data <- read.table(file="data.tsv",header=T)
systems <- levels(data$sys)
workloads <- levels(data$workload)

for (w in workloads) {
  cat("iterating for workload ", w, "\n")
  s <- subset(data, workload==w)
  dists <- unique(s$dist)
  for (d in dists) {
    s2 <- subset(s, dist==d)
    cat("Plotting", nrow(s2), "points for workload", w, "dist", d, "\n")
    filename = paste(d, "-", w, ".png", sep="")
    cat("into filename", filename, "\n")
    png(filename)
    print(qplot(time, tput, data=s2, colour=sys,
        main=paste("Workload '", w, "'\n", d, " distribution", sep=""),
        geom="line", xlab="Time (sec)", ylab="Throughput\n(ops/sec)") +
        scale_y_continuous(labels=comma) +
        theme(legend.position="bottom"))
    dev.off()
  }
}
