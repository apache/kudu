library(ggplot2)
library(reshape)

d <- read.table(file="/tmp/graph.tsv", header=T)

d$insert_rate = c(0, diff(d$inserted)/diff(d$time))
d <- subset(d, select = -c(inserted))

# Put scan rate in millions/sec
d$scan_rate <- d$scan_rate / 1000000

# Put insert rate in K/sec
d$insert_rate <- d$insert_rate / 1000

# Put memstore usage in MB
d$memstore_mb <- d$memstore_kb / 1000
d <- subset(d, select = -c(memstore_kb))

d <- rename(d, c(
  insert_rate="Insert rate (K rows/s)",
  memstore_mb="Memstore Memory Usage (MB)",
  scan_rate="Scan int col (M rows/sec)"))


d.melted = melt(d, id="time")
print(qplot(time, value, data=d.melted, geom="line", group = variable)
                  + facet_grid(variable~., scale = "free_y"))
