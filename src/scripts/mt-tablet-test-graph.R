library(ggplot2)
library(reshape)

source("si_vec.R")


if (!exists("filename")) {
  filename <- "/tmp/graph.tsv"
}
print(c("Using file ", filename))

d <- read.table(file=filename, header=T)

d$insert_rate = c(0, diff(d$inserted)/diff(d$time))
d$scan_rate = c(0, diff(d$scanned)/diff(d$time))
d <- subset(d, select = -c(scanned))

if (!is.null(d$updated)) {
  d$update_rate = c(0, diff(d$updated)/diff(d$time))
  d <- subset(d, select = -c(updated))
}

# Put memstore usage in bytes
d$memstore_bytes <- d$memstore * 1024
d <- subset(d, select = -c(memstore_kb))

print(ggplot(d, aes(inserted, insert_rate)) +
          geom_point(alpha=0.02) +
          scale_x_continuous(labels=si_vec) +
          scale_y_log10(labels=si_vec))

dev.new()

print(ggplot(d, aes(inserted, scan_rate)) +
          geom_point(alpha=0.01) +
          scale_x_continuous(labels=si_vec) +
          scale_y_log10(labels=si_vec))

dev.new()


d <- rename(d, c(
  insert_rate="Insert rate (rows/sec)",
  memstore="Memstore Memory Usage",
  scan_rate="Scan int col (rows/sec)"))


# set span to 5 seconds worth of data
span = 5.0/max(d$time)

d.melted = melt(d, id="time")
print(qplot(time, value, data=d.melted, geom="line", group = variable)
                  + scale_y_continuous(labels=si_vec)
                  + facet_grid(variable~., scale = "free_y")
                  + stat_smooth())

