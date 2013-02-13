library(ggplot2)
library(reshape)

si_num <- function (x) {

  if (!is.na(x)) {
    if (x >= 1e9) { 
      rem <- format(x/1e9, digits=3)
      rem <- append(rem, "B");
    } else if (x >= 1e6) { 
      rem <- format(x/1e6, digits=3)
      rem <- append(rem, "M");
    } else if (x > 1e3) { 
      rem <- format(x/1e3, digits=3)
      rem <- append(rem, "K");
    }
    else {
      return(x);
    }

    return(paste(rem, sep="", collapse=""));
  }
  else return(NA);
}

si_vec <- function(x) {
  sapply(x, FUN=si_num);
}

d <- read.table(file="/tmp/graph.tsv", header=T)

d$insert_rate = c(0, diff(d$inserted)/diff(d$time))
d$update_rate = c(0, diff(d$updated)/diff(d$time))
d <- subset(d, select = -c(updated))


# Put memstore usage in bytes
d$memstore_bytes <- d$memstore * 1024
d <- subset(d, select = -c(memstore_kb))

d <- rename(d, c(
  insert_rate="Insert rate (rows/sec)",
  memstore="Memstore Memory Usage",
  scan_rate="Scan int col (rows/sec)"))


d.melted = melt(d, id="time")
print(qplot(time, value, data=d.melted, geom="line", group = variable)
                  + scale_y_continuous(labels=si_vec)
                  + facet_grid(variable~., scale = "free_y")
                  + stat_smooth())
