# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
# All rights reserved.

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

