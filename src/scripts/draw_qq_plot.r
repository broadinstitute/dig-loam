#!/broad/tools/apps/R-2.6.0/bin/Rscript

args <- commandArgs(TRUE)
verbose <- TRUE
file.name <- args[1]
output.file.name <- args[2]
value.delim <- ','
title <- args[3]
do.unif <- TRUE
use.whitespace <- FALSE
non.zero.cols <- c()
header <- TRUE
sep <- ""
extra.arg.delim <- "="
nrow <- 1
ncol <- 1
max.plot.points <- NULL
confidence.intervals <- TRUE
unif.max <- 1
main.cex <- 1
cex <- 1
n.unif.perm <- 25
print.lambda <- TRUE
plot.size <- 3
height.scale <- 1
ylab <- "Sample quantiles"
draw.quantile <- NULL
draw.abline <- NULL

t.test.flag <- "T"
or.flag <- "OR"
unif.flag <- "UNIF"
binom.flag <- "BINOM"
binom.no.ties.flag <- "BINOM_NO_TIES"

all.value.cols <- list()
all.value.names <- list()
all.filter.cols <- list()
all.filter.values <- list()
all.value.thresh <- list()
all.freq.cols <- list()
all.count.cols <- list()
all.tot.cols <- list()
all.shade.cols <- list()
all.label.cols <- list()
all.text.cols <- list()
all.adj.cols <- list()
all.types <- list()
all.ltys <- list()
all.colors <- list()
all.p.types <- list()
all.p.type.cols <- list()
all.p2.types <- list()

append.arg <- function(the.list, the.arg)
{
  if (is.na(the.arg) || nchar(the.arg) == 0)
  {
    return(append(the.list, list(NULL)))
  } else
  {
    return(append(the.list, list(the.arg)))
  }
}

if (length(args) > 3)
{
  for (extra.arg in args[4:length(args)])
  {
    if (unlist(gregexpr(extra.arg.delim, extra.arg))[1] > 0)
    {
      cur.arg <- as.character(unlist(strsplit(extra.arg, extra.arg.delim)))
      cur.arg[1] <- as.character(cur.arg[1])
      if (length(cur.arg) <= 2)
      {
        if (as.character(cur.arg[1]) == 'do.unif')
        {
          do.unif <- as.logical(cur.arg[2])
        } else if (cur.arg[1] == "sep")
        {
          sep <- cur.arg[2]
        } else if (cur.arg[1] == "non.zero.cols")
        {
          use.whitespace <- as.numeric(unlist(strsplit(cur.arg[2], ',')))
        } else if (cur.arg[1] == "header")
        {
          header <- as.logical(cur.arg[2])
        } else if (cur.arg[1] == "nrow")
        {
          nrow <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "ncol")
        {
          ncol <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "max.plot.points")
        {
          max.plot.points <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "unif.max")
        {
          unif.max <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "confidence.intervals")
        {
          confidence.intervals <- as.logical(cur.arg[2])
        } else if (cur.arg[1] == "main.cex")
        {
          main.cex <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "cex")
        {
          cex <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "n.perm")
        {     
          n.unif.perm <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "value.col")
        {     
          all.value.cols <- append.arg(all.value.cols, cur.arg[2])
        } else if (cur.arg[1] == "value.name")
        {     
          all.value.names <- append.arg(all.value.names, cur.arg[2])
        } else if (cur.arg[1] == "filter.col")
        {     
          all.filter.cols <- append.arg(all.filter.cols, cur.arg[2])
        } else if (cur.arg[1] == "filter.value")
        {     
          all.filter.values <- append.arg(all.filter.values, cur.arg[2])
        } else if (cur.arg[1] == "value.thresh")
        {     
          all.value.thresh <- append.arg(all.value.thresh, cur.arg[2])
        } else if (cur.arg[1] == "freq.col")
        {     
          all.freq.cols <- append.arg(all.freq.cols, cur.arg[2])
        } else if (cur.arg[1] == "count.col")
        {     
          all.count.cols <- append.arg(all.count.cols, cur.arg[2])
        } else if (cur.arg[1] == "tot.col")
        {     
          all.tot.cols <- append.arg(all.tot.cols, cur.arg[2])
        } else if (cur.arg[1] == "shade.col")
        {     
          all.shade.cols <- append.arg(all.shade.cols, cur.arg[2])
        } else if (cur.arg[1] == "label.col")
        {     
          all.label.cols <- append.arg(all.label.cols, cur.arg[2])
        } else if (cur.arg[1] == "text.col")
        {     
          all.text.cols <- append.arg(all.text.cols, cur.arg[2])
        } else if (cur.arg[1] == "adj.col")
        {     
          all.adj.cols <- append.arg(all.adj.cols, cur.arg[2])
        } else if (cur.arg[1] == "plot.type")
        {     
          all.types <- append.arg(all.types, cur.arg[2])
        } else if (cur.arg[1] == "plot.lty")
        {     
          all.ltys <- append.arg(all.ltys, cur.arg[2])
        } else if (cur.arg[1] == "plot.size")
        {     
          plot.size <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "height.scale")
        {     
          height.scale <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "plot.color")
        {     
          all.colors <- append.arg(all.colors, cur.arg[2])
        } else if (cur.arg[1] == "p.type.col")
        {     
          all.p.type.cols <- append.arg(all.p.type.cols, cur.arg[2])
        } else if (cur.arg[1] == "p.type")
        {     
          all.p.types <- append(all.p.types, cur.arg[2])
        } else if (cur.arg[1] == "p2.type")
        {     
          all.p2.types <- append(all.p2.types, cur.arg[2])
        } else if (cur.arg[1] == "t.test.flag")
        {     
          t.test.flag <- as.character(cur.arg[2])
        } else if (cur.arg[1] == "or.flag")
        {     
          or.flag <- as.character(cur.arg[2])
        } else if (cur.arg[1] == "unif.flag")
        {     
          unif.flag <- as.character(cur.arg[2])
        } else if (cur.arg[1] == "binom.flag")
        {     
          binom.flag <- as.character(cur.arg[2])
        } else if (cur.arg[1] == "binom.no.ties.flag")
        {     
          binom.no.ties.flag <- as.character(cur.arg[2])
        } else if (cur.arg[1] == "print.lambda")
        {     
          print.lambda <- as.logical(cur.arg[2])
        } else if (cur.arg[1] == "ylab")
        {     
          ylab <- cur.arg[2]
        } else if (cur.arg[1] == "draw.quantile")
        {     
          draw.quantile <- as.numeric(cur.arg[2])
        } else if (cur.arg[1] == "draw.abline")
        {     
          draw.abline <- as.numeric(cur.arg[2])
        }
      }

    }
  }
}



if (sep == '\\t')
{
  sep = "\t"
}

to.read <- file.name
if (file.name == "/dev/stdin")
{
  to.read <- file(description="stdin")
}

table <- as.data.frame(read.table(to.read, header=header, sep=sep, quote="", check.names=F))

get.cols <- function(initial.col)
{
  if (is.null(initial.col))
  {
    return(NULL)
  }
  complement <- FALSE
  if (substr(initial.col, 1, 1) == "-")
  {
    complement <- TRUE
    initial.col <- substr(initial.col, 2, nchar(initial.col))
  }
  cols <- as.character(unlist(strsplit(initial.col, value.delim)))
  for (i in 1:length(cols))
  {
    match <- colnames(table) == cols[i]
    if (sum(match) > 0)
    {
      cols[i] <- (1:length(colnames(table)))[match][1]
    } else if (is.na(as.integer(cols[i])))
    {
      print(colnames(table))
      stop(paste("Bad column",cols[i]))
    }
  }
  cols <- unique(as.integer(cols))

  if (complement)
  {
    cols <- (1:ncol(table))[-cols]
  }
  return(cols)
}


all.value.cols <- lapply(all.value.cols, get.cols)

all.filter.cols <- lapply(all.filter.cols, get.cols)
all.filter.values <- lapply(all.filter.values, function(x) {as.character(unlist(strsplit(x, value.delim)))})
all.value.thresh <- lapply(all.value.thresh, function(x) {as.character(unlist(strsplit(x, value.delim)))})
all.freq.cols <- lapply(all.freq.cols, get.cols)
all.count.cols <- lapply(all.count.cols, get.cols)
all.tot.cols <- lapply(all.tot.cols, get.cols)
all.shade.cols <- lapply(all.shade.cols, get.cols)
all.label.cols <- lapply(all.label.cols, get.cols)
all.text.cols <- lapply(all.text.cols, get.cols)
all.adj.cols <- lapply(all.adj.cols, get.cols)
all.p.type.cols <- lapply(all.p.type.cols, get.cols)
if (length(all.types) == 0)
{
  all.types <- list("b")
}	
if (length(all.ltys) == 0)
{
  all.ltys <- list(2)
}	
if (length(all.colors) == 0)
{
  all.colors <- list("black")
}	

num.value.cols <- max(sapply(all.value.cols, length))
num.per.page <- 1
if (num.value.cols <= nrow)
{
  nrow <- num.value.cols
  ncol <- 1
}
num.per.page <- nrow * ncol

width <- max(plot.size * ncol, 6)
height <- max(plot.size * height.scale * nrow, 6)

if (substr(output.file.name, nchar(output.file.name)-4, nchar(output.file.name)) == ".jpeg") {
  jpeg(file=output.file.name,width=width, height=height)
} else {
  pdf(file=output.file.name, width=width, height=height)
}

if (num.value.cols > 1)
{
  oma <- par("oma")
  oma[3] <- oma[3] + 1.5
  par(oma=oma)
  par(mfrow=c(nrow,ncol))

  mar <- par("mar")
  mar[1] <- .85 * mar[1]
  par(mar=mar)

}

get.list.val <- function(the.list, the.ind)
{
  if (length(the.list) == 0)
  {
    return(NULL)
  }
  list.index <- (the.ind - 1) %% length(the.list) + 1
  if (is.list(the.list))
  {
    return(the.list[[list.index]])
  } else
  { 
    return(the.list[list.index])
  }
}

it <- 0

binom.val <- function(n, p, no.ties, v=NULL)
{
  if (is.null(v))
  {
    v <- rbinom(1, n, p)
  }

  obs.p <- p

  if (no.ties)
  {
    d <- dbinom(v, n, obs.p)
    value <- binom.test(v, n, obs.p, alternative="two.sided")$p.value - .5 * sum(dbinom(seq(0,n), n, obs.p) == d) * d
    return(value)
  }
  else
  {
    value <- binom.test(v, n, obs.p, alternative="two.sided")$p.value
    return(value)
  }
}

t.test.val <- function(n, p, p2.type)
{
  if (n == 1)
  {
    return(1)
  }
  v <- rbinom(n, 1, p)
  if (min(v) == max(v))
  {
    if (!is.null(p2.type) && (p2.type == binom.flag || p2.type == binom.no.ties.flag))
    {
      return(binom.val(n, p, p2.type == binom.no.ties.flag, v=(mean(v) * v)))
    }
    else
    {
      return(1)
    }
  }
  value <- t.test(v, mu=p, alternative="two.sided")$p.value
  return(value)
}

or.val <- function(n, p, t, p2.type)
{
  v1 <- rbinom(n, 1, p)
  pi1 <- mean(v1)
  v0 <- rbinom(t - n, 1, p)
  pi0 <- mean(v0)
  constant <- !(pi1 != 0 && pi1 != 1 && pi0 != 0 && pi0 != 1)

  n11 <- pi1 * n
  n10 <- n - n11

  n01 <- pi0 * (t - n)
  n00 <- t - n - n01

  or1 <- pi1 / (1 - pi1)
  or0 <- pi0 / (1 - pi0)
  L <- log(or1 / or0)
  se <- sqrt(1 / n11 + 1 / n10 + 1 / n01 + 1 / n00)

  if (se == Inf || is.nan(se))
  {
    if (!is.null(p2.type) && (p2.type == binom.flag || p2.type == binom.no.ties.flag))
    {
      value <- binom.val(n, p, p2.type == binom.no.ties.flag, v=n11)
    }
    else
    {
      value <- 1
    }
  } else
  {
    value <- 2 * pnorm(-abs(L), 0, se)
  }

  return(value)

}

legend.colors <- c()
legend.pt.bgs <- c()
legend.ltys <- c()
legend.pchs <- c()  
legend.lambdas <- c()
do.legend <- length(all.value.names) == length(all.value.cols)

for (i in 1:num.value.cols)
{
  plotxs = list()
  plotys = list()
  adjs = list()

  first <- TRUE
  for (k in length(all.value.cols):1)
  {
    columns <- all.value.cols[[k]]
  
    label.cols <- get.list.val(all.label.cols, k)
    adj.cols <- get.list.val(all.adj.cols, k)    
    text.cols <- get.list.val(all.text.cols, k)    
    filter.cols <- get.list.val(all.filter.cols, k)
    filter.values <- get.list.val(all.filter.values, k)
    value.threshes <- get.list.val(all.value.thresh, k)
    count.cols <- get.list.val(all.count.cols, k)
    tot.cols <- get.list.val(all.tot.cols, k)
    freq.cols <- get.list.val(all.freq.cols, k)
    p.type.cols <- get.list.val(all.p.type.cols, k)
    p.type <- get.list.val(all.p.types, k)
    p2.type <- get.list.val(all.p2.types, k)

    column <- get.list.val(columns, i)

    cur.table <- as.data.frame(table[!is.nan(table[,column]),])
    

    name <- colnames(cur.table)[column]
    if (num.value.cols > 1)
    {
      cur.title <- name
    }
    else
    {
      cur.title <- title
    }

    filter.col <- get.list.val(filter.cols, i)
    filter.value <- get.list.val(filter.values, i)    

    if (!is.null(filter.col) > 0 && !is.null(filter.value))
    {
      op <- substr(filter.value, 1, 1)
      filter.number <- as.numeric(substr(filter.value, 2, nchar(filter.value)))

      cur.table <- subset(cur.table, !is.nan(cur.table[,filter.col]))

      if (op == "<")
      {
        mask <- cur.table[,filter.col] < filter.number
      } else if (op == ">")
      {
        mask <- cur.table[,filter.col] > filter.number
      } else if (op == "=")
      {
        mask <- cur.table[,filter.col] == filter.number
      } else
      {
        stop("Error: filter value must begin with <, >, or =")
      }
      if (sum(mask) > 0)
      {
        cur.table <- subset(cur.table, mask)
      }
    }

    if (length(non.zero.cols) > 0)
    {
      cur.table <- cur.table[apply(cur.table[,non.zero.cols], 1, sum) > 0,]
    }
    if (nrow(cur.table) == 0)
    {
      next
    }

    data <- subset(cur.table, !is.na(cur.table[,column]) & cur.table[,column] > 0)

    label.col <- get.list.val(label.cols, i)
    if (!is.null(label.col))
    {
      duplicate.mask <- duplicated(data[,label.col])
      data <- subset(data, !duplicate.mask)
    }

    if (do.unif)
    {
      randunif <- qunif(seq(0,1,length=length(data[,column]) + 2))
      randunif <- randunif[2:(length(randunif) - 1)]

      count.col <- get.list.val(count.cols, i)
      tot.col <- get.list.val(tot.cols, i)
      freq.col <- get.list.val(freq.cols, i)
      p.type.col <- get.list.val(p.type.cols, i)

      if (!is.null(count.col) && !is.null(freq.col))
      {
        nit <- n.unif.perm

        #unique.value.counts <- as.data.frame(table(as.data.frame(data[,c(cur.count.col, cur.freq.col)])))
        #unique.value.counts <- subset(unique.value.counts, unique.value.counts$Freq > 0)
        #unique.value.counts <- cbind(as.numeric(as.character(unique.value.counts[,1])),
        #                             as.numeric(as.character(unique.value.counts[,2])),
        #                             as.numeric(as.character(unique.value.counts[,3])))
        
        #plotx <- apply(unique.value.counts, 1,
        #                function(x)
        #                {				 
        #                  c <- as.numeric(x[3])
        #                  cur.nit <- ceiling(nit / c)
        #                  value <- sapply(1:c,
        #                                  function(y)
        #                                  {
        #                                    n <- x[1]
        #                                    p <- x[2]
        #                                    v <- rbinom(cur.nit, n, p)
        #                                    print(v)
        #                                    m <- mean(sapply(v,
        #                                                     function(z)
        #                                                     {						    
        #                                                       if (break.ties)
        #                                                       {
        #                                                         d <- dbinom(z, n, p)
        #                                                         binom.test(z, n, p, alternative="two.sided")$p.value - .5 * sum(dbinom(seq(0,n), n, p) == d) * d
        #                                                       }
        #                                                       else
        #                                                       {
        #                                                         binom.test(z, n, p, alternative="two.sided")$p.value
        #                                                       }
        #                                                     }))
        #                                    print(m)
        #                                  })
        #                                })

        plotx <- sapply(1:nit, function (z)
                               {
                                 tot.values <- NA

                                 if (!is.null(tot.col))
                                 {
                                   tot.values <- data[,tot.col]
                                 }
                                 if (!is.null(p.type.col))
                                 {
                                   cur.data <- cbind(data[,c(count.col, freq.col, p.type.col)], tot.values)
                                 }
                                 else if (!is.null(p.type))
                                 {
                                   cur.data <- cbind(data[,c(count.col, freq.col)], p.type, tot.values)
                                 }
                                 else
                                 {
                                   cur.data <- cbind(data[,c(count.col, freq.col)], unif.flag, tot.values)
                                 }
                                 r <- as.vector(apply(cur.data,
                                                      1,
                                                      function (x)
                                                      {
                                                        flag <- as.character(x[3])
                                                        n <- as.numeric(as.character(x[1]))
                                                        p <- as.numeric(as.character(x[2]))
                                                        t <- as.numeric(as.character(x[4]))
                                                        if (n == 0)
                                                        {
                                                          return(1)
                                                        } else
                                                        {
                                                          if (flag == binom.no.ties.flag || flag == binom.flag)
                                                          {
                                                            return(binom.val(n, p, flag == binom.no.ties.flag))
                                                          }
                                                          else if (flag == t.test.flag)
                                                          {
                                                            return(t.test.val(n, p, p2.type))
                                                          }
                                                          else if (flag == or.flag)
                                                          {
                                                            if (is.null(t))
                                                            {
                                                              stop("Require tot.col for OR test")
                                                            }
                                                            return(or.val(n, p, t, p2.type))
                                                          }
                                                          else
                                                          {
                                                            return(runif(1))
                                                          }
                                                        }
                                                      }))
                                 return(r[order(r)])
                               })
        if (!is.null(dim(plotx)))
        {
          plotx <- apply(plotx, 1, mean)
        } else
        {
          plotx <- mean(plotx)
        }

      } else
      {
        plotx <- randunif
      }

      if (unif.max)
      {
        unif.max <- max(unif.max, data[,column])
        plotx <- plotx * unif.max
      }

      ploty <- data[,column]

      text.col <- get.list.val(text.cols, i)
      if (!is.null(text.col))
      {
        names(ploty) <- data[,text.col]
      }

      med.theoretical <- qchisq(median(plotx, na.rm=T), 1, lower.tail=F)
      med.empirical <- qchisq(median(ploty, na.rm=T), 1, lower.tail=F)
      legend.lambdas[k] <- sprintf("%.3f",med.empirical/med.theoretical)

      ploty <- -log(ploty, b=10)
      plotx <- -log(plotx, b=10)
      
      adj.col <- get.list.val(adj.cols, i)
      if (!is.null(adj.col))
      {
        adj <- data[,adj.col]
        adj <- adj[order(ploty)]
      }

      plotx <- plotx[order(plotx)]
      ploty <- ploty[order(ploty)]
      
      print(length(plotx))
      print(length(ploty))

      if (!is.null(max.plot.points))
      {
        down.sample <- max.plot.points / length(ploty)
        if (down.sample < 1)
        {
          proportion.top <- .2
          quantile <- quantile(ploty, probs=(1 - down.sample * proportion.top), names=FALSE, na.rm=TRUE)
          mask <- ploty >= quantile
          num.mask <- sum(mask)
          down.sample <- (max.plot.points - num.mask) / (length(ploty) - num.mask)
          plot.mask <- runif(length(ploty)) < down.sample | mask
          plotx <- plotx[plot.mask]
          ploty <- ploty[plot.mask]
          if (!is.null(adj.col)) {
            adj <- adj[plot.mask]
          }
        }
      }
      plotxs[[k]] <- plotx
      plotys[[k]] <- ploty
      if (!is.null(adj.col)) {
        adjs[[k]] <- adj
      }
    } else
    {
      if (!first)
      {   
        par(new=T)
      }
      qqnorm(data[,column], main=cur.title, cex.main=main.cex, cex=cex, ylab=ylab)
      qqline(data[,column])
      first <- FALSE
    }
  }

  if (do.unif)
  {
    #NEW
    xlim <- NULL
    ylim <- NULL

    max.x <- NULL
    for (k in length(all.value.cols):1)
    { 

      plotx <- plotxs[[k]]
      ploty <- plotys[[k]]

      xrange <- range(plotx, na.rm=TRUE)
      yrange <- range(ploty, na.rm=TRUE)

      if (is.null(xlim))
      {     
        max.x <- k
        xlim <- xrange
      } else
      {
        if (xlim[1] > xrange[1])
        {
          xlim[1] <- xrange[1]
        }
        if (xlim[2] < xrange[2])
        {
          max.x <- k
          xlim[2] <- xrange[2]
        }
      }
      if (is.null(ylim))
      {     
        ylim <- yrange
        if (ylim[1] == ylim[2])
        {
          ylim <- c(0,1)
        }
      } else
      {
        if (ylim[1] > yrange[1])
        {
          ylim[1] <- yrange[1]
        }
        if (ylim[2] < yrange[2])
        {
          ylim[2] <- yrange[2]
        }
      }
      if (!is.null(draw.abline) && draw.abline > ylim[2])
      {
        ylim[2] <- draw.abline
      }
    }

    first <- TRUE

    for (k in length(all.value.cols):1)
    {
      plotx <- plotxs[[k]]
      ploty <- plotys[[k]]
      if (first)
      {   
        plot(plotx, ploty, tck=1, xlim=xlim, ylim=ylim, type="n", xlab="Theoretical quantiles", cex=cex, cex.axis=cex, cex.lab=cex, ylab=ylab, main=cur.title, cex.main=main.cex);

        if (print.lambda && !do.legend)
        {
          mtext(substitute(lambda == lval, list(lval = legend.lambdas[1])), side=3,line=1.1,padj=1,cex=main.cex * .8)
        }
      }

      if (confidence.intervals && k == max.x)
      {
        origN = nrow(data);
        mult <- 5
        #x <- -1*log10(seq(1/origN, 1, by=mult/origN));
        #cvalues <- seq(1,origN,by=mult)

        x <- rev(plotx)
        cvalues <- 10**-x * origN

        c95 <- sapply(cvalues, function (x) {qbeta(.95, x, origN - x + 1)})
        c05 <- sapply(cvalues, function (x) {qbeta(.05, x, origN - x + 1)})      
        c99 <- sapply(cvalues, function (x) {qbeta(.99, x, origN - x + 1)})
        c01 <- sapply(cvalues, function (x) {qbeta(.01, x, origN - x + 1)})      

        polygon(c(x, rev(x)), c(-1*log10(c95), rev(-1*log10(c05))), col="grey90", border=NA, xlim=xlim, ylim=ylim);
        polygon(c(x, rev(x)), c(-1*log10(c95), rev(-1*log10(c05))), col="grey90", border=NA, xlim=xlim, ylim=ylim);

        points(x, -1*log10(c95), type="l", col="blue", lwd=1.5);
        points(x, -1*log10(c05), type="l", col="blue", lwd=1.5);
      }
      points(c(0,10), c(0,10), type="l", col="red", lwd=1.5);

      text.cols <- get.list.val(all.text.cols, k)    
      text.col <- get.list.val(text.cols, i)
      value.threshes <- get.list.val(all.value.thresh, k)
      value.thresh <- sapply(get.list.val(value.threshes, i), as.numeric)

      if (!is.null(text.col) && !is.null(value.thresh))
      {
        text.mask <- ploty > -log(value.thresh,b=10)

        cur.adj <- 1.1
        if (length(adjs) > 0) {
          cur.adj <- adjs[[k]][text.mask]
        }

        for (temp.adj in unique(cur.adj)) {
          adj.mask <- cur.adj == temp.adj
          text(plotx[text.mask][adj.mask], ploty[text.mask][adj.mask], names(ploty)[text.mask][adj.mask], adj=temp.adj, cex=main.cex * .9)
        }
      }

      first <- FALSE

    }
    for (k in length(all.value.cols):1)
    {
      plotx <- plotxs[[k]]
      ploty <- plotys[[k]]

      type <- get.list.val(all.types, k)
      col <- get.list.val(all.colors, k)
      lty <- get.list.val(all.ltys, k)	
      shade.col <- get.list.val(all.shade.cols, k)

      legend.colors[k] <- if (is.null(col)) {NA} else {col}
      legend.pt.bgs[k] <- if (!is.null(shade.col)) {"grey"} else {legend.colors[k]}

      if (!is.null(type) && type == "l")
      {   
        points(plotx, ploty, type=type, lt=lty, xlim=xlim, ylim=ylim, col=col); 
        legend.pchs[k] <- NA
        legend.ltys[k] <- if (is.null(lty)) {NA} else {lty}      
      }
      else
      {   
        color <- col
        if (!is.null(shade.col))
        {
          shade.vals <- data[,shade.col]
          if (max(shade.vals) > 1 || min(shade.vals) < 0)
          {
            shade.vals <- (shade.vals - min(shade.vals)) / max(shade.vals)
          }
          color <- gray(shade.vals)
        }
    
        plotx.points <- plotx
        ploty.points <- ploty
      
        if (length(plotx.points) > 100)
        {
          line.thresh <- -log(.25, b=10)
        }
        else
        {
          line.thresh <- 0
        }
      
        plotx.mask <- plotx >= line.thresh
      
        plotx.points <- plotx[plotx.mask]
        ploty.points <- ploty[plotx.mask]
        if (!is.null(shade.col))
        {
          color <- color[plotx.mask]
        }
      
        points(plotx[!plotx.mask], ploty[!plotx.mask], type="l", col=col);
        points(plotx.points, ploty.points, type=type, pch=21, cex=.7, lwd=1.5, bg=color, col=col);
        legend.pchs[k] <- 21
        legend.ltys[k] <- NA
      }
      first <- FALSE
    } 

    if (num.value.cols > 1 && it %% num.per.page == 0)
    {
      mtext(title,side=3,outer=T,padj=1,cex=1.2 * main.cex)
    }
    it <- it + 1
  }
  if (!is.null(draw.quantile))
  {
    abline(h=as.numeric(quantile(data[,column], draw.quantile)), lt=2)
  }
  if (!is.null(draw.abline))
  {
    abline(h=draw.abline, lt=2)
  }

  if (do.legend)
  {

    legend.names <- rev(unlist(all.value.names))

    legend.names <- sapply(1:length(legend.names), function(x) {as.expression(bquote(.(legend.names[x])~(lambda == .(rev(legend.lambdas)[x]))))})

    if (sum(!is.na(legend.ltys)) == 0)
    {
      legend.ltys <- NULL
    }
    if (sum(!is.na(legend.colors)) == 0)
    {
      legend.colors <- NULL
    }
    if (sum(!is.na(legend.pchs)) == 0)
    {
      legend.pchs <- NULL
    }
    if (sum(!is.na(legend.pt.bgs)) == 0)
    {
      legend.pt.bgs <- NULL
    }


    legend("topleft", legend.names, lty=rev(legend.ltys), col=rev(legend.colors), bg="white", pch=rev(legend.pchs), pt.bg=rev(legend.pt.bgs), cex=.9)
  }
}



dev.off()

