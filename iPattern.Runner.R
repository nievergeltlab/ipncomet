##########################################################
#
# iPattern, R script for CNV Mining
# Copyright 2008 Junjun Zhang, The Hospital for Sick Children
# All rights reserved.
# Last Modified Date: Jan. 29, 2008
# Please keep the Copyright information.
#
# PLEASE DO NOT REMOVE THIS HEADER FOR ANY REASON.
##########################################################

require(ppc)
require(cluster)

rm(list=ls())

ipVersion = "0.58"

source("iPattern.Func.R")

filenames = c("","");

i = 0
for(e in commandArgs()){
  if (length(grep("--",e))>0) {
    next
  }
  if (length(grep("R$",e))>0) {
    next
  }
  if (length(grep("Rgui.exe$",e))>0) {
    next
  }
  i = i + 1
  filenames[i] = e
}

if (filenames[1]=="") {
  filenames[1] = "ipattern.conf"
}

# The second input file is the data file
if (filenames[2]=="") {  ## this is only for testing
  filenames[2] = "chr14.affy6.295.105Mb.int"
}

debug = FALSE

settings = scan (file=filenames[1],what=list("",""),skip=0,sep="=",comment.char="#")
conf = setting.parser(settings)
winSize = conf$"winSize"
maxGapProbe = winSize
bandWidth = conf$"bandWidth"  ## bandwidth for density estimation
v.p.ratio = conf$"peakSeparation"
maxValleyHeightForPerfectSeparation = 0.01
maxProbeDistance = conf$"maxProbeDistance"
offProbeSdThreshold = 0.8  #conf$"offProbeSdThreshold"

pTrim = 0.1

data = read.table(file=filenames[2], sep="\t", header=T, comment.char = "") #Adam hack : sometimes, for a really stupid reason, SNPs have hashtags in the names, and cant be read!!! this turns that off!

# the first line should be header line, most of them are sample names
columnHeaders = names(data[1,])  ## header

c = 0
cA = c()
cB = c()
cCall = c()
samples = c()
for (h in columnHeaders) {
  c = c + 1
  if (length(grep("_signal_a", h, ignore.case=T))) {
    cA = c(cA, c)
  } else if (length(grep("_signal_b", h, ignore.case=T))) {
    cB = c(cB, c)
  } else if (length(grep("call", h, ignore.case=T))) {
    cCall = c(cCall, c)
    samples = c(samples, sub(".call","",h,ignore.case=T))
  }
}

dataSnp = as.matrix(data[,1:3]) ## keep probe ID, chr, position
dataInt = c() ## keep total probe intensity (sum of A and B if it's SNP type)
dataIntLogAoB = c() ## keep the ratio of intensities of allele A over allele B, or empty for non-SNP type probe
dataCall = c() ## keep genotype calls, "-" for non-SNP type probe
countSnp = 0
countSample = 0

if (length(columnHeaders) == (3 + length(cA) + length(cCall) + length(cB))) { ## SNP format
  dataA = as.matrix(data[,cA])
  dataA[which(dataA<=0)] = 1e-30
  dataB = as.matrix(data[,cB])
  dataB[which(dataB<=0)] = 1e-30
  dataInt = dataA + dataB
  dimnames(dataInt)[[2]] = samples
  dataIntLogAoB = log(dataA / dataB, base=10)
  dimnames(dataIntLogAoB)[[2]] = samples
  dataCall = as.matrix(data[,cCall])
  dimnames(dataCall)[[2]] = samples
  countSnp = length(dataInt[,1])
  countSample = length(dataInt[1,])
  rm(dataA)
  rm(dataB)
}else{ ## non-SNP format, no allele specific intensity
  dataInt = as.matrix(data[,-(1:3)])
  countSnp = length(dataInt[,1])
  countSample = length(dataInt[1,])
  samples = names(dataInt[1,1:countSample])
}

rm(data)
gcinfo(F)
gc()

if (typeof(dataInt) == "character" | typeof(dataInt) == "NULL") {
  stop("Input file in wrong format!")
}

if (countSnp < winSize) {
  stop("Window size must be no larger than the total number of probes!")
}

# Read in which samples can be used for representatives of peak 
sampleRepFlag = rep(T, countSample)
if (length(list.files(path = ".", pattern ="^sample.info.txt$")) > 0) {
  sampleInfo = read.table(file="sample.info.txt", sep="\t", header=T)
  dimnames(sampleInfo)[[1]]=sampleInfo[,1]

  if (length(grep("X$", dataSnp[1,2], ignore.case = T))>0 || length(grep("Y$", dataSnp[1,2], ignore.case = T))>0) { # now this is the sex chromosome
    if (sampleInfo[samples[1],"sexChro"] == "XX") { # female
      sampleInfo = sampleInfo[sampleInfo[,"sexChro"]=="XX",]
    }else if (sampleInfo[samples[1],"sexChro"] == "XY") { # male
      sampleInfo = sampleInfo[sampleInfo[,"sexChro"]=="XY",]
    }
  }

  if (length(samples %in% sampleInfo[,"sampleID"]) < countSample) {
    stop("Sample IDs in intensity file and sample.info.txt file do not match!")
  }
  sampleRepFlag[samples %in% sampleInfo[unique(sort(c(which(sampleInfo[,"mother"] != "-"), which(sampleInfo[,"father"] != "-"), which(sampleInfo[,"replicate"] == 1), which(sampleInfo[,"excludeFromRep"] == 1)))), "sampleID"]] = F
}

# Not sure what this part means??????
dataIRank = matrix(nrow=dim(dataInt)[1],ncol=dim(dataInt)[2])
dimnames(dataIRank)[[2]] = samples
dimnames(dataIRank)[[1]] = 1:countSnp
mode(dataIRank) = 'integer'
for(i in 1:countSnp){
  dataIRank[i,] = rank(dataInt[i,], ties.method="r")
  dataInt[i,] = dataInt[i,]/median(dataInt[i,])*2
}


knownCnvrFile = list.files(".","^known.cnvr.txt$")
knownCnvrExt = 0.2

# a temporary solution of inputing parameters

kd.bw=-1
bgSD=-1
clusterWidthMax=-1
if ( is.null ( conf$"bw" ) ) {
    bg = bg.assessing(knownCnvrFile, knownCnvrExt)
    kd.bw = bg$bw
    bgSD = bg$bg.sd
    cw = bg$cw
    clusterWidthMax = round(sd(cw)*4 + mean(cw,trim=0.2), 2)
} else {
    kd.bw=conf$"bw"
    bgSD=conf$"bgSD"
    clusterWidthMax=conf$"clusterWidthMax"
}

summaryOut = c( "## iPattern CNV Analysis Report ##",
                paste("# iPattern version:",ipVersion,collapse=""),
                paste("# Input file name:",filenames[2],collapse=""),
                paste("# Chromosome:",dataSnp[1,2],collapse=""),
                paste("# Number of markers:",countSnp,collapse=""),
                paste("# Number of samples:",countSample,collapse=""),
                paste("# Number of unrelated samples:",length(which(sampleRepFlag)),collapse=""),
                paste("# Allowed max adjacent probe distance in one CNV (Kb):",maxProbeDistance/1000,collapse=""),
                paste("# Scannin window size:",winSize,collapse=""),
                paste("# Peak separation:",v.p.ratio,collapse=""),
                paste("# Background SD:",bgSD,collapse=""),
                paste("# Bandwidth setting:",bandWidth,collapse=""),
                paste("# Bandwidth used for density estimation:",kd.bw,collapse=""),
                paste("# Cluster width threshold for complex CNVR:",clusterWidthMax,collapse="")
               )
write.table(matrix(summaryOut,ncol=1,byrow=T), file=paste(basename(filenames[2]),"ipttn.txt",sep="."), append=F, sep="\t", col.names=FALSE, row.names=FALSE, quote=FALSE)


# Inisitalization?
iws.clusters = list(start.probe.idx=c(),sum.int=list(),cluster.no=c(),centre=c(),height=c(),width=c(),left.wing=c(),right.wing=c(),samples=list())
win.clusters = list()

start=1 ## for testing
winCount=1 ## for testing

probes = c() ## for testing
for (s in 1:(countSnp-winSize+1)) { 
  probes = s:(s+winSize-1)

  # only do things for probes within certain distance
  if (!distance.valid(probes[1],probes[length(probes)])) next

  win.clusters = clusterAnalyzer(probes)

  if (win.clusters$cluster.no == 1 & win.clusters$width[1] <= clusterWidthMax) next  ## this is not a interesting window


  iws.idx = length(iws.clusters$start.probe.idx) + 1
  for (wc in 1:win.clusters$cluster.no) {  ## add all clusters found in the current window to the iws.clusters variable for later use
    iws.clusters$start.probe.idx[iws.idx] = s
    iws.clusters$sum.int[[iws.idx]] = win.clusters$sum.int[win.clusters$samples[[wc]]]
    iws.clusters$cluster.no[iws.idx] = win.clusters$cluster.no
    iws.clusters$centre[iws.idx] = win.clusters$centre[wc]
    iws.clusters$height[iws.idx] = win.clusters$height[wc]
    iws.clusters$width[iws.idx] = win.clusters$width[wc]
    iws.clusters$left.wing[iws.idx] = win.clusters$left.wing[wc]
    iws.clusters$right.wing[iws.idx] = win.clusters$right.wing[wc]
    iws.clusters$samples[[iws.idx]] = win.clusters$samples[[wc]]
    iws.idx = iws.idx + 1
  }
}

start = start + winCount  ## for testing




candidate.windows = unique(iws.clusters$start.probe.idx)
# stop the program, as there might be no interesting windows
if (length(candidate.windows)==0) stop("No candidate windows!") 

# Slide down interesting windows and merge two windows if necessary
candidate.cnvr.idx = list()
candidate.cnvr.idx[[1]] = candidate.windows[1]
if (length(candidate.windows)>1) {
  for (cwi in 2:length(candidate.windows)) {
    prev.start = candidate.windows[cwi-1]
    curr.start = candidate.windows[cwi]

    if (curr.start > prev.start + winSize + maxGapProbe | !distance.valid(prev.start, curr.start+winSize-1)) { # these two should not be merged, then add a new cnvr. The merging criterion should be the same as merging initial CNVs found in scanning windows
      candidate.cnvr.idx[[length(candidate.cnvr.idx)+1]] = c(curr.start)
    }else{ # these two should be merged
      candidate.cnvr.idx[[length(candidate.cnvr.idx)]] = c(candidate.cnvr.idx[[length(candidate.cnvr.idx)]], curr.start)
    }
  }
}

cnv.miner()
