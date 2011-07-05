rbbt.ruby <- function(code, load = TRUE){
  file = system('rbbt_exec.rb - file', input = code, intern=TRUE);
  if (load){
    data = rbbt.tsv(file);
    rm(file);
    return(data);
  }else{
    return(file);
  }
}

rbbt.glob <- function(d, pattern){
    d=sub("/$", '', d);
    sapply(dir(d, pattern), function(file){paste(d,file,sep="/")});
}

rbbt.png_plot <- function(filename, width, height, p){
    png(filename="temp.png", width=width, height=height);
    eval(p);
    dev.off();
}

rbbt.load.data <- function(filename, sep = "\t",  ...){
  data=read.table(file=filename, sep=sep, fill=TRUE,  as.is=TRUE, ...);
  return(data);
}

rbbt.tsv <- function(filename, sep = "\t", comment.char ="#", row.names=1,  ...){
  data=read.table(file=filename, sep=sep, fill=TRUE,  as.is=TRUE, quote='', row.names= row.names, comment.char = comment.char, ...);
  f = file(filename, 'r');
  headers = readLines(f, 1);
  if (length(grep("^#: ", headers)) > 0){ 
      headers = readLines(f, 1); 
  } 
  if (length(grep("^#", headers)) > 0){
      fields = strsplit(headers, sep)[[1]];
      fields = fields[2:length(fields)];
      names(data) <- fields;
  }
  close(f);
  return(data);
}

rbbt.tsv2matrix <- function(data){
  new <- data.matrix(data);
  colnames(new) <- colnames(data);
  rownames(new) <- rownames(data);
  return(new);
}

rbbt.tsv.write <- function(filename, data, key.field = NULL){
  if (is.null(key.field)){ key.field = "ID";}

  f = file(filename, 'w');

  header = paste("#", key.field, sep="");
  for (name in colnames(data)){ header = paste(header, name, sep="\t");}
  header = paste(header, "\n", sep="");
  cat(header, file=f);
  
  close(f);

  write.table(data, file=filename, quote=FALSE, append=TRUE, col.names=FALSE, row.names=TRUE, sep="\t");

  return(NULL);
}

rbbt.print.data <- function(data, file="", ...){
    write.table(data, quote=FALSE, row.name=FALSE,col.name=FALSE,file=file, ...);
}

rbbt.percent <- function(values){
    values=values/sum(values);
}

rbbt.split <- function(string){
  return(unlist(strsplit(string, "\\|")));
}

rbbt.last <-function(data){
  data[length(data)];
}

rbbt.sort_by_field <- function(data, field, is.numeric=TRUE){
    if (is.numeric){
        field.data=as.numeric(data[,field]);
    }else{
        field.data=data[,field];
    }
    index = sort(field.data, index.return = TRUE)$ix;
    return(data[index,]);
}

rbbt.add <- function(data, new){
    if (is.null(data)){
        return(c(new));
    }else{
        return(c(data, new));
    }
}

rbbt.acc <- function(data, new){
    if (is.null(data)){
        return(c(new));
    }else{
        return(unique(c(data, new)));
    }
}

rbbt.init <- function(data, new){
    if (is.null(data)){
        return(new);
    }else{
        return(data);
    }
}

rbbt.this.script = system("rbbt_Rutil.rb", intern =T)

rbbt.reload <- function (){
    source(rbbt.this.script)
}

rbbt.parse <- function(filename){
    f <- file(filename, open='r');
    lines <- readLines(f);
    close(f);

    from = match(1,as.vector(sapply(lines, function(x){grep('#[[:space:]]*START',x,ignore.case=TRUE)})));
    to   = match(1,as.vector(sapply(lines, function(x){grep('#[[:space:]]*END',x,ignore.case=TRUE)})));
    if (is.na(from)){from = 1}
    if (is.na(to)){to = length(lines)}
    return(parse(text=paste(lines[from:to],sep="\n")));
}

rbbt.run <- function(filename){
    rbbt.reload();
    eval(rbbt.parse(filename), envir=globalenv());
}


# UTILITIES

# Addapted from http://www.phaget4.org/R/image_matrix.html
rbbt.plot.matrix <- function(x, ...){
    min <- min(x);
    max <- max(x);
    yLabels <- rownames(x);
    xLabels <- colnames(x);
    title <-c();
# check for additional function arguments
    if( length(list(...)) ){
        Lst <- list(...);
        if( !is.null(Lst$zlim) ){
            min <- Lst$zlim[1];
            max <- Lst$zlim[2];
        }
        if( !is.null(Lst$yLabels) ){
            yLabels <- c(Lst$yLabels);
        }
        if( !is.null(Lst$xLabels) ){
            xLabels <- c(Lst$xLabels);
        }
        if( !is.null(Lst$title) ){
            title <- Lst$title;
        }
    }
# check for null values
    if( is.null(xLabels) ){
        xLabels <- c(1:ncol(x));
    }
    if( is.null(yLabels) ){
        yLabels <- c(1:nrow(x));
    }

    layout(matrix(data=c(1,2), nrow=1, ncol=2), widths=c(4,1), heights=c(1,1));

# Red and green range from 0 to 1 while Blue ranges from 1 to 0
    ColorRamp <- rgb( seq(0,1,length=256),  # Red
                      seq(0,1,length=256),  # Green
                      seq(1,0,length=256))  # Blue
        ColorLevels <- seq(min, max, length=length(ColorRamp));

# Reverse Y axis
    reverse <- nrow(x) : 1;
    yLabels <- yLabels[reverse];
    x <- x[reverse,];

# Data Map
    par(mar = c(3,5,2.5,2));
    image(1:length(xLabels), 1:length(yLabels), t(x), col=ColorRamp, xlab="",
          ylab="", axes=FALSE, zlim=c(min,max));
    if( !is.null(title) ){
        title(main=title);
    }
    axis(BELOW<-1, at=1:length(xLabels), labels=xLabels, cex.axis=0.7);
    axis(LEFT <-2, at=1:length(yLabels), labels=yLabels, las= HORIZONTAL<-1,
         cex.axis=0.7);

# Color Scale
    par(mar = c(3,2.5,2.5,2));
    image(1, ColorLevels,
          matrix(data=ColorLevels, ncol=length(ColorLevels),nrow=1),
          col=ColorRamp,
          xlab="",ylab="",
          xaxt="n");

    layout(1);
}


