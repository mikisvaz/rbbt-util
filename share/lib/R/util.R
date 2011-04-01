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
  data=read.table(file=filename, sep=sep, fill=TRUE,  as.is=TRUE, row.names=row.names, comment.char = comment.char, ...);
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
  return(strstring(string, "\\|"));
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
        return(new);
    }else{
        return(c(data, new));
    }
}

rbbt.acc <- function(data, new){
    if (is.null(data)){
        return(new);
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

rbbt.libdir = '~/config/lib/R/lib'
rbbt.this.script = paste(rbbt.libdir, 'util.R',sep="/")

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


