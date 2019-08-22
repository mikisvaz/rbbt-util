rbbt.ruby.exec.singularity <- function(code, image){
  return(system(paste('singularity exec -e ', image,' rbbt_exec.rb - file', sep=""), input = code, intern=TRUE));
}

rbbt.ruby.exec <- function(code){
  return(system('rbbt_exec.rb - file', input = code, intern=TRUE));
}

rbbt.ruby <- function(code, load = TRUE, flat = FALSE, type = 'tsv', ...){
  file = rbbt.ruby.exec(code)

  error_str = "^#:rbbt_exec Error"
  if (regexpr(error_str, file)[1] != -1 ){
    print(file);
    return(NULL);
  }

  if (load){
      if(type == 'tsv'){
          if(flat){
              data = rbbt.flat.tsv(file, ...);
          }else{
              data = rbbt.tsv(file, ...);
          }
          rm(file);
          return(data)
      }

      if(type == 'list' || type == 'array'){
          data = scan(file, what='string', sep="\n", ...)
          return(data);
      }

      if(type == 'string'){
          return(file);
      }
      
      return(NULL);
  }else{
    return(file);
  }
}

rbbt.job.prov <- function(path){
  code <- '
require "rbbt-util"
require "rbbt/workflow"

path="PATH"

job = Workflow.load_step path

data = TSV.setup({}, "ID~Workflow,Task,Path#:type=:list")

job.rec_dependencies.each_with_index do |dep,i|
  id = "dependency-#{i}"
  data[id] = [dep.workflow, dep.task_name, dep.path]
end

data
'
  code = sub("PATH", path, code)
  return(rbbt.ruby(code, type='tsv'))
}

rbbt.job <- function(workflow, task, load=TRUE, flat = FALSE, type = 'tsv', jobname="Default", code='', log=4, ...){

    str = "require 'rbbt/workflow'"

    log_str = paste("Log.severity=", log)

    str = paste(str, log_str, code, sep="\n")

    str = paste(str, paste("wf = Workflow.require_workflow '", workflow, "'", sep=""), sep="\n")

    args_list = list(...)
    args_strs = c()
    tmp_files = c()

    for (input in names(args_list)){
        value = args_list[[input]]
        input = sub('input\\.', '', input)
        if (is.vector(value) && length(value) > 1){
            file = tempfile()
            writeLines(value, file)
            tmp_files = c(tmp_files, file)
            value = paste("Open.read('", file, "').split(\"\\n\")", sep="")
        }else{
            if (!is.numeric(value)){
                if (all(value %in% TRUE)){
                    value = 'true'
                }else{
                    if (all(value %in% FALSE)){
                        value = 'false'
                    }else{
                        if (is.data.frame(value)){
                            file = tempfile()
                            rbbt.tsv.write(file, value)
                            tmp_files = c(tmp_files, file)
                            value = paste("TSV.open('", file, "')", sep="")
                        }else{
                            value = paste("'", value, "'", sep="")
                        }
                    }
                }
            }
        }
        args_strs = c(args_strs, paste(":",input,' => ',value, sep=""))
    }

    args_str = paste(args_strs, collapse=",")
    str = paste(str, paste('wf.job(:', task, ", '", jobname, "', ", args_str,').produce.path', sep=""), sep="\n")

    res = rbbt.ruby(str, load, flat, type)

    unlink(tmp_files)

    return(res);
}

rbbt.ruby.substitutions <- function(script, substitutions = list(), ...){
    
    for (keyword in names(substitutions)){
        script = sub(keyword, substitutions[[keyword]], script);
    }

    result = rbbt.ruby(script, ...);

    return(result);
}

rbbt.glob <- function(d, pattern){
    d=sub("/$", '', d);
    sapply(dir(d, pattern), function(file){paste(d,file,sep="/")});
}


rbbt.load.data <- function(filename, sep = "\t",  ...){
  data=read.table(file=filename, sep=sep, fill=TRUE,  as.is=TRUE, ...);
  return(data);
}

rbbt.flat.tsv <- function(filename, sep = "\t", comment.char ="#", ...){
  f = file(filename, 'r');
  line = readLines(f, 1);
  if (length(grep("^#: ", line)) > 0){ 
      line = readLines(f, 1); 
  } 
  if (comment.char=="" || length(grep("^# ", line)) > 0){ 
      line = readLines(f, 1); 
  } 
  result = list();
  while( TRUE ){
    parts = unlist(strsplit(line, sep, fixed = TRUE));
    id = parts[1];
    result[[id]] = parts[2:length(parts)];
    line = readLines(f, 1);
    if (length(line) == 0){ break;}
  }
  close(f);
  return(result);
}

rbbt.tsv.columns <- function(filename, sep="\t", comment.char="#"){
  f = file(filename, 'r');
  headers = readLines(f, 1);
  if (length(grep("^#:", headers)) > 0){ 
      headers = readLines(f, 1); 
  } 
  if (comment.char == "" || length(grep("^#", headers)) > 0){
      fields = strsplit(headers, sep)[[1]];
      close(f);
      return(fields);
  }
  close(f);
  return(NULL);
}

rbbt.tsv <- function(filename, sep = "\t", comment.char ="#", row.names=1, check.names=FALSE, fill=TRUE, as.is=TRUE, quote='',  ...){
 
  if (comment.char == ""){
      data=read.table(file=filename, sep=sep, fill=fill, as.is=as.is, quote=quote, row.names= row.names, comment.char = comment.char, skip=1, ...);
  }else{
      data=read.table(file=filename, sep=sep, fill=fill, as.is=as.is, quote=quote, row.names= row.names, comment.char = comment.char, ...);
  }

  columns = rbbt.tsv.columns(filename, sep, comment.char=comment.char)
  if (! is.null(columns)){
      names(data) <- columns[2:length(columns)];
      attributes(data)$key.field = substring(columns[1],2);
  }

  return(data);
}

rbbt.tsv.comma <- function(tsv){
    for (c in names(tsv)){
        v = tsv[,c]
        if (is.character(v)){
            v = gsub('\\|', ', ', v)
            tsv[,c] = v
        }
    }
    return(tsv)
}

rbbt.tsv.numeric <- function(filename, sep="\t", ...){

    columns = rbbt.tsv.columns(filename, sep)

    colClasses = c('character', rep('numeric', length(columns) - 1))

    return(rbbt.tsv(filename, sep, colClasses= colClasses, ...))
}

rbbt.tsv2matrix <- function(data){
  d = data

  d[d == TRUE] = 1
  d[d == FALSE] = 0

  d[d == 'true'] = 1
  d[d == 'false'] = 0

  new <- data.matrix(d);
  colnames(new) <- colnames(data);
  rownames(new) <- rownames(data);
  return(new);
}

rbbt.tsv.write <- function(filename, data, key.field = NULL, extra_headers = NULL, eol="\n", ...){

  if (is.null(key.field)){ key.field = attributes(data)$key.field;}
  if (is.null(key.field)){ key.field = "ID";}

  f = file(filename, 'wb');

  if (!is.null(extra_headers)){
      extra_headers = paste("#: ", extra_headers, "\n", sep="");
      cat(extra_headers, file=f);
  }

  header = paste("#", key.field, sep="");
  for (name in colnames(data)){ header = paste(header, name, sep="\t");}
  header = paste(header, "\n", sep="");
  cat(header, file=f);
  
  write.table(data, file=f, quote=FALSE, append=TRUE, col.names=FALSE, row.names=TRUE, sep="\t", eol="\n", ...);

  close(f);

  return(NULL);
}

rbbt.print.data <- function(data, file="", ...){
    write.table(data, quote=FALSE, row.name=FALSE,col.name=FALSE,file=file, ...);
}

rbbt.percent <- function(values){
    values=values/sum(values);
}

rbbt.a.to.string <- function(a){
   paste("'",paste(a, collapse="', '"), "'", sep="");
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

rbbt.this.script = NULL;

rbbt.reload <- function (){
    if (is.null(rbbt.this.script)){
        rbbt.this.script = system("rbbt_Rutil.rb", intern =T)
    }
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

rbbt.pull.keys <- function(items, key){
    pulled = list()
    rest = list()

    names = names(items)

    prefix = paste("^",key,'.',sep='')
    matches = grep(prefix, names)

    for (i in seq(1,length(names))){
        if (i %in% matches){
            name = names[i]
            name = sub(prefix, "", name)
            pulled[[name]] = items[[i]]
        }else{
            name = names[i]
            rest[[name]] = items[[i]]
        }
    }
    
    list(pulled=pulled, rest=rest)
}

rbbt.run <- function(filename){
    rbbt.reload();
    eval(rbbt.parse(filename), envir=globalenv());
}



# UTILITIES

rbbt.log <- function(m){
    head = "R-Rbbt"
    cat(paste(head, "> ", m,"\n",sep=""), file = stderr())
}

rbbt.ddd <- function(o){
    cat(toString(o), file = stderr())
    cat("\n", file = stderr())
}

# From: http://ryouready.wordpress.com/2008/12/18/generate-random-string-name/
rbbt.random_string <- function(n=1, length=12){
    randomString <- c(1:n)
    for (i in 1:n){
        randomString[i] <- paste(sample(c(0:9, letters, LETTERS), length, replace=TRUE), collapse="")
    }
    return(randomString)
}


# {{{ MODELS


rbbt.model.fit <- function(data, formula, method=lm, ...){
    method(formula, data = data, ...);
}

rbbt.model.groom <- function(data, variables = NULL, classes = NULL, formula = NULL){
    names = names(data)
    if (is.null(variables)){
        if (is.null(formula)){
            variables = names
        }
        variables = names[names %in% all.vars(formula)]
    }

    data.groomed = data[,variables,drop=F]

    if (! is.null(classes)){
        if (is.character(classes)){ classes = rep(classes,dim(data.groomed)[2]) }
        i = 1
        for (class in classes){
            v = data.groomed[, i]
            v = switch(class, numeric =as.numeric(v), character = as.character(v), factor = as.factor(v), boolean = as.logical(v), logical = as.logical(v))
            data.groomed[,i] = v
            i = i+1
        }
    }

    data.groomed
}

rbbt.model.predict <- function(model, data, ...){
    predictions = predict(model, newdata = data, ...);
    predictions
}

rbbt.loaded.models = list();
rbbt.model.load <- function(file, force = F){
    if (is.null(rbbt.loaded.models[[file]])){
        load(file)
        rbbt.loaded.models[[file]] = model
    }
    rbbt.loaded.models[[file]]
}

rbbt.model.add_fit <- function(data, formula, method, classes=NULL, ...){
    data.groomed = rbbt.model.groom(data, formula=formula, classes = classes);

    args = list(...)
    args.pull = rbbt.pull.keys(args, 'predict')
    predict.args = args.pull$pulled

    args.pull = rbbt.pull.keys(args.pull$rest, 'fit')

    fit.args = args.pull$pulled
    args.rest = args.pull$rest

    fit.args =c(fit.args, args.rest)
    predict.args =c(predict.args, args.rest)

    fit.args[["data"]] = data.groomed
    fit.args[["formula"]] = formula
    fit.args[["method"]] = method

    model = do.call(rbbt.model.fit, fit.args);

    response = rbbt.model.formula.reponse(formula)
    data.groomed[[response]] = NULL

    predict.args[["model"]] = model
    predict.args[["data"]] = data.groomed

    predictions = do.call(rbbt.model.predict,predict.args)

    data$Prediction = predictions;
    data
}

rbbt.model.formula.reponse <- function(formula){
    tt <- terms(formula)
    vars <- as.character(attr(tt, "variables"))[-1] ## [1] is the list call
    response.index <- attr(tt, "response") # index of response var
    as.character(vars[response.index])
}

rbbt.model.inpute <- function(data, formula, ...){
    data = rbbt.model.add_fit(data, formula=formula, ...)

    response = rbbt.model.formula.reponse(formula)

    rows = rownames(data)
    missing = rows[is.na(data[,c(response)])]

    predictions = data[missing, "Prediction"]

    data[missing,c(response)] = predictions
#    data$Prediction = NULL
    data
}

rbbt.tsv.melt <- function(tsv, variable = NULL, value = NULL, key.field = NULL){
    if (is.null(key.field)){ key.field = attributes(data)$key.field;}
    if (is.null(key.field)){ key.field = "ID" }

    if (is.null(variable)){ variable = "variable" }
    if (is.null(value)){ value = "value" }

    tsv[key.field] = rownames(tsv)

    m <- melt(tsv)

    names(m) <- c(key.field, variable, value)

    return(m)
}

rbbt.ranks <- function(x){
    l = sum(!is.na(x))
    i = sort(x, index.return=T, na.last=NA)$ix 
    vv = rep(NA,length(x))

    c = 1
    for (pos in i){
        vv[pos] = c / l
        c = c + 1
    }
    return(vv)
}

rbbt.ranks <- function(x){
    x = as.numeric(x)
    missing = is.na(x)
    l = sum(!missing)
    x.fixed = x[!missing]
    x.i = sort(x.fixed, index.return=T, na.last=NA)$ix 

    vv = rep(NA,length(x))

    c = 1
    for (pos in x.i){
        vv[pos] = c / l
        c = c + 1
    }

    vv.complete = rep(NA,length(x))
    vv.complete[!missing] = vv
    return(vv.complete)
}

rbbt.default_code <- function(organism){
    return(organism + "/feb2014")
}

# Adapted from
# http://stackoverflow.com/questions/27418461/calculate-the-modes-in-a-multimodal-distribution-in-r
# by http://stackoverflow.com/users/6388753/ferroao
rbbt.get.modes <- function(x,bw = NULL,spar = NULL) {  
    if (is.null(bw)) bw = bw.nrd0(x);
    if (is.null(spar)) spar = 0.1;

    den <- density(x, kernel=c("gaussian"),bw=bw)
    den.s <- smooth.spline(den$x, den$y, all.knots=TRUE, spar=spar)
    s.1 <- predict(den.s, den.s$x, deriv=1)
    s.0 <- predict(den.s, den.s$x, deriv=0)
    den.sign <- sign(s.1$y)
    a<-c(1,1+which(diff(den.sign)!=0))
    b<-rle(den.sign)$values
    df<-data.frame(a,b)
    df = df[which(df$b %in% -1),]
    modes<-s.1$x[df$a]
    density<-s.0$y[df$a]
    df2<-data.frame(modes,density)
    df2<-df2[with(df2, order(-density)), ] # ordered by density
    df2
}

#{{{ PLOTS

rbbt.png_plot <- function(filename, p, width=500, height=500, ...){
    png(filename=filename, width=width, height=height, ...);
    eval(parse(text=p));
    dev.off()
}

rbbt.tiff_plot <- function(filename, p, width=500, height=500, ...){
    tiff(filename=filename, width=width, height=height, ...);
    eval(parse(text=p));
    dev.off()
}


rbbt.pheatmap <- function(filename, data, width=800, height=800, take_log=FALSE, stdize=FALSE, positive=FALSE, ...){
    rbbt.require('pheatmap')

    #opar = par()
    png(filename=filename, width=width, height=height);

    data = as.matrix(data)
    data[is.nan(data)] = NA

    data = data[rowSums(is.na(data))==0, ]

    if (take_log){
        for (study in colnames(data)){
            skip = sum(data[, study] <= 0) != 0
            if (!skip){
                data[, study] = log(data[, study])
            }
        }
        data = data[, colSums(is.na(data))==0]
    }


    if (stdize){
        rbbt.require('pls')
        data = stdize(data)
    }

    if (positive){
        pheatmap(data,color= colorRampPalette(c("white", "red"))(100), ...)
    }else{
        pheatmap(data, ...)
    }

    #par(opar)
    dev.off();
}

rbbt.heatmap <- function(filename, data, width=800, height=800, take_log=FALSE, stdize=FALSE, ...){
    opar = par()
    png(filename=filename, width=width, height=height);

    #par(cex.lab=0.5, cex=0.5, ...)

    data = as.matrix(data)
    data[is.nan(data)] = NA

    #data = data[rowSums(!is.na(data))!=0, colSums(!is.na(data))!=0]
    data = data[rowSums(is.na(data))==0, ]

    if (take_log){
        for (study in colnames(data)){
            skip = sum(data[, study] <= 0) != 0
            if (!skip){
                data[, study] = log(data[, study])
            }
        }
        data = data[, colSums(is.na(data))==0]
    }

    rbbt.require('pls')

    if (stdize){
        data = stdize(data)
    }

    heatmap.2(data, scale='column', ...)

    dev.off();
    par(opar)
}

# Addapted from http://www.phaget4.org/R/image_matrix.html
rbbt.plot.matrix <- function(x, ...){
    min <- min(x, na.rm=T);
    max <- max(x, na.rm=T);
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

# Adapted from: https://rstudio-pubs-static.s3.amazonaws.com/13301_6641d73cfac741a59c0a851feb99e98b.html
rbbt.plot.venn <- function(data, a, ...) {
    rbbt.require('VennDiagram')
    group.matches <- function(data, fields) {
        sub = data
        for (i in 1:length(fields)) {
            sub <- subset(sub, sub[,fields[i]] == T)
        }
        nrow(sub)
    }

    if (length(a) == 1) {
        out <- draw.single.venn(group.matches(data, a), ...)
    }
    if (length(a) == 2) {
        out <- draw.pairwise.venn(group.matches(data, a[1]), group.matches(data, a[2]), group.matches(data, a[1:2]), ...)
    }
    if (length(a) == 3) {
        out <- draw.triple.venn(group.matches(data, a[1]), group.matches(data, a[2]), group.matches(data, a[3]), group.matches(data, a[1:2]), 
            group.matches(data, a[2:3]), group.matches(data, a[c(1, 3)]), group.matches(data, a), ...)
    }
    if (length(a) == 4) {
        out <- draw.quad.venn(group.matches(data, a[1]), group.matches(data, a[2]), group.matches(data, a[3]), group.matches(data, a[4]), 
            group.matches(data, a[1:2]), group.matches(data, a[c(1, 3)]), group.matches(data, a[c(1, 4)]), group.matches(data, a[2:3]), 
            group.matches(data, a[c(2, 4)]), group.matches(data, a[3:4]), group.matches(data, a[1:3]), group.matches(data, a[c(1, 2, 
                4)]), group.matches(data, a[c(1, 3, 4)]), group.matches(data, a[2:4]), group.matches(data, a), ...)
    }
    if (length(a) == 5) {
        out <- draw.quintuple.venn(
            group.matches(data, a[1]), group.matches(data, a[2]), group.matches(data, a[3]), group.matches(data, a[4]), group.matches(data, a[5]), 
            group.matches(data, a[c(1, 2)]), group.matches(data, a[c(1, 3)]), group.matches(data, a[c(1, 4)]), group.matches(data, a[c(1, 5)]), 
            group.matches(data, a[c(2, 3)]), group.matches(data, a[c(2, 4)]), group.matches(data, a[c(2, 5)]),
            group.matches(data, a[c(3, 4)]), group.matches(data, a[c(3, 5)]),
            group.matches(data, a[c(4, 5)]),
            group.matches(data, a[c(1, 2, 3)]),group.matches(data, a[c(1, 2, 4)]),group.matches(data, a[c(1, 2, 5)]),
            group.matches(data, a[c(1, 3, 4)]),group.matches(data, a[c(1, 3, 5)]),
            group.matches(data, a[c(1, 4, 5)]),
            group.matches(data, a[c(2, 3, 4)]),group.matches(data, a[c(2, 3, 5)]),
            group.matches(data, a[c(2, 4, 5)]),
            group.matches(data, a[c(3, 4, 5)]),
            group.matches(data, a[c(1, 2, 3, 4)]),group.matches(data, a[c(1, 2, 3, 5)]),group.matches(data, a[c(1, 2, 4, 5)]),group.matches(data, a[c(1, 3, 4, 5)]),group.matches(data, a[c(2, 3, 4, 5)]),
            group.matches(data, a),
            ...)

    }
    if (!exists("out")) 
        out <- "Oops"
    return(out)
}

rbbt.plot.pca <- function(data, center = TRUE, scale. = TRUE, ...) {
  rbbt.require('vqv/ggbiplot')
  data <- rbbt.impute(data)
  pca <- prcomp(data, center=center, scale.=scale.)
  ggbiplot(pca, ...)
}


rbbt.plot.text_scatter <- function(formula, data) {
    plot(formula, data=data, cex = 0)
    text(formula, data=data, cex = 0.6, labels=rownames(data))
}

rbbt.install.CRAN <- function(pkg){
    cat("Try CRAN install:", pkg, "\n")
    res = FALSE
    tryCatch({ install.packages(pkg); library(pkg); res = TRUE; }, error = function(e){ str(e); warning(paste("Could not install CRAN ", pkg)); res = FALSE })
    return(res)
}

rbbt.install.bioc <-function(pkg){
    cat("Try BIOC install:", pkg, "\n")
    res = FALSE
    tryCatch({ source("http://bioconductor.org/biocLite.R"); biocLite(pkg, ask=FALSE, suppressUpdates = TRUE); res = TRUE }, error = function(e){ warning(paste("Could not install Bioconductor ", pkg, "\n")); res = FALSE })
    return(res)
}

rbbt.install.biocManager <-function(pkg, ...){
    cat("Try BiocManager install:", pkg, "\n")
    res = FALSE
    tryCatch({ BiocManager::install(pkg, ...); res = TRUE }, error = function(e){ warning(paste("Could not install BiocManager ", pkg, "\n")); res = FALSE })
    return(res)
}

rbbt.install.github <- function(pkg, ...){
    cat("Try GITHUB install:", pkg, "\n")
    res = FALSE
    tryCatch({ library(devtools); install_github(pkg, ...); res = TRUE }, error = function(e){ warning(paste("Could not install GITHUB ", pkg, "\n")); res = FALSE })
    return(res)
}

rbbt.require <- function(pkg, ...){
    list.of.packages <- c(pkg)

    clean.packages <- c()
    for (pkg in list.of.packages){ 
        parts = strsplit(pkg,'/')[[1]]
        clean.packages <- c(clean.packages, parts[length(parts)])
    }

    new.packages <- list.of.packages[!(clean.packages %in% installed.packages()[,"Package"])]

    for (pkg in new.packages){ 
        if (!rbbt.install.github(pkg, ...)){
            if (!rbbt.install.CRAN(pkg, ...)){
              if (!rbbt.install.biocManager(pkg, ...)){
                if (!rbbt.install.bioc(pkg, ...)){
                        stop("Error!", pkg)
                    }
                }
            }
        }
    }

    library(clean.packages, character.only=T)
}

rbbt.psd <- function(m){
    e = eigen(m) 
    library(MASS)

    values = e$values  
    values[values < 0] = 0 

    p = e$vectors %*% diag(values) %*% t(e$vectors)

    rownames(p) <- rownames(m)
    colnames(p) <- colnames(m)

    return(p)
}

rbbt.impute <- function(data){
    m = as.matrix(data)
    if (sum(is.na(m)) == 0){
        return (m)
    }else{
        rbbt.require('Hmisc')
        m.i = impute(m)
        return(m.i)
    }
}

rbbt.fix_distance <- function(data){

    m = rbbt.impute(data)

    p <- rbbt.psd(m)
    p <- m

    d <- as.dist(p)   

    return(d)
}

