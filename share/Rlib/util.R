rbbt.ruby <- function(code, load = TRUE, flat = FALSE, type = 'tsv', ...){
  file = system('rbbt_exec.rb - file', input = code, intern=TRUE);

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

      if(type == 'list'){
          data = scan(file, ...)
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
  headers = readLines(f, 1);
  if (length(grep("^#: ", headers)) > 0){ 
      headers = readLines(f, 1); 
  } 
  result = list();
  while( TRUE ){
    line = readLines(f, 1);
    if (length(line) == 0){ break;}
    parts = unlist(strsplit(line, sep, fixed = TRUE));
    id = parts[1];
    result[[id]] = parts[2:length(parts)];
  }
  close(f);
  return(result);
}

rbbt.tsv <- function(filename, sep = "\t", comment.char ="#", row.names=1, check.names=FALSE, fill=TRUE, as.is=TRUE, quote='',  ...){
  data=read.table(file=filename, sep=sep, fill=fill, as.is=as.is, quote=quote, row.names= row.names, comment.char = comment.char, ...);
  f = file(filename, 'r');
  headers = readLines(f, 1);
  if (length(grep("^#:", headers)) > 0){ 
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

rbbt.tsv.write <- function(filename, data, key.field = NULL, extra_headers = NULL){
  if (is.null(key.field)){ key.field = "ID";}

  f = file(filename, 'w');

  if (!is.null(extra_headers)){
      extra_headers = paste("#: ", extra_headers, "\n", sep="");
      cat(extra_headers, file=f);
  }

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

rbbt.png_plot <- function(filename, width, height, p, ...){
    png(filename=filename, width=width, height=height, ...);
    eval(parse(text=p));
}

rbbt.heatmap <- function(filename, width, height, data, take_log=FALSE, ...){
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

    data = stdize(data)

    heatmap.2(data, margins = c(20,5), scale='column')

    dev.off();
    par(opar)
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

rbbt.tsv.melt <- function(tsv, key_field = 'ID'){
    tsv[key_field] = rownames(tsv)
    return(melt(tsv))
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
