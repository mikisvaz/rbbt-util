rbbt.require('plyr')
rbbt.require('proto')
rbbt.require('ggplot2')
rbbt.require('gridSVG')
rbbt.require('grid')
rbbt.require('XML')
rbbt.require('ggthemes')
rbbt.require('Cairo')

# Modified from http://aaronecay.com/blog/2014/02/tooltips-in-ggplot/

rbbt.SVG.extract <- function(plot, size=NULL, prefix=NULL, ...){

    if (is.null(prefix)) prefix = rbbt.random_string();
    if (is.null(size)){
        print(plot, type='cairo');
        mysvg <- grid.export(prefix=prefix, ...)
    }else{
        base.size = 10 * (7/size)
        resolution = 72 * (size/7)

        if (length(plot$theme) == 0) plot <- plot + theme_light();
        if (length(plot$theme$text) == 0) plot <- plot + theme(text = element_text(size=base.size));

        plot$theme$text$size = base.size

        print(plot, type='cairo')
        mysvg <- grid.export(res=resolution, prefix=prefix, ...)
    }

    xml <- saveXML(mysvg$svg)
    xml
}

rbbt.SVG.save <- function(filename, plot, width=NULL, height=NULL, ...){
    if (is.null(width)){
        if (is.null(height)){
            size = NULL
        }else{
            size=height
        }
    }else{
        if (is.null(height)){
            size = width
        }else{
            size=max(width, height)
        }
    }

    xml = rbbt.SVG.extract(plot, size, ...)
    fileConn<-file(filename, 'w')
    cat(xml, file=fileConn)
    close(fileConn)
}

rbbt.SVG.save.fast <- function(filename, plot, width=3, height=3){
    ggsave(file=filename, plot, width=width, height=height);
}

