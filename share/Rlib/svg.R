rbbt.require('plyr')
rbbt.require('proto')
rbbt.require('ggplot2')
rbbt.require('gridSVG')
rbbt.require('grid')
rbbt.require('XML')
rbbt.require('ggthemes')
rbbt.require('Cairo')

# Modified from http://aaronecay.com/blog/2014/02/tooltips-in-ggplot/

rbbt.SVG.extract <- function(plot, size=NULL, prefix=NULL, entity.geom='geom_point', data=NULL, ...){

    if (is.null(data)) data = plot$data;

    if (is.null(prefix)) prefix = rbbt.random_string();

    if (!endsWith(prefix, '.'))
        prefix = paste(prefix, ".", sep="")

    if (is.null(size)){
        print(plot, type='cairo');
    }else{
        base.size = 10 * (7/size)
        resolution = 72 * (size/7)

        if (length(plot$theme) == 0) plot <- plot + theme_light();

        if (length(plot$theme$text) == 0) plot <- plot + theme(text = element_text(size=base.size));

        if (is.null(plot$theme$text$size))
          plot$theme$text$size = base.size
        print(plot, type='cairo');
    }

    grid.force()

    if (!is.null(data[["Entity"]]))
        grid.garnish(entity.geom, 'data-entity'= data[["Entity"]], group = FALSE, grep = TRUE, redraw = TRUE)
    else
        grid.garnish(entity.geom, 'data-entity'= rownames(data), group = FALSE, grep = TRUE, redraw = TRUE)

    if (!is.null(data[["Entity type"]]))
        grid.garnish(entity.geom, 'data-entity_type' = data[["Entity type"]], group = FALSE, grep = TRUE, redraw = TRUE)
    else if (!is.null(attributes(data)$key.field))
        grid.garnish(entity.geom, 'data-entity_type' = rep(attributes(data)$key.field, length(rownames(data))), group = FALSE, grep = TRUE, redraw = TRUE)

    mysvg <- grid.export(prefix=prefix, strict = FALSE, ...)

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

