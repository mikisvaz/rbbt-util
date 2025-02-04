rbbt.require('ggplot2')

#geom_entity <- function (real.geom = NULL, mapping = NULL, data = NULL, stat = "identity",
#                          position = "identity", ...) {
#    rg <- real.geom(mapping = mapping, data = data, stat = stat, 
#                    position = position, ...)
#
#    rg$geom <- ggproto(rg$geom, { 
#        draw <- function(., data, ...) {
#            grobs <- list()
#
#            for (i in 1:nrow(data)) {
#                grob <- .super$draw(., data[i,], ...) 
#                if (is.null(data$entity.type)) 
#                    grobs[[i]] <- garnishGrob(grob, `data-entity`=data[i,]$entity)
#                else
#                    grobs[[i]] <- garnishGrob(grob, `data-entity`=data[i,]$entity, `data-entity-type`=data[i,]$entity.type)
#            }
#
#            ggplot2:::ggname("geom_entity", gTree(children = do.call("gList", grobs)))
#        }
#
#        draw_groups <- function(., data, ...) {
#            grobs <- list()
#
#            for (i in 1:nrow(data)) {
#                grob <- .super$draw_groups(., data[i,], ...) 
#                if (is.null(data$entity.type)) 
#                    grobs[[i]] <- garnishGrob(grob, `data-entity`=data[i,]$entity)
#                else
#                    grobs[[i]] <- garnishGrob(grob, `data-entity`=data[i,]$entity, `data-entity-type`=data[i,]$entity.type)
#            }
#
#            ggplot2:::ggname("geom_entity", gTree(children = do.call("gList", grobs)))
#        }
#    })
#
#    rg
#}

rbbt.ggplot2.rotate_x_labels <- function(){ theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1)) }

rbbt.ggplot2.theme <- function(plot){

    plot + theme_classic() + scale_y_continuous(labels=scales::comma) + scale_x_continuous(labels=scales::comma) + rbbt.ggplot2.rotate_x_labels()

}
