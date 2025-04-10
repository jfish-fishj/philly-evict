library(tidyverse)
library(data.table)
library(janitor)
library(sf)


theme_philly_evict <- function(){
  theme_bw() +
    theme(legend.position = 'right',
          legend.direction = "vertical",
          strip.text = element_text(colour = "black",size=25),
          strip.background =element_rect(fill="white"),
          plot.title = element_text(size=22, face = "bold"),
          plot.subtitle  = element_text(size=18, face = "bold"),
          axis.title.x=element_text(size=20, margin = margin(t = 20)),
          axis.title.y=element_text(size=20),
          legend.title = element_text(size=20, color = "blue"),
          plot.caption = element_text(hjust = 0)
    ) %>%
    return()
}
