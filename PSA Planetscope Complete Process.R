library(dplyr);library(lubridate)
library(terra);library(tidyverse);library(magrittr)
library(sf);library(beepr);library(httr)
library(exactextractr);library(stringr)
library(tidyterra);library(jsonlite)
library(ggplot2);library(ggtext)

##OBJECTIVE: ORDER, DOWNLOAD, AND PROCESS PLANETSCOPE EXTRACTION DATA
##FOR PSA ON-FARM FIELDS. 

##SETTING UP THE DATA--------------------------------------
fields = st_read(
  paste0(getwd(),'\\DATA\\Biomass_polygon1.geojson')) %>% 
  mutate(
    code.rep = paste0(code,'_',rep),
    # cover_planting = cover_planting %>% as.Date(),
    cover_planting = ifelse(code == 'LHR','2022-09-14',cover_planting %>% as.character()),
    cover_planting = cover_planting %>% as.Date(),
    cc_termination_date = ifelse(code == 'VSJ', '2021-05-02',cc_termination_date %>% as.character()),
    cc_termination_date = cc_termination_date %>% as.Date()
  )

fields.df <- fields %>%
  st_drop_geometry() %>% 
  mutate(
    year = year %>% as.integer(),
    cover_planting = cover_planting %>% as.Date(),
    # cover_planting = is.na(cover_planting),
    cover_planting = ifelse(is.na(cover_planting)==T,
                            paste0(year-1,'-09-01'),
                            cover_planting %>% as.character()),
    cover_planting = ifelse(code == 'LHR','2022-09-14',cover_planting),
    cover_planting = cover_planting %>% as.Date(),
    cc_termination_date = ifelse(is.na(cc_termination_date)==T,
                                 paste0(
                                   year,'-06-01'),
                                 cc_termination_date %>% as.character()),
    cc_termination_date = ifelse(code == 'VSJ', '2021-05-02',cc_termination_date),
    cc_termination_date = cc_termination_date %>% as.Date(),
    date.range.fields = abs(cc_termination_date - cover_planting)
  ) %>% 
  select(code,cover_planting,cc_termination_date,date.range.fields,
         treatment,type,year,cc_harvest_date,cc_species) %>% 
  distinct()

extent.files.main <- list.files(
  paste0(getwd(),'\\DATA'),
  pattern = '*.geojson$',full.names = T) %>% 
  as.data.frame() %>% rename('file'='.') %>% 
  mutate(Code = str_extract(file,'[A-Z]{3}.geojson'),
         Code = str_remove(Code,'.geojson')) %>% 
  na.omit()

##OPTION 1: IMAGERY ORDERS by single day----------------------------------------------
codes = fields.df %>% 
  dplyr::filter(order.id =='NA') %>%
  select(code) %>% as.vector() %>% unlist() %>% 
  unique()

df.1 = data.frame()
for (i in 1:length(codes)){
  
  print(paste0(
    "Processing ",
    i," of ",length(codes)
  ))
  
  
  aoi <- extent.files.main %>% 
    dplyr::filter(str_detect(Code,codes[i])) %>% 
    select(file) %>% as.character();aoi
  
  product.name <- paste0('Onfarm',str_extract(aoi,'_[A-Z]{3}'),'_');product.name
  
  extent <- fromJSON(aoi,
                     simplifyVector = F) %>%
    .$features %>%
    .[[1]] %>%
    .$geometry
  
  
  extent.filter <- list(type='GeometryFilter',
                        field_name='geometry',
                        config = extent) %>% 
    jsonlite::toJSON(auto_unbox = T)
  
  
  
  codes.sub = st_drop_geometry(fields.df) %>% distinct() %>% 
    dplyr::filter(code == codes[i]) %>% 
    rename('date.begin' = 'cover_planting',
           'date.end' = 'cc_termination_date')
  
  if(nrow(codes.sub) <1)next
  
  
  
  df.dates <- seq(from = min(codes.sub$date.begin), 
                  to = max(codes.sub$date.end), by = "day")  %>% 
    as.data.frame() %>% 
    rename('date' = '.')
  
  for(j in 1:length(df.dates$date)){
    
    print(paste0(
      "Processing ",
      j," of ",length(df.dates$date),
      ' ',code,", ",df.dates$date[j]
    ))
    
    date.begin = df.dates$date[j]
    date.end = df.dates$date[j]+1
    
    dates <- c(date.begin,
               date.end) %>%
      ymd() %>%
      as_datetime() %>%
      format_ISO8601(usetz = 'Z') %>%
      set_names(c('gte','lte')) %>%
      as.list()
    
    date.filter <- list(type='DateRangeFilter',field_name='acquired',
                        config = dates) %>% 
      jsonlite::toJSON(auto_unbox = T)
    
    ##acceptable cloud cover range from 0-60%; only "standard" images vs. "test"
    data.search.template <- '{
        "item_types":["PSScene"],
          "filter":{
            "type":"AndFilter",
            "config":[
                ${date.filter}$,
                ${extent.filter}$,
                {
                    "type":"RangeFilter",
                    "config":{
                       "gte":0,
                       "lte":0.6
                    },
                    "field_name":"cloud_cover"
                 },
        {"type": "StringInFilter",
                "field_name": "quality_category",
                "config": ["standard"]
              },
        {"type": "RangeFilter",
                "field_name": "clear_confidence_percent",
                "config": {"gte":80,"lte":100}
              }
            ]
          }
        }'  
    
    request <-  glue::glue(data.search.template,
                           .open= '${',
                           .close = '}$')
    
    
    search.results <-  POST(url='https://api.planet.com/data/v1/quick-search',
                            body = as.character(request),
                            authenticate(planet.api.key3,
                                         ''), 
                            content_type_json()
    );search.results$status_code
    
    
    search.results.c <- content(search.results); search.results.c[[2]] %>% length()
    
    results.length <- length(search.results.c[[2]])
    
    if(results.length <1)next
    
    ids = data.frame()
    for (o in 1:length(search.results.c$features)){
      
      assets = search.results.c$features[[o]]$assets
      
      if (any('ortho_analytic_8b_sr' ==  unlist(assets))){
        bundle.id = 'analytic_8b_sr_udm2'
      } else if (any('ortho_analytic_4b_sr' ==  unlist(assets))){
        bundle.id = 'analytic_sr_udm2'
      }else{next}
      
      pl.id = search.results.c$features[[o]]$id %>%
        as.data.frame() %>% rename('planet.id' = '.') %>%
        mutate(
          planet.id = paste0(
            str_extract(planet.id,'[0-9]{8}'),
            str_extract(planet.id,'_[0-9]{6}'),
            str_extract(planet.id,'_[:alnum:]{4}$')),
          bundle.id = bundle.id,
          date = str_extract(planet.id,'[0-9]{8}')
        )
      ids = rbind(ids,pl.id) 
    }
    
    if(nrow(ids)<1)next
    
    bundle.first = ids$bundle.id %>% unique() %>% sort()
    bundle.first = ifelse(length(bundle.first)=='2',bundle.first[1],
                          bundle.first)
    #https://docs.planet.com/develop/apis/orders/product_bundles/
    
    row.count = nrow(ids)
    
    row.pos <- c(which(is.na(ids$planet.id) == FALSE))
    
    products.list <- list(
      item_ids = as.list(ids$planet.id),  # Force array structure for single scene days
      item_type = 'PSScene',
      product_bundle = ids$bundle.id)
    
    products.json.1 <- toJSON(products.list, auto_unbox = TRUE, pretty = TRUE)
    
    # products.json.1 <- purrr::map_chr(search.results.c$features,
    #                                   'id') %>% ####CHECK THIS LINE!!!!!
    #   list(item_ids=.,item_type='PSScene',
    #        product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
    #   toJSON(auto_unbox = T,pretty = T)
    
    
    products.json.2 <- purrr::map_chr(search.results.c$features[row.pos],
                                      'id') %>% ####CHECK THIS LINE!!!!!
      list(item_ids=.,item_type='PSScene',
           product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
      toJSON(auto_unbox = T,pretty = T)
    
    products.json = ifelse(row.count <2,
                           products.json.1 %>% unlist(),
                           products.json.2)
    
    product.order.name <- paste0(
      product.name,
      dates$gte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '-',
      dates$lte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '_',
      bundle.first);product.order.name
    
    
    product.order.template <- '{
      "name":"${product.order.name}$",
      "source_type":"scenes",
      "products":[
        ${products.json}$
      ],
      "tools":[
        {
          "clip":{
            "aoi":${toJSON(extent,auto_unbox=T)}$
          }
        }
      ],
       "delivery":{
      "archive_type":"zip",
      "single_archive":true,
      "archive_filename":"${product.order.name}$.zip"
       }}'
    
    order.request <- glue::glue(
      product.order.template,
      .open ='${',
      .close = '}$'
    )
    
    # # ONLY EXECUTE order.pending ONCE!!!!!!!
    order.pending <- POST(url='https://api.planet.com/compute/ops/orders/v2',
                          body = as.character(order.request),
                          authenticate(planet.api.key3,
                                       ''),
                          content_type_json()
    );order.pending$status_code
    
    Sys.sleep(5)
    
    order.status = order.pending$status_code %>% as.character()
    
    order.id <-  ifelse(
      is.null(content(order.pending)[['id']]) == T,'NA',
      content(order.pending)[['id']])
    
    order.message = ifelse(order.id =='NA',content(order.pending)$field$Details %>%
                             unlist() %>% as.matrix() %>% as.data.frame() %>%
                             rename('Date' = 'V1') %>%
                             mutate(
                               message = Date,
                               Date = str_extract(Date,'[0-9]{8}')
                             ) %>%
                             distinct() %>%
                             dplyr::arrange(Date) %>%
                             mutate(Code = code),"Success")
    
    df.2 = data.frame(product.order.name,
                      code,
                      date.begin,
                      date.end,
                      order.id,
                      order.status,
                      results.length)
    
    df.1 = rbind(df.1,df.2)
    
    if(str_detect(order.status,'400')==F)next
    
    
    
    # order.message = content(order.pending)$field$Details %>% 
    #   unlist() %>% as.matrix() %>% as.data.frame() %>% 
    #   rename('Date' = 'V1') %>% 
    #   mutate(
    #     Scene = str_extract(Date,'[0-9]{8}_[:graph:]{6}_[:graph:]{4}'),
    #     Date = str_extract(Date,'[0-9]{8}'),
    #   ) %>% 
    #   distinct() %>%
    #   dplyr::arrange(Date) %>% 
    #   mutate(Code = code)
    
    if(length(order.message)<1)next
    
    ids.2 = ids %>% 
      mutate(
        invalid = date %in% order.message$Date
      )
    
    if(nrow(ids.2)<1)next
    
    bundle.first = ids.2$bundle.id %>% unique() %>% sort()
    bundle.first = ifelse(length(bundle.first)=='2',bundle.first[1],
                          bundle.first)
    
    row.count = nrow(ids.2 %>% dplyr::filter(invalid =='FALSE'))
    
    row.pos <- c(which(ids.2$invalid == 'FALSE'))
    
    products.json.2 <- purrr::map_chr(search.results.c$features[row.pos],
                                      'id') %>% ####CHECK THIS LINE!!!!!
      list(item_ids=.,item_type='PSScene',
           product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
      toJSON(auto_unbox = T,pretty = T)
    
    products.json = ifelse(row.count <2,products.json.1,products.json.2)
    
    product.order.name <- paste0(
      product.name,
      dates$gte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '-',
      dates$lte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '_',
      bundle.first);product.order.name
    
    
    product.order.template <- '{
      "name":"${product.order.name}$",
      "source_type":"scenes",
      "products":[
        ${products.json}$
      ],
      "tools":[
        {
          "clip":{
            "aoi":${toJSON(extent,auto_unbox=T)}$
          }
        }
      ],
       "delivery":{
      "archive_type":"zip",
      "single_archive":true,
      "archive_filename":"${product.order.name}$.zip"
       }}'
    
    order.request <- glue::glue(
      product.order.template,
      .open ='${',
      .close = '}$'
    )
    
    # # ONLY EXECUTE order.pending ONCE!!!!!!!
    order.pending <- POST(url='https://api.planet.com/compute/ops/orders/v2',
                          body = as.character(order.request),
                          authenticate(planet.api.key3,
                                       ''),
                          content_type_json()
    );order.pending$status_code
    
    
    Sys.sleep(5)
    
    df.3 = data.frame(product.order.name,
                      code,
                      date.begin,
                      date.end,
                      order.id,
                      order.status,
                      results.length
    )
    
    df.1 = rbind(df.1,df.3)
    
    Sys.sleep(.1)
  }
  
}

##OPTION 2: IMAGERY ORDERS by entire date range----------------------------------------------
codes = fields.df %>% 
  # dplyr::filter(order.id =='NA') %>%
  select(code) %>% as.vector() %>% unlist() %>% 
  unique()

df.1 = data.frame()
# codes = codes[c(7,51,54,65,114,138,165,183,184,185)]
for (i in 1:length(codes)){
  
  print(paste0(
    "Processing ",
    i," of ",length(codes)
  ))
  
  code = codes[i];code
  
  aoi <- extent.files.main %>% 
    dplyr::filter(str_detect(Code,codes[i])) %>% 
    select(file) %>% as.character();aoi
  
  product.name <- paste0('Onfarm',str_extract(aoi,'_[A-Z]{3}'),'_');product.name
  
  extent <- fromJSON(aoi,
                     simplifyVector = F) %>%
    .$features %>%
    .[[1]] %>%
    .$geometry
  
  
  extent.filter <- list(type='GeometryFilter',
                        field_name='geometry',
                        config = extent) %>% 
    jsonlite::toJSON(auto_unbox = T)
  
  
  
  codes.sub = st_drop_geometry(fields.df) %>% distinct() %>% 
    dplyr::filter(code == codes[i]) %>% 
    rename('date.begin' = 'cover_planting',
           'date.end' = 'cc_termination_date')
  
  if(nrow(codes.sub) <1)next
  
  
  dates <- c(codes.sub$date.begin,
             codes.sub$date.end) %>%
    ymd() %>%
    as_datetime() %>%
    format_ISO8601(usetz = 'Z') %>%
    set_names(c('gte','lte')) %>%
    as.list()
  
  date.filter <- list(type='DateRangeFilter',field_name='acquired',
                      config = dates) %>% 
    jsonlite::toJSON(auto_unbox = T)
  
  ##acceptable cloud cover range from 0-60%; only "standard" images vs. "test"
  data.search.template <- '{
        "item_types":["PSScene"],
          "filter":{
            "type":"AndFilter",
            "config":[
                ${date.filter}$,
                ${extent.filter}$,
                {
                    "type":"RangeFilter",
                    "config":{
                       "gte":0,
                       "lte":0.6
                    },
                    "field_name":"cloud_cover"
                 },
        {"type": "StringInFilter",
                "field_name": "quality_category",
                "config": ["standard"]
              },
        {"type": "RangeFilter",
                "field_name": "clear_confidence_percent",
                "config": {"gte":80,"lte":100}
              }
            ]
          }
        }'  
  
  request <-  glue::glue(data.search.template,
                         .open= '${',
                         .close = '}$')
  
  
  search.results <-  POST(url='https://api.planet.com/data/v1/quick-search',
                          body = as.character(request),
                          authenticate(planet.api.key3,
                                       ''), 
                          content_type_json()
  );search.results$status_code
  
  
  search.results.c <- content(search.results); search.results.c[[2]] %>% length()
  
  results.length <- length(search.results.c[[2]])
  
  if(results.length <1)next
  
  ids = data.frame()
  for (o in 1:length(search.results.c$features)){
    
    assets = search.results.c$features[[o]]$assets
    
    if (any('ortho_analytic_8b_sr' ==  unlist(assets))){
      bundle.id = 'analytic_8b_sr_udm2'
    } else if (any('ortho_analytic_4b_sr' ==  unlist(assets))){
      bundle.id = 'analytic_sr_udm2'
    }else{next}
    
    pl.id = search.results.c$features[[o]]$id %>%
      as.data.frame() %>% rename('planet.id' = '.') %>%
      mutate(
        row.id = o,
        planet.id = paste0(
          str_extract(planet.id,'[0-9]{8}'),
          str_extract(planet.id,'_[0-9]{6}'),
          str_extract(planet.id,'_[:alnum:]{4}$')),
        bundle.id = bundle.id,
        date = str_extract(planet.id,'[0-9]{8}')
      )
    ids = rbind(ids,pl.id) 
  }
  
  if(nrow(ids)<1)next
  
  bundle.first = ids$bundle.id %>% unique() %>% sort()
  bundle.first = ifelse(length(bundle.first)=='2',bundle.first[1],
                        bundle.first)
  #https://docs.planet.com/develop/apis/orders/product_bundles/
  
  row.count = nrow(ids)
  
  row.pos <- c(which(is.na(ids$planet.id) == FALSE))
  
  products.list <- list(
    item_ids = as.list(ids$planet.id),  # Force array structure for single scene days
    item_type = 'PSScene',
    product_bundle = ids$bundle.id)
  
  products.json.1 <- toJSON(products.list, auto_unbox = TRUE, pretty = TRUE)
  
  # products.json.1 <- purrr::map_chr(search.results.c$features,
  #                                   'id') %>% ####CHECK THIS LINE!!!!!
  #   list(item_ids=.,item_type='PSScene',
  #        product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
  #   toJSON(auto_unbox = T,pretty = T)
  
  
  products.json.2 <- purrr::map_chr(search.results.c$features[row.pos],
                                    'id') %>% ####CHECK THIS LINE!!!!!
    list(item_ids=.,item_type='PSScene',
         product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
    toJSON(auto_unbox = T,pretty = T)
  
  products.json = ifelse(row.count <2,
                         products.json.1 %>% unlist(),
                         products.json.2)
  
  product.order.name <- paste0(
    product.name,
    dates$gte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
    '-',
    dates$lte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
    '_',
    bundle.first);product.order.name
  
  
  product.order.template <- '{
      "name":"${product.order.name}$",
      "source_type":"scenes",
      "products":[
        ${products.json}$
      ],
      "tools":[
        {
          "clip":{
            "aoi":${toJSON(extent,auto_unbox=T)}$
          }
        }
      ],
       "delivery":{
      "archive_type":"zip",
      "single_archive":true,
      "archive_filename":"${product.order.name}$.zip"
       }}'
  
  order.request <- glue::glue(
    product.order.template,
    .open ='${',
    .close = '}$'
  )
  
  # # ONLY EXECUTE order.pending ONCE!!!!!!!
  order.pending <- POST(url='https://api.planet.com/compute/ops/orders/v2',
                        body = as.character(order.request),
                        authenticate(planet.api.key3,
                                     ''),
                        content_type_json()
  );order.pending$status_code
  
  Sys.sleep(5)
  
  order.status = order.pending$status_code %>% as.character()
  
  order.id <-  ifelse(
    is.null(content(order.pending)[['id']]) == T,'NA',
    content(order.pending)[['id']])
  
  order.message = ifelse(order.id =='NA',content(order.pending)$field$Details %>%
                           unlist() %>% as.matrix() %>% as.data.frame() %>%
                           rename('Date' = 'V1') %>%
                           mutate(
                             message = Date,
                             Date = str_extract(Date,'[0-9]{8}')
                           ) %>%
                           distinct() %>%
                           dplyr::arrange(Date) %>%
                           mutate(Code = code),"Success")
  
  df.2 = data.frame(product.order.name,
                    code,
                    date.begin,
                    date.end,
                    order.id,
                    order.status,
                    results.length)
  
  df.1 = rbind(df.1,df.2)
  
  if(str_detect(order.status,'400')==F)next
  
  
  
  # order.message = content(order.pending)$field$Details %>% 
  #   unlist() %>% as.matrix() %>% as.data.frame() %>% 
  #   rename('Date' = 'V1') %>% 
  #   mutate(
  #     Scene = str_extract(Date,'[0-9]{8}_[:graph:]{6}_[:graph:]{4}'),
  #     Date = str_extract(Date,'[0-9]{8}'),
  #   ) %>% 
  #   distinct() %>%
  #   dplyr::arrange(Date) %>% 
  #   mutate(Code = code)
  
  if(length(order.message)<1)next
  
  ids.2 = ids %>% 
    mutate(
      invalid = date %in% order.message$Date
    )
  
  if(nrow(ids.2)<1)next
  
  bundle.first = ids.2$bundle.id %>% unique() %>% sort()
  bundle.first = ifelse(length(bundle.first)=='2',bundle.first[1],
                        bundle.first)
  
  row.count = nrow(ids.2 %>% dplyr::filter(invalid =='FALSE'))
  
  row.pos <- ids$row.id
  
  # row.pos <- c(which(ids.2$invalid == 'FALSE'))
  
  products.json.2 <- purrr::map_chr(search.results.c$features[row.pos],
                                    'id') %>% ####CHECK THIS LINE!!!!!
    list(item_ids=.,item_type='PSScene',
         product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
    toJSON(auto_unbox = T,pretty = T)
  
  products.json = ifelse(row.count <2,products.json.1,products.json.2)
  
  product.order.name <- paste0(
    product.name,
    dates$gte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
    '-',
    dates$lte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
    '_',
    bundle.first);product.order.name
  
  
  product.order.template <- '{
      "name":"${product.order.name}$",
      "source_type":"scenes",
      "products":[
        ${products.json}$
      ],
      "tools":[
        {
          "clip":{
            "aoi":${toJSON(extent,auto_unbox=T)}$
          }
        }
      ],
       "delivery":{
      "archive_type":"zip",
      "single_archive":true,
      "archive_filename":"${product.order.name}$.zip"
       }}'
  
  order.request <- glue::glue(
    product.order.template,
    .open ='${',
    .close = '}$'
  )
  
  # # ONLY EXECUTE order.pending ONCE!!!!!!!
  order.pending <- POST(url='https://api.planet.com/compute/ops/orders/v2',
                        body = as.character(order.request),
                        authenticate(planet.api.key3,
                                     ''),
                        content_type_json()
  );order.pending$status_code
  
  
  Sys.sleep(5)
  
  df.3 = data.frame(product.order.name,
                    code,
                    date.begin,
                    date.end,
                    order.id,
                    order.status,
                    results.length
  )
  
  df.1 = rbind(df.1,df.3)
  
  Sys.sleep(.1)
  
  
}


##READ IMAGERY METADATA------------------------------

#list all metadata files with the path
pl.m <- list.files(path= 'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\IMAGERY\\ALL IMAGERY\\IMAGERY', #name path where all files reside
                   recursive = T, full.names = T,
                   pattern = '*metadata.json$')

# pl.m = pl.m[1178:24611]
# pl.m = pl.m[1181:23434]
# pl.m = pl.m[871:22254]
# pl.m = pl.m[3732:21384]
# pl.m = pl.m[3677:17653]
# pl.m = pl.m[9088:13977]

#Loop through list of metadata files to create a dataframe
meta.df <- data.frame()
for (m in 1:length(pl.m)){
  print(paste("Loop m at:", m))
  
  if(file.size(pl.m[m])<1000)next
  
  file = fromJSON(pl.m[m])
  
  code = str_extract(pl.m[m],'Onfarm_[A-Z]{3}') %>% str_remove('Onfarm_')
  
  id = file$id #the scene id
  
  instrument = file$properties$instrument
  
  Date = file$properties$acquired
  
  satellite_id = file$properties$satellite_id
  
  strip_id = file$properties$strip_id
  
  quality_category = (file$properties$quality_category %>% as.character())
  
  clear_confidence_percent = (file$properties$clear_confidence_percent %>% as.numeric())
  
  clear_percent = (file$properties$clear_confidence_percent %>% as.numeric())
  
  sun_elevation = (file$properties$sun_elevation %>% as.numeric())
  
  sun_azimuth = (file$properties$sun_azimuth %>% as.numeric())
  
  view_angle = (file$properties$view_angle %>% as.numeric()) #aka off-nadir angle
  
  # Print the length of each variable
  print(paste("Length of code:", length(code)))
  print(paste("Length of id:", length(id)))
  print(paste("Length of instrument:", length(instrument)))
  print(paste("Length of Date:", length(Date)))
  print(paste("Length of satellite_id:", length(satellite_id)))
  print(paste("Length of strip_id:", length(strip_id)))
  print(paste("Length of quality_category:", length(quality_category)))
  print(paste("Length of clear_confidence_percent:", length(clear_confidence_percent)))
  print(paste("Length of clear_percent:", length(clear_percent)))
  print(paste("Length of sun_elevation:", length(sun_elevation)))
  print(paste("Length of sun_azimuth:", length(sun_azimuth)))
  print(paste("Length of view_angle:", length(view_angle)))
  
  df <- data.frame(code,
                   id,
                   instrument, 
                   Date, 
                   satellite_id, 
                   strip_id, 
                   quality_category,
                   clear_confidence_percent, 
                   clear_percent, 
                   sun_elevation, 
                   sun_azimuth, 
                   view_angle)
  
  meta.df <- rbind(meta.df,df)
}

meta.df.fin <- meta.df %>% 
  mutate(
    Date = str_extract(Date,'[0-9]{4}-[0-9]{2}-[0-9]{2}') %>% 
      as.Date()
  )
##CHECK
##Onfarm_CWZ/20220918_152304_38_2475_metadata.json
##Onfarm_EJU/20230211_144800_78_241e_metadata.json
##Onfarm_HAP/20230309_154420_96_2479_metadata.json
##Onfarm_PHZ/20230506_144142_07_24a7_metadata.json
##Onfarm_XFS/20220301_151308_80_2429_metadata.json
##Onfarm_PHZ/20230506_144142_07_24a7_metadata.json
##Onfarm_BRQ/20221123_161506_09_247f_3B_udm2_clip.tif

##SUMMARIZE DOWNLOADED DATA--------------------------------------
pl.files.summary <- list.files(
  'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\IMAGERY\\ALL IMAGERY\\IMAGERY',
  full.names = T,
  recursive = T,
  pattern = '*MS_SR[_8b]{0,3}_clip.tif$') %>%
  as.data.frame() %>%
  rename('file' = '.') %>%
  mutate(
    code = str_extract(file,'Onfarm_[A-Z]{3}'),
    code = str_remove(code,'Onfarm_'),
    date = str_extract(file,'[0-9]{8}_[0-9]{6}'),
    date = str_remove(date,'_[0-9]{6}'),
    date = paste0(
      substr(date,1,4),'-',substr(date,5,6),'-',substr(date,7,8)),
    date = as.Date(date),
    bands = ifelse(str_detect(file,'AnalyticMS_SR_8b'),'8','4')
  ) %>% 
  group_by(code) %>%
  dplyr::summarise(
    count = length(unique(date)),
    date.min = min(date),
    date.max = max(date),
    product.count = bands %>% table() %>% 
      as.data.frame() %>% select(Freq) %>% nrow()
  ) %>% 
  mutate(
    date.range = (date.max - date.min) %>% as.integer()
  ) %>% 
  left_join(fields.df,by='code') %>% 
  mutate(
    date.range.diff = abs(date.range-date.range.fields), ##compares imagery date range v. agronomic date range
    planting.min.diff = (cover_planting - date.min), ##gap between planting and available imagery
    term.max.diff = (cc_termination_date - date.max), ##gap between termination and available imagery
    # Flagged = ifelse(
    #   (pl.files.summary$date.range.fields / pl.files.summary$count)>3,'1','0'
    # ),
    Flagged = ifelse(
      term.max.diff > -10,'1','0'
    )
  )

pl.files.summary.flagged <- pl.files.summary %>% 
  dplyr::filter(Flagged =='1') %>% 
  select(code,count,date.min,cover_planting,
         date.max,cover_planting,cc_termination_date) %>% 
  mutate(
    date.begin.diff = abs(date.min - cover_planting),
    date.end.diff = abs(date.max - cc_termination_date),
    date.begin = (ifelse(date.begin.diff>7,(date.min-8) %>% 
                           as.character(),date.min %>% 
                           as.character())) %>% as.Date(),
    date.end = (ifelse(date.end.diff>7,(date.max+8) %>% 
                         as.character(),date.max %>% 
                         as.character())) %>% as.Date()
  )


##FOLLOW UP ORDER FOR PLANTING/TERMINATION DATES----------------------

orders.4 <- list.files(
  'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\IMAGERY\\ALL IMAGERY\\IMAGERY',
  full.names = T,
  recursive = T,
  pattern = '*MS_SR[_8b]{0,3}_clip.tif$') %>%
  as.data.frame() %>%
  rename('file' = '.') %>% 
  mutate(
    code = str_extract(file,'Onfarm_[A-Z]{3}'),
    code = str_remove(code,'Onfarm_'),
    date = str_extract(file,'[0-9]{8}_[0-9]{6}'),
    date = str_remove(date,'_[0-9]{8}'),
    date = paste0(
      substr(date,1,4),'-',substr(date,5,6),'-',substr(date,7,8)),
    date = as.Date(date)) %>%
  dplyr::filter(code %in% pl.files.summary.flagged$code)

codes <- pl.files.summary.flagged$code
# codes = codes[c(2:27)]
df.1 = data.frame()
for (i in 1:length(codes)){
  
  print(paste0(
    "Processing ",
    i," of ",length(codes)
  ))
  
  
  aoi <- extent.files.main %>% 
    dplyr::filter(str_detect(Code,codes[i])) %>% 
    select(file) %>% as.character();aoi
  
  product.name <- paste0('Onfarm',str_extract(aoi,'_[A-Z]{3}'),'_');product.name
  
  extent <- fromJSON(aoi,
                     simplifyVector = F) %>%
    .$features %>%
    .[[1]] %>%
    .$geometry
  
  
  extent.filter <- list(type='GeometryFilter',
                        field_name='geometry',
                        config = extent) %>% 
    jsonlite::toJSON(auto_unbox = T)
  
  
  
  codes.sub = pl.files.summary.flagged %>% 
    dplyr::filter(code == codes[i])
  
  if(nrow(codes.sub) <1)next
  
  dates.omit <- orders.4 %>% 
    dplyr::filter(code == codes[i]) %>% 
    select(date) %>% 
    unique()  %>% unlist() %>% as.vector() %>% as.Date()
  
  ##CHANGE TO INCLUDE ALL DATES OR ONLY POST TERMINATION
  # df.dates <- seq(from = min(codes.sub$date.begin), 
  #                 to = max(codes.sub$date.end), by = "day")  %>% 
  #   as.data.frame() %>% 
  #   rename('date' = '.') %>% 
  #   dplyr::filter(!(date %in% dates.omit)) ##CHANGE TO INCLUDE ALL DATES
  
  df.dates <- seq(from = min(codes.sub$date.begin), 
                  to = max(codes.sub$cc_termination_date)+10, by = "day")  %>% 
    as.data.frame() %>% 
    rename('date' = '.') %>% 
    dplyr::filter(!(date %in% dates.omit) & 
                    date > codes.sub$cc_termination_date) ##CHANGE TO INCLUDE ALL DATES
  
  for(j in 1:length(df.dates$date)){
    
    print(paste0(
      "Processing ",
      j," of ",length(df.dates$date),
      ' ',codes[i],", ",df.dates$date[j]
    ))
    
    date.begin = df.dates$date[j]
    date.end = df.dates$date[j]+1
    
    dates <- c(date.begin,
               date.end) %>%
      ymd() %>%
      as_datetime() %>%
      format_ISO8601(usetz = 'Z') %>%
      set_names(c('gte','lte')) %>%
      as.list()
    
    date.filter <- list(type='DateRangeFilter',field_name='acquired',
                        config = dates) %>% 
      jsonlite::toJSON(auto_unbox = T)
    
    ##acceptable cloud cover range from 0-60%; only "standard" images vs. "test"
    data.search.template <- '{
        "item_types":["PSScene"],
          "filter":{
            "type":"AndFilter",
            "config":[
                ${date.filter}$,
                ${extent.filter}$,
                {
                    "type":"RangeFilter",
                    "config":{
                       "gte":0,
                       "lte":0.6
                    },
                    "field_name":"cloud_cover"
                 },
        {"type": "StringInFilter",
                "field_name": "quality_category",
                "config": ["standard"]
              },
        {"type": "RangeFilter",
                "field_name": "clear_confidence_percent",
                "config": {"gte":80,"lte":100}
              }
            ]
          }
        }'  
    
    request <-  glue::glue(data.search.template,
                           .open= '${',
                           .close = '}$')
    
    
    search.results <-  POST(url='https://api.planet.com/data/v1/quick-search',
                            body = as.character(request),
                            authenticate(planet.api.key3,
                                         ''), 
                            content_type_json()
    );search.results$status_code
    
    
    search.results.c <- content(search.results); search.results.c[[2]] %>% length()
    
    results.length <- length(search.results.c[[2]])
    
    if(results.length <1)next
    
    ids = data.frame()
    for (o in 1:length(search.results.c$features)){
      
      assets = search.results.c$features[[o]]$assets
      
      if (any('ortho_analytic_8b_sr' ==  unlist(assets))){
        bundle.id = 'analytic_8b_sr_udm2'
      } else if (any('ortho_analytic_4b_sr' ==  unlist(assets))){
        bundle.id = 'analytic_sr_udm2'
      }else{next}
      
      pl.id = search.results.c$features[[o]]$id %>%
        as.data.frame() %>% rename('planet.id' = '.') %>%
        mutate(
          row.id = o,
          planet.id = paste0(
            str_extract(planet.id,'[0-9]{8}'),
            str_extract(planet.id,'_[0-9]{6}'),
            str_extract(planet.id,'_[:alnum:]{4}$')),
          bundle.id = bundle.id,
          date = str_extract(planet.id,'[0-9]{8}')
        )
      ids = rbind(ids,pl.id) 
    }
    
    if(nrow(ids)<1)next
    
    bundle.first = ids$bundle.id %>% unique() %>% sort()
    bundle.first = ifelse(length(bundle.first)=='2',bundle.first[1],
                          bundle.first)
    #https://docs.planet.com/develop/apis/orders/product_bundles/
    
    row.count = nrow(ids)
    
    row.pos <- c(which(is.na(ids$planet.id) == FALSE))
    
    products.list <- list(
      item_ids = as.list(ids$planet.id),  # Force array structure for single scene days
      item_type = 'PSScene',
      product_bundle = ids$bundle.id)
    
    products.json.1 <- toJSON(products.list, auto_unbox = TRUE, pretty = TRUE)
    
    # products.json.1 <- purrr::map_chr(search.results.c$features,
    #                                   'id') %>% ####CHECK THIS LINE!!!!!
    #   list(item_ids=.,item_type='PSScene',
    #        product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
    #   toJSON(auto_unbox = T,pretty = T)
    
    
    products.json.2 <- purrr::map_chr(search.results.c$features[row.pos],
                                      'id') %>% ####CHECK THIS LINE!!!!!
      list(item_ids=.,item_type='PSScene',
           product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
      toJSON(auto_unbox = T,pretty = T)
    
    products.json = ifelse(row.count <2,
                           products.json.1 %>% unlist(),
                           products.json.2)
     
    order.date = today() %>% str_remove_all('-')
    
    product.order.name <- paste0(
      product.name,
      dates$gte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '-',
      dates$lte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '_',
      bundle.first,
      '_ordered_',order.date);product.order.name
    
    
    product.order.template <- '{
      "name":"${product.order.name}$",
      "source_type":"scenes",
      "products":[
        ${products.json}$
      ],
      "tools":[
        {
          "clip":{
            "aoi":${toJSON(extent,auto_unbox=T)}$
          }
        }
      ],
       "delivery":{
      "archive_type":"zip",
      "single_archive":true,
      "archive_filename":"${product.order.name}$.zip"
       }}'
    
    order.request <- glue::glue(
      product.order.template,
      .open ='${',
      .close = '}$'
    )
    
    ## ONLY EXECUTE order.pending ONCE!!!!!!!
    order.pending <- POST(url='https://api.planet.com/compute/ops/orders/v2',
                          body = as.character(order.request),
                          authenticate(planet.api.key3,
                                       ''),
                          content_type_json()
    );order.pending$status_code
    
    Sys.sleep(5)
    
    order.status = order.pending$status_code %>% as.character()
    
    order.id <-  ifelse(
      is.null(content(order.pending)[['id']]) == T,'NA',
      content(order.pending)[['id']])
    
    order.message = ifelse(order.id =='NA',content(order.pending)$field$Details %>%
                             unlist() %>% as.matrix() %>% as.data.frame() %>%
                             rename('Date' = 'V1') %>%
                             mutate(
                               message = Date,
                               Date = str_extract(Date,'[0-9]{8}')
                             ) %>%
                             distinct() %>%
                             dplyr::arrange(Date) %>%
                             mutate(Code = code),"Success")
    
    df.2 = data.frame(product.order.name,
                      codes[i],
                      date.begin,
                      date.end,
                      order.id,
                      order.status,
                      results.length)
    
    df.1 = rbind(df.1,df.2)
    
    if(str_detect(order.status,'400')==F)next
    
    
    
    # order.message = content(order.pending)$field$Details %>% 
    #   unlist() %>% as.matrix() %>% as.data.frame() %>% 
    #   rename('Date' = 'V1') %>% 
    #   mutate(
    #     Scene = str_extract(Date,'[0-9]{8}_[:graph:]{6}_[:graph:]{4}'),
    #     Date = str_extract(Date,'[0-9]{8}'),
    #   ) %>% 
    #   distinct() %>%
    #   dplyr::arrange(Date) %>% 
    #   mutate(Code = code)
    
    if(length(order.message)<1)next
    
    ids.2 = ids %>% 
      mutate(
        invalid = date %in% order.message$Date
      )
    
    if(nrow(ids.2)<1)next
    
    bundle.first = ids.2$bundle.id %>% unique() %>% sort()
    bundle.first = ifelse(length(bundle.first)=='2',bundle.first[1],
                          bundle.first)
    
    row.count = nrow(ids.2 %>% dplyr::filter(invalid =='FALSE'))
    
    row.pos <- ids$row.id
    
    products.json.2 <- purrr::map_chr(search.results.c$features[row.pos],
                                      'id') %>% ####CHECK THIS LINE!!!!!
      list(item_ids=.,item_type='PSScene',
           product_bundle =   "analytic_8b_sr_udm2,analytic_sr_udm2") %>% #surface reflectance, 8 band. For 4-band or other products, visit https://developers.planet.com/apis/orders/product-bundles-reference/
      toJSON(auto_unbox = T,pretty = T)
    
    products.json = ifelse(row.count <2,products.json.1,products.json.2)
    
    product.order.name <- paste0(
      product.name,
      dates$gte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '-',
      dates$lte %>% as.character() %>% str_remove('T00:00:00Z') %>% str_remove_all('-'),
      '_',
      bundle.first);product.order.name
    
    
    product.order.template <- '{
      "name":"${product.order.name}$",
      "source_type":"scenes",
      "products":[
        ${products.json}$
      ],
      "tools":[
        {
          "clip":{
            "aoi":${toJSON(extent,auto_unbox=T)}$
          }
        }
      ],
       "delivery":{
      "archive_type":"zip",
      "single_archive":true,
      "archive_filename":"${product.order.name}$.zip"
       }}'
    
    order.request <- glue::glue(
      product.order.template,
      .open ='${',
      .close = '}$'
    )
    
    # # ONLY EXECUTE order.pending ONCE!!!!!!!
    order.pending <- POST(url='https://api.planet.com/compute/ops/orders/v2',
                          body = as.character(order.request),
                          authenticate(planet.api.key3,
                                       ''),
                          content_type_json()
    );order.pending$status_code
    
    
    Sys.sleep(5)
    
    df.3 = data.frame(product.order.name,
                      codes[i],
                      date.begin,
                      date.end,
                      order.id,
                      order.status,
                      results.length
    )
    
    df.1 = rbind(df.1,df.3)
    
    Sys.sleep(.1)
  }
  
}




##READ IN ORDERS FOR DOWNLOADS 4------------------------

orders.4.files <- list.files(path = 'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\ORDERS',
                             recursive = F,full.names = T,
                             pattern = '*.csv$')

orders.4.initial = data.frame()
for(i in 1:length(orders.4.files)){
  
  orders.4.filter <- orders.4.initial %>%
    dplyr::filter(nchar(order.id) == '36') %>%
    mutate(
      date.begin = date.begin %>% as.Date(),
      date.end = date.end %>% as.Date()
    ) %>% 
    dplyr::filter(!(date.begin %in% orders.4$date.begin))
  
  codes.orders.4 <- orders.4$code %>% unique() %>% sort()
  df.1 <- data.frame()
  for(i in 1:length(codes.orders.4)){
    
    code = codes.orders.4[i]
    
    codes.sub = pl.files.final.flagged %>% 
      dplyr::filter(code == codes.orders.4[i])
    
    dates.omit <- orders.4$date %>% unique()
    
    df.dates <- seq(from = min(codes.sub$date.begin), 
                    to = max(codes.sub$date.end), by = "day")  %>% 
      as.data.frame() %>% 
      rename('date' = '.') %>% 
      dplyr::filter(!(date %in% dates.omit))
    
    for(j in 1:nrow(df.dates)){
      
      date.begin = df.dates$date[j] %>% str_remove_all('-')
      date.end = (df.dates$date[j]+1) %>% str_remove_all('-')
      
      
      product.order.name = paste0('Onfarm_',codes.orders.4[i],'_',
                                  date.begin,'-',date.end)
      
      df.2 = c(code,product.order.name)
      df.1 = rbind(df.1,df.2)
      
      colnames(df.1)[2] <- 'product.name'
    }
  }
}

##DOWNLOAD ORDERS-----------------------------------------

library(tidyverse)
library(glue)
library(curl)
library(purrr)
library(progress);library(httr)
library(jsonlite);source('secrets.R') 


auth = authenticate(planet.api.key3, "")

download_root = "D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\DOWNLOADS 6"

download.code.list <-  df.1 %>% rename('codes' = 'codes.i.')

get_all_orders = function(download.code.list, prefix_column = "product.name") {
  # Extract prefix value from the data frame
  # prefix_value = download.code.list[['code']][1]  # Adjust index if needed
  prefix_value = 'Onfarm_'  # Adjust index if needed
  
  orders = list()
  next_url = "https://api.planet.com/compute/ops/orders/v2?page_size=100"
  while (!is.null(next_url)) {
    resp = GET(next_url, auth)
    stop_for_status(resp)
    content_data = content(resp, as = "parsed", type = "application/json")
    page_orders = content_data$orders
    
    # Filter orders by dynamic prefix
    filtered_orders = Filter(function(order) startsWith(order$name, prefix_value), page_orders)
    
    clean_page = map_dfr(filtered_orders, function(order) {
      tibble(
        id = order$id,
        name = order$name,
        state = order$state,
        created = order$created,
        updated = order$last_modified,
        items_count = length(order$products),
        url = order$`_links`$self
      )
    })
    
    orders = bind_rows(orders, clean_page)
    message(glue("Fetched {nrow(clean_page)} filtered orders from page. Total so far: {nrow(orders)}"))
    next_url = content_data$`_links`$`next`
  }
  return(orders)
}


#call order names
all_orders = get_all_orders(download.code.list) %>% 
  mutate(
    name.short = str_extract(name,
                             'Onfarm_[A-Z]{3}_[0-9]{8}-[0-9]{8}')
  ) %>% 
  dplyr::filter(str_detect(name,'20251103'))##ordered on this date


setwd(download_root)

for(i in 1:length(all_orders$id)){
  # pb$tick()
  print(paste0("Processing order: ",i))
  order_url = paste0("https://api.planet.com/compute/ops/orders/v2/", all_orders$id[i])
  resp = GET(order_url, authenticate(planet.api.key3, "", type = "basic"))
  stop_for_status(resp)
  order_json = content(resp, "text", encoding = "UTF-8")
  order_data = fromJSON(order_json, flatten = TRUE)
  id = all_orders[c(1:2),] %>%
    dplyr::filter(id == all_orders$id[i]) %>%
    pull(name)
  #set name of folders to order name place in org script
  fn = paste0(all_orders$name[i], ".zip")
  # Check results URLs
  file_urls = order_data$`_links`$results$location
  print(file_urls)
  file_url = file_urls[1]
  #Download quietly without printing http information (not working) 
  #TODO: fix for quiet download
  suppressMessages(
    suppressWarnings(
      curl::curl_download(
        url = file_url,
        destfile = fn,
        handle = curl::new_handle(httpauth = 1, userpwd = paste0(planet.api.key3, ":"))
      )
    )
  )
}


##UNZIP FOLDERS----------------------------------------

# Set your target directory
zip_dir <- 'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\DOWNLOADS 6'

# List all .zip files in the directory
zip_files <- list.files(zip_dir, pattern = "\\.zip$", full.names = TRUE,
                        recursive = F) %>% 
  as.data.frame() %>% rename('file' = '.') 

# Create an output directory for unzipped content
output_dir <- file.path(zip_dir, "unzipped")
dir.create(output_dir, showWarnings = FALSE)

# Loop through and unzip each file
for (zip_file in zip_files$file) {
  unzip(zip_file, exdir = file.path(output_dir, tools::file_path_sans_ext(basename(zip_file))))
}



##EXTRACTION SETUP: PLANET DATA BY FARM CODE AND REP---------------------------

library(dplyr)
library(terra)
library(sf)
library(exactextractr);library(stringr)

##identify which imagery files to read in

pl.files <- list.files(
  path= 'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\IMAGERY\\ALL IMAGERY' ,
  recursive = T, full.names = T,
  pattern = '*MS_SR[_8b]{0,3}_clip.tif$') %>% 
  as.data.frame() %>% 
  rename('file' = '.') %>% 
  mutate(
    code = str_extract(file,'Onfarm_[A-Z]{3}'),
    code = str_remove(code,'Onfarm_'),
    date = str_extract(file,'[0-9]{8}_[0-9]{6}'),
    date = str_remove(date,'_[0-9]{6}'),
    Date = paste0(str_sub(date,1,4),'-',
                  str_sub(date,5,6),'-',
                  str_sub(date,7,8)) %>% as.Date(),
    bands = ifelse(str_detect(file,'AnalyticMS_SR_8b'),'8','4'),
    season = ifelse(Date < '2019-06-10','2019',NA),
    season = ifelse(Date >= '2019-08-01' & Date <= '2020-07-31','2020',season),
    season = ifelse(Date >= '2020-08-01' & Date <= '2021-07-31','2021',season),
    season = ifelse(Date >= '2021-08-01' & Date <= '2022-07-31','2022',season),
    season = ifelse(Date >= '2022-08-01' & Date <= '2023-07-31','2023',season),
    season.na = is.na(season),
    id = paste0(code,'_',str_extract(
      file,'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'
    ))
  ) %>% 
  dplyr::filter(season.na == 'FALSE') %>% 
  dplyr::filter(!(duplicated(id))) %>% 
  dplyr::arrange(date)

code.rep <- fields$code.rep %>% unique() %>% sort()
codes <- fields.df$code %>%unique() %>% sort()

seasons <- fields.df$year %>% unique() %>% sort()
##EXTRACTION LOOP, NESTED BY BY COVER CROP YEAR THEN FIELDS BY THOSE YEARS--------------------------------
start.time = Sys.time()
# seasons = seasons[5]
for (i in 1:length(seasons)){
  
  
  files.raster = pl.files %>% 
    dplyr::filter(season == seasons[i]) 
  
  pl.df = fields %>% 
    st_drop_geometry() %>% 
    dplyr::filter(year == seasons[i]) %>% 
    select(code.rep) 
  
  
  for(j in 1:length(files.raster$file)){
    
    tryCatch({
      print(paste("Loop  i at:", i,' of ',length(seasons)))
      
      print(paste("Loop", i,' of ',length(seasons),'seasons --',
                  j,' of ',length(files.raster$file),' rasters'))
      
      raster = terra::rast(files.raster$file[j]) 
      crs.rast = paste0('epsg: ',crs(raster,describe=T)[3])
      
      field = fields %>% 
        dplyr::filter(year == seasons[i])  %>% 
        st_transform(crs.rast)
      
      
      # raster = raster.initial #%>%
      # terra::crop(field$geometry %>% vect())
      
      raster$NDVI = (raster$nir - raster$red)/(raster$nir + raster$red)
      
      if (files.raster$bands[j] == '8') {
        raster$NDRE <- (raster$nir - raster$rededge) / (raster$nir + raster$rededge)
      } else {
        raster$NDRE <- rast(raster, nlyr=1)
        values(raster$NDRE) <- NA
      }
      
      ifelse(
        files.raster$bands[j]=='4',
        set.names(raster,c(      
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
        ),
        set.names(raster,c(
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_cb'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_greeni'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_yellow'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_rededge'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
        ))
      
      mask.file = str_replace(files.raster$file[j],'AnalyticMS_SR[_8b]{0,3}_clip','udm2_clip')
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask = rast(mask.file,
                      lyrs = c(1,7))
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask[udm.mask$clear != 1] <- NA
      udm.mask[udm.mask$confidence <90] <- NA
      udm.mask[udm.mask$confidence >=90] <-  1
      
      mask.cells = ncell(udm.mask)
      df.na = ifelse(
        is.na((values(udm.mask$confidence) %>% is.na() %>% 
                 table() %>% as.data.frame() %>% dplyr::filter(. == 'TRUE') %>% 
                 select(Freq) %>% as.integer()))==F,
        values(udm.mask$confidence) %>% is.na() %>% 
          table() %>% as.data.frame() %>% dplyr::filter(. == 'TRUE') %>% 
          select(Freq) %>% as.integer(),
        0
      )
      
      if((df.na >= mask.cells*.6)==T)next
      
      stk.mask1 = mask(raster, mask = udm.mask$clear) # mask by clear band
      stk.mask = mask(stk.mask1, mask = udm.mask$confidence) # mask by confidence band
      
      extracted = exactextractr::exact_extract(
        x= stk.mask,
        y= field,
        fun = 'median')
      
      
      pl.df = cbind(pl.df,extracted)
      
      gc()
      tmpFiles(remove = TRUE)
      
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")
    })
    
  }
  
  
  write.csv(pl.df,paste0(
    'psa_planet_extractions_conf90_nobuffer_',seasons[i],'.csv')
    ,
    row.names = F)
  
}


##SAME LOOP BUT WITH -2m BUFFER--------------------------------

pl.files.2mbuffer <- fields %>% 
  st_transform('epsg:32617') %>% 
  st_buffer(dist= -2) %>% 
  dplyr::filter(st_is_empty(geometry)==F) %>% 
  st_drop_geometry() %>% 
  select(code) %>% 
  distinct() %>% 
  left_join(pl.files,by='code') %>% 
  dplyr::arrange(date)


for (i in 1:length(seasons)){
  
  files.raster = pl.files.2mbuffer %>% 
    dplyr::filter(season == seasons[i])
  
  field = fields %>% 
    st_transform('epsg:32617') %>% 
    st_buffer(dist= -2) %>% 
    dplyr::filter(st_is_empty(geometry)==F)
  
  pl.df = field %>% 
    st_drop_geometry() %>% 
    dplyr::filter(year == seasons[i]) %>% 
    select(code.rep)
  
  for(j in 1:length(files.raster$file)){
    
    tryCatch({
      print(paste("Loop  i at:", i,' of ',length(seasons)))
      
      print(paste("Loop", i,' of ',length(seasons),'seasons --',
                  j,' of ',length(files.raster$file),' rasters'))
      
      raster.initial = terra::rast(files.raster$file[j]) 
      crs.rast = paste0('epsg: ',crs(raster.initial,describe=T)[3])
      
      field = fields %>% 
        dplyr::filter(year == seasons[i])  %>%
        st_transform(crs.rast) %>% 
        st_buffer(dist= -2) %>% 
        dplyr::filter(st_is_empty(geometry)==F)
      
      raster = raster.initial #%>% 
      # terra::crop(field$geometry %>% vect())
      
      raster$NDVI = (raster$nir - raster$red)/(raster$nir + raster$red)
      
      if (files.raster$bands[j] == '8') {
        raster$NDRE <- (raster$nir - raster$rededge) / (raster$nir + raster$rededge)
      } else {
        raster$NDRE <- rast(raster, nlyr=1)
        values(raster$NDRE) <- NA
      }
      
      ifelse(
        files.raster$bands[j]=='4',
        set.names(raster,c(      
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
        ),
        set.names(raster,c(
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_cb'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_greeni'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_yellow'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_rededge'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
        ))
      
      mask.file = str_replace(files.raster$file[j],'AnalyticMS_SR[_8b]{0,3}_clip','udm2_clip')
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask = rast(mask.file,
                      lyrs = c(1,7)) #%>% 
      # terra::crop(field$geometry %>% vect())
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask[udm.mask$clear != 1] <- NA
      udm.mask[udm.mask$confidence <90] <- NA
      udm.mask[udm.mask$confidence >=90] <-  1
      
      mask.cells = ncell(udm.mask)
      df.na = ifelse(
        is.na((values(udm.mask$confidence) %>% is.na() %>% 
                 table() %>% as.data.frame() %>% dplyr::filter(. == 'TRUE') %>% 
                 select(Freq) %>% as.integer()))==F,
        values(udm.mask$confidence) %>% is.na() %>% 
          table() %>% as.data.frame() %>% dplyr::filter(. == 'TRUE') %>% 
          select(Freq) %>% as.integer(),
        0
      )
      
      if((df.na >= mask.cells*.6)==T)next
      
      stk.mask1 = mask(raster, mask = udm.mask$clear) # mask by clear band
      stk.mask = mask(stk.mask1, mask = udm.mask$confidence) # mask by clear band
      
      extracted = exactextractr::exact_extract(
        x= stk.mask,
        y= field,
        fun = 'median')
      
      pl.df = cbind(pl.df,extracted)
      
      gc()
      tmpFiles(remove = TRUE)
      
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")
    })
    
  }
  
  
  write.csv(pl.df,paste0(
    'psa_planet_extractions_conf90_2mbuffer_',seasons[i],'.csv')
    ,
    row.names = F)
  
}


##SAME LOOP BUT WITH -3m BUFFER--------------------------------

pl.files.3mbuffer <- fields %>% 
  st_transform('epsg:32617') %>% 
  st_buffer(dist= -3) %>% 
  dplyr::filter(st_is_empty(geometry)==F) %>% 
  st_drop_geometry() %>% 
  select(code) %>% 
  distinct() %>% 
  left_join(pl.files,by='code') %>% 
  dplyr::arrange(date)


for (i in 1:length(seasons)){
  
  
  files.raster = pl.files.3mbuffer %>% 
    dplyr::filter(season == seasons[i])
  
  field = fields %>% 
    st_transform('epsg:32617') %>% 
    st_buffer(dist= -3) %>% 
    dplyr::filter(st_is_empty(geometry)==F)
  
  pl.df = field %>% 
    st_drop_geometry() %>% 
    dplyr::filter(year == seasons[i]) %>% 
    select(code.rep)
  
  for(j in 1:length(files.raster$file)){
    
    tryCatch({
      print(paste("Loop  i at:", i,' of ',length(seasons)))
      
      print(paste("Loop", i,' of ',length(seasons),'seasons --',
                  j,' of ',length(files.raster$file),' rasters'))
      
      raster.initial = terra::rast(files.raster$file[j]) 
      crs.rast = paste0('epsg: ',crs(raster.initial,describe=T)[3])
      
      field = fields %>% 
        dplyr::filter(year == seasons[i])  %>%
        st_transform(crs.rast) %>% 
        st_buffer(dist= -3) %>% 
        dplyr::filter(st_is_empty(geometry)==F)
      
      raster = raster.initial #%>% 
      # terra::crop(field$geometry %>% vect())
      
      raster$NDVI = (raster$nir - raster$red)/(raster$nir + raster$red)
      
      if (files.raster$bands[j] == '8') {
        raster$NDRE <- (raster$nir - raster$rededge) / (raster$nir + raster$rededge)
      } else {
        raster$NDRE <- rast(raster, nlyr=1)
        values(raster$NDRE) <- NA
      }
      
      ifelse(
        files.raster$bands[j]=='4',
        set.names(raster,c(      
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}[_0-9]{0,3}_[:alnum:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
        ),
        set.names(raster,c(
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_cb'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_greeni'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_yellow'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_rededge'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[0-9]{2}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
        ))
      
      mask.file = str_replace(files.raster$file[j],'AnalyticMS_SR[_8b]{0,3}_clip','udm2_clip')
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask = rast(mask.file,
                      lyrs = c(1,7)) #%>% 
      # terra::crop(field$geometry %>% vect())
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask[udm.mask$clear != 1] <- NA
      udm.mask[udm.mask$confidence <90] <- NA
      udm.mask[udm.mask$confidence >=90] <-  1
      
      mask.cells = ncell(udm.mask)
      df.na = ifelse(
        is.na((values(udm.mask$confidence) %>% is.na() %>% 
                 table() %>% as.data.frame() %>% dplyr::filter(. == 'TRUE') %>% 
                 select(Freq) %>% as.integer()))==F,
        values(udm.mask$confidence) %>% is.na() %>% 
          table() %>% as.data.frame() %>% dplyr::filter(. == 'TRUE') %>% 
          select(Freq) %>% as.integer(),
        0
      )
      
      if((df.na >= mask.cells*.6)==T)next
      
      stk.mask1 = mask(raster, mask = udm.mask$clear) # mask by clear band
      stk.mask = mask(stk.mask1, mask = udm.mask$confidence) # mask by clear band
      
      extracted = exactextractr::exact_extract(
        x= stk.mask,
        y= field,
        fun = 'median')
      
      pl.df = cbind(pl.df,extracted)
      
      gc()
      tmpFiles(remove = TRUE)
      
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")
    })
    
  }
  
  
  write.csv(pl.df,paste0(
    'psa_planet_extractions_conf90_3mbuffer_',seasons[i],'.csv')
    ,
    row.names = F)
  
}
# end.time = Sys.time()
# total.time = (end.time - start.time)/60;total.time
beepr::beep(8)

##PREPARE DATA FOR LONG FORMAT---------------------------

##read in outputs from extractions
extraction.files <- list.files(
  path = 'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\OUTPUTS',
  recursive = F,full.names = T,
  pattern = '*conf90_[23mno]{2}buffer_[0-9]{4}') %>% 
  as.data.frame() %>% 
  rename('file' = '.') %>% 
  mutate(
    buffer = ifelse(str_detect(file,'nobuffer'),'NoBuffer','Y'),
    buffer = ifelse(str_detect(file,'2m'),'2m',buffer),
    buffer = ifelse(str_detect(file,'3m'),'3m',buffer)
  )

##Loop through extractions to to make row wise
df.all = data.frame()
for(i in 1:length(extraction.files$file)){
  
  df = read.csv(extraction.files$file[i])
  
  buffer = extraction.files$buffer[i]
  
  df.cols = dim(df)[2]
  
  df.long = pivot_longer(df,cols=c(2:df.cols)) %>% 
    mutate(
      name = str_remove(name,'median.x'), ##scene name with #/bands and band name
      scene = str_remove(name,'_[48]{1}b_[:alpha:]{1,5}'),##scene id
      sensor = str_extract(name,'[:alnum:]{4}_[48]{1}b'),##sensor id
      sensor = str_remove(sensor,'_[48]{1}b'),
      band = str_extract(name,'_[:alpha:]{1,5}$') %>% ##specific band or index
        str_remove('_'),
      date = str_extract(scene,'[0-9]{8}') %>% ymd(),##image acquisition date
      bands = str_extract(name,'[48]{1}b'),## 4 or 8 band imagery
      field.date = paste0(code.rep,'_',date),##used to calculate #/scenes per day by polygon feature
      buffered = buffer ##no buffer, 2m, or 3m reverse buffer
    ) %>% 
    dplyr::filter(band == 'NDVI' | band == 'NDRE') %>% 
    na.omit() ##omits NA band values that did not intersect with code.rep
  ##as well as NDRE values not found in 4-band imagery.
  ##there were multiple imagery files having the same scene name 
  ##that were clipped to separate fields.
  
  df.band.table = df.long %>% 
    select(field.date,bands) %>% 
    table() %>% as.data.frame() %>% 
    mutate(
      bands = as.character(bands),
      band8.detect = any(df.long$bands == "8b")
    ) %>% 
    dplyr::filter(!(bands =='4b' & band8.detect == 'TRUE')) %>% 
    rename('band8.Freq' = 'Freq') %>% 
    select(-bands,-band8.detect)
  
  
  ##calculate #/ daily scenes per code-rep
  date.count <- df.long$field.date %>% 
    table() %>% 
    as.data.frame() %>% 
    dplyr::rename('field.date' = '.'
                  ,'Scene.Count' = 'Freq')
  
  data <- df.long %>% 
    left_join(date.count, by='field.date') %>%
    left_join(df.band.table, by='field.date') %>% 
    # dplyr::filter(!(bands == '4b' & band8.Freq >0)) %>% 
    group_by(field.date,band) %>%
    mutate(
      MeanValue = mean(value,na.rm=T) ##calculate mean of median values per day
    ) %>%
    ungroup() %>% 
    mutate(
      MedianValue = ifelse(Scene.Count<2,value,MeanValue) ##return MeanValue (of median values) if more than one scene on a given day, otherwise return the original median value
    ) %>% 
    na.omit() %>% 
    rename('Band' = 'band')
  
  df.all = rbind(df.all,data)
}
gc()
write.csv(df.all,'psa_planet_extractions_conf90_long.csv',row.names = F)


##PEPARE DATA FOR WIST/WIDE FORMAT----------------------------

df.all.fin <- df.all %>% 
  mutate(
    date = str_remove_all(date,'-') %>% ymd(),
    date.j = paste0(substr(
      date %>% 
        str_remove_all('-'),1,4),yday(date)), ##Julian date
    date.j = ifelse(nchar(date.j) == '6', ##Format Julian dates to be YYYYXXX
                    paste0(
                      substr(date.j,1,4),
                      '0',
                      substr(date.j,5,6))
                    ,date.j),
    date.j = ifelse(nchar(date.j) == '5',
                    paste0(
                      substr(date.j,1,4),
                      '00',
                      substr(date.j,5,5))
                    ,date.j),
    Date = paste0('x',date.j), ##this will be used for the outputs
    season = ifelse(date < '2019-06-10','2019',NA), ##used for WIST loop
    season = ifelse(date >= '2019-08-01' & date < '2020-08-01','2020',season),
    season = ifelse(date >= '2020-08-01' & date < '2021-08-01','2021',season),
    season = ifelse(date >= '2021-08-01' & date < '2022-08-01','2022',season),
    season = ifelse(date >= '2022-08-01' & date < '2023-08-01','2023',season),
    season.na = is.na(season),
    buffered = ifelse(buffered == 'NoBuffer','No',buffered)
  ) %>% 
  dplyr::filter(Band == 'NDVI' | Band == 'NDRE')

gc()
##Loop through long format to save WIST outputs  
for(i in 1:length(seasons)){
  
  df.sub = df.all.fin %>% 
    dplyr::filter(season == seasons[i])
  
  
  buffer = df.sub$buffered %>% unique()
  
  for(j in 1:length(buffer)){
    
    df.sub.buff = df.sub %>% 
      dplyr::filter(buffered == buffer[j])
    
    indices = df.sub.buff$Band %>% unique()
    
    
    for(o in 1:length(indices)){
      
      df.sub.index = df.sub.buff %>% 
        dplyr::filter(Band == indices[o]) %>% 
        select(code.rep,Date,MedianValue) %>% 
        rename('value' = 'MedianValue') %>% 
        dplyr::arrange(Date) %>% 
        distinct() %>%
        pivot_wider(names_from = Date)
      
      write.csv(df.sub.index,paste0(
        'psa_WIST_planet_',seasons[i],'_extractions_',indices[o],
        '_',buffer[j],'buffer','.csv'),
        row.names = F)
    }
  }
}
gc()
# end.time = Sys.time()
# total.time = (end.time - start.time)/60;total.time
