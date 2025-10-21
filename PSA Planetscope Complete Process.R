library(dplyr)
library(terra)
library(sf)
library(exactextractr);library(stringr)
library(tidyterra);library(jsonlite)

##OBJECTIVE: ORDER, DOWNLOAD, AND PROCESS PLANETSCOPE EXTRACTION DATA
##FOR PSA ON-FARM FIELDS. 

##SETTING UP THE DATA--------------------------------------
fields = st_read(
  paste0(getwd(),'\\DATA\\Biomass_polygon1.geojson')) %>% 
  mutate(
    code.rep = paste0(code,'_',rep))

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
  dplyr::filter(order.id =='NA') %>%
  select(code) %>% as.vector() %>% unlist() %>% 
  unique()

df.1 = data.frame()
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


##SUMMARIZE DOWNLOADED DATA--------------------------------------
pl.files.summary <- list.files(
  'E:\\Remote Sensing Team\\PSA\\IMAGERY\\PLANET\\IMAGERY',
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
    planting.min.diff = (date.min - cover_planting), ##gap between planting and available imagery
    term.min.diff = abs(date.max - cc_termination_date), ##gap between termination and available imagery
    Flagged = ifelse(planting.min.diff >7 |
                       term.min.diff>7
                     ,'1','0')
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
  'E:\\Remote Sensing Team\\PSA\\IMAGERY\\PLANET\\IMAGERY',
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
  dplyr::filter(code %in% pl.files.final.flagged$code)

# codes <- pl.files.final.flagged$code
# codes = codes[c(1,3:27)]
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
  
  
  
  codes.sub = pl.files.final.flagged %>% 
    dplyr::filter(code == codes[i])
  
  if(nrow(codes.sub) <1)next
  
  dates.omit <- orders.4$date %>% unique()
  
  df.dates <- seq(from = min(codes.sub$date.begin), 
                  to = max(codes.sub$date.end), by = "day")  %>% 
    as.data.frame() %>% 
    rename('date' = '.') %>% 
    dplyr::filter(!(date %in% dates.omit))
  
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
      bundle.first,
      '_followup');product.order.name
    
    
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

orders.4.files <- list.files(path = 'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\DOWNLOADS 4',
                             recursive = F,full.names = T,
                             pattern = '*.csv$')

orders.4.initial = data.frame()
for(i in 1:length(orders.4.files)){
  file = read.csv(orders.4.files[i]) 
  
  colnames(file)[2] <- 'code'
  
  
  orders.4.initial = rbind(orders.4.initial,file)
}

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

##DOWNLOAD ORDERS-----------------------------------------

library(tidyverse)
library(glue)
library(curl)
library(purrr)
library(progress);library(httr)
library(jsonlite);source('secrets.R') 


auth = authenticate(planet.api.key3, "")

download_root = "D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\DOWNLOADS 4"

download.code.list <-  df.1

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
  dplyr::filter(str_detect(name,'followup'))


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
zip_dir <- 'D:\\Projects\\Planet Orders - PSA\\Planet Orders - PSA\\DOWNLOADS 4'

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
    season = ifelse(Date >= '2019-08-01' & Date < '2020-07-01','2020',season),
    season = ifelse(Date >= '2020-08-01' & Date < '2021-07-01','2021',season),
    season = ifelse(Date >= '2021-08-01' & Date < '2022-07-01','2022',season),
    season = ifelse(Date >= '2022-08-01' & Date < '2023-07-01','2023',season),
    season.na = is.na(season)
  ) %>% 
  dplyr::filter(season.na == 'FALSE') %>% 
  dplyr::arrange(date)

code.rep <- fields$code.rep %>% unique() %>% sort()
codes <- fields.df$code %>%unique() %>% sort()

seasons <- fields.df$year %>% unique() %>% sort()
##EXTRACTION LOOP, NESTED BY BY COVER CROP YEAR THEN FIELDS BY THOSE YEARS--------------------------------

# pl.df <- code.rep %>% as.data.frame() %>% rename('code.rep' = '.')
# seasons = seasons[1:2]
for (i in 1:length(seasons)){
  
  
  
  # files.raster = pl.files %>% 
  #   dplyr::filter(code == codes[i])
  
  files.raster = pl.files %>% 
    dplyr::filter(season == seasons[i])
  
  # files.raster = files.raster[c(1:50),]
  
  pl.df = fields %>% 
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
        st_transform(crs.rast) #%>% st_buffer(dist= -2)
      
      # for(j in 1:nrow(files.raster)){
      
      
      # raster.initial = terra::rast(files.raster$file[j])
      
      raster = raster.initial %>% 
        terra::crop(field$geometry %>% vect())
      
      raster$NDVI = (raster$nir - raster$red)/(raster$nir + raster$red)
      
      if (pl.files$bands[i] == '8') {
        raster$NDRE <- (raster$nir - raster$rededge) / (raster$nir + raster$rededge)
      } else {
        raster$NDRE <- rast(raster, nlyr=1)
        values(raster$NDRE) <- NA
      }
      
      ifelse(
        files.raster$bands[j]=='4',
        set.names(raster,c(      
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
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
                      lyrs = c(1,7)) %>% 
        terra::crop(field$geometry %>% vect())
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask[udm.mask$clear != 1] <- NA
      udm.mask[udm.mask$confidence <80] <- NA
      udm.mask[udm.mask$confidence >=80] <-  1
      
      stk.mask = mask(raster, mask = udm.mask) # mask by clear band
      
      extracted = exactextractr::exact_extract(
        x= stk.mask,
        y= field,
        fun = 'median')
      
      # df.2 = cbind(field$code.rep,extracted)
      
      
      pl.df = cbind(pl.df,extracted)
      
      gc()
      tmpFiles(remove = TRUE)
      
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")
    })
    
  }
  
  
  write.csv(pl.df,paste0(
    'psa_planet_extractions_conf80_nobuffer_',seasons[i],'.csv')
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
 
      raster = raster.initial %>% 
        terra::crop(field$geometry %>% vect())
      
      raster$NDVI = (raster$nir - raster$red)/(raster$nir + raster$red)
      
      if (pl.files$bands[i] == '8') {
        raster$NDRE <- (raster$nir - raster$rededge) / (raster$nir + raster$rededge)
      } else {
        raster$NDRE <- rast(raster, nlyr=1)
        values(raster$NDRE) <- NA
      }
      
      ifelse(
        files.raster$bands[j]=='4',
        set.names(raster,c(      
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
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
                      lyrs = c(1,7)) %>% 
        terra::crop(field$geometry %>% vect())
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask[udm.mask$clear != 1] <- NA
      udm.mask[udm.mask$confidence <80] <- NA
      udm.mask[udm.mask$confidence >=80] <-  1
      
      stk.mask = mask(raster, mask = udm.mask) # mask by clear band
      
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
    'psa_planet_extractions_conf80_2mbuffer_',seasons[i],'.csv')
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

      raster = raster.initial %>% 
        terra::crop(field$geometry %>% vect())
      
      raster$NDVI = (raster$nir - raster$red)/(raster$nir + raster$red)
      
      if (pl.files$bands[i] == '8') {
        raster$NDRE <- (raster$nir - raster$rededge) / (raster$nir + raster$rededge)
      } else {
        raster$NDRE <- rast(raster, nlyr=1)
        values(raster$NDRE) <- NA
      }
      
      ifelse(
        files.raster$bands[j]=='4',
        set.names(raster,c(      
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_blue'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_green'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_red'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_nir'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDVI'),
          paste0('x',str_extract(files.raster$file[j],'[0-9]{8}_[0-9]{6}_[:graph:]{4}'),'_',files.raster$bands[j],'b','_NDRE'))
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
                      lyrs = c(1,7)) %>% 
        terra::crop(field$geometry %>% vect())
      
      #resource for UDM2 cloud mask https://developers.planet.com/docs/data/udm-2/
      udm.mask[udm.mask$clear != 1] <- NA
      udm.mask[udm.mask$confidence <80] <- NA
      udm.mask[udm.mask$confidence >=80] <-  1
      
      stk.mask = mask(raster, mask = udm.mask) # mask by clear band
      
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
    'psa_planet_extractions_conf80_3mbuffer_',seasons[i],'.csv')
    ,
    row.names = F)
  
}

