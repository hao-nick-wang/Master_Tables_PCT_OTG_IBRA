# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Master file for OTG and PCT

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###Sources of data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC BioNet Plant Community Type data
# MAGIC 
# MAGIC https://www.environment.nsw.gov.au/-/media/OEH/Corporate-Site/Documents/BioNet/bionet-plant-community-type-data.xlsx

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC BioNet Threatened Ecological Community to Plant Community Types Association data
# MAGIC 
# MAGIC https://www.environment.nsw.gov.au/-/media/OEH/Corporate-Site/Documents/BioNet/bionet-threatened-ecological-community-to-plant-community-types-association-data.xlsx

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Uploading Libraries and functions

# COMMAND ----------

library(readxl)
library(tidyverse)

# COMMAND ----------

### Function to split columns by delimeter

#https://stackoverflow.com/questions/4350440/split-data-frame-string-column-into-multiple-columns

split_into_multiple <- function(column, pattern = ", ", into_prefix){
  cols <- str_split_fixed(column, pattern, n = Inf)
  # Sub out the ""'s returned by filling the matrix to the right, with NAs which are useful
  cols[which(cols == "")] <- NA
  cols <- as.tibble(cols)
  # name the 'cols' tibble as 'into_prefix_1', 'into_prefix_2', ..., 'into_prefix_m' 
  # where m = # columns of 'cols'
  m <- dim(cols)[2]

  names(cols) <- paste(into_prefix, 1:m, sep = "_")
  return(cols)
}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Downloading files

# COMMAND ----------

### Download the file
options(download.file.method="curl", download.file.extra="-k -L")
url1<-'https://www.environment.nsw.gov.au/-/media/OEH/Corporate-Site/Documents/BioNet/bionet-plant-community-type-data.xlsx'
destfile <- paste0(tempfile(),'.xls')
download.file(url = url1, destfile = paste0(destfile), method = "curl")
## Copying the file into data lake 
dbutils.fs.cp(paste0('file:',destfile), paste0('/mnt/projects-dpie-paas/Master_OTG/Input_data/BioNet_PCT.xlsx'))

# COMMAND ----------

### Download the file
options(download.file.method="curl", download.file.extra="-k -L")
url1<-'https://www.environment.nsw.gov.au/-/media/OEH/Corporate-Site/Documents/BioNet/bionet-threatened-ecological-community-to-plant-community-types-association-data.xlsx'
destfile <- paste0(tempfile(),'.xlsx')
download.file(url = url1, destfile = paste0(destfile), method = "curl")
## Copying the file into data lake 
dbutils.fs.cp(paste0('file:',destfile), paste0('/mnt/projects-dpie-paas/Master_OTG/Input_data/BioNet_PCT_Association_data.xlsx'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Reading Files from data lake

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Uploading Bionet Data

# COMMAND ----------

dbutils.fs.cp(paste0('/mnt/projects-dpie-paas/Master_OTG/Input_data/BioNet_PCT.xlsx'),"file:/tmp/BioNet_PCT.xlsx")
excel_sheets('/tmp/BioNet_PCT.xlsx')

bionet_data <- read_excel('/tmp/BioNet_PCT.xlsx', sheet = "PCT Data PQ" )

# COMMAND ----------

display(bionet_data)

# COMMAND ----------

dim(bionet_data)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Uploading Associated Data

# COMMAND ----------

dbutils.fs.cp(paste0('/mnt/projects-dpie-paas/Master_OTG/Input_data/BioNet_PCT_Association_data.xlsx'),"file:/tmp/BioNet_PCT_Association_data.xlsx")
excel_sheets('/tmp/BioNet_PCT_Association_data.xlsx')

bionet_associated <- read_excel('/tmp/BioNet_PCT_Association_data.xlsx', sheet = "All PCTs - State TECs Data PQ" )

# COMMAND ----------

display(bionet_associated)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Ecosystems

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Preparing Non-TEC OTG

# COMMAND ----------

### Potential error from bionet ####
### Certain PCT's have assesed TEC but no TECName - we will create their TEC Name as we are doing with No Associated TECs

potential_errors <- bionet_associated[bionet_associated$TECAssessed == 'Has associated TEC' & is.na(bionet_associated$TECName)==T, ]
length(unique(potential_errors$PCTID))

# COMMAND ----------

non_tec <- bionet_data[bionet_data$TECAssessed == 'No associated TEC'| bionet_data$PCTID %in% potential_errors$PCTID,]  ## Filtering Non_associated_TECs

# COMMAND ----------

# Creating rule for OTG
non_tec$PCTPercentCleared <- as.numeric(non_tec$PCTPercentCleared)
non_tec$pct_cleared_text <- ifelse(is.na(non_tec$PCTPercentCleared)==T, '', ifelse((non_tec$PCTPercentCleared < 0.5), 'less than 50%',  
                                   ifelse((non_tec$PCTPercentCleared < 0.7),'greater than or equal to 50% and less than 70%', 
                                          ifelse((non_tec$PCTPercentCleared <0.9),'greater than or equal to 70% and less than 90%', 
                                                 ifelse((non_tec$PCTPercentCleared < 1),'greater than or equal to 90%', '')))))

# COMMAND ----------

display(non_tec[,c('PCTPercentCleared','pct_cleared_text')])

# COMMAND ----------

table(non_tec$status, useNA = 'always')

# COMMAND ----------

non_tec$OTG <- ifelse(is.na(non_tec$PCTPercentCleared)==T, '', paste(non_tec$vegetationClass,non_tec$pct_cleared_text)) ### PCT with no clearing percentage will have OTG Blank

# COMMAND ----------

pct_otg_nontec <- non_tec[,c('PCTID','OTG')] ### getting PCTID and OTGs

# COMMAND ----------

dim(pct_otg_nontec)

# COMMAND ----------

complete_nontec <- merge(pct_otg_nontec,non_tec, by.x = 'PCTID', by.y = 'PCTID', all.x = T ) ### Adding all the features to the database

# COMMAND ----------

display(complete_nontec[complete_nontec$PCTID==689,])

# COMMAND ----------

dim(complete_nontec)

# COMMAND ----------

length(unique(complete_nontec$PCTID))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Getting all IBRA Subregions

# COMMAND ----------

complete_non_tec_splitted <- complete_nontec %>% 
  dplyr::bind_cols(split_into_multiple(.$IBRASubregion, ";", "IBRASubregion")) %>% 
  dplyr::select(PCTID, OTG.x, starts_with("IBRASubregion_")) # selecting those that start with 'type_' will remove the original 'type' column

complete_non_tec_splitted$IBRASubregion_1 <- ifelse(is.na(complete_non_tec_splitted$IBRASubregion_1)==T, '',complete_non_tec_splitted$IBRASubregion_1 )
complete_non_tec_splitted <- complete_non_tec_splitted %>% 
  gather(key, val, -c(PCTID, OTG.x), na.rm = T) %>%
  select(PCTID,OTG.x, val)%>%
  rename(IBRASubregion = val)

display(complete_non_tec_splitted)
  

# COMMAND ----------

dim(complete_non_tec_splitted)

# COMMAND ----------

display(complete_non_tec_splitted[complete_non_tec_splitted$PCTID==689,])

# COMMAND ----------

complete_nontec_final <- merge(complete_non_tec_splitted, complete_nontec, by.x = c('PCTID','OTG.x'), by.y = c('PCTID','OTG.x'), all.x = T)

# COMMAND ----------

display(complete_nontec_final[complete_nontec_final$PCTID==689,])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Selecting relevant fields

# COMMAND ----------

complete_nontec_final <- complete_nontec_final %>%
   select(PCTID, PCTName, IBRASubregion.x, OTG.x, status, classificationType, classificationConfidenceLevel, vegetationClass, vegetationFormation, IBRA, county, landscapeName, isADerivedPlantCommunityType, originalCommunityThisPCTDerivedFrom, derivedFromCommunityTypeComment, vegetationDescription, variationAndNaturalDisturbance, fireRegime, PCTPercentClearedStatus, PCTPercentCleared, PCTPercentClearedAccuracy, PCTPercentClearedComments,PCTPercentClearedSource, preEuropeanExtent, preEuropeanAccuracy, 
preEuropeanQualifiers, preEuropeanComments,currentExtent, currentAccuracy,currentQualifiers,currentComments, TECAssessed)

# COMMAND ----------

display(complete_nontec_final[complete_nontec_final$PCTID==689,])

# COMMAND ----------

dim(complete_nontec_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Preparing TEC

# COMMAND ----------

tec <- bionet_data[bionet_data$TECAssessed == 'Has associated TEC' & !(bionet_data$PCTID %in% potential_errors$PCTID),]

# COMMAND ----------

display(tec)

# COMMAND ----------

tec_splitted <- tec %>% 
  dplyr::bind_cols(split_into_multiple(.$stateTECProfileID, ",", "stateTECProfileID")) %>% 
  dplyr::select(PCTID, starts_with("stateTECProfileID_")) # selecting those that start with 'type_' will remove the original 'type' column

tec_splitted$stateTECProfileID_1 <- ifelse(is.na(tec_splitted$stateTECProfileID_1)==T, '', tec_splitted$stateTECProfileID_1 )
tec_splitted <- tec_splitted %>% 
  gather(key, val, -PCTID, na.rm = T) %>%
  select(PCTID, val)%>%
  rename(stateTECProfileID = val)

display(tec_splitted)
  

# COMMAND ----------

dim(tec_splitted)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Merge with tec

# COMMAND ----------

complete <- merge(tec_splitted, tec, by.x = 'PCTID', by.y = 'PCTID', all.x = T)

# COMMAND ----------

#dim(complete)
display(complete)

# COMMAND ----------

dim(complete)

# COMMAND ----------

length(unique(complete$PCTID))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Extending IBRA's

# COMMAND ----------

complete_splitted <- complete %>% 
  dplyr::bind_cols(split_into_multiple(.$IBRASubregion, ";", "IBRASubregion")) %>% 
  dplyr::select(PCTID, stateTECProfileID.x, starts_with("IBRASubregion_")) # selecting those that start with 'type_' will remove the original 'type' column


complete_splitted$IBRASubregion_1 <- ifelse(is.na(complete_splitted$IBRASubregion_1)==T, '',complete_splitted$IBRASubregion_1 )
complete_splitted <- complete_splitted %>% 
  gather(key, val, -c(PCTID, stateTECProfileID.x), na.rm = T) %>%
  select(PCTID,stateTECProfileID.x, val)%>%
  rename(IBRASubregion = val)

display(complete_splitted)
  

# COMMAND ----------

dim(complete_splitted)

# COMMAND ----------

complete_tec <- merge(complete_splitted, complete, by.x = c('PCTID','stateTECProfileID.x'), by.y = c('PCTID','stateTECProfileID.x'), all.x = T)

# COMMAND ----------

dim(complete_tec)

# COMMAND ----------

# MAGIC %md
# MAGIC Merging with OTG's

# COMMAND ----------

unique_OTG <- bionet_associated[!duplicated(bionet_associated$stateTECProfileID),c('stateTECProfileID','TECName','threats', 'habitatAndEcology','classOfCredit','sensitivityToLoss','sensitivityToLossJustification','SAII')]
unique_OTG <- unique_OTG[!is.na(unique_OTG$stateTECProfileID),]

# COMMAND ----------

display(unique_OTG)

# COMMAND ----------

complete_tec_otg <- merge(complete_tec, unique_OTG, by.x = 'stateTECProfileID.x', by.y = 'stateTECProfileID', all.x = T )
dim(complete_tec_otg)

# COMMAND ----------

display(complete_tec_otg)

# COMMAND ----------

complete_tec_otg <- complete_tec_otg %>%
   select(PCTID, PCTName,  IBRASubregion.x, stateTECProfileID.x, TECName, status, classificationType, classificationConfidenceLevel, vegetationClass, vegetationFormation, IBRA, county, landscapeName, isADerivedPlantCommunityType, originalCommunityThisPCTDerivedFrom, derivedFromCommunityTypeComment, vegetationDescription, variationAndNaturalDisturbance, fireRegime, PCTPercentClearedStatus, PCTPercentCleared, PCTPercentClearedAccuracy, PCTPercentClearedComments,PCTPercentClearedSource, preEuropeanExtent, preEuropeanAccuracy, 
preEuropeanQualifiers, preEuropeanComments,currentExtent, currentAccuracy,currentQualifiers,currentComments, TECAssessed, threats, habitatAndEcology,classOfCredit,sensitivityToLoss,sensitivityToLossJustification,SAII) %>%
  rename(OTG.x = TECName)

# COMMAND ----------

display(complete_tec_otg)

# COMMAND ----------

dim(complete_tec_otg)

# COMMAND ----------

dim(complete_nontec_final)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Merging TEC and NonTEC databases

# COMMAND ----------

all_pct <- plyr::rbind.fill(complete_tec_otg, complete_nontec_final)

# COMMAND ----------

display(all_pct[all_pct$PCTID==689,])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Adding IBRA Subregion ID

# COMMAND ----------

all_pct <- all_pct %>%  # Create ID by group
  group_by(IBRASubregion.x) %>%
  arrange(IBRASubregion.x) %>%
  dplyr::mutate(IBRASubregionID = cur_group_id()) %>%
  ungroup()
 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Preparing Extended table

# COMMAND ----------

all_pct <- all_pct %>%
   rename(IBRASubregion = IBRASubregion.x,
         stateTECProfileID = stateTECProfileID.x,
         OTGName = OTG.x)

# COMMAND ----------

all_pct <- all_pct %>% 
   relocate(IBRASubregionID, .before = IBRASubregion) %>%
   arrange(PCTID, IBRASubregionID, stateTECProfileID)

# COMMAND ----------

display(all_pct[all_pct$PCTID==689,])

# COMMAND ----------

length(unique(all_pct$PCTID))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Preparing Extended grouped by PCT and OTG

# COMMAND ----------

grouping_table <- all_pct[,c('PCTID','PCTName','stateTECProfileID','IBRASubregion','OTGName')]

# COMMAND ----------

display(grouping_table[grouping_table$PCTID==3020,])

# COMMAND ----------

unique_pct_otg <- grouping_table %>%
    group_by(PCTID,PCTName, stateTECProfileID,OTGName) %>%
    mutate(grp = row_number()) %>%
    pivot_wider(names_from = grp, values_from = IBRASubregion, names_prefix = 'Val') %>%
    ungroup()

unique_pct_otg <- unique_pct_otg %>% unite(IBRASubregion, 5:ncol(unique_pct_otg), sep="; ")

library(stringi)
unique_pct_otg$IBRASubregion <- trimws(stri_match_first_regex(unique_pct_otg$IBRASubregion, "(.*?)\\; NA;")[,2],'both')

# COMMAND ----------

display(unique_pct_otg[unique_pct_otg$PCTID==689,])

# COMMAND ----------

dim(unique_pct_otg)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Adding Attributes

# COMMAND ----------

all_attributes<- all_pct%>%
 select(-c(PCTName,IBRASubregionID,IBRASubregion, OTGName))

all_attributes <- unique(all_attributes)

# COMMAND ----------

display(all_attributes)
dim(all_attributes)

# COMMAND ----------

extended_pct_otg_final <- merge(unique_pct_otg, all_attributes, by.x = c('PCTID','stateTECProfileID'), by.y = c('PCTID','stateTECProfileID'), all.x = T)
dim(extended_pct_otg_final)

# COMMAND ----------

extended_pct_otg_final <- extended_pct_otg_final %>% 
   relocate(PCTName, .after = PCTID) %>%
   arrange(PCTID, stateTECProfileID)

# COMMAND ----------

display(extended_pct_otg_final)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Preparing Extended grouped by PCT

# COMMAND ----------

unique_pct <- unique_pct_otg %>%
    select(PCTID, PCTName, OTGName, IBRASubregion) %>%
    group_by(PCTID,PCTName, IBRASubregion) %>%
    mutate(grp = row_number()) %>%
    pivot_wider(names_from = grp, values_from = OTGName, names_prefix = 'Val') %>%
    ungroup()

unique_pct <- unique_pct %>% unite(OTGName, 4:ncol(unique_pct), sep="; ")

library(stringi)
unique_pct$OTGName <- trimws(stri_match_first_regex(unique_pct$OTGName, "(.*?)\\; NA;")[,2],'both')

# COMMAND ----------

display(unique_pct[unique_pct$PCTID==689,])

# COMMAND ----------

dim(unique_pct)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Adding Attributes

# COMMAND ----------

display(all_attributes)

# COMMAND ----------

### Removing attributes specific to each otg
all_attributes_pct  <- all_attributes%>%
 select(-c(stateTECProfileID, threats, habitatAndEcology,classOfCredit,sensitivityToLoss,sensitivityToLossJustification,SAII))

all_attributes_pct <- unique(all_attributes_pct)

# COMMAND ----------

dim(all_attributes_pct)
#display(all_attributes_pct)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Merging with attributes

# COMMAND ----------

extended_pct_final <- merge(unique_pct, all_attributes_pct, by.x = c('PCTID'), by.y = c('PCTID'), all.x = T)
dim(extended_pct_final)

# COMMAND ----------

display(extended_pct_final[extended_pct_final$PCTID==689,])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Saving tables to DL

# COMMAND ----------

display(extended_pct_final)

# COMMAND ----------

write.csv(all_pct, '/tmp/Master_extended.csv', row.names = FALSE)
dbutils.fs.cp(paste0('file:','/tmp/Master_extended.csv'), paste0('/mnt/projects-dpie-paas/Master_OTG/Output_data/Master_extended.csv'))

# COMMAND ----------

write.csv(extended_pct_otg_final, '/tmp/Master_PCTOTGGrouped_extended.csv',row.names = FALSE)
dbutils.fs.cp(paste0('file:','/tmp/Master_PCTOTGGrouped_extended.csv'), paste0('/mnt/projects-dpie-paas/Master_OTG/Output_data/Master_PCTOTGGrouped_extended.csv'))

# COMMAND ----------

write.csv(extended_pct_final, '/tmp/Master_PCTGrouped_extended.csv',row.names = FALSE)
dbutils.fs.cp(paste0('file:','/tmp/Master_PCTGrouped_extended.csv'), paste0('/mnt/projects-dpie-paas/Master_OTG/Output_data/Master_PCTGrouped_extended.csv'))


# COMMAND ----------

# MAGIC %md
# MAGIC https://stackoverflow.com/questions/71238152/how-to-send-a-pdf-object-from-databricks-to-sharepoint
# MAGIC 
# MAGIC https://stackoverflow.com/questions/55922791/azure-sharepoint-multi-factor-authentication-with-python

# COMMAND ----------

#%python
#from office365.runtime.auth.user_credential import UserCredential
#from office365.sharepoint.client_context import ClientContext
#from office365.sharepoint.files.file import File

# paths
#sharepoint_site = "MST_OEH_DataEconomicsandAnalyticsTeam" 
#sharepoint_folder = "Shared%20Documents/Product%20Management/Current_Supported_Products/Master_PCT_OTG" 
#sharepoint_user = "maximiliano.micheli@environment.nsw.gov.au" 
#sharepoint_user_pw 
#sharepoint_folder = sharepoint_folder.strip("/")

# set environment variables
#SITE_URL = f"https://environmentnswgov.sharepoint.com/sites/{sharepoint_site}"
#RELATIVE_URL = f"/sites/{sharepoint_site}/{sharepoint_folder}"

# connect to sharepoint
#ctx = ClientContext(SITE_URL).with_credentials(UserCredential(sharepoint_user, sharepoint_user_pw))
#web = ctx.web
#ctx.load(web).execute_query()

