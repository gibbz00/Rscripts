contents<- read.csv(file="C:/Users/ssizan/Documents/Propensity2.0/Propensity2.0/Content1.csv",sep=",",header=TRUE,stringsAsFactors=FALSE)

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=proddb-fsphl-01;trusted_connection=true")
consumption<-sqlQuery(odbcChannel,"
                      SELECT
                      [master].[dbo].[SF_15to18](a.[CONTACT_ID]) AS [ContactID]
                      , b.[Name]
                      , 'WebHit' as ActivityType
                      , convert(date,a.[EVENT_TS]) as ActivityDate
                      ,a.EVENT_NAME
                      , a.EVENT_URL
                      FROM 
                      [Silverpop].[dbo].[CrosswalkedWebTracking] a
                      JOIN
                      [SalesForce Backups].[dbo].[Contact] b
                      ON
                      b.[Id] = [master].[dbo].[SF_15to18](a.[CONTACT_ID])
                      WHERE 
                      --a.[EVENT_TYPE_NAME] IN ('click', /*'sitevisit',*/ 'pageview','form','multimedia','videoplay')
                      
                      a.EVENT_NAME IN ('why-education','brexit','easy-is-relative','private-equity','Its-not-polite-to-ask-an-expansion-how-old-it-is','mentor','middle-market-lending','never-settle','open-end-and-closed-end-funds','best-practices','iceberg-right-ahead','manager-matters','pokemon-go','wheres-the-growth','structure-structure-structure','Its-not-polite-to-ask-an-expansion-how-old-it-is','private-us-energy','AT-FSCP-ENERGY','FL-PERS-ENERGY','WP-FSCP-INV2','AT-FSCP-KODAK','WP-FSCP-CREDIT','AS-FSCP-FLOAT','FS-PERS-ENERGY','AL-FSCP-SSL','AT-FSCP-EIRCOM','AT-FS-NORMAL','AS-FSCP-FRONT','WP-FSCP-DEFAULT','WP-IC3-GM')
                      and  b.SFDC_Account_Name_Test__c != 'FS Investment Solutions, LLC'
                      
                      and a.EVENT_URL like '%fsinvestments%'
                      ")

odbcClose(odbcChannel)

odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

CLC<-sqlQuery(odbcChannel,"
              SELECT [ContactID]
              ,[SalesForce_Name]
              ,[EngagementLevel1]   
              FROM [DataScience].[dbo].[CurrentAdvisorStatus]
              where EngagementLevel1 <> 'PresentDay'
              ")

odbcClose(odbcChannel)

library(data.table)
setDT(consumption)
consumption<-consumption[!duplicated(consumption, by=c("ContactID","EVENT_NAME"))]

setDT(contents)

#contents[,SKU:= sprintf("%-15s %15s", SKU_Description__c, EVENT_URL)]

contents1<- contents[,c("EVENT_NAME","EVENT_URL","Level"),with=FALSE]
nonzerocontents<- contents1[Level == 1][,c("EVENT_NAME","EVENT_URL"),with=FALSE]
zerocontents<- contents1[Level == 0][,c("EVENT_NAME","EVENT_URL"),with=FALSE]
setDT(CLC)



setkey(CLC, ContactID)
setkey(consumption, ContactID)

recom<-consumption[CLC]

recom<- recom[,c("ContactID","SalesForce_Name","EngagementLevel1","EVENT_NAME"),with=FALSE]

set.seed(123)
recom[,SKUNextContent:= ifelse(EngagementLevel1 == "Level0" ,sample(setdiff(zerocontents$EVENT_NAME,EVENT_NAME),1), sample(setdiff(nonzerocontents$EVENT_NAME,EVENT_NAME),1)),by=ContactID]
recom<-recom[!duplicated(recom, by="ContactID")]

setkey(recom, SKUNextContent)
setkey(contents1,EVENT_NAME)

recom1<- contents1[recom]

recom1<- recom1[,c("ContactID","SalesForce_Name","EngagementLevel1","EVENT_NAME","EVENT_URL"),with=FALSE]

setnames(recom1,"EVENT_NAME","SKUNextContent")
setnames(recom1,"EVENT_URL","URLNextContent")

recom1[,RecommendationDate:= Sys.Date()]
setkey(recom1,ContactID)
#setkey(recom1,ContactID, RecommendationDate)
#recom1[, RecommendationID := rep(1:.N, each=1, length.out=.N),by=ContactID]


#recom1<- recom1[,c("RecommendationID","RecommendationDate","ContactID","SalesForce_Name","EngagementLevel1","SKUNextContent","URLNextContent"),with=FALSE]

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

recom2<-sqlQuery(odbcChannel,"
                 SELECT *
                 FROM [DataScience].[dbo].[ContentRecommendation]
                 ")
odbcClose(odbcChannel)

setDT(recom2)
recom2[, RecommendationDate := as.Date(RecommendationDate, format="%Y-%m-%d")]

#Combining Old and New and then we will put it into sql as a snapshot
recom3<-unique(rbind(recom2,recom1), by=c("ContactID", "SKUNextContent"))

setkey(recom3, ContactID,RecommendationDate)
snapshot<- recom3
setkey(snapshot, ContactID,RecommendationDate)

recom3[, RecommendationID := rep(1:.N, each=1, length.out=.N),by=ContactID]

setkey(consumption,ContactID,EVENT_NAME)
setkey(recom3,ContactID,SKUNextContent)

recom4<- consumption[recom3]

setnames(recom4,"EVENT_NAME","SKUNextContent")
setnames(recom4,"ActivityDate","ExecutionDate")

recom4<- recom4[,c("RecommendationID","ContactID","SalesForce_Name","EngagementLevel1","SKUNextContent","URLNextContent","RecommendationDate","ExecutionDate"),with=FALSE]
setkey(recom4, ContactID,RecommendationDate)
snapshot[,RecommendationID:=NULL]

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")
sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[ContentRecommendation]")


sqlSave(odbcChannel,snapshot,tablename = "ContentRecommendation",rownames=FALSE,append=TRUE)



sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[RecommendationEngine]")

sqlSave(odbcChannel,recom4,tablename = "RecommendationEngine",rownames=FALSE,append=TRUE)
odbcClose(odbcChannel)


