fun<-function(x, date, thresh) {
  D <- as.matrix(dist(date)) #distance matrix between dates
  D <- D <= thresh
  D[lower.tri(D)] <- FALSE #don't sum to future
  R <- D * x #FALSE is treated as 0
  colSums(R)
}
library("RODBC")
#odbcChannel<- odbcDriverConnect("driver={SQL Server};server=PRODDB-FSPHL-01;trusted_connection=true")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=proddb-fsphl-01;trusted_connection=true")
df<-sqlQuery(odbcChannel,"With ContactMinDate AS(
             select
             b.ContactID,b.Salesforce_Name ,
             case When b.ActivityDate<b.CreatedDate Then ActivityDate
             When b.CreatedDate<b.ActivityDate Then CreatedDate
             Else CreatedDate
             End As MinDate
             From
             (select
             
             a.ContactID, a.SalesForce_Name,min(a.ActivityDate) as ActivityDate, cast(c.CreatedDate as date) as [CreatedDate]
             
             FROM 
             [PropensityScore].[dbo].[PropensityInput] a
             left join [SalesForce Backups].[dbo].Contact c on c.Id = a.ContactID
             
             group by a.ContactID,a.SalesForce_Name,c.CreatedDate
             
             
             )b
             UNION ALL
             
             SELECT distinct c.Id as ContactID,c.name,cast(c.CreatedDate as date) as [CreatedDate]
             FROM   [SalesForce Backups].[dbo].Contact c
             LEFT OUTER JOIN [PropensityScore].[dbo].[PropensityInput] a
             ON (c.id = a.ContactID)
             WHERE a.ContactID IS NULL	
             UNION ALL
             
             SELECT distinct c.Id as ContactID,c.name,CONVERT(DATE, GETDATE()) as [TodaysDate]
             FROM   [SalesForce Backups].[dbo].Contact c
             LEFT OUTER JOIN [PropensityScore].[dbo].[PropensityInput] a
             ON (c.id = a.ContactID)
             WHERE a.ContactID IS NULL	
             
)	
             
             SELECT
             d.ContactID
             , d.SalesForce_Name
             , NULL AS [ActivityType]
             , d.MinDate as [ActivityDate]
             , NULL [SalesAmount]
             , NULL As [AdditionalDetail]
             , NULL as [Link]
             ,e.Type__c as [Type]
             ,cast(e.First_Sale_Date_All_Funds_New__c as date) as [FirstSaleDate]
             
             FROM 
             ContactMinDate d
             left join [SalesForce Backups].[dbo].Contact e on e.Id = d.ContactID
             
             UNION ALL
             
             SELECT 
             a.ContactID
             , a.SalesForce_Name
             , a.ActivityType
             , a.ActivityDate
             , a.SalesAmount
             , a.AdditionalDetail 
             , a.Link
             ,c.Type__c as [Type]
             ,cast(c.First_Sale_Date_All_Funds_New__c as date) as [FirstSaleDate]
             
             
             FROM 
             [PropensityScore].[dbo].[PropensityInput] a
             left join [SalesForce Backups].[dbo].Contact c on c.Id = a.ContactID
             
             WHERE 
             a.ActivityType NOT IN ('DD Kit Mailing', 'To-Do', 'Voicemail','BD Event','Bond Deal') 
             
             
             UNION ALL
             
             SELECT
             ContactID
             , SalesForce_Name
             , NULL AS [ActivityType]
             , CONVERT(DATE, GETDATE()) AS [ActivtyDate]
             , NULL [SalesAmount]
             , NULL As [AdditionalDetail]
             , NULL as [Link]
             ,c.Type__c as [Type]
             ,cast(c.First_Sale_Date_All_Funds_New__c as date) as [FirstSaleDate]
             
             FROM 
             [PropensityScore].[dbo].[PropensityInput] a
             left join [SalesForce Backups].[dbo].Contact c on c.Id = a.ContactID
             
             WHERE
             a.ActivityType NOT IN ('DD Kit Mailing', 'To-Do', 'Voicemail','BD Event','Bond Deal')
             
             GROUP BY 
             ContactID
             , SalesForce_Name
             , AdditionalDetail
             , Link
             ,c.Type__c
             ,c.First_Sale_Date_All_Funds_New__c
             
             ORDER BY 
             ContactId
             , ActivityDate")

odbcClose(odbcChannel)
#dfbackup<-df
#Cleaning up ActivityType factor level names
levels(df$ActivityType)<-c("Admin.Call","Assisted.Roadshow","Business.Development","Call","Clicked.Email","Client.Services.Call","Conference.Call","Seminar","Seminar.Entertainment","EdForum","Email","HardMail","Inbound","Investor.University","VoiceMail","LitOrder","Marketing.Webinar","Meeting","Opened.Email","Outbound","Sale","Sales.Presentation","Selling.Agreement","Unassisted.Roadshow","Web.Presentation","WebHit")
df$ActivityType <- as.character(df$ActivityType)
df$AdditionalDetail <- as.character(df$AdditionalDetail)
df$Link <- as.character(df$Link)
df$ActivityType[is.na(df$ActivityType)]<- "Nothing"
df$AdditionalDetail[is.na(df$AdditionalDetail)]<- "Nothing"
df$ActivityType <- factor(df$ActivityType)
df$AdditionalDetail <- factor(df$AdditionalDetail)
library(data.table)
setDT(df)

df<-df[!duplicated(df, by=c("ContactID","ActivityType","ActivityDate","AdditionalDetail"))]

setkey(df, ContactID, ActivityDate)
df[, ActivityDate := as.Date(ActivityDate, format="%Y-%m-%d")]
df[, Website:= fun(ActivityType == "WebHit" , ActivityDate, 21), by = ContactID]
df[, Webinar := fun(ActivityType == "Marketing.Webinar" , ActivityDate, 35), by = ContactID]
#df[, SalesPresentation := fun(ActivityType == "Sales.Presentation" , ActivityDate, 45), by = ContactID]
df[, Roadshow := fun((ActivityType == "Assisted.Roadshow" | ActivityType == "Unassisted.Roadshow") , ActivityDate, 180), by = ContactID]
df[, Meeting := fun(ActivityType == "Meeting" , ActivityDate, 180), by = ContactID]
df[, EdForum:= fun(ActivityType == "EdForum" , ActivityDate, 540), by = ContactID]

df1<-df[,c("ContactID","SalesForce_Name","Website","Webinar","Roadshow","Meeting","EdForum"),with=FALSE][, .SD[.N], by=ContactID]

odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

CLC<-sqlQuery(odbcChannel,"
SELECT [ContactID]
      ,[SalesForce_Name]
              ,[EngagementLevel1]   
              FROM [DataScience].[dbo].[CurrentAdvisorStatus]
              
              where EngagementLevel1 <> 'PresentDay' ")
odbcClose(odbcChannel)
setDT(CLC)
setkey(CLC, ContactID)
setkey(df1,ContactID)

df1<-df1[CLC] #joining with CurrentAdvisorStatus table
# 
# levelmap = data.table(Level =  c("Level0","Level1","Level2","Level3","Level4","Level5","New Producer","Rising Star","Developing Producer","Core Producer","Advocate"), order = list(
#   c("Website","Webinar"),
#   c("Website", "Webinar", "SalesPresentation","RoadShow"), 
#   c("Webinar", "SalesPresentation", "Roadshow","Meeting"),
#   c("SalesPresentation", "Webinar", "Roadshow","Meeting"),
#   c("Roadshow", "Webinar", "Meeting","EdForum"),
#   c("Meeting", "EdForum", "Roadshow","SalesPresentation"),
#   c("SalesPresentation","Webinar","Meeting","Roadshow","Edforum"),
#   c("SalesPresentation","Webinar","Meeting","Roadshow","Edforum"),
#   c("SalesPresentation","Webinar","Meeting","Roadshow","Edforum"),
#   c("SalesPresentation","Webinar","Meeting","Roadshow","Edforum"),
#   c("SalesPresentation","Webinar","Meeting","Roadshow","Edforum") ))

  

levelmap = data.table(Level =  c("Level0","Level1","Level2","Level3","Level4","Level5","New Producer","Rising Star","Developing Producer","Core Producer","Advocate"), order = list(
  c("Website","Webinar"),
  c("Website", "Webinar","RoadShow"), 
  c("Webinar",  "Roadshow","Meeting"),
  c( "Webinar", "Roadshow","Meeting"),
  c("Roadshow", "Webinar", "Meeting","EdForum"),
  c("Meeting", "EdForum", "Roadshow"),
  c("Webinar","Meeting","Roadshow","Edforum"),
  c("Webinar","Meeting","Roadshow","Edforum"),
  c("Webinar","Meeting","Roadshow","Edforum"),
  c("Webinar","Meeting","Roadshow","Edforum"),
  c("Webinar","Meeting","Roadshow","Edforum") ))

#names(df1)[names(df1) == "SalesPrsentation"] = "SalesPresentation"
  names(df1)[names(df1) == "EngagementLevel1"] = "Level"        
         


myDT = melt(
  df1[, , with=FALSE],
  id.var = c("ContactID", "SalesForce_Name","Level"))

library(magrittr)
NextStep = myDT[levelmap, on="Level"][, .( NextStep = 
                                        variable[value == 0] %>% factor(levels = order[[1]]) %>% sort %>% toString
), keyby=.(ContactID,SalesForce_Name,Level)]


#Content Recommendation--------------------------------------------------------------------------------------------------------------------------------------------

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
                      
                      a.EVENT_NAME IN ('why-education','brexit','easy-is-relative','private-equity','Its-not-polite-to-ask-expansion-how-old-it-is','mentor','middle-market-lending','never-settle','open-end-and-closed-end-funds','best-practices','iceberg-right-ahead','manager-matters','pokemon-go','wheres-the-growth','structure-structure-structure','Its-not-polite-to-ask-an-expansion-how-old-it-is','private-us-energy','AT-FSCP-ENERGY','FL-PERS-ENERGY','WP-FSCP-INV2','AT-FSCP-KODAK','WP-FSCP-CREDIT','AS-FSCP-FLOAT','FS-PERS-ENERGY','AL-FSCP-SSL','AT-FSCP-EIRCOM','AT-FS-NORMAL','AS-FSCP-FRONT','WP-FSCP-DEFAULT','WP-IC3-GM')
                      and  b.SFDC_Account_Name_Test__c != 'FS Investment Solutions, LLC'
                      
                      and a.EVENT_URL like '%fsinvestments%'
                      ")

odbcClose(odbcChannel)

library(data.table)
setDT(consumption)
consumption<-consumption[!duplicated(consumption, by=c("ContactID","EVENT_NAME"))]

setDT(contents)

#contents[,SKU:= sprintf("%-15s %15s", SKU_Description__c, EVENT_URL)]

contents1<- contents[,c("EVENT_URL","Level"),with=FALSE]
nonzerocontents<- contents1[Level == 1][,1,with=FALSE]
zerocontents<- contents1[Level == 0][,1,with=FALSE]
setDT(CLC)



setkey(CLC, ContactID)
setkey(consumption, ContactID)

recom<-consumption[CLC]
recom<- recom[,c("ContactID","SalesForce_Name","EngagementLevel1","EVENT_NAME"),with=FALSE]

recom[,NextContent:= ifelse(EngagementLevel1 == "Level0" ,sample(setdiff(zerocontents$EVENT_URL,EVENT_NAME),1), sample(setdiff(nonzerocontents$EVENT_URL,EVENT_NAME),1)),by=ContactID]

recom<-recom[!duplicated(recom, by="ContactID")]

names(CLC)[names(CLC) == "EngagementLevel1"] = "Level" 
setkey(recom, ContactID)
setkey(NextStep, ContactID)

final<- NextStep[recom]
final<- final[,c("ContactID","SalesForce_Name","Level","NextContent","NextStep"),with=FALSE]

#Uploading to SQL

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[NextStepsForAdvisor]")

sqlSave(odbcChannel,final,tablename = "NextStepsForAdvisor",rownames=FALSE,append=TRUE)
odbcClose(odbcChannel)