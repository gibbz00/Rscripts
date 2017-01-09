
library("RODBC")
#odbcChannel<- odbcDriverConnect("driver={SQL Server};server=PRODDB-FSPHL-01;trusted_connection=true")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=proddb-fsphl-01;trusted_connection=true")
df<-sqlQuery(odbcChannel,"SELECT 
             a.ContactID
             , a.SalesForce_Name
             , a.ActivityType
             , a.ActivityDate
             , a.SalesAmount
             , ISNULL(b.[SingleFundCode],a.FundCode) AS [FundCode]
             , a.EventID
             FROM 
             [PropensityScore].[dbo].[PropensityInput] a
             LEFT JOIN
             [PropensityScore].[dbo].[FundCodes] b
             ON
             b.[FundCode] = a.[FundCode]
             WHERE 
             a.ActivityType NOT IN ('DD Kit Mailing', 'To-Do', 'Voicemail','BD Event','Bond Deal') 
             AND a.ActivityDate >= '2016-03-01'
             
             ORDER BY 
             ContactId
             , ActivityDate")

df1<-sqlQuery(odbcChannel,"SELECT  *
              FROM [PropensityScore].[dbo].[PropensityHistory] a
              where a.processdate >= '2016-03-01'
              ORDER BY ContactId, ProcessDate ")


odbcClose(odbcChannel)

#levels(df$ActivityType)
#Cleaning up ActivityType factor level names
levels(df$ActivityType)<-c("Admin.Call","Assisted.Roadshow","Business.Development","Call","Clicked.Email","Client.Services.Call","Conference.Call","Seminar","Seminar.Entertainment","EdForum","Email","HardMail","Inbound","Investor.University","VoiceMail","LitOrder","Marketing.Webinar","Meeting","Opened.Email","Outbound","Sale","Sales.Presentation","Selling.Agreement","Unassisted.Roadshow","Web.Presentation","WebHit")
#df$ActivityType <- as.character(df$ActivityType)

#df$ActivityType <- factor(df$ActivityType)

#Building Last 90 days quanitity with fun function and Sales in last 90,180 and 365 days
df$SalesAmount[is.na(df$SalesAmount)] <- 0
#library(data.table)

library(data.table)
setDT(df)
df<-df[!duplicated(df, by=c("ContactID","ActivityType","ActivityDate", "FundCode"))]
setDT(df1)
df[, ActivityDate := as.Date(ActivityDate, format="%Y-%m-%d")]
df1[, ProcessDate := as.Date(ProcessDate, format="%Y-%m-%d")]
df1[, rollDate:=ProcessDate] #Add a column, rollDate equal to ProcessDate
df[, rollDate:=ActivityDate] #Add a column, rollDate equal to ActivityDate

setkey(df1, "ContactID","rollDate")
setkey(df, "ContactID","rollDate")

df2<- df1[df, roll='nearest']
df3<- df2[,c("ContactID","SalesForce_Name","ActivityType","ActivityDate","Propensity_Score","EventID"),with=FALSE]

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")
sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[PropDescChartData]")


sqlSave(odbcChannel,df3,tablename = "PropDescChartData",rownames=FALSE,append=TRUE)
odbcClose(odbcChannel)
