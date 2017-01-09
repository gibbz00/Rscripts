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
             , a.GDC
             , a.AUM
             , a.[X.ToAdvisory]
             , a.[X.ToAlternatives]
             , a.Age
             , case when a.advisortype ='Broker Dealer' then 1 else 0 end as AdvisorType
             ,AdditionalDetail
             FROM 
             [PropensityScore].[dbo].[PropensityInput] a
             LEFT JOIN
             [PropensityScore].[dbo].[FundCodes] b
             ON
             b.[FundCode] = a.[FundCode]
             WHERE 
             a.ActivityType NOT IN ('DD Kit Mailing', 'To-Do', 'Voicemail','BD Event','Bond Deal') 
             AND YEAR(a.ActivityDate) > 2011
             
            
             
             
             ORDER BY 
             ContactId
             , ActivityDate")

odbcClose(odbcChannel)

#levels(df$ActivityType)
#Cleaning up ActivityType factor level names
levels(df$ActivityType)<-c("Admin.Call","Assisted.Roadshow","Business.Development","Call","Clicked.Email","Client.Services.Call","Conference.Call","Seminar","Seminar.Entertainment","EdForum","Email","HardMail","Inbound","Investor.University","VoiceMail","LitOrder","Marketing.Webinar","Meeting","Opened.Email","Outbound","Sale","Sales.Presentation","Selling.Agreement","Unassisted.Roadshow","Web.Presentation","WebHit")
df$ActivityType <- as.character(df$ActivityType)
df$ActivityType[is.na(df$ActivityType)]<- "Nothing"
df$ActivityType <- factor(df$ActivityType)

#Building Last 90 days quanitity with fun function and Sales in last 90,180 and 365 days
df$SalesAmount[is.na(df$SalesAmount)] <- 0
#library(data.table)

library(data.table)
setDT(df)
setkey(df, ContactID, ActivityDate)
df<-df[!duplicated(df, by=c("ContactID","ActivityType","ActivityDate", "FundCode","AdditionalDetail"))]

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")



sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[PropensityInput]")

sqlSave(odbcChannel,df,tablename = "PropensityInput",rownames=FALSE,append=TRUE)

odbcClose(odbcChannel)

