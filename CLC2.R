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



df[, ActivityDate := as.Date(ActivityDate, format="%Y-%m-%d")]
df[, FirstSaleDate := as.Date(FirstSaleDate, format="%Y-%m-%d")]
df[, MaxActivityDate:= max(ActivityDate),by=ContactID]
df[, MaxActivityDate := as.character(MaxActivityDate)]
df[, FirstSaleDate := as.character(FirstSaleDate)]
df[, MonthsTillFirstSale:= (as.Date(ActivityDate) - as.Date(FirstSaleDate))/30]

df$SalesAmount[is.na(df$SalesAmount)] <- 0
df[, Sales := cumsum(SalesAmount), by = ContactID]

setkey(df, ContactID, ActivityDate)
df[, Admin.Call := fun(ActivityType == "Admin.Call" , ActivityDate, 3000), by = ContactID]
df[, Assisted.Roadshow3000 := fun(ActivityType == "Assisted.Roadshow" , ActivityDate, 3000), by = ContactID]
df[, Business.Development3000 := fun(ActivityType == "Business.Development" , ActivityDate, 3000), by = ContactID]
df[, Call3000 := fun(ActivityType == "Call" , ActivityDate, 3000), by = ContactID]
df[, Clicked.Email3000 := fun(ActivityType == "Clicked.Email" , ActivityDate, 3000), by = ContactID]
df[, Client.Services.Call3000 := fun(ActivityType == "Client.Services.Call" , ActivityDate, 3000), by = ContactID]
df[, Conference.Call3000 := fun(ActivityType == "Conference.Call" , ActivityDate, 3000), by = ContactID]
df[, Seminar3000 := fun(ActivityType == "Seminar" , ActivityDate, 3000), by = ContactID]
df[, Seminar.Entertainment3000 := fun(ActivityType == "Seminar.Entertainment" , ActivityDate, 3000), by = ContactID]
df[, EdForum3000 := fun(ActivityType == "EdForum" , ActivityDate, 3000), by = ContactID]
df[, Email3000 := fun(ActivityType == "Email" , ActivityDate, 3000), by = ContactID]
df[, HardMail3000 := fun(ActivityType == "HardMail" , ActivityDate, 3000), by = ContactID]
df[, Inbound3000 := fun(ActivityType == "Inbound" , ActivityDate, 3000), by = ContactID]
df[, Investor.University3000 := fun(ActivityType == "Investor.University" , ActivityDate, 3000), by = ContactID]
df[, LitOrder3000 := fun(ActivityType == "LitOrder" , ActivityDate, 3000), by = ContactID]
df[, Marketing.Webinar3000 := fun(ActivityType == "Marketing.Webinar" , ActivityDate, 3000), by = ContactID]
df[, Meeting3000 := fun(ActivityType == "Meeting" , ActivityDate, 3000), by = ContactID]
df[, Opened.Email3000 := fun(ActivityType == "Opened.Email" , ActivityDate, 3000), by = ContactID]
df[, Outbound3000 := fun(ActivityType == "Outbound" , ActivityDate, 3000), by = ContactID]
df[, Sales.Presentation3000 := fun(ActivityType == "Sales.Presentation" , ActivityDate, 3000), by = ContactID]
df[, Unassisted.Roadshow3000 := fun(ActivityType == "Unassisted.Roadshow" , ActivityDate, 3000), by = ContactID]
df[, Web.Presentation3000 := fun(ActivityType == "Web.Presentation" , ActivityDate, 3000), by = ContactID]
df[, WebHit3000 := fun(ActivityType == "WebHit" , ActivityDate, 3000), by = ContactID]

df[, Outbound90 := fun(ActivityType == "Outbound" , ActivityDate, 90), by = ContactID]
df[, Marketing.Webinar60 := fun(ActivityType == "Marketing.Webinar" , ActivityDate, 60), by = ContactID]
df[, Sales.Presentation60 := fun(ActivityType == "Sales.Presentation" , ActivityDate, 60), by = ContactID]
df[, Assisted.Roadshow180 := fun(ActivityType == "Assisted.Roadshow" , ActivityDate, 180), by = ContactID]
df[, Unassisted.Roadshow180 := fun(ActivityType == "Unassisted.Roadshow" , ActivityDate, 180), by = ContactID]
df[, Investor.University180 := fun(ActivityType == "Investor.University" , ActivityDate, 180), by = ContactID]
df[, EdForum180 := fun(ActivityType == "EdForum" , ActivityDate, 180), by = ContactID]
df[, Meeting180 := fun(ActivityType == "Meeting" , ActivityDate, 180), by = ContactID]
df[, LitOrder60 := fun(ActivityType == "LitOrder" , ActivityDate, 60), by = ContactID]
df[, Meeting90 := fun(ActivityType == "Meeting" , ActivityDate, 90), by = ContactID]


df[, KIT := fun(AdditionalDetail=="KIT" , ActivityDate, 60), by = ContactID]
df[, PRO := fun(AdditionalDetail=="PRO" , ActivityDate, 60), by = ContactID]
df[, PR := fun(AdditionalDetail=="PR" , ActivityDate, 60), by = ContactID]
df[, EduCenter := fun(AdditionalDetail=="education-center" , ActivityDate, 35), by = ContactID]
df[, WebsiteWebinar := fun(AdditionalDetail=="webinars" , ActivityDate, 35), by = ContactID]
df[, OurFunds := fun(AdditionalDetail=="our-funds" , ActivityDate, 35), by = ContactID]
df[, Learn := fun(Link %like% 'fsinvestments.com/learn' , ActivityDate, 35), by = ContactID]
df[, Investments := fun(Link %like% 'fsinvestments.com/investments' , ActivityDate, 35), by = ContactID]


setkey(df,ContactID)
df[df[unique(df),,mult="last", which=TRUE], last:=1L]
df[df[unique(df),,mult="first", which=TRUE], first:=1L]


df[, EngagementLevel:= 
     ifelse(!is.na(last),"PresentDay",
            ifelse(!is.na(FirstSaleDate) & Sales>0 & MonthsTillFirstSale<=6 & Sales<=150000 ,"New Producer",
                   ifelse(!is.na(FirstSaleDate) & Sales>0 & MonthsTillFirstSale<=6 & Sales >150000 ,"Rising Star",
                          ifelse(!is.na(FirstSaleDate) & Sales>0 & MonthsTillFirstSale>6  & Sales <=350000,"Developing Producer",
                                 ifelse(!is.na(FirstSaleDate) & Sales>0 & MonthsTillFirstSale>20 & Sales >1000000,"Advocate",
                                        ifelse(!is.na(FirstSaleDate) & Sales>0 & MonthsTillFirstSale>6  & Sales >350000,"Core Producer",     
                                               
                                               ifelse(
                                                 (  (LitOrder60>0 & (KIT >0 | PRO>0 | PR>0) ) | EdForum180>0 | Meeting90>0  ), "Level5",
                                                 ifelse( 
                                                   (Assisted.Roadshow180>0| Unassisted.Roadshow180>0 | Investor.University180>0  ), "Level4",    
                                                   ifelse( 
                                                     (Sales.Presentation60>0| Marketing.Webinar60>0), "Level3",         
                                                     ifelse(
                                                       (EduCenter>0| WebsiteWebinar>0 | OurFunds>0 | Learn>0 | Investments>0 | Outbound90>0 ), "Level2",        
                                                       ifelse(
                                                         (Outbound3000>0| Marketing.Webinar3000>0|Sales.Presentation3000>0|Assisted.Roadshow3000>0|Unassisted.Roadshow3000>0|Investor.University3000>0|EdForum3000>0|Meeting3000>0|LitOrder3000>0|
                                                            WebHit3000>0| Clicked.Email3000>0 | Opened.Email3000>0 | Conference.Call3000>0 | Client.Services.Call3000>0 |Seminar3000>0|Seminar.Entertainment3000>0|Inbound3000>0|Admin.Call>0 | Call3000>0|Business.Development3000>0), 
                                                         "Level1",  "Level0"      
                                                         
                                                       )))))))))))]    


df[,RecentLevelChange := as.character(ifelse(shift(EngagementLevel,1)==EngagementLevel  ,NA,paste(shift(EngagementLevel,1),EngagementLevel,sep="-->"))),by=ContactID]

df[,FullLevelCycle := vapply(seq_len(.N), function(i) 
  paste(unique(EngagementLevel[1:i]), collapse="-->"), character(1)) , by = ContactID]

spell = df[,{.(
  w    = .I[1L],
  ActivityDate = ActivityDate[1L]
)}, by=.(ContactID, rleid(EngagementLevel))][, .(
  w = tail(w,-1), 
  d = diff(ActivityDate)
), by=ContactID]

df[spell$w, DurationLastLevel := spell$d]

df[, ActivityType:= as.character(ifelse(!is.na(first),"JourneyBegins",ifelse(!is.na(last),"Present",as.character(ActivityType))))]

df[, EngagementLevel1:= ifelse(EngagementLevel=="PresentDay",shift(EngagementLevel,1),EngagementLevel)]
df2<-df[,c("ContactID","SalesForce_Name","ActivityType", "ActivityDate", "EngagementLevel1","DurationLastLevel","RecentLevelChange","FullLevelCycle","Type"),with=FALSE][!is.na(RecentLevelChange)]
df3<-df[, .SD[.N], by=ContactID][,c("ContactID","SalesForce_Name","EngagementLevel1","DurationLastLevel","FullLevelCycle"),with=FALSE]
CLCFull<-df[,c("ContactID","SalesForce_Name","ActivityType", "ActivityDate", "EngagementLevel1","DurationLastLevel","RecentLevelChange","FullLevelCycle","Type"),with=FALSE]
#write.csv(df2, "C:/Users/ssizan/Documents/SQL_CSV/FullLifeCycle.csv",row.names = FALSE)
#Put it into sql server----------------------------------------------------------------------------------------------
library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[CurrentAdvisorStatus]")

sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[FullLifeCycle]")

sqlQuery(odbcChannel,"
         truncate table
         [DataScience].[dbo].[CLCFull]")

sqlSave(odbcChannel,df3,tablename = "CurrentAdvisorStatus",rownames=FALSE,append=TRUE)
sqlSave(odbcChannel,df2,tablename = "FullLifeCycle",rownames=FALSE,append = TRUE)
sqlSave(odbcChannel,CLCFull,tablename = "CLCFull",rownames=FALSE,append = TRUE)
odbcClose(odbcChannel)
#Writing csv file

write.csv(df3, file=paste("C:/Users/ssizan/Documents/PropensityHistory/LifeCycle ",Sys.Date(),".csv",sep=""),row.names = FALSE)



#df4<- df2[ActivityDate > "2015-12-31"  ][!(RecentLevelChange %like% 'PresentDay')][RecentLevelChange %like% 'Level']