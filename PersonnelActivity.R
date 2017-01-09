library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=proddb-fsphl-01;trusted_connection=true")

df4<-sqlQuery(odbcChannel,"
Select 
                   u.Name as PersonnelName
             , t.Activity_Type_Copy__c as 'ActivityType'
             , terr.Name as TerritoryName
             --, count( Distinct (t.id)) as '# of Calls / Activities'
             ,c.Id as ContactID
             ,c.Name as SalesForce_Name
             ,t.ActivityDate
             
             
             
             FROM [SalesForce Backups].dbo.Task t
             
             Left Join [SalesForce Backups].dbo.Contact c 
             ON t.WhoId = c.Id
             
             LEFT JOIN [SalesForce Backups].dbo.[User] u
             ON t.OwnerId = u.Id
             
             LEFT JOIN [SalesForce Backups].dbo.Territory__c terr
             ON u.Id = terr.Internal_Wholesaler__c OR u.Id = terr.Sales_Associate__c
             
             
             Where t.ActivityDate > '12/31/2014' AND 
             t.Activity_Type_Copy__c IS NOT NULL AND t.Activity_Type_Copy__c IN ('Outbound','Left Voicemail/Message with Assistant','Sales Presentation','Meeting') and 
             t.Assigned_Profile__c  in ('IBD ISC','RIA ISC','IBD SA','RIA SA') AND 
             t.Task_Completed_At__c is not null
             and u.IsActive = 'True'
             AND terr.Name is not null
             
             GROUP BY 
             u.Name
             , terr.Name
             
             , t.Activity_Type_Copy__c
             ,t.ActivityDate
             ,c.id
             ,c.name
             
             UNION ALL
             
             SELECT 
             u.Name as PersonnelName
             ,e.[type] as ActivityType
             ,c.Territory__c
             ,er.RelationId AS ContactID
             , c.Name as SalesForce_Name
             , e.ActivityDate
             FROM
             [SalesForce Backups].dbo.[Event] e
             INNER JOIN
             [SalesForce Backups].dbo.EventRelation er
             ON
             e.ID = er.EventID
             INNER JOIN
             [SalesForce Backups].dbo.Contact c
             ON
             er.RelationId = c.ID
             
             LEFT JOIN [SalesForce Backups].dbo.[User] u
             ON e.OwnerId = u.Id
             
             --LEFT JOIN [SalesForce Backups].dbo.Territory__c terr
             --                ON u.Id = terr.Internal_Wholesaler__c OR u.Id = terr.Sales_Associate__c or u.id = terr.Territory_Manager__c or u.id = terr.Division_Manager__c or u.id= terr.Regional_Sales_Director_1__c or u.id=terr.Regional_Sales_Director_2__c or u.id=terr.Active__c 
             
             WHERE
             e.[Type] = 'Meeting' 
             AND u.IsActive = 'True'
             AND c.Territory__c is not null
             AND e.ActivityDate > '12/31/2014'
             group by
             
             u.Name
             ,e.[type] 
             , c.Territory__c
             ,er.RelationId 
             , c.Name
             , e.ActivityDate
             
             ORDER BY
             terr.Name desc,
             t.ActivityDate
             
             
           ")
odbcClose(odbcChannel)

odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

df<-sqlQuery(odbcChannel,"
SELECT  [ContactID]
,[SalesForce_Name]
,[ActivityType]
,[ActivityDate]
,[EngagementLevel1]
,[DurationLastLevel]
,[RecentLevelChange]
,[FullLevelCycle]
,[Type]
FROM [DataScience].[dbo].[CLCFull]
")

odbcClose(odbcChannel)


library(data.table)
setDT(df4)
levels(df4$ActivityType)<-c("VoiceMail","Meeting","Outbound","Sales.Presentation")
#levels(df4$ActivityType)                          
df4[, ActivityDate := as.Date(ActivityDate, format="%Y-%m-%d")]

setDT(df)
df[, ActivityDate := as.Date(ActivityDate, format="%Y-%m-%d")]

setkey(df, ContactID, ActivityDate)
df[, EngagementLevel1 := as.character(EngagementLevel1)]
df[, PriorLevel:= ifelse(is.na(shift(EngagementLevel1,1)),EngagementLevel1,shift(EngagementLevel1,1)),by=ContactID]

funfuture<-function(x, date, thresh) {
  D <- as.matrix(dist(date)) #distance matrix between dates
  D <- D <= thresh
  D[upper.tri(D)] <- FALSE #don't sum to the past
  R <- D * x #FALSE is treated as 0
  colSums(R)
}

setkey(df, ContactID, ActivityDate)
df[, Sales.Presentation_F := funfuture(ActivityType == "Sales.Presentation" , ActivityDate, 60), by = ContactID]
df[, Tickets_F := funfuture(ActivityType == "Sale"  , ActivityDate, 35), by = ContactID]
df[, Meeting_F := funfuture(ActivityType == "Meeting" , ActivityDate, 35), by = ContactID]



setkey(df4, ContactID, SalesForce_Name, ActivityDate,ActivityType)
setkey(df, ContactID,SalesForce_Name, ActivityDate, ActivityType)


df5<-df[df4][,c(14,15,1,2,3,4,10,11,12,13),with=FALSE]

df5[, Success := ifelse((ActivityType == "VoiceMail" | ActivityType=="Outbound") & (Sales.Presentation_F>0 | Tickets_F>0 | Meeting_F>0) ,1,
                 ifelse(ActivityType=="Sales.Presentation" & (Meeting_F>0 | Tickets_F>0),1,       
                 ifelse(ActivityType  == "Meeting"   & Tickets_F>0,1,0)))]
                        
                        
#df6<-df[,c(1,2,3,4,47,44),with=FALSE]




#df6[,FullActivityCycle := vapply(seq_len(.N), function(i) 
  #paste(unique(ActivityType[1:i]), collapse="-->"), character(1)) , by = ContactID]

#df7<-df6[!is.na(RecentLevelChange)]
#rm(df7)
#write.csv(df7, "C:/Users/ssizan/Documents/SequenceToNewProducer.csv",row.names = FALSE)

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

sqlQuery(odbcChannel,"
truncate table
         [DataScience].[dbo].SalesPersonnelActivity")

sqlSave(odbcChannel,df5,tablename = "SalesPersonnelActivity",rownames=FALSE,append=TRUE)

odbcClose(odbcChannel)
