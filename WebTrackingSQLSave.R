

library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=proddb-fsphl-01;trusted_connection=true")
df<-sqlQuery(odbcChannel,"
SELECT 
      [CONTACT_ID]
      
      ,[VISITOR_KEY]
      ,[SESSION_KEY]
      ,[SESSION_START_TS]
     
      ,[EVENT_NAME]
     
      ,[EVENT_URL]
      
      ,[SITE_NAME]
      ,[SITE_URL]
  FROM [Silverpop].[dbo].[CrosswalkedWebTracking]")

odbcClose(odbcChannel)
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

sqlQuery(odbcChannel,"
truncate table
[DataScience].[dbo].[WebTracking]")


sqlSave(odbcChannel,df,tablename = "WebTracking",rownames=FALSE,append=TRUE)
odbcClose(odbcChannel)


