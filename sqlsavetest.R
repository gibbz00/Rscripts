library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

df4<-sqlQuery(odbcChannel,"SELECT TOP 1000 [ContactID]
              ,[SalesForce_Name]
              ,[EngagementLevel1]
              ,[DurationLastLevel]
              ,[FullLevelCycle]
              FROM [DataScience].[dbo].[CurrentAdvisorStatus]")

sqlSave(odbcChannel,df1,tablename = "NextStepsForAdvisor",rownames=FALSE,append=FALSE)
odbcClose(odbcChannel)

