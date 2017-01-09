
library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")
df<-sqlQuery(odbcChannel," 
SET NOCOUNT ON;  
    
             
             IF OBJECT_ID('tempdb..#PropDescLimit') IS NOT NULL
             DROP TABLE #PropDescLimit
             
             SELECT 
             [ContactID]
             ,[SalesForce_Name]
             ,[ActivityType]
             ,[ActivityDate]
             ,[Propensity_Score]
             ,[EventID]
             , ROW_NUMBER() OVER (PARTITION BY [ContactID] ORDER BY [ActivityDate] desc) AS [Rows]
             INTO #PropDescLimit
             FROM [DataScience].[dbo].[PropDescChartData]
             ORDER BY [ContactID], [ActivityDate] asc
             
             SET NOCOUNT OFF;
             SELECT
             [ContactID]
             ,[SalesForce_Name]
             ,[ActivityType]
             ,[ActivityDate]
             ,[Propensity_Score]
             ,[EventID]
             
            
             
             FROM #PropDescLimit
             WHERE [Rows] <= 15 AND [Propensity_Score] IS NOT NULL
             ORDER BY [ContactID], [ActivityDate] asc
")
odbcClose(odbcChannel)
library(data.table)
setDT(df)
#Writing csv file
write.csv(df, "//SC12-FSPHL-01/PropensityScore/PropensityEvents.csv",row.names = FALSE)
