library("RODBC")
odbcChannel<- odbcDriverConnect("driver={SQL Server};server=DPHL-PROPSCORE;database=DataScience;trusted_connection=true")

# construct a query string with a parameter
query <- paste("exec  dbo.GenerateSFPropChartData");
# execute the query
sqlQuery(odbcChannel, query)


odbcClose(odbcChannel)