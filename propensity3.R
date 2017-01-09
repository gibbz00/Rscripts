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
             
             UNION ALL
             
             SELECT
             ContactID
             , SalesForce_Name
             , NULL AS [ActivityType]
             , CONVERT(DATE, GETDATE()) AS [ActivtyDate]
             , NULL [SalesAmount]
             , '' [FundCode]
             , GDC
             , AUM
             , [X.ToAdvisory]
             , [X.ToAlternatives]
             , Age
             , case when a.advisortype ='Broker Dealer' then 1 else 0 end as AdvisorType
             , '' [AdditionalDetail]
           
             FROM 
             [PropensityScore].[dbo].[PropensityInput] a
             WHERE
             a.ActivityType NOT IN ('DD Kit Mailing', 'To-Do', 'Voicemail','BD Event','Bond Deal')
             AND YEAR(a.ActivityDate) > 2011
             GROUP BY 
             ContactID
             , SalesForce_Name
             , GDC
             , AUM
             , [X.ToAdvisory]
             , [X.ToAlternatives]
             , Age
             ,AdvisorType
           
             
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
df<-df[!duplicated(df, by=c("ContactID","ActivityType","ActivityDate", "FundCode","AdditionalDetail"))]
w = df[ActivityType == "Sale", .I[1L], by = .(ContactID, FundCode)]$V1 #UniqueFundsSale
df[, UniqueFundsSale := cumsum(.I %in% w), by = ContactID]
df[, UniqueFunds :=cumsum(!duplicated(FundCode)& !FundCode=="" ), by = ContactID] #UniqueFunds
df[, ActivityDate := as.Date(ActivityDate, format="%Y-%m-%d")]
df<- df[ActivityDate > "2013-12-31"]
setkey(df, ContactID, ActivityDate)
df[, RT365 := fun(SalesAmount, ActivityDate, 365), by = ContactID]
df[, RT180 := fun(SalesAmount, ActivityDate, 180), by = ContactID]
df[, RT90 := fun(SalesAmount, ActivityDate, 90), by = ContactID]

df[, Tickets90 := fun(ActivityType == "Sale" & SalesAmount>0 , ActivityDate, 90), by = ContactID]

df[, Selling.Agreement90 := fun(ActivityType == "Selling.Agreement" , ActivityDate, 90), by = ContactID]
df[, Admin.Call90 := fun(ActivityType == "Admin.Call" , ActivityDate, 90), by = ContactID]
df[, Assisted.Roadshow90 := fun(ActivityType == "Assisted.Roadshow" , ActivityDate, 90), by = ContactID]
df[, Business.Development90 := fun(ActivityType == "Business.Development" , ActivityDate, 90), by = ContactID]
df[, Call90 := fun(ActivityType == "Call" , ActivityDate, 90), by = ContactID]
df[, Clicked.Email90 := fun(ActivityType == "Clicked.Email" , ActivityDate, 90), by = ContactID]
df[, Client.Services.Call90 := fun(ActivityType == "Client.Services.Call" , ActivityDate, 90), by = ContactID]
df[, Conference.Call90 := fun(ActivityType == "Conference.Call" , ActivityDate, 90), by = ContactID]
df[, Seminar90 := fun(ActivityType == "Seminar" , ActivityDate, 90), by = ContactID]
df[, Seminar.Entertainment90 := fun(ActivityType == "Seminar.Entertainment" , ActivityDate, 90), by = ContactID]
df[, EdForum90 := fun(ActivityType == "EdForum" , ActivityDate, 90), by = ContactID]
df[, Email90 := fun(ActivityType == "Email" , ActivityDate, 90), by = ContactID]
df[, HardMail90 := fun(ActivityType == "HardMail" , ActivityDate, 90), by = ContactID]
df[, Inbound90 := fun(ActivityType == "Inbound" , ActivityDate, 90), by = ContactID]
df[, Investor.University90 := fun(ActivityType == "Investor.University" , ActivityDate, 90), by = ContactID]
df[, VoiceMail90 := fun(ActivityType == "VoiceMail" , ActivityDate, 90), by = ContactID]
df[, LitOrder90 := fun(ActivityType == "LitOrder" , ActivityDate, 90), by = ContactID]
df[, Marketing.Webinar90 := fun(ActivityType == "Marketing.Webinar" , ActivityDate, 90), by = ContactID]
df[, Meeting90 := fun(ActivityType == "Meeting" , ActivityDate, 90), by = ContactID]
df[, Opened.Email90 := fun(ActivityType == "Opened.Email" , ActivityDate, 90), by = ContactID]
df[, Outbound90 := fun(ActivityType == "Outbound" , ActivityDate, 90), by = ContactID]
df[, Sales.Presentation90 := fun(ActivityType == "Sales.Presentation" , ActivityDate, 90), by = ContactID]
df[, Unassisted.Roadshow90 := fun(ActivityType == "Unassisted.Roadshow" , ActivityDate, 90), by = ContactID]
df[, Web.Presentation90 := fun(ActivityType == "Web.Presentation" , ActivityDate, 90), by = ContactID]
df[, WebHit90 := fun(ActivityType == "WebHit" , ActivityDate, 90), by = ContactID]

require(zoo)
require(dplyr)
#Computing Days Till Last Sale2
df[, ActivityDate := as.character(ActivityDate)]
df[, LastSaleDate:=as.character(na.locf(lag(ifelse(ActivityType=="Sale",ActivityDate,NA)),na.rm=FALSE)),by = ContactID]
df[, DaysTillLastSale:= as.Date(ActivityDate) - as.Date(LastSaleDate)]
df[, DaysTillLastSale:= ifelse(is.na(DaysTillLastSale),-999,DaysTillLastSale)]
df[, LastSaleDate:= NULL]

#Build Sale in 35 days

setDT(df)
df[, ActivityDate := as.character(ActivityDate)]
df[, NextSaleDate:=as.character(na.locf(lead(ifelse(ActivityType=="Sale",ActivityDate,NA)),na.rm=FALSE,fromLast=TRUE)),by = ContactID]
df[, DaysTillNextSale:= as.Date(NextSaleDate)-as.Date(ActivityDate)]
df[, SaleIn35:= ifelse(DaysTillNextSale>35|is.na(DaysTillNextSale),0,1)]
df[, NextSaleDate:= NULL]
df[, DaysTillNextSale:= NULL]
df[, ActivityDate := as.Date(ActivityDate, format="%Y-%m-%d")]

df1<- df[ActivityDate > "2014-12-31" & ActivityDate < (Sys.Date()-35) ] #getting original propensityinput table data ready for training

#Take out duplicate ContactID and ActivityDate Rows
final<-df1[!duplicated(df1, by=c("ContactID","ActivityDate"), fromLast=TRUE)]
#final<- df[,7:44, with=FALSE]
final<- final[,c(7:12,14:46),with=FALSE]    #PAY ATTENTION TO COLUMN NUMBERE HERE
#Imputing demo values
final$X.ToAlternatives[is.na(final$X.ToAlternatives)|final$X.ToAlternatives<=0]<- median(final$X.ToAlternatives,na.rm=T)
final$X.ToAdvisory[is.na(final$X.ToAdvisory)|final$X.ToAdvisory<=0]<- median(final$X.ToAdvisory,na.rm=T)
final$GDC[is.na(final$GDC)|final$GDC<=0]<- median(final$GDC,na.rm=T)
final$Age[is.na(final$Age)|final$Age<=0]<- median(final$Age,na.rm=T)
final$AUM[is.na(final$AUM)|final$AUM<=0]<- median(final$AUM,na.rm=T)

# load in the training data
df_train<- final

library(xgboost)

# xgboost fitting with arbitrary parameters
xgb_params_1 = list(
  objective = "binary:logistic",                                               # binary classification
  eta = 0.001,                                                                  # learning rate
  max.depth = 11,                                                               # max tree depth
  subsample = .9,                                                              #add randomness
  eval_metric = "auc"                                                          # evaluation/loss metric
)


# fit the model with the arbitrary parameters specified above
xgb_1 = xgboost(data = as.matrix(df_train %>%
                                   select(-SaleIn35)),
                label = df_train$SaleIn35,
                params = xgb_params_1,
                nrounds = 10000,                                                 # max number of trees to build
                verbose = TRUE,                                         
                print.every.n = 1,
                #early.stop.round = 5                                          # stop if no improvement within 5 trees
)

#Predictions
predictorNames <- names(df_train)[names(df_train) != 'SaleIn35'] 
ContactIDs<-df[,c(1:2),with=FALSE][, .SD[.N], by=ContactID]

#Imputing demo values
df$X.ToAlternatives[is.na(df$X.ToAlternatives)|df$X.ToAlternatives<=0]<- median(df$X.ToAlternatives,na.rm=T)
df$X.ToAdvisory[is.na(df$X.ToAdvisory)|df$X.ToAdvisory<=0]<- median(df$X.ToAdvisory,na.rm=T)
df$GDC[is.na(df$GDC)|df$GDC<=0]<- median(df$GDC,na.rm=T)
df$Age[is.na(df$Age)|df$Age<=0]<- median(df$Age,na.rm=T)
df$AUM[is.na(df$AUM)|df$AUM<=0]<- median(df$AUM,na.rm=T)

#Get last rows of original df
FeedModelLastRows<- df[, .SD[.N], by=ContactID][,c(7:12,14:45),with=FALSE]  #pay attention to the column numbers here

#predit probabilities
predictions <- predict(xgb_1, as.matrix(FeedModelLastRows[,predictorNames,with=FALSE]), outputmargin=FALSE)

#Combine ContactIDs and Predictions
FeedSF<-cbind(ContactIDs,predictions)

#Writing csv file
write.csv(FeedSF, "//SC12-FSPHL-01/PropensityScore/PropensityScore.csv",row.names = FALSE)
write.csv(FeedSF, file=paste("C:/Users/ssizan/Documents/PropensityHistory/PropensityScore ",Sys.Date(),".csv",sep=""),row.names = FALSE)
#save(list = ls(all.names = TRUE), file = ".RData", envir = .GlobalEnv)


#Propensity Delta Calculations
library("RODBC")

odbcChannel<- odbcDriverConnect("driver={SQL Server};server=proddb-fsphl-01;trusted_connection=true")

MasterContacts<-sqlQuery(odbcChannel,"SELECT
             c.Id as ContactID
             , c.Name
             , c.Propensity_to_Buy_7_Days_Ago__c
             FROM [SalesForce Backups].[dbo].[Contact] c 
             
             WHERE 
             (c.Rating__c NOT IN ('O', 'X') OR c.Rating__c IS NULL)
             AND c.Approved_Active_Funds__c = 'y'
             AND c.DST_Contact_Type__c = 'Single'
             
             ")

odbcClose(odbcChannel)
library(data.table)
setDT(MasterContacts)
setDT(FeedSF)
setkey(MasterContacts, ContactID)
setkey(FeedSF, ContactID)

MasterContacts<-FeedSF[MasterContacts]

MasterContacts[, PropensityDeltaPercent := (predictions-(Propensity_to_Buy_7_Days_Ago__c/100))/(Propensity_to_Buy_7_Days_Ago__c/100)]

df2<- df[ActivityDate > (Sys.Date()-21) ] [,c(1:4,13),with=FALSE] [ActivityType %in% c("Selling.Agreement","Assisted.Roadshow","Business.Development","Client.Services.Call","Conference.Call","Seminar","Edforum","Investor.University","LitOrder","Marketing.Webinar","Opened.Email","Unassisted.Roadshow","WebHit")]
df2<-df2[!duplicated(df2, by=c("ContactID","ActivityType","ActivityDate", "AdditionalDetail"))]

df2[, ActivityType := as.character(ActivityType)]
#df2[, ActivityType := ifelse(ActivityType == "Nothing" , " ", ActivityType)]
df2[, Index := rep(1:.N, each=1, length.out=.N),by=ContactID]
setDT(df2)

df3<-as.data.frame(x=df2[,{
  
  subdat = .(
    OpeningBracket= as.character("{"),
    ActivityType = c( as.character(ActivityType)),
    ActivityDate = c( as.character(ActivityDate)),
    AdditionalDetail = c(as.character( AdditionalDetail)),
    ClosingBracket= as.character("}")
  )
  .( sprintf("%s %-21s %-15s %s %s", subdat$OpeningBracket,subdat$ActivityType, subdat$ActivityDate,subdat$AdditionalDetail,subdat$ClosingBracket))
}, by=ContactID], nrows=Inf, row.names=FALSE)

setDT(df3)
df3[, Index := rep(1:.N, each=1, length.out=.N),by=ContactID]
df4<-df3[, .(MaxIndex = max(Index), PropDesc = paste(V1, collapse="\n")),
         by = .(ContactID)]

df4<-df4[,c("ContactID","PropDesc"),with=FALSE]
setkey(df4, ContactID)
propdelta<- df4[MasterContacts]
propdelta<-propdelta[,c("ContactID","SalesForce_Name","predictions","PropensityDeltaPercent","PropDesc"),with=FALSE]

write.csv(propdelta, file=paste("C:/Users/ssizan/Documents/PropensityHistory/PropDelta ",Sys.Date(),".csv",sep=""),row.names = FALSE)


