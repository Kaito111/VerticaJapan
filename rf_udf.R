#******************************************************************************************
##########       Random Forest UDT
#  Builds and saves a randomForest model
#  Only works for classification and turns the dependent variable into a factor variable
#  Requires first column to the dependent variable
#  Requires all other columns to be the independent variables
#  Outputs a one row and one column message saying it is done
#  Takes 3 parameters: 
#  append_date (boolean indicating if wether or not to add the sys.date to the model name)
#  model_folder (e.g. /home/dbadmin/randomforest)
#  model_name (e.g. mymodel)
#*****************************************************************************************


rf_build_udf=function(data, params){

	library("randomForest")

	x <- data

	#set location and name for stored model
	add_date <- params[['append_date']]
	mf <- params[['model_folder']]
	mn <- params[['model_name']]

	#add_timestamp is a boolean to prefix the time to the model name
	ifelse(add_date, mn <- paste(mn, Sys.time(), sep = "-"), mn <- mn)
	mn <- gsub(":","-",mn)
	mn <- gsub(" ", "-", mn)
	mn <- paste(mn, "rda", sep = ".")
	m_complete <- paste(mf, mn, sep="/")

	#clear bad rows
	#x <- x[complete.cases(x),]

	#ensure dependent varialbe is of type factor
	x[,1] <- as.factor(x[,1])

	#build the formula
	rf.independent <- names(x[,-1])
	rf.dependent <- names(x[1])
	rf.form <- paste(rf.independent, collapse = "+")
	rf.form <- as.formula(paste(rf.dependent, rf.form, sep = " ~ "))

	#build the model
	rf.model <- randomForest(rf.form,x,ntree=20)

	#save the model
	save(rf.model, file = m_complete)

	msg <- as.data.frame(paste("model created as ", m_complete))
	msg
}

rf_build_factory <- function()
{
  list(name=rf_build_udf,
      udxtype=c("transform"),
      intype=c("any"),
      outtype=c("varchar(1000)"), 
      outnames=c("msg"),
	  parametertypecallback=build_tokenizerParameters	
	  #details of the parameters that comes from vertica
  
  )
}

#The Parameter Function
build_tokenizerParameters <- function() {
  #This function contains all the details of the parameters coming in from the 'USING PARAMETERS' clause
  #This is required if the function has as USING PARAMETERS clause
  
  data.frame(datatype = c("int", "varchar", "varchar"),
                     length   = c(NA, 500, 500),
                     scale    = c(NA, NA, NA),
                     name     = c("append_date","model_folder","model_name"))
  
}



#******************************************************************************************
##########       Random Forest UDT
#  Scores a saved rf model on a dataset
#  First column must be a key field so we can join back up with other data later
#  If an error occurs ensure the model exists and the 
#  dataset has the proper column names to match the existing model.
#  Takes one parameter, model, which is the complete path and name of the saved model
#*****************************************************************************************



rf_score_udf=function(data, params){

library("randomForest")

x <- data
model <- params[['model']]

#load the model
load(model)

#build result set
colnames(x)[1] <- "id"
x$pred <- predict(rf.model, x, type="response")
x$maxprob <- apply(predict(rf.model, x, type="prob"), 1, max)
x <- x[,c("id","pred","maxprob")]
x
}

rf_score_factory <- function()
{
  list(name=rf_score_udf,
      udxtype=c("transform"),
      intype=c("any"),
      outtype=c("varchar","varchar","float"), 
      outnames=c("id","pred","maxprob"),
	  parametertypecallback=score_tokenizerParameters	#details of the parameters that comes from vertica
  )
}

#The Parameter Function
score_tokenizerParameters <- function() {
  #This function contains all the details of the parameters coming in from the 'USING PARAMETERS' clause
  #This is required if the function has as USING PARAMETERS clause
  
  data.frame(datatype = c("varchar"),
	length   = c(1000),
    scale    = c(NA),
    name     = c("model"))
  
}
