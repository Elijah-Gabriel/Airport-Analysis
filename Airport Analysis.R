#1. define the system variables inside RStudio#
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/hdp/2.3.0.0-2557/hadoop-mapreduce/hadoop-streaming-2.7.1.2.3.0.0-2557.jar")
options(max.print=99999) 

#2. unload & reload the rhdfs & rmr2 packages#
detach("package:rhdfs", unload=TRUE)
detach("package:rmr2", unload=TRUE)
library("rhdfs", lib.loc="/usr/lib64/Revo-7.4/R-3.1.3/lib64/R/library")
library("rmr2", lib.loc="/usr/lib64/Revo-7.4/R-3.1.3/lib64/R/library")
#3. re-initialize the connection in RStudio#

hdfs.init()

# Specify the path
hdfs.root = '/user/share/student'
# append the data filename to the pathname 
whole.hdfs.data = file.path(hdfs.root,'wholeEnchilada.csv')
# append the output filename to the pathname

taxi.hdfs.out=file.path(hdfs.root, "taxi")


Taximap = function(k,flights) 
{
  return(keyval(as.character(flights[[17]]), as.numeric(flights[[20]]) + as.numeric(flights[[21]])))
}
Reducetaxi = function(origin, Time) 
{
  Taximean = mean(Time,na.rm=TRUE)
  keyval(origin, Taximean)
}

Taxi1 = function(in1, out = NULL) 
{
  mapreduce(input = in1,
            output = out,
            input.format = make.input.format("csv", sep=","),
            map = Taximap,
            reduce = Reducetaxi)
}

out = Taxi1(whole.hdfs.data, taxi.hdfs.out)

Timeresults = from.dfs(out) 
Timeresults.df = as.data.frame(Timeresults, stringsAsFactors=F)
colnames(Timeresults.df) = c('Origin', 'Mean Taxi Time')
Timeresults.df 

sink('P1_output.txt')# redirect the output to a file named P1_output.txt
Timeresults.df # printout the dataframe to our output file
sink() # close the output file 

MinMaxMap = function(k,flights) 
{
  return (keyval(paste(as.character(flights[[9]]),as.character(flights[[17]])),as.integer(flights[[20]]) + as.integer(flights[[21]]))) 
}

ReduceTaxiMinMax = function(origin, Time) 

{
  MinTaxi=min(Time,na.rm=TRUE)
  MaxTaxi=max(Time,na.rm=TRUE)
  keyval(origin,cbind(MinTaxi, MaxTaxi))
  
}

TaxiMinMax = function(in1, out = NULL) 
{
  mapreduce(input = in1,
            output = out,
            input.format = make.input.format("csv", sep=","),
            map = MinMaxMap,
            reduce = ReduceTaxiMinMax)
}

out2 = TaxiMinMax(whole.hdfs.data, taxi.hdfs.out)

MinMaxresults = from.dfs(out2)
MinMaxresults.df = as.data.frame(MinMaxresults, stringsAsFactors=F)
colnames(MinMaxresults.df) = c('Arline/Origin', 'Min/Max_Taxi')
MinMaxresults.df

sink('P2_output.txt')# redirect the output to a file named P1_output.txt
MinMaxresults.df # printout the dataframe to our output file
sink() # close the output file 

