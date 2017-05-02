#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Import packages and initialize  spark context
from pyspark import SparkContext
sc = SparkContext("local", "Benefit")
sc.setLogLevel("ERROR")
import pandas as pd

#Read Benefits file
benefitdf = pd.read_csv('/home/cloudera/BDM/BenefitsCostSharing.zip', compression ='zip')

#Filter required columns
filterCols = ['BenefitName', 'BusinessYear', 'IsCovered', 'PlanId', 'StandardComponentId', 'StateCode'] 

#Create dataframe
benefitdf = pd.DataFrame(benefitdf, columns=filterCols)

# Filter out benefit records which are Not Covered 
benefitdf = benefitdf[benefitdf.IsCovered == 'Covered']

#Taking out list of unique states
unique_states = benefitdf.StateCode.unique()
#Define file path
files_append = '/home/cloudera/BDM/Data/'
#Writing seperate record files for each state
for state in unique_states:
    dfstate = benefitdf[benefitdf.StateCode==state]
    outfile = files_append + 'Benefit-{}.csv'.format(state)
    dfstate.to_csv(outfile,sep='|', mode='w+')


#Define empty RDD for Benefit and Plan
benefitRDD = sc.emptyRDD()
planRDD = sc.emptyRDD()
#Define Read file path
read_files_append = 'file:///home/cloudera/BDM/Data/'
#Perform transfomration for each state file in loop
for state in unique_states:
    infile = read_files_append + 'Benefit-{}.csv'.format(state)    # Final input file path
    tempyrdd=(sc.textFile(infile)				   # Read input file and place in temporary RDD
               .filter(lambda l: not l.startswith('|'))		   # Filter Header
               .map(lambda l: l.split("|"))			   # Split records by Pipe '|'
               # Make a Key Value pair of ('BenefitName', 'BusinessYear',  'StateCode','StandardComponentId') as Key and 'PlanId' as Value
               .map(lambda l:((l[1].encode('utf-8'),l[2],l[6],l[5]),(l[4])))      
	       .groupByKey()							  
             )
    # Reduce by key to find count sum of Standard Component IDs for ('BenefitName','BusinessYear','StateCode’) combinations
    tempbenefitRDD=tempyrdd.map(lambda ((x1,x2,x3,x4),(y1)):((x1,x2,x3),(1))).reduceByKey(lambda x, y: x + y)
    # Reduce by key to find count sum of BenefitName for ('Standard Component IDs','BusinessYear','StateCode’) combinations
    tempplanRDD=tempyrdd.map(lambda ((x1,x2,x3,x4),(y1)):((x2,x3,x4),(1))).reduceByKey(lambda x, y: x + y)
    benefitRDD = sc.union([benefitRDD, tempbenefitRDD])			# Joining Benefit RDDs iteratively
    planRDD = sc.union([planRDD,tempplanRDD])				# Joining Plan RDDs iteratively

#Define functions for formatting the output RDD
def covtostr(tp):     
     if type(tp) is int:
	return str(tp)
     else:
	return ','.join(str(d) for d in tp)

	
def convertStr(data):
        return ','.join( covtostr(d) for d in data)
#Call functions for RDD format
lines1 = benefitRDD.map(convertStr)
lines2 = planRDD.map(convertStr)
#Write Output files 
lines1.saveAsTextFile('file:///home/cloudera/BDM/AllStateBnft.csv')
lines2.saveAsTextFile('file:///home/cloudera/BDM/AllStatePlan.csv')

