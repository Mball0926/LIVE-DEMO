#Import your libraries for the tools that you will be using for program

import numpy as np
import pandas as pdsleep
import random
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
import json
import re
from pandas import Series, DataFrame
from pandas import Categorical
import pandas as pd
import datetime as dt
import multiprocessing
from multiprocessing import Pool
from contextlib import closing
from random import randint
from time import sleep
import os
from scipy.io import loadmat

#Send out request and retry if request are unsuccessful 
y=1
while y!=2:
	r=requests.get('http://ifcb-data.whoi.edu/mvco/api/feed/temperature/start/2016-06-01/end/2016-07-25')
	#check for status return code 200 to see if request.get is successful. 
	if r.status_code==200:
		y=2
	else:
		y=1
	
feed=r.json()
#Integrate data into Global Dataframe and drop unnessessary data
fd=pd.DataFrame(feed)
Py=pd.to_datetime(fd.date)
fd=fd.drop('temperature',1)
#Grab pid so that when loading the mat file, you can align data based on pid and merge the data
fd['pid'] = fd['pid'].map(lambda x: x.lstrip('http://ifcb-data.whoi.edu/mvco/').rstrip('w'))
k=loadmat('ml_analyzed_all.mat',squeeze_me=True)
D=list(k['filelist_all'])
V=list(k['ml_analyzed'])
gd=pd.DataFrame(D)
gd.columns = ['pid']
gd["Volume"]=V
result = pd.merge(fd, gd, how='left')
#add csv file extension back on since merge is complete so that you can read the pid to get binned data
result['pid']='http://ifcb-data.whoi.edu/mvco/'+result['pid']+'_class_scores.csv'
#fd['pid']=fd['pid']+'_class_scores.csv'
NUM=len(result.index)




#function grabs PID from dataframe
def get_pid(result,arg):
	
	k=arg
	url=result['pid'][k]
	return url
	
#function produces abundance counts of phytoplankton and stores them in individually generated csv files
def do_counts(args):
	arg, filename = args
	
	k=arg
	#call function to grab URL
	URL=get_pid(result,arg)
		
	try:
	#read data from URL into dataframe
		dataf=pd.read_csv(URL,index_col='pid')
	except:
	#if reading URL fails do nothing because this means URL data does not exist
		return None
	#find maximum values for each column(e.g Phytoplankton classes) and divide by volume of water from IFCB inflow for 
	#population abundance
	
	abundance = dataf.idxmax(axis=1)
	Try=pd.Categorical(abundance,categories=dataf.columns)
	Abundance=Try.value_counts().to_frame().T.sort_index(axis=1)
	Abundance.index=[fd['date'][arg]]
	#if no water volume available return nothing and store in csv file
	if result['Volume'][arg] is None:
		Abundance=None
		Abundance.to_csv(filename, index=True)
	#if there is volume data, divide for population abundance and store in csv file
	else:
		Abundance=Abundance/result['Volume'][arg]
		Abundance.to_csv(filename, index=True)

		
		#request okay
	


if __name__ == '__main__':
	
	
	
	#from multiprocessing import Pool to utilize batch processing
	pool = Pool()
	arg = range(NUM)
	#call do_counts function and generate file with batch processing
	filename = ['Abundance%1d.csv'% a for a in arg]
	pool.map(do_counts, zip(arg,filename))
	#let proccesses sleep first so that all process dont call for the web service all at once 
	sleep(random.uniform(0.0,0.10))

	
	#Now open and create a file that will be used in the end to hold all data from all individually created files earlier
	fout=open("Population_Abundance.csv","a")
	#first file:
	for line in open("Abundance0.csv"):
			fout.write(line)
	#now the rest:    
	for num in range(1,NUM):
		if os.path.exists("Abundance"+str(num)+".csv"):
			f = open("Abundance"+str(num)+".csv")
			f.next() # skip the header
			for line in f:
				fout.write(line)
				
				
			
		else:
			None
	fout.close()
