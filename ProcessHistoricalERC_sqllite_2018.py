#-------------------------------------------------------------------------------
# Name:        ProcessHistoricalERC.py
# Purpose:     Process the historical ERC MAX and AVG and save them into a database
#              (now PostgreSQL, can be extended to other database such SQL Server or ACCESS)
# Author:      pyang
# Created:     02/12/2015
# Copyright:   (c) pyang 2015
#-------------------------------------------------------------------------------
import os
import pandas
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import MonthLocator, WeekdayLocator, DateFormatter
import datetime
import csv
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, MetaData
import pandas.io.sql as psql
import numpy as np
import sqlite3


##SQL for appending the 2016 year data to the time series table in database
'''
INSERT INTO NTXS_ERC_ALLYEAR
SELECT "index", NTXS FROM "PSA_ERC_AVG"
'''

##def getHistERC(d):
##    dailyERC = []
##
##    date_lastyear = datetime.datetime.date(d) - relativedelta(years=1)
##    strDate_last = date_lastyear.strftime("%m/%d/%Y")
##    print strDate_last
##    while (ercdict_20012016.get(strDate_last,None)==None):
##        date_lastyear =  date_lastyear - relativedelta(days=1)
##        strDate_last = date_lastyear.strftime("%m/%d/%Y")
##        print strDate_last
##    dailyERC2016 = ercdict_20012016[strDate_last]
##
##    #Actually it is 17 years now
##    for i in range(20):
##        date = datetime.datetime.date(d) - relativedelta(years=i)
##        strDate = date.strftime("%m/%d/%Y")
##        #print strDate
##        if ercdict_20012016.get(strDate,None)!=None:
##            dailyERC.append(ercdict_20012016[strDate])
##
##    #print dailyERC,len(dailyERC)
##    MAX = max(dailyERC)
##    AVG = sum(dailyERC)/len(dailyERC)
##
##    #print MAX,AVG
##    return (MAX,AVG,dailyERC2016)

##Updated on 12/01/2017
def getHistERC(d,ercdict_allyear):
    dailyERC = []

    date_lastyear = datetime.datetime.date(d) - relativedelta(years=1)
    strDate_last = date_lastyear.strftime("%m/%d/%Y")
    #print strDate_last
    while (ercdict_allyear.get(strDate_last,None)==None):
        date_lastyear =  date_lastyear - relativedelta(days=1)
        #print type(date_lastyear),date_lastyear
        if date_lastyear.year < 2001:
            break
        strDate_last = date_lastyear.strftime("%m/%d/%Y")

    dailyERClastyear = ercdict_allyear[strDate_last]

    #Actually it is 17 years now
    for i in range(20):
        date = datetime.datetime.date(d) - relativedelta(years=i)
        strDate = date.strftime("%m/%d/%Y")
        #print strDate
        if ercdict_allyear.get(strDate,None)!=None:
            dailyERC.append(ercdict_allyear[strDate])

    MAX = max(dailyERC)
    AVG = sum(dailyERC)/len(dailyERC)

    ##print MAX,AVG,dailyERClastyear
    return (MAX,AVG,dailyERClastyear)


###########################################################################
## First appending the 2017 data into the _ALL_YEAR table from PSA_ERX_AVG
###########################################################################
PSAs = ['NTXS','TRANSPEC','SETX','HIGHPLAN','SOUTHPLN','ROLLPLN','HILLCNTY','RIOGRAND','CENTRLTX','COASTLPL','NETX','WESTPINE','UPRCOAST','LOWCOAST']
##A solution to detect if there are missing data for today
##conn = sqlite3.connect('C:\\DEV\\ERC\\ercdb_updated.sqlite')
##cur = conn.cursor()
##for psa in PSAs:
#### Remove records existed for 2016, difficult to compare time, may just delete it by hand!
##    cmd = 'INSERT INTO ' + psa + '_ERC_ALLYEAR SELECT PSA_ERC_AVG."index",' + psa + ' from PSA_ERC_AVG'
##    print cmd
##    cur.execute(cmd)
##    row = cur.fetchall()
##    print row
##conn.commit()
##conn.close()

#PSAs = ['NTXS']

WD = os.getcwd()
#Excel_Book = pandas.ExcelWriter(os.path.join(WD, "ERC_HISTDATA.xlsx"))

engine = create_engine('sqlite:///C:\\DEV\\ERC\\ercdb_updated.sqlite')
for PSA in PSAs:

    table_hist_full = PSA + '_ERC_ALLYEAR'
    pread = pandas.read_sql_table(table_hist_full,engine)
    #print pread
    erc_ALLYEAR = pread.loc[:,'ERC'].values
    erc_df = pandas.DataFrame(pread.loc[:,'ERC'])
    erc_df.index = pandas.to_datetime(pread.loc[:,'DATE'],errors='coerce')
    date = pread.loc[:,'DATE'].values
    ts= pandas.Series(erc_ALLYEAR, index=date)
    ercdict_test = pandas.Series.to_dict(ts)
    #print ercdict_test
##    ercdict_20012016 ={}
##    for i,v in ercdict_test.items():
##        #print i.strftime("%m/%d/%Y"), v
##        ercdict_20012016[i.strftime("%m/%d/%Y")] = v
##    ercdict_20012016= dict(zip(date,erc_ALLYEAR))
##    print ercdict_20012016
    #Updated on 12/01/2017 for next year
    ercdict_allyear = {}
    for i,v in ercdict_test.items():
        #print i.strftime("%m/%d/%Y"), v
        ercdict_allyear[i.strftime("%m/%d/%Y")] = v
    ##ercdict_allyear= dict(zip(date,erc_ALLYEAR))
    #print ercdict_allyear
    #Create a dataframe that has a field for each year to get the ERC value for the same date
##    dates = pandas.date_range('2017-01-01', '2017-12-31', freq='D')
    dates = pandas.date_range('2018-01-01', '2018-12-31', freq='D')
    df = pandas.DataFrame(0, index = dates, columns = ['ercMax','ercAvg','lastYear'])

    for index,row in df.iterrows():
        row['ercMax'],row['ercAvg'],row['lastYear'] = getHistERC(index,ercdict_allyear)

    #Save the derived historical MAX and AVG (pandas datafram) to the database
    try:
        tablename = PSA + '_ERC_HIST2017'
        print 'processing: ' + tablename
        df.to_sql(tablename,engine,flavor='postgresql',if_exists='replace')
        #Try Retrieving the data form the data
        #dfread = pandas.read_sql_table(tablename,engine)
        #print dfread
    except:
        print 'there is a problem for writting dataframe into database'

engine.dispose()
