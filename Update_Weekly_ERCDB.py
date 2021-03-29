#-------------------------------------------------------------------------------

# Name:        Update_Weekly_ERCDB.py

# Purpose:     Check the archived database for missing value for the past week

# Author:      pyang

# Created:     05/31/2016
# Updated:     09/09/2016 for creating alert when there are missing obs data

# Copyright:   (c) pyang 2016

#-------------------------------------------------------------------------------

import csv

import xml.etree.ElementTree as ET

import urllib

import os

from itertools import groupby

from sqlalchemy import create_engine, MetaData

import pandas.io.sql as psql

import pandas

import numpy as np

import datetime

import sqlite3

import smtplib

#Send email if issue happenned!
def sendEmail(TXT):
    server = smtplib.SMTP('tfsbarracuda.tamu.edu', 25)
    #server.set_debuglevel(1)
    SUBJECT = 'Missing data for ERC Seasonal Graph'
    message = 'Subject: %s\n\n%s' % (SUBJECT, TXT)
    tolist = ["pyang@**.edu"]
    server.sendmail("pyang@**.edu", tolist, message)


root = os.getcwd()

rawFolder = os.path.join(root,'XML')

#rawFile = r'c:\DEV\FireDanger\WIMS\TWIM_19-Jan-16-20-Jan-16.csv'

# create groups and sorted stations lists for later use

def FilterFuelModel(rawFile):

    groups = []

    groups8g = []

    groups7g = []

    stations = sorted([])

    #psa = rawFile[]

    # read new csv file

    with open(rawFile) as rawcsv:

        rawReader = csv.reader(rawcsv, delimiter = ',')

        header = next(rawReader)

        for keys, rows in groupby(rawReader, lambda row: row[0]):#Groupby station id

            #print keys,rows,list(rows)

            groups.append(list(rows))

            stations.append(keys)

    print 'There are total ',len(stations),' stations for ERC on ',DATE

    # create filtered file

    #inputTableName = 'TodayERC'

    filteredFile = os.path.join(rawFolder, "TWIM_filtered_" + end + ".csv")



    # filter and write filtered file--

    with open(filteredFile, 'wb') as filteredcsv:

        writer = csv.writer(filteredcsv, delimiter = ',')

        writer.writerow(header)

        for group in groups:

            #there is only one fule model then write it directly

            if len(group) == 1:

                writer.writerow((group)[0])

                groups8g.append(group)

                continue

            #if there is a 8G model also write to file

            else:

                for rowg in group:

                    if (rowg[3])[:2] == '8G':

                        groups8g.append(group)

                        writer.writerow(rowg)

                        break

        #there is no 8G model but there are 7G model then write

        groups7g = [x for x in groups if x not in groups8g]

        #print groups7g

        for group in groups7g:

            for rowg in group:

                if (rowg[3])[:2] == '7G':

                    writer.writerow(rowg)

                    break

    return(filteredFile)



#Parse the XML into csv

#It is better to parse them into a dataframe instead of a csv file

def ParseXML(XMLFileName):

    rawFile = XMLFileName[:-4] + '.csv'

    # Header for output csv file. Contains all fields for final csv file

    header0 = ['sta_id', 'sta_nm', 'nfdr_dt', 'msgc', 'ec','mp']

    with open(rawFile, 'wb') as xmlf:

        writer = csv.writer(xmlf, delimiter = ',')

        writer.writerow(header0)

        tree = ET.parse(XMLFileName)

        root = tree.getroot()

        for row in root.findall('row'):

            #print row

            if row.find('sta_id').text is None:

                sta_id = -99

            else:

                sta_id = row.find('sta_id').text

            if row.find('sta_nm').text is None:

                sta_nm = -99

            else:

                sta_nm = row.find('sta_nm').text

            if row.find('nfdr_dt').text is None:

                nfdr_dt = -99

            else:

                nfdr_dt = row.find('nfdr_dt').text

                #nfdr_tm = row.find('nfdr_tm').text

            if row.find('msgc').text is None:

                msgc = -99

            else:

                msgc = row.find('msgc').text

            if row.find('ec') is None:

                ec = -99

            else:

                ec = row.find('ec').text

            if row.find('mp').text is None:

                mp = -99

            else:

                mp = row.find('mp').text

            rows = (sta_id, sta_nm, nfdr_dt, msgc, ec,mp)

            #print rows

            writer.writerow(rows)

        return(rawFile)



#Download the XML WIMS output for different Sigs

def DownloadERC(PSA):

    #Should be today's ERC date

    # define xml file locations

    xmlobs = rawFolder + '\\' + PSA + "_"+ start + "-" + end + ".xml"

    print xmlobs

    # Get reponse from WIMS server

    serverResponse = urllib.urlopen('https://fam.nwcg.gov/wims/xsql/nfdrs.xsql')

    # Check reponse code

    # If WIMS server is available, download xml files. Otherwise, exit process.

    if serverResponse.getcode() == 200:

        # Observations

        urllib.urlretrieve("https://fam.nwcg.gov/wims/xsql/nfdrs.xsql?stn=&sig=" + PSA + "&type=O&start=" + start + "&end=" + end + "&time=&user=pyang&priority=",

                           xmlobs)

                           # NTXS

    else:

        logging.info('WIMS System is unavailable')

        logging.info('The input tables for Fire Danger Processing have not been updated')

        print 'WIMS System is unavailable'

        raise SystemExit()



    #Parse the XML into csv

    #ParseXML(xmlobs)

    FilterFuelModel(ParseXML(xmlobs))

    #Here to generate the data structure for today's data

    #df ={}

    #return df



PSADICT = {'CENTRLTX':[413101,415501,415602,416401,416601,419701,419702,418101,417903,417904,417905,419801,419802],#8G

           'NTXS':[410202,419601,419602,419702,419701,419402],

           'TRANSPEC':[417101,417103,417403,417201,417401,417404],#7G

           'ROLLPLN':[418902,419301,419401,419402,419203,419001],

           'SETX':[413509,414402,414501,415201,412801], #8G

           'NETX':[410401,410501,411102,411401,411901,412101,412202,419602,412801], #8G

           'COASTLPL':[418101,418201,418602,418605],#8G

           'HIGHPLAN':[418701,418901,418801,419001,418802],#7G

           'HILLCNTY':[417801,417901,419501,419502,417701,418001,419203,419403,418002,417802], #7G and 8G

           'LOWCOAST':[418202,418502,418603], #8G

           'RIOGRAND':[418001,418101,418102,418201,418604,418401,417802],#8G

           'SOUTHPLN':[417701,418001,419202,419203,418002,419001,419101], #7G and 8G

           'UPRCOAST':[418301,419901,415201,416099], #8G

           'WESTPINE':[412601,413101,413302,414102,415109,416601] #8G

           }

#----------------------------------------------------------

#418001 (Kickapoo Caverns) was assgined to both of the Rio Grande Plains PSA and Hill County

#419701(CEDAR HILL S.P.) and 419702(Granbury) were both assigned to NorthTX and Central TX

#418101(GUADALUPE RIVER S.P) and 418201(GEORGE WEST) were both assigned to CentralTX and Coastal Plains

def UpdateDB(DATE):

    filteredFile = os.path.join(rawFolder, "TWIM_filtered_" + DATE + ".csv")

    df = pandas.read_csv(filteredFile,sep=',')

    print df

    #Create a dataframe to save the daily average erc for each PSA and append it to the table

    # df = DataFrame (data, index, columns)

    erc_Table_clolum = ['NTXS','TRANSPEC','SETX','HIGHPLAN','SOUTHPLN','ROLLPLN','HILLCNTY','RIOGRAND','CENTRLTX','COASTLPL','NETX','WESTPINE','UPRCOAST','LOWCOAST']

##        index = pandas.date_range(todays_date-datetime.timedelta(1), periods=1, freq='D')

    index = df.loc[:,['nfdr_dt']].values[0]

    index = pandas.to_datetime(index)

    df_avg_erc =pandas.DataFrame(index=index, columns=erc_Table_clolum)

    #Then use a loop to fill all value for the avg talbe

    for PSA,STATIONs in PSADICT.items():

        #print PSA,STATIONs

        df_psa = df.loc[df['sta_id'].isin(STATIONs)]

        avg_psa=df_psa.loc[:,['ec']].apply(np.mean)

        df_avg_erc.loc[index,[PSA]]=round(float(avg_psa),1)

    #print 'Average ERC for all PSAs:',df_avg_erc,type(df_avg_erc)

    return df_avg_erc

    #Connect to a database and write the record into it

    #PostgerSQL through ps** pm windows

    #engine = create_engine(r'postgresql://postgres:2016@tFs@localhost/ERC')

    #sqlite through sqlite3 on Windows

##        engine = create_engine('sqlite:///C:\\DEV\\ERC\\ercdb_updated.sqlite')

##        tablename = 'PSA_ERC_AVG'

##        df_avg_erc.to_sql(tablename,engine,flavor='sqlite',if_exists='append')

##        erc_df = pandas.DataFrame(df,columns=['ec'])

##        erc_df.index = df.loc[:,'sta_id']

##        erc_df.index = pandas.to_datetime(df.loc[:,'nfdr_dt'])

##        df.to_sql(tablename,engine,flavor='postgresql',if_exists='replace')

##        #Try Retrieving the data form the data

##        dfread = pandas.read_sql_table(tablename,engine)

##        engine.dispose()





##A solution to detect if there are missing data for today

conn = sqlite3.connect(root + '\\ercdb_updated.sqlite')

print conn

cur = conn.cursor()

cur.execute('''SELECT PSA_ERC_AVG."index" FROM PSA_ERC_AVG WHERE NTXS is null OR TRANSPEC is null OR \

      SETX is null OR HIGHPLAN is null OR SOUTHPLN is null OR ROLLPLN is null OR HILLCNTY is null OR RIOGRAND is null OR CENTRLTX is null OR \

      COASTLPL is null OR NETX is null OR WESTPINE is null OR UPRCOAST is null OR LOWCOAST is null''')



#cur.execute('SELECT count FROM Counts WHERE org = ? ', (org, ))

row = cur.fetchall()



datelist = []

print datelist

##start from the 2nd date before the Feb 20 is a day are recognized no solution for retrieving the data

#for tdate in row[1:]:

for tdate in row:
    print tdate
    index = tdate.__getitem__(0)
    dt = tdate.__getitem__(0)[:10]
    #date = datetime.datetime.strptime(dt,"%Y-%m-%d %H:%M:%S-%Z")
    date = datetime.datetime.strptime(dt,"%Y-%m-%d")
    print date
    #Turn the date string into format like: 29-May-16
    DATE = date.strftime("%d-%b-%y")
    #DATE = "29-June-17"
    start = str(DATE)
    end = start
    DownloadERC('TWIM')
    df = UpdateDB(end)
    #tranform the data frame into a tuple for updating the records
    tuples = [tuple(x) for x in df.to_records(index=False)]
    list_n =list(tuples[0])
    list_n.append(index)
    allvalue = tuple(list_n)
##then update the field using the index that retrieved:
##    cur.execute('UPDATE PSA_ERC_AVG SET NTXS =?, HILLCNTY = ? WHERE PSA_ERC_AVG."index" = ?',(666,888,index, ))
    cur.execute('UPDATE PSA_ERC_AVG SET NTXS=?,TRANSPEC=?,SETX=?,HIGHPLAN=?,SOUTHPLN=?,ROLLPLN=?,HILLCNTY=?,RIOGRAND=?,CENTRLTX=?,COASTLPL=?,NETX=?,WESTPINE=?,UPRCOAST=?,LOWCOAST=? WHERE PSA_ERC_AVG."index" = ?', allvalue)
    conn.commit()


#DATE = date.strftime("%d-%b-%y")
##DATE = "29-June-17"
##start = str(DATE)
##end = "17-July-17"
##print start,end
##DownloadERC('TWIM')
##df = UpdateDB(end)
###tranform the data frame into a tuple for updating the records
##tuples = [tuple(x) for x in df.to_records(index=False)]
##list_n =list(tuples[0])
##list_n.append(index)
##allvalue = tuple(list_n)
####then update the field using the index that retrieved:
####    cur.execute('UPDATE PSA_ERC_AVG SET NTXS =?, HILLCNTY = ? WHERE PSA_ERC_AVG."index" = ?',(666,888,index, ))
##cur.execute('UPDATE PSA_ERC_AVG SET NTXS=?,TRANSPEC=?,SETX=?,HIGHPLAN=?,SOUTHPLN=?,ROLLPLN=?,HILLCNTY=?,RIOGRAND=?,CENTRLTX=?,COASTLPL=?,NETX=?,WESTPINE=?,UPRCOAST=?,LOWCOAST=? WHERE PSA_ERC_AVG."index" = ?', allvalue)
##conn.commit()

############################################################
#Do the inquiry again to see if there more days has no data
cur.execute('''SELECT PSA_ERC_AVG."index" FROM PSA_ERC_AVG WHERE NTXS is null OR TRANSPEC is null OR \
      SETX is null OR HIGHPLAN is null OR SOUTHPLN is null OR ROLLPLN is null OR HILLCNTY is null OR RIOGRAND is null OR CENTRLTX is null OR \
      COASTLPL is null OR NETX is null OR WESTPINE is null OR UPRCOAST is null OR LOWCOAST is null''')

#cur.execute('SELECT count FROM Counts WHERE org = ? ', (org, ))
row = cur.fetchall()
datelist = []
print datelist

for tdate in row:
    print tdate
    index = tdate.__getitem__(0)
    dt = tdate.__getitem__(0)[:10]
    #date = datetime.datetime.strptime(dt,"%Y-%m-%d %H:%M:%S-%Z")
    date = datetime.datetime.strptime(dt,"%Y-%m-%d")
    #Turn the date string into format like: 29-May-16
    DATE = date.strftime("%d-%b-%y")
    datelist.append(DATE)

##################################################################################
##Send alert email when there are more days missing data other than Feb 20 and 21
##Updated on 10/03/2016 now September 22 2016 also missing data
######################################################################################
if(len(datelist)) > 3:
    missdateday = str( datelist[len(datelist)-1])
    MSG = "Observation data missing for ERC Seasonal Graph on: %s ,Please check out!"% (missdateday)
    print MSG
    #sendEmail(MSG)


conn.close()
