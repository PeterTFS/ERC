#-------------------------------------------------------------------------------
# Name:        erc_plot_sqlite_Updated_OnlinemapSupport.py
# Purpose:     Retrive archived erc from database for climtology plotting
# Author:      pyang
# Created:     21/01/2016
# Updated:     06/14/2016 for creating png as well as pdf files
# Updated? 12/31/2016 for new year!
# Updated:     05/11/2017 to support arcgis online map
# Copyright:   (c) pyang 2016
#-------------------------------------------------------------------------------
##def main():
##    pass
##if __name__ == '__main__':
##    main()
import os
import csv
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import pandas
import datetime
import numpy as np
from sqlalchemy import create_engine, MetaData
import pandas.io.sql as psql
import sqlite3
from matplotlib.backends.backend_pdf import PdfPages  #combine several pdfs file into one file!

plt.style.use('ggplot')

#A function for writing a dict to a file
def saveDict(fn,dict_rap):
    f=open(fn, "wb")
    w = csv.writer(f)
    for key, val in dict_rap.items():
        w.writerow([key, val])
    f.close()

FuelModel = {'CENTRLTX':'8G',
             'NTXS' : '8G',
             'TRANSPEC':'7G',
             'SETX':'8G',
             'ROLLPLN':'7G',
             'NETX':'8G',
             'COASTLPL':'8G',
             'HIGHPLAN':'7G',
             'HILLCNTY':'7G',
             'LOWCOAST':'8G',
             'RIOGRAND':'8G',
             'SOUTHPLN':'7G',
             'UPRCOAST':'8G',
             'WESTPINE':'8G'}

PSAs = ['P_CentralTX','P_CoastalPlns','P_HighPlns','P_HillCountry','P_LowerCoast','P_NETX','P_NorthTX',
        'P_RioGrandPlns','P_RollingPlns','P_SETX','P_SouthernPlns','P_TransPecos','P_UpperCoast','P_WPineywoods']

PSAinDB = {'P_CentralTX':'CENTRLTX',
            'P_CoastalPlns':'COASTLPL',
            'P_HighPlns':'HIGHPLAN',
            'P_HillCountry':'HILLCNTY',
            'P_LowerCoast':'LOWCOAST',
            'P_NETX':'NETX',
            'P_NorthTX':'NTXS',
            'P_RioGrandPlns':'RIOGRAND',
            'P_RollingPlns':'ROLLPLN',
            'P_SETX':'SETX',
            'P_SouthernPlns':'SOUTHPLN',
            'P_TransPecos':'TRANSPEC',
            'P_UpperCoast':'UPRCOAST',
            'P_WPineywoods':'WESTPINE'
            }

PSAwithLongname ={'CENTRLTX':'Central TX',
                'NTXS':'North TX',
                'TRANSPEC':"Trans Pecos",
                'ROLLPLN':"Rolling Plains",
                'SETX':"SE TX",
                'NETX':"NE TX",
                'COASTLPL':"Coastal Plains",
                'HIGHPLAN':"High Plains",
                'HILLCNTY':"Hill Country",
                'LOWCOAST':"Lower Coast",
                'RIOGRAND':"Rio Grande Pla",
                'SOUTHPLN':"Southern Plain",
                'UPRCOAST':"Upper Coast",
                'WESTPINE':"W Pinewoods"}

PSAPDF ={'CENTRLTX':'erc_ctx',
                'NTXS':'erc_ntx',
                'TRANSPEC':"erc_tp",
                'ROLLPLN':"erc_rp",
                'SETX':"erc_setx",
                'NETX':"erc_netx",
                'COASTLPL':"erc_cp",
                'HIGHPLAN':"erc_hp",
                'HILLCNTY':"erc_hc",
                'LOWCOAST':"erc_lgc",
                'RIOGRAND':"erc_rgp",
                'SOUTHPLN':"erc_sp",
                'UPRCOAST':"erc_ugc",
                'WESTPINE':"erc_wpw"}

#For Seasonal ERC arcgis online updating, a dict created for contain the percentile value for each PSA
PSAUpdate  ={'CENTRLTX':'Central Texas',
                'NTXS':'North Texas',
                'TRANSPEC':"Trans Pecos",
                'ROLLPLN':"Rolling Plains",
                'SETX':"Southeast Texas",
                'NETX':"Northeast Texas",
                'COASTLPL':"Coastal Plains",
                'HIGHPLAN':"High Plains",
                'HILLCNTY':"Hill Country",
                'LOWCOAST':"Lower Gulf Coast",
                'RIOGRAND':"Rio Grande Plains",
                'SOUTHPLN':"Southern Plains",
                'UPRCOAST':"Upper Gulf Coast",
                'WESTPINE':"Western Pineywoods"}

#Then A dict for value for each PSA
PSALevelDict = {}

#For review purpose, create a pdf book to the FTP(or SyncPlicity) for sharing
producetime = datetime.datetime.now()
date_today = producetime.strftime("%Y%m%d")
pdf_ARCHIVE_Path = os.path.join(os.getcwd(),'ARCHIVE')
if not os.path.exists(pdf_ARCHIVE_Path):
    os.makedirs(pdf_ARCHIVE_Path)
multipage_pdf = pdf_ARCHIVE_Path + '\\' + date_today + ".pdf"
print multipage_pdf

with PdfPages(multipage_pdf) as pdf:
    for PSA,PSA_longname in PSAwithLongname.items():
        print PSA,PSA_longname

        #List a series of table need to acceess from database
        #Table of erc for all PSAs for this year uptodate (updated every day)
        table_erc = 'PSA_ERC_AVG'
        #Table for the historical analyses (including Max, Avg and last year ERC) for each PSA
        #The historial should be 2016 for 2017
        table_hist =PSA + '_ERC_HIST2016' #Updated 12/31/2016
        #Table for daily historical erc for all the years for each PSA
        table_hist_full = PSA + '_ERC_ALLYEAR'

        #retrive the plotting data from database(PostgreSQL)
##        try:
##            #PostgerSQL through ps** pm windows
##            #engine = create_engine(r'postgresql://postgres:2016@tFs@localhost/ERC')
##            #sqlite through sqlite3 on Windows
##            #engine = create_engine('sqlite:///E:\\ERC\\ercdb_updated.sqlite')
##            engine = create_engine('sqlite:///C:\\DEV\\ERC\\ercdb_updated.sqlite')
##            #Try Retrieving the data form the data
##            df_ERC = pandas.read_sql_table(table_erc,engine)
##            df_HIST = pandas.read_sql_table(table_hist,engine)
##            df_HIST_FULL = pandas.read_sql_table(table_hist_full,engine)
##            engine.dispose()
##        except:
##            print 'there is a problem in connecting to database'
##            exit(0)

        engine = create_engine('sqlite:///C:\\DEV\\ERC\\ercdb_updated.sqlite')
        #Try Retrieving the data form the data
        df_ERC = pandas.read_sql_table(table_erc,engine)
        df_HIST = pandas.read_sql_table(table_hist,engine)
        df_HIST_FULL = pandas.read_sql_table(table_hist_full,engine)
        engine.dispose()



        date_todate = df_ERC.loc[:,'index'].values
        erc = df_ERC.loc[:,PSA].values
        date_2016 = df_HIST.loc[:,'index'].values
        avg_erc = df_HIST.loc[:,'ercAvg'].values
        max_erc = df_HIST.loc[:,'ercMax'].values
        lastyear_erc = df_HIST.loc[:,'lastYear'].values
        #df_HIST_FULL.index = pandas.to_datetime(df_HIST_FULL.loc[:,'DATE'])
        ts_ERC = df_HIST_FULL.loc[:,'ERC'].values

        firstYear = df_HIST_FULL.loc[0,'DATE'].year
        latestYear = '2016'
        latestYear = '2017' #For year 2017
        print firstYear,latestYear
        #ts= pandas.Series(df_HIST_FULL.loc[:,'ERC'].values, index=df_HIST_FULL.loc[:,'DATE'].values)
        # percentile97 = np.percentile(erc_allyear,97)
        Percentile97 = pandas.Series(np.nanpercentile(ts_ERC,97), index=date_2016 )
        Percentile90 = pandas.Series(np.nanpercentile(ts_ERC,90), index=date_2016 )
        Percentile75 = pandas.Series(np.nanpercentile(ts_ERC,75), index=date_2016 )
        Percentile50 = pandas.Series(np.nanpercentile(ts_ERC,50), index=date_2016 )


        #Create a number based on current days ERC value regarding to the historical percentile
        '''
        5--- >97%
        4--- 90~97%
        3--- 75~90%
        2--- 50~75%
        1--- <50%
        '''
        PSAname = PSAUpdate[PSA]
        print erc
        print len(erc)
        ercPSA = erc[-1]

        print ercPSA,Percentile50[0],Percentile75[0],Percentile90[0],Percentile97[0]
        if ercPSA <= Percentile50[0]:
            ercLevel = 1
        elif ercPSA > Percentile50[0] and ercPSA <= Percentile75[0]:
            ercLevel = 2
        elif ercPSA > Percentile75[0] and ercPSA <= Percentile90[0]:
            ercLevel = 3
        elif ercPSA > Percentile90[0] and ercPSA <= Percentile97[0]:
            ercLevel = 4
        else:
            ercLevel = 5
        PSALevelDict[PSAname] = ercLevel

        #the number of days with observation data
        dayofObs = len(ts_ERC) + len(erc)
        #print dfread


        formatter = mdates.DateFormatter('%b %d')
        matplotlib.rc('xtick', labelsize=9)
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.xaxis.set_major_formatter(formatter)

        #Historical Average ERC
        ax.plot_date(date_2016, avg_erc, '-',c='grey',lw=1.5, label='Avg')
        #Historical Maximum ERC
        ax.plot_date(date_2016, max_erc, '-',c='red',lw=1.5, label='Max')
        #Last year ERC
        ax.plot_date(date_2016, lastyear_erc, ':',c='blue',lw=1.3, label='2016')
        #97 and 90 Percentile for all the Previous years
##        ax.plot_date(date_2016, Percentile97, '-',c='purple',lw=1,label='97%')
##        ax.plot_date(date_2016, Percentile90, '-',c='green',lw=1,label='90%')
        ax.plot_date(date_2016, Percentile97, '-',c='purple',lw=1,label='97% ['+ str(int(Percentile97[0])) + ']')
        ax.plot_date(date_2016, Percentile90, '-',c='green',lw=1,label='90% ['+ str(int(Percentile90[0])) + ']')
        #Uptodate ERC
        ax.plot_date(date_todate, erc,'-',c='black',lw=1.5, label='2017')

        #add titles and legends,etc
        plt.xlabel('1 Day Periods',fontsize='xx-large')
        plt.ylabel('Energy Release Component',fontsize='x-large')
    #    PSA_longname = PSA_longname[PSA]
        subtitlename = "PSA - " + PSA_longname
        plt.suptitle(subtitlename,fontsize='x-large')
        title_year = str(firstYear) + '-' + str(latestYear)
        plt.title(title_year)
        leg = plt.legend(loc='lower center',ncol=2,fontsize='small')
        bb = leg.legendPatch.get_bbox().inverse_transformed(ax.transAxes)
        xOffset = 0.305
        yOffset = 0.15
        newX0 = bb.x0 + xOffset
        newX1 = bb.x1 + xOffset
        newY0 = bb.y0 - yOffset
        newY1 = bb.y1 - yOffset
        bb.set_points([[newX0, newY0], [newX1, newY1]])
        leg.set_bbox_to_anchor(bb)

        #Text to show the fuel model used and the date generated
        fuelmodel = 'Fuel Model: 8G' #Either 8G or 7G
        #Need to create a dictionary for the fuel model definition(it has all 8G, use 8G, otherwise will be 7G), it can be based on each PSA
        #fuelmodel = 'Fuel Model: 7G'
        #fuelmodel = FuelModel[PSA]
        #fuelmodel = 'Fuel Model: ' +  fuelmodel
        fuelmodel = 'Fuel Model: G' ##Meeting on Jan 26 2015, no difference will be made between 7G or 8G for the signature
        plt.figtext(0.9, 0.09, fuelmodel, horizontalalignment='right')
        observationtext = str(dayofObs) + ' Wx Observations'
        plt.figtext(0.9, 0.055, observationtext , horizontalalignment='right')
        producetime = datetime.datetime.now()
        producetext = 'Generated on ' + producetime.strftime("%m/%d/%Y-%H:%M")
        plt.figtext(0.9, 0.02, producetext , horizontalalignment='right')

        fig.autofmt_xdate()
        #plt.show()

        fig = plt.gcf()  # get current figure
        date_today = producetime.strftime("%Y%m%d")
        pdf_filePath = os.path.join(os.getcwd(),'PDF',date_today)
        png_filePath = os.path.join(os.getcwd(),'PNG',date_today)
        if not os.path.exists(pdf_filePath):
            os.makedirs(pdf_filePath)
        if not os.path.exists(png_filePath):
            os.makedirs(png_filePath)
        pdf_filename =  os.path.join(pdf_filePath, PSAPDF[PSA] +".pdf")
        png_filename =  os.path.join(png_filePath, PSAPDF[PSA] +".png")
        if(fig.savefig(pdf_filename)==None):
            pdf.savefig()
            print 'ERC graph: ' + pdf_filename + ' Successfully Produced!'
        if(fig.savefig(png_filename)==None):
            pdf.savefig()
            print 'ERC graph: ' + png_filename + ' Successfully Produced!'

print PSALevelDict
csv_filename = os.path.join(os.getcwd(),"PSALevel.csv")
saveDict(csv_filename,PSALevelDict)