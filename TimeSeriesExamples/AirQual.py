# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 10:05:53 2015

Air Quality Data Scrape

@author: kiransathaye
"""
import scipy
import requests
import numpy as np
import csv
import pandas as pd
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import sqlite3 as db


def saveAirQualityIndia():
    engine = create_engine('sqlite:///IndiaAir.db');

    AirLinks=('http://newdelhi.usembassy.gov/airqualitydataemb/rawaqmreadings2015.csv','http://newdelhi.usembassy.gov/airqualitydataemb/aqm2013.csv','http://newdelhi.usembassy.gov/airqualitydataemb/aqm2014.csv')
    FileNamesCSV=('IndiaAir2015.csv','IndiaAir2013.csv','IndiaAir2014.csv')

    col=('Date','Time','DelhiPM','ChennaiPM','KolkataPM','MumbaiPM','HyderabadPM')
    col2=('Date','Time','DelhiPM','DelhiPMAQI','ChennaiPM','ChennaiPMAQI','KolkataPM','KolkataPMAQI','MumbaiPM','MubmaiPMAQI','HyderabadPM','HyderabadPMAQI')
    col3=('Date','Time','DelhiPM','DelhiPMAQI','ChennaiPM','ChennaiPMAQI','KolkataPM','KolkataPMAQI','MumbaiPM','MubmaiPMAQI','HyderabadPM','HyderabadPMAQI','Dummy')

    colString=str(col)
    colString=colString.replace("'",'')
    colString=colString.replace(" ",'')

    col2String=str(col2)
    col2String=col2String.replace("'",'')
    col2String=col2String.replace(" ",'')

    col3String=str(col3)
    col3String=col3String.replace("'",'')
    col3String=col3String.replace(" ",'')


    FirstSpace=0
    FirstComma=0
    dfList=list()
    CurrentDate=datetime.datetime.now()
    for i in range(len(AirLinks)):
        FiltList=list()
        if i==0:
            FiltList.append(colString[1:-1]+'\n')
        elif i==1:
            FiltList.append(col3String[1:-1]+'\n')
        elif i==2:
            FiltList.append(col2String[1:-1]+'\n')

        print(AirLinks[i])
        r = (requests.get(AirLinks[i]).text).split('\n')
        NumVal=np.zeros(len(r),dtype=int)
        for k in range(len(r)):
            try:
                NumVal[k]=int(r[k][0])
                FirstSpace=r[k].find(' ')
                FirstComma=r[k].find(',')
                #r[k]=r[k][0:]
                #print(FirstSpace)
                #print('First Comma: ' +str(FirstComma))
                if FirstSpace<FirstComma:
                    r[k]=r[k][0:FirstSpace]+','+r[k][FirstSpace:FirstComma]+r[k][FirstComma+1:]
                FirstComma=r[k].find(',')
                #r[k]=r[k][0:FirstComma] +' ' +r[k][FirstComma+1:]
                FiltList.append(r[k])
            except:
                NumVal[k]=-999
        f=open(FileNamesCSV[i], 'w')
        f.writelines(FiltList)
        f.close()
        dfList.append(pd.read_csv(FileNamesCSV[i]))
        TimeWrong=dfList[i].Time==' 24:00 AM'
        dfList[i].loc[TimeWrong,'Time']='0:00 AM'
        dfList[i].Date=pd.to_datetime(dfList[i].Date,dayfirst=True)

        dfList[i].Time=pd.to_datetime(dfList[i].Time,dayfirst=True)-datetime.datetime(CurrentDate.year,CurrentDate.month,CurrentDate.day)
        dfList[i]['Datetime']=dfList[i].Date+dfList[i].Time

    dfAll=pd.concat(dfList,axis=0,join='inner')
    dfAll=dfAll.drop('Date',axis=1)
    dfAll=dfAll.drop('Time',axis=1)

    for i in range(len(dfAll.columns)-1):
        dfAll[dfAll.columns[i]]=np.genfromtxt(dfAll[dfAll.columns[i]])

    dfAll.to_sql('IndiaAirAll',engine,if_exists='replace')
    dfAll.to_csv('IndiaAirAll.csv')


    return dfAll

def saveAirQualChina(cityNum):

    cityList=('Beijing','Chengdu','Guangzhou','Shanghai','Shenyang')

    if cityNum>5:
        print('Invalid City Index')
        return -1

    AirQualURl='http://www.stateair.net/web/historical/1/' + str(cityNum) +'.html'

    r = requests.get(AirQualURl)
    rText=r.text
    LINK=rText.split('target')
    listLinks=list()
    for i in range(len(LINK)):
        LinkSub=LINK[i]
        StartIND=LinkSub.find('href="http://www.stateair.net/web/assets/historical/')
        if StartIND>-1:
            linkRef=LinkSub[StartIND+6:-2]

            r = requests.get(linkRef)
            text_file = open('AirData' + cityList[cityNum-1] + str(i) +'.csv', "w")
            text_file.write(r.content)
            listLinks.append(text_file.name)
            text_file.close()
    AddDateTime(listLinks,cityList[cityNum-1])

    return listLinks

def AddDateTime(listLinks,cityName):
    DFAll=pd.DataFrame(columns=('Datetime',cityName+'PM'))
    DF=pd.DataFrame(columns=('Datetime',cityName+'PM'))

    for i in range(len(listLinks)):
        DF=pd.DataFrame(columns=('Datetime',cityName+'PM'))
        P=pd.read_csv(listLinks[i],skiprows=3)
        P['Datetime']=pd.to_datetime(P['Date (LST)'])
        PVal=np.array(P.Value,dtype=float)

        DT=P.Datetime
        DF[cityName+'PM']=PVal
        DF.Datetime=DT
        DFAll=DFAll.append(DF)
    DFAll.to_csv(cityName+str('.csv'))


def MergeAllChina():
    cityList=('Beijing','Chengdu','Guangzhou','Shanghai','Shenyang')
    columns=['Datetime','BeijingPM','ChengduPM','GuangzhouPM','ShanghaiPM','ShenyangPM']
    dfs=list(np.zeros(len(cityList)))
    for i in range(len(dfs)):
        DF=pd.read_csv(cityList[i]+str('.csv'))
        DF[columns[i+1]][DF[columns[i+1]]<0]=np.nan
        dfs[i]=DF

    df_final = reduce(lambda left,right: pd.merge(left,right,on='Datetime',how='outer'), dfs)
    dfOut=df_final[columns]
    dfOut.Datetime=pd.to_datetime(dfOut.Datetime)
    dfOut=dfOut.sort(columns='Datetime',axis=0,ascending=False)
    #PlotTimeSeries(dfOutpd.to_datetime)
    dfOut.to_csv('ChinaAirAll.csv')
    return dfOut

def PollCov(Frame):
    NumCities=len(D.columns)
    col=Frame.columns

    CovMatrix=np.zeros([NumCities-1,NumCities-1])
    for i in range(1,NumCities):
        for k in range(1,NumCities):
            if i!=k:
                MI=Frame[col[i]]
                CovMatrix[i-1,k-1]=int(MI.cov(Frame[col[k]]))
                print(i,k)
    return CovMatrix


def SignalToy():
    Fs = 1000.0;  # sampling rate
    Ts = 1.0/Fs; # sampling interval
    t = np.arange(0,1,Ts) # time vector
    ff = 5;   # frequency of the signal
    y = np.sin(4*np.pi*ff*t)+np.sin(16*np.pi*ff*t)
    #y[20:80]=np.nan
    t=t[np.isnan(y)==False]
    y=y[np.isnan(y)==False]
    n = len(y) # length of the signal
    k = np.arange(n)
    T = n/Fs
    frq = k/T # two sides frequency range
    frq = frq[range(n/2)] # one side frequency range
    Y = scipy.fft(y)/n # fft computing and normalization
    Y = Y[range(n/2)]
    plt.subplot(2,1,1)
    plt.plot(t,y)
    plt.subplot(2,1,2)
    plt.plot(frq,abs(Y))
    plt.xlim([0,50])
    return Y

def SignalSpectrum(Frame,cityNum):
    cityList=Frame.columns
    Frame['UnixTime']=(Frame.Datetime-datetime.datetime(1970,1,1))
    Frame.UnixTime=Frame.UnixTime.astype('timedelta64[s]')

    t=Frame.UnixTime
    y=Frame[cityList[cityNum]]
    t=t[np.isnan(y)==False]
    y=y[np.isnan(y)==False]
    y=pd.rolling_mean(y,720,min_periods=1)
    n = len(y) # length of the signal
    k = np.arange(n)
    Fs=t[1]-t[0]
    T = n/Fs
    frq = k/T # two sides frequency range
    frq = frq[range(n/2)] # one side frequency range
    Y = scipy.fft(y)/n # fft computing and normalization
    Y = Y[range(n/2)]
    plt.subplot(2,1,1)
    plt.plot(Frame.Datetime,Frame.ShanghaiPM)
    plt.subplot(2,1,2)
    plt.plot(frq,abs(Y))
    plt.ylim([0,5])
    plt.xlim([0,100])

    #plt.plot(Frame.Datetime,MovAve,lw=0.5,marker='',markersize=1)
    #plt.xticks(rotation='vertical')

    return t

def PlotTimeSeries(Frame):

    if 1==0:
        plt.fill_between([min(Frame.Datetime),max(Frame.Datetime)],[50,50],[100,100],color='yellow',alpha=0.5,zorder=2)
        plt.fill_between([min(Frame.Datetime),max(Frame.Datetime)],[100,100],[150,150],color='orange',alpha=0.5,zorder=2)
        plt.fill_between([min(Frame.Datetime),max(Frame.Datetime)],[150,150],[200,200],color='m',alpha=0.5,zorder=2)
        plt.fill_between([min(Frame.Datetime),max(Frame.Datetime)],[200,200],[250,250],color='#800000',alpha=0.5,zorder=2)
        plt.fill_between([min(Frame.Datetime),max(Frame.Datetime)],[200,200],[250,250],color='#800000',alpha=0.5,zorder=2)


if "__main__":
    try:
        india=pd.read_csv('IndiaAirAll.csv')
    except:
        india=saveAirQualityIndia()
    try:
        china=pd.read_csv('ChinaAirAll.csv')
    except:
        try:
            china=MergeAllChina()
        except:
            saveAirQualChina()
            china=MergeAllChina()
    A=china.BeijingPM
    A[A<0]=np.nan
    N=np.isnan(A)
    A=A[N==False]

    plt.hist(np.array(A),100,cumulative=True,normed=True)
    plt.plot([50,50],[0,1],c='red',lw=3)
    plt.ylim([0,1])
    plt.ylabel('Fraction of Hours')
    plt.xlabel('PM 2.5 concentration')
    plt.title('Beijing Air Quality 20085-2013')
    plt.text(50,.2,'$\leftarrow$ US EPA Maximum', color='y', fontsize=14)
    plt.savefig('BeijingCDF.pdf')