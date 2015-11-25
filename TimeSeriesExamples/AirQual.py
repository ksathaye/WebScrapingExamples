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

def saveAirQual(cityNum):

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

def MergeAllCities():
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
    plt.plot(t,y)
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

#D=MergeAllCities()
#CM=PollCov(D)
#F=SignalSpectrum(D,1)
Y=SignalToy()