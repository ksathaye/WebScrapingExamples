# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 16:32:19 2015

@author: kiransathaye
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark import SparkContext
import pyspark as spark
import multiprocessing as MP
from pyspark.sql import SQLContext
import time
import matplotlib.lines as mlines

import pyspark.sql.functions as SpFun

def RandAR(s):

    R=np.random.rand(s,4)
    return R

def LoadNFL(sc):
    sqlc = SQLContext(sc)

    years=range(2010,2016)
    DF09=pd.read_csv('NFL09Drives.csv')

    DFAll=sqlc.createDataFrame(DF09)
    #DFSpark10=sqlc.createDataFrame(DF10)

    for i in range(len(years)):
        yr=(str(years[i]))
        print(yr)
        FName='NFLPlays' + yr +'.csv'
        PDLoad=pd.read_csv(FName)
        A=sqlc.createDataFrame(PDLoad)
        DFAll=DFAll.unionAll(A)

    DistDowns=DFAll.select('DownGo').distinct().toPandas()
    DistDowns=list(DistDowns.DownGo)

    RandFrame=np.zeros([len(DistDowns),4])
    OutFrame=pd.DataFrame(data=RandFrame,columns=('Down','Distance','Rate','Number'))

    for i in range(len(DistDowns)):
        print(i)
        DownFilt=DFAll.filter(DFAll['DownGo']==DistDowns[i])
        LenAll=DownFilt.count()
        LenSuccess=DownFilt.filter(DownFilt['Success']==1).count()
        OutFrame.loc[i].Down=int(DistDowns[i][0])
        OutFrame.loc[i].Distance=int(DistDowns[i][2:])
        OutFrame.loc[i].Rate=LenSuccess/float(LenAll)
        OutFrame.loc[i].Number=LenAll

    return OutFrame

def SparkPandasTest(sc):
    NumCores=MP.cpu_count();
    years=range(2010,2016)
    DF09=pd.read_csv('NFL09Drives.csv')
    for i in range(len(years)):
        yr=(str(years[i]))
        print(yr)
        FName='NFLPlays' + yr +'.csv'
        PDLoad=pd.read_csv(FName)
        DF09=DF09.append(PDLoad)

    DistDowns=list(DF09.DownGo.unique())
    RateNum=np.zeros([len(DistDowns),2])
    t=time.time()
    for i in range(len(DistDowns)):
        RateNum[i,:]=MapFilt(DF09,DistDowns[i])
    print('For Loop Time Elapsed: ' + str(time.time()-t) + ' sec')

    t=time.time()
    count1 = sc.parallelize(range(len(DistDowns)),NumCores)
    count2=count1.map(lambda x: MapFilt(DF09,DistDowns[x]))
    R=np.array(count2.collect())

    print('Spark Time Elapsed: ' + str(time.time()-t) + ' sec')

    DG=DF09[['Down','ToGo']]
    DG=DG.drop_duplicates()
    P=pd.DataFrame(R,columns=('Prob','Number'))
    ind=DG.index
    P.index=DG.index
    DG['Prob']=P.Prob
    DG['Number']=P.Number

    return DG

def MapFilt(DFAll,DistDowns):
    DownFilt=DFAll[DFAll.DownGo==DistDowns]
    LenAll=len(DownFilt)
    LenSuccess=len(DownFilt[DownFilt['Success']==1])
    RateNum=np.zeros(2)
    RateNum[0]=LenSuccess/float(LenAll)
    RateNum[1]=LenAll
    return RateNum

def PlotSucces(DF):
    DF=DF[DF.Number>50]
    DownList=list()
    First10Prob=0
    for i in range(1,5):
        YDS=DF.ToGo[DF.Down==i]
        Prob=DF.Prob[DF.Down==i]
        NP=np.zeros([len(Prob),2])
        NP[:,0]=np.array(YDS)
        NP[:,1]=np.array(Prob)
        NP=NP[NP[:,0].argsort()]
        plt.plot(NP[:,0],NP[:,1]*100,marker='o')
        if i==1:
            First10Prob=np.argwhere(NP[:,0]==10)
            print(First10Prob)
            First10Prob=100*NP[int(First10Prob),1]

    print(NP)
    plt.xlim([0,25])
    plt.xlabel('Yard To Go')
    plt.ylabel('Probablity of 1st Down or TD')
    plt.title('NFL Drive Continuation 2009-2015')
    blue_line = mlines.Line2D([], [], color='blue', marker='o',
                          markersize=5, label='1st Down')
    green_line = mlines.Line2D([], [], color='g', marker='o',
                          markersize=5, label='2nd Down')
    red_line = mlines.Line2D([], [], color='r', marker='o',
                          markersize=5, label='2nd Down')
    c_line = mlines.Line2D([], [], color='cyan', marker='o',
                          markersize=5, label='4th Down')
    plt.legend(handles=[blue_line,green_line,red_line,c_line])
    plt.plot([0,25],[First10Prob,First10Prob],c='k')
    plt.savefig('FirstProbs.pdf')
    return NP

try:
    sc = SparkContext()
    print('Making sc')
except:
    print('Spark Context already exists')

#A=SparkPandasTest(sc)