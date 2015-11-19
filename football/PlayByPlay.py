# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 15:25:21 2015

@author: kiransathaye
"""

import numpy as np
import nflgame as nfl
import pandas as pd
import matplotlib.pyplot as plt
from pyspark import SparkContext
import pyspark as spark
import multiprocessing as MP
from pyspark.sql import SQLContext


def FieldPostoINT(FieldPosString):
    if FieldPosString[0:3]=='OWN':
       FieldInt=100-int(FieldPosString[-2:])
    elif FieldPosString[0:3]=='OPP':
       FieldInt=int(FieldPosString[-2:])
    elif FieldPosString[0:3]=='MID':
       FieldInt=50
    else:
        FieldInt=np.nan
    return FieldInt

def DriveAssign(Drive):
    columnNames=('Down','ToGo','Success','Attempt','Team','DriveResult','DriveStart','DownGo','FieldPosition')
    if Drive==1:
        return pd.DataFrame(columns=columnNames)

    NumPlays=Drive.play_cnt
    n=0
    playArray=np.zeros([NumPlays,5])
    DownGo=list(range(NumPlays))
    FieldPos=list(range(NumPlays))
    for p in Drive.plays.sort(''):
        playArray[n,0]=p.down
        playArray[n,1]=p.yards_togo
        FieldPos[n]=FieldPostoINT(str(p.yardline))

        DownGo[n]=str(p.down)+','+str(p.yards_togo)
        n=n+1
    #playArray[playArray[:,0]==0,0]=np.nan
    try:
        FirstDown=np.where(playArray[:,0]==1)
        playArray[0:np.max(FirstDown),2]=1
    except:
        return pd.DataFrame(columns=columnNames)

    if np.sum(playArray[:,0]==1)==1:
       playArray[:,2]=0

    if Drive.result=='Touchdown':
       playArray[:,2]=1
    playArray[:,3]=1

    df=pd.DataFrame(columns=columnNames)
    df['Down']=np.array(playArray[:,0],dtype=int)
    df['ToGo']=np.array(playArray[:,1],dtype=int)
    df['Success']=np.array(playArray[:,2],dtype=int)
    df['Attempt']=np.array(playArray[:,3],dtype=int)
    df['DriveResult']=Drive.result
    df['DriveStart']=Drive.field_start.offset
    df['Team']=Drive.team
    df['DownGo']=DownGo
    df['FieldPosition']=FieldPos
    df=df[df.Down>0]
    return df


def DownProbs(PlayData):
    PlayData=PlayData[PlayData.FieldPosition>9] # eliminate goal line situations
    U=list(pd.unique(PlayData['DownGo']))
    DG=np.zeros([len(U),4])

    for i in range(len(U)):
        DU=D[D.DownGo==U[i]]
        DG[i,0]=float(U[i][0])
        DG[i,1]=float(U[i][2:])
        DG[i,2]=np.sum(DU.Success)/len(DU)
        DG[i,3]=len(DU)

    DG[DG[:,3]<25,1]=np.nan
    DG1=DG[DG[:,0]==1,:]
    DG1=DG1[DG1[:,1].argsort(),]
    DG2=DG[DG[:,0]==2,:]
    DG2=DG2[DG2[:,1].argsort(),]
    DG3=DG[DG[:,0]==3,:]
    DG3=DG3[DG3[:,1].argsort(),]
    DG4=DG[DG[:,0]==4,:]
    DG4=DG4[DG4[:,1].argsort(),]

    First10=100*DG1[np.nanargmin(np.abs(DG1[:,1]-10)),2]

    plt.figure()
    plt.plot(DG1[:,1],DG1[:,2]*100,marker='o')
    plt.plot(DG2[:,1],DG2[:,2]*100,c='red',marker='o')
    plt.plot(DG3[:,1],DG3[:,2]*100,c='k',marker='o')
    #plt.plot(DG4[:,1],DG4[:,2]*100,c='c',marker='o')
    plt.plot([1,15],[First10,First10],c='k',ls='--')
    plt.xlim([1,15])
    plt.xlabel('Yards To Go')
    plt.ylabel('% Chance of 1st Down or TD')

def DrivesFromGame(gameObj):
    D=DriveAssign(1)
    NumDrives=len(gameObj.data['drives'])-1
    for i in range(1,NumDrives):
        Drives=gameObj.drives.number(i)
        D=D.append(DriveAssign(Drives))
    return D

def LoadNFL(sc):
    sqlc = SQLContext(sc)
    NumCores=MP.cpu_count();

    years=range(2010,2016)
    #DFYears=list()
    DF09=pd.read_csv('NFL09Drives.csv')
    #DF10=pd.read_csv('NFL09Drives.csv')

    DFAll=sqlc.createDataFrame(DF09)

    for i in range(len(years)):
        yr=(str(years[i]))
        print(yr)
        FName='NFLPlays' + yr +'.csv'
        PDLoad=pd.read_csv(FName)
        A=sqlc.createDataFrame(PDLoad)
        DFAll=DFAll.unionAll(A)

    #DFAll=sc.parallelize(DFAll)
    DistDowns=DFAll.select('DownGo').distinct().toPandas()
    DistDowns=list(DistDowns.DownGo)

    RandFrame=np.zeros([len(DistDowns),4])
    OutFrame=pd.DataFrame(data=RandFrame,columns=('Down','Distance','Rate','Number'))

    #count1 = sc.parallelize(range(len(DistDowns)),NumCores)
    #count2=count1.map(lambda x: MapFilt(DFAll,DistDowns[x]))
 #   OutFrame=count2.collect()
#    sc.stop()

    for i in  range(1,10): #range(len(DistDowns)):
        OutFrame.loc[i].Down=int(DistDowns[i][0])
        OutFrame.loc[i].Distance=int(DistDowns[i][2:])
        RateNum=MapFilt(DFAll,DistDowns[i])
        OutFrame.loc[i].Rate=RateNum
        print(RateNum)

        #OutFrame.loc[i].Number=RateNum[1]

    return OutFrame

def MapFilt(DFAll,DistDowns):
    DownFilt=DFAll.filter(DFAll['DownGo']==DistDowns)
    LenAll=DownFilt.count()
    LenSuccess=DownFilt.filter(DownFilt['Success']==1).count()
    #RateNum=np.zeros(2)
    RateNum=LenSuccess/float(LenAll)
    #RateNum[1]=LenAll
    return RateNum


def SaveSeasonPlays():
    weekRange=range(1,17)
    for y in range(2009,2016):
        D=DriveAssign(1)
        games = nfl.games(year=y,week=weekRange)
        for i in range(len(games)):
            D=D.append(DrivesFromGame(games[i]))
        NameString='NFLPlays'+ str(y) + '.csv'
        D.to_csv(NameString)
        print('Year: '+str(y))

if __name__ == "__main__":
    try:
        sc = SparkContext()
        print('Making sc')
    except:
        print('Spark Context already exists')

    OF=LoadNFL(sc)
