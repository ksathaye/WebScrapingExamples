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
    DownFilt=DFAll.filter(DFAll['DownGo']==DistDowns)
    LenAll=DownFilt.count()
    LenSuccess=DownFilt.filter(DownFilt['Success']==1).count()
    #RateNum=np.zeros(2)
    RateNum=LenSuccess/float(LenAll)
    #RateNum[1]=LenAll
    return RateNum


def SaveSeasonPlays():
    weekRange=range(1,18)
    for y in range(2009,2016):
        D=DriveAssign(1)
        games = nfl.games(year=y,week=weekRange)
        for i in range(len(games)):
            D=D.append(DrivesFromGame(games[i]))
        NameString='NFLPlays'+ str(y) + '.csv'
        D.to_csv(NameString)
        print('Year: '+str(y))

def MapFilt(DFAll,DistDowns):
    DownFilt=DFAll[DFAll.DownGo==DistDowns]
    LenAll=len(DownFilt)
    LenSuccess=len(DownFilt[DownFilt['Success']==1])
    RateNum=np.zeros(2)
    RateNum[0]=LenSuccess/float(LenAll)
    RateNum[1]=LenAll
    return RateNum

def PlotSucces(DF):
    DF=DF[DF.Number>100]
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

    #print(NP)
    plt.xlim([0,25])
    plt.xlabel('Yards To Go (Minimum 100 Plays)')
    plt.ylabel('Probablity of 1st Down or TD (%)')
    plt.title('NFL Drive Continuation 2009-2015')
    blue_line = mlines.Line2D([], [], color='blue', marker='o',
                          markersize=5, label='1st Down')
    green_line = mlines.Line2D([], [], color='g', marker='o',
                          markersize=5, label='2nd Down')
    red_line = mlines.Line2D([], [], color='r', marker='o',
                          markersize=5, label='3rd Down')
    c_line = mlines.Line2D([], [], color='cyan', marker='o',
                          markersize=5, label='4th Down')
    plt.legend(handles=[blue_line,green_line,red_line,c_line])
    plt.plot([0,25],[First10Prob,First10Prob],c='k')
    plt.savefig('FirstProbs.pdf')
    return NP

if __name__ == "__main__":
    try:
        sc = SparkContext()
        print('Making sc')
    except:
        print('Spark Context already exists')

    A=SparkPandasTest(sc)
    NP=PlotSucces(A)
