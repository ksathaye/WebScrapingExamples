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
import nflgame as nfl
import time
import matplotlib.lines as mlines
import pyspark.sql.functions as SpFun

def RandAR(s):

    R=np.random.rand(s,4)
    return R


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

def DrivesFromGame(gameObj):
    D=DriveAssign(1)
    NumDrives=len(gameObj.data['drives'])-1
    for i in range(1,NumDrives):
        Drives=gameObj.drives.number(i)
        D=D.append(DriveAssign(Drives))
    return D

def DriveAssign(Drive,sc):
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
    sqlContext = SQLContext(sc)
    print(df.count())
    dfSP=sqlContext.createDataFrame(df)

    return dfSP

def DriveResult(gameInfo):
    games = nfl.games(year=int(gameInfo[0]),week=int(gameInfo[1]))
    numGames=len(games)
    if gameInfo[2]>=numGames:
        return (np.nan,np.nan)
    GameObj=games[int(gameInfo[2])]
    NumDrives=len(GameObj.data['drives'])-1
    OutList=[0]*(NumDrives-1)
    #FieldPosStart=np.zeros(NumDrives-1)
    for i in range(1,NumDrives):
        D=GameObj.drives.number(i)
        try:
            FieldPosStart=50-D.field_start.offset
            Result=D.result
        except:
            FieldPosStart=np.nan
            Result=np.nan
        OutList[i-1]=(FieldPosStart,Result)
        print(str(OutList[i-1]) + 'Drive Number' + str(i))
    #return OutList
    return pd.DataFrame(OutList,columns=('Field Position', 'Result'))

def DriveStartResult(Frame):

    YardGo=np.arange(0,11)*10
    EndPoss=list(pd.unique(Frame.Result))
    EndPoss.extend(['YardsToGo'])
    #d=np.zeros(np.size(EndPoss))*np.nan
    try:
        EndPoss.remove(np.nan)
    except:
        print('no nan')
    try:
        EndPoss.remove('UNKNOWN')
    except:
        print('No Unknown')


    ResFrame=pd.DataFrame(columns=EndPoss)
    ResFrame.YardsToGo=YardGo[1:]

    U=EndPoss[0:-1]

    #print(ResFrame.columns)
    for i in range(len(YardGo)-1):
        F=Frame[(Frame['Field Position']>YardGo[i]) & (Frame['Field Position']<YardGo[i+1])]
        #print(str(YardGo[i])  + 'Length' + str(len(F)))
        for k in range(len(ResFrame.columns)-1):
            ColN=U[k]
            N=len(F[F.Result==ColN])/float(len(F))
            try:
                ResFrame.loc[i,ColN]=N
            except:
                print('Row: ' + str(i))
                print('Col: ' + str(ColN) +str(k))

    plt.subplot(2,1,1)
    plt.plot(ResFrame.YardsToGo-5,ResFrame.Touchdown,marker='o',label='Touchdown')
    plt.plot(ResFrame.YardsToGo-5,ResFrame.Punt,marker='x',color='r',label='Punt')
    plt.plot(ResFrame.YardsToGo-5,ResFrame['Field Goal'],marker='s',color='g',label='Field Goal')
    plt.legend(ncol=3)
    plt.ylim([0,1])

    plt.subplot(2,1,2)
    #plt.plot(ResFrame.YardsToGo-5,ResFrame.Interception,marker='o',label='Interception')
    plt.plot(ResFrame.YardsToGo-5,ResFrame.Interception+ResFrame.Fumble,marker='o',label='Fumble/INT')
    plt.plot(ResFrame.YardsToGo-5,ResFrame.Downs,marker='x',label='Downs',color='red')
    plt.plot(ResFrame.YardsToGo-5,ResFrame['Missed FG']+ResFrame['Blocked FG'],marker='s',label='Missed FG',color='g')
    plt.ylim([0,.23])
    plt.legend(ncol=3)

    return ResFrame

def MakeDriveResultFrames():

    try:
        sc = SparkContext()
        print('Making sc')
    except:
        print('Spark Context already exists')
    NumCores=MP.cpu_count();
    yr=range(2009,2015)
    weekRange=range(1,18)
    gameRange=range(0,16)
    GameID=np.zeros([len(yr)*len(weekRange)*len(gameRange),3])
    GameID[:,0]=np.repeat(yr,272)
    GameID[:,1]=np.tile(np.sort(np.tile(weekRange,16)),6)
    GameID[:,2]=np.tile(gameRange,17*len(yr))

    count1=sc.parallelize(range(len(GameID)),NumCores)
    count2=count1.map(lambda x: DriveResult(GameID[x,:]))
    R=count2.collect()
    count1=sc.parallelize(range(len(GameID)),NumCores)
    count2=count1.map(lambda x: DriveResult(GameID[x,:]))
    R=count2.collect()
    FrameR=pd.DataFrame(columns=('Field Position','Result'))
    for i in range(len(R)):
        if len(np.shape(R[i]))==2:
            FrameR=FrameR.append(R[i])
    #ResFrame=DriveStartResult(FrameR)
    sc.stop()

    return FrameR

if  __name__ == "__main__":

    #try:
    #    sc = SparkContext()
    #    print('Making sc')
    #except:
    #    print('Spark Context already exists')
    #NumCores=MP.cpu_count();


    FrameR=pd.read_csv('DriveResults.csv')
    E=DriveStartResult(FrameR)

    #count1=sc.parallelize(range(len(GameID)),NumCores)
    #count2=count1.map(lambda x: DriveResult(GameID[x,:]))
    #R=count2.collect()

    #FrameR=pd.DataFrame(columns=('Field Position','Result'))
    #for i in range(len(R)):
    #    if len(np.shape(R[i]))==2:
    #        FrameR=FrameR.append(R[i])
    #ResFrame=DriveStartResult(FrameR)
    #sc.stop()

