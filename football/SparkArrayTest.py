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

def DriveResult(GameObj):
    NumDrives=len(GameObj.data['drives'])-1
    OutList=[0]*(NumDrives-1)
    FieldPosStart=np.zeros(NumDrives-1)
    for i in range(1,NumDrives):
        D=GameObj.drives.number(i)
        FieldPosStart=50-D.field_start.offset
        Result=D.result
        OutList[i-1]=(FieldPosStart,Result)
        print(OutList[i-1])
    return OutList


if  __name__ == "__main__":

    try:
        sc = SparkContext()
        print('Making sc')
    except:
        print('Spark Context already exists')
    NumCores=MP.cpu_count();

    DRAll=list()
    weekRange=range(1,17)

    #DR=DriveResult(G1)
    #count1=sc.parallelize(range(len(games)),NumCores)
    #count2=count1.map(lambda x: DriveResult(games[x]))
    #print('Collecting Results')
    #R=count2.collect()

    #DriveSQL=DriveAssign(Drive,sc)
    #print(DriveSQL.count())
    #DriveSQL2=DriveAssign(Drive2,sc)
    #DriveSQL=DriveSQL.unionAll(DriveSQL2)
    #print(DriveSQL.count())
    sc.stop()

#A=SparkPandasTest(sc)