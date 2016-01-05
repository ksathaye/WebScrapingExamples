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
import time
import multiprocessing as MP
from pyspark.sql import SQLContext
import matplotlib.lines as mlines


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

def DriveAssign(Drive,DriveNum):
    columnNames=('Down','ToGo','Success','Attempt','Team','DriveResult','DriveStart','DownGo','FieldPosition','GameKey','Opponent','DriveNum')
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
    df['GameKey']=int(Drive.game.gamekey)
    if Drive.home==False:
        df['Opponent']=Drive.game.home
    else:
        df['Opponent']=Drive.game.away
    df['DriveNum']=DriveNum


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
    D=DriveAssign(1,1)
    NumDrives=len(gameObj.data['drives'])-1
    for i in range(1,NumDrives):
        Drives=gameObj.drives.number(i)
        D=D.append(DriveAssign(Drives,i))
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
    GameFrame=pd.DataFrame(columns=('GameKey','year','week','Home','Away','HomeScore','AwayScore'))
    gamekey=list()
    Away=list()
    Home=list()
    week=list()
    year=list()
    HS=list()
    AS=list()

    weekRange=range(1,18)
    for y in range(2009,2016):
        D=DriveAssign(1,1)
        games = nfl.games(year=y,week=weekRange)
        for i in range(len(games)):
            D=D.append(DrivesFromGame(games[i]))
            print('game: ' +str(i))
            gamekey.append(games[i].gamekey)
            year.append(games[i].season())
            week.append(games[i].schedule['week'])
            Home.append(games[i].schedule['home'])
            Away.append(games[i].schedule['away'])
            HS.append(games[i].score_home)
            AS.append(games[i].score_away)


        NameString='NFLPlays'+ str(y) + '.csv'
        D.to_csv(NameString)
        print('Year: '+str(y))

    GameFrame['GameKey']=gamekey
    GameFrame['year']=year
    GameFrame['week']=week
    GameFrame['Home']=Home
    GameFrame['Away']=Away
    GameFrame['HomeScore']=HS
    GameFrame['AwayScore']=AS

    GameFrame.to_csv('Gamekeys.csv')

def MapFilt(DFAll,DistDowns):
    DownFilt=DFAll[DFAll.DownGo==DistDowns]
    LenAll=len(DownFilt)
    LenSuccess=len(DownFilt[DownFilt['Success']==1])
    RateNum=np.zeros(2)
    RateNum[0]=LenSuccess/float(LenAll)
    RateNum[1]=LenAll
    return RateNum

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
    FrameR=pd.DataFrame(columns=('Field Position','Result'))
    for i in range(len(R)):
        if len(np.shape(R[i]))==2:
            FrameR=FrameR.append(R[i])
    #ResFrame=DriveStartResult(FrameR)
    sc.stop()
    FrameR.to_csv('DriveResults.csv')

    return FrameR

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

def DriveStartResultPlot(Frame):

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
    plt.title('Probability of Drive Result')

    plt.subplot(2,1,2)
    #plt.plot(ResFrame.YardsToGo-5,ResFrame.Interception,marker='o',label='Interception')
    plt.plot(ResFrame.YardsToGo-5,ResFrame.Interception+ResFrame.Fumble,marker='o',label='Fumble/INT')
    plt.plot(ResFrame.YardsToGo-5,ResFrame.Downs,marker='x',label='Downs',color='red')
    plt.plot(ResFrame.YardsToGo-5,ResFrame['Missed FG']+ResFrame['Blocked FG'],marker='s',label='Missed FG',color='g')
    plt.ylim([0,.23])
    plt.legend(ncol=3)
    plt.xlabel('Yards From End Zone')

    plt.savefig('DriveResults.pdf')
    plt.close('all')

    return ResFrame

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


if __name__ == "__main__":

    try:
        M=pd.read_csv('DriveResults.csv')
    except:
        M=MakeDriveResultFrames()

    DriveStartResultPlot(M)
    #try:
    #    PDLoad=pd.read_csv('NFLPlays2010.csv')
    #except:
    SaveSeasonPlays()
    try:
        sc = SparkContext()
        print('Making sc')
    except:
        print('Spark Context already exists')

    A=SparkPandasTest(sc)
    NP=PlotSucces(A)