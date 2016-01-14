# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 16:32:19 2015

@author: kiransathaye
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#import nflgame as nfl
import time
#import matplotlib.lines as mlines
#import pyspark.sql.functions as SpFun
from sqlalchemy import create_engine
import sqlite3 as db
#import pandas.io.sql as pdsql

def TestDB():
    conn = db.connect('NFLDB.db')
    c=conn.cursor();

    Query='SELECT * FROM  (SELECT  year, Home, count(Gamekey) hwin FROM Games WHERE Home IN (SELECT distinct Plays.Team FROM Plays)  AND  HomeScore>AwayScore AND year in (SELECT distinct year FROM Games) GROUP BY Home, year ORDER BY Home)  '
    HomeWins=c.execute(Query).fetchall()

    Query='SELECT year, Away, count(Gamekey) FROM Games WHERE Away IN (SELECT distinct Plays.Team FROM Plays)  AND  HomeScore<AwayScore AND year in (SELECT distinct year FROM Games) GROUP BY Away, year'
    AwayWins=c.execute(Query).fetchall()

    Query='SELECT year, Team, count(P.Gain), avg(Gain)   FROM Plays P, Games G WHERE P.Team IN (SELECT distinct Plays.Team FROM Plays) AND year IN (SELECT distinct year FROM Games) AND P.Down=1 AND P.ToGo=10  AND P.Gamekey=G.Gamekey GROUP BY Team, year ORDER BY Team, year'
    First10Total=c.execute(Query).fetchall()

    Query='SELECT year, Team, count(Gain) FROM Plays, Games WHERE Team IN (SELECT distinct Plays.Team FROM Plays) AND year IN (SELECT distinct year FROM Games) AND Plays.Down=1 AND Plays.ToGo=10 AND Gain>4 AND Plays.Gamekey=Games.Gamekey GROUP BY Opponent, year ORDER BY Team, year'
    First10Success=c.execute(Query).fetchall()

    Query='SELECT year, Opponent, count(Gain) FROM Plays, Games WHERE Opponent IN (SELECT distinct Plays.Opponent FROM Plays) AND year IN (SELECT distinct year FROM Games) AND Plays.Down=1 AND Plays.ToGo=10 AND Gain>4 AND Plays.Gamekey=Games.Gamekey GROUP BY Opponent, year ORDER BY Opponent, year'
    First10SuccessOpp=c.execute(Query).fetchall()

    Query='SELECT year, Opponent, count(P.Gain), avg(Gain) FROM Plays P, Games G WHERE P.Opponent IN (SELECT distinct Plays.Opponent FROM Plays) AND year IN (SELECT distinct year FROM Games) AND P.Down=1 AND P.ToGo=10  AND P.Gamekey=G.Gamekey GROUP BY Opponent, year ORDER BY Opponent, year'
    First10TotalOpp=c.execute(Query).fetchall()

    Query='SELECT year, Team, count(Gain) FROM Plays P, Games G WHERE Down=3 AND Gain>=ToGo AND Team IN (SELECT distinct Team FROM Plays) AND year IN (SELECT distinct year FROM Games) AND P.Gamekey=G.Gamekey GROUP BY Team, year ORDER BY Team, year'
    ThirdDownSuccess=c.execute(Query).fetchall()

    Query='SELECT year, Team, count(Gain) FROM Plays P, Games G WHERE Down=3 AND Team IN (SELECT distinct Team FROM Plays) AND year IN (SELECT distinct year FROM Games) AND P.Gamekey=G.Gamekey GROUP BY Team, year ORDER BY Team, year'
    ThirdDownAttempts=c.execute(Query).fetchall()

    conn.close()

    P=pd.DataFrame(HomeWins, columns=('year','Team','HomeWins'))
    PA=pd.DataFrame(AwayWins, columns=('year','Team','AwayWins'))
    First10Total=pd.DataFrame(First10Total,columns=('year','Team','First10Plays','AveGainFirst10'))
    First10Success=pd.DataFrame(First10Success,columns=('year','Team','First10Success'))
    First10SuccessOpp=pd.DataFrame(First10SuccessOpp,columns=('year','Team','First10SuccessOpp'))
    First10TotalOpp=pd.DataFrame(First10TotalOpp,columns=('year','Team','First10TotalOpp','AveGainOppFirst10'))
    ThirdDownSuccess=pd.DataFrame(ThirdDownSuccess,columns=('year','Team','ThirdDownSuccesses'))
    ThirdDownAttempts=pd.DataFrame(ThirdDownAttempts,columns=('year','Team','ThirdDownAttempts'))

    P=P.merge(PA,how='outer')
    P=P.merge(First10Total,how='outer')
    P=P.merge(First10Success,how='outer')
    P=P.merge(First10SuccessOpp,how='outer')
    P=P.merge(First10TotalOpp,how='outer')
    P=P.merge(ThirdDownSuccess,how='outer')
    P=P.merge(ThirdDownAttempts,how='outer')

    FourthDowns=PuntDrives()

    P=P.merge(FourthDowns,how='outer')
    P.to_csv('TeamAggregateData.csv')

    return P

def PuntDrives():

    conn = db.connect('NFLDB.db')
    c=conn.cursor();

    cursor = c.execute('select * from Plays')
    rowt = c.description
    colList=list()
    for i in range(len(rowt)):
        colList.append(rowt[i][0])
    print(colList)

    GetDrivesQuery='SELECT distinct PL.GameKey, PL.DriveNum, PL.c, PL.DriveResult FROM (SELECT  GameKey, DriveNum, count(Down) c, DriveResult FROM Plays WHERE Down=4  GROUP BY GameKey, DriveNum) PL  WHERE  DriveResult != "Punt" AND DriveResult != "Field Goal" AND DriveResult != "Missed FG" AND DriveResult != "Blocked Punt" AND DriveResult != "End of Half"  AND DriveResult != "Blocked FG" AND DriveResult != "End of Game" OR c>1'

    GetPlaysQuery='SELECT  Plays.ToGo ToGo, Plays.Team Team , dq.Gamekey Gamekey, dq.DriveNum DriveNum, Plays.FieldPosition FieldPosition, Plays.DriveResult DriveResult, Plays.Gain Gain  FROM ' + '(' + GetDrivesQuery +' ) dq, Plays WHERE dq.DriveNum=Plays.DriveNum AND dq.GameKey=Plays.Gamekey AND Down=4'

    GetNonPunt='SELECT gpq.DriveResult,  gpq.Team Team, gpq.GameKey, gpq.FieldPosition FieldPosition, gpq.Gain Gain, gpq.DriveNum DriveNum, gpq.ToGo ToGo FROM ' + '(' + GetPlaysQuery + ') gpq WHERE DriveResult != "Punt" AND DriveResult != "Field Goal" AND DriveResult != "Missed FG" AND DriveResult != "Blocked Punt"  AND DriveResult != "Blocked FG" OR Gain>ToGo'

    GetNonPunt2='SELECT gnp.Gamekey Gamekey, Team, Gain, DriveNum, ToGo, year, DriveResult FROM ( ' + GetNonPunt + ') gnp, Games WHERE GAMES.Gamekey=gnp.Gamekey'

    TotalSuccess='SELECT * FROM ((SELECT  count(Gamekey) Convers FROM (' + GetNonPunt2 + ') WHERE Gain>ToGo), (SELECT count(Gamekey) Attempt FROM (' +GetNonPunt2 + '))) '

    GroupTeamYear='SELECT Team, year, FourthAttempt, FourthSuccess FROM ((SELECT Team, year, count(Gamekey) FourthAttempt FROM (' + GetNonPunt2 + ')  GROUP BY Team, year), (SELECT  Team Team2, year year2, count(Gamekey) FourthSuccess FROM (' + GetNonPunt2 + ') WHERE ToGo<=Gain GROUP BY Team, year)) WHERE Team2=Team AND year=year2'

    S=c.execute(GroupTeamYear)
    ColNames=S.description
    colList=list()
    for i in range(len(ColNames)):
        colList.append(ColNames[i][0])
    print(colList)

    FourthDownAttempts=S.fetchall()
    FourthDownAttempts=pd.DataFrame(FourthDownAttempts,columns=colList)

    return FourthDownAttempts

def MakeDB():

    year=2009
    FN='NFLPlays' +str(year)+'.csv'
    P=pd.read_csv(FN)
    for year in range(2010,2016):
        FN='NFLPlays' +str(year)+'.csv'
        P=P.append(pd.read_csv(FN))
        #SeasonFiles=('NFLPlays')
    P.drop('DownGo',inplace=True,axis=1)
    P.drop('Attempt',inplace=True,axis=1)
    P.drop('Unnamed: 0',inplace=True,axis=1)
    P['Gain']=P.FieldPosition.diff(periods=-1)
    P.loc[P.DriveNum.diff(periods=-1)!=0,'Gain']=np.nan

    TF=(P.DriveResult=='Touchdown') & np.isnan(P.Gain)
    P.Gain[TF==True]=P.FieldPosition[TF==True]
    TF=(P.DriveResult != 'Touchdown') & np.isnan(P.Gain) & (P.DriveResult != 'Field Goal')
    P.Gain[TF==True]=0

    GK=pd.read_csv('Gamekeys.csv')
    engine = create_engine('sqlite:///NFLDB.db')
    GK.to_sql('Games', engine,if_exists='replace')
    P.to_sql('Plays', engine,if_exists='replace')

def TeamYearCorr():
    HW=TestDB()
    HW.HomeWins[np.isnan(HW.HomeWins)]=0
    HW.AwayWins[np.isnan(HW.AwayWins)]=0
    HW['FourthRate']=HW.FourthSuccess/HW.FourthAttempt
    HW['ThirdRate']=HW.ThirdDownSuccesses/HW.ThirdDownAttempts
    HW['FirstRate']=HW.First10Success/HW.First10Plays
    HW['TotalWins']=HW.HomeWins+HW.AwayWins

    A=np.transpose(np.array(HW[HW.columns[4:]]))
    CC=np.corrcoef(A)
    Param=HW.columns[4:-1]
    C = dict()

    for i in range(len(Param)):
        C[Param[i]]=CC[-1,i]
    return C



if '__main__':

    t=time.time()
    #S=FirstDownFiveYard()
    CC=TeamYearCorr()
    D=PuntDrives()
    print(str(time.time()-t) + ' seconds to query')