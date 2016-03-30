# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 13:13:15 2016


Test Nearest Neighbor for Mens Wearhouse with Spark

@author: kiransathaye
"""
import pyspark as spark
from pyspark import SparkContext
import multiprocessing as MP

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import pandas as pd
import time

def CallDistanceSpark(Logins,W):
    NumCores=MP.cpu_count();
    try:
        sc = SparkContext()
        print('Making sc')
    except:
        print('Spark Context already exists')

    count1 = sc.parallelize(range(len(Logins)),NumCores)
    t=time.time();
    count2=count1.map(lambda x: CircleVec(Logins[x,:],W))
    Dist=count2.collect()
    SparkTime=time.time()-t;
    print(['Great Circle Spark Time: ' + str(int(SparkTime*1e3)) +' milliseconds with ' + str(NumCores) +' CPUs'])

    sc.stop()

    return Dist

def RunSeriesLoop(Logins,W):

    t1=time.time()
    DistVec=list()
    for i in range(len(Logins)):
        DistVec.append(CircleVec(Logins[i,:],W))
    t1=time.time()-t1
    print('\n')
    print(t1)
    return DistVec

def CircleVec(Point1,PointsIn):
    lat1=Point1[0]
    lat2=PointsIn[0]
    long1=Point1[1]
    long2=PointsIn[1]

    deg2rad = np.pi/180.0
    phi1 = (90.0 - lat1)*deg2rad
    phi2 = (90.0 - lat2)*deg2rad
    theta1 = long1*deg2rad
    theta2 = long2*deg2rad
    cos = (np.sin(phi1)*np.sin(phi2)*np.cos(theta1 - theta2) +
           np.cos(phi1)*np.cos(phi2))
    arc = np.arccos(cos)*6373;
    MININD=np.argmin(arc)
    if 1==0:
        print('Login Location')
        print(Point1[::-1])
        print('Nearest Wearhouse')
        print(long2[MININD],lat2[MININD])
        print([str(min(arc)) + ' km'])
        print('\n')

    return MININD # returns great circle distance between points in km


def BasePlot(W,Logins,res):
    m = Basemap(projection='merc',llcrnrlat=24,urcrnrlat=51,llcrnrlon=-125,urcrnrlon=-65,lat_ts=20,resolution=res)
    m.drawcoastlines();
    m.drawcountries(linewidth=2,zorder=3);
    m.drawstates(zorder=3);
    LocationsCoord=m(np.array(W['Longitude']),np.array(W['Latitude']));
    m.drawmapboundary(fill_color='aqua',zorder=1)
    m.fillcontinents(color='w',lake_color='aqua')
    m.scatter(LocationsCoord[0],LocationsCoord[1],s=50,edgecolor='k',c='b',zorder=3);
    plt.title('Men' + "'" + 's Wearhouse Locations')
    meridians = np.arange(10.,351.,10.)
    parallels = np.arange(0.,81,5.)
    m.drawparallels(parallels,labels=[False,True,True,False])
    m.drawmeridians(meridians,labels=[True,False,False,True])

    LoginsCoord=m(np.array(Logins['Longitude']),np.array(Logins['Latitude']));
    m.scatter(LoginsCoord[0],LoginsCoord[1],s=25,edgecolor='k',c='r',zorder=2);

    return WLand
    #plt.savefig('WearPlot.pdf')

def MakeSynthetic(N):

    Logins=np.zeros([N,2])
    Logins[:,0]=np.random.rand(N)*60-125
    Logins[:,1]=np.random.rand(N)*25+25
    W=LoadWearhouse(0)

    #DistVec=RunSeriesLoop(Logins,W)
    DistVec=CallDistanceSpark(Logins,W)

    return DistVec

def LoadWearhouse(PD):
    W=pd.read_csv('Wearhouse.csv')
    W2=pd.DataFrame(columns=('Longitude','Latitude'))
    W2['Latitude']=W.Latitude
    W2['Longitude']=W.Longitude

    if PD==0:
        WTuple=(np.array(W.Longitude),np.array(W.Latitude))
        return WTuple
    return W2

if "__main__":
    N=int(1e5)
    WLand=MakeSynthetic(N)
    Wearhouses=LoadWearhouse(1)