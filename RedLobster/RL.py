# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 13:16:52 2015

@author: kiransathaye
"""

import numpy as np
import urllib2
import matplotlib.pyplot as plt
import csv
import re
from mpl_toolkits.basemap import Basemap
import pandas as pd
from matplotlib.patches import Polygon
import requests as rq
import matplotlib as mpl
import multiprocessing as MP


def ZipCodeSpark():

    try:
        from pyspark import SparkContext
        import pyspark as spark
        NumCores=MP.cpu_count();
        print('Spark Loaded')
    except:
        print('no Spark Package installed')
        return -1
    try:
        #LatLong=np.loadtxt('RedLobsterLoc.csv')
        LatLong=pd.read_csv('RedLobsterLoc.csv')
        print('Lobsters Loaded')
    except:
        LatLong=LobsterScrapeUSA();
    LatLong=pd.DataFrame(LatLong,columns=('Latitude','Longitude','Postal Code','StateID'))
    try:
        ZipPop=pd.read_csv('ZipPop.csv')
        ZipLoc=pd.read_csv('USPostalCodes.csv')
        print('Zips Loaded')
    except:
        print('No Zip Data')
        return -1

    ZipPop.rename(columns={'Zip Code ZCTA':'Postal Code'},inplace=True)
    ZipCodes=pd.merge(ZipPop,ZipLoc,on='Postal Code')
    ZipCodesRL=pd.merge(ZipCodes,LatLong,on='Postal Code')
    m=Basemap(projection='merc',llcrnrlat=24,urcrnrlat=51,llcrnrlon=-125,urcrnrlon=-65,lat_ts=20,resolution='c')

    return ZipCodes

#def GetTravelTime(Add1,Add2):

def PlotRL(LatLong,res):
    m = Basemap(projection='merc',llcrnrlat=24,urcrnrlat=51,llcrnrlon=-125,urcrnrlon=-65,lat_ts=20,resolution=res)
    m.drawcoastlines();
    m.drawcountries(linewidth=2,zorder=3);
    m.drawstates(zorder=3);
    LocationsCoord=m(LatLong['Longitude'],LatLong['Latitude']);
    m.drawmapboundary(fill_color='aqua',zorder=1)
    m.fillcontinents(color='w',lake_color='aqua')
    m.scatter(LocationsCoord[0],LocationsCoord[1],s=25,edgecolor='k',c='red',zorder=3);
    plt.title('United States of Red Lobster')
    meridians = np.arange(10.,351.,10.)
    parallels = np.arange(0.,81,5.)
    m.drawparallels(parallels,labels=[False,True,True,False])
    m.drawmeridians(meridians,labels=[True,False,False,True])
    #shp_info = m.readshapefile('st99_d00','states',drawbounds=True)
    plt.savefig('RLPlot.png')


def PlotRLDense(States):

    m = Basemap(llcrnrlon=-126,llcrnrlat=24,urcrnrlon=-64,urcrnrlat=51,
            projection='merc',lat_1=33,lat_2=45,lon_0=-95)
    shp_info = m.readshapefile('st99_d00','states',drawbounds=True)
    meridians = np.arange(-125.,-60.,10.)
    parallels = np.arange(0.,81,5.)
    m.drawparallels(parallels,labels=[True,False,False,False])
    m.drawmeridians(meridians,labels=[True,False,False,True])
    plt.title('Red Lobsters Per Million Persons')
    colors={}
    statenames=[]
    cmap = plt.cm.seismic
    vmin = 0;
    vmax = np.max(States['RL Per Million Person'])

    FakeX=np.linspace(20,50,100)
    FakeY=np.linspace(-20,-50,100)
    FakeC=np.linspace(vmin,vmax,100)

    for shapedict in m.states_info:
        statename = shapedict['NAME']
        if statename not in ['District of Columbia','Puerto Rico','Alaska','Hawaii']:
            pop = float(States[States['State']==statename]['RL Per Million Person'])
            colors[statename] = cmap(pop/(vmax-vmin))
        statenames.append(statename)
    for nshape,seg in enumerate(m.states):
        xx,yy = zip(*seg)
        if statenames[nshape] not in ['District of Columbia','Puerto Rico','Alaska','Hawaii']:
            color = colors[statenames[nshape]]
            C=plt.fill(xx,yy,color=np.array(color))
    f=plt.scatter(FakeX,FakeY,c=FakeC,cmap=cmap)
    plt.colorbar(orientation='horizontal')
    plt.savefig('RLDensity.pdf')

def GetState(state,stateID):

    Site='http://www.yellowpages.com/search?search_terms=red+lobster&geo_location_terms=' + state
    #F=urllib2.urlopen(Site).read();
    F=rq.get(Site).text
    stringlist=GetStrings(F)

    while ifNext(F)!=-False:
        F=ifNext(F)
        stringlist=stringlist+GetStrings(F)
        print('Next Page')

    LatLong=GetDataFromStrings(stringlist)
    if len(LatLong)==0:
        #LatLong=np.zeros([1,4])*np.nan
        LatLong=pd.DataFrame(columns=('Latitude','Longitude','Postal Code','StateID','Address'))
    LatLong['StateID']=state

    return LatLong

def GetStrings(sitestring):

    F=sitestring
    mend=list();
    mstart=list()
    for m in re.finditer('"name":"Red Lobster"', F):
        mend.append(m.end())
        mstart.append(m.start())
    mend=np.array(mend)
    mstart=np.array(mstart)
    try:
        mstart[0]=0;
    except:
        return list()
    stringlist=range(len(mstart))
    StringStart=np.zeros(len(mstart))
    StringEnd=np.zeros(len(mstart))

    for a in range(len(mend)):
        StringEnd[a]=F[mend[a]:].find('}')
    StringEnd=StringEnd+mend;
    StringStart[1:]=StringEnd[0:-1]+2;
    StringStart[0]=F[0:int(StringEnd[0])].rfind('{')

    for a in range(len(mend)):
        stringlist[a]=F[int(StringStart[a]):int(StringEnd[a])]

    return stringlist

def ifNext(F):

    nextFind='rel="next" href="'
    nextBool=F.find(nextFind)
    if nextBool==-1:
        return False
    else:
        nextBool2=F[nextBool:].find('>')+nextBool
        nextLink=F[nextBool+17:nextBool2-2]
        #nextSite=urllib2.urlopen(nextLink).read();
        nextSite=rq.get(nextLink).text
        print(nextLink)
        return nextSite


def GetDataFromStrings(stringlist):

    LatLong=np.zeros([len(stringlist),4])
    Address=list()
    for a in range(len(stringlist)):
        LatString=stringlist[a].find('latitude')
        LongString=stringlist[a].find('longitude')
        ZipString=stringlist[a].find('zip":')
        AdString=stringlist[a].find('addressLine1":"')
        CityString=stringlist[a].find('city":"')

        try:
            LatLong[a,2]=float(stringlist[a][ZipString+6:ZipString+11])
        except:
            LatLong[a,2]=-1
        try:
            LatLong[a,0]=float(stringlist[a][LatString+10:LatString+17])
        except:
            try:
                LatLong[a,0]=float(stringlist[a][LatString+10:LatString+16])
            except:
                LatLong[a,0]=np.nan
        try:
            LatLong[a,1]=float(stringlist[a][LongString+11:LongString+18])
        except:
            try:
                LatLong[a,1]=float(stringlist[a][LongString+11:LongString+17])
            except:
                LatLong[a,1]=np.nan
        AD=stringlist[a][AdString+15:CityString-3]
        #print(AD,)
        Address.append(AD)
    LatLong=pd.DataFrame(LatLong,columns=('Latitude','Longitude','Postal Code','StateID'))
    LatLong['Address']=Address

    return LatLong

def LobsterScrapeUSA():

    States=pd.read_csv('States.csv')
    S=list(States['Abbreviation'])
    LatLong=GetState(S[0],0)
    #LatLong=np.zeros([1,4])*np.nan
    for i in range(len(S)):
        LatLong=LatLong.append(GetState(S[i],i))
    #PlotRL(LatLong,'c')
    return LatLong

def LoadLobster():

    try:
        LatLong=np.loadtxt('RedLobsterLoc.csv')
        return LatLong
    except:
        print('No Lobster')
        return False

def MapLoadLobster():

    try:
        LatLong=np.loadtxt('RedLobsterLoc.csv')
        PlotRL(LatLong,'c')
        print('Loaded Lobster')
        return 1
    except:
        print('No Lobster')
        return False

def LobsterStates(PlotDenseMap):

    LatLong=LoadLobster();
    States=pd.read_csv('States.csv')
    USpop=322000000
    LatLongNan=np.isnan(LatLong[:,0])
    LatLong=LatLong[LatLongNan==0,:]
    print('RL Per Million USA: '+ str(1e6*float(len(LatLong))/USpop))
    Count=np.zeros(len(States))
    for i in range(len(States)):
        Count[i]=np.sum(LatLong[:,3]==i)
    Count=pd.DataFrame(Count)
    Count=Count.rename(columns={'0':'Red Lobster Count'})
    States['Lobster Count']=Count
    States['RL Per Million Person']=1e6*States['Lobster Count']/States['Population2013']
    #PlotRLDense(States)

    #plt.scatter(States['Population2013']*1e-6,States['Lobster Count'],s=100)
    #plt.xlim([0,40])
    #plt.ylim([0,70])
    #plt.xlabel('Population (millions)')
    #plt.ylabel('Number Red Lobsters')
    #plt.plot([0,40],[0,40*1e6*float(len(LatLong))/USpop],c='k',lw=2)

    #plt.figure()
    if PlotDenseMap==True:
        PlotRLDense(States)

    return States

#States=LobsterStates(True)
#ZC=ZipCodeSpark()