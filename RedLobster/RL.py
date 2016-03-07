# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 13:16:52 2015

@author: kiransathaye
"""

import numpy as np
import matplotlib.pyplot as plt
import csv
import re
from mpl_toolkits.basemap import Basemap
import pandas as pd
from matplotlib.patches import Polygon
import requests as rq
import matplotlib as mpl
import multiprocessing as MP
import googlemaps
import time
import os
from geopy.distance import vincenty

def GreatCircle(Point1,PointsIn):
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
    arc = np.arccos(cos)*3959.871227;
    return arc # returns great circle distance between points in miles

def CircleVec(Point1,RLFrame):

    RLLatLong=np.array(RLFrame[['Longitude','Latitude']])
    AD=list(RLFrame['Address'])
    Zip=list(RLFrame['Postal Code'])
    lat1=Point1[1]
    lat2=RLLatLong[:,1]
    long1=Point1[0]
    long2=RLLatLong[:,0]

    deg2rad = np.pi/180.0
    phi1 = (90.0 - lat1)*deg2rad
    phi2 = (90.0 - lat2)*deg2rad
    theta1 = long1*deg2rad
    theta2 = long2*deg2rad
    cos = (np.sin(phi1)*np.sin(phi2)*np.cos(theta1 - theta2) +
           np.cos(phi1)*np.cos(phi2))
    arc = np.arccos(cos)*6373;
    IND=np.nanargmin(arc)
    ZIPIND=Zip[IND]
    if ZIPIND<10000:
        ZIPIND='0'+str(int(ZIPIND))
    else:
        ZIPIND=str(int(ZIPIND))

    AdZip=AD[IND]+ ' ' + ZIPIND

    return AdZip

def CircleVecRetLatLong(Point1,RLFrame):
    RLLatLong=np.array(RLFrame[['Longitude','Latitude']])
    AD=list(RLFrame['Address'])
    Zip=list(RLFrame['Postal Code'])
    lat1=Point1[1]
    lat2=RLLatLong[:,1]
    long1=Point1[0]
    long2=RLLatLong[:,0]

    deg2rad = np.pi/180.0
    phi1 = (90.0 - lat1)*deg2rad
    phi2 = (90.0 - lat2)*deg2rad
    theta1 = long1*deg2rad
    theta2 = long2*deg2rad
    cos = (np.sin(phi1)*np.sin(phi2)*np.cos(theta1 - theta2) +
           np.cos(phi1)*np.cos(phi2))
    arc = np.arccos(cos)*6373;
    IND=np.nanargmin(arc)
    return RLLatLong[IND,:]

def VincentyDistanceSpark():
    try:
        from pyspark import SparkContext
        import pyspark as spark
        NumCores=MP.cpu_count();
        print(NumCores)
        print('Spark Loaded')
    except:
        print('no Spark Package installed, running 1 CPU')
        NumCores=-1

    try:
        ZipPop=pd.read_csv('ZipPop.csv')
        ZipLoc=pd.read_csv('USPostalCodes.csv')
        print('Zips Loaded')
    except:
        print('No Zip Data')
        return -1
    try:
        LatLong=pd.read_csv('RedLobsterLoc.csv')
        print('Lobsters Loaded')
    except:
        LatLong=LobsterScrapeUSA();
    LatLong=pd.DataFrame(LatLong,columns=('Latitude','Longitude','Postal Code','StateID','Address'))

    ZipPop.rename(columns={'Zip Code ZCTA':'Postal Code'},inplace=True)
    ZipCodes=pd.merge(ZipPop,ZipLoc,on='Postal Code')
    ZipTest=np.array(ZipCodes['Postal Code'],dtype=int)
    LatLongRL=np.zeros([len(ZipCodes),2])
    LatLongRL[:,0]=np.array(ZipCodes['Longitude'])
    LatLongRL[:,1]=np.array(ZipCodes['Latitude'])
    #N=np.array(np.round(np.random.rand(2500)*len(ZipTest)),dtype=int)
    #LatLongRL=LatLongRL[N,:]
    #ZipTest=ZipTest[N]
    LatLongDist=np.zeros([len(LatLongRL),3])
    LatLongDist[:,0]=LatLongRL[:,0]
    LatLongDist[:,1]=LatLongRL[:,1]

    t=time.time()
    sc = SparkContext()
    count1 = sc.parallelize(range(len(ZipTest)),NumCores)
    count2=count1.map(lambda x: VinceSparkFunction(LatLongRL[x,:],LatLong))
    C=count2.collect()
    sc.stop()

    Cout=np.zeros([len(C),3])
    Cout[:,0]=C
    Cout[:,1]=LatLongRL[:,0]
    Cout[:,2]=LatLongRL[:,1]
    print(time.time()-t)

    Cout=pd.DataFrame(Cout,columns=('Distance','Longitude','Latitude'))
    PlotRLDistance(Cout,'c')
    return Cout


    #for i in range(len(LatLongRL)):
        #NearestRL=CircleVecRetLatLong(LatLongRL[i,:],LatLong)
        #LatLongDist[:,2]=VinceSparkFunction(LatLongRL[i,:],LatLong)
        #print(str(i) + str((LatLongDist[i,:])))

def PlotRLDistance(LatLong,res):
    LatLong=LatLong[LatLong['Longitude']>-125]
    LatLong.loc[LatLong.Distance>150,'Distance']=150
    m = Basemap(projection='merc',llcrnrlat=24,urcrnrlat=51,llcrnrlon=-125,urcrnrlon=-65,lat_ts=20,resolution=res)
    m.drawcoastlines();
    m.drawcountries(linewidth=2,zorder=3);
    m.drawstates(zorder=3);
    LocationsCoord=m(np.array(LatLong['Longitude']),np.array(LatLong['Latitude']));
    m.drawmapboundary(fill_color='aqua',zorder=1)
    m.fillcontinents(color='w',lake_color='aqua')
    cmap = plt.cm.jet

    m.scatter(LocationsCoord[0],LocationsCoord[1],s=2,edgecolor='none',c=LatLong['Distance'],zorder=3,cmap=cmap);
    plt.title('Absolute Distance to Red Lobster (miles)')
    meridians = np.arange(10.,351.,10.)
    parallels = np.arange(0.,81,5.)
    m.drawparallels(parallels,labels=[False,True,True,False])
    m.drawmeridians(meridians,labels=[True,False,False,True])
    #shp_info = m.readshapefile('st99_d00','states',drawbounds=True)

    FakeX=np.linspace(20,50,100)
    FakeY=np.linspace(-20,-50,100)
    FakeC=np.linspace(np.nanmin(np.array(LatLong['Distance'])),np.nanmax(np.array(LatLong['Distance'])),100)
    f=plt.scatter(FakeX,FakeY,c=FakeC,cmap=cmap)
    plt.colorbar(orientation='horizontal')

    plt.savefig('RLPlot.png',dpi=1000)


def VinceSparkFunction(LatLongRL,LatLongAll):
    NearestRL=CircleVecRetLatLong(LatLongRL,LatLongAll)
    VinceDist=vincenty(NearestRL,LatLongRL).miles
    if VinceDist>12000:
        VinceDist=GreatCircle(NearestRL,LatLongRL)
        print('Circle Used')
    AD=CircleVec(LatLongRL,LatLongAll)
    print(int(VinceDist),AD,NearestRL,LatLongRL)
    return VinceDist

def ZipCodeSpark():

    try:
        from pyspark import SparkContext
        import pyspark as spark
        NumCores=MP.cpu_count();
        print('Spark Loaded')
    except:
        print('no Spark Package installed, running 1 CPU')
        NumCores=-1
    try:
        LatLong=pd.read_csv('RedLobsterLoc.csv')
        print('Lobsters Loaded')
    except:
        LatLong=LobsterScrapeUSA();
    LatLong=pd.DataFrame(LatLong,columns=('Latitude','Longitude','Postal Code','StateID','Address'))
    try:
        ZipPop=pd.read_csv('ZipPop.csv')
        ZipLoc=pd.read_csv('USPostalCodes.csv')
        print('Zips Loaded')
    except:
        print('No Zip Data')
        return -1

    ZipPop.rename(columns={'Zip Code ZCTA':'Postal Code'},inplace=True)
    ZipCodes=pd.merge(ZipPop,ZipLoc,on='Postal Code')

    N=np.array(np.random.rand(2500)*len(ZipPop),dtype=int) #2500 free google directions queries
    ZipTest=np.array(ZipCodes['Postal Code'],dtype=int)
    LatLongRL=np.zeros([len(ZipCodes),2])
    LatLongRL[:,0]=np.array(ZipCodes['Longitude'])
    LatLongRL[:,1]=np.array(ZipCodes['Latitude'])
    try:
        GMapsKeyLoc=os.path.expanduser('~')+'/Documents/'
        GMapsKey=open(GMapsKeyLoc+'GMapsKey.txt').read()
    except:
        return 'Need Google Maps API Key in Documents Folder'
    T=np.zeros(len(ZipTest))

    if 1==0:
        for i in range(len(ZipTest)):
            ZipTown=ZipTest[i]
            if ZipTown<10000:
                ZipTown='0'+str(ZipTown)
            else:
                ZipTown=str(ZipTown)
            NearestRL=CircleVec(LatLongRL[i,:],LatLong)
            T[i]=TravTimeGoogle(NearestRL,ZipTown,GMapsKey)
            print('Travel Time ' + str(np.round(T[i]/60)) +' Minutes '+ NearestRL +' to ' + ZipTown )

    #ZC=np.array(ZipCodes['Postal Code'],dtype=int)

    N=np.array(np.round(np.random.rand(2500)*len(ZipTest)),dtype=int)
    print(len(LatLongRL))
    LatLongRL=LatLongRL[N,:]
    ZipTest=ZipTest[N]
    if NumCores>0:
        t=time.time()
        sc = SparkContext()
        count1 = sc.parallelize(range(len(ZipTest)),NumCores)

        try:
            count2=count1.map(lambda x: TravTimeGoogleCall(LatLongRL[x,:],ZipTest[x],LatLong,GMapsKey))
        except:
            print('Map Didnt Work')
            count2=-1
        C3=count2.collect()
        sc.stop()
        print(str(time.time()-t) + ' seconds to fire up Spark')
        Cout=np.zeros([len(C3),2])
        Cout[:,1]=C3
        Cout[:,0]=ZipTest
    else:
        Cout=np.zeros([len(ZipTest),2])
        for x in range(len(ZipTest)):
            Cout[x,1]=TravTimeGoogleCall(LatLongRL[x,:],ZipTest[x],LatLong,GMapsKey)
        Cout[:,0]=ZipTest


    #t=time.time()
    #T1=np.zeros([len(C3),2])
    #for x in range(len(ZipTest)):
    #    T1[x]=TravTimeGoogleCall(LatLongRL[x,:],ZipTest[x],LatLong,GMapsKey)


    #(str(time.time()-t) + ' seconds to run loop 1 CPU')

    return Cout

def TravTimeGoogleCall(LatLongZip,PopZip,LatLongAll,GMapsKey):

    A1=CircleVec(LatLongZip,LatLongAll)
    A2=PopZip

    if A2<10000:
        A2='0'+str(A2)

    T=TravTimeGoogle(A1,A2,GMapsKey)
    return T


def TravTimeGoogle(A1,A2,GMapsKey):

    gmaps = googlemaps.Client(key=GMapsKey)
    A1=str(A1)
    A2=str(A2)
    try:
        G = gmaps.directions(A1,A2,mode='driving')
        #print('Directions Found')
        #print('Address 2 ' + A2)
        #print('Address 1 ' + A1)

    except:
        G=np.nan
        print('No Directions Found')

    try:
        G=G[0]['legs'][0]['duration']['value']
        #print('Duration Found')
    except:
        G=np.nan
        print('Could not Extract')
    return G

def GetTravelTime(Add1,Zip1,Add2,Zip2):

    Add1=str(Add1).replace(' ','+')
    Add2=str(Add2).replace(' ','+')

    Zip1=int(Zip1)
    Zip2=int(Zip2)
    if Zip1<10000:
        Zip1='0'+str(Zip1)
    else:
        Zip1=str(Zip1)

    if Zip2<10000:
        Zip2='0'+str(Zip2)
    else:
        Zip2=str(Zip2)

    TravelLink='https://www.google.com/maps/dir/'+Zip1+'+'+Add1+'/'+Zip2+ '+'+Add2
    TravelSite=str(rq.get(TravelLink).text)
    minFirst=TravelSite.find('miles"')-1
    timestart=TravelSite[minFirst:].find(',"')+2
    timeend=TravelSite[timestart+minFirst:].find('"')
    tf=open('MapSite.txt','w')
    tf.write(TravelSite)
    tf.close()
    timeString=TravelSite[timestart+minFirst:minFirst+timestart+timeend]
    TravelTime=-1
    if timeString.find('h')==-1:
        SpaceFind=timeString.find(' ')
        try:
            TravelTime=int(timeString[0:SpaceFind])
        except:
            TravelTime=-1
    else:
        SpaceFind=timeString.find(' ')
        Hours=int(timeString[0:SpaceFind])
        if timeString.find('min')==-1:
            try:
                TravelTime=int(Hours*60)
            except:
                TravelTime=-1
                print('No Hours Find')
        else:
            minFind=timeString.find('min')-1
            minFindStart=timeString[0:minFind].rfind(' ')+1
            minutes=int(timeString[minFindStart:minFind])
            TravelTime=int(Hours*60)+minutes
            #print(str(TravelTime)+' Minutes')
    return TravelTime



def PlotRL(LatLong,res):
    m = Basemap(projection='merc',llcrnrlat=24,urcrnrlat=51,llcrnrlon=-125,urcrnrlon=-65,lat_ts=20,resolution=res)
    m.drawcoastlines();
    m.drawcountries(linewidth=2,zorder=3);
    m.drawstates(zorder=3);
    LocationsCoord=m(np.array(LatLong['Longitude']),np.array(LatLong['Latitude']));
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
    plt.savefig('RLDensity.png')

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
        LatLong=pd.read_csv('RedLobsterLoc.csv')
        return LatLong
    except:
        print('No Lobster')
        return False

def MapLoadLobster():

    try:
        LatLong=pd.read_csv('RedLobsterLoc.csv')
        print('Loaded Lobster')

    except:
        print('No Lobster')
        return False
    PlotRL(LatLong,'c')
    return 1

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

D2=VincentyDistanceSpark()
#T=ZipCodeSpark()