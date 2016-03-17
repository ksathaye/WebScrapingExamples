# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 10:55:50 2016

Wearhouse

@author: kiransathaye
"""

import requests as rq
import numpy as np
import pandas as pd
import re
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

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

def GetStrings(sitestring):

    F=sitestring
    mend=list();
    mstart=list()
    for m in re.finditer(',"name":"Sally Beauty Supply",', F):
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

def GetState(state,stateID):

    Site='http://www.yellowpages.com/search?search_terms=Sally+Beauty+Supply&geo_location_terms=' + state
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

def LobsterScrapeUSA():

    States=pd.read_csv('States.csv')
    S=list(States['Abbreviation'])
    LatLong=GetState(S[0],0)
    #LatLong=np.zeros([1,4])*np.nan
    for i in range(len(S)):
        LatLong=LatLong.append(GetState(S[i],i))
    #PlotRL(LatLong,'c')

    return LatLong

def PlotRL(LatLong,res):
    m = Basemap(projection='merc',llcrnrlat=24,urcrnrlat=51,llcrnrlon=-125,urcrnrlon=-65,lat_ts=20,resolution=res)
    m.drawcoastlines();
    m.drawcountries(linewidth=2,zorder=3);
    m.drawstates(zorder=3);
    LocationsCoord=m(np.array(LatLong['Longitude']),np.array(LatLong['Latitude']));
    m.drawmapboundary(fill_color='aqua',zorder=1)
    m.fillcontinents(color='w',lake_color='aqua')
    m.scatter(LocationsCoord[0],LocationsCoord[1],s=25,edgecolor='k',c='red',zorder=3);
    plt.title('Men' + "'" + 's Wearhouse Locations')
    meridians = np.arange(10.,351.,10.)
    parallels = np.arange(0.,81,5.)
    m.drawparallels(parallels,labels=[False,True,True,False])
    m.drawmeridians(meridians,labels=[True,False,False,True])
    #shp_info = m.readshapefile('st99_d00','states',drawbounds=True)
    plt.savefig('WearPlot.pdf')


def LatLongFill():
    WearHouseLL=pd.read_csv('SallyLocationsRaw.csv')

    ZipCodes=pd.read_csv('USPostalCodes.csv')
    ZC=ZipCodes[['Postal Code','Latitude','Longitude']]
    ZDict=ZC.set_index('Postal Code').T.to_dict('list')

    NanVals=WearHouseLL[np.isnan(WearHouseLL.Latitude)==True]
    NanVals=NanVals[NanVals['Postal Code']>1]

    WearHouseLL=WearHouseLL[np.isnan(WearHouseLL.Latitude)==False]
    LatLong=np.zeros([len(NanVals),2])
    c=0
    for i in NanVals.index:
        Lat=ZDict[NanVals.loc[i]['Postal Code']]
        LatLong[c,:]=Lat
        c=c+1
    NanVals.Latitude=LatLong[:,0]
    NanVals.Longitude=LatLong[:,1]
    WearHouseLL=pd.concat([WearHouseLL,NanVals])
    WearHouseLL.to_csv('SallyLocationsUSA.csv',index=False)

    return WearHouseLL


#WearHouseLL=pd.read_csv('Wearhouse.csv')
#PlotRL(WearHouseLL,'i')
NV=LatLongFill()
#LL=LobsterScrapeUSA()

