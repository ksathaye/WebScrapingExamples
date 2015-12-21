# -*- coding: utf-8 -*-
"""
Created on Sat Dec 12 09:58:12 2015

@author: kiransathaye
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import os
from geopy.geocoders import GoogleV3 as G3
import matplotlib.pyplot as plt

def GeocodeUni():
    GMapsKeyLoc=os.path.expanduser('~')+'/Documents/'
    GMapsKey=open(GMapsKeyLoc+'GMapsKey.txt').read()
    gl = G3()
    gl.api_key=GMapsKey
    #gl=Nominatim()
    uniList=getUniList()
    LongLatUni=np.zeros([len(uniList),2])
    for i in range(len(LongLatUni)):
        try:
            Coords=gl.geocode(uniList.GEOID[i],exactly_one=True)
            LongLatUni[i,0]=Coords.longitude
            LongLatUni[i,1]=Coords.latitude
            print(uniList.School[i]+' Found')
            if Coords.longitude>0:
                print('Wrong Location')

        except:
            LongLatUni[i,0]=np.nan
            LongLatUni[i,1]=np.nan
            print(uniList.School[i]+' Not Found')

    uniList['Latitude']=LongLatUni[:,1]
    uniList['Longitude']=LongLatUni[:,0]
    uniList.to_csv('LinkedinCollegeID.csv')


def getCityNum(t,name):
    rw=str(t).split('code":"us:')

    cityList=list()
    countList=list()

    for i in range(1,len(rw)):
        numInd1=rw[i].find('count":"')+8
        numInd2=rw[i][numInd1:].find('",')
        cityCount=rw[i][numInd1:numInd2+numInd1].replace(',','')
        cityCount=int(cityCount)
        nameInd1=rw[i].find('"name":"')+8
        nameInd2=rw[i][nameInd1:].find('","')
        cityName=rw[i][nameInd1:nameInd2+nameInd1]
        cityName=cityName.replace('Greater','')
        cityName=cityName.replace('Area','')
        cityName=cityName.replace('\u002d','-')
        cityName=cityName.replace('Metro','')
        cityName=cityName.rstrip()
        cityName=cityName.lstrip()
        cityList.append(cityName)
        countList.append(cityCount)

    P=pd.DataFrame(columns=('city',name))
    P.city=pd.Series(cityList)
    P[name]=pd.Series(countList)
    return P


def getUniList():
    L=pd.read_csv('LinkedinCollegeID2.csv').sort(columns='USnewsrank')
    return L


def GetDataFromWeb():

    try:
        LinkedinKeyLoc=os.path.expanduser('~')+'/Documents/'
        text_file=open(LinkedinKeyLoc+"LIDpass.txt", "r")
        login=text_file.readline()[0:-1]
        pw=text_file.readline()[0:-1]
        text_file.close()
    except:
        login= raw_input('Enter Email: ')
        pw=raw_input('Enter Linkedin Password: ')

    client = requests.Session()
    HOMEPAGE_URL = 'https://www.linkedin.com'
    LOGIN_URL = 'https://www.linkedin.com/uas/login-submit'

    html = client.get(HOMEPAGE_URL).content
    soup = BeautifulSoup(html)
    csrf = soup.find(id="loginCsrfParam-login")['value']

    login_information = {
        'session_key':login,
        'session_password':pw,
        'loginCsrfParam': csrf,}

    client.post(LOGIN_URL, data=login_information)

    uniList=getUniList()
    ID=uniList.ID[0]
    name=uniList.School[0]

    CL=client.get('https://www.linkedin.com/edu/alumni?id='+str(ID))
    rw=CL.text
    rw = rw.encode('ascii', 'ignore').decode('ascii')
    #text_file = open("RiceUniCities.txt", "w")
    #text_file.write(rw)
    #text_file.close()
    P=getCityNum(rw,name)

    for i in range(1,len(uniList)):

        ID=uniList.ID[i]
        name=uniList.School[i]
        print(name)

        CL=client.get('https://www.linkedin.com/edu/alumni?id='+str(ID))
        rw=CL.text
        rw = rw.encode('ascii', 'ignore').decode('ascii')
        P2=getCityNum(rw,name)
        P=pd.merge(P,P2,how='outer')
    P.to_csv('UniData.csv')
    return P


def TopSFNYC(NumSchools,UniData):

    SFpop=8.469854
    NYpop=23.484225
    SF=L[2:3]
    SF=np.array(SF[SF.columns[3:NumSchools+3]])
    NYC=L[1:2]
    NYC=np.array(NYC[NYC.columns[3:NumSchools+3]])
    A=L[0:1]
    A=np.array(A[A.columns[3:NumSchools+3]])
    ax = plt.gca()

    NYC=NYC/A
    SF=SF/A
    b=np.zeros([np.size(NYC),2])
    b[:,0]=NYC
    b[:,1]=SF
    NYplus=b[b[:,0]/b[:,1]>NYpop/SFpop,:]
    SFplus=b[b[:,0]/b[:,1]<NYpop/SFpop,:]

    ax.scatter(NYplus[:,0],NYplus[:,1],s=50)
    ax.scatter(SFplus[:,0],SFplus[:,1],s=50,c='red')

    ax.set_yscale('log')
    ax.set_xscale('log')

    p=plt.plot([0.01,NYpop],[0.01*SFpop/NYpop,SFpop],c='k',lw=2,label='Population\nProportion')
    plt.xlim([.01,1])
    plt.ylim([.01,1])
    plt.text(.45,.015,'N=' +str(len(NYplus)),color='b',fontsize=16)
    plt.text(.015,.5,'N=' +str(len(SFplus)),color='red',fontsize=16)
    plt.legend(handles=p)

    plt.xlabel('Fraction in New York City Area')
    plt.ylabel('Fraction in SF Bay Area')
    plt.title('University Alumni by City')
    plt.savefig('SFNYCscatter.pdf')

    return A

def CityPair(L):
    L2=L[L.Population>0]
    col=L2.columns
    L2.rename(columns={col[0]:'cityID'}, inplace=True)
    print(L2.city[1:60])

    c1=input('Enter City ID  1: ')
    c2=input('Enter City ID 2: ')
    NumSchools=input('Enter Number Universities: ')
    c1Name=list(L2.city[c1==L2.cityID])[0]
    c2Name=list(L2.city[c2==L2.cityID])[0]
    Pop1=float(L2[L2.cityID==c1].Population)
    Pop2=float(L2[L2.cityID==c2].Population)

    C1D=L2[L2.cityID==c1]
    C1D=np.array(C1D[C1D.columns[3:NumSchools+3]])

    C2D=L2[L2.cityID==c2]
    C2D=np.array(C2D[C2D.columns[3:NumSchools+3]])
    A=L[0:1]
    A=np.array(A[A.columns[3:NumSchools+3]])
    ax = plt.gca()

    C1D=C1D/A
    C2D=C2D/A
    b=np.zeros([np.size(C1D),2])
    b[:,0]=C1D
    b[:,1]=C2D
    c1plus=b[b[:,0]/b[:,1]>Pop1/Pop2,:]
    c2plus=b[b[:,0]/b[:,1]<Pop1/Pop2,:]
    ax.scatter(c1plus[:,0],c1plus[:,1],s=50)
    ax.scatter(c2plus[:,0],c2plus[:,1],s=50,c='red')

    ax.set_yscale('log')
    ax.set_xscale('log')
    p=plt.plot([0.01,Pop1],[0.01*Pop2/Pop1,Pop2],c='k',lw=2,label='Population\nProportion')
    plt.plot([.01,1],[.01,1],ls='--',color='k')
    plt.xlim([.01,1])
    plt.ylim([.01,1])
    plt.text(.45,.015,'N=' +str(len(c1plus)),color='b',fontsize=16)
    plt.text(.015,.5,'N=' +str(len(c2plus)),color='red',fontsize=16)
    #plt.legend(handles=p)
    plt.xlabel('Fraction in ' + c1Name)
    plt.ylabel('Fraction in ' + c2Name)
    plt.title('University Alumni by City')


if '__main__':
    try:
        L=pd.read_csv('UniData.csv')
    except:
        L=GetDataFromWeb()
    #A=TopSFNYC(75,L)
    CityPair(L)