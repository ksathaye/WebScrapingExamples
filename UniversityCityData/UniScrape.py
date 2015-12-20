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
    L=pd.read_csv('LinkedinCollegeID.csv').sort(columns='USnewsrank')
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

if '__main__':
    GetDataFromWeb()
