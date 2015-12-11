# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 10:20:10 2015

Markov Chain Text generation test

@author: kiransathaye
"""

import requests
from pymarkovchain import MarkovChain
import os

ConfLink='http://www.patriots.com/news/2015/10/21/bill-belichick-press-conference-transcript-1021'
loc=os.getcwd()

Site=requests.get(ConfLink)
T=Site.text
BB=T.split('BB: </strong>')
BB=BB[1:]
for i in range(len(BB)):
    BB[i]=BB[i].split('<a href=')[0]

mc = MarkovChain()
mc.generateDatabase("This is a string of Text. It won't generate an interesting database though.")
