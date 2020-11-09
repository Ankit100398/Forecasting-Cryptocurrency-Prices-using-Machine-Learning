#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 18:39:55 2020

@author: student
"""

from ProjETLFinalFinal import Project
from ProjAlgoFinalFinal import Prophet

p=Project()
p.download()
p.hadoop_load()
p.hive_load()

a=Prophet()

def BTCFlask():
    a.ProphetBTC()

def ETHFlask():
    a.ProphetETH()

def LTCFlask():
    a.ProphetLTC()
    
