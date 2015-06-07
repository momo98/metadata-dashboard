# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 18:41:29 2015

@author: agitzes
"""

import pandas

def transform2Binary(df):
    for i in df.columns:
        if df[i].dtype not in ("int64", "float64", "datetime64"):
            for k in df[i].value_counts().index:
                Example= df[i]==k
                df[k]=0
                df[k][Example]=1
            df.drop(i,inplace=True,axis=1)
    return df
                
            
            
        
    