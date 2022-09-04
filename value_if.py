# -*- coding: utf-8 -*-
"""
Created on Sat Sep  3 23:39:58 2022

@author: lenovo
"""

def value_if(in_values, in_names, statement, tru_result, fal_result):
    s_in_names = ""
    for i in range(len(in_names)):
        if i > 0:
            s_in_names = s_in_names + ","
        s_in_names = s_in_names + in_names[i]
    
    condi = eval("lambda " + s_in_names + ":" + statement)
    
    s_in_values = ""
    for i in range(len(in_values)):
        if i > 0:
            s_in_values = s_in_values + ","
        s_in_values = s_in_values + f"in_values[{i}]"
    
    
    
    if eval("condi("+s_in_values+")"):
        return tru_result
    else:
        return fal_result
