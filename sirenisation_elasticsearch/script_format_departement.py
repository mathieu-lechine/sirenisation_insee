#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 14:48:04 2018

@author: mathi
"""

import pandas as pd
import os
import re
import unidecode
os.chdir(u"/Users/mathi/Documents/XXX/201806_sirenisation_ADM/Projet_sirenisation_ADM/param")

#%%
def normalize_string(string_input):
    """
    Fonction de normalisation des strings/unicode:
        - suppression des espcaes avant et après
        - suppression des Tirets remplacer par des espaces
        - suppression des accents
        - mise en majuscule
    None en entrée, retourne une unicode vide : u""
    Retourne un unicode
    """
    if string_input:
        #return unidecode.unidecode(string_input.strip().replace("-", " ").
        #                           replace("\n", " ").
        #                           replace("\r", "")).upper()
        return unicode(unidecode.unidecode(string_input.strip().replace("-", " ").
                                   replace("\n", " ").
                                   replace("\r", "")).upper(), encoding = "UTF-8")
    else:
        return u""
def normalize_company_name(company):
    res = normalize_string(company)
    #remove the name SOCIETE
    res = re.sub("SOCIETE", u"", res)
    #remove multiple space
    res = re.sub("(\ ){2,}", u" ", res)
    return res.strip()

#%%

df = pd.read_csv(u"departement.csv", header = None, encoding ="utf8")

df[2] = df[2].apply(normalize_company_name)

df2export = df[[1, 2]]
df2export.columns = [u"prefix_code_postal", u"nom_departement"]
df2export.iloc[19,0] = u"201"
df2export.iloc[20,0] = u"202"
df2export.to_csv(u"df_departement.csv", encoding = "utf8", sep = ";", index =False)






