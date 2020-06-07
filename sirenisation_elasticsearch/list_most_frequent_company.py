#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 24 10:15:25 2018

@author: mathi
"""

import pandas as pd
import os

os.chdir(u"/Users/mathi/Documents/XXX/201806_sirenisation_ADM/Projet_sirenisation_ADM")

res_df_filename = u"./output/20180724_V3_recap_results_sirenisation.csv"
df = pd.read_csv(res_df_filename, dtype=unicode, sep=";", encoding="utf-8")
df[u"nb_occ"] = 1

df_group = df.groupby(by=[u'company_name_normalized'], as_index=False).aggregate({u"nb_occ": sum, }).sort_values(by=u"nb_occ", ascending=False)

df_group.nb_occ.sum()
df_group.nb_occ[:50].sum()

df_group.to_csv(u"./output/list_most_frequent_company.csv", sep=";", encoding="utf-8", index=False)









