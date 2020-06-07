#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 14 17:33:50 2018

@author: mathieu.lechine
"""

import unidecode
import numpy as np
import pandas as pd
import os
import glob
import csv
import codecs
import yaml

#Personnalisation de la fonction de lecture YAML pour qu'elle lise tous les strings en unicode
def construct_yaml_str(self, node):
    # Override the default string handling function 
    # to always return unicode objects
    return self.construct_scalar(node)
yaml.Loader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)
yaml.SafeLoader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)


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

def convert2unicode(input_str):
    """
    Convertit l'input en string unicode (convertit None en u"")
    """
    if input_str is None:
        input_str = u""
    if type(input_str) != unicode:
            input_str = unicode(str(input_str), encoding = "UTF-8")
    return input_str

def touch(path, file_mode = 'a'):
    #open(path, file_mode).close()
    with open(path, file_mode):
        os.utime(path, None)
    return None

def aggregate_in_dictOfList(dict_features_all, dict_features):
    """
    Ajout des valeur de dict_features dans les listes de dict_features_all
    """
    for key, val in dict_features.items():
        dict_features_all[key] += [val]
    return dict_features_all


def dict_to_listOflist_ordered(dict_features_ws_all, ls_output_column_names):
    """
    Cette fonction renvoit une liste des champs du dictionnaire dict_features_ws_all.
    L'ordre des champs dans la list est défini par l'ordre des champs dans la liste ls_output_column_names.
    """
    ls_res = []
    for output_column_name in ls_output_column_names:
        ls_res.append(dict_features_ws_all[output_column_name])
    ls_res = map(list, zip(*ls_res))
    return ls_res

def None2emptyString(s):
    if s is None:
        return ''
    else:
        return s




def stat_descriptive(pd_siren):
    #import feather
    #feather.write_dataframe(pd_siren, "Data/pd_siren_nofilter.feather")
    #pd_siren = feather.read_dataframe("Data/pd_siren_nofilter.feather")
    #pd_siren.info()
    #pd_siren.head()
    nb_row = pd_siren.shape[0]
    #!py36
    #file = open("Output/stat_descriptive_SIREN_filter.txt","w", encoding = 'UTF-8') 
    file = codecs.open("Output/stat_descriptive_SIREN_filter.txt","w", encoding = 'UTF-8') 
    file.write("Taille du fichier SIREN importé {}\n".format(pd_siren.shape))
    for col in pd_siren.columns:
        nb_na_val = np.sum(pd_siren[col].isnull())
        nb_val_unique = len(pd.unique(pd_siren[col]))
        file.write("La colonne '{}' a :\n".format(col))
        file.write("\t - {} valeurs manquantes soit environ {:.2f}% des lignes\n".
                   format(int(nb_na_val), float(nb_na_val)/nb_row*100))
        file.write("\t - {} valeurs différentes soit environ {:.2f}% des valeurs sont uniques\n".
                   format(int(nb_val_unique), float(nb_val_unique)/nb_row*100))
    file.close()



def aggregate_csv_siren_results(mapping_output_path):
    allFiles = glob.glob(mapping_output_path + "/*.csv")
    csv_entete = mapping_output_path + "entete.csv"
    frame = pd.DataFrame()
    list_ = []
    for file_ in allFiles:
        if file_ != csv_entete and os.path.getsize(file_)>0:
            df = pd.read_csv(file_,index_col=None, header=None, sep = ";", decimal = ",",  encoding = 'UTF-8')
            list_.append(df)
    frame = pd.concat(list_)
    #!py36
    #with open(csv_entete, 'r', encoding = 'UTF-8') as f:
    with codecs.open(csv_entete, 'r', encoding = 'UTF-8') as f:
        reader = csv.reader(f, delimiter = ";")
        entete = list(reader)[0]
    frame.columns = entete
    frame  = frame.set_index(entete[0])
    return frame





    