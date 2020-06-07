#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 11:31:08 2018

@author: mathieu.lechine
"""

     
import os
import csv
import ntpath
import glob
import re
import inspect
import logging
import codecs
import unicodecsv
#my packages
import sirenisation.miscellaneous.tools as tools

#initialisation du logger
logger = logging.getLogger(__name__)

def autolog_msg(message):
    "Automatically log the current function details."
    # Get the previous frame in the stack, otherwise it would
    # be this function!!!
    func = inspect.currentframe().f_back.f_code
    # Dump the message + the name of this function to the log.
    return(u"{}: {} at line {}".format (
        message, 
        func.co_name,
        func.co_firstlineno
    ))

def normalize_results_for_export(dict_features_ws, numero_entreprise):
    """
    Cette fonction calcule les variables à exporter (SIREN, SIRET, RCS, APE) et vérifie
    leur format.
    Elle renvoit un dataframe d'une seule ligne avec création de colonnes 
    "NOMVARIABLE_format" a true si le format est bon et sinon à False
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    reg_SIREN = re.compile(r"^[0-9]{9}$")
    reg_SIRET = re.compile(r"^[0-9]{14}$")
    reg_APE = re.compile(r"^[0-9]{4}[A-Z]")
    SIREN_to_export = dict_features_ws[u'SIREN']
    SIRET_to_export = dict_features_ws[u'SIRET (siege)']
    LIBCOM_to_export = dict_features_ws[u'LIBCOM_ws']
    APE_to_export = dict_features_ws[u'Code APE']

    if SIREN_to_export is not None and reg_SIREN.match(SIREN_to_export):
        dict_features_ws[u"ToExport_SIREN"] = SIREN_to_export[:3] + u" " + SIREN_to_export[3:6] + u" " + SIREN_to_export[6:9]
        dict_features_ws[u"ToExport_SIREN_format"] = True 
        if SIRET_to_export is not None and reg_SIRET.match(SIRET_to_export):
            NIC_to_export = SIRET_to_export[9:14]
            dict_features_ws[u"ToExport_SIRET"] = dict_features_ws[u"ToExport_SIREN"] + u" " + NIC_to_export
            dict_features_ws[u"ToExport_SIRET_format"] = True 
        else:
            dict_features_ws[u"ToExport_SIRET"] = SIRET_to_export
            dict_features_ws[u"ToExport_SIRET_format"] = False 
        dict_features_ws[u"ToExport_RCS"] = "RCS" + " " + LIBCOM_to_export + " " + dict_features_ws[u"ToExport_SIREN"]
        dict_features_ws[u"ToExport_RCS_format"] = True if len(LIBCOM_to_export)>0 else False
    else:
        dict_features_ws[u"ToExport_SIREN"] = tools.None2emptyString(SIREN_to_export)
        dict_features_ws[u"ToExport_SIREN_format"] = False
        if SIRET_to_export is not None and reg_SIRET.match(SIRET_to_export):
            NIC_to_export = SIRET_to_export[9:14]
            dict_features_ws[u"ToExport_SIRET"] = dict_features_ws[u"ToExport_SIREN"] + u" " + NIC_to_export
            dict_features_ws[u"ToExport_SIRET_format"] = False 
        else:
            dict_features_ws[u"ToExport_SIRET"] = SIRET_to_export
            dict_features_ws[u"ToExport_SIRET_format"] = False 
        dict_features_ws[u"ToExport_RCS"] = u"RCS" + " " + tools.None2emptyString(LIBCOM_to_export) + u" " + dict_features_ws[u"ToExport_SIREN"]
        dict_features_ws[u"ToExport_RCS_format"] = False
    dict_features_ws[u"ToExport_APE"] = APE_to_export       
    dict_features_ws[u"ToExport_APE_format"] = True if (APE_to_export is not None and reg_APE.match(APE_to_export)) else False
    #ajout du numéro du demandeur
    dict_features_ws[u"NUMERO_ENTREPRISE"] = unicode(numero_entreprise)
    return dict_features_ws

def export_ws_entete_csv(webscraping_results_path, param_ws_file):
    """
    Ecriture du fichier entête des CSV
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_entete = param_ws_file[u"output_column_names"]
    #!py36
    #with open(webscraping_results_path + "entete.csv", 'w', encoding = 'UTF-8') as myfile:
    with open(webscraping_results_path + "entete.csv", 'wb') as myfile:
                wr = unicodecsv.writer(myfile, delimiter = ";", encoding = 'UTF-8')
                wr.writerow(ls_entete)
    return True

def export_ws_results2csv(dict_features_ws_all, df_siren_file, webscraping_results_path, siren_file, param_ws_file):
    """
    Ecriture des résultats contenu dans dict_features_ws_all dans le fichier csv df_siren_file
    L'ordre des colonnes est défini par param_ws_file["output_column_names"].
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ws_output_filename = "{}{}".format(webscraping_results_path, ntpath.basename(siren_file))
    if len(dict_features_ws_all.keys()) == 0:
        #export d'un fichier vide
        tools.touch(ws_output_filename, file_mode = 'w')
    else:
        #définition de l'ordre des colonnes
        ls_output_column_names = param_ws_file[u"output_column_names"]
        #écriture du dictionnaire en liste de lignes
        list2export = tools.dict_to_listOflist_ordered(dict_features_ws_all, ls_output_column_names)
        #export des résultats au format csv (attention le séparateur décimal est le point)
        #!py36
        #with open(ws_output_filename, "w", encoding = 'UTF-8') as f:
        with open(ws_output_filename, "wb") as f:
            writer = unicodecsv.writer(f, delimiter = ";", encoding = 'UTF-8')
            writer.writerows(list2export)
    return True

    
    
def get_results_siren(csv_siren_path, webscraping_results_path, overwrite = False): 
    """
    Cette fonction fait la liste des fichiers CSV siren qui doivent être traités par 
    web scraping.
    Les fichiers csv vides sont traités à la volée (création de fichiers vides dans le nouveau répertoire).
    L'option overwrite indique si l'on doit écraser les fichiers déjà traités et en regénérer de nouveaux.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_siren_files = glob.glob(csv_siren_path + "/*.csv")
    ls_webscraping_files = glob.glob(webscraping_results_path + "/*.csv")
    fichier_a_filtrer = [os.path.splitext(ntpath.basename(file))[0] for file in ls_webscraping_files] + ["entete"]
    csv_siren_entete = csv_siren_path + "entete.csv"
    #filtre des fichiers déjà traités
    if not overwrite:
        ls_siren_files = [file for file in ls_siren_files if os.path.splitext(ntpath.basename(file))[0] not in fichier_a_filtrer]
    else:
        #filtre du fichier entete uniquement
        ls_siren_files = [file for file in ls_siren_files if os.path.splitext(ntpath.basename(file))[0] != "entete"]
    #Traitement des fichiers vides
    ls_fichier_a_traiter = []
    for file in ls_siren_files:
        if os.path.getsize(file)>0:
            ls_fichier_a_traiter.append(file)
        else: #écriture d'un fichier vide pour indiquer qu'il a été traité
            tools.touch(webscraping_results_path + ntpath.basename(file), file_mode = 'w')
    #lecture du fichier entete
    #!py36
    #with open(csv_siren_entete, 'r', encoding = 'UTF-8') as f:
    with open(csv_siren_entete, 'rb') as f:
        reader = unicodecsv.reader(f, delimiter = ";", encoding = 'UTF-8')
        entete_csv_siren = list(reader)[0]
    return entete_csv_siren, ls_fichier_a_traiter


