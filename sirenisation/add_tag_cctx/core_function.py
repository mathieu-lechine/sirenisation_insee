#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 16:19:35 2018

@author: mathieu.lechine
"""

import os
import pandas as pd
import glob
import ntpath
import csv
import logging
import inspect
import unicodecsv
#my packages
import sirenisation.miscellaneous.tools as tools
from sirenisation.sirenisation_insee.DecisionJurisprudence import DecisionJurisprudence

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


def insert_cctx_tags_and_save(curr_filename, input_decision_path, output_decision_path, dict_feature2export_all, list_keys_to_export, param_add_tag_cctx_file):
    """
    Insertion des tags cctx dans le fichier json de jurisprudence et sauvegarde du fichier modifié 
    dans le dossier output_decision_path
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #formation des noms des fichiers d'input et d'output
    input_jurisprud_file = "{}{}{}".format(input_decision_path, curr_filename, '.json')
    output_jurisprud_file = "{}{}{}".format(output_decision_path, curr_filename, '.json')
    #lecture du fichier json de jurisprudence
    curr_decision = DecisionJurisprudence(input_jurisprud_file)
    #modifie le nom des clés pour correspondre aux noms des clés d'export
    for import_name, export_name in param_add_tag_cctx_file[u'list_keys_import_export']:
        dict_feature2export_all[tools.convert2unicode(export_name)] = dict_feature2export_all.pop(import_name)
    #dict of lists to list of dicts
    list2export = add_payload_tag_for_each_field([dict(zip(dict_feature2export_all,t)) for t in zip(*dict_feature2export_all.values())])
    #insertion des tags
    tagname = u"CCTX_entreprises"
    curr_decision.insert_tag_cctx_entreprise(tagname, list2export)
    #sauvegarde
    curr_decision.export_json_content2jsonfile(output_jurisprud_file)    
    return None

#list2export = [{'demandeur_name': 'SCP SILVESTRI BAUJET', 'Score_Total': '35', 'APE': '4334Z', 'APE_format': 'True', 'RCS': 'RCS DRAGUIGNAN 482 581 352', 'RCS_format': 'True', 'SIREN': '482 581 352', 'SIREN_format': 'True', 'SIRET': '482 581 352 00010', 'SIRET_format': 'True'}]


def add_payload_tag_for_each_field(list2export):
    """
    Ajout d'un tag payload à tous les champs de la liste de dictionnaire, afin d'avoir une structure
    du type:
        champ:
            payload: valeur_du_champs
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_res = []
    for dict_demandeur in list2export:
        dict_res = {}
        for key, val in dict_demandeur.items():
            dict_res[key] = {u"payload": val}
        ls_res.append(dict_res)
    return ls_res

#!20180308 passage_par_reference
#def compute_dict_feature2export_all(dict_feature2export_all, df_siren_file, df_ws_file, siren_nrow, list_keys_to_export):
#    """
#    Extrait les features list_keys_to_export du df_siren ou du df_ws_file (selon l'indice 
#    de confiance le plus élevé) dans dict_feature2export_all
#    """
#    logger.debug(autolog_msg(u"Received a call to function"))
#    for num_demandeur in range(siren_nrow):
#        dict_csv_siren_file = dict(df_siren_file.iloc[num_demandeur ,:])
#        dict_ws_siren_file = dict(df_ws_file.iloc[num_demandeur ,:])
#        #calcul du score de reconciliation
#        #compute_score_reconciliation(dict_csv_siren_file, dict_ws_siren_file)
#        siren_score_total = convert2int_otherwise_0(dict_csv_siren_file[u'Score_Total'])
#        ws_score_total = convert2int_otherwise_0(dict_ws_siren_file[u'Score_Total'])
#        #nom du demandeur
#        demandeur_name = dict_csv_siren_file[u'JsonFeature_demandeur_name']
#        #selection de la source de données avec l'indice de confiance le plus élevé
#        if siren_score_total>=ws_score_total:
#            #on retient les tags siren
#            dict_feature2export = {key: dict_csv_siren_file[key] for key in list_keys_to_export}
#        else:
#            #on retient les tags du webscraping
#            dict_feature2export = {key: dict_ws_siren_file[key] for key in list_keys_to_export}
#        dict_feature2export[u'demandeur_name'] = demandeur_name
#        dict_feature2export_all = tools.aggregate_in_dictOfList(dict_features_all = dict_feature2export_all, dict_features = dict_feature2export)
#    return dict_feature2export_all

#!20180308 passage_par_reference
#La décision de faire le web scraping était au niveau du fichier alors qu’elle 
#devait être au niveau de l’entreprise. Une entreprise est identifiée de manière 
#unique par le nom du fichier et son numéro. 

def compute_dict_feature2export_all(dict_feature2export_all, df_siren_file, df_ws_file, siren_nrow, list_keys_to_export):
    """
    Extrait les features list_keys_to_export du df_siren ou du df_ws_file (selon l'indice 
    de confiance le plus élevé) dans dict_feature2export_all
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    for num_demandeur in range(siren_nrow):
        dict_csv_siren_file = dict(df_siren_file.iloc[num_demandeur ,:])
        numero_entreprise = unicode(dict_csv_siren_file[u"NUMERO_ENTREPRISE"])
        #recherche de l'entreprise dans le fichier de web scraping
        df_ws_file_entreprise = df_ws_file.loc[df_ws_file[u"NUMERO_ENTREPRISE"]==numero_entreprise,:]
        nb_match_ws_entreprise = df_ws_file_entreprise.shape[0]
        if nb_match_ws_entreprise==0: #l'entreprise n'a pas été web scrapé
            logger.debug(u"L'entreprise n'a pas été web scrapée")
            #on retient les tags siren
            dict_feature2export = {key: dict_csv_siren_file[key] for key in list_keys_to_export}
            #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
            #dict_feature2export[u'demandeur_name'] = dict_csv_siren_file[u'JsonFeature_demandeur_name']
            dict_feature2export[u'company_name_from_jurisprud_file'] = dict_csv_siren_file[u'JsonFeature_demandeur_name']
            dict_feature2export[u'company_name_related_to_extracted_siren_number'] = dict_csv_siren_file[u'raison_sociale']
            dict_feature2export_all = tools.aggregate_in_dictOfList(dict_features_all = dict_feature2export_all, dict_features = dict_feature2export)
        elif nb_match_ws_entreprise==1:
            logger.debug(u"L'entreprise a été web scrapée")
            dict_ws_siren_file = dict(df_ws_file_entreprise.iloc[0,:])
            #calcul du score de reconciliation 
            #compute_score_reconciliation(dict_csv_siren_file, dict_ws_siren_file)
            siren_score_total = convert2int_otherwise_0(dict_csv_siren_file[u'Score_Total'])
            ws_score_total = convert2int_otherwise_0(dict_ws_siren_file[u'Score_Total'])
            #nom du demandeur
            demandeur_name = dict_csv_siren_file[u'JsonFeature_demandeur_name']
            #selection de la source de données avec l'indice de confiance le plus élevé
            if siren_score_total>=ws_score_total:
                #on retient les tags siren
                dict_feature2export = {key: dict_csv_siren_file[key] for key in list_keys_to_export}
                #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
                #dict_feature2export[u'demandeur_name'] = demandeur_name
                dict_feature2export[u'company_name_from_jurisprud_file'] = demandeur_name
                dict_feature2export[u'company_name_related_to_extracted_siren_number'] = dict_csv_siren_file[u'raison_sociale']
            else:
                #on retient les tags du webscraping
                dict_feature2export = {key: dict_ws_siren_file[key] for key in list_keys_to_export}
                #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
                #dict_feature2export[u'demandeur_name'] = demandeur_name
                dict_feature2export[u'company_name_from_jurisprud_file'] = demandeur_name
                dict_feature2export[u'company_name_related_to_extracted_siren_number'] = dict_ws_siren_file[u'raison_sociale_ws']
            dict_feature2export_all = tools.aggregate_in_dictOfList(dict_features_all = dict_feature2export_all, dict_features = dict_feature2export)
        else:
            logger.warning(u"Plusieurs entreprises ont le même numéro entreprise '{}'. ça ne devrait pas arriver. \
                           Les résultats de web scraping seront ignorés pour cette entreprise.")
            #on retient les tags siren
            dict_feature2export = {key: dict_csv_siren_file[key] for key in list_keys_to_export}
            #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
            #dict_feature2export[u'demandeur_name'] = dict_csv_siren_file[u'JsonFeature_demandeur_name']
            dict_feature2export[u'company_name_from_jurisprud_file'] = dict_csv_siren_file[u'JsonFeature_demandeur_name']
            dict_feature2export[u'company_name_related_to_extracted_siren_number'] = dict_csv_siren_file[u'raison_sociale']
            dict_feature2export_all = tools.aggregate_in_dictOfList(dict_features_all = dict_feature2export_all, dict_features = dict_feature2export)
    return dict_feature2export_all


def compute_dict_feature2export_all_from_df_siren(dict_feature2export_all, df_siren_file, siren_nrow, list_keys_to_export):
    """
    Extrait les features list_keys_to_export du df_siren dans dict_feature2export_all
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    for num_demandeur in range(siren_nrow):
        dict_csv_siren_file = dict(df_siren_file.iloc[num_demandeur ,:])
        #on retient les tags siren
        dict_feature2export = {key: dict_csv_siren_file[key] for key in list_keys_to_export}
        #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
        #dict_feature2export[u'demandeur_name'] = dict_csv_siren_file[u'JsonFeature_demandeur_name']
        dict_feature2export[u'company_name_from_jurisprud_file'] = dict_csv_siren_file[u'JsonFeature_demandeur_name']
        dict_feature2export[u'company_name_related_to_extracted_siren_number'] = dict_csv_siren_file[u'raison_sociale']
        dict_feature2export_all = tools.aggregate_in_dictOfList(dict_features_all = dict_feature2export_all, dict_features = dict_feature2export)
    return dict_feature2export_all


def convert2int_otherwise_0(s):
    """
    Fonction convertissant un string en int ou renvoyant 0 si impossible.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    try: 
        return int(s)
    except ValueError:
        return 0

def read_csv_ws_entete(webscraping_results_path, filename = "entete"):
    """
    lecture du fichier entete siren
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    file2read = webscraping_results_path+filename+".csv"
    if os.path.isfile(file2read):
        #!py36
        #with open(file2read, 'r', encoding = 'UTF-8') as f:
        with open(file2read, 'r') as f:
            reader = unicodecsv.reader(f, delimiter = ";", encoding = 'UTF-8')
            entete_csv_ws = list(reader)[0]
    else:
        entete_csv_ws = None
        logger.warning(u"Le fichier entete.csv des fichiers résultats du web scraping n'a pas été trouvé \
                       dans le dossier '{}'. L'ajout des tags SIREN dans les fichiers json de jurisprudence \
                       ne prendra pas en compte les résultats éventuels du web scraping.".format(webscraping_results_path))
    return entete_csv_ws

def read_csv_ws_file(webscraping_results_path, curr_filename, entete_csv_ws):
    """
    lecture du fichier csv de web scraping
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ws_file = "{}{}{}".format(webscraping_results_path, curr_filename, '.csv')
    if os.path.getsize(ws_file)>0:
        df_ws_file = pd.read_csv(ws_file, index_col=None, header=None, sep = ";", decimal = ".", dtype = unicode, na_filter = False, encoding = 'UTF-8')
        df_ws_file.columns = entete_csv_ws
    else:
        df_ws_file = None
    return df_ws_file

def read_csv_siren_entete(csv_siren_path, filename = "entete"):
    """
    lecture du fichier entete siren
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    file2read = csv_siren_path+filename+".csv"
    if os.path.isfile(file2read):
        #!py36
        #with open(file2read, 'r', encoding = 'UTF-8') as f:
        with open(file2read, 'rb') as f:
            reader = unicodecsv.reader(f, delimiter = ";", encoding = 'UTF-8')
            entete_csv_siren = list(reader)[0]
    else:
        entete_csv_siren = None
        logger.error(u"Le fichier entete.csv des fichiers siren n'a pas été trouvé dans le dossier \
                      '{}'. Assurez vous d'avoir lancé le script sirenisation_insee_main.py au préalable \
                      et d'avoir indiqué le bon dossier siren.".format(csv_siren_path))
    return entete_csv_siren

def read_csv_siren_file(csv_siren_path, curr_filename, entete_csv_siren):
    """
    lecture du fichier csv siren
    """    
    logger.debug(autolog_msg(u"Received a call to function"))
    siren_file = "{}{}{}".format(csv_siren_path, curr_filename, '.csv')
    if os.path.getsize(siren_file)>0:
        df_siren_file = pd.read_csv(siren_file, index_col=None, header=None, sep = ";", decimal = ",", dtype = unicode, na_filter = False, encoding = 'UTF-8')
        df_siren_file.columns = entete_csv_siren
    else:
        df_siren_file = None
    return df_siren_file
    
def get_json_files2process(input_decision_path, output_decision_path, csv_siren_path, overwrite = False): 
    """
    Cette fonction fait la liste des fichiers CSV siren qui doivent être traités par 
    web scraping.
    Les fichiers csv vides sont traités à la volée (création de fichiers vides dans le nouveau répertoire).
    L'option overwrite indique si l'on doit écraser les fichiers déjà traités et en regénérer de nouveaux.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_input_decision_files = [os.path.splitext(ntpath.basename(file))[0] for file in glob.glob(input_decision_path + "*.json")]
    ls_siren_files = [os.path.splitext(ntpath.basename(file))[0] for file in glob.glob(csv_siren_path + "*.csv")]
    ls_files2process = list(set(ls_input_decision_files) & set(ls_siren_files))
    #fichier_a_filtrer = [os.path.splitext(ntpath.basename(file))[0] for file in ls_siren_files]
    #fichier_a_filtrer = [file for file in fichier_a_filtrer if file!="entete"]
    #seuls les fichiers json dont les PM ont été sirenisés sont à traiter
    #ls_json_files2process = [file for file in ls_input_decision_files if os.path.splitext(ntpath.basename(file))[0] in fichier_a_filtrer]
    if not overwrite:
        #les fichiers json déjà traités ne seront pas traités une seconde fois
        ls_output_decision_files = [os.path.splitext(ntpath.basename(file))[0] for file in glob.glob(output_decision_path + "*.json")]
        ls_files2process = list(set(ls_files2process) - set(ls_output_decision_files))
    return ls_files2process

