#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 16:16:47 2018

@author: mathieu.lechine
"""


import os
import pandas as pd
import logging
import inspect
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
    
    
def update_csv_recap(curr_filename, dict_feature2export_all, df_siren_file, df_ws_file, entete_csv_ws, entete_csv_resultat, output_csv_recap, param_add_tag_cctx_file):
    """
    Add the new lines corresponding to the new PM demandeur inserted in the json file of jurisprudence
    in the csv file output_csv_recap
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
    #ls_export_name = [u"demandeur_name"]+[tools.convert2unicode(export_name) for import_name,export_name in param_add_tag_cctx_file[u'list_keys_import_export']]
    ls_export_name = [u"company_name_from_jurisprud_file", u'company_name_related_to_extracted_siren_number']+[tools.convert2unicode(export_name) for import_name,export_name in param_add_tag_cctx_file[u'list_keys_import_export']]
    pd_feature2export_all = pd.DataFrame(list(tools.dict_to_listOflist_ordered(dict_feature2export_all, ls_export_name)))
    if entete_csv_ws is None: #pas de web scraping
        df_recap = pd.concat([df_siren_file, pd_feature2export_all], axis = 1, ignore_index=True)
        df_recap.insert(0, column = u"FILENAME", value = curr_filename)
        df_recap.to_csv(output_csv_recap, sep = ';', decimal = ',', header = False, index=False, mode="a", encoding = 'UTF-8')
    else:
        if df_ws_file is None: #fichier de web scraping non présent
            df_ws_file = pd.DataFrame(index = range(df_siren_file.shape[0]), columns = entete_csv_ws)
            df_merge_file = pd.concat([df_siren_file, df_ws_file], axis = 1, ignore_index=True)
        else: #fichier de web scraping présent
            df_merge_file = df_siren_file.merge(df_ws_file, how = u'left', on = u"NUMERO_ENTREPRISE")
            #rajout de la colonne NUMERO entreprise pour le web_scraping
            df_merge_file.insert(df_siren_file.shape[1], column = u"NUMERO_ENTREPRISE"+u"_y", value = df_merge_file[u"NUMERO_ENTREPRISE"])
        df_recap = pd.concat([df_merge_file, pd_feature2export_all], axis = 1, ignore_index=True)
        df_recap.insert(0, column = u"FILENAME", value = curr_filename)
        df_recap.to_csv(output_csv_recap, sep = ';', decimal = ',', header = False, index=False, mode="a", encoding = 'UTF-8')


def filter_files_with_no_PM_demandeur(csv_siren_path, ls_files2process, entete_csv_resultat, output_csv_recap):
    """
    Cette fonction traite les fichiers json sans demandeur PM et renvoit la liste des 
    fichiers à traiter avec des demandeurs PM.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_empty_files = [file for file in ls_files2process if os.path.getsize("{}{}{}".format(csv_siren_path, file, '.csv'))==0]
    ls_files2process = list(set(ls_files2process) - set(ls_empty_files))
    #empty files into the recap file as empty lines
    df_recap = pd.DataFrame(index = range(len(ls_empty_files)), columns = entete_csv_resultat)
    df_recap.insert(0, column = u"FILENAME", value = ls_empty_files)
    df_recap.to_csv(output_csv_recap, sep = ';', decimal = ',', header = True, index=False, mode="w", encoding = 'UTF-8')
    return ls_files2process


def get_entete_df_resultat(entete_csv_siren, entete_csv_ws, param_add_tag_cctx_file):
    """
    Fonction renvoyant l'entête du fichier de résultats résumant les choix d'insertion
    de SIREN effectué
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_export_name = [tools.convert2unicode(export_name) for import_name,export_name in param_add_tag_cctx_file[u'list_keys_import_export']]
    #entete_csv_siren + entete_csv_ws + ['demandeur_name'] + ls_export_name
    if entete_csv_ws is None:
        #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
        #entete_csv_resultat = [u'siren@'+name for name in entete_csv_siren] + [u'res@demandeur_name'] + [u'res@'+name for name in ls_export_name]
        entete_csv_resultat = [u'siren@'+name for name in entete_csv_siren] + [u'res@company_name_from_jurisprud_file', u'res@company_name_related_to_extracted_siren_number'] + [u'res@'+name for name in ls_export_name]
    else:
        #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
        #entete_csv_resultat = [u'siren@'+name for name in entete_csv_siren] + [u'ws@'+name for name in entete_csv_ws] + [u'res@demandeur_name'] + [u'res@'+name for name in ls_export_name]
        entete_csv_resultat = [u'siren@'+name for name in entete_csv_siren] + [u'ws@'+name for name in entete_csv_ws] + [u'res@company_name_from_jurisprud_file', u'res@company_name_related_to_extracted_siren_number'] + [u'res@'+name for name in ls_export_name]
    return entete_csv_resultat

