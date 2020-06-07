#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 15:16:42 2018

@author: mathieu.lechine
"""


import glob
import Levenshtein
import numpy as np
import pandas as pd
import re
import os
import csv
import ntpath
import logging
import inspect
import codecs
import unicodecsv
import random
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

def map_all_demandeur_SIREN(df_siren, db_decision_jurisprud_path, mapping_output_path,  param_file, overwrite = False, extension_file = ".json"):
    """
    Cette fonction écrit le mapping SIREN dans un fichier CSV
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #Liste des fichiers json à traiter
    list_jurisprud_files = filtre_fichiers_json_deja_traite(db_decision_jurisprud_path, mapping_output_path, overwrite, extension_file)
    compt_append_file = 0
    statut_export = True
    nb_files2process = len(list_jurisprud_files)
    #!20180308 protection de la fonction mapping_siren
    write_siren_entete_file(mapping_output_path, param_file)
    for jurisprud_file in list_jurisprud_files: 
        logger.info(u"Processing the jurisprud file {}".format(jurisprud_file))
        #lecture du fichier .json -> objet décision de justice
        #logger.debug("Traitement du fichier {}".format(jurisprud_file))
        curr_decision = DecisionJurisprudence(jurisprud_file)
        #extraction des PM du fichier .json
        curr_decision.compute_all_features()
        #print(curr_decision)
        #logger.debug("{} demandeurs de type PM ont été trouvés".format(len(Demandeur_PM_list)))
        df2export_all = pd.DataFrame()
        for i in range(curr_decision.get_nb_demandeur_PM()):
            dict_features = curr_decision.get_dict_features_demandeur(i)
            logger.info(u"Processing the company: '{}'".format(dict_features[u"raison_sociale"]))
            #!20180308 protection de la fonction mapping_siren
            try:
                df_mapping = mapping_siren(df_siren, dict_features, param_file) 
                df_mapping = compute_confidence_index(dict_features, df_mapping, param_file)
                df2export = normalize_results_for_export(df_mapping, dict_features, filename = curr_decision.jurisprud_filename, numero_entreprise = i, param_file = param_file)
            except Exception as msg_erreur:
                logger.error(u"L'entreprise numéro '{}' du fichier '{}' n'a pas pu être traitée due à l'erreur suivante renvoyée par python: \n'{}'".format(i, jurisprud_file, msg_erreur))
                df2export = pd.DataFrame()
            df2export_all = pd.concat([df2export_all, df2export])
        mapping_output_filename = mapping_output_path + curr_decision.jurisprud_filename + ".csv"
        statut_export = statut_export and export_resultat_mapping2csv(df2export_all, mapping_output_filename , overwrite = overwrite)
        #!20180308 protection de la fonction mapping_siren
        #écriture de l'entête des fichiers csv si nécessaire
        #if compt_append_file == 0: 
        #    ls_entete = [u"index"] + list(df2export_all.columns)
        #    #!py36
        #    #with open(mapping_output_path + "entete.csv", 'w', encoding = 'UTF-8') as myfile:
        #    with open(mapping_output_path + "entete.csv", 'wb') as myfile:
        #        wr = unicodecsv.writer(myfile, delimiter = ";", encoding = 'UTF-8')
        #        wr.writerow(ls_entete)
        compt_append_file = compt_append_file+1
        if compt_append_file % 100 == 0:
            logger.info(u"{} (out of {}) jurisprud files successfully processed and exported".format(compt_append_file, nb_files2process))
    logger.info(u"All jurisprud files ({}) have been successfully processed and exported to '{}'".format(nb_files2process, mapping_output_path))
    return statut_export

def write_siren_entete_file(mapping_output_path, param_file):
    """
    Ecriture du fichier d'entête des fichiers siren CSV
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_entete = param_file[u"output_column_names_val_defaut"]
    ls_entete = [u"index"] + [colname for colname, val in ls_entete]
    #!py36
    #with open(webscraping_results_path + "entete.csv", 'w', encoding = 'UTF-8') as myfile:
    with open(mapping_output_path + "entete.csv", 'wb') as myfile:
                wr = unicodecsv.writer(myfile, delimiter = ";", encoding = 'UTF-8')
                wr.writerow(ls_entete)
    return True
    
    

def filtre_fichiers_json_deja_traite(db_decision_jurisprud_path, mapping_output_path, overwrite, extension_file):
    """
    Cette fonction prend en entrée la liste des fichiers json à traiter et retrourne la
    liste des fichiers json qui n'ont pas encore été traité en se basant sur les csv
    déjà créés.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    list_jurisprud_files = glob.glob(db_decision_jurisprud_path+"*"+extension_file)
    if not overwrite:
        list_csv_files = glob.glob(mapping_output_path+"*.csv")
        list_csv_files = [os.path.splitext(ntpath.basename(file))[0] for file in list_csv_files]
        list_jurisprud_files = [file for file in list_jurisprud_files if os.path.splitext(ntpath.basename(file))[0] not in list_csv_files]
    return list_jurisprud_files

def export_resultat_mapping2csv(df2export_all, mapping_output_filename, overwrite):
    """
    Cette fonction exporte les résultats de mapping au format CSV.
    Seul le maching le plus pertinent est renvoyé.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    if df2export_all.shape == (0,0):
        tools.touch(mapping_output_filename, file_mode = 'w')
    else:
        bool_csv_file_exists = os.path.isfile(mapping_output_filename)
        if overwrite or not bool_csv_file_exists:
            df2export_all.to_csv(mapping_output_filename, sep = ';', decimal = ',', header = False, index=True, mode="w", encoding = 'UTF-8')
    return True



def normalize_results_for_export(df_mapping, dict_features, filename, numero_entreprise, param_file):
    """
    Cette fonction calcule les variables à exporter (SIREN, SIRET, RCS, APE) et vérifie
    leur format.
    Elle renvoit un dataframe d'une seule ligne avec création de colonnes 
    "NOMVARIABLE_format" a true si le format est bon et sinon à False
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #select the first line
    df2export = df_mapping.head(1)
    #add the infos from the JSON file
    df_features = pd.DataFrame(dict_features, index = df2export.index)
    df_features.columns = [u"JsonFeature_" + i for i in list(df_features.columns)]
    df2export = pd.concat([df2export, df_features], axis = 1)
    #Calcul des informations voulues et vérification de leur format 
    #(création de colonne "NOMVARIABLE_format" a true si le format est bon et sinon à False
    first_row = df2export.index[0]
    reg_SIREN = re.compile(r"^[0-9]{9}$")
    reg_NIC = re.compile(r"^[0-9]{5}$")
    reg_APE = re.compile(r"^[0-9]{4}[A-Z]")
    SIREN_to_export = df2export.loc[first_row,u"SIREN"]
    NIC_to_export = df2export.loc[first_row,u"NIC"]
    LIBCOM_to_export = df2export.loc[first_row,u"LIBCOM_pretty"]
    APE_to_export = df2export.loc[first_row,u"APET700"]
    if reg_SIREN.match(SIREN_to_export):
        df2export[u"ToExport_SIREN"] = SIREN_to_export[:3] + " " + SIREN_to_export[3:6] + " " + SIREN_to_export[6:9]
        df2export[u"ToExport_SIREN_format"] = True 
        if reg_NIC.match(NIC_to_export):
            df2export[u"ToExport_SIRET"] = df2export.loc[first_row, u"ToExport_SIREN"] + " " + NIC_to_export
            df2export[u"ToExport_SIRET_format"] = True 
        else:
            df2export[u"ToExport_SIRET"] = df2export.loc[first_row, u"ToExport_SIREN"] + " " + NIC_to_export
            df2export[u"ToExport_SIRET_format"] = False 
        df2export[u"ToExport_RCS"] = "RCS" + " " + LIBCOM_to_export + " " + df2export.loc[first_row, u"ToExport_SIREN"]
        df2export[u"ToExport_RCS_format"] = True if len(LIBCOM_to_export)>0 else False
    else:
        df2export[u"ToExport_SIREN"] = SIREN_to_export
        df2export[u"ToExport_SIREN_format"] = False
        if reg_NIC.match(NIC_to_export):
            df2export[u"ToExport_SIRET"] = df2export.loc[first_row, u"ToExport_SIREN"] + " " + NIC_to_export
            df2export[u"ToExport_SIRET_format"] = False 
        else:
            df2export[u"ToExport_SIRET"] = df2export.loc[first_row, u"ToExport_SIREN"] + " " + NIC_to_export
            df2export[u"ToExport_SIRET_format"] = False 
        df2export[u"ToExport_RCS"] = "RCS" + " " + LIBCOM_to_export + " " + df2export.loc[first_row, u"ToExport_SIREN"]
        df2export[u"ToExport_RCS_format"] = False
    df2export[u"ToExport_APE"] = APE_to_export       
    df2export[u"ToExport_APE_format"] = True if reg_APE.match(APE_to_export) else False
    #insertion du nom du fichier en première position
    df2export.insert(loc=0, column=u'FILENAME', value = filename)
    #insertion du numéro du demandeur en deuxième position
    df2export.insert(loc=1, column=u"NUMERO_ENTREPRISE", value=unicode(numero_entreprise))
    df2export = check_column_names_and_order(df2export, param_file)
    return df2export

#!20180308 protection de la fonction mapping_siren: ajout de cette fonction 
def check_column_names_and_order(df2export, param_file):
    """
    Cette fonction:
        - filtre les colonnes qui ne sont pas dans les colonnes d'output attendues
        - créer les colonnes manquantes en les remplissant par la valeur par défaut
        - met les colonnes dans l'ordre attendu
    """
    ls_entete_val_defaut = param_file[u"output_column_names_val_defaut"]
    ls_entete = [colname for colname, val in ls_entete_val_defaut]
    set_col_df = set(df2export.columns)
    #filtre des colonnes non-attendues
    if set(ls_entete) == set_col_df:
        df_res = df2export[ls_entete]
    else:
        logger.warning(u"Le dataframe à la fin du mapping n'a pas les colonnes attendues dans le fichier de paramètres.")
        df_res = pd.DataFrame(index = df2export.index)
        for colname, val_defaut in ls_entete_val_defaut:
            if colname in set_col_df:
                df_res[colname] = df2export[colname]
            else:
                logger.warning(u"La colonne '{}' n'a pas été remplie. Elle sera remplie avec sa valeur par défaut".format(colname, val_defaut))
                df_res[colname] = unicode(val_defaut)
    return df_res

def compute_confidence_index(dict_features, df_mapping, param_file):
    """
    Cette fonction prend le dataframe filtré avec les différentes métriques de matching et 
    renvoit un indice de confiance pour chaque ligne de matching.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #calcul du score : Score_mapping_raison_sociale
    max_point = param_file[u'mapping_raison_sociale'][u'attribution_max_point']
    alpha = param_file[u'mapping_raison_sociale'][u'coefficent_alpha']
    bonus = param_file[u'mapping_raison_sociale'][u'bonus_substring']
    df_mapping[u"Score_mapping_raison_sociale"] = np.maximum(df_mapping[u"is_subtring_raison_sociale"]*bonus,
            np.maximum(0, max_point - alpha * df_mapping[u"Levenstein_dist_raison_sociale"]))
    #calcul du score: Score_mapping_adresse_complete
    max_point = param_file[u'mapping_adresse_complete'][u'attribution_max_point']
    alpha = param_file[u'mapping_adresse_complete'][u'coefficent_alpha']
    df_mapping[u"Score_mapping_adresse_complete"] = np.maximum(0,
              max_point - alpha * df_mapping[u"Levenstein_dist_ADRESSE_COMPLETE"])
    #calcul du score: Score_mapping_ville
    max_point = param_file[u'mapping_ville'][u'attribution_max_point']
    alpha = param_file[u'mapping_ville'][u'coefficent_alpha']
    df_mapping[u"Score_mapping_ville"] = np.maximum(0,
              max_point - alpha * df_mapping[u"Levenstein_dist_ville"])
    #calcul du score: Score_mapping_code_postal
    max_point = param_file[u'mapping_code_postal'][u'attribution_max_point']
    bonus = param_file[u'mapping_code_postal'][u'bonus_prefix']
    df_mapping[u"Score_mapping_code_postal"] = np.maximum(bonus*df_mapping[u"match_code_postal_prefix"],
              max_point*df_mapping[u"match_code_postal"])
    #calcul du score: Score_mapping_num_rue
    max_point = param_file[u'mapping_num_rue'][u'attribution_max_point']
    df_mapping[u"Score_mapping_num_rue"] = max_point*df_mapping[u"match_exact_NUMVOIE"]
    #calcul du score total
    df_mapping[u"Score_Total"] = df_mapping[u'Score_mapping_raison_sociale'] + df_mapping[u'Score_mapping_adresse_complete'] + \
        df_mapping[u'Score_mapping_ville'] + df_mapping[u'Score_mapping_code_postal'] + df_mapping[u'Score_mapping_num_rue']
    #tri des résultats par score
    df_mapping = df_mapping.sort_values(by=[u'Score_Total', u'Score_mapping_raison_sociale',
                                            u'Score_mapping_adresse_complete', u'Score_mapping_ville', u'Score_mapping_code_postal', u'Score_mapping_num_rue'], ascending=False)
    return df_mapping


# =============================================================================
# def mapping_raison_sociale(df_siren, raison_sociale, tol_dist_lev = 2):
#     """
#     Cette fonction fait un premier filter sur la raison sociale
#     et renvoie l'index des lignes de df_siren qui passe se filtre
#     """
#     logger.debug(autolog_msg("Received a call to function"))
#     col_dist_Lev = df_siren["raison_sociale"].apply(lambda enseigne: Levenshtein.distance(enseigne,raison_sociale))
#     min_dist = np.min(col_dist_Lev)
#     if min_dist == 0: #matching exacte -> on renvoit uniquement les lignes avec un matching exacte
#         res_index = col_dist_Lev.index[col_dist_Lev==min_dist]
#         pd_dist_raison_sociale = pd.DataFrame({"Levenstein_dist_raison_sociale": col_dist_Lev.loc[res_index],
#                                                "is_subtring_raison_sociale": [True]*len(res_index)},index =res_index)
#         return res_index, pd_dist_raison_sociale 
#     else:
#         res_index = col_dist_Lev.index[col_dist_Lev<=min_dist+tol_dist_lev]
#         #ajout des lignes qui sont des substrings
#         col_is_substring = df_siren["raison_sociale"].apply(lambda enseigne: raison_sociale in enseigne)
#         res_index = res_index.union(col_dist_Lev.index[col_is_substring])
#         pd_dist_raison_sociale = pd.DataFrame({"Levenstein_dist_raison_sociale": col_dist_Lev.loc[res_index],
#                                                "is_subtring_raison_sociale": col_is_substring[res_index]},index =res_index)
#         return res_index, pd_dist_raison_sociale
# =============================================================================

def mapping_raison_sociale(df_siren, raison_sociale, tol_dist_lev = 2, tol_len = 2):
    """
    Cette fonction fait un premier filter sur la raison sociale
    et renvoie l'index des lignes de df_siren qui passe se filtre
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #!20180308 protection de la fonction mapping_siren: ajout de cette fonction 
    #if random.randint(0,3)==0: raise ValueError("Simulated error")
    raison_sociale = tools.convert2unicode(raison_sociale)
    len_raison_sociale = len(raison_sociale)
    #np_raison_sociale_Leven = np.abs(np_raison_sociale_len-len_raison_sociale)<=tol_len
    raison_sociale_filter = (df_siren[u"raison_sociale_len"]-len_raison_sociale).abs()<=tol_len
    col_filtered_raison_sociale_Leven = df_siren.loc[raison_sociale_filter,u"raison_sociale"] 
    col_exact_match = col_filtered_raison_sociale_Leven == raison_sociale
    nb_exact_match = col_exact_match.sum()
    if nb_exact_match>0: #matching exacte -> on renvoit uniquement les lignes avec un matching exacte
        res_index = col_filtered_raison_sociale_Leven.index[col_exact_match]
        pd_dist_raison_sociale = pd.DataFrame({u"Levenstein_dist_raison_sociale": [0]*len(res_index),
                                           u"is_subtring_raison_sociale": [True]*len(res_index)}, index = res_index)
        return res_index, pd_dist_raison_sociale 
    else:
        #filtre selon la métrique de levensthein
        col_dist_Lev = col_filtered_raison_sociale_Leven.apply(lambda enseigne: Levenshtein.distance(enseigne,raison_sociale))
        min_dist = col_dist_Lev.min()
        res_index = col_dist_Lev.index[col_dist_Lev<=min_dist+tol_dist_lev]
        #filtre selon les substring
        np_raison_sociale_substring = df_siren[u"raison_sociale_len"]>len_raison_sociale
        col_is_substring = df_siren.loc[np_raison_sociale_substring, u"raison_sociale"].apply(lambda enseigne: raison_sociale in enseigne)
        res_index = res_index.union(col_is_substring.index[col_is_substring])
        #
        series_raison_soc = df_siren.loc[res_index,u"raison_sociale"]
        pd_dist_raison_sociale = pd.DataFrame(index = res_index)
        pd_dist_raison_sociale[u"Levenstein_dist_raison_sociale"] = series_raison_soc.apply(lambda enseigne: Levenshtein.distance(enseigne,raison_sociale))
        pd_dist_raison_sociale[u"is_subtring_raison_sociale"] = series_raison_soc.apply(lambda enseigne: raison_sociale in enseigne)
        return res_index, pd_dist_raison_sociale





def mapping_code_postal(df_mapping, code_postal):
    """
    Cette fonction fait un filtre sur le code postal
    Elle renvoie le data frame filtré
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    if code_postal:
        code_postal = tools.convert2unicode(code_postal)
        match_code_postal = df_mapping[u"CODPOS"] == code_postal
        res_index = df_mapping.index[match_code_postal]
        n1 = len(res_index)
        if n1>=1:
            pd_dist_code_postal = pd.DataFrame({u"match_code_postal": match_code_postal[res_index],
                                                    u"match_code_postal_prefix": [True]*n1}, 
                                                index =res_index)
        else:
            #on essaie un match des deux premiers chiffres du code postal
            code_postal_prefix = code_postal[:2]
            match_code_postal_prefix = df_mapping[u"CODPOS"].apply(lambda codpos: codpos[:2] == code_postal_prefix)
            n2 = np.sum(match_code_postal_prefix)
            if n2>=1:
                res_index = df_mapping.index[match_code_postal_prefix]
                pd_dist_code_postal = pd.DataFrame({u"match_code_postal": match_code_postal[res_index],
                                                    u"match_code_postal_prefix": match_code_postal_prefix},
                                                    index =res_index)
            else: #pas de filtre sur le code postal
                res_index = df_mapping.index
                pd_dist_code_postal = pd.DataFrame({u"match_code_postal": match_code_postal[res_index],
                                                    u"match_code_postal_prefix": [False]*len(res_index)},
                                                    index =res_index)
        df_mapping = pd.concat([df_mapping.loc[res_index], pd_dist_code_postal], axis = 1)
    else: #le code postale n'a pas été trouvé dans le json
        df_mapping[u"match_code_postal"] = False
        df_mapping[u"match_code_postal_prefix"] = False
    return df_mapping

def mapping_adresse_complete(df_mapping, adresse_complete, tol_dist_lev = 4):
    """
    Cette fonction fait un filtre sur la ville
    Elle renvoie le data frame filtré:
        - si match exacte alors seuls les matchs excacts sont renvoyés
        - sinon toutes les lignes ayant une distance de Levenshtein inférieur au minimimum
        + tol_dist_lev sont renvoyées.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    if adresse_complete:
        adresse_complete = tools.convert2unicode(adresse_complete)
        df_mapping[u"Levenstein_dist_ADRESSE_COMPLETE"] = df_mapping[u"ADRESSE_COMPLETE"].apply(lambda row: Levenshtein.distance(row, adresse_complete))
        min_dist = np.min(df_mapping[u"Levenstein_dist_ADRESSE_COMPLETE"])
        if min_dist == 0:
            df_mapping = df_mapping.loc[df_mapping[u"Levenstein_dist_ADRESSE_COMPLETE"] == min_dist]
        else:
            df_mapping = df_mapping.loc[df_mapping[u"Levenstein_dist_ADRESSE_COMPLETE"] <= min_dist+tol_dist_lev]
    else:
        df_mapping[u"Levenstein_dist_ADRESSE_COMPLETE"] = 1000
    return df_mapping


def mapping_ville(df_mapping, ville, tol_dist_lev = 0):
    """
    Cette fonction fait un filtre sur la ville
    Elle renvoie le data frame filtré:
        - si match exacte alors seuls les matchs excacts sont renvoyés
        - sinon toutes les lignes ayant une distance de Levenshtein inférieur au minimimum
        + tol_dist_lev sont renvoyées.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    if ville:
        ville = tools.convert2unicode(ville)
        df_mapping[u"Levenstein_dist_ville"] = df_mapping[u"LIBCOM_pretty"].apply(lambda libcom: Levenshtein.distance(libcom, ville))
        min_dist = np.min(df_mapping[u"Levenstein_dist_ville"])
        if min_dist == 0:
            df_mapping = df_mapping.loc[df_mapping[u"Levenstein_dist_ville"] == min_dist]
        else:
            df_mapping = df_mapping.loc[df_mapping[u"Levenstein_dist_ville"] <= min_dist+tol_dist_lev]
    else:
        df_mapping[u"Levenstein_dist_ville"] = 1000
    return df_mapping

def mapping_num_rue(df_mapping, num_rue):
    """
    Cette fonction ajoute une colonne indiquant si le numero de voie est identique.
    """ 
    logger.debug(autolog_msg(u"Received a call to function"))
    if num_rue:
        num_rue = tools.convert2unicode(num_rue)
        df_mapping[u"match_exact_NUMVOIE"] = df_mapping[u"NUMVOIE_pretty"] == num_rue
    else:
        df_mapping[u"match_exact_NUMVOIE"] = False
    return df_mapping



#df = pd.DataFrame({1: [1,2],2: ['a', 'b'],3: [3,4]})
#def myfun(df):
#    df['a'] = 'klm'
#    #df = df.iloc[:,:2]
#    return 0
#myfun(df)

