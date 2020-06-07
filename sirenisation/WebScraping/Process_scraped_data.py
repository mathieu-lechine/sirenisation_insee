#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 11:26:53 2018

@author: mathieu.lechine
"""

import Levenshtein
import re
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

def compute_confidence_index(dict_features_ws, dict_csv_siren_file, param_ws_file):
    """
    Cette fonction prend le dataframe filtré avec les différentes métriques de matching et 
    renvoit un indice de confiance pour chaque ligne de matching.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #score raison sociale
    dict_features_ws[u'score_raison_social'],  dict_features_ws[u'Levenstein_dist_raison_sociale'], dict_features_ws[u'is_subtring_raison_sociale'] = compute_score_raison_social(raison_sociale_ws = dict_features_ws[u"raison_sociale_ws"],
                                raison_sociale = dict_csv_siren_file[u'JsonFeature_raison_sociale'], 
                                param_ws_file = param_ws_file)
    #score adresse complete
    dict_features_ws[u'score_adresse_complete'], dict_features_ws[u'Levenstein_dist_ADRESSE_COMPLETE'] = compute_score_adresse_complete(ADRESSE_COMPLETE_ws = dict_features_ws[u"ADRESSE_COMPLETE_ws"],
                                                            adresse_complete =  dict_csv_siren_file[u"JsonFeature_adresse_complete"], 
                                                            param_ws_file = param_ws_file)
    #score code postal
    dict_features_ws[u'score_code_postal'], dict_features_ws[u'match_code_postal'], dict_features_ws[u'match_code_postal_prefix'] = compute_score_code_postal(CODPOS_ws = dict_features_ws[u"CODPOS_ws"], 
                                                  code_postal = dict_csv_siren_file[u'JsonFeature_code_postal'], 
                                                  format_CODPOS_ws = dict_features_ws[u"format_CODPOS_ws"],
                                                  param_ws_file = param_ws_file)
    #score ville
    dict_features_ws[u'score_ville'], dict_features_ws[u'Levenstein_dist_ville'] = compute_score_ville(dict_features_ws[u"LIBCOM_ws"], dict_csv_siren_file[u'JsonFeature_ville'], param_ws_file = param_ws_file)
    #score num_rue
    dict_features_ws[u'score_num_rue'], dict_features_ws[u'match_exact_NUMVOIE']  = compute_score_num_rue(dict_features_ws[u'NUMVOIE_ws'], dict_csv_siren_file[u'JsonFeature_num_rue'], param_ws_file)
    #score total
    dict_features_ws[u'Score_Total'] = compute_Score_Total(dict_features_ws)
    return dict_features_ws

def compute_Score_Total(dict_features_ws):
    """
    Calcul le score total en additionnant les scores intermédiaires
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    Score_Total = 0
    if dict_features_ws[u'score_raison_social']:
        Score_Total += dict_features_ws[u'score_raison_social']
    if dict_features_ws[u'score_adresse_complete']:
        Score_Total += dict_features_ws[u'score_adresse_complete']
    if dict_features_ws[u'score_code_postal']:
        Score_Total += dict_features_ws[u'score_code_postal']
    if dict_features_ws[u'score_ville']:
        Score_Total += dict_features_ws[u'score_ville']
    if dict_features_ws[u'score_num_rue']:
        Score_Total += dict_features_ws[u'score_num_rue']
    return Score_Total
    

def compute_score_num_rue(NUMVOIE_ws, num_rue, param_ws_file):
    """
    #calcul des points de confiance: Max(0; attribution_max_point - alpha * distance_Levenshtein)
    """   
    logger.debug(autolog_msg(u"Received a call to function"))
    max_point = param_ws_file[u'mapping_num_rue'][u'attribution_max_point']
    if NUMVOIE_ws is not None and num_rue is not None:
        NUMVOIE_ws = tools.convert2unicode(NUMVOIE_ws)
        num_rue = tools.convert2unicode(num_rue)
        match_exact_NUMVOIE = NUMVOIE_ws == num_rue
        score_num_rue = max(0, max_point * match_exact_NUMVOIE)
    else:
        score_num_rue = 0
        match_exact_NUMVOIE = None
    return score_num_rue, match_exact_NUMVOIE

def compute_score_ville(LIBCOM_ws, ville, param_ws_file):
    """
    #calcul des points de confiance: Max(0; attribution_max_point - alpha * distance_Levenshtein)
    """  
    logger.debug(autolog_msg(u"Received a call to function"))
    max_point = param_ws_file[u'mapping_ville'][u'attribution_max_point']
    alpha = param_ws_file[u'mapping_ville'][u'coefficent_alpha']
    if LIBCOM_ws is not None and ville is not None:
        LIBCOM_ws = tools.convert2unicode(LIBCOM_ws)
        ville = tools.convert2unicode(ville)
        Levenstein_dist_ville = Levenshtein.distance(LIBCOM_ws, ville)
        score_ville = max(0, max_point - alpha * Levenstein_dist_ville)
    else:
        score_ville = 0
        Levenstein_dist_ville = None
    return score_ville, Levenstein_dist_ville

def compute_score_code_postal(CODPOS_ws, code_postal, format_CODPOS_ws, param_ws_file):
    """
    #calcul des points de confiance: Max(0; attribution_max_point - alpha * distance_Levenshtein)
    """  
    logger.debug(autolog_msg(u"Received a call to function"))
    max_point = param_ws_file[u'mapping_code_postal'][u'attribution_max_point']
    bonus = param_ws_file[u'mapping_code_postal'][u'bonus_prefix']
    if CODPOS_ws is not None and code_postal is not None and format_CODPOS_ws:
        CODPOS_ws = tools.convert2unicode(CODPOS_ws)
        code_postal = tools.convert2unicode(code_postal)
        match_code_postal = CODPOS_ws == code_postal
        match_code_postal_prefix = CODPOS_ws[:2] == code_postal[:2]
        score_code_postal = max(bonus*match_code_postal_prefix, max(0, max_point * match_code_postal))
    else:
        score_code_postal = 0
        match_code_postal, match_code_postal_prefix = None, None
    return score_code_postal, match_code_postal, match_code_postal_prefix
    

def compute_score_adresse_complete(ADRESSE_COMPLETE_ws, adresse_complete, param_ws_file):
    """
    #calcul des points de confiance: Max(0; attribution_max_point - alpha * distance_Levenshtein)
    """  
    logger.debug(autolog_msg(u"Received a call to function"))
    max_point = param_ws_file[u'mapping_adresse_complete'][u'attribution_max_point']
    alpha = param_ws_file[u'mapping_adresse_complete'][u'coefficent_alpha']
    if ADRESSE_COMPLETE_ws is not None and adresse_complete is not None:
        ADRESSE_COMPLETE_ws = tools.convert2unicode(ADRESSE_COMPLETE_ws)
        adresse_complete = tools.convert2unicode(adresse_complete)
        Levenstein_dist_ADRESSE_COMPLETE = Levenshtein.distance(ADRESSE_COMPLETE_ws, adresse_complete)
        score_adresse_complete = max(0, max_point - alpha * Levenstein_dist_ADRESSE_COMPLETE)
    else:
        score_adresse_complete = 0
        Levenstein_dist_ADRESSE_COMPLETE = None
    return score_adresse_complete, Levenstein_dist_ADRESSE_COMPLETE
    
def compute_score_raison_social(raison_sociale_ws, raison_sociale, param_ws_file):
    """
    #calcul des points de confiance: Max(bonus_substring;Max(0; attribution_max_point - alpha * distance_Levenshtein))
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    max_point = param_ws_file[u'mapping_raison_sociale'][u'attribution_max_point']
    alpha = param_ws_file[u'mapping_raison_sociale'][u'coefficent_alpha']
    bonus = param_ws_file[u'mapping_raison_sociale'][u'bonus_substring']
    raison_sociale_ws = tools.convert2unicode(raison_sociale_ws)
    if raison_sociale is not None and raison_sociale!=u"":
        raison_sociale = tools.convert2unicode(raison_sociale)
        is_substring = (raison_sociale_ws in raison_sociale) #or (raison_sociale in raison_sociale_ws)
        Levenstein_dist_raison_sociale = Levenshtein.distance(raison_sociale_ws, raison_sociale)
        score_raison_social = max(is_substring*bonus, 
                                 max(0, max_point - alpha * Levenstein_dist_raison_sociale))
    else:
        score_raison_social = 0
        Levenstein_dist_raison_sociale, is_substring = None, None
    return score_raison_social, Levenstein_dist_raison_sociale, is_substring 

def preprocess_features_ws(dict_features_ws):
    """
    Fonction mettant au bon format les champs extraits pour permettre le calcul de 
    l'indice de confiance
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #raison social
    logger.debug(u"Preprocessing raison social")
    dict_features_ws[u"raison_sociale_ws"] = tools.normalize_string(dict_features_ws[u'Dénomination'])
    #code postal
    logger.debug(u"Preprocessing code postal")
    reg_code_postal = re.compile(r"^[0-9]{5}$")
    code_postal = tools.normalize_string(dict_features_ws[u'Code postal'])
    if code_postal:
        dict_features_ws[u"format_CODPOS_ws"] = True if reg_code_postal.match(code_postal) else False
        dict_features_ws[u"CODPOS_ws"] = code_postal
    else:
        dict_features_ws[u"format_CODPOS_ws"] = False
        dict_features_ws[u"CODPOS_ws"] = None
    #ville
    logger.debug(u"Preprocessing ville")
    dict_features_ws[u"LIBCOM_ws"] = tools.normalize_string(dict_features_ws[u'Ville'])
    #num rue
    logger.debug(u"Preprocessing num rue")
    reg_num_rue = re.compile(r'[0-9]+')
    if dict_features_ws[u'Adresse']:
        num_rue = reg_num_rue.search(tools.normalize_string(dict_features_ws[u'Adresse']))
    else:
        num_rue = None
    if num_rue:
        dict_features_ws[u"NUMVOIE_ws"] = tools.normalize_string(num_rue.group())
    else:
        dict_features_ws[u"NUMVOIE_ws"] = None
    #adresse complete
    logger.debug(u"Preprocessing Adresse complete")
    dict_features_ws[u"ADRESSE_COMPLETE_ws"] = tools.normalize_string(u"{} {} {}".format(dict_features_ws[u'Adresse'], dict_features_ws[u"CODPOS_ws"], dict_features_ws[u"LIBCOM_ws"]))
    logger.debug(u"End of preprocessing")
    return dict_features_ws
