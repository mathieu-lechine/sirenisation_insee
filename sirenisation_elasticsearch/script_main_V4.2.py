#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 28 11:32:43 2018

@author: mathi
"""
#/usr/local/Cellar/elasticsearch/6.2.4/bin/elasticsearch
import os, sys
import fnmatch
import yaml
import codecs
import logging
import logging.config
import argparse
import unicodecsv
from collections import defaultdict
from datetime import datetime
import pandas as pd
import re
#elasticsearch
from elasticsearch import Elasticsearch
#my packages
import tools
logger = logging.getLogger(__name__)


if False:
    os.chdir(u"/Users/mathi/Documents/XXX/201806_sirenisation_ADM/Projet_sirenisation_ADM")
    #input
    driver_file = u"./driver/geckodriver"
    path2data = u"/Users/mathi/Documents/XXX/201806_sirenisation_ADM/var_tmp_redo_CE_test"
    param_communes_filename = u'/Users/mathi/Documents/XXX/201806_sirenisation_ADM/Projet_sirenisation_ADM/param/df_communes.csv'
    param_known_companies_filename = u"./param/df_known_companies.csv"
    param_most_frequent_company_filename = u"./param/df_most_frequent_company_sirenized.csv"
    param_departement_filename = u"./param/df_departement.csv"
    tag_demandeur = u"Requerant"
    tag_defendeur = u"Defendeur"
    param_filename = u"./param/param.yaml"
    output_file = u"./output/recap_results_sirenisation.csv"
    logging_param_filename = u"./param/logging.yaml"
    



###############################################################################
#list of files to process
def build_list_of_ARIANE_files(path2data, pattern="*.xml"):
    """
    List all the files in the folder path2data that follow the pattern
    input:
        - path2data: path to the folder
        - pattern: searcg pattern for the files
    output: list of the absolute path of each files
    """
    matches = []
    for root, dirnames, filenames in os.walk(path2data):
        for filename in fnmatch.filter(filenames, pattern):
            matches.append(os.path.join(root, filename))
    return matches

###############################################################################
#process xml files
def extract_demandeur(root, tag_demandeur, files2process):
    """
    Extract the demandeur from file or return None
    """
    ls_demandeur = root.findall(u'.//'+tag_demandeur)
    n_demandeur = len(ls_demandeur)
    if n_demandeur>1:
        logger.warning(u"warning: {} demandeurs dans le fichier {}".format(n_demandeur, files2process))
        return ls_demandeur[0].text
    elif n_demandeur==0:
        logger.warning(u"warning: 0 demandeur dans le fichier {}".format(n_demandeur, files2process))
        return None
    else:
        return ls_demandeur[0].text

def extract_defendeur(root, tag_defendeur, files2process):
    """
    Extract the defendeur from file or return None
    """
    ls_defendeur = root.findall(u'.//'+tag_defendeur)
    n_defendeur = len(ls_defendeur)
    if n_defendeur>1:
        logger.warning(u"warning: {} defendeurs dans le fichier {}".format(n_defendeur, files2process))
        return ls_defendeur[0].text
    elif n_defendeur==0:
        logger.warning(u"warning: 0 defendeur dans le fichier {}".format(n_defendeur, files2process))
        return None
    else:
        return ls_defendeur[0].text

def extract_demandeurs_list(text_document, tag_demandeur, files2process):
    """
    Extract the demandeur from file or return None
    """
    #reg_demandeurs = re.compile(r"<{0}>(?!<{0}>)*<\\{0}>".format(tag_demandeur))
    start_word = u'<' + tag_demandeur + u'>'
    start_index = text_document.find(start_word)
    if start_index==-1:
        logger.warning(u"Pas de demandeurs dans le fichier {}".format(files2process))
        return []
    start_index = start_index + len(start_word)
    end_word = u'</' + tag_demandeur + u'>'
    end_index = text_document.find(end_word)
    if end_index==-1:
        logger.warning(u"Tag demandeur broken in the file {}".format(files2process))
        return []
    str_demandeurs = text_document[start_index:end_index]
    ls_demandeur = [s.strip() for s in str_demandeurs.split(";") if len(s.strip())>0]
    #ls_demandeur = [s1.strip() for s1 in str_demandeurs.split(";") for s2 in s1.split(",") if len(s1.strip())>0]
    return ls_demandeur


def extract_defendeurs_list(text_document, tag_defendeur, files2process):
    """
    Extract the defendeur from file or return None
    """
    start_word = u'<' + tag_defendeur + u'>'
    start_index = text_document.find(start_word)
    if start_index==-1:
        logger.warning(u"Pas de defendeurs dans le fichier {}".format(files2process))
        return []
    start_index = start_index + len(start_word)
    end_word = u'</' + tag_defendeur + u'>'
    end_index = text_document.find(end_word)
    if end_index==-1:
        logger.warning(u"Tag defendeur broken in the file {}".format(files2process))
        return []
    str_defendeurs = text_document[start_index:end_index]
    ls_defendeur = [s.strip() for s in str_defendeurs.split(";") if len(s.strip())>0]
    #ls_defendeur = [s1.strip() for s1 in str_defendeurs.split(";") for s2 in s1.split(",") if len(s1.strip())>0]
    return ls_defendeur  

def extract_text_from_tag(text_document, tag, files2process):
    """
    Extract text from a tag
    """
    start_word = u'<' + tag + u'>'
    start_index = text_document.find(start_word)
    if start_index==-1:
        logger.warning(u"Pas de tag '{}' dans le fichier {}".format(tag, files2process))
        return u""
    start_index = start_index + len(start_word)
    end_word = u'</' + tag + u'>'
    end_index = text_document.find(end_word)
    if end_index==-1:
        logger.warning(u"Tag '{}' broken in the file {}".format(tag, files2process))
        return u""
    str_tag = text_document[start_index:end_index]
    return str_tag

def extract_Degre_Juridiction(text_document, tag, files2process):
    """
    Extract the text from the tag "tag"
    """
    return extract_text_from_tag(text_document, tag, files2process)


def extract_Numero_Dossier(text_document, tag, files2process):
    return extract_text_from_tag(text_document, tag, files2process)

def extract_Date_Lecture(text_document, tag, files2process):
    str_res = extract_text_from_tag(text_document, tag, files2process)
    try:
        datetime_object = datetime.strptime(str_res, '%Y-%m-%d').strftime('%d-%m-%Y')
    except:
        datetime_object = u""   
    return datetime_object

###############################################################################
#TOOLS
def normalize_company_name(company):
    res = tools.normalize_string(company)
    #remove the name SOCIETE
    #res = re.sub("SOCIETE", u"", res)
    #remove *
    res = re.sub(u"\*", u" ", res)
    #remove "
    res = re.sub('"', u" ", res)
    #remove multiple space
    res = re.sub("(\ ){2,}", u" ", res)
    return res.strip()

###############################################################################
#ELASTIC SEARCH
def get_query_es(raison_sociale2find, prefix_codpostal, lib_commune, known_company, size_request=10):
    if known_company is None:
        if prefix_codpostal is not None and lib_commune is None:
            #query fuzzy + adresse
            dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "must": {"prefix" : { "CODPOS" : prefix_codpostal}},
                              "should": {"match": { "raison_sociale": {
                                      "query": raison_sociale2find,
                                      "fuzziness": "AUTO",
                                      }
                                  }
                              }
                          }
                      }
            }
        elif prefix_codpostal is None and lib_commune is not None:
            dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "should": [{"match": {"LIBCOM_pretty": lib_commune}},
                                         {"match": { "raison_sociale": {
                                                    "query": raison_sociale2find,
                                                    "fuzziness": "AUTO",
                                                    }
                                                  }
                                          }]
                              }
                      }
            }
        elif prefix_codpostal is not None and lib_commune is not None:
            dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "must": {"prefix" : { "CODPOS" : prefix_codpostal}},
                              "should": [{"match": {"LIBCOM_pretty": lib_commune}},
                                         {"match": { "raison_sociale": {
                                                    "query": raison_sociale2find,
                                                    "fuzziness": "AUTO",
                                                    }
                                                  }
                                          }]
                              }
                      }
            }
        else:
            dict_query = {
                  "size": size_request,
                  "query": {
                    "match": {
                      "raison_sociale": {
                        "query": raison_sociale2find,
                        "fuzziness": "AUTO",
                      }
                    }
                  }
                }
    elif known_company is not None:
        if prefix_codpostal is not None and lib_commune is None:
            #query fuzzy + adresse
            dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "must": [{"prefix" : { "CODPOS" : prefix_codpostal}},
                                       {"match_phrase": {"raison_sociale": known_company}}],
                              "should": {"match": { "raison_sociale": {
                                      "query": raison_sociale2find,
                                      "fuzziness": "AUTO",
                                      }
                                  }
                              }
                          }
                      }
            }
        elif prefix_codpostal is None and lib_commune is not None:
            dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "must": {"match_phrase": {"raison_sociale": known_company}},
                              "should": [{"match": {"LIBCOM_pretty": lib_commune}},
                                         {"match": { "raison_sociale": {
                                                    "query": raison_sociale2find,
                                                    "fuzziness": "AUTO",
                                                    }
                                                  }
                                          }]
                              }
                      }
            }
        elif prefix_codpostal is not None and lib_commune is not None:
            dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "must": [{"prefix" : { "CODPOS" : prefix_codpostal}},
                                       {"match_phrase": {"raison_sociale": known_company}}],
                              "should": [{"match": {"LIBCOM_pretty": lib_commune}},
                                         {"match": { "raison_sociale": {
                                                    "query": raison_sociale2find,
                                                    "fuzziness": "AUTO",
                                                    }
                                                  }
                                          }]
                              }
                      }
            }
        else:
            dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "must": {"match_phrase": {"raison_sociale": known_company}},
                              "should": [{"match": { "raison_sociale": {
                                                    "query": raison_sociale2find,
                                                    "fuzziness": "AUTO",
                                                    }
                                                  }
                                          }]
                              }
                      }
            }
    return dict_query

def get_query_es_with_SIREN(dict_frequent_company, size_request=10):
    dict_query = {
              "size": size_request,
              "query": {
                      "bool":{
                              "must": {"match_phrase": {"SIREN": dict_frequent_company[u"SIREN"]}},
                              "should": {"match_phrase": {"NIC": dict_frequent_company[u"NIC"]}}
                              }
                      }
            }
    return dict_query

###############################################################################
###############################################################################
def process_query_results(res_query, dict_features, ls_dict_features, company, files2process):
    """
    Function filling the dict_features from the results of the query res_query, and storing the results in ls_dict_features
    Input: 
        - res_query: results of the query
        - dict_features: dict of the features
        - ls_dict_features: list of the dict_features for the current file
    Output: None
    """
    #post-traitement des résultats de la requête
    query_successful = res_query['_shards'][u'successful']>0
    if query_successful:
        nb_results = res_query['hits']["total"]
        ls_hits = res_query['hits']["hits"]
        if nb_results>0:
            diff_score = 1000 
            score_0 = ls_hits[0]["_score"]
            siren_0 = ls_hits[0][u"_source"][u"SIREN"]
            compt_es = 0
            for hit in ls_hits:
                compt_es += 1
                if hit[u"_source"][u"SIREN"] != siren_0:
                    diff_score = score_0 - hit["_score"]
                    break
            #fill dict_features
            dict_features[u"Number_of_hits"] = nb_results
            dict_features[u"score_0"] = score_0
            dict_features[u"compt_es"] = compt_es
            dict_features[u"diff_score"] = diff_score
            dict_features[u"diff_score_relatif"] = (diff_score * 100.0)/score_0 if score_0>0 else u"NA"
            dict_features[u"SIREN"] = siren_0
            dict_features[u'raison_sociale'] = ls_hits[0][u"_source"][u'raison_sociale']
            dict_features[u"NIC"] = ls_hits[0][u"_source"][u"NIC"]
            dict_features[u"NJ"] = ls_hits[0][u"_source"][u"NJ"]
            dict_features[u"LIBNJ"] = ls_hits[0][u"_source"][u"LIBNJ"]
            dict_features[u"APET700"] = ls_hits[0][u"_source"][u"APET700"]
            dict_features[u"ADRESSE_COMPLETE"] = ls_hits[0][u"_source"][u"ADRESSE_COMPLETE"]
            dict_features[u'NUMVOIE_pretty'] = ls_hits[0][u"_source"][u"NUMVOIE_pretty"]
            dict_features[u'TYPVOIE'] = ls_hits[0][u"_source"][u"TYPVOIE"]
            dict_features[u'LIBVOIE'] = ls_hits[0][u"_source"][u"LIBVOIE"]
            dict_features[u'CODPOS'] = ls_hits[0][u"_source"][u"CODPOS"]
            dict_features[u'LIBCOM_pretty'] = ls_hits[0][u"_source"][u"LIBCOM_pretty"]
            #save results
            ls_dict_features.append(dict_features)
        else:
            logger.warning(u"Aucun résultat pour l'entreprise '{}' dans le fichier '{}'".format(company, files2process))
    else:
        logger.warning(u"La requête Elasticsearch a renvoyé une erreur l'enstreprise '{}' dans le fichier '{}'".format(company, files2process))
    return None




def process_query_results_frequent_company(res_query, dict_features, ls_dict_features, company, files2process, dict_frequent_company):
    """
    Function filling the dict_features from the results of the query res_query, and storing the results in ls_dict_features
    Input: 
        - res_query: results of the query
        - dict_features: dict of the features
        - ls_dict_features: list of the dict_features for the current file
    Output: None
    """
    dict_features[u"Sirenisation_most_frequent_company"] = u"1"
    #no overide
    dict_features[u"SIREN"] = dict_frequent_company[u"SIREN"]
    #possible overide
    dict_features[u'raison_sociale'] = dict_frequent_company[u'company_name_normalized']
    dict_features[u"NIC"] = dict_frequent_company[u"NIC"]
    
    #post-traitement des résultats de la requête
    query_successful = res_query['_shards'][u'successful']==1
    if query_successful:
        nb_results = res_query['hits']["total"]
        ls_hits = res_query['hits']["hits"]
        if nb_results>0:
            diff_score = 1000 
            score_0 = ls_hits[0]["_score"]
            siren_0 = ls_hits[0][u"_source"][u"SIREN"]
            compt_es = 0
            for hit in ls_hits:
                compt_es += 1
                if hit[u"_source"][u"SIREN"] != siren_0:
                    diff_score = score_0 - hit["_score"]
                    break
            #fill dict_features
            dict_features[u"Number_of_hits"] = nb_results
            dict_features[u"score_0"] = score_0
            dict_features[u"compt_es"] = compt_es
            dict_features[u"diff_score"] = diff_score
            dict_features[u"diff_score_relatif"] = (diff_score * 100.0)/score_0 if score_0>0 else u"NA"
            #dict_features[u"SIREN"] = siren_0
            dict_features[u'raison_sociale'] = ls_hits[0][u"_source"][u'raison_sociale']
            dict_features[u"NIC"] = ls_hits[0][u"_source"][u"NIC"]
            dict_features[u"NJ"] = ls_hits[0][u"_source"][u"NJ"]
            dict_features[u"LIBNJ"] = ls_hits[0][u"_source"][u"LIBNJ"]
            dict_features[u"APET700"] = ls_hits[0][u"_source"][u"APET700"]
            dict_features[u"ADRESSE_COMPLETE"] = ls_hits[0][u"_source"][u"ADRESSE_COMPLETE"]
            dict_features[u'NUMVOIE_pretty'] = ls_hits[0][u"_source"][u"NUMVOIE_pretty"]
            dict_features[u'TYPVOIE'] = ls_hits[0][u"_source"][u"TYPVOIE"]
            dict_features[u'LIBVOIE'] = ls_hits[0][u"_source"][u"LIBVOIE"]
            dict_features[u'CODPOS'] = ls_hits[0][u"_source"][u"CODPOS"]
            dict_features[u'LIBCOM_pretty'] = ls_hits[0][u"_source"][u"LIBCOM_pretty"]
    #    else:
    #        logger.warning(u"Aucun résultat pour l'entreprise '{}' dans le fichier '{}'".format(company, files2process))
    #else:
    #    logger.warning(u"La requête Elasticsearch a renvoyé une erreur l'enstreprise '{}' dans le fichier '{}'".format(company, files2process))
    #save results
    ls_dict_features.append(dict_features)
    return None

###############################################################################
###############################################################################
#export new tag in the XML file
#xml_filename = u"/Users/mathi/Documents/XXX/201806_sirenisation_ADM/var_tmp_redo_CE_test/CE_393311_20170208.xml"
def export_cctx_tags2xml_file(xml_filename, ls_dict_features, param_file):
    """
    Function inserting the cctx_tags in the file xml_filename
    """
    with codecs.open(xml_filename, mode="a", encoding="UTF8") as file2append:
        file2append.write(u"\n<cctx_tags>\n")
        for dict_features in ls_dict_features:
            file2append.write(u"  <company>\n")
            for tag in param_file[u"XML_tags"]:
                file2append.write(u"    <{}>".format(tag))
                file2append.write(unicode(dict_features[tag]) if not None else u"")
                file2append.write(u"</{}>\n".format(tag))
            file2append.write(u"  </company>\n")
        file2append.write(u"</cctx_tags>\n")
    return None

###############################################################################
###############################################################################

def read_params(logging_param_filename, override_filename=False, info_file_handler_filename='', error_file_handler_filename=''):
    #!py36
    #with open(logging_param_filename, 'rt', encoding = 'UTF-8') as f:
    with codecs.open(logging_param_filename, 'rt', encoding = 'UTF-8') as f:
        config = yaml.safe_load(f.read())
        #20180320 logging
        if override_filename:
            config['handlers']['info_file_handler']['filename'] = info_file_handler_filename
            config['handlers']['error_file_handler']['filename'] = error_file_handler_filename
        logging.config.dictConfig(config)
        logger = logging.getLogger(__name__)
        logger.info(u"The YAML file '{}' for the configuration of the logs has been successfully imported".format(logging_param_filename))
    return logger 

##################################################################################
    


def main():
    parser = argparse.ArgumentParser(description=u"""Sirenisation ADM via web scraping de societe.com""")
    parser.add_argument(u"--path2data", default=u"/Users/mathi/Documents/XXX/201806_sirenisation_ADM/var_tmp_redo_CE_test",
                         help=u"Chemin vers le dossier contenant les dossiers de json (sur lxbdata: var/tmp/redo/CE)")
    parser.add_argument(u"--param_filename", default=u"./param/param.yaml",
                         help=u"Chemin vers le fichier yaml de paramètres")
    parser.add_argument(u"--param_communes_filename", default=u"./param/df_communes.csv",
                         help=u"Chemin vers le fichier des communes")
    parser.add_argument(u"--param_departement_filename", default=u"./param/df_departement.csv",
                         help=u"Chemin vers le fichier des départements")
    parser.add_argument(u"--param_known_companies_filename", default=u"./param/df_known_companies.csv",
                         help=u"Chemin vers le fichier des entreprises connues")
    parser.add_argument(u"--param_most_frequent_company_filename", default=u"./param/df_most_frequent_company_sirenized.csv",
                         help=u"Chemin vers le fichier des entreprises les plus souvent rencontrées")
    parser.add_argument(u"--output_file", default=u"./output/recap_results_sirenisation2.csv",
                         help=u"Nom du fichier généré en sortie")
    parser.add_argument(u"--logging_param_filename", default=u"./param/logging.yaml",
                         help=u"Fichier de paramétrage des logs (.yaml)")
    parser.add_argument(u"--info_file_handler_filename", default=u'./logs/info.log',
                         help=u"Fichier de log (sortie standard)")
    parser.add_argument(u"--error_file_handler_filename", default=u'./logs/errors.log',
                         help=u"Fichier de log (sortie error)")
    #Parsing the input arguments
    #logger.info(u"Parsing the input arguments")
    args = parser.parse_args()
    path2data = args.path2data
    param_filename = args.param_filename
    param_communes_filename = args.param_communes_filename
    param_departement_filename = args.param_departement_filename
    param_known_companies_filename = args.param_known_companies_filename
    param_most_frequent_company_filename = args.param_most_frequent_company_filename
    output_file = args.output_file
    logging_param_filename = args.logging_param_filename

    
    #logging: initialisation du logger
    logging_param_filename = args.logging_param_filename
    logger = read_params(logging_param_filename, override_filename=True, info_file_handler_filename=args.info_file_handler_filename, error_file_handler_filename=args.error_file_handler_filename)
    #extract parameters
    param_file = yaml.load(codecs.open(param_filename, 'r', encoding = 'UTF-8'))
    tag_demandeur = param_file[u"tag_demandeur"]
    tag_defendeur = param_file[u"tag_defendeur"]
    #import list departement, communes et entreprises connues
    df_departement = pd.read_csv(param_departement_filename, header = 0, dtype = unicode, sep = ";", encoding ="utf8")
    df_communes = pd.read_csv(param_communes_filename, header = 0, dtype = unicode, sep = ";", encoding ="utf8")
    df_communes = df_communes[[u'Prefix_codpos', u'Libelle_acheminement', u'Libelle_acheminement_len']]
    df_communes[u'Libelle_acheminement_len'] = df_communes[u'Libelle_acheminement_len'].apply(int)
    df_known_companies = pd.read_csv(param_known_companies_filename, header = 0, dtype = unicode, sep = ";", encoding ="utf8")
    df_most_frequent_company = pd.read_csv(param_most_frequent_company_filename, header = 0, dtype = unicode, sep = ";", encoding ="utf8")
    df_most_frequent_company.fillna(u"NA", inplace=True)
    
    #list files to process
    ls_files2process = build_list_of_ARIANE_files(path2data, pattern="*.xml")
    ls_files2process = sorted(ls_files2process)
    
    #connect to elasticsearch cluster
    es = Elasticsearch(hosts = [param_file["ES_HOST"]])
    
    #write header of the csv file
    with open(output_file, "w") as myfile:
        spamwriter = unicodecsv.writer(myfile, delimiter=';', encoding = "utf-8")
        spamwriter.writerow([i for i in param_file[u"output_columns"]])
        
    #loop over all the files
    for files2process in ls_files2process:
        try:
            #files2process = ls_files2process[1]
            #read the file 
            #open the html file CE_389806_20170208.xml
            #print files2process
            with codecs.open(files2process,'r',encoding=u"utf8") as f:
                text_document = f.read()
            #extract demandeur
            ls_demandeur = extract_demandeurs_list(text_document, tag_demandeur, files2process)
            #extract defendeur
            ls_defendeur = extract_defendeurs_list(text_document, tag_defendeur, files2process)
            #extract Degre_Juridiction
            Degre_Juridiction = extract_Degre_Juridiction(text_document, param_file[u"tag_Degre_Juridiction"], files2process = files2process)
            #Numero_Dossier
            Numero_Dossier = extract_Numero_Dossier(text_document, param_file[u"tag_Numero_Dossier"], files2process = files2process)
            #Date_Lecture
            Date_Lecture = extract_Date_Lecture(text_document, param_file[u"tag_Date_Lecture"], files2process = files2process)
            ls_company = [(demandeur, "Demandeur") for demandeur in ls_demandeur] + [(defendeur, "Defendeur") for defendeur in ls_defendeur ]
            #loop over demandeurs
            ls_dict_features = []
            compt_company = 0
            for company, type_company in ls_company: 
                logger.info("Entreprise: "+company)
                compt_company += 1
                #company = ls_company[0]
                dict_features = defaultdict(lambda: None)
                dict_features[u"complete_path"] = files2process
                dict_features[u"Degre_Juridiction"] = Degre_Juridiction
                dict_features[u"Numero_Dossier"] = Numero_Dossier
                dict_features[u"Date_Lecture"] = Date_Lecture
                dict_features[u"defendeur_demandeur"] = type_company
                dict_features[u"Numero"] = compt_company
                dict_features[u"company_name"] = company
                #normalize string
                company_normalized = normalize_company_name(company)#tools.normalize_string(company)
                dict_features[u"company_name_normalized"] = company_normalized
                ls_parse_company_normalized = company_normalized.split()
                #check if the company is part of the most frequent companies
                col_filter_most_frequent_company = df_most_frequent_company[u'company_name_normalized'] == company_normalized
                
                
                if company_normalized in [u"DEFENDEUR1", u"REQUERANT1"]: #
                    dict_features[u"SIREN"] = "NO COMPANY"
                
                
                elif col_filter_most_frequent_company.sum()>0: #case of a frequent company
                    dict_frequent_company = dict(df_most_frequent_company[col_filter_most_frequent_company].iloc[0,:])
                    if dict_frequent_company[u"SIREN"]==u"NA":
                        dict_features[u"SIREN"] = "NO COMPANY"
                    else:
                        #use the siren already extracted to find the company in the database
                        dict_query = get_query_es_with_SIREN(dict_frequent_company, size_request=10)
                        #requete de la base de données Elasticsearch
                        logger.info(str(dict_query))
                        res_query = es.search(index = param_file[u'INDEX_NAME'], body=dict_query)
                        #post-traitement des résultats de la requête
                        process_query_results_frequent_company(res_query, dict_features, ls_dict_features, company, files2process, dict_frequent_company)
                
                
                else: #general case
                    #check if a dpartment name is present in the company name
                    dict_dep = defaultdict(lambda: None)
                    dict_commune = defaultdict(lambda: None)
                    dict_known_company = defaultdict(lambda: None)
                    col_filter_departement = df_departement[u"nom_departement"].apply(lambda dep: dep in company_normalized.split(" "))
                    col_filter_communes = df_communes[u'Libelle_acheminement'].apply(lambda lib_commune: lib_commune in company_normalized.split(" "))
                    #check if there is a known company
                    def is_known_company(known_company):
                        n_word = len(known_company.split())
                        if n_word==1: #if only one word, then look for the exact match of this word
                            return (known_company in ls_parse_company_normalized)
                        else: #else(case: several words) check that it is a substring
                            return (known_company in company_normalized)
                    col_filter_known_companies = df_known_companies[u'known_companies'].apply(is_known_company)
                    if col_filter_departement.sum()>0:
                        dict_dep = dict(df_departement[col_filter_departement].iloc[0,:])
                    if col_filter_communes.sum()>0:
                        dict_commune = dict(df_communes[col_filter_communes].sort_values(by=u'Libelle_acheminement_len', ascending=False).iloc[0,:])
                    if col_filter_known_companies.sum()>0:
                        dict_known_company = dict(df_known_companies[col_filter_known_companies].sort_values(by=u'known_companies_len', ascending=False).iloc[0,:])
                    #formation de la requête json selon les infos extraites
                    dict_query = get_query_es(raison_sociale2find=company, prefix_codpostal=dict_dep[u'prefix_code_postal'], lib_commune = dict_commune[u"Libelle_acheminement"], known_company = dict_known_company["known_companies"], size_request=param_file[u'size_request'])
                    #requete de la base de données Elasticsearch
                    logger.info(str(dict_query))
                    res_query = es.search(index = param_file[u'INDEX_NAME'], body=dict_query)
                    #post-traitement des résultats de la requête
                    process_query_results(res_query, dict_features, ls_dict_features, company, files2process)
            
            #################################################################################################
            #export results
            
            #export result in XML
            export_cctx_tags2xml_file(files2process, ls_dict_features, param_file)
            
            #export results in CSV recap
            with open(output_file, "a") as myfile:
                #myfile.write(u"{};{}\n".format(complete_path, ))
                spamwriter = unicodecsv.writer(myfile, delimiter=';', encoding="utf-8")
                for dict_features in ls_dict_features:
                    if dict_features is not None:
                        ls_features = [dict_features[i] for i in param_file[u"output_columns"]]
                    else:
                        ls_features = [u"" for i in param_file[u"output_columns"]]
                    spamwriter.writerow(ls_features)
                
        except Exception as msg_erreur:
            logger.error(u"ERROR: The file '{}' was not processed due to the following python error: \n'{}'".format(files2process, msg_erreur))
            
                
if __name__ == "__main__":
    main()





