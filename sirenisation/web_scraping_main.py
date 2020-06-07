#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 18 10:53:14 2018

@author: mathieu.lechine
"""

import os
import sys
import logging
import logging.config
import argparse
import pandas as pd
import yaml
from collections import defaultdict
import codecs
#package for scraping
from fake_useragent import UserAgent

#add the current path to the PYTHONPATH
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
PACKAGE_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))
if PACKAGE_PATH not in sys.path:
    sys.path.append(PACKAGE_PATH)
#my packages
import sirenisation.WebScraping.Scraping as Scraping
import sirenisation.WebScraping.Process_scraped_data as Process_scraped_data
import sirenisation.WebScraping.HandleImportExport as HandleImportExport
import sirenisation.miscellaneous.tools as tools #Personnalisation de la fonction de lecture YAML pour qu'elle lise tous les strings en unicode

 
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
    
    
def main():
    #logger.debug(u"Received a call to function 'main'")
    #parametres de lancement du programme en ligne de commande
    parser = argparse.ArgumentParser(description=u"Ce programme prend en entrée les fichiers .csv siren générés par le script \
                                     sirenisation_insee_main.py. Pour chaque demandeur PM de ces fichiers dont le niveau de confiance \
                                     est inférieur au seuil fixé dans le fichier de paramètre, il envoit une requête au site societe.com \
                                     pour récupérer des données. Ces données sont stockées dans des fichiers .csv dans le dossier webscraping_results_path \
                                     spécifié dans le fichier de paramètre.")
    parser.add_argument(u"--param_file", default=u"./parameters/parameters_WS.yaml",
                         help=u"Chemin vers le fichier de paramètre (.yaml)")
    parser.add_argument(u"--overwrite", default=False, type = bool, choices=[True, False],
                         help=u"Si True, les fichiers de résultats de web scraping déjà générés seront écrasés et regénérés")
    #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
    parser.add_argument(u"--csv_siren_path", default=u"./Output/output_siren_csv/",
                         help=u"Dossier contenant les résultats de la sirenisation INSEE des fichiers de jurisprudence (resultats de 'sirenisation_insee_main.py')")
    parser.add_argument(u"--webscraping_results_path", default=u"./Output/ouput_WebScraping/",
                         help=u"Dossier dans lequel seront sauvegardes les resultats du webscraping")
    parser.add_argument(u"--logging_param_filename", default=u"./parameters/logging.yaml",
                         help=u"Fichier de paramétrage des logs (.yaml)")
    parser.add_argument(u"--info_file_handler_filename", default=u'./logs/info.log',
                         help=u"Fichier de log (sortie standard)")
    parser.add_argument(u"--error_file_handler_filename", default=u'./logs/errors.log',
                         help=u"Fichier de log (sortie error)")
    #logger.info(u"Parsing the input arguments")
    args = parser.parse_args()
    param_ws_filename = args.param_file
    overwrite = args.overwrite
    #20180320 logging: initialisation du logger
    logging_param_filename = args.logging_param_filename
    logger = read_params(logging_param_filename, override_filename=True, info_file_handler_filename=args.info_file_handler_filename, error_file_handler_filename=args.error_file_handler_filename)
    #lecture du fichier de paramètres
    #!py36
    #param_ws_file = yaml.load(open(param_ws_filename, 'r', encoding = 'UTF-8'))
    param_ws_file = yaml.load(codecs.open(param_ws_filename, 'r', encoding = 'UTF-8'))
    logger.info(u"The yaml file of parameters '{}' has been successfully imported".format(param_ws_filename))
    #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
    #Ajout des arguments de ligne de commande dans le dictionnaire des paramètres
    param_ws_file[u'csv_siren_path'] = args.csv_siren_path
    param_ws_file[u'webscraping_results_path'] = args.webscraping_results_path
    #Récupération de certains paramètres du dictionnaires de paramètres
    csv_siren_path = param_ws_file[u'csv_siren_path']
    webscraping_results_path = param_ws_file[u'webscraping_results_path']
    racine_url = param_ws_file[u'racine_url']
    base_requete_url = param_ws_file[u'base_requete_url']
    table_name_rensjur = param_ws_file[u"table_name_rensjur"]
    table_name_rensjur_complete = param_ws_file[u'table_name_rensjur_complete']
    #liste des fichiers à traiter
    entete_csv_siren, ls_fichier_a_traiter = HandleImportExport.get_results_siren(csv_siren_path, webscraping_results_path, overwrite = overwrite)
    #User Agent
    logger.info(u"Initialisation of the list of Web User Agents")
    user_agent = UserAgent() # From here we generate a random user agent
    #Ecriture du fichier entête des CSV
    logger.info(u"Writing the csv file 'entete.csv' at {}".format(webscraping_results_path))
    HandleImportExport.export_ws_entete_csv(webscraping_results_path, param_ws_file)
    logger.info(u"Start to process siren csv files located at ".format(csv_siren_path))
    compt_processed_files = 0
    nb_files2process = len(ls_fichier_a_traiter)
    for siren_file in ls_fichier_a_traiter:
        logger.info(u"Processing the file: {}".format(siren_file))
        #lecture du fichier csv
        df_siren_file = pd.read_csv(siren_file, index_col=0, header=None, sep = ";", decimal = ",", dtype = str, na_filter = False, encoding = 'UTF-8')
        df_siren_file.columns = entete_csv_siren[1:]
        nrow = df_siren_file.shape[0]
        random_useragent = user_agent.random
        dict_features_ws_all = defaultdict(lambda: [])
        for i in range(nrow):
            logger.debug(u"Processing the PM demandeur {} (out of {})".format(i+1, nrow))
            dict_csv_siren_file = dict(df_siren_file.iloc[i,:])
            #vérification du seuil de confiance
            seuil_indice_confiance = int(param_ws_file["seuil_indice_confiance"])
            Score_total_siren = dict_csv_siren_file["Score_Total"]
            try:
                Score_total_siren = int(Score_total_siren)
            except:
                Score_total_siren = 0
            if seuil_indice_confiance>Score_total_siren:
                logger.debug(u"The confidence index of the siren file is below the confidence threshold")
                #requete sur le site societe.com
                #raison_sociale = dict_csv_siren_file[u"raison_sociale"]
                raison_sociale = dict_csv_siren_file[u"JsonFeature_raison_sociale"]
                logger.info(u"Processing the company: '{}'".format(raison_sociale))
                requete_url = Scraping.make_search_url_for_raison_sociale(base_requete_url, raison_sociale)
                search_soup = Scraping.make_url_request(requete_url, random_useragent, ls_proxies = None)
                if search_soup is not None:
                    #analyse du code html
                    company_url = Scraping.get_url_page_company(search_soup, racine_url)
                    if company_url is not None:
                        #requete de la page company
                        company_soup = Scraping.make_url_request(company_url, random_useragent, ls_proxies = None)
                        if company_soup is not None:
                            #extraction des features de l'entreprise de la page web
                            dict_features_ws = Scraping.extract_data_from_company_soup(company_soup, param_ws_file, table_name_rensjur, table_name_rensjur_complete)
                            #preprocess features
                            dict_features_ws = Process_scraped_data.preprocess_features_ws(dict_features_ws)
                            #compute confidence index
                            dict_features_ws = Process_scraped_data.compute_confidence_index(dict_features_ws, dict_csv_siren_file, param_ws_file)
                            #Normalisation des champs à exporter
                            dict_features_ws = HandleImportExport.normalize_results_for_export(dict_features_ws, numero_entreprise = dict_csv_siren_file[u"NUMERO_ENTREPRISE"])
                            #accumulation des résultats dans un dictionnaire de liste 
                            dict_features_ws_all = tools.aggregate_in_dictOfList(dict_features_ws_all, dict_features_ws)    
                            del(dict_features_ws)
                        else:
                            logger.warning(u"Aucun résultat pour la requête '{}' lors de la recherche de la raison sociale '{}'".format(company_url, raison_sociale))
                    else:
                        logger.warning(u"Aucun résultat concluant dans la page '{}' lors de la recherche de la raison sociale '{}'".format(requete_url, raison_sociale))
                else:
                    logger.warning(u"Aucun résultat pour la requête '{}' lors de la recherche de la raison sociale '{}'".format(requete_url, raison_sociale))
        HandleImportExport.export_ws_results2csv(dict_features_ws_all, df_siren_file, webscraping_results_path, siren_file, param_ws_file)
        compt_processed_files += 1
        if compt_processed_files % 100 == 0:
            logger.info(u"{} (out of {}) files with 'PM demandeurs' successfully processed".format(compt_processed_files, nb_files2process))
    logger.info(u"All siren files ({}) were successfully processed".format(nb_files2process))
    logger.info(u"END")
   

if __name__ == "__main__":
    #logging_param_filename = u"./parameters/logging.yaml"
    #logger = read_params(logging_param_filename)
    main()
    