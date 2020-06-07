#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 05:30:43 2018

@author: mathieu.lechine
"""


#import packages
import os
import logging
import logging.config
import yaml 
import argparse
import sys
import codecs
#add the current path to the PYTHONPATH
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
PACKAGE_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))
if PACKAGE_PATH not in sys.path:
    sys.path.append(PACKAGE_PATH)
#my modules
#import sirenisation.sirenisation_insee.preprocess_siren_db as preprocess_siren_db
#import sirenisation.sirenisation_insee.mapping as mapping
import sirenisation.sirenisation_insee.mapping_sql as mapping_sql
import sirenisation.miscellaneous.tools as tools #Personnalisation de la fonction de lecture YAML pour qu'elle lise tous les strings en unicode


    #os.chdir("/Users/mathieu.lechine/Desktop/Privé/XXX/201801_SIRENisation_WebScraping/projet_sirenisation_py27/sirenisation")
    #db_siren_filename = "Data/sirc-17804_9075_14211_2018031_E_Q_20180201_031903109.csv"
    #db_siren_filename ="Data/sirc-17804_9075_14209_201801_L_M_20180201_031103300.csv" #big file
#param_filename = "./parameters/parameters_siren.yaml"
#download_new_db = 0 #0: par defaut, vérifie si nouveaux fichier stock sur l'INSEE
   #1: Pas de téléchargement du nouveaux fichiers sur l'INSEE
    #2: forcer le téléchargement du fichier le plus récent sur l'INSEE
#overwrite_csv_siren = True
#db_decision_jurisprud_path = "./Data/decision_jurisprud/"
#siren_data_folder = "./Data/siren_data/"
#mapping_output_path = "./Output/output_siren_csv/"

    
    
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
    parser = argparse.ArgumentParser(description=u"Ce programme prend en entrée les fichiers .json de \
                                     jurisprudence. Pour chaque demandeur PM de ces fichiers, il extrait \
                                     la ligne de la base de données SIREN (stock) qui lui correspond le mieux via \
                                     un mapping pondéré des champs relatifs à la raison sociale et à l'adresse. \
                                     Les résultats sont écrits dans des fichiers csv dans le dossier mapping_output_path \
                                     spécifié dans le fichier de paramètre. \
                                     Ce script s'assure aussi de mettre à jour la base SIREN (en la téléchargeant depuis le site de l'INSEE).")
    parser.add_argument(u"--param_file", default=u"./parameters/parameters_siren.yaml",
                         help=u"Chemin vers le fichier de paramètre (.yaml)")
#     parser.add_argument("--loglevel", type = str, 
#                         choices=["debug", "info", "warning", "error", "critical"], 
#                         default="info",
#                         help="choice of the logging level")
    parser.add_argument(u"--download_new_db", choices=[0, 1, 2], default=0, type = int,
                       help=u"Indique la méthode de mise à jour du fichier SIREN stock depuis le site de l'INSEE \n \
                       - 0: par defaut, vérifie si un nouveau fichier stock est disponible sur l'INSEE et le télécharge le cas échéant \n \
                       - 1: Utilisation du fichier SIREN stock le plus récent du dossier de données (pas de téléchargement de nouveaux fichiers sur le site de INSEE) \n \
                       - 2: Force le téléchargement du fichier le plus récent sur l'INSEE")
    parser.add_argument(u"--overwrite_csv_siren", default=False, type = bool, choices=[True, False],
                         help=u"Si True, les fichiers csv de résultats déjà générés seront écrasés et regénérés")
    #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
    parser.add_argument(u"--db_decision_jurisprud_path", default=u"./Data/decision_jurisprud/",
                         help=u"Dossier contenant les fichiers json de jurisprudence à sireniser")
    parser.add_argument(u"--siren_data_folder", default=u"./Data/siren_data/",
                         help=u"Dossier contenant les bases de données SIREN (stock) déjà  téléchargées")
    parser.add_argument(u"--mapping_output_path", default=u"./Output/output_siren_csv/",
                         help=u"Dossier d'output où seront sauvegardés les fichiers contenant les resultats")
    parser.add_argument(u"--logging_param_filename", default=u"./parameters/logging.yaml",
                         help=u"Fichier de paramétrage des logs (.yaml)")
    parser.add_argument(u"--info_file_handler_filename", default=u'./logs/info.log',
                         help=u"Fichier de log (sortie standard)")
    parser.add_argument(u"--error_file_handler_filename", default=u'./logs/errors.log',
                         help=u"Fichier de log (sortie error)")
    #Parsing the input arguments
    #logger.info(u"Parsing the input arguments")
    args = parser.parse_args()
    param_filename = args.param_file
    download_new_db = args.download_new_db
    overwrite_csv_siren = args.overwrite_csv_siren
    #20180320 logging: initialisation du logger
    logging_param_filename = args.logging_param_filename
    logger = read_params(logging_param_filename, override_filename=True, info_file_handler_filename=args.info_file_handler_filename, error_file_handler_filename=args.error_file_handler_filename)
    #lecture du fichier de parametre YAML
    #!py36
    #param_file = yaml.load(open(param_filename , 'r', encoding = 'UTF-8'))
    param_file = yaml.load(codecs.open(param_filename , 'r', encoding = 'UTF-8'))
    logger.info(u"The yaml file of parameters '{}' has been successfully imported".format(param_filename))
    #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
    #Ajout des arguments de ligne de commande dans le dictionnaire des paramètres
    param_file[u'db_decision_jurisprud_path'] = args.db_decision_jurisprud_path
    param_file[u'siren_data_folder'] = args.siren_data_folder
    param_file[u'mapping_output_path'] = args.mapping_output_path
    #Récupération de certains paramètres du dictionnaires de paramètres
    db_decision_jurisprud_path = param_file[u'db_decision_jurisprud_path']
    siren_data_folder = param_file[u'siren_data_folder']
    mapping_output_path = param_file[u'mapping_output_path']
    #téléchargement du nouveaux fichier de données stock (si nécessaire) ##fonction à tester avec WIFI
    #logger.info(u"Start to check if the SIREN Database is up-to-date")
    #bool_nouveau_fichier_stock, most_recent_csv_file = preprocess_siren_db.update_database(siren_data_folder, download_new_db)
    #logger.info(u"The SIREN Database is up-to-date")
    #chargement de la base de données SIREN dans la mémoire (avec preprocessing si nécessaire)
    #logger.info(u"Start to load the SIREN Database in memory")
    #df_siren = preprocess_siren_db.load_preprocessed_siren_database_in_memory(siren_data_folder, param_file, bool_nouveau_fichier_stock)
    #logger.info(u"The SIREN Database is in memory")
    #20180502
    logger.info(u"""The PostgreSQL database siren must have been created manually and it must have been split in 
                22 small tables named: siren_light_x with x=1..22""")
    logger.info(u"Starting to process the jurisprudence json files and to extract data from the SIREN database")
    #mapping.map_all_demandeur_SIREN(df_siren, db_decision_jurisprud_path, mapping_output_path,  param_file, overwrite = overwrite_csv_siren, extension_file = ".json")
    mapping_sql.map_all_demandeur_SIREN(db_decision_jurisprud_path, mapping_output_path,  param_file, overwrite = overwrite_csv_siren, extension_file = ".json")
    logger.info(u"END")
    
    
if __name__ == "__main__":
    #logging_param_filename = u"./parameters/logging.yaml"
    #logger = read_params(logging_param_filename)
    main()
    
    
    
    