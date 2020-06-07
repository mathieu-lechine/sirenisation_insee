#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 13:52:40 2018

@author: mathieu.lechine
"""


#ce script permet d'ajouter les tags cctx entreprises dans les fichiers json de décision
#de jurisprudence selon les résultats de sirenisation et de web scraping

import os
import sys
import logging
import logging.config
import argparse
import glob
import yaml
import ntpath
from collections import defaultdict
import codecs
#add the current path to the PYTHONPATH
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
PACKAGE_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))
if PACKAGE_PATH not in sys.path:
    sys.path.append(PACKAGE_PATH)
#my packages
import sirenisation.add_tag_cctx.core_function as core_function 
import sirenisation.add_tag_cctx.create_csv_recap as create_csv_recap
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
    parser = argparse.ArgumentParser(description=u"Ce programme est chargé d'insérer les tags SIREN dans les fichiers json de jurisprudence \
                                     se situant dans le dossier input_decision_path spécifié dans le fichier de paramètre. Les fichiers sirénisés sont \
                                     générés à l'emplacement output_decision_path (fichier de paramètre). Un fichier csv récapitulant les insertions faites est générés à l'emplacemen \
                                     output_csv_recap (fichier de paramètre). Le programme va récupérer les résultats des scripts sirenisation_insee_main.py et web_scraping_main.py respectivement dans les \
                                     dossiers csv_siren_path et webscraping_results_path (fichier de paramètre) et insérer les données avec l'indice de confiance le plus élévé. \
                                     Si les données de web scraping ne sont pas disponibles, le programme insérera uniquement les données extraites de la base SIREN.")
    parser.add_argument(u"--param_file", default=u"./parameters/parameters_add_tag_cctx.yaml",
                         help=u"Chemin vers le fichier de paramètre (.yaml)")
    parser.add_argument(u"--overwrite", default=False, type = bool, choices=[True, False],
                         help=u"Si True, les fichiers json de jurisprudence sirénisés déjà générés seront écrasés et regénérés")
    #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
    parser.add_argument(u"--input_decision_path", default=u"./Data/decision_jurisprud/",
                         help=u"Dossier contenant les fichiers json de jurisprudence a sireniser")
    parser.add_argument(u"--csv_siren_path", default=u"./Output/output_siren_csv/",
                         help=u"Dossier contenant les résultats de la sirenisation INSEE des fichiers de jurisprudence (resultats de 'sirenisation_insee_main.py')")
    parser.add_argument(u"--webscraping_results_path", default=u"./Output/ouput_WebScraping/",
                         help=u"Dossier contenant les résultats du web scraping")
    parser.add_argument(u"--output_decision_path", default=u"./Output/output_decision_jurisprud/",
                         help=u"Dossier dans lequel seront sauvegardés les fichiers json de jurisprudence sirenrisés")
    parser.add_argument(u"--output_csv_recap", default=u"./Output/resultat_recap_insertion.csv",
                         help=u"Fichier csv recapitulant les insertions de tags SIREN dans les fichiers json de jurisprudence")
    parser.add_argument(u"--logging_param_filename", default=u"./parameters/logging.yaml",
                         help=u"Fichier de paramétrage des logs (.yaml)")
    parser.add_argument(u"--info_file_handler_filename", default=u'./logs/info.log',
                         help=u"Fichier de log (sortie standard)")
    parser.add_argument(u"--error_file_handler_filename", default=u'./logs/errors.log',
                         help=u"Fichier de log (sortie error)")
    #Parsing the input arguments
    #logger.info(u"Parsing the input arguments")
    args = parser.parse_args()
    param_add_tag_cctx_filename = args.param_file
    overwrite = args.overwrite
    #20180320 logging: initialisation du logger
    logging_param_filename = args.logging_param_filename
    logger = read_params(logging_param_filename, override_filename=True, info_file_handler_filename=args.info_file_handler_filename, error_file_handler_filename=args.error_file_handler_filename)
    #lecture du fichier de paramètres
    #!py36
    #param_add_tag_cctx_file = yaml.load(open(param_add_tag_cctx_filename, 'r', encoding = 'UTF-8'))
    param_add_tag_cctx_file = yaml.load(codecs.open(param_add_tag_cctx_filename, 'r', encoding = 'UTF-8'))
    logger.info(u"The yaml file of parameters '{}' has been successfully imported".format(param_add_tag_cctx_filename))
    #20180320: Ajout des paramètres d'input/output en arguments de ligne de commande
    #Ajout des arguments de ligne de commande dans le dictionnaire des paramètres
    param_add_tag_cctx_file[u'input_decision_path'] = args.input_decision_path
    param_add_tag_cctx_file[u'csv_siren_path'] = args.csv_siren_path
    param_add_tag_cctx_file[u'webscraping_results_path'] = args.webscraping_results_path
    param_add_tag_cctx_file[u'output_decision_path'] = args.output_decision_path
    param_add_tag_cctx_file[u'output_csv_recap'] = args.output_csv_recap
    #Récupération de certains paramètres du dictionnaires de paramètres
    input_decision_path = param_add_tag_cctx_file[u'input_decision_path']
    csv_siren_path = param_add_tag_cctx_file[u'csv_siren_path']
    webscraping_results_path = param_add_tag_cctx_file[u'webscraping_results_path']
    output_decision_path = param_add_tag_cctx_file[u'output_decision_path']
    output_csv_recap = param_add_tag_cctx_file[u'output_csv_recap']
    #liste des fichiers de décision à traiter (filtre selon les résultats présents dans siren)
    ls_files2process = core_function.get_json_files2process(input_decision_path, output_decision_path, csv_siren_path, overwrite = overwrite)
    #liste des fichiers de web scraping non-vides (excepté le fichier entêtes)
    ls_ws_files = [os.path.splitext(ntpath.basename(file))[0] for file in glob.glob(webscraping_results_path+"*.csv") if os.path.splitext(ntpath.basename(file))[0]!="entete" and os.path.getsize(file)>0] 
    #lecture du fichier entete siren
    entete_csv_siren = core_function.read_csv_siren_entete(csv_siren_path)
    #lecture du fichier entete web scraping
    entete_csv_ws = core_function.read_csv_ws_entete(webscraping_results_path)
    #lecture des clés à garder
    list_keys_to_export = [import_name for import_name,export_name in param_add_tag_cctx_file[u'list_keys_import_export']]
    entete_csv_resultat = create_csv_recap.get_entete_df_resultat(entete_csv_siren, entete_csv_ws, param_add_tag_cctx_file)
    #traitement et filtrage des fichiers vides (sans demandeur PM)
    logger.info(u"Start to process files with no 'PM demandeur'")
    ls_files2process = create_csv_recap.filter_files_with_no_PM_demandeur(csv_siren_path, ls_files2process, entete_csv_resultat, output_csv_recap)
    logger.info(u"Files with no 'PM demandeur' successfully processed")
    #traitement des fichiers non vides (avec demandeur PM)
    logger.info(u"Start to process files with 'PM demandeurs'")
    compt_processed_files = 0
    nb_files2process = len(ls_files2process)
    for curr_filename in ls_files2process:
        logger.info(u"Processing the file: {}".format(curr_filename))
        df_ws_file = None
        dict_feature2export_all = defaultdict(lambda: [])
        #si un fichier de web scraping existe
        if entete_csv_ws is not None and curr_filename in ls_ws_files:
            #lecture du fichier SIREN
            df_siren_file = core_function.read_csv_siren_file(csv_siren_path, curr_filename, entete_csv_siren)
            siren_nrow = df_siren_file.shape[0]
            #lecture du fichier de Web scraping
            df_ws_file = core_function.read_csv_ws_file(webscraping_results_path, curr_filename, entete_csv_ws)
            #!20180308 passage_par_reference
            #extraction des features à exporter
            dict_feature2export_all = core_function.compute_dict_feature2export_all(dict_feature2export_all, df_siren_file, df_ws_file, siren_nrow, list_keys_to_export)
        #si le fichier de web scraping n'existe pas
        else:
            #lecture du fichier SIREN
            df_siren_file = core_function.read_csv_siren_file(csv_siren_path, curr_filename, entete_csv_siren)
            siren_nrow = df_siren_file.shape[0]
            #extraction des features à exporter
            dict_feature2export_all = core_function.compute_dict_feature2export_all_from_df_siren(dict_feature2export_all, df_siren_file, siren_nrow, list_keys_to_export)
        #insertion des tags dans le fichier json de jurisprudence
        logger.debug(u"Insertion des tags dans le fichier de jurisprudence")
        core_function.insert_cctx_tags_and_save(curr_filename, input_decision_path, output_decision_path, dict_feature2export_all, list_keys_to_export, param_add_tag_cctx_file)
        #Mise à jour du fichier recap
        logger.debug(u"Mise à jour du fichier recap")
        create_csv_recap.update_csv_recap(curr_filename, dict_feature2export_all, df_siren_file, df_ws_file, entete_csv_ws, entete_csv_resultat, output_csv_recap, param_add_tag_cctx_file)
        df_ws_file = None
        compt_processed_files += 1
        if compt_processed_files % 100 == 0:
            logger.info(u"{} (out of {}) files with 'PM demandeurs' successfully processed".format(compt_processed_files, nb_files2process))
    logger.info(u"All files ({}) with 'PM demandeurs' successfully processed".format(nb_files2process))
    logger.info(u"END")

if __name__ == "__main__":
    #20180320 logging
    #logging_param_filename = u"./parameters/logging.yaml"
    #logger = read_params(logging_param_filename)
    main()
    
