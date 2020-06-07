#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 08:31:48 2018

@author: mathieu.lechine
"""

import os
import glob
import ntpath
import re
import zipfile
import pandas as pd
import feather
import logging
import inspect
#import threading
#package for scraping
#!py36
#import urllib.request #https://stackoverflow.com/questions/3745771/urllib-request-in-python-2-7
import urllib2
from bs4 import BeautifulSoup
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



def extract_filename_from_path(pathname, keep_extension = False):
    """
    Extraction du nom du fichier à partir du chemin d'accès
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    resultat = ntpath.basename(pathname)
    if keep_extension:
        return resultat
    else:
        return os.path.splitext(resultat)[0]

def extract_date_of_data_from_siren_filename(siren_filename, character_sep = "_"):
    """
    Extraction de la date des données contenues dans le fichier CSV à partir du nom du fichier.
    Présupposé: 
        - la date se trouve entre le 3ème et le 4ème underscore
        - la date est sous le formar yyyymm
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    list_underscore_position = [i for i, letter in enumerate(siren_filename) if letter == character_sep]
    if len(list_underscore_position)<4:
        logger.warning(u"Function extract_date_of_data_from_siren_filename: the input filename '{}' has a wrong format.".format(siren_filename))
        return None
    data_date = siren_filename[(list_underscore_position[2]+1):list_underscore_position[3]]
    #si la date n'est pas au bon format on renvoit none
    if len(data_date) != 6:
        return None
    else:
        return data_date

def most_recent_siren_file_in_folder(siren_data_folder, file_extension = ".csv"):
    """
    extraction du fichier csv le plus récent du dossier
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    most_recent_siren_csv_file = None
    most_recent_siren_csv_date = None
    siren_csv_files = glob.glob(siren_data_folder+"/*"+file_extension)
    for siren_csv_file in siren_csv_files:
        siren_csv_filename = extract_filename_from_path(siren_csv_file, keep_extension = False)
        #print(siren_csv_filename)
        siren_csv_date = extract_date_of_data_from_siren_filename(siren_csv_filename)
        #print(siren_csv_date)
        if siren_csv_date:
            if most_recent_siren_csv_date:
                if siren_csv_date>most_recent_siren_csv_date:
                    most_recent_siren_csv_date = siren_csv_date
                    most_recent_siren_csv_file = siren_csv_file
            else:
                most_recent_siren_csv_date = siren_csv_date
                most_recent_siren_csv_file = siren_csv_file
    return(most_recent_siren_csv_file,most_recent_siren_csv_date)
    
    
def extract_date_of_data_from_stock_filename(stock_filename, character_sep = "_"):
    """
    Extraction de la date des données contenues dans le fichier CSV à partir du nom du fichier.
    Présupposé: 
        - la date se trouve entre le 1er et le 2ème underscore
        - la date est sous le formar yyyymm
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    list_underscore_position = [i for i, letter in enumerate(stock_filename) if letter == character_sep]
    if len(list_underscore_position)<2:
        logger.warning(u"Function extract_date_of_data_from_stock_filename: the input filename '{}' has a wrong format.".format(stock_filename))
        return None
    stock_date = stock_filename[(list_underscore_position[0]+1):list_underscore_position[1]]
    #si la date n'est pas au bon format on renvoit none
    if len(stock_date) != 6:
        return None
    else:
        return stock_date

def most_recent_stock_file_on_INSEEsite(url_INSEE):
    """
    Extraction de la date du fichier stock le plus récent disponible sur le site de l'INSEE (url_INSEE)
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    try:
        #!py36
        #html_response = urllib.request.urlopen(urllib.request.Request(url_INSEE)).read()
        html_response = urllib2.urlopen(url_INSEE).read()
    except:
        #mettre un warning
        logger.warning(u"L'url '{}' ne répond pas.".format(url_INSEE))
        return(None,None)
    soup = BeautifulSoup(html_response, 'html.parser')
    #la liste des fichiers disponibles est dans la section <div class="list-group resources-list">
    div_html = soup.find_all("div", attrs={"class": "list-group resources-list"})[0]
    #with open("Output/html_INSEE.txt","w") as file: 
    #    file.write(div_html.prettify())
    #extract all links for the stock files available on INSEE site
    list_stock_files = [link.get('href') for link in div_html.findAll('a', attrs={'href': re.compile("^http://files.data.gouv.fr/sirene/.*_L_M.zip$")})]
    #
    most_recent_stock_file = None
    most_recent_stock_date = None
    for stock_file in list_stock_files:
        filename = os.path.splitext(ntpath.basename(stock_file))[0]
        #extract the date
        stock_date = extract_date_of_data_from_stock_filename(filename)
        if stock_date:
            if most_recent_stock_date:
                if stock_date>most_recent_stock_date:
                    most_recent_stock_date = stock_date
                    most_recent_stock_file = stock_file
            else:
                most_recent_stock_date = stock_date
                most_recent_stock_file = stock_file
    logger.info(u"The most recent stock file found on INSEE website '{}' dates from '{}'".format(url_INSEE, most_recent_stock_date))
    return(most_recent_stock_file, most_recent_stock_date)
    
def download_and_unzip_stock_file(stock_file_url, zip_dest_filename, dest_folder):
    """
    Télécharge le le fichier à l'url stock_file_url à l'emplacement dest_filename
    Unzip le fichier télécharger et renvoie le nom du nouveau fichier
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #téléchargement du fichier
    logger.info(u"Start of the download of the file '{}' to the folder {}".format(stock_file_url, dest_folder))
    try:
        #!py36
        #zip_filename,_ = urllib.request.urlretrieve(stock_file_url, filename=zip_dest_filename)
        with open(zip_dest_filename,'wb') as f:
            f.write(urllib2.urlopen(stock_file_url).read())
            f.close()
        zip_filename = zip_dest_filename
    except:
        logger.warning(u"L'url '{}' ne répond pas.".format(stock_file_url))
        return None
    logger.info(u"The file has successfully be downloaded")
    #dezippage de l'archive
    logger.info(u"Start to decompress the archive '{}'".format(zip_filename))
    zip_ref = zipfile.ZipFile(zip_filename, 'r')
    zip_filename_list = zip_ref.namelist()
    zip_ref.extractall(dest_folder)
    zip_ref.close()
    logger.info(u"The archive has been successfully decompressed to {}".format(dest_folder))
    #recuperation du nom du fichier
    return zip_filename_list[0]
 


def update_database(siren_data_folder, download_new_db, file_extension = ".csv"):
    """
    Cette fonction prend en argument le dossier de données SIREN. Elle vérifie la date 
    du dernier fichier .csv
    download_new_db:
        - 0: par defaut, vérifie si nouveaux fichier stock sur l'INSEE
        - 1: Pas de téléchargement du nouveaux fichiers sur l'INSEE
        - 2: forcer le téléchargement du fichier le plus récent sur l'INSEE
    return bool_nouveau_fichier_stock, nom_fichier_du_csv_le_plus_recent
        Indique_nouveaux_fichiers==True indique la présence d'un nouveau fichier CSV
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    url_INSEE = "https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/"
    #extraction du fichier csv le plus récent du dossier
    most_recent_siren_csv_file, most_recent_siren_csv_date = most_recent_siren_file_in_folder(siren_data_folder, file_extension = ".csv")
    if download_new_db==0:  
        #extraction du fichier stock le plus recent du site de l'INSEE
        most_recent_stock_file, most_recent_stock_date = most_recent_stock_file_on_INSEEsite(url_INSEE)
        if not most_recent_stock_file: #si site INSEE indisponible
            logger.warning(u"The INSEE website is not available")
            if most_recent_siren_csv_file: #on utilise le dernier CSV
                bool_nouveau_fichier_stock = False
                most_recent_csv_file = most_recent_siren_csv_file
                logger.warning(u"The most recent SIREN (stock) file '{}' of the folder {} will be used".format(most_recent_csv_file, siren_data_folder))
            else: #si aucun CSV -> abort
                logger.error(u"The INSEE website does not respond and no CSV files in the folder {}.".format(siren_data_folder))
                raise ValueError(u"The website of INSEE does not respond and no CSV files in the folder {}.".format(siren_data_folder))
        else: #si site de l'INSEE disponible
            if most_recent_siren_csv_file: 
                #on compare la date des fichier stock du site de l'INSEE et du répertoire
                if most_recent_siren_csv_date<most_recent_stock_date:
                    #téléchargement de la base de données
                    new_csv_filename = download_and_unzip_stock_file(stock_file_url = most_recent_stock_file,
                                                                     zip_dest_filename = siren_data_folder + ntpath.basename(most_recent_stock_file),
                                                                     dest_folder = siren_data_folder)
                    bool_nouveau_fichier_stock = True
                    most_recent_csv_file = siren_data_folder + new_csv_filename
                else:
                    logger.info(u"There is no new stock SIREN file on the INSEE website")
                    bool_nouveau_fichier_stock = False
                    most_recent_csv_file = most_recent_siren_csv_file
            else: #répertoire vide
                #téléchargement de la base de données
                new_csv_filename = download_and_unzip_stock_file(stock_file_url = most_recent_stock_file,
                                                                 zip_dest_filename = siren_data_folder + ntpath.basename(most_recent_stock_file),
                                                                 dest_folder = siren_data_folder)
                bool_nouveau_fichier_stock = True
                most_recent_csv_file = siren_data_folder + new_csv_filename
    elif download_new_db==1:
        bool_nouveau_fichier_stock = False
        if most_recent_siren_csv_file:
            most_recent_csv_file = most_recent_siren_csv_file
        else:
            logger.error(u"No CSV files in the folder {}. The user need to use one of \
                             the following option --download_new_db 0 or --download_new_db 2".format(siren_data_folder))
            raise ValueError(u"No CSV files in the folder {}. The user need to use one of \
                             the following option --download_new_db 0 or --download_new_db 2".format(siren_data_folder))
    elif download_new_db==2:
        #extraction du fichier stock le plus recent du site de l'INSEE
        most_recent_stock_file, most_recent_stock_date = most_recent_stock_file_on_INSEEsite(url_INSEE)
        if most_recent_stock_file is None:
            logger.warning(u"Le fichier stock INSEE le plus récent déjà téléchargé sera utilisé à la place.")
            if most_recent_siren_csv_file:
                most_recent_csv_file = most_recent_siren_csv_file
                bool_nouveau_fichier_stock = False
            else:
                logger.error(u"No CSV files in the folder {}. Aucune base de données SIREN à disposition \
                             le programme va s'arrêter.".format(siren_data_folder))
                raise ValueError(u"No CSV files in the folder {}.".format(siren_data_folder))
        else:
            #téléchargement de la base de données
            new_csv_filename = download_and_unzip_stock_file(stock_file_url = most_recent_stock_file,
                                          zip_dest_filename = siren_data_folder + ntpath.basename(most_recent_stock_file),
                                          dest_folder = siren_data_folder)
            bool_nouveau_fichier_stock = True
            most_recent_csv_file = siren_data_folder + new_csv_filename
    return (bool_nouveau_fichier_stock, most_recent_csv_file)


def preprocess_and_save_siren_database(siren_csv_file, output_feather_filename, param_file):
    """
    Cette fonction prend en entrée le fichier stock au format CSV, fait un preprocessing des
    données et enregistre le résultats au format .feather
    Cette fonction renvoit le nom du fichier .feather
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    logger.info(u"Starting to load the file {} in memory".format(siren_csv_file))
    df_siren = pd.read_csv(siren_csv_file, sep = ";", header = 0, index_col = False,
                    usecols = param_file[u'column2import'], encoding = "iso8859_15", 
                    dtype = unicode, na_filter = False)
    logger.info(u"The file has been successfully loaded")
    logger.info(u"Start the processsing of the SIREN database")
    #creation d'une  colonne raison sociale
    #def make_raison_sociale_col(row):
    #    if row.NOM=='' and row.PRENOM=='':
    #        return row.NOMEN_LONG.strip()
    #    else:
    #        return (row.NOM.strip() + " " + row.PRENOM.strip()).strip()
    #df_siren["raison_sociale"] = df_siren.apply(make_raison_sociale_col, axis=1)
    index_NOMEN_LONG = (df_siren[u"NOM"]=="") & (df_siren[u"PRENOM"]=="")
    df_siren[u"raison_sociale"] = u""
    df_siren.loc[index_NOMEN_LONG,u"raison_sociale"] = df_siren.loc[index_NOMEN_LONG,u"NOMEN_LONG"]
    df_siren.loc[~index_NOMEN_LONG,u"raison_sociale"] = (df_siren.loc[~index_NOMEN_LONG,u"NOM"] + u" " + df_siren.loc[~index_NOMEN_LONG,u"PRENOM"])
    df_siren[u"raison_sociale"] = df_siren[u"raison_sociale"].apply(tools.normalize_string)
    #verification du pattern du code postal
    #si le code postal est vide alors on le remplace par le pays car la plupart du temps
    #ce sont des entreprises à l'étranger
    reg_code_postal = re.compile(r"^[0-9]{5}$")
    df_siren[u"format_CODPOS"] = df_siren[u"CODPOS"].apply(lambda codpos: True if reg_code_postal.match(codpos) else False)
    #prétraitement du nom des villes
    def preprocess_libcom(libcom):
        """
        Remove all digits from the string and normalize the string with the usual 
        fucnction tools.normalize_string
        """
        return tools.normalize_string(re.sub(r'[0-9]+', '', libcom))
    df_siren[u'LIBCOM_pretty'] = df_siren[u'LIBCOM'].apply(preprocess_libcom)
    #prétraitement des numéros de voie
    df_siren[u'NUMVOIE_pretty'] = df_siren[u'NUMVOIE'].apply(tools.normalize_string)
    #tag des lignes sans numéros de voie ou avec des numéros à un format incorrect
    reg_num_voie = re.compile(r"^[0-9]+$")
    df_siren[u"NUMVOIE_isempty"] = df_siren[u"NUMVOIE_pretty"].apply(lambda num_voie: False if reg_num_voie.match(num_voie) else True)
    df_siren.loc[df_siren[u"NUMVOIE_isempty"], u'NUMVOIE_pretty'] = u""
    #creation de l'adresse complete
    index_adresse_complete = (df_siren[u"L4_NORMALISEE"] != u"") & (df_siren[u"L6_NORMALISEE"]!= u"")
    df_siren[u"ADRESSE_COMPLETE"] = u""
    df_siren.loc[index_adresse_complete, u"ADRESSE_COMPLETE"] = (df_siren[u"L4_NORMALISEE"]+ u" " + df_siren[u"L6_NORMALISEE"]).apply(tools.normalize_string)
    #creation de la colonne longueur de la raison sociale
    df_siren[u"raison_sociale_len"] = df_siren[u"raison_sociale"].str.len()
    logger.info(u"The SIREN database has been successfully preprocessed")
    #enregistrement de la base au format feather
    logger.info(u"Start to save the preprocessed database at {}".format(output_feather_filename))
    feather.write_dataframe(df_siren, output_feather_filename)
    #df_siren.to_feather(output_feather_filename)
    logger.info(u"The preprocessed database has been successfully saved in feather format")
    return(df_siren)
    
    
def load_preprocessed_siren_database_in_memory(siren_data_folder, param_file, bool_nouveau_fichier_stock):
    """
    Cette fonction mets en mémoire la base de données SIREN la plus à jour du dossier.
    Elle présuppose que les fichiers CSV de la base de données sont à jour.
    Elle renvoit le base de données SIREN préprocessée.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #détection du fichier CSV le plus récent
    most_recent_siren_csv_file, most_recent_siren_csv_date = most_recent_siren_file_in_folder(siren_data_folder)
    #vérifie si le fichier feather associé est déjà créé
    associated_feather_file = most_recent_siren_csv_file.replace(".csv", ".feather")
    if bool_nouveau_fichier_stock:
        #nouveau fichier stock le fichier feather doit être crée (ou recréé)
        df_siren = preprocess_and_save_siren_database(most_recent_siren_csv_file, associated_feather_file, param_file) 
    else:
        bool_feather_file_exists = os.path.isfile(associated_feather_file)
        if bool_feather_file_exists: 
            logger.info(u"Loading the file {} in memory".format(associated_feather_file))
            #le fichier feather existe déjà, il suffit de le charger en mémoire
            df_siren = feather.read_dataframe(associated_feather_file)
            #df_siren = pd.read_feather(associated_feather_file, nthreads=threading.active_count()-1)
        else:
            #le fichier feather n'existe pas encore, il faut le créer à partir du csv
            df_siren = preprocess_and_save_siren_database(most_recent_siren_csv_file, associated_feather_file, param_file) 
    return(df_siren)



