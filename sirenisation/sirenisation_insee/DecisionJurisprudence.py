#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 18:31:50 2018

@author: mathieu.lechine
"""


import os
import ntpath
import json
import re
from collections import defaultdict
import logging
import inspect
import codecs
import copy
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

class DecisionJurisprudence:
    """
    Object représentant une décision de jurisprudence au format JSON
    """
    def __init__(self, jurisprud_filename):
        logger.debug(autolog_msg(u"Received a call to function"))
        self.jurisprud_filename = os.path.splitext(ntpath.basename(jurisprud_filename))[0]
        self.jurisprud_pathAndfilename = jurisprud_filename
        #!py36
        #self.json_content = json.load(codecs.open(jurisprud_filename, 'r', encoding = 'UTF-8'))
        with codecs.open(jurisprud_filename, 'r', encoding = 'UTF-8') as f:
            self.json_content = json.load(f, encoding = 'UTF-8')   
        #self.json_content = json.load(codecs.open(jurisprud_filename, 'r', encoding = 'UTF-8'), encoding = 'UTF-8')
        self.list_demandeur_features = None
        self.list_demandeurs_PM = None
        self.nb_demandeur_PM = 0
    
    def __repr__(self):
        logger.debug(autolog_msg(u"Received a call to function"))
        res = u"Nombre de demandeurs: {}".format(self.nb_demandeur_PM)
        res = res + "\n" + str(self.list_demandeur_features)
        return res
    
    def get_nb_demandeur_PM(self):
        """
        Retourne le nombre de demandeurs de nature PM
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        return self.nb_demandeur_PM
    
    def get_dict_features_demandeur(self, numero):
        """
        Retourne le dictionnaire de features lié au demandeur numero "numero"
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        if numero<=self.nb_demandeur_PM:
            #!20180308 passage_par_reference -> copy par valeur pour éviter la modification depuis l'extérieur de la classe
            return copy.deepcopy(self.list_demandeur_features[numero])
        else:
            logger.warning(u"{} :: Le numero '{}' de demandeur demandé excède le nombre de demandeurs.".
                          format(self.jurisprud_filename, numero))
            return None
    
#    def compute_list_demandeurs_PM(self):
#        """
#        Calcule la liste des demandeurs de nature PM (personne morale) 
#        """
#        logger.debug(autolog_msg(u"Received a call to function"))
#        list_key = [u"contractTags", u"metaTags", u"Demandeur", u"tags"]
#        list_demandeurs_PM = check_keys_existence(self.json_content, list_key)
#        if list_demandeurs_PM:
#            self.list_demandeurs_PM = [demandeur for demandeur in list_demandeurs_PM if 'PM' in demandeur]
#            self.nb_demandeur_PM = len(self.list_demandeurs_PM)
#            self.list_demandeur_features = [defaultdict(lambda: None)]*self.nb_demandeur_PM
#        else:
#            self.list_demandeurs_PM = None
#            self.nb_demandeur_PM = 0
    
    #20180306: prise en compte des entreprises se trouvant dans le tag: "contractTags>metaTags>Defendeur"
    def compute_list_demandeurs_PM(self):
        """
        Calcule la liste des demandeurs et defendeurs de nature PM (personne morale)
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        #demandeurs
        list_key = [u"contractTags", u"metaTags", u"Demandeur", u"tags"]
        list_demandeurs_PM = check_keys_existence(self.json_content, list_key)
        #defendeurs
        list_key = [u"contractTags", u"metaTags", u"Defendeur", u"tags"]
        list_defendeurs_PM = check_keys_existence(self.json_content, list_key)
        #concatenation des listes demandeurs et defendeurs avec gestion des None
        if list_demandeurs_PM is None and list_defendeurs_PM is None:
            list_PM = None
        elif list_demandeurs_PM is None:
            list_PM = list_defendeurs_PM
        elif list_defendeurs_PM is None:
            list_PM = list_demandeurs_PM
        else:
            list_PM = list_demandeurs_PM + list_defendeurs_PM
        #filtre des Personnes Morales (PM)
        if list_PM is not None:
            self.list_demandeurs_PM = [demandeur for demandeur in list_PM if 'PM' in demandeur]
            self.nb_demandeur_PM = len(self.list_demandeurs_PM)
            #!20180308 passage_par_reference
            self.list_demandeur_features = [defaultdict(lambda: None) for _ in xrange(self.nb_demandeur_PM)]
        else:
            self.list_demandeurs_PM = None
            self.nb_demandeur_PM = 0
    
    def compute_demandeurs_name(self):
        """
        Extrait le nom de chaque demandeur PM et remplit list_demandeur_features
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        for i in range(self.nb_demandeur_PM):
            demandeur_PM = self.list_demandeurs_PM[i]
            if u"payload" in demandeur_PM:
                self.list_demandeur_features[i][u"demandeur_name"] = tools.normalize_string(demandeur_PM[u"payload"])
    
    def compute_demandeurs_raison_sociale(self):
        """
        Extrait la raison sociale chaque demandeur PM et remplit list_demandeur_features
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        for i in range(self.nb_demandeur_PM):
            demandeur_PM = self.list_demandeurs_PM[i]
            raison_sociale = check_keys_existence(demandeur_PM, [u"PM", 0, u"Raison", 0, u"payload"])
            if raison_sociale:
                self.list_demandeur_features[i][u"raison_sociale"] = tools.normalize_string(raison_sociale)
    
    def compute_demandeurs_code_postal(self):
        """
        Extrait du code postal de chaque demandeur PM et remplit list_demandeur_features
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        for i in range(self.nb_demandeur_PM):
            demandeur_PM = self.list_demandeurs_PM[i]
            code_postal = check_keys_existence(demandeur_PM, 
                                                  [u"PM", 0, u"Adresse",0, u"CodePostal", 0, u"payload"])
            if code_postal:
                code_postal = tools.normalize_string(code_postal)
                reg_code_postal = re.compile(r"^[0-9]{5}$")
                match_code_postal = reg_code_postal.match(code_postal)
                if match_code_postal:
                    self.list_demandeur_features[i][u"code_postal"] = code_postal
    
    def compute_demandeurs_num_rue(self):
        """
        Extrait le numero de la rue de chaque demandeur PM et remplit list_demandeur_features
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        for i in range(self.nb_demandeur_PM):
            demandeur_PM = self.list_demandeurs_PM[i]
            num_rue = check_keys_existence(demandeur_PM, [u"PM", 0, u"Adresse",0 , u"NumRue", 0, u"payload"])
            if num_rue:
                self.list_demandeur_features[i][u"num_rue"] = tools.normalize_string(tools.convert2unicode(num_rue))
    
    def compute_demandeurs_ville(self):
        """
        Extrait la ville de chaque demandeur PM et remplit list_demandeur_features
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        for i in range(self.nb_demandeur_PM):
            demandeur_PM = self.list_demandeurs_PM[i]
            ville = check_keys_existence(demandeur_PM, 
                                                  [u"PM", 0, u"Adresse",0, u"Ville", 0, u"payload"])
            if ville:
                self.list_demandeur_features[i][u"ville"] = tools.normalize_string(tools.convert2unicode(ville))
    
    def compute_demandeurs_adresse_complete(self):
        """
        Extrait le numero de la rue de chaque demandeur PM et remplit list_demandeur_features
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        for i in range(self.nb_demandeur_PM):
            demandeur_PM = self.list_demandeurs_PM[i]
            adresse_complete = check_keys_existence(demandeur_PM, 
                                                  [u"PM", 0, u"Adresse",0, u"payload"])
            if adresse_complete:
                self.list_demandeur_features[i][u"adresse_complete"] = tools.normalize_string(tools.convert2unicode(adresse_complete))
    
    def compute_all_features(self):
        """
        Remplit list_demandeur_features
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        if self.json_content is None:
            logging.warning(u"The json file {} is empty.".format(self.jurisprud_filename))
        else:
            self.compute_list_demandeurs_PM()
            self.compute_demandeurs_name()
            self.compute_demandeurs_raison_sociale()
            self.compute_demandeurs_code_postal()
            self.compute_demandeurs_num_rue()
            self.compute_demandeurs_ville()
            self.compute_demandeurs_adresse_complete()
    
    def insert_tag_cctx_entreprise(self, tagname, list2export):
        """
        Insère le tag tagname à la racince du contenu json
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        #on vérifie que le nombre de demandeurs PM est cohérent
        self.compute_list_demandeurs_PM()
        self.json_content[tagname] = list2export
        if(self.nb_demandeur_PM != len(list2export)):
            logger.warning(u"Le nombre de d'entreprises trouvées dans le fichier json jurisprudence et le nombre \
                           d'entreprise sirénisées ne sont pas égaux. Il est probrable que certaines entreprises n'aient \
                           pas été sirénisées.")
    
    def export_json_content2jsonfile(self, output_jurisprud_file):
        """
        Export du contenu json_content au format json
        """
        logger.debug(autolog_msg(u"Received a call to function"))
        #!py36
        #with open(output_jurisprud_file, "w", encoding = 'UTF-8') as out:
        with codecs.open(output_jurisprud_file, "w", encoding = 'UTF-8') as out:
            #json.dump(self.json_content, out, ensure_ascii=False)
            json.dump(self.json_content, out, ensure_ascii=False, encoding='UTF-8')
        
def check_keys_existence(objet, list_key):
    """
    Vérifie si la liste de clé list_key est un chemin valide de l'objet
    Si oui, l'objet au bout du chemin d'accès, sinon retourne None
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #!20180308 passage_par_reference
    #deep copy pour ne pas modidier le dictionnaire en entrée
    copy_objet = copy.deepcopy(objet)
    for key in list_key:
        if type(key) == str or type(key) == unicode: #cas d'un dictionnaire
            key = tools.convert2unicode(key)
            if key not in copy_objet:
                logger.warning(u"La clé {} n'a pas été trouvé.".format(key))
                return None
            copy_objet = copy_objet[key]
        elif type(key) == int:
            if len(copy_objet) <= key:
                logger.warning(u"La liste a une longeur insuffisante pour la clé {}.".format(key))
                return None
            copy_objet = copy_objet[key]
        else:
            logger.warning(u"La clé {} a un type inconnu".format(key))
            return None
    return copy_objet
     







