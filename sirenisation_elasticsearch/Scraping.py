#!/usr/bin/env python2 (2.7)
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 11:23:20 2018

@author: mathieu.lechine
"""

import random
import re
from collections import defaultdict
import logging
import inspect
#package for scraping
#!py36
#import urllib.request
import urllib2
from bs4 import BeautifulSoup
import signal
#my packages


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

def extract_data_from_company_soup(company_soup, param_file):
    """
    Cette fonction extrait les infos de la page web selon le fichier de paramètres et les
    renvoit sous forme d'un dictionnaire.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    table_name_rensjur = param_file[u"table_name_rensjur"]
    table_name_rensjur_complete = param_file[u"table_name_rensjur_complete"]
    #table_info_etablissement = param_file[u"table_info_etablissement"]
    #logger.debug("Received a call to function 'extract_data_from_company_soup'")
    dict_features_ws = defaultdict(lambda: None)
    #recupération des tableaux de données
    table_rensjur = get_table_renseignement(company_soup, table_name_rensjur)
    table_rensjurcomplete = get_table_renseignement(company_soup, table_name_rensjur_complete)
    #extraction des infos du tableau: table_name_rensjur
    if table_rensjur: #sinon page inattendue
        ls_field_names = param_file[table_name_rensjur]
        for field in ls_field_names:
            dict_features_ws[field] = get_field_soup_table(table_rensjur, field, match_exact = True)
    #extraction des infos du tableau: table_name_rensjur_complete
    if table_rensjurcomplete: #sinon page inattendue
        ls_field_names = param_file[table_name_rensjur_complete]
        for field in ls_field_names:
            dict_features_ws[field] = get_field_soup_table(table_rensjurcomplete, field, match_exact = False)        
    return dict_features_ws




def get_field_soup_table(table_rensjur, str_field, match_exact = True):
    """
    Extrait la denomination/raison sociale du tableau
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #logger.debug("Received a call to function 'get_field_soup_table'")
    if table_rensjur:
        if match_exact:
            table_row = table_rensjur.find(u"td", text = str_field)
        else:
            table_row = table_rensjur.find(u"td", text = re.compile(r'.*'+str_field+'.*'))
        if table_row:
            for sibling in table_row.next_siblings:
                if sibling.name == u"td":
                    return unicode(sibling.string)
    return None


    
def get_table_renseignement(company_soup, id_table):
    """
    Fonction renvoyant le table ayant pour id id_table
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    table_soup = company_soup.find_all(u"table", attrs={u"id": id_table})
    if len(table_soup)==0:
        table_soup = None
    else:
        table_soup = table_soup[0]
    return table_soup
    

def get_url_page_company(search_soup, racine_url):
    """
    Cette fonction prend en entrée le code html de la première page de recherche.
    Elle indique si c'est un match exacte ou de type 'incluant' et renvoit l'url 
    de la page la plus pertinente.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_div_monocadre = search_soup.find_all(u"div", attrs={u"class": u"monocadre"})
    ls_str_match_approx = [u"incluant", u"dont"]
    ls_str_match_exact = [u"exact"]
    ls_div_monocadre_exact, ls_div_monocadre_approx = get_monocadre_exact_approx(ls_div_monocadre, ls_str_match_exact, ls_str_match_approx)
    if len(ls_div_monocadre_exact)>0:
        div_monocadre_exact = ls_div_monocadre_exact[0] #on prend la recherche la plus pertinente
        ls_company_url = div_monocadre_exact.find_all(u'a', href=True)
        company_url = u"{}{}".format(racine_url ,ls_company_url[0][u"href"])
    elif len(ls_div_monocadre_approx)>0:
        div_monocadre_approx = ls_div_monocadre_approx[0] #la recherche la plus pertinente
        ls_company_url = div_monocadre_approx.find_all(u'a', href=True)
        company_url = u"{}{}".format(racine_url ,ls_company_url[0][u"href"])
    else:
        company_url = None
    return company_url

def get_url_page_company_and_details(search_soup, racine_url):
    """
    Cette fonction prend en entrée le code html de la première page de recherche.
    Elle indique si c'est un match exacte ou de type 'incluant' et renvoit l'url 
    de la page la plus pertinente.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    ls_div_monocadre = search_soup.find_all(u"div", attrs={u"class": u"monocadre"})
    ls_str_match_approx = [u"incluant", u"dont"]
    ls_str_match_exact = [u"exact"]
    ls_div_monocadre_exact, ls_div_monocadre_approx = get_monocadre_exact_approx(ls_div_monocadre, ls_str_match_exact, ls_str_match_approx)
    if len(ls_div_monocadre_exact)>0:
        type_of_search = u"exact"
        div_monocadre_exact = ls_div_monocadre_exact[0] #on prend la recherche la plus pertinente
        ls_company_url = div_monocadre_exact.find_all(u'a', href=True)
        company_url = u"{}{}".format(racine_url ,ls_company_url[0][u"href"])
        number_of_results = get_number_of_results(div_monocadre_exact)
    elif len(ls_div_monocadre_approx)>0:
        type_of_search = u"approx"
        div_monocadre_approx = ls_div_monocadre_approx[0] #la recherche la plus pertinente
        ls_company_url = div_monocadre_approx.find_all(u'a', href=True)
        company_url = u"{}{}".format(racine_url ,ls_company_url[0][u"href"])
        number_of_results = get_number_of_results(div_monocadre_approx)
    else:
        type_of_search = None
        company_url = None
        number_of_results = None
    return company_url, type_of_search, number_of_results

def get_number_of_results(div_monocadre_exact):
    """
    Function extracting the number of results in the monocadre
    input: div_monocadre
    output: nombre de resultats (unicode) or None if nothing is found
    """
    a_number_of_results = div_monocadre_exact.find(u'a', u'Button levelOne mt-16')
    if a_number_of_results:
        str_number_of_results = a_number_of_results.text
        match_number_of_results = re.search(r"\d+", str_number_of_results)
        if match_number_of_results:
            res_number_of_results = match_number_of_results.group()
            return res_number_of_results
    return None

def get_monocadre_exact_approx(ls_div_monocadre, ls_str_match_exact, ls_str_match_approx):
    """
    Fonction renvoyant les monocadre correspondant aux matchx exactes et approximatifs.
    Méthode: recherche de div monocadre ayant une balise h3 contenant str_match...
    """
    #logger.debug(autolog_msg(u"Received a call to function"))
    ls_div_monocadre_exact = []
    ls_div_monocadre_approx = []
    for div_monocadre in ls_div_monocadre:
        h3 = div_monocadre.find(u"h3")
        if h3:
            #logger.debug(u"h3: {}, h3.string : {}, type h3.string: {}".format(h3, h3.string, type(h3.string)))
            #chercher le monocadre avec un h3 contenant Résultat exact
            h3_unicode = unicode(h3.string)
            if sum([(str_match_exact in h3_unicode) for str_match_exact in ls_str_match_exact])>0:
                ls_div_monocadre_exact.append(div_monocadre) 
            #chercher le monocadre avec un h3 contenant Sociétés incluant
            if sum([(str_match_approx in h3_unicode) for str_match_approx in ls_str_match_approx])>0:
                ls_div_monocadre_approx.append(div_monocadre)
    return ls_div_monocadre_exact, ls_div_monocadre_approx

def make_search_url_for_raison_sociale(base_requete_url, raison_sociale):
    """
    Fonction renvoiyant l'url à requêter pour la raison sociale en entrée.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    return base_requete_url + "+".join(raison_sociale.split())
    
    
def make_url_request(requete_url, random_useragent, ls_proxies = None):
    """
    Cette fonction fait la requete de l'url (en prenant en compte des paramètres tels que 
    le proxy ou le user-agent).
    Elle renvoit la page html de la requête sous forme de BeautifulSoup.
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    #force a time out exception
    def handler(signum, frame):
        logger.info(autolog_msg(u"Customized timeout Exception triggered for url request."))
        raise Exception("Customized timeout Exception")
    # Register the signal function handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(10)
    #make url request
    try:
        #!py36
        #req = urllib.request.Request(requete_url)
        req = urllib2.Request(requete_url)
        req.add_header(u'User-Agent', random_useragent)
        if ls_proxies: #on définit un proxy pour la requête
            proxy = random_proxy(ls_proxies)
            req.set_proxy(proxy[u'ip'] + ':' + proxy[u'port'], u'http')
        #!py36
        #html_response = urllib.request.urlopen(req).read()
        #html_response = unicode(urllib2.urlopen(req).read(), encoding = "iso-8859-1")
        html_response = unicode(urllib2.urlopen(req, timeout=5.0).read(), encoding = "iso-8859-1")
        soup = BeautifulSoup(html_response, 'html.parser')
    except:
        logger.warning(u"The web request to the url '{}' failed.".format(requete_url))
        soup = None
    # Disable the alarm
    signal.alarm(0)
    return soup
    
def random_proxy(ls_proxies):
    """
    Fonction renvoyant un proxy au hasard parmi la liste de proxy
    """
    logger.debug(autolog_msg(u"Received a call to function"))
    return ls_proxies[random.randint(0, len(ls_proxies) - 1)] 