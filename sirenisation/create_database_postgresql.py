#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May  2 14:47:26 2018

@author: mathi
"""
#lines to create the database SIREN via psql
#psql postgres
#DROP DATABASE siren;
#CREATE DATABASE siren;
#\connect siren
#CREATE EXTENSION fuzzystrmatch;

#split the database in small chunks
import psycopg2
import math
import yaml
import codecs

#lecture du fichier de paramétres
print(u'lecture du fichier de paramétres')
param_filename = u'./parameters/parameters_siren.yaml'
param_file = yaml.load(codecs.open(param_filename , 'r', encoding = 'UTF-8'))
nb_of_chunks_db = param_file[u"nb_of_chunks_db"]
path2csv4database_psql = u"/Users/mathi/Desktop/projet_sirenisation_py27_sql/sirenisation/Data/siren_data/sirc-17804_9075_14209_201803_L_M_20180331_030235515_feather.csv"

#connect to the database
print(u'connect to the database')
connect_text = "dbname='%s' user='%s' host=%s port=%s password='%s'" % ('siren', 'postgres', 'localhost', 5432, '')
con = psycopg2.connect(connect_text)
cur = con.cursor()

#create the table
print(u'create the table SIREN')
query="""CREATE TABLE siren
(
id BIGSERIAL PRIMARY KEY,
siren varchar(255),
nic varchar(255),
l1normalisee varchar(255),
l2normalisee varchar(255),
l3normalisee varchar(255),
l4normalisee varchar(255),
l5normalisee varchar(255),
l6normalisee varchar(255),
l7normalisee varchar(255),
numvoie varchar(255),
indrep varchar(255),
typevoie varchar(255),
libvoie varchar(255),
codpos varchar(255),
cedex varchar(255),
libcom varchar(255),
enseigne varchar(255),
apet700 varchar(255),
nomen varchar(255),
nom varchar(255),
prenom varchar(255),
civilite varchar(255),
nj varchar(255),
libnj varchar(255),
datemaj varchar(255),
raisonsociale varchar(255),
formatcodepos varchar(255),
libcompretty varchar(255),
numvoiepretty varchar(255),
numvoieisempty varchar(255),
adressecomplete varchar(255),
raisonsocialelen varchar(255)
)"""
cur.execute(query)
con.commit()

#fill the database
print(u'fill the database')
query="""COPY siren(id,siren,nic,l1normalisee,l2normalisee,l3normalisee,l4normalisee,l5normalisee,l6normalisee,l7normalisee,numvoie,indrep,typevoie,libvoie,codpos,cedex,libcom,enseigne,apet700,nomen,nom,prenom,civilite,nj,libnj,datemaj,raisonsociale,formatcodepos,libcompretty,numvoiepretty,numvoieisempty,adressecomplete,raisonsocialelen)
FROM '{}' DELIMITER ',' CSV HEADER;""".format(path2csv4database_psql)
cur.execute(query)
con.commit()

#count the number of rows
print(u'count the number of rows')
query="""SELECT count(*) from siren"""
cur.execute(query)
nb_rows = cur.fetchall()
nb_rows = float(nb_rows[0][0])
print(u'Number of rows: {}'.format(nb_rows))
con.commit()

#split the database in several chunks
print(u'split the database in several chunks')
nb_of_chunks = nb_of_chunks_db
limit = int(math.ceil(nb_rows/float(nb_of_chunks)))
offset = 0
query = ''
for num in range(nb_of_chunks):
    num_dt = num + 1
    offset = int(num*limit)
    offset_1 = int((num+1)*limit)
    #query+=u"SELECT * INTO siren_light_{num_dt} FROM siren limit {limit} offset {offset};".format(num_dt=num_dt, limit=limit, offset=offset)
    query+="CREATE TABLE siren_light_{num_dt} AS SELECT * FROM siren WHERE id >= {offset} AND id < {offset_1};".format(num_dt=num_dt, offset=offset, offset_1=offset_1)
cur.execute(query)
con.commit()

