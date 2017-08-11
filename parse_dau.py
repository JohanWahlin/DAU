# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 13:39:00 2017

@author: johwah
"""

import psycopg2
import pandas as pd
conn = psycopg2.connect("dbname=daudata user=postgres password=aron")
cur = conn.cursor()

cur.execute("create schema if not exists raw_data")
sql = "create table raw_data.daudata( \
    header_datumtid timestamp,\
    vehicle_id text,\
    dau_kod text,\
    datumtid timestamp,\
    start_stop text,\
    lat double precision,\
    lon double precision,\
    dist_kjort double precision,\
    hastighet double precision,\
    dist_kjort_torrspreder double precision,\
    sprederbredde_torrstoff double precision,\
    dosering_torrstoff double precision,\
    prc_vatstoff_i_torrstoff double precision,\
    tot_teo_mengd_torrstoff double precision,\
    tot_bruk_vatstoff_i_torrstoff double precision,\
    stroing_torr_aktiv bit,\
    plog_nede bit,\
    sprederbredde_vatstoff double precision,\
    dosering_vatstoff double precision,\
    tot_teo_mengd_vatstoff double precision,\
    dist_kjort_vatspreder double precision,\
    stroing_vat_aktiv bit,\
    spredesymmetri double precision,\
    forventet_bruk_torrstoff double precision,\
    forventet_bruk_vatstoff double precision,\
    saltrester double precision,\
    materialtype_kode text,\
    friktion double precision,\
    vegtemp double precision,\
    lufttemp double precision,\
    luftfukt double precision,\
    vegforhold text,\
    vaerforhold text,\
    skydekke text,\
    sensorer_paskrudd text,\
    vegreferense text,\
    hovel_nede bit,\
    midtskjaer_nede bit,\
    sideplog_ibruk bit,\
    mengde_torrt_veiesys double precision,\
    mengde_vat_veiesys double precision)"

cur.execute("DROP TABLE IF EXISTS raw_data.daudata")
cur.execute(sql)
conn.commit()
print("Database tables created")



import numpy as np
import fnmatch
import os
import codecs
matches = []
for root, dirnames, filenames in os.walk('O:/6/TMT/68060 Vegteknologi/02. Personlige mapper/Wåhlin/data/DAU/elrapp.nois.no'):
  for filename in fnmatch.filter(filenames, '*.SUCCESS'):
      matches.append(os.path.join(root, filename))
print("The following files where found")
print(matches)

def parse_dau(filename,conn,cur):


        file = codecs.open(filename, 'rb', 'cp1252')
        i = 0
        for line in file:
            i += 1     
            line_columns=line.split(';') 
            code=line_columns[0] #reads the first column of the file
  
            if code =='DAU': #identifies file as DAU
                print('Dau file')
            elif code == '0001': #indentifies beginning of file
                print('BOF')
            elif code == '931100': #identifies a VINTERMAN header containing vehicle_id. Vinterman headers can show up on any line in a file.
                vehicle_id=line_columns[3] #vehicle ID is found in the 4th column of a VINTERMAN header
                header_date=line_columns[1]
                header_time=line_columns[2]
            elif code == '931105' or code == '931106' or code == '931107' or code == '931108':
                line = line.strip()
                line = line.replace(',','.')
                line = line.strip()
                
                ''' Different read-patterns depending on the elrapp dau data version'''
                if code =='931105':
                    (code, date, time, status,lat,lon,dist,speed,dist_m_spreder_torr,sprederbredde_torr,
                     dosering_torr,prc_vatstoff_tillsatt,teo_mengde_torrstoff,tot_bruk_vatstoff,torr_spreder_bruk_bool,
                     plog_nede_bool,sprederbredde_vat,dosering_vat,teo_mengde_vatstoff,dist_m_spreder_vat,
                     vat_spreder_bruk_bool,spredesymmetri,forventet_bruk_torr,forventet_bruk_vat,saltrester,
                     material_type,friktion,vegtemp,lufttemp,luftfukt, vegforhold,vaerforhold,skydekke,
                     sensorer_paskrudd, vegref)=[(x if x!='' else None) for x in line.split(';')]
                    
                    hovel_nede_bool=None
                    midskjer_nede_bool=None
                    sideplog_bruk_bool=None
                    mengd_torrt_veiesys=None
                    mengd_vatt_veiesys=None
                
                if code =='931106':
                    (code, date, time, status,lat,lon,dist,speed,dist_m_spreder_torr,sprederbredde_torr,
                     dosering_torr,prc_vatstoff_tillsatt,teo_mengde_torrstoff,tot_bruk_vatstoff,torr_spreder_bruk_bool,
                     plog_nede_bool,sprederbredde_vat,dosering_vat,teo_mengde_vatstoff,dist_m_spreder_vat,
                     vat_spreder_bruk_bool,spredesymmetri,forventet_bruk_torr,forventet_bruk_vat,saltrester,
                     material_type,friktion,vegtemp,lufttemp,luftfukt, vegforhold,vaerforhold,skydekke,
                     sensorer_paskrudd, vegref,hovel_nede_bool)=[(x if x!='' else None) for x in line.split(';')]
        
                    midskjer_nede_bool=None
                    sideplog_bruk_bool=None
                    mengd_torrt_veiesys=None
                    mengd_vatt_veiesys=None
            
                if code =='931107':
                    (code, date, time, status,lat,lon,dist,speed,dist_m_spreder_torr,sprederbredde_torr,
                     dosering_torr,prc_vatstoff_tillsatt,teo_mengde_torrstoff,tot_bruk_vatstoff,torr_spreder_bruk_bool,
                     plog_nede_bool,sprederbredde_vat,dosering_vat,teo_mengde_vatstoff,dist_m_spreder_vat,
                     vat_spreder_bruk_bool,spredesymmetri,forventet_bruk_torr,forventet_bruk_vat,saltrester,
                     material_type,friktion,vegtemp,lufttemp,luftfukt,vegforhold,vaerforhold,skydekke,
                     sensorer_paskrudd, vegref,hovel_nede_bool,midskjer_nede_bool,
                     sideplog_bruk_bool)=[(x if x!='' else None) for x in line.split(';')]
                    
                    mengd_torrt_veiesys=None
                    mengd_vatt_veiesys=None
                    
                if code =='931108':
                    (code, date, time, status,lat,lon,dist,speed,dist_m_spreder_torr,sprederbredde_torr,
                     dosering_torr,prc_vatstoff_tillsatt,teo_mengde_torrstoff,tot_bruk_vatstoff,torr_spreder_bruk_bool,
                     plog_nede_bool,sprederbredde_vat,dosering_vat,teo_mengde_vatstoff,dist_m_spreder_vat,
                     vat_spreder_bruk_bool,spredesymmetri,forventet_bruk_torr,forventet_bruk_vat,saltrester,
                     material_type,friktion,vegtemp,lufttemp,luftfukt,vegforhold,vaerforhold,skydekke,
                     sensorer_paskrudd, vegref,hovel_nede_bool,midskjer_nede_bool,sideplog_bruk_bool,
                     mengd_torrt_veiesys,mengd_vatt_veiesys)=[(x if x!='' else None) for x in line.split(';')]


                '''Read data is then treated the same way, and written to db in the same way'''
                timestamp = date[0:4]+'-'+date[4:6]+'-'+date[6:] +' '+time[0:2]+':'+time[2:4]+':'+time[4:] #date and time into sql timestamp
                header_timestamp = header_date[0:4]+'-'+header_date[4:6]+'-'+header_date[6:] +' '+header_time[0:2]+':'+header_time[2:4]+':'+header_time[4:]
                lat=float(lat)*180/np.pi #lat from radians to decimal degrees
                lon=float(lon)*180/np.pi #lon from radians to decimal degrees
                ''' bool variables to bool datatype unless empty'''
                #torr_spreder_bruk_bool==[bool(torr_spreder_bruk_bool) if torr_spreder_bruk_bool!= None else None] 
                #plog_nede_bool=[bool(plog_nede_bool) if plog_nede_bool!= None else None]
                #vat_spreder_bruk_bool=[bool(vat_spreder_bruk_bool) if vat_spreder_bruk_bool!= None else None]
                #hovel_nede_bool=[bool(hovel_nede_bool) if hovel_nede_bool!= None else None]
                #midskjer_nede_bool=[bool(midskjer_nede_bool) if midskjer_nede_bool!= None else None]
                #sideplog_bruk_bool=[bool(sideplog_bruk_bool) if sideplog_bruk_bool!= None else None]


                '''write everything to database'''
                cur.execute("INSERT INTO raw_data.daudata values (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                                  %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                                  %s,%s,%s,%s,%s,%s,%s,%s,%s)",(header_timestamp,vehicle_id,code, timestamp, status,lat,lon,dist,speed,dist_m_spreder_torr,sprederbredde_torr,
                                                                 dosering_torr,prc_vatstoff_tillsatt,teo_mengde_torrstoff,tot_bruk_vatstoff,torr_spreder_bruk_bool,
                                                                 plog_nede_bool,sprederbredde_vat,dosering_vat,teo_mengde_vatstoff,dist_m_spreder_vat,
                                                                 vat_spreder_bruk_bool,spredesymmetri,forventet_bruk_torr,forventet_bruk_vat,saltrester,
                                                                 material_type,friktion,vegtemp,lufttemp,luftfukt,vegforhold,vaerforhold,skydekke,
                                                                 sensorer_paskrudd, vegref,hovel_nede_bool,midskjer_nede_bool,sideplog_bruk_bool,
                                                                 mengd_torrt_veiesys,mengd_vatt_veiesys))
            elif code == '0002':
                print('EOF')
            else:
                print('Unknown code %s' %code)
                print(filename)
                input('Press enter to continue')
            
        conn.commit()
        
for filename in matches:
    print("Arbeider med " + filename)
    parse_dau(filename, conn, cur)