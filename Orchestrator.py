# -*- coding: utf-8 -*-
'''
Created on 21 ���. 2019 �.

@author: Ivan Cherny i Borja Garbayo
'''
import sys, yaml,time,re
from COSBackend import COSBackend
from ibm_cf_connector import CloudFunctions
from math import ceil
from pip._vendor.html5lib.constants import EOF
from botocore.vendored.requests.compat import str


def espera(nom_fitxer,nom_bucket,c_max):
    tst=1
    while(tst):
        arxius=connector_date.list_objects(nom_bucket, nom_fitxer)
        cont=0
        for i in arxius:
            cont+=1       #mirarho
            if(cont >= c_max):
                return 0
    return 0

def generator(cloud_function):
    with open("wordCount.zip", 'rb') as arxiu:
        bytes_f=arxiu.read()
    res=cloud_function.create_action("wordCount", bytes_f, 'blackbox', 'ibmfunctions/action-python-v3.6', is_binary=True, overwrite=True)
    if(res == 1): return 1;
    with open("countWords.zip", 'rb') as arxiu:
        bytes_f=arxiu.read()
    res=cloud_function.create_action("countWords", bytes_f, 'blackbox', 'ibmfunctions/action-python-v3.6', is_binary=True, overwrite=True)
    if(res == 1): return 1;
    with open("reducer.zip", 'rb') as arxiu:
        bytes_f=arxiu.read()
    res=cloud_function.create_action("reducer", bytes_f, 'blackbox', 'ibmfunctions/action-python-v3.6', is_binary=True, overwrite=True)
    if(res == 1): return 1;
    return 0

if __name__ == '__main__':
    nom_bucket="practica1v2"
    if (len(sys.argv) == 3):
        with open('ibm_cloud_config', 'r') as config_file:
            res = yaml.safe_load(config_file)
            
        connector_date = COSBackend(res['ibm_cos'])
        connector_functions=CloudFunctions(res['ibm_cf'])
        #connector_date.delete_object(nom_bucket,"wordcount1.txt")
        #connector_date.delete_object(nom_bucket,"wordcount2.txt")
        resultat=generator(connector_functions)
        if(resultat == 1):
            print("No s'ha pogut generar les funccions,comrova els arxius")
            sys.exit()
        else:
            print("Funcions Generades correctament")
        
        info=connector_date.head_object(nom_bucket, sys.argv[1])
        sizebytes=info['content-length']
        max_mappers=int(sys.argv[2])
        
        '''WordCount'''
        parts=int(sizebytes)/max_mappers #quantes pomes va per cadascu
        parts=ceil(parts)
        parts=int(parts)
        cont_mappers=0
        b_min=0
        b_max=parts
        optim=0
        flag =1
        inici_wordCount=time.time()
        while(cont_mappers<max_mappers and flag):
            bit_cmp=b_max-1            
            dades=connector_date.get_object(nom_bucket, sys.argv[1],extra_get_args={'Range': 'bytes='+str(bit_cmp)+'-'+str(b_max)})
            dades=dades.decode("iso-8859-15")
            control=dades[-1:]
            
            while(control !=' ' and control !='\n' and control !='\r' and control !=EOF and b_max<int(sizebytes)):
                b_max+=1
                dades=connector_date.get_object(nom_bucket, sys.argv[1],extra_get_args={'Range': 'bytes='+str(bit_cmp)+'-'+str(b_max)})
                dades=dades.decode("iso-8859-15")
                control=dades[-1:]
                optim+=1
                
            dades=connector_date.get_object(nom_bucket, sys.argv[1],extra_get_args={'Range': 'bytes='+str(b_min)+'-'+str(b_max)})
            dades=dades.decode("iso-8859-15")
            
            connector_functions.invoke('wordCount', {"login":res['ibm_cos'],"bucket":nom_bucket, "text":dades, "index":str(cont_mappers+1)})
            b_min=b_max+1
            if(b_min>=int(sizebytes)): flag=0
            if(cont_mappers==cont_mappers-1):
                b_max=b_max+parts-optim
            else:
                b_max=b_max+parts

            if(b_max>int(sizebytes)): b_max=int(sizebytes)
            cont_mappers+=1
        print("S'han executat "+str(cont_mappers) + " mappers dels " + str(max_mappers) + " desitjats" )
        print("Els mappers estan treballant")

        v = espera('wordcount',nom_bucket,cont_mappers) 
        fi_wordCount=time.time()
        tempsTotalWordCount=fi_wordCount-inici_wordCount
        
        print("Els mappers han finalitzat anb un temps total de: "+str(tempsTotalWordCount))
        
        
        nom_base=sys.argv[1]
        nom_base=nom_base.split(".")
        inici_reducer=time.time()
        connector_functions.invoke('reducer', {"login":res['ibm_cos'],"bucket":nom_bucket,"nom_arxiu":nom_base[0]})
        print("Executant el reducer")
        v = espera(str(nom_base[0])+'_resultat',nom_bucket,1)        
        fi_reducer=time.time()
        tempsTotalReduce=fi_reducer-inici_reducer
        print("Reducer ha finalitzat "+ str(tempsTotalReduce))


        inici_countW=time.time()       
        connector_functions.invoke('countWords', {"login":res['ibm_cos'],"nom_arxiu":sys.argv[1],"bucket":nom_bucket})
        print("Executant Count")
        v = espera('countwords',nom_bucket,1)
        fi_countW=time.time()
        tempsTotalCountWords=fi_countW-inici_countW
        print("Count finalitzat " + str(tempsTotalCountWords))
        
            
        text=connector_date.get_object(nom_bucket,str(nom_base[0])+ '_resultat.txt')
        file=open(str(nom_base[0])+"resultat.txt","w+")
        file.write("El temps total del Word Count: " +str(tempsTotalWordCount) + '\n')
        file.write("El temps total del Reducer: " +str(tempsTotalReduce)  + '\n')
        text=text.decode("utf-8")
        text= re.sub(r'[-,;?!.¿¡\'\(\)\n\r\t]',"",text)
        file.write(str(text.encode("utf-8")))
        file.close()

        text=connector_date.get_object(nom_bucket,'countwords.txt')
        file=open("coutwords.txt","w+")
        file.write("El temps total del CountWords: " +str(tempsTotalCountWords))
        text=text.decode("iso-8859-15")
        file.write(str(text))
        file.close()
        connector_date.delete_object(nom_bucket,'countwords.txt')
        print(str(text))
        print("Fi del programma")
    else:
        print ("Sintaxis: Orchestrator.py nom_arxiu num_mappers")