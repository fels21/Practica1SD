import ast
from COSBackend import COSBackend
def main(args):
    login=args.get("login")
    nom_bucket=args.get("bucket")
    nom_base=args.get("nom_arxiu")
    
    connector_date=COSBackend(login)
    
    fileList=connector_date.list_objects(nom_bucket,'wordcount')
    dictGeneral=[]
    for i in fileList:
        fileName=i['Key']
        contingut=connector_date.get_object(nom_bucket, fileName)
        contingut_string=contingut.decode("utf-8")
        str_to_dict=ast.literal_eval(contingut_string) #coversio a dicionari
        dictGeneral.append(str_to_dict)
        connector_date.delete_object(nom_bucket,fileName)
               
    dict_final={}
    for dictn in dictGeneral:
        for key in dictn:
            if key not in dict_final:
                dict_final[key]=dictn[key] #afegim
            else:
                dict_final[key]=dict_final[key]+dictn[key]
    text=""
    for x in dict_final:
        text=text +"["+ x+ "]" + ":" + str(dict_final[x])+" "

        
    connector_date.put_object(nom_bucket,str(nom_base)+'_resultat.txt', text)
    
    return {'ok':'ok'}