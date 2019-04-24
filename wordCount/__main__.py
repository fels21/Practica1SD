import re
from COSBackend import COSBackend
def main(args):
    login=args.get("login")
    nom_bucket=args.get("bucket")
    text=args.get("text")
    index=args.get("index") 
    cos=COSBackend(login)
                    
    dictionary={}
    text= re.sub(r'[-,;:?!.¿¡\'\(\)\[\]\n\r\t]',"",text) #Eliminamos caracteres especiales con Regular Expression
    
    for word in text.split(' '):    
        word=word.lower()                     #Pasamos a minusculas
        if(word != '\n' and word != '\r' ):
            if word in dictionary: #Si la clave ya este en el diccionario
                dictionary[word]=dictionary[word]+1
            else:                                 #Si no esta en el diccionario
                dictionary[word]=1
    
    cos.put_object(nom_bucket,'wordcount'+str(index)+'.txt', str(dictionary))
    return {'ok':'ok'}