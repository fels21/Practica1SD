from COSBackend import COSBackend
import re
def main(args):
	login=args.get("login")
	nom_arxiu=args.get("nom_arxiu")
	nom_bucket=args.get("bucket")
	connector_date=COSBackend(login)
	
	counter = 0
	dades=connector_date.get_object(nom_bucket, nom_arxiu)
	dades=dades.decode("utf-8")
	dades= re.sub(r'[-,;:?!.¿¡\'\(\)\[\]\n\r\t]',"",dades)
	
	for word in dades.split(' '):
		if(word !='\n'):
			print (word)
			counter+=1
			
	string = "Total paraules " + str(counter)
	connector_date.put_object(nom_bucket,'countwords.txt',str(string))
	
	return {'ok':'ok'}