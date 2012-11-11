import json
from datetime import datetime, timedelta
import logging

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.runtime import DeadlineExceededError
from google.appengine.api import app_identity


from models import *
import models
from sendmail import sendmail

H = 6
APPID = app_identity.get_default_version_hostname()

class searchCache(webapp.RequestHandler):
	def get(self):
		keywords = self.request.get('keywords')
		gkind = self.request.get('kind')
		categoria = self.request.get('categoria')
		estado = self.request.get('estado')
		tipo = self.request.get('tipo')
		batchsize = self.request.get('batchsize')
		pagina = self.request.get('pagina')
		callback = self.request.get('callback')
		self.response.headers['Content-Type'] = 'application/json'
		self.response.headers.add_header("Access-Control-Allow-Origin", "*")
		if pagina:
                	try:
                        	pagina = int(pagina)
                                if pagina < 1:
                                       	pagina = 1
                        except ValueError:
                                pagina = 1
                else:
                        pagina = 1
                if batchsize:
                        try:
                                batchsize = int(batchsize)
                                if batchsize < 1:
                                       batchsize = 12
                        except ValueError:
                                       batchsize = 12
                else:
                        batchsize = 12

                batchstart = batchsize * (pagina - 1)
                batchsize = batchstart + batchsize
		#self.response.out.write(str(batchsize) + " " + str(batchstart))

		if keywords and keywords != '':
			keywords = keywords.lower().replace('buen fin','').replace('buenfin','').replace('oferta','').replace('descuento','').replace('barat','')
		if keywords and keywords != '':
			kwlist = []
			keywordslist = keywords.replace('+',' ').replace('%2B',' ').replace('%2b',' ').replace('%',' ').replace('.',' ').replace(',',' ').replace(';',' ').replace('\'',' ').replace('"',' ').split(' ')
			for kw in keywordslist:
				if len(kw) >= 4:
					#keywordslist.remove(kw)
					kwlist.append(kw.lower())
			nbkeywords = len(kwlist)
			if nbkeywords > 0:
				for kw in kwlist:
					kwcache = memcache.get(kw)
					if kwcache is None:
						#self.response.out.write('Create cache')
						searchdata = SearchData.all().filter("Value =", kw)
						if gkind:
							searchdata.filter("Kind =", gkind)
						searchdata.order("-FechaHora")
						sdlist = []
						for sd in searchdata:
							if gkind and gkind == 'Oferta':
								try:
									oferta = Oferta.get(sd.Sid)
									try:
										estados = OfertaEstado.all().filter("IdOft =", oferta.IdOft)
									except AttributeError:
										estados = []
									hasestado = False
									logourl = ''
									try:
										if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
											logourl = oferta.Codigo
										elif oferta.BlobKey and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
											logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
									except AttributeError:
										logourl = ''
									for estado in estados:
										hasestado = True
										sddict = {'Key': sd.Sid, 'Value': sd.Value, 'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': estado.IdEnt, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'Enlinea': oferta.Enlinea, 'IdEmp': oferta.IdEmp, 'FechaHoraPub': str(oferta.FechaHoraPub)}
										sdlist.append(sddict)
									if not hasestado:
										sddict = {'Key': sd.Sid, 'Value': sd.Value, 'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': None, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'Enlinea': oferta.Enlinea, 'IdEmp': oferta.IdEmp, 'FechaHoraPub': str(oferta.FechaHoraPub)}
		       	                                                       	sdlist.append(sddict)
								except AttributeError:
									pass
								except db.BadValueError:
									pass
							elif gkind and gkind == 'Empresa':
								empresa = Empresa.get(sd.Sid)
								logourl = '/eimg?id=' + empresa.IdEmp
								sddict = {'Key': sd.Sid, 'Value': sd.Value, 'IdEmp': empresa.IdEmp, 'Desc': empresa.Desc, 'IdEnt': empresa.DirEnt, 'Logo': logourl}
       	                                                 	sdlist.append(sddict)
							else:
								sddict = {'Sid': sd.Sid, 'Kind': sd.Kind, 'Field': sd.Field, 'Value': sd.Value}
								sdlist.append(sddict)
						sdlist = sortu(sdlist, 'IdOft')
						memcache.add(kw, json.dumps(sdlist), 5400)

						kwresults = sdlist
					else:
						kwresults = json.loads(kwcache)

				if nbkeywords > 1:
					kwresults = []
					for kw in kwlist:
						try:
							kwresultscache = json.loads(memcache.get(kw))
							for element in kwresultscache:
								kwresults.append(element)
						except TypeError:
							pass

				resultslist = []
				nbvalidresults = 0

				kwresults = sortu(kwresults, 'IdOft')
				for kwresult in kwresults:
					#self.response.out.write('far\n')
					if nbvalidresults < batchsize:
						validresult = True
						if categoria:
							if kwresult['IdCat'] != categoria:
								validresult = False
						if estado:
							if kwresult['IdEnt'] != estado:
								validresult = False
						if gkind == 'Oferta':
							fechapub = datetime.strptime(kwresult['FechaHoraPub'].split('.')[0], '%Y-%m-%d %H:%M:%S')
							if fechapub > datetime.now():
								#self.response.out.write(str(fechapub) + ' > ' + str(datetime.now()) + '\n')
								validresult = False
							else:
                                                                for result in resultslist:
									if result['IdOft'] == kwresult['IdOft']:
										validresult = False	
						"""if validresult == True and nbkeywords > 1:
							xtrafound = True
							for kw in kwlist:
								if kw != kwresult['Value']:
									xtrafound = False
									xtrakw = json.loads(memcache.get(kw))
									for xoft in xtrakw:
										if xoft['Key'] == kwresult['Key']:
											xtrafound = True
									if xtrafound == False:
										break
							if xtrafound == False:
								validresult = False"""

						if validresult and  gkind == 'Oferta' and tipo:
							try:
								tipo = int(tipo)
							except ValueError:
								tipo = 3
							if tipo == 1:
								if kwresult['Enlinea'] != True:
									validresult = False
							if tipo == 2:
								if kwresult['Enlinea'] != False:
									validresult = False
								
						#self.response.out.write('Almost\n')
						if validresult == True:
							nbvalidresults += 1
							if nbvalidresults >= batchstart:
								#logging.info('Adding ' + kwresult['IdOft'])
								resultslist.append(kwresult)
					else:
						break
				self.response.out.write(callback + '(' + json.dumps(sortu(resultslist)) + ')')
			else:
				#errordict = {'error': -2, 'message': 'keyword variable present but no valid keyword found: with len(keyword) > 3'}
	                        #self.response.out.write(json.dumps(errordict))
				self.response.out.write(callback + '(' + json.dumps([]) + ')')
		else:
			if gkind == 'Oferta' and estado and estado is not None and estado != '':
				#self.response.out.write("1")
				outputlist = []
				if tipo == '3':
					tipo = None
				ofertas = json.loads(cacheEstado(estado,cid=categoria,tipo=tipo))
				for oferta in ofertas[batchstart:batchsize]:
					outputlist.append(oferta)
				#self.response.out.write('[' + str(batchstart) + ':' + str(batchstart + batchsize) + ']')
				self.response.out.write(callback + '(' + json.dumps(outputlist) + ')')
			elif gkind == 'Oferta' and categoria is not None and categoria != '':
				#self.response.out.write("2")
				outputlist = []
                                if tipo == '3':
                                        tipo = None
                                ofertas = json.loads(cacheCategoria(categoria,tipo=tipo))
                                for oferta in ofertas[batchstart:batchsize]:
                                        outputlist.append(oferta)
                                #self.response.out.write('[' + str(batchstart) + ':' + str(batchstart + batchsize) + ']')
                                self.response.out.write(callback + '(' + json.dumps(outputlist) + ')')
			elif gkind == 'Oferta':
				#self.response.out.write("3")
				outputlist = []
                                if tipo == '3':
                                        tipo = None
                                ofertas = json.loads(cacheGeneral(tipo=tipo))
                                for oferta in ofertas[batchstart:batchsize]:
                                        outputlist.append(oferta)
				#logging.info('Batch[%s:%s]', batchstart, batchsize)
                                #self.response.out.write('[' + str(batchstart) + ':' + str(batchstart + batchsize) + ']')
                                self.response.out.write(callback + '(' + json.dumps(outputlist) + ')')
			else:
				#self.response.out.write("4")
				logging.error('Kind ' + gkind + ' not sopported in Search.')

def cacheEstado(eid, cid=None,tipo=None):
	#logging.info('eid: ' + str(eid) + '. cid: ' + str(cid) + '. tipo: ' + str(tipo) + '.')
	ocache = memcache.get('cacheEstado' + str(eid))
	if ocache is None:
		ofertaslist = []
		ofertasE = OfertaEstado.all().filter("IdEnt =", str(eid))
		unfoundo = 0
		for ofertaE in ofertasE.run():
			idoft = ofertaE.IdOft
			ofts = Oferta.all().filter("IdOft =", ofertaE.IdOft)
			#logging.info('IdOft: ' + idoft)
			try:
				for oft in ofts:
					oferta = oft
				try:
		                        logourl = ''
	                                if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
		                                logourl = oferta.Codigo
	                                elif oferta.BlobKey  and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
	                                        logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
				except AttributeError:
	                                logourl = ''
				if oferta.Enlinea == True:
					tipo = 1
				else:
					tipo = 2
				if oferta.FechaHoraPub <= datetime.now() and oferta.Oferta != 'Nueva oferta':
					ofertadict = {'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': eid, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp, 'Tipo': tipo, 'fechapub': str(oferta.FechaHoraPub)}
					ofertaslist.append(ofertadict)
			except UnboundLocalError:
				unfoundo += 1
				pass
			except TypeError, e:
	                        logging.error(str(e))
	                        if str(e) == 'Must provide Entity or BlobKey':
        	                        receipient = 'thomas@clicker360.com,ahuezo@clicker360.com'
	                                subject = 'BlobKey error in ' + APPID
	                                body = 'BlobKey error in ' + APPID
	                                errmail = sendmail(receipient, subject, body)
	                                errmail.send()
				pass
		ofertaslist = sortu(ofertaslist)
		memcache.add('cacheEstado' + str(eid), json.dumps(ofertaslist), 3600)
		if unfoundo > 0:
			logging.error('Inconsistency OfertaEstado/Oferta found: ' + str(unfoundo))
	else:
		ofertaslist = json.loads(ocache)
	if cid is None and tipo is None:
		return json.dumps(ofertaslist)
	else:
		outputlist = []
		for oferta in ofertaslist:
			cvalid = True
			tvalid = True
			if cid and cid is not None and cid != '':
				try:
					cid = int(cid)
					if oferta['IdCat'] != cid:
						cvalid = False
				except ValueError:
					cvalid = False
			if tipo and tipo is not None and tipo != '':
				try:
					tipo = int(tipo)
					if oferta['Tipo'] != tipo:
						tvalid = False
				except ValueError:
					tvalid = False
			if cvalid and tvalid:
				outputlist.append(oferta)
		return json.dumps(sortu(outputlist))

def cacheCategoria(cid,tipo=None):
        #logging.info('cid: ' + str(cid) + '. tipo: ' + str(tipo) + '.')
        ocache = memcache.get('cacheCategoria' + str(cid))
        if ocache is None:
                ofertaslist = []
		try:
	                ofertas = Oferta.all().filter("IdCat =", int(cid)).order("-FechaHora").run()
		except db.BadValueError:
			ofertas = db.GqlQuery("SELECT IdOft, IdCat, Oferta, Descripcion, IdEmp, Codigo, Enlinea FROM Oferta")
                unfoundo = 0
		try:
	                for oferta in ofertas:
	                       	logourl = ''
				try:
	                                if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
	                                	logourl = oferta.Codigo
	                                elif oferta.BlobKey  and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
	                                	logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
	                        except AttributeError:
	                                err = 'logourl'
				elist = []
	                        try:
	                                ofertasE = OfertaEstado.all().filter("IdOft =", oferta.IdOft)
	                                for ofertaE in ofertasE.run(limit=1):
	                                        elist.append(ofertaE.IdEnt)
	                        except AttributeError:
	                                elist = []
	                        if len(elist) == 0:
	                                elist.append('')
	                        if oferta.Enlinea == True:
	                                tipo = 1
	                        else:
	                                tipo = 2
				for eid in elist:
					if oferta.FechaHoraPub <= datetime.now() and oferta.Oferta != 'Nueva oferta':
			                        ofertadict = {'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': eid, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp, 'Tipo': tipo, 'fechapub': str(oferta.FechaHoraPub)}
			                        ofertaslist.append(ofertadict)
			ofertaslist = sortu(ofertaslist)
	                memcache.add('cacheCategoria' + str(cid), json.dumps(ofertaslist), 3600)
		except TypeError, e:
                        logging.error(str(e))
                        if str(e) == 'Must provide Entity or BlobKey':
                                receipient = 'thomas@clicker360.com,ahuezo@clicker360.com'
                                subject = 'BlobKey error'
                                body = 'BlobKey error'
                                errmail = sendmail(receipient, subject, body)
                                errmail.send()
        else:
                ofertaslist = json.loads(ocache)
        if tipo is None:
                return json.dumps(ofertaslist)
	else:
                outputlist = []
                for oferta in ofertaslist:
                        tvalid = True
                        if tipo and tipo is not None and tipo != '':
                                try:
                                        tipo = int(tipo)
                                        if oferta['Tipo'] != tipo:
                                                tvalid = False
                                except ValueError:
                                        tvalid = False
                        if tvalid:
                                outputlist.append(oferta)
                return json.dumps(sortu(outputlist))

def cacheGeneral(tipo=None):
        #logging.info('tipo: ' + str(tipo) + '.')
        ocache = memcache.get('cacheGeneral')
        if ocache is None:
                ofertaslist = []
		try:
	                ofertas = Oferta.all().order("-FechaHora").run(limit=1500)
		except db.BadValueError:
			ofertas = db.GqlQuery("SELECT IdOft, IdCat, Oferta, Descripcion, IdEmp, Codigo, Enlinea FROM Oferta")
                unfoundo = 0
		try:
	                for oferta in ofertas:
	                        try:
	                                logourl = ''
	                                if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
	                                        logourl = oferta.Codigo
	                                elif oferta.BlobKey  and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
	                                        logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
	                        except AttributeError:
	                                logourl = ''
				elist = []
				try:
					ofertasE = OfertaEstado.all().filter("IdOft =", oferta.IdOft).run(limit=1)
					for ofertaE in ofertasE:
						elist.append(ofertaE.IdEnt)
				except AttributeError:
					elist = []
				if len(elist) == 0:
					elist.append('')
	                        if oferta.Enlinea == True:
	                                tipo = 1
	                        else:
	                                tipo = 2
				for eid in elist:
					if oferta.FechaHoraPub <= datetime.now() and oferta.Oferta != 'Nueva oferta':
			                        ofertadict = {'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': eid, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp, 'Tipo': tipo, 'fechapub': str(oferta.FechaHoraPub)}
			                        ofertaslist.append(ofertadict)
			ofertaslist = sortu(ofertaslist, 'IdOft')
	                memcache.add('cacheGeneral', json.dumps(ofertaslist), 1800)
		except TypeError, e:
			logging.error(str(e))
			if str(e) == 'Must provide Entity or BlobKey':
				receipient = 'thomas@clicker360.com,ahuezo@clicker360.com'
				subject = 'BlobKey error'
				body = 'BlobKey error'
				errmail = sendmail(receipient, subject, body)
				errmail.send()
        else:
                ofertaslist = json.loads(ocache)
        if tipo is None:
                return json.dumps(ofertaslist)
        else:
                outputlist = []
                for oferta in ofertaslist:
                        tvalid = True
                        if tipo and tipo is not None and tipo != '':
                                try:
                                        tipo = int(tipo)
                                        if oferta['Tipo'] != tipo:
                                                tvalid = False
                                except ValueError:
                                        tvalid = False
                        if tvalid:
                                outputlist.append(oferta)
                return json.dumps(sortu(outputlist))

def sortu(alist, uelement=None):
	inputnb = len(alist)
	masterlist = []
	index = 0
	for element in alist:
		present = False
		for mastere in masterlist:
			if uelement is not None:
				if element[uelement] == mastere:
					present = True
			else:
				if element == mastere:
					present = True
		if present:
			alist.pop(index)
		else:
			if uelement is not None:
				masterlist.append(element[uelement])
			else:
				masterlist.append(element)
		index += 1
	if inputnb != len(alist):
		logging.info('Sorting list. Input: ' + str(inputnb) + '. Output: ' + str(len(alist)) + ' elements.')
	return alist
