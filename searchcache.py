import json
from datetime import datetime, timedelta
import logging

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.runtime import DeadlineExceededError

from models import *
import models

H = 6

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
                batchsize = batchsize * pagina
		#self.response.out.write(str(batchsize) + " " + str(batchstart))

		if keywords and keywords != '':
			kwlist = []
			keywordslist = keywords.replace('+',' ').replace('.',' ').replace(',',' ').replace(';',' ').replace('\'',' ').replace('"',' ').split(' ')
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
						memcache.add(kw, json.dumps(sdlist), 5400)

						kwresults = sdlist
					else:
						kwresults = json.loads(kwcache)

					resultslist = []
					nbvalidresults = 0

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
							if validresult == True and nbkeywords > 1:
								xtrafound = False
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
									validresult = False

								if gkind == 'Oferta' and tipo:
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
									resultslist.append(kwresult)
						else:
							break
					self.response.out.write(callback + '(' + json.dumps(resultslist) + ')')
			else:
				errordict = {'error': -2, 'message': 'keyword variable present but no valid keyword found: with len(keyword) > 3'}
	                        self.response.out.write(json.dumps(errordict))
		else:
			if gkind == 'Oferta' and estado and estado is not None and estado != '':
				outputlist = []
				if categoria is not None and tipo is not None and tipo != '3':
					#self.response.out.write("1")
					ofertas = json.loads(cacheEstado(estado,cid=categoria,tipo=tipo))
				elif categoria is not None and tipo is None:
					#self.response.out.write("2")
					ofertas = json.loads(cacheEstado(estado,cid=categoria,tipo=None))
				elif categoria is None and tipo is not None and tipo != '3':
					#self.response.out.write("3")
					ofertas = json.loads(cacheEstado(estado,cid=None,tipo=tipo))
				else:
					#self.response.out.write("4")
					ofertas = json.loads(cacheEstado(estado))
				for oferta in ofertas[batchstart:batchstart + batchsize]:
					outputlist.append(oferta)
				#self.response.out.write('[' + str(batchstart) + ':' + str(batchstart + batchsize) + ']')
				self.response.out.write(callback + '(' + json.dumps(outputlist) + ')')
			else:
				sd = SearchData.all()
	                        if gkind:
		                        sd.filter("Kind =", gkind)
	                        if gkind == 'Oferta':
	                                if categoria:
	       		                        sd.filter("IdCat =", int(categoria))
	                                resultslist = []
					truncresultslist = []
        	                        nbvalidresults = 0
					onotfound = 0
	                                for result in sd.order("-FechaHora").run(limit=10000):
	                                        validresult = True
	                                        if nbvalidresults < batchsize:
							try:
								#self.response.out.write('far\n')
	       	                                        	oferta = Oferta.get(result.Sid)
								if oferta.FechaHoraPub > datetime.now():
									#self.response.out.write(str(fechapub) + ' > ' + str(datetime.now()) + '\n')
									validresult = False
								if validresult and estado and estado != '':
									try:
										validresult = False
										oeQ = OfertaEstado.all().filter("IdOft =", oferta.IdOft).filter("IdEnt =", str(estado))
										for oe in oeQ.run(limit=1):
											validresult = True
									except DeadlineExceededError:
										validresult = False
										self.response.clear()
			                                                        self.response.headers['Content-Type'] = 'application/json'
								if validresult == True:
									for oft in resultslist:
										if oft['IdOft'] == oferta.IdOft:
											#self.response.out.write(oferta.IdOft  + ' already in results\n')
											validresult = False
								
								if validresult == True and tipo:
		                                                        try:
		                                                                tipo = int(tipo)
		                                                        except ValueError:
		                                                                tipo = 3
		                                                        if tipo == 1:
		                                                                if oferta.Enlinea != True:
		                                                                     	validresult = False
		                                                        if tipo == 2:
		                                                                if oferta.Enlinea != False:
		                                                                        validresult = False

								#self.response.out.write('almost\n')
								if validresult == True:
									try:
										logourl = ''
										if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
		                                                                        logourl = oferta.Codigo
										elif oferta.BlobKey  and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
			                                              			logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
									except AttributeError:
										logourl = ''
			                                                sddict = {'Key': result.Sid, 'Value': result.Value, 'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': estado, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp}
									if nbvalidresults >= batchstart:
										truncresultslist.append(sddict)
				                                        resultslist.append(sddict)
									nbvalidresults += 1
							except AttributeError:
								onotfound += 1
								pass
							except db.BadValueError:
								#logging.error('Bad Oferta key [' + result.Sid + ']. PASS.')
								onotfound += 1
								pass
							except DeadlineExceededError:
								self.response.clear()
								self.response.headers['Content-Type'] = 'application/json'
								#logging.error('Deadline exceeded. PASS.')
								pass
	                                        else:
	                                        	break
					if onotfound > 0:
						logging.error('Ofertas keys not found or bad Oferta keys: ' + str(onotfound) + '. PASS.')
	                                self.response.out.write(callback + '(' + json.dumps(truncresultslist) + ')')
				"""errordict = {'error': -1, 'message': 'Correct use: /search?keywords=<str>[&kind=<str>&categoria=<int>&estado=<str>&tipo=<int>]'}
	                        self.response.out.write(json.dumps(errordict))"""

def cacheEstado(eid, cid=None,tipo=None):
	logging.info('eid: ' + str(eid) + '. cid: ' + str(cid) + '. tipo: ' + str(tipo) + '.')
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
				ofertadict = {'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': eid, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp, 'Tipo': tipo}
				ofertaslist.append(ofertadict)
			except UnboundLocalError:
				unfoundo += 1
				pass
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
		return json.dumps(outputlist)
