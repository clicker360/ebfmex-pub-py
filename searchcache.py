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
LIMIT = 1000

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
			keywords = keywords.lower().replace('buen fin','').replace('buenfin','').replace('oferta','').replace('descuento','').replace('barat','').replace('+para+',' ').replace('%2Bpara%2B',' ').replace('%2bpara%2b',' ')
		if len(keywords.replace(' ','')) == 0:
			keywords = ''
		if keywords and keywords != '':
			kwlist = []
			keywordslist = keywords.replace('+',' ').replace('%2B',' ').replace('%2b',' ').replace('%',' ').replace('.',' ').replace(',',' ').replace(';',' ').replace('\'',' ').replace('"',' ').split(' ')
			for kw in keywordslist:
				if len(kw) >= 4 and kw != 'para':
					kwlist.append(kw.lower())
			nbkeywords = len(kwlist)
			if nbkeywords > 0:
				for kw in kwlist:
					init = kw[0:3]
					kwcache = memcache.get(init)
					if kwcache is None:
						logging.error('No se encontro o no se pudo cargar cache ' + init + '. Busqueda en DataStore y creacion de cache.')
						#self.response.out.write('Create cache')
						searchdata = SearchData.all().filter("Kind =", 'Oferta').filter("Value >=", init).filter("Value <", init + u"\ufffd")
						#searchdata.order("-FechaHora")
						sdlist = []
						for sd in searchdata.run(limit=LIMIT):
							try:
								oferta = Oferta.get(sd.Sid)
								if oferta.FechaHoraPub <= datetime.now() - timedelta(hours = H):
									try:
										estados = OfertaEstado.all().filter("IdOft =", oferta.IdOft)
									except AttributeError:
										estados = []
									hasestado = False
									logourl = ''
									promocion = 'http://www.elbuenfin.org/imgs/imageDefault.png'
									if oferta.Promocion is not None and oferta.Promocion != '':
										promocion = oferta.Promocion
									try:
										if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
											logourl = oferta.Codigo
										elif oferta.BlobKey and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
											logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
									except AttributeError:
										logourl = ''
									for estado in estados:
										hasestado = True
										sddict = {'Key': sd.Sid, 'Value': sd.Value, 'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': estado.IdEnt, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'Enlinea': oferta.Enlinea, 'IdEmp': oferta.IdEmp, 'FechaHoraPub': str(oferta.FechaHoraPub), 'EmpLogo': promocion, 'Empresa': oferta.Empresa, 'url': oferta.Url}
										sdlist.append(sddict)
									if not hasestado:
										sddict = {'Key': sd.Sid, 'Value': sd.Value, 'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': None, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'Enlinea': oferta.Enlinea, 'IdEmp': oferta.IdEmp, 'FechaHoraPub': str(oferta.FechaHoraPub), 'EmpLogo': promocion, 'Empresa': oferta.Empresa, 'url': oferta.Url}
		       	                                                       	sdlist.append(sddict)
								else:
									pass
							except AttributeError:
								pass
							except db.BadValueError:
								pass
							except db.BadKeyError:
								logging.error('Key error: ' + sd.Sid)
								pass
						sdlist = sortu(sdlist, 'IdOft')
						try:
							memcache.add(kw[0:3], json.dumps(sdlist), 5400)
						except ValueError:
							lim = LIMIT - 100
							logging.error('Couldn\'t create cache ' + kw[0:3] + ' of sdlist. Down to sdlist[0:' + str(lim) + ']')
							try:
								memcache.add(kw[0:3], json.dumps(sdlist[0:lim]), 5400)
							except ValueError:
								lim = LIMIT - 200
								logging.error('Couldn\'t create cache ' + kw[0:3] + ' of sdlist. Down to sdlist[0:' + str(lim) + ']')
								try:
									memcache.add(kw[0:3], json.dumps(sdlist[0:lim]), 5400)
								except ValueError:
									lim = LIMIT - 300
									logging.error('Couldn\'t create cache ' + kw[0:3] + ' of sdlist. Down to sdlist[0:' + str(lim) + ']')
									try:
										memcache.add(kw[0:3], json.dumps(sdlist[0:lim]), 5400)
									except ValueError:
										lim = LIMIT - 400
										logging.error('Couldn\'t create cache ' + kw[0:3] + ' of sdlist. Down to sdlist[0:' + str(lim) + ']')
										try:
											memcache.add(kw[0:3], json.dumps(sdlist[0:lim]), 5400)
										except ValueError:
											logging.error('Couldn\'t create cache ' + kw[0:3] + ' of sdlist[0:' + str(lim) + ']. EXIT')

						kwresults = sdlist
					else:
						kwresults = json.loads(kwcache)

				if nbkeywords > 1:
					kwresults = []
					for kw in kwlist:
						try:
							kwresultscache = json.loads(memcache.get(kw[0:3]))
							for element in kwresultscache:
								kwresults.append(element)
						except TypeError:
							pass

				kwclean = []
				for kw in kwresults:
					for kw2 in kwlist:
						if kw['Value'] == kw2:
							kwclean.append(kw)

				resultslist = []
				nbvalidresults = 0

				kwclean = sortu(kwclean, 'IdOft')
				for kwresult in kwclean:
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
							if fechapub > datetime.now() - timedelta(hours = H):
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
				if callback and callback is not None and callback != '':
					self.response.out.write(callback + '(' + json.dumps(sortu(resultslist)) + ')')
				else:
					self.response.out.write(json.dumps(sortu(resultslist)))
			else:
				#errordict = {'error': -2, 'message': 'keyword variable present but no valid keyword found: with len(keyword) > 3'}
	                        #self.response.out.write(json.dumps(errordict))
				if callback and callback is not None and callback != '':
					self.response.out.write(callback + '(' + json.dumps([]) + ')')
				else:
					self.response.out.write(json.dumps([]))
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
				if callback and callback is not None and callback != '':
					self.response.out.write(callback + '(' + json.dumps(outputlist) + ')')
				else:
					self.response.out.write(json.dumps(outputlist))
			elif gkind == 'Oferta' and categoria is not None and categoria != '':
				#self.response.out.write("2")
				outputlist = []
                                if tipo == '3':
                                        tipo = None
                                ofertas = json.loads(cacheCategoria(categoria,tipo=tipo))
                                for oferta in ofertas[batchstart:batchsize]:
                                        outputlist.append(oferta)
                                #self.response.out.write('[' + str(batchstart) + ':' + str(batchstart + batchsize) + ']')
				if callback and callback is not None and callback != '':
	                                self.response.out.write(callback + '(' + json.dumps(outputlist) + ')')
				else:
					self.response.out.write(json.dumps(outputlist))
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
				if callback and callback is not None and callback != '':
	                                self.response.out.write(callback + '(' + json.dumps(outputlist) + ')')
				else:
					self.response.out.write(json.dumps(outputlist))
			else:
				#self.response.out.write("4")
				logging.error('Kind ' + gkind + ' not sopported in Search.')

def cacheEstado(eid, cid=None,tipo=None):
	#logging.info('eid: ' + str(eid) + '. cid: ' + str(cid) + '. tipo: ' + str(tipo) + '.')
	ocache = memcache.get('cacheEstado' + str(eid))
	if ocache is None:
		logging.error('No se encontro o no se pudo cargar cache cacheEstado' + str(eid) + '. Busqueda en DataStore y creacion de cache.')
		ofertaslist = []
		ofertasE = OfertaEstado.all().filter("IdEnt =", str(eid))
		unfoundo = 0
		for ofertaE in ofertasE.run(limit=800):
			idoft = ofertaE.IdOft
			ofts = Oferta.all().filter("IdOft =", ofertaE.IdOft)
			#logging.info('IdOft: ' + idoft)
			try:
				for oft in ofts.run(limit=1):
					oferta = oft
				try:
		                        logourl = ''
	                                if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
		                                logourl = oferta.Codigo
	                                elif oferta.BlobKey  and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
	                                        logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
				except AttributeError:
					logourl = ''
	                        promocion = 'http://www.elbuenfin.org/imgs/imageDefault.png'
                                if oferta.Promocion is not None and oferta.Promocion != '':
                                        promocion = oferta.Promocion
				if oferta.Enlinea == True:
					tipo = 1
				else:
					tipo = 2
				if oferta.FechaHoraPub <= datetime.now() - timedelta(hours = H) and oferta.Oferta != 'Nueva oferta':
					ofertadict = {'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': eid, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp, 'Tipo': tipo, 'fechapub': str(oferta.FechaHoraPub), 'EmpLogo': promocion, 'Empresa': oferta.Empresa, 'url': oferta.Url}
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
		memcache.add('cacheEstado' + str(eid), json.dumps(ofertaslist), 7200)
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
		logging.error('No se encontro o no se pudo cargar cache cacheCategoria' + str(cid) + '. Busqueda en DataStore y creacion de cache.')
                ofertaslist = []
		try:
	                ofertas = Oferta.all().filter("IdCat =", int(cid)).order("-FechaHora").run(limit=800)
		except db.BadValueError:
			ofertas = db.GqlQuery("SELECT IdOft, IdCat, Oferta, Descripcion, IdEmp, Codigo, Enlinea FROM Oferta")
                unfoundo = 0
		try:
	                for oferta in ofertas:
	                       	promocion = 'http://www.elbuenfin.org/imgs/imageDefault.png'
				logourl = ''
				try:
	                                if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
	                                	logourl = oferta.Codigo
	                                elif oferta.BlobKey  and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
	                                	logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
	                        except AttributeError:
	                                logourl = ''
                                if oferta.Promocion is not None and oferta.Promocion != '':
                                        promocion = oferta.Promocion
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
					if oferta.FechaHoraPub <= datetime.now() - timedelta(hours = H) and oferta.Oferta != 'Nueva oferta':
			                        ofertadict = {'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': eid, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp, 'Tipo': tipo, 'fechapub': str(oferta.FechaHoraPub), 'EmpLogo': promocion, 'Empresa': oferta.Empresa, 'url': oferta.Url}
			                        ofertaslist.append(ofertadict)
			ofertaslist = sortu(ofertaslist)
	                memcache.add('cacheCategoria' + str(cid), json.dumps(ofertaslist), 7200)
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
		logging.error('No se encontro o no se pudo cargar cache cacheGeneral. Busqueda en DataStore y creacion de cache.')
                ofertaslist = []
		try:
	                ofertas = Oferta.all().order("-FechaHora").run(limit=800)
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
	                        promocion = 'http://www.elbuenfin.org/imgs/imageDefault.png'
                                if oferta.Promocion is not None and oferta.Promocion != '':
	                                promocion = oferta.Promocion
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
					if oferta.FechaHoraPub <= datetime.now() - timedelta(hours = H) and oferta.Oferta != 'Nueva oferta':
			                        ofertadict = {'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': eid, 'Logo': logourl, 'Descripcion': oferta.Descripcion, 'IdEmp': oferta.IdEmp, 'Tipo': tipo, 'fechapub': str(oferta.FechaHoraPub), 'EmpLogo': promocion, 'Empresa': oferta.Empresa, 'url': oferta.Url}
			                        ofertaslist.append(ofertadict)
			ofertaslist = sortu(ofertaslist, 'IdOft')
	                memcache.add('cacheGeneral', json.dumps(ofertaslist), 7200)
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
