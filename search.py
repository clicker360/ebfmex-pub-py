import json
from datetime import datetime, timedelta
import logging

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import memcache

from models import *
import models

H = 6

class search(webapp.RequestHandler):
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
			keywordslist = keywords.replace('+',' ').replace('.',' ').replace(',',' ').replace(';',' ').split(' ')
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
										if oferta.BlobKey:
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
							elif gkind and gkind == 'Empresa':
								empresa = Empresa.get(sd.Sid)
								logourl = '/eimg?id=' + empresa.IdEmp
								sddict = {'Key': sd.Sid, 'Value': sd.Value, 'IdEmp': empresa.IdEmp, 'Desc': empresa.Desc, 'IdEnt': empresa.DirEnt, 'Logo': logourl}
       	                                                 	sdlist.append(sddict)
							else:
								sddict = {'Sid': sd.Sid, 'Kind': sd.Kind, 'Field': sd.Field, 'Value': sd.Value}
								sdlist.append(sddict)
						memcache.add(kw, json.dumps(sdlist), 3600)

				attempt = 0
				kwcache = memcache.get(kwlist[0])
				while len(kwcache) < 1 and attempt < 100:
					kwcache = memcache.get(kwlist[0])
					attempt += 1

				if kwcache:
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
					errordict = {'error': -2, 'message': 'Deadline of cache writing intents reached. Couldn\'t write cache'}
	                                self.response.out.write(json.dumps(errordict))
			else:
				errordict = {'error': -2, 'message': 'keyword variable present but no valid keyword found: with len(keyword) > 3'}
	                        self.response.out.write(json.dumps(errordict))
		else:
			#self.response.out.write('2')
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
                                for result in sd.order("-FechaHora").run(batch_size=100000):
                                        validresult = True
                                        if nbvalidresults < batchsize:
						try:
							#self.response.out.write('far\n')
       	                                        	oferta = Oferta.get(result.Sid)
							if oferta.FechaHoraPub > datetime.now():
								#self.response.out.write(str(fechapub) + ' > ' + str(datetime.now()) + '\n')
								validresult = False
							if validresult and estado and estado != '':
								validresult = False
								oeQ = OfertaEstado.all().filter("IdOft =", oferta.IdOft).filter("IdEnt =", str(estado))
								for oe in oeQ.run(limit=1):
									validresult = True
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
								if oferta.BlobKey:
	                                              			logourl = '/ofimg?id=' + str(oferta.BlobKey.key())
								else:
									logourl = None
		                                                sddict = {'Key': result.Sid, 'Value': result.Value, 'IdOft': oferta.IdOft, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'IdEnt': estado, 'Logo': logourl, 'Descripcion': oferta.Descripcion}
								if nbvalidresults >= batchstart:
									truncresultslist.append(sddict)
			                                        resultslist.append(sddict)
								nbvalidresults += 1
						except AttributeError:
							onotfound += 1
							pass
                                        else:
                                        	break
				if onotfound > 0:
					logging.error('Ofertas keys not found: ' + str(onotfound) + '.')
                                self.response.out.write(callback + '(' + json.dumps(truncresultslist) + ')')
			"""errordict = {'error': -1, 'message': 'Correct use: /search?keywords=<str>[&kind=<str>&categoria=<int>&estado=<str>&tipo=<int>]'}
                        self.response.out.write(json.dumps(errordict))"""

class searchds(webapp.RequestHandler):
	def get(self):
		gvalue = self.request.get('value')
		gfield = self.request.get('field')
		gkind = self.request.get('kind')
		self.response.headers['Content-Type'] = 'application/json'
		if not gvalue or gvalue == '':
			errordict = {'error': -1, 'message': 'Correct use: /searchds?value=<str>[&kind=<str>&field=<str>]'}
                        self.response.out.write(json.dumps(errordict))
		else:
			search = SearchData.all().filter("Value =", gvalue)
			if gfield and gfield != '':
				search.filter("Field =", gfield)
			if gkind and gkind != '':
				search.filter("Kind =", gkind)
			found = False
			resultlist = []
			try:
				for result in search:
					found = True
					if gkind == 'Oferta':
						#self.response.out.write('gkind: ' + gkind + ' gvalue: ' + gvalue + '\n')
						oferta = Oferta.get_by_id(int(result.Sid))
						ofertadict = {'IdOft': oferta.IdOft, 'IdEmp': oferta.IdEmp, 'IdCat': oferta.IdCat, 'Oferta': oferta.Oferta, 'Descripcion': oferta.Descripcion}
						resultlist.append(ofertadict)
					elif gkind == 'Empresa':
						empresa = Empresa.get_by_id(int(result.Sid))
						empresadict = {'IdEmp': empresa.IdEmp, 'Nombre': empresa.Nombre, 'Desc': Desc, 'OrgEmp': empresa.OrgEmp}
						resultlist.append(empresadict)
					elif gkind == 'Sucursal':
						sucursal = Sucursal.get_by_id(int(result.Sid))
						sucursaldict = {'IdSuc': sucursal.IdSuc, 'Nombre': sucursal.Nombre, 'IdEmp': sucursal.IdEmp, 'Geo1': sucursal.Geo1, 'Geo2': sucursal.Geo2, 'direccion': {'DirCalle': sucursal.DirCalle, 'DirCol': sucursal.DirCol, 'DirCp': sucursal.DirCp, 'DirEnt': sucursal.DirEnt, 'DirMun': sucursal.DirMun}, 'Tel': sucursal.Tel} 
						resultlist.append(sucursaldict)
					else:
						resultdict = {'Sid': result.Sid, 'Value': result.Value, 'Kind': result.Kind, 'Field': result.Field}
						resultlist.append(resultdict)
				if not found:
					errordict = {'error': -2, 'message': 'No result found for search " ' + gvalue + '"'}
		                        self.response.out.write(json.dumps(errordict))
				else:
					self.response.out.write(json.dumps(resultlist))
			except AttributeError:
				errordict = {'error': -1, 'message': 'Datastore inconsistency.'}
				self.response.out.write(json.dumps(errordict))

class generatesearch(webapp.RequestHandler):
	def get(self):
		kindg = self.request.get('kind')
		field = self.request.get('field')
		gid = self.request.get('id')
		gvalue = self.request.get('value')
		#genlinea = self.request.get('enlinea')
		gcat = self.request.get('categoria')
		self.response.headers['Content-Type'] = 'application/json'
		if not kindg or not field or kindg == '' or field == '':
			errordict = {'error': -1, 'message': 'Correct use: /backend/generatesearch?kind=<str>&field=<str>[&id=<int>&value=<str>&enlinea=<int>]'}
			self.response.out.write(json.dumps(errordict))
		elif gid and gid != '' and gvalue and gvalue != '':
			try:
				data = getattr(models, kindg)
				kdata = data.get(str(gid))
				existsQ = SearchData.all().filter("Kind = ", kindg).filter("Sid = ",gid).filter("Field = ",field)
				"""if genlinea:
					existsQ.filter("Enlinea =", genlinea)"""
				for searchdata in existsQ:
					db.delete(searchdata)
				values = gvalue.replace('%20',' ').replace('+',' ').replace('.',' ').replace(',',' ').split(' ')
				"""if genlinea == 'true' or genlinea == 'True' or genlinea == '0':
					genlinea = True
				else:
					genlinea = False"""
				for value in values:
					if len(value) > 3:
						sd = SearchData()
						sd.Sid = gid
						sd.Kind = kindg
						sd.Field = field
						sd.Value = value.lower()
						"""if genlinea:
							sd.Enlinea = genlinea"""
						if gcat:
							sd.IdCat = int(gcat)
						sd.FechaHora = datetime.now() - timedelta(hours = H)
						sd.put()
			except db.BadRequestError:
				errordict = {'error': -2, 'message': 'Inconsistency in kind/ID/value.'}
	                        self.response.out.write(json.dumps(errordict))
			except db.BadKeyError:
				errordict = {'error': -2, 'message': 'Inconsistency in kind/ID/value.'}
                                self.response.out.write(json.dumps(errordict))
		else:
			try:
				split = self.request.get('split')
				if split:
					batchsize = self.request.get('batchsize')
					if not batchsize:
						batchsize = 100
					else:
						batchsize = int(batchsize)
					batchnumber = self.request.get('batchnumber')
					if not batchnumber:
						batchnumber = 0
					else:
						batchnumber = int(batchnumber)
					offset = batchnumber * batchsize
					kindsQ = db.GqlQuery("SELECT * FROM " + kindg + " ORDER BY FechaHora DESC")[offset:offset + batchsize]
				else:
					kindsQ = db.GqlQuery("SELECT * FROM " + kindg)
				for kind in kindsQ:
					#self.response.out.write("1")
					values = getattr(kind, field)
					values = values.replace('\n',' ').replace('\r',' ').replace('.',' ').replace(',',' ').split(' ')
					for value in values:
						if len(value) > 3:
							value = value.lower()
							exists = False
							existsQ = SearchData.all().filter("Kind = ",kindg).filter("Sid = ",str(kind.key())).filter("Field = ",field).filter("Value = ", value)
							existsR = existsQ.run(limit=1)
							for searchdata in existsR:
								exists = True
							if not exists:
								#self.response.out.write("2")
								newsd = SearchData()
								newsd.Sid = str(kind.key())
								newsd.Kind = kindg
								newsd.Field = field
								newsd.Value = value
								newsd.FechaHora = datetime.now() - timedelta(hours = H)
                       				                if kindg == 'Oferta':
									newsd.Enlinea = kind.Enlinea
									newsd.IdCat = kind.IdCat
								newsd.put()
			except db.KindError:
				errordict = {'error': -2, 'message': 'Kind ' + kind + ' couldn\'t be found. Careful it is case sensitive.'}
	                        self.response.out.write(json.dumps(errordict))
			except AttributeError:
				errordict = {'error': -2, 'message': 'Kind ' + kindg+ ' doesn\'t have any attribute ' + field + '. Careful it is case sensitive.'}
                                self.response.out.write(json.dumps(errordict))

class SearchKeyword(webapp.RequestHandler):
	def get(self):
		keyword = self.request.get('keyword')
		results = self.request.get('results')
		page = self.request.get('page')
                self.response.headers['Content-Type'] = 'application/json'
                if not keyword or not results or not page:
                        errordict = {'error': -1,'message': 'Correct use: /search/keyword?keyword=<str>&results=<int>[&page=<int>'}
                        self.response.out.write(json.dumps(errordict))
                else:
                        if not page or page == '':
                                page = 1
                        page = int(page)
                        if page > 0:
                                page -= 1
                                page *= int(results)
                        keyword = keyword.lower()
                        keywordsQ = db.GqlQuery("SELECT * FROM OfertaPalabra WHERE Palabra = :1 ORDER BY FechaHora DESC", keyword)
                        keywords = keywordsQ.fetch(results, offset=page)
                        idofts = []
                        for keyword in keywords:
                                idofts.append(keyword.IdOft)
                        ofertasQ = db.GqlQuery("SELECT * FROM Oferta WHERE IdOft IN :1", idofts)
                        ofertas = ofertasQ.fetch(results)
                        ofertalist = []
                        for oferta in ofertas:
                                #self.response.out.write("1")
                                ofertadict = {}
                                ofertadict['id'] = oferta.IdOft
                                tipo = None
                                if oferta.Descuento == '' or oferta.Descuento == None:
                                        tipo = 1
                                else:
                                        tipo = 2
                                ofertadict['tipo_oferta'] = tipo
                                ofertadict['oferta'] = oferta.Oferta
                                ofertadict['descripcion'] = oferta.Descripcion
                                ofertadict['url_logo'] = 'http://' + APPID + '/ofimg?id=' + oferta.IdOft
                                suclist = []
                                sucursalQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft = :1", oferta.IdOft)
                                sucursales = sucursalQ.run(batch_size=100)
                                for suc in sucursales:
                                        sucdict = {'id': suc.IdSuc, 'lat': suc.Lat, 'long': suc.Lng}
                                        suclist.append(sucdict)
                                ofertadict['sucursales'] = suclist
                                empQ = db.GqlQuery("SELECT * FROM Empresa WHERE IdEmp = :1", oferta.IdEmp)
                                empresas = empQ.fetch(1)
                                emplist = {}
                                for empresa in empresas:
                                        emplist['id'] = empresa.IdEmp
                                        emplist['nombre'] = empresa.Nombre
                                ofertadict['empresa'] = emplist
                                ofertadict['ofertas_relacionadas'] = randOffer(3,oferta.IdEmp)
                                ofertalist.append(ofertadict)
                        self.response.out.write(json.dumps(ofertalist))
