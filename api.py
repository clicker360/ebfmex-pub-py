import math, json, random, urllib, urllib2

import logging

from datetime import datetime, timedelta, date

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.api import urlfetch

from models import Sucursal, OfertaSucursal, Oferta, OfertaPalabra, Entidad, Municipio, Empresa, ChangeControl, Categoria, MvBlob, EmpresaNm
from randString import randLetter, randString

#APPID = app_identity.get_default_version_hostname()
#APPID = app_identity.get_application_id()
APPID = 'www.elbuenfin.org'

def randOffer(nb,empresa=None):
	numoffer = 0
	deadline = 50
	randoffer = memcache.get('randoffer')
	if randoffer is None:
		offerlist = []
		while len(offerlist) < 250 and deadline > 0:
			letter = randLetter()
			ofertas = OfertaSucursal.all().filter("IdOft >=", letter).filter("IdOft <", letter + u"\ufffd")
			if ofertas:
				for oferta in ofertas:
					exists = False
					for oft in offerlist:
						if oferta.IdOft == oft['id']:
							exists = True
					if exists == False:
						offerdict = {'id': oferta.IdOft, 'oferta': oferta.Oferta, 'lat': oferta.Lat, 'long': oferta.Lng, 'url_logo': 'http://' + APPID + '/ofimg?id=' + oferta.IdOft, 'IdEmp': oferta.IdEmp}
						offerlist.append(offerdict)
			deadline -= 1
		memcache.add('randoffer', offerlist, 1800)

	for i in range(10):
		randoffer = memcache.get('randoffer')
		if randoffer is not None:
			break
	if randoffer is None:
		return []
	else:
		returnlist = []
		for oferta in randoffer:
			if numoffer < nb:
				valid = True
				if empresa and empresa is not None:
					if oferta['IdEmp'] != empresa:
						valid = False
				if valid:
					returnlist.append(oferta)
					numoffer += 1
			else:
				break
		return returnlist

class MvBlobServePub(webapp.RequestHandler):
        def get(self):
                self.response.headers['Content-Type'] = 'text/plain'
                timestamp = self.request.get('timestamp')
                horas = self.request.get('horas')
                if not timestamp or not horas or timestamp == None or horas == None or timestamp == '' or horas == '':
                        errordict = {'error': -1, 'message': 'Must specify variables in GET method (i.e. /db?timestamp=<YYYYMMDDHH24>&horas=<int>)'}
                        self.response.out.write(json.dumps(errordict))
                elif len(timestamp) != 10:
                        errordict = {'error': -2, 'message': 'timestamp must be 10 chars long: YYYYMMDDHH24'}
                        self.response.out.write(json.dumps(errordict))
                else:
                        try:
                                fechastr = timestamp[0:8]
                                timestamp = datetime.strptime(timestamp,'%Y%m%d%H')
                                timestampdia = datetime.strptime(fechastr, '%Y%m%d')
                                horas = int(horas)
                                timestampend = timestamp + timedelta(hours = horas)
                        except ValueError:
                                errordict = {'error': -2, 'message': 'Value Error. Timestamp must be YYYYMMDDHH24 and horas is an integer'}
                                self.response.out.write(json.dumps(errordict))
                        if horas > 24:
                                errordict = {'error': -2, 'message': 'Horas must be <= 24'}
                                self.response.out.write(json.dumps(errordict))
                        else:
                                mvblobs = MvBlob.all().filter("FechaHora >=", timestamp).filter("FechaHora <=", timestampend)
                                outputlist = []
                                for mvblob in mvblobs.run():
					blob = json.loads(mvblob.Blob)
					ofertaslist = []
					oidlist = []
					for oferta in blob['ofertas']:
						alreadyadded = False
						for oid in oidlist:
							if oferta['id'] == oid:
								alreadyadded = True
						if not alreadyadded:
							fechapub = datetime.strptime(oferta['fechapub'], '%Y-%m-%d')
							if fechapub <= timestamp:
								ofertaslist.append(oferta)
								oidlist.append(oferta['id'])
					blob['ofertas'] = ofertaslist
					outputlist.append(blob)
				self.response.out.write(json.dumps(outputlist))

class MvBlobServe(webapp.RequestHandler):
	def get(self):
		self.response.headers['Content-Type'] = 'text/plain'
		timestamp = self.request.get('timestamp')
                horas = self.request.get('horas')
                if not timestamp or not horas or timestamp == None or horas == None or timestamp == '' or horas == '':
                        errordict = {'error': -1, 'message': 'Must specify variables in GET method (i.e. /db?timestamp=<YYYYMMDDHH24>&horas=<int>)'}
                        self.response.out.write(json.dumps(errordict))
                elif len(timestamp) != 10:
                        errordict = {'error': -2, 'message': 'timestamp must be 10 chars long: YYYYMMDDHH24'}
                        self.response.out.write(json.dumps(errordict))
                else:
                        try:
                                fechastr = timestamp[0:8]
                                timestamp = datetime.strptime(timestamp,'%Y%m%d%H')
                                timestampdia = datetime.strptime(fechastr, '%Y%m%d')
                                horas = int(horas)
                                timestampend = timestamp + timedelta(hours = horas)
                        except ValueError:
                                errordict = {'error': -2, 'message': 'Value Error. Timestamp must be YYYYMMDDHH24 and horas is an integer'}
                                self.response.out.write(json.dumps(errordict))
                        if horas > 24:
                                errordict = {'error': -2, 'message': 'Horas must be <= 24'}
                                self.response.out.write(json.dumps(errordict))
                        else:
				mvblobs = MvBlob.all().filter("FechaHora >=", timestamp).filter("FechaHora <=", timestampend)
				outputlist = []
				for mvblob in mvblobs.run():
					outputlist.append(json.loads(mvblob.Blob))
				self.response.out.write(json.dumps(outputlist))

class MvBlobGenTask(webapp.RequestHandler):
	def get(self):
		#count = Sucursal.all().count()
		count = memcache.get('MvBlobCount')
		if count is None:
			count = 0
			suc = Sucursal.all()
			for b in MvBlob.all().order ("-FechaHora").run(limit=1):
	                        suc.filter("FechaHora >", b.FechaHora)
			for s in suc.run(batch_size=30000):
				count += 1
			memcache.add('MvBlobCount', count, 3600)
		else:
			count = int(count)
		logging.info('MvBlob: loading ' + str(count) + ' Sucursales.')
		batchsize = 50
		batchnumber = 0

		while count > 0:
			logging.info('Batch ' + str(batchnumber))
			taskqueue.add(url='/mvblob/generate/run', params={'batchsize': batchsize, 'batchnumber': batchnumber})
			batchnumber += 1
			count -= batchsize

class MvBlobGen(webapp.RequestHandler):
	def post(self):
		self.response.headers['Content-Type'] = 'text/plain'
		try:
			batchsize = int(self.request.get('batchsize'))
			batchnumber = int(self.request.get('batchnumber'))
			if batchsize is None:
				batchsize = 10
			if batchnumber is None:
				batchnumber = 0
		except ValueError:
			batchsize = 10
			batchnumber = 0

		offset = batchnumber * batchsize
		sucs = Sucursal.all()
		for b in MvBlob.all().order ("-FechaHora").run(limit=1):
			sucs.filter("FechaHora >", b.FechaHora)
		sucs.order("FechaHora")#[offset:offset + batchsize]
		logging.info('MvBlob generation, batchsize: ' + str(batchsize) + ',batchnumber: ' + str(batchnumber) + '. [' + str(offset) + ':' + str(offset + batchsize) + ']')
		for suc in sucs.run(offset=offset, limit=batchsize):
			HasOferta = False
			olist = []
			OSs = OfertaSucursal.all().filter("IdSuc =", suc.IdSuc)
			for OS in OSs:
				HasOferta = True
				olist.append(OS.IdOft)
			if HasOferta:
				als = MvBlob.all().filter("IdSuc =", suc.IdSuc)
				for al in als:
					db.delete(al)
				sucdict = {'id': suc.IdSuc, 'nombre': suc.Nombre, 'lat': suc.Geo1, 'long': suc.Geo2, 'fechamod': str(suc.FechaHora)}
				ent = None
	                        entidades = Entidad.all().filter("CveEnt =", suc.DirEnt)
	                        for entidad in entidades:
		                        ent = entidad.Entidad
	                        mun = None
	                        municipios = Municipio.all().filter("CveEnt =", suc.DirEnt).filter("CveMun =", suc.DirMun)
	                        for municipio in municipios:
		                        mun = municipio.Municipio
				sucdict['direccion'] = {'calle': suc.DirCalle, 'colonia': suc.DirCol, 'cp': suc.DirCp,'entidad_id': suc.DirEnt, 'entidad': ent,'municipio_id': suc.DirMun, 'municipio': mun}
				empresas = Empresa.all().filter("IdEmp = ", suc.IdEmp)
	                        for empresa in empresas.run(limit=1):
			                empresadict = {'id': empresa.IdEmp, 'nombre': empresa.Nombre, 'url': empresa.Url, 'url_logo': ''}
		                        sucdict['empresa'] = empresadict
				urllogo = 'http://www.elbuenfin.org/imgs/imageDefault.png'
				ofertaslist = []
				for o in olist:
					ofertas = Oferta.all().filter("IdOft =", o).run()
					for oferta in ofertas:
						url = 'http://www.elbuenfin.org/imgs/imageDefault.png'
		                                try:
		                                        if oferta.Codigo and oferta.Codigo.replace('https://','http://')[0:7] == 'http://':
		                                                url = oferta.Codigo
						except AttributeError:
	                                                err = 'logourl'
						try:
		                                        if oferta.Codigo is None and oferta.BlobKey  and oferta.BlobKey != None and oferta.BlobKey.key() != 'none':
								url = 'http://' + APPID + '/ofimg?id=' + str(oferta.BlobKey.key())
		                                except AttributeError:
		                                        err = 'logourl'
						if url == 'http://www.elbuenfin.org/imgs/imageDefault.png' and oferta.Promocion is not None and oferta.Promocion != '':
							url = oferta.Promocion
						if oferta.Promocion is not None and oferta.Promocion != '':
							urllogo = oferta.Promocion
						ofertadict = {'id': oferta.IdOft, 'oferta': oferta.Oferta, 'descripcion': oferta.Descripcion, 'descuento': oferta.Descuento, 'promocion': oferta.Promocion, 'enlinea': oferta.Enlinea, 'precio': oferta.Precio, 'url': oferta.Url, 'url_logo': url, 'fechapub': str(oferta.FechaHoraPub.strftime('%Y-%m-%d'))}
	                                        palabraslist = []
	                                        palabras = OfertaPalabra.all().filter("IdOft =", oferta.IdOft)
	                                        for palabra in palabras:
		                                        palabraslist.append(palabra.Palabra)
	                                        ofertadict['palabras'] = palabraslist
	                                        cat = None
	                                        categorias = Categoria.all().filter("IdCat =", oferta.IdCat)
	                                        for categoria in categorias:
	                                        	cat = categoria.Categoria
	                                        ofertadict['categoria_id'] = oferta.IdCat
	                                        ofertadict['categoria'] = cat

						ofertaslist.append(ofertadict)
				sucdict['ofertas'] = ofertaslist
				sucdict['empresa']['url_logo'] = urllogo
				mvblob = MvBlob()
				mvblob.FechaHora = suc.FechaHora
				mvblob.IdSuc = suc.IdSuc
				mvblob.Blob = json.dumps(sucdict)
				mvblob.put()
		else:
			pass
		#self.response.out.write(json.dumps(sucdict) + '\n')

class sucursales(webapp.RequestHandler):
	def get(self):
		timestamp = self.request.get('timestamp')
		horas = self.request.get('horas')
		self.response.headers['Content-Type'] = 'text/plain'
		if not timestamp or not horas or timestamp == None or horas == None or timestamp == '' or horas == '':
			errordict = {'error': -1, 'message': 'Must specify variables in GET method (i.e. /db?timestamp=<YYYYMMDDHH24>&horas=<int>)'}
			self.response.out.write(json.dumps(errordict))
		elif len(timestamp) != 10:
			errordict = {'error': -2, 'message': 'timestamp must be 10 chars long: YYYYMMDDHH24'}
                        self.response.out.write(json.dumps(errordict))
		else:
			try:
				fechastr = timestamp[0:8]
				timestamp = datetime.strptime(timestamp,'%Y%m%d%H')
				timestampdia = datetime.strptime(fechastr, '%Y%m%d')
				horas = int(horas)
				timestampend = timestamp + timedelta(hours = horas)
			except ValueError:
				errordict = {'error': -2, 'message': 'Value Error. Timestamp must be YYYYMMDDHH24 and horas is an integer'}
				self.response.out.write(json.dumps(errordict))
			if horas > 24:
				errordict = {'error': -2, 'message': 'Horas must be <= 24'}
				self.response.out.write(json.dumps(errordict))
			else:
				self.response.headers['Content-Type'] = 'text/plain'

				suclist = memcache.get('wssucursales-' + fechastr)
				if suclist is None:
					outputlist = []
					sucursales = Sucursal.all().filter("FechaHora >=", timestampdia).filter("FechaHora <", timestampdia + timedelta(days = 1))
					for sucursal in sucursales:
						sucdict = {}
						sucdict['id'] = sucursal.IdSuc
						sucdict['nombre'] = sucursal.Nombre
						ent = None
						entidades = Entidad.all().filter("CveEnt =", sucursal.DirEnt)
						for entidad in entidades:
							ent = entidad.Entidad
						mun = None
						municipios = Municipio.all().filter("CveEnt =", sucursal.DirEnt).filter("CveMun =", sucursal.DirMun)
						for municipio in municipios:
							mun = municipio.Municipio
						sucdict['direccion'] = {'calle': sucursal.DirCalle, 'colonia': sucursal.DirCol, 'cp': sucursal.DirCp,'entidad_id': sucursal.DirEnt, 'entidad': ent,'municipio_id': sucursal.DirMun, 'municipio': mun}
						sucdict['logo'] = None
						sucdict['lat'] = sucursal.Geo1
						sucdict['long'] = sucursal.Geo2
						empresaQ = db.GqlQuery("SELECT * FROM Empresa WHERE IdEmp = :1", sucursal.IdEmp)
						empresas = empresaQ.fetch(1)
						empresadict = {}
						for empresa in empresas:
							empresadict = {'id': empresa.IdEmp, 'nombre': empresa.Nombre, 'url': empresa.Url, 'url_logo': 'http://' + APPID + '/spic?IdEmp=' + empresa.IdEmp}
						sucdict['empresa'] = empresadict
						ofertas = OfertaSucursal.all().filter("IdSuc =", sucursal.IdSuc)
						ofertaslist = []
						for oferta in ofertas.run(batch_size=10000):
							ofs = Oferta.all().filter("IdOft =", oferta.IdOft)
							of = []
							for ofinst in ofs.run(limit=1):
								of = ofinst
							ofertadict = {}
							ofertadict['id'] = oferta.IdOft
							ofertadict['oferta'] = oferta.Oferta
							ofertadict['descripcion'] = oferta.Descripcion
							ofertadict['descuento'] = oferta.Descuento
							ofertadict['promocion'] = oferta.Promocion
							ofertadict['enlinea'] = oferta.Enlinea
							#ofertadict['categoria'] = oferta.IdCat
							ofertadict['precio'] = oferta.Precio
							ofertadict['url'] = oferta.Url
							if of.BlobKey and of.BlobKey is not None:
								url = 'http://' + APPID + '/ofimg?id=' + str(of.BlobKey.key())
							else:
								url = ''
							ofertadict['url_logo'] = url
							palabraslist = []
							palabras = OfertaPalabra.all().filter("IdOft =", oferta.IdOft)
							for palabra in palabras:
								palabraslist.append(palabra.Palabra)
							ofertadict['palabras'] = palabraslist
							cat = None
							# CATEGORIA -temporal-
							idcat = None
							ofts = Oferta.all().filter("IdOft =", oferta.IdOft)
							for oft in ofts:
								idcat = oft.IdCat
							categorias = Categoria.all().filter("IdCat =", idcat)
							for categoria in categorias:
								cat = categoria.Categoria
							ofertadict['categoria_id'] = idcat
							ofertadict['categoria'] = cat
							ofertaslist.append(ofertadict)
						sucdict['ofertas'] = ofertaslist
						sucdict['timestamp'] = str(sucursal.FechaHora)
						outputlist.append(sucdict)
					memcache.add('wssucursales-' + fechastr, outputlist, 3600)
					suclist = outputlist

				if suclist is None:
					self.response.out.write(json.dumps([]))
				else:
					outputlist = []
					for suc in suclist:
						valid = False
						suctimestamp = datetime.strptime(suc['timestamp'].split('.')[0], '%Y-%m-%d %H:%M:%S')
						if timestamp <= suctimestamp and suctimestamp <= timestampend:
							valid = True
						if valid == True:
							outputlist.append(suc)
					self.response.out.write(json.dumps(outputlist))

class wsoferta(webapp.RequestHandler):
        def get(self):
		self.response.headers['Content-Type'] = 'text/plain'
		oid = self.request.get('id')
		if oid and oid != '' and oid != None:
			#self.response.out.write(oid)
			ofertasQ = db.GqlQuery("SELECT * FROM Oferta WHERE IdOft = :1", oid)
			ofertas = ofertasQ.fetch(1)
			ofertadict = {}
			for oferta in ofertas:
				ofertadict['id'] = oid
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
			self.response.out.write(json.dumps(ofertadict))
		else:
			errordict = {'error': -1, 'message': 'Use: /wsoferta?id=<string>'}
			self.response.out.write(json.dumps(errordict))

class wsofertas(webapp.RequestHandler):
	def get(self):
		latitud = None
		longitud = None
		distancia = None
		try:
			latitud = float(self.request.get('latitud'))
			longitud = float(self.request.get('longitud'))
			distancia = float(self.request.get('distancia'))
		except ValueError:
			#self.response.out.write('Uso equivocado')
			latitud = None
	                longitud = None
	                distancia = None

		self.response.headers['Content-Type'] = 'text/plain'

		if not latitud or not longitud or not distancia:
			#self.response.out.write('Ofertas random')
			oidlist = []
			ofertas = randOffer(10)
			if ofertas:
				for oferta in ofertas:
					oidlist.append(oferta['id'])
			else:
				oidlist.append('0')
			ROQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft IN :1", oidlist)
			RO = ROQ.fetch(10)
			outputlist = []
			for oferta in RO:
				ofertadict = {'id': oferta.IdOft, 'oferta': oferta.Oferta, 'descripcion': oferta.Descripcion, 'url': oferta.Url, 'url_logo': 'http://' + APPID + '/ofimg?id=' + oferta.IdOft}
				outputlist.append(ofertadict)
			self.response.out.write(json.dumps(outputlist))
		else:
			maxx = latitud + distancia
			minx = latitud - distancia
			maxy = longitud + distancia
			miny = longitud - distancia
			#sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal")
			#sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal WHERE Geo1 <= :1 AND Geo2 <= :2", maxx, maxy)
			#sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal WHERE Latitud >= :1 AND Latitud <= :2", minx, maxx)
			#sucursalQ = db.GqlQuery("SELECT * FROM Sucursal WHERE Latitud >= :1 AND Latitud <= :2", minx, maxx)
			sucursales = sucursalQ.run(batch_size=100)
			sucursallist = []
			sucursaldict = {}
			for sucursal in sucursales:
				geo1 = sucursal.Latitud
				geo2 = sucursal.Longitud
				#self.response.out.write(str(minx) + ' <= ' + str(geo1) + ' <= ' + str(maxx))
				sqdist = (latitud - geo1) ** 2  + (longitud - geo2) ** 2
				#self.response.out.write(str(sqdist) + ' <= ' + str(distancia ** 2) + '\n')
				if sqdist <= distancia ** 2:
					#self.response.out.write(sucursal.Nombre)
					sucursallist.append(sqdist)
					sucursaldict[sqdist] = sucursal.IdSuc

			for i in range(len(sucursallist) - 1):
				for j in range(len(sucursallist) - 1):
					#self.response.out.write(str(i) + ',' + str(j) + '\n')
					tempvar = None
					if i < (len(sucursallist) - 1):
						if sucursallist[i] > sucursallist [i + 1]:
							#self.response.out.write('cambio ' + str(i) + ',' + str(j) + '\n')
							tempvar = sucursallist[i]	
							sucursallist[i] = sucursallist[i + 1]
							sucursallist[i + 1] = tempvar

			nbofertas = 0
			outputlist = []
			for sucursalsqdist in sucursallist:
                                #self.response.out.write(str(sucursalsqdist) + ' - ' + sucursaldict[sucursalsqdist] + '\n')
				ofertasQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdSuc = :1 ORDER BY IdOft DESC, Oferta", sucursaldict[sucursalsqdist])
				ofertas = ofertasQ.fetch(10)
				for oferta in ofertas:
					outputdict = {}
					if nbofertas < 10:
						outputdict['id'] = oferta.IdOft
						outputdict['empresa'] = {'id': oferta.IdEmp, 'nombre': oferta.Empresa}
						tipoO = None
						if oferta.Descuento == '' or oferta.Descuento == None:
							tipoO = 1
						else:
							tipoO = 2
						outputdict['tipo_oferta'] = tipoO
						outputdict['oferta'] = oferta.Oferta
						outputdict['descripcion'] = oferta.Descripcion
						outputdict['distancia'] = math.sqrt(sucursalsqdist)
						ofertadict['url_logo'] = 'http://' + APPID + '/ofimg?id=' + oferta.IdOft

						suclist = []
						sucursalQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft = :1", oferta.IdOft)
						sucursales = sucursalQ.run(batch_size=100)
						for suc in sucursales:
							sucdict = {'id': suc.IdSuc, 'lat': suc.Lat, 'long': suc.Lng}
							suclist.append(sucdict)
						outputdict['sucursales'] = suclist
						outputdict['ofertas_relacionadas'] = randOffer(3)

						outputlist.append(outputdict)			
					nbofertas += 1
			jsonoutput = json.dumps(outputlist)
			self.response.out.write(jsonoutput)

class wsofertaxc(webapp.RequestHandler):
	def get(self):
		"""try:
			latitud = self.request.get('latitud')
			longitud = self.request.get('longitud')
			distancia = self.request.get('distancia')
			categoria = self.request.get('categoria')
			status = self.request.get('status')
			start = self.request.get('start')
			entidad = self.request.get('entidad')
			if latitud and longitud and distancia:
				self.response.out.write("Coord")
			elif entidad:
				self.response.out.write("entidad")
			else:
				self.response.out.write("N/A")
		except ValueError:
			self.response.out.write('Value Error')"""

		categoria = self.request.get('categoria')
		pagina = self.request.get('pagina')
		self.response.headers['Content-Type'] = 'text/plain'
		if not categoria or categoria == '':
			errordict = {'error': -1,'message': 'Correct use: /wsofertaxc?categoria=<int>[&pagina=<int>]'}
			self.response.out.write(json.dumps(errordict))
		else:
			categoria = int(categoria)
			if not pagina or pagina == '':
				pagina = 1
			pagina = int(pagina)
			if pagina > 0:
				pagina -= 1
				pagina *= 10
			ofertasQ = db.GqlQuery("SELECT * FROM Oferta WHERE IdCat = :1 ORDER BY FechaHora DESC", categoria)
			ofertas = ofertasQ.fetch(10, offset=pagina)
			#self.response.out.write(pagina)
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

class wsofertaxp(webapp.RequestHandler):
        def get(self):
		latitud = self.request.get('latitud')
                longitud = self.request.get('longitud')
                distancia = self.request.get('distancia')
		palabra = self.request.get('palabra')
		pagina = self.request.get('pagina')
		start = self.request.get('start')
                estado = self.request.get('estado')
		self.response.headers['Content-Type'] = 'text/plain'
		if not palabra or palabra == '':
			errordict = {'error': -1,'message': 'Correct use: /wsofertaxp?palabra=<string>[&pagina=<int>]'}
                        self.response.out.write(json.dumps(errordict))
		else:
                        if not pagina or pagina == '':
                                pagina = 1
                        pagina = int(pagina)
                        if pagina > 0:
                                pagina -= 1
                                pagina *= 10
			palabra = palabra.lower()
                        palabrasQ = db.GqlQuery("SELECT * FROM OfertaPalabra WHERE Palabra = :1 ORDER BY FechaHora DESC", palabra)
                        palabras = palabrasQ.fetch(10, offset=pagina)
			idofts = []
			for palabra in palabras:
				idofts.append(palabra.IdOft)
			ofertasQ = db.GqlQuery("SELECT * FROM Oferta WHERE IdOft IN :1", idofts)
			ofertas = ofertasQ.fetch(10)
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

class wsempresastest(webapp.RequestHandler):
	def get(self):
		self.response.headers['Content-Type'] = 'text/plain'
		#self.response.out.write(APPID + '\n')
		ecache = memcache.get('wsEmpresas_alfabeto')
		if ecache is None:
			alfabeto = []
			empresasQ = db.GqlQuery("SELECT Nombre, IdEmp FROM EmpresaNm")
			empresasR = empresasQ.run(batch_size=100000)
			empresas = []
			for empresa in empresasR:
				empresas.append(empresa)
			empresas = sorted(empresas, key=lambda k: k.Nombre.lower().replace(' ','').replace('"','').replace('\'','').replace('(','').replace('{','').replace('[','').replace('+','').replace('1','0').replace('2','0').replace('3','0').replace('4','0').replace('5','0').replace('6','0').replace('7','0').replace('8','0').replace('9','0').replace('.','').replace(',','').replace(':',''))
			letradict = {}
			inite = None
			lastinit = inite
			empresadict = {}
			for empresa in empresas:
				#self.response.out.write(empresa.Nombre + '\n')
				nombreClean = empresa.Nombre.replace(' ','').replace('"','').replace('\'','').replace('(','').replace('{','').replace('[','').replace('+','').replace('1','0').replace('2','0').replace('3','0').replace('4','0').replace('5','0').replace('6','0').replace('7','0').replace('8','0').replace('9','0').replace('.','').replace(',','').replace(':','')
				inite = nombreClean[0].lower().replace(u'\u00e1',u'a').replace(u'\u00e9',u'e').replace(u'\u00ed',u'i').replace(u'\u00f3',u'o').replace(u'\u00fa',u'u').replace(u'\u00f1',u'n')
				empresadict = {'id': empresa.IdEmp, 'nombre': empresa.Nombre}
				#logging.info(empresadict)
				try:
					letradict[inite].append(empresadict)
					letradict[inite] = sorted(letradict[inite], key=lambda k: k['nombre'])
					lastinit = inite
				except KeyError:
					if lastinit is not None:
						memcache.add('wsEmpresas_' + lastinit, json.dumps(letradict[lastinit]), 86400)
						alfabeto.append(lastinit)
						logging.info('Append ' + lastinit)
						lastinit = inite
					letradict[inite] = []
					letradict[inite].append(empresadict)
					letradict[inite] = sorted(letradict[inite], key=lambda k: k['nombre'])
			islastinit = False
			for i in alfabeto:
				if inite == i:
					islastinit = True
			if islastinit == False:
				#letradict[inite].append(empresadict)
				memcache.add('wsEmpresas_' + inite, json.dumps(letradict[inite]), 86400)
				alfabeto.append(inite)
			memcache.add('wsEmpresas_alfabeto', json.dumps(alfabeto), 86400)
			self.response.out.write(json.dumps({'empresa_participantes': letradict}))	
		else:
			alfabeto = json.loads(ecache)
			letradict = {}
			for letra in alfabeto:
				#logging.info('Reading wsEmpresas_' + letra)
				letracache = memcache.get('wsEmpresas_' + letra)
				letracache = json.loads(letracache)
				letradict[letra] = letracache
			self.response.out.write(json.dumps(letradict))
		#self.response.out.write(json.dumps({'empresas_participantes': []}))

class wsempresas(webapp.RequestHandler):
	def get(self):
		alfabeto = 'abcdefghijklmnopqrstuvwxyz'
		outputdict = {}
		for i in range(26):
			letra = alfabeto[i]
			logging.info('Getting count dirprefix_count_' + letra)
			count = memcache.get('dirprefix_count_' + letra)
			pages = int(count) / 200
			letralist = []
			if int(count) % 200 > 0:
				pages += 1
			for j in range(pages):
				try:
					empresas = json.loads(memcache.get('dirprefix_' + letra + '_' + str(j)))
					for empresa in empresas:
						empresadict = {'id': empresa['IdEmp'], 'nombre': empresa['Nombre']}
						letralist.append(empresadict)
				except TypeError:
					pass
			outputdict[letra] = sorted(letralist, key=lambda k: k['nombre'])
		self.response.headers['Content-Type'] = 'text/plain'
		self.response.out.write(json.dumps(outputdict))

class wsfaq(webapp.RequestHandler):
        def get(self):
		self.response.headers['Content-Type'] = 'text/plain'
		#self.response.out.write('FAQ')
		faqfile = 'faq.txt'
		data = open(faqfile)
		data.seek(0)
		linenb = 0
		preguntaslist = []
		preguntadict = {}
		for line in data:
			if line != '\n':
				linenb += 1
				if (linenb % 2) == 1:
					preguntadict['pregunta'] = line[:-1]
				else:
					preguntadict['respuesta'] = line[:-1]
			else:
				preguntadict['id'] = linenb / 2
				preguntaslist.append(preguntadict)
				preguntadict = {}
		preguntadict['id'] = linenb / 2
                preguntaslist.append(preguntadict)
		outputdict = {'preguntas': preguntaslist}
		self.response.out.write(json.dumps(outputdict))

class oxs(webapp.RequestHandler):
	def get(self):
		paramlist = ["homsunoftslgmy","pkfeswypnqnqwf"]
		paramdict = {"params": paramlist}
		params = urllib.urlencode(paramdict)
		req = urllib2.Request('http://movil.ebfmex-pub.appspot.com/ofertaxsucursal',params)
		#req.add_header("content-type", "application/json")
		response = urllib2.urlopen(req)
		resp = response.read()
		self.response.out.write(resp)

class ofertaxsucursal(webapp.RequestHandler):
	def get(self):
                paramdict = self.request.get('params')
                params=paramdict.encode('ascii').split(",")

                ofertalist = []
                for param in params:
			scache = memcache.get('OxS_' + param)
			if scache is None:
	                        ofertasQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdSuc = :1", param)
	                        ofertas = ofertasQ.fetch(1)
	                        ofertadict = {}
	                        for oferta in ofertas:
	                                ofertadict['id'] = oferta.IdOft
	                                tipo = None
	                                if oferta.Descuento == '' or oferta.Descuento == None:
	                                        tipo = 1
	                                else:
	                                        tipo = 0
	                                ofertadict['tipo_oferta'] = tipo
	                                ofertadict['oferta'] = oferta.Oferta
	                                ofertadict['descripcion'] = oferta.Descripcion
					ofts = db.GqlQuery("SELECT Codigo FROM Oferta WHERE IdOft = :1", oferta.IdOft)
					oft = ofts.fetch(1)[0]
					logourl = ''
					if oft.Codigo and oft.Codigo.replace('https://','http://')[0:7] == 'http://':
	                                        logourl = oft.Codigo
					"""try:
	                                        if logourl == '' and oft.BlobKey and oft.BlobKey != None and oft.BlobKey.key() != 'none':
	                                                logourl = '/ofimg?id=' + str(oft.BlobKey.key())
					except AttributeError:
						error = 'logourl'"""
					if logourl == '' and oft.Promocion and oft.Promocion != '':
						logourl = oft.Promocion
	                                ofertadict['url_logo'] = logourl
	                                suclist = []
	                                sucursalQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft = :1", oferta.IdOft)
	                                sucursales = sucursalQ.run(batch_size=100)
	                                for suc in sucursales:
	                                        sucdict = {'id': suc.IdSuc, 'lat': suc.Lat, 'long': suc.Lng}
	                                        suclist.append(sucdict)
	                                ofertadict['sucursales'] = suclist
	                                emplist = {'id': oferta.IdEmp, 'nombre': oferta.Empresa}
	                                ofertadict['empresa'] = emplist
	                                ofertadict['ofertas_relacionadas'] = randOffer(3,oferta.IdEmp)
	                        ofertalist.append(ofertadict)
				memcache.add('OxS_' + param, json.dumps(ofertalist), 86400)
			else:
				ofertalist = json.loads(scache)

	                outputdict = {'ofertas': ofertalist}
	                self.response.out.write(json.dumps(outputdict))

class changecontrol(webapp.RequestHandler):
	def get(self):
		timestamp = self.request.get('timestamp')
                horas = self.request.get('horas')
                self.response.headers['Content-Type'] = 'text/plain'
                if not timestamp or not horas or timestamp == None or horas == None or timestamp == '' or horas == '':
                        errordict = {'error': -1, 'message': 'Must specify variables in GET method (i.e. /changecontrol?timestamp=<YYYYMMDDHH24>&horas=<int>)'}
                        self.response.out.write(json.dumps(errordict))
                elif len(timestamp) != 10:
                        errordict = {'error': -2, 'message': 'timestamp must be 10 chars long: YYYYMMDDHH24'}
                        self.response.out.write(json.dumps(errordict))
                else:
                        try:
                                timestamp = datetime.strptime(timestamp,'%Y%m%d%H')
                                horas = int(horas)
                                timestampend = timestamp + timedelta(hours = horas)
                        except ValueError:
                                errordict = {'error': -2, 'message': 'Value Error. Timestamp must be YYYYMMDDHH24 and horas is an integer'}
                                self.response.out.write(json.dumps(errordict))
                        if horas > 24:
                                errordict = {'error': -2, 'message': 'Horas must be <= 24'}
                                self.response.out.write(json.dumps(errordict))
                        else:
                                #timestamp += timedelta(hours = 5)
                                #timestampend += timedelta(hours = 5)
				changes = ChangeControl.all().filter("FechaHora >=", timestamp).filter("FechaHora <=", timestampend)
				changeslist = []
				for change in changes:
					changesdict = {'fecha': str(change.FechaHora), 'id': change.Id, 'modelo': change.Kind, 'tipo': change.Status}
					changeslist.append(changesdict)
				self.response.out.write(json.dumps(changeslist))

class ultimasOfertas(webapp.RequestHandler):
	def get(self):
		url = "http://movil.ebfmxorg.appspot.com/search?pagina=1&tipo=1&kind=Oferta&batchsize=800"
		result = urlfetch.fetch(url)
		self.response.out.write(result.content)

class wsempresasblob(webapp.RequestHandler):
	def get(self):
		cache = memcache.get('wsEmpresasBlob')
		if cache is None:
			blobs = MvBlob.all()
			emplist = []
			outputdict = {}
			for blob in blobs:
				blob = json.loads(blob.Blob)
				init = blob['empresa']['nombre'].lower().replace(u'\u00e1',u'a').replace(u'\u00e9',u'e').replace(u'\u00ed',u'i').replace(u'\u00f3',u'o').replace(u'\u00fa',u'u').replace(u'\u00f1',u'n').replace(':','').replace('.','').replace(' ','').replace(',','').replace('(','').replace('{','').replace('[','')[0]
				empresadict = {'id': blob['empresa']['id'], 'nombre': blob['empresa']['nombre']}
				exists = False
				for emp in emplist:
					if emp == empresadict['id']:
						exists = True
				if not exists:
					emplist.append(empresadict['id'])
					try:
						outputdict[init].append(empresadict)
						outputdict[init] = sorted(outputdict[init], key=lambda k: k['nombre'].lower().replace(u'\u00e1',u'a').replace(u'\u00e9',u'e').replace(u'\u00ed',u'i').replace(u'\u00f3',u'o').replace(u'\u00fa',u'u').replace(u'\u00f1',u'n').replace(':','').replace('.',''))
					except KeyError:
						outputdict[init] = []
						outputdict[init].append(empresadict)
			memcache.add('wsEmpresasBlob', json.dumps(outputdict), 86400)
		else:
			outputdict = json.loads(cache)
		self.response.headers['Content-Type'] = 'text/plain'
		self.response.out.write(json.dumps({'empresa_participantes': outputdict}))
