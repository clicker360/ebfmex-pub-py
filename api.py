import math, json, random, urllib, urllib2

from datetime import datetime, timedelta

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import app_identity

from models import Sucursal, OfertaSucursal, Oferta, OfertaPalabra, Entidad, Municipio, Empresa, ChangeControl, Categoria
from randString import randLetter, randString

APPID = app_identity.get_default_version_hostname()
#APPID = app_identity.get_application_id()

def randOffer(nb,empresa=None):
	numoffer = 0
	offerlist = []
	deadline = 52
	while numoffer < nb and deadline > 0:
		letter = randLetter()
		ofertasQ = None
		if empresa == None:
			ofertasQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft >= :1 AND IdOft < :2", letter, letter + u"\ufffd")
		else:
			ofertasQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdEmp = :3 AND IdOft >= :1 AND IdOft < :2", letter, letter + u"\ufffd", empresa)
		ofertas = ofertasQ.fetch(nb)
		if ofertas:
			for oferta in ofertas:
				offerdict = {'id': oferta.IdOft, 'oferta': oferta.Oferta, 'lat': oferta.Lat, 'long': oferta.Lng, 'url_logo': 'http://' + APPID + '/ofimg?id=' + oferta.IdOft}
				offerlist.append(offerdict)
				numoffer += 1
		else:
			deadline -= 1
	if deadline == 0:
		errordict = []
		return errordict
	else:
		return offerlist

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
				#sucursales = Sucursal.all()
				#self.response.out.write(str(timestamp) + ' - ' + str(timestampend))
				#timestamp += timedelta(hours = 5)
				#timestampend += timedelta(hours = 5)
				sucursales = Sucursal.all().filter("FechaHora >=", timestamp).filter("FechaHora <=", timestampend)
				#sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal")
				#sucursales = sucursalesQ.fetch(250)
				self.response.headers['Content-Type'] = 'text/plain'
				outputlist = []
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
					for oferta in ofertas:
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
						ofertadict['url_logo'] = 'http://' + APPID + '/ofimg?id=' + oferta.IdOft
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
					outputlist.append(sucdict)
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
			sucursalQ = db.GqlQuery("SELECT * FROM Sucursal WHERE Latitud >= :1 AND Latitud <= :2", minx, maxx)
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

class wsempresas(webapp.RequestHandler):
	def get(self):
		self.response.headers['Content-Type'] = 'text/plain'
		#self.response.out.write(APPID + '\n')
		empresas = Empresa.all().order("Nombre")
		letradict = {}
		inite = None
		for empresa in empresas:
			#self.response.out.write(empresa.Nombre + '\n')
			inite = empresa.Nombre[0].lower().replace(u'\u00e1',u'a').replace(u'\u00e9',u'e').replace(u'\u00ed',u'i').replace(u'\u00f3',u'o').replace(u'\u00fa',u'u').replace(u'\u00f1',u'n')
			empresadict = {'id': empresa.IdEmp, 'nombre': empresa.Nombre}
			try:
				letradict[inite].append(empresadict)
			except KeyError:
				letradict[inite] = []
				letradict[inite].append(empresadict)
		self.response.out.write(json.dumps({'empresa_participantes': letradict}))

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
	def post(self):
		paramdict = self.request.get('params')
		#params = paramdict.encode('ascii').replace('\'','')[1:-1].replace(' ','').split(",")
		params=paramdict.encode('ascii').split(",")

		ofertalist = []
		for param in params:
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

		outputdict = {'ofertas': ofertalist}
		self.response.out.write(json.dumps(outputdict))
	def get(self):
                paramdict = self.request.get('params')
                params=paramdict.encode('ascii').split(",")

                ofertalist = []
                for param in params:
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
