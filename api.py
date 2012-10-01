import math, json, random

from google.appengine.ext import webapp
from google.appengine.ext import db

from models import Sucursal, OfertaSucursal
from randString import randLetter, randString

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
				offerdict = {'id': oferta.IdOft, 'lat': oferta.lat, 'long': oferta.lng}
				offerlist.append(offerdict)
				numoffer += 1
		else:
			deadline -= 1
	if deadline == 0:
		errordict = {'error': -1}
		return errordict
	else:
		return offerlist

class wsoferta(webapp.RequestHandler):
        def get(self):
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
				suclist = []
                                sucursalQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft = :1", oferta.IdOft)
                                sucursales = sucursalQ.run(batch_size=100)
                                for suc in sucursales:
	                                sucdict = {'id': suc.IdSuc, 'lat': suc.lat, 'long': suc.lng}
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
			errordict = {'error': -1}
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
			for oferta in ofertas:
				oidlist.append(oferta['id'])
			ROQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft IN :1", oidlist)
			RO = ROQ.fetch(10)
			outputlist = []
			for oferta in RO:
				ofertadict = {'id': oferta.IdOft, 'oferta': oferta.Oferta, 'descripcion': oferta.Descripcion, 'url': oferta.Url}
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

						suclist = []
						sucursalQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdOft = :1", oferta.IdOft)
						sucursales = sucursalQ.run(batch_size=100)
						for suc in sucursales:
							sucdict = {'id': suc.IdSuc, 'lat': suc.lat, 'long': suc.lng}
							suclist.append(sucdict)
						outputdict['sucursales'] = suclist
						outputdict['ofertas_relacionadas'] = randOffer(3)

						outputlist.append(outputdict)			
					nbofertas += 1
			jsonoutput = json.dumps(outputlist)
			self.response.out.write(jsonoutput)

class wsofertaxc(webapp.RequestHandler):
	def get(self):
		try:
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
			self.response.out.write('Value Error')

class wsofertaxp(webapp.RequestHandler):
        def get(self):
		latitud = self.request.get('latitud')
                longitud = self.request.get('longitud')
                distancia = self.request.get('distancia')
		keyword = self.request.get('keyword')
		start = self.request.get('start')
                estado = self.request.get('estado')

class wsfaq(webapp.RequestHandler):
        def get(self):
		self.response.out.write('FAQ')
