from google.appengine.ext import webapp
from google.appengine.ext import db

from models import Sucursal

class wsoferta(webapp.RequestHandler):
        def get(self):
		oid = self.request.get('id')

class wsofertas(webapp.RequestHandler):
	def get(self):
		try:
			latitud = float(self.request.get('latitud'))
			longitud = float(self.request.get('longitud'))
			distancia = float(self.request.get('distancia'))
		except ValueError:
			self.response.out.write('Uso equivocado')

		if not latitud or not longitud or not distancia:
			self.response.out.write('Ofertas random')
		else:
			maxx = latitud + distancia
			minx = latitud - distancia
			maxy = longitud + distancia
			miny = longitud - distancia
			#sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal")
			#sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal WHERE Geo1 <= :1 AND Geo2 <= :2", maxx, maxy)
			sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal WHERE Latitud >= :1 AND Latitud <= :2", minx, maxx)
			sucursales = sucursalesQ.run(batch_size=100)
			output = {}
			sucursalesList = ''
			for sucursal in sucursales:
				geo1 = sucursal.Latitud
				geo2 = sucursal.Longitud
				#self.response.out.write(str(minx) + ' <= ' + str(geo1) + ' <= ' + str(maxx))
				sqdist = (latitud - geo1) ** 2  + (longitud - geo2) ** 2
				if sqdist <= distancia ** 2:
					#self.response.out.write(sucursal.Nombre + '\n')
					sucursalesList += '\'' + sucursal.IdSuc + '\','
			sucursalesList = sucursalesList[:-1]
			#self.response.out.write("SELECT * FROM OfertaSucursal WHERE IdSuc IN(" + sucursalesList + ")")
			ofertasucursalQ = db.GqlQuery("SELECT * FROM OfertaSucursal WHERE IdSuc IN(" + sucursalesList + ")")

class wsofertaxc(webapp.RequestHandler):
	def get(self):
		latitud = self.request.get('latitud')
                longitud = self.request.get('longitud')
                distancia = self.request.get('distancia')
		categoria = self.request.get('categoria')
		status = self.request.get('status')
		start = self.request.get('start')
		estado = self.request.get('estado')

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
