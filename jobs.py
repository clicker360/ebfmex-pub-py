import datetime

from google.appengine.ext import db
from google.appengine.ext import webapp

from models import Sucursal, Oferta, OfertaSucursal, Empresa

class migrateGeo(webapp.RequestHandler):
	def get(self):
		sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal")
		sucursales = sucursalesQ.run(batch_size=100000)

		for sucursal in sucursales:
			latitud = float(sucursal.Geo1)
			longitud = float(sucursal.Geo2)
			sucursal.Latitud = latitud
			sucursal.Longitud = longitud
			sucursal.put()

class dummyOfertas(webapp.RequestHandler):
	def get(self):
		sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal")
                for sucursal in sucursalesQ.run(batch_size=100000):
			OQ = db.GqlQuery("SELECT * FROM Oferta ORDER BY IdOft DESC")
			LastO = OQ.fetch(1)
			lastID = 0
			for O in LastO:
				lastID = int(O.IdOft)
			EQ = db.GqlQuery("SELECT * FROM Empresa WHERE IdEmp = :1", sucursal.IdEmp)
			Es = EQ.fetch(1)
			NombreE = None
			for E in Es:
				NombreE = E.Nombre
			for i in range(0,10):
				now = datetime.datetime.now()

				oferta = Oferta()
				ofertasucursal = OfertaSucursal()
				oferta.IdOft = str(lastID + 1)
				oferta.IdEmp = sucursal.IdEmp
				oferta.IdCat = '1'
				oferta.Empresa = NombreE
				oferta.Oferta = 'Dummy oferta ' + str(i)
				oferta.Descripcion = 'Dummy desc ' + str(i)
				oferta.Codigo = 'dummycodigo' + str(i)
				oferta.Precio = str(i) + '99.99'
				oferta.Descuento = '10%'
				oferta.Enlinea = True
				oferta.Url = 'http://localhost:8080/' + sucursal.Nombre + '/' + str(i)
				oferta.Tarjetas = None
				oferta.Meses = None
				oferta.FechaHoraPub = now
				oferta.StatusPub = True
				oferta.FechaHora = now
				oferta.put()

				ofertasucursal.IdOft = str(lastID + 1)
				ofertasucursal.IdEmp = sucursal.IdEmp
				ofertasucursal.IdSuc = sucursal.IdSuc
				ofertasucursal.Empresa = NombreE
				ofertasucursal.Sucursal = sucursal.Nombre
				ofertasucursal.Oferta = 'Dummy oferta ' + str(i)
				ofertasucursal.Precio = str(i) + '99.99'
				ofertasucursal.Descuento = '10%'
				ofertasucursal.Url = 'http://localhost:8080/' + sucursal.Nombre + '/' + str(i)
				ofertasucursal.StatusPub = True
				ofertasucursal.lat = sucursal.Latitud
				ofertasucursal.lng = sucursal.Longitud
				ofertasucursal.put()
