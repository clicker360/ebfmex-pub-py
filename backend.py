import datetime, random

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

from models import Sucursal, Oferta, OfertaSucursal, Empresa, Categoria, OfertaPalabra, SearchData
from randString import randLetter, randString
from search import generatesearch

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

class cleandummy(webapp.RequestHandler):
	def get(self):
		"""cats = Categoria.all()
		for cat in cats:
			db.delete(cat)"""
		ofs = Oferta.all()
		for of in ofs:
			db.delete(of)
		ofsucs = OfertaSucursal.all()
		for ofsuc in ofsucs:
			db.delete(ofsuc)
		ofpals = OfertaPalabra.all()
		for ofpal in ofpals:
			db.delete(ofpal)

class dummysucursal(webapp.RequestHandler):
	def get(self):
		suc = Sucursal()
		suc.IdSuc = '123'
	        suc.IdEmp = '123'
		suc.Nombre = 'Animo Inc.'
	        suc.Tel = '5551011852'
       		suc.DirCalle = 'Kamurocho'
	        suc.DirCol = 'Kabukicho'
	        suc.DirEnt = 'Tokyo'
	        suc.DirMun = 'Shinjuku'
	        suc.Geo1 = '35.694798'
	        suc.Geo2 = '139.703166'
	        suc.FechaHora = datetime.datetime.now()
	        suc.Latitud = float(suc.Geo1)
	        suc.Longitud = float(suc.Geo2)
		suc.put()

class dummyOfertas(webapp.RequestHandler):
	def get(self):
		"""for j in range(0,10):
                        categoria = Categoria()
                        categoria.IdCat = j
                        categoria.Categoria = 'Dummy Categoria ' + str(j)
                        categoria.put()"""

		sucursalesQ = db.GqlQuery("SELECT * FROM Sucursal")
                for sucursal in sucursalesQ.run(batch_size=100000):
			EQ = db.GqlQuery("SELECT * FROM Empresa WHERE IdEmp = :1", sucursal.IdEmp)
			Es = EQ.fetch(1)
			NombreE = None
			for E in Es:
				NombreE = E.Nombre
			for i in range(0,10):
				now = datetime.datetime.now()

				oferta = Oferta()
				ofertasucursal = OfertaSucursal()

				oferta.IdOft = randString(10,False)
				oferta.IdEmp = sucursal.IdEmp
				oferta.IdCat = random.randrange(10)
				oferta.Empresa = NombreE
				oferta.Oferta = 'Dummy oferta ' + str(i)
				oferta.Descripcion = 'Dummy desc ' + str(i)
				oferta.Codigo = 'dummycodigo' + str(i)
				oferta.Precio = str(i) + '99.99'
				oferta.Descuento = '10%'
				oferta.Enlinea = True
				oferta.Url = 'http://localhost:8080/' + sucursal.Nombre + '/' + str(i)
				oferta.Tarjetas = ''
				oferta.Meses = None
				oferta.FechaHoraPub = now
				oferta.StatusPub = True
				oferta.FechaHora = now
				oferta.put()

				ofertasucursal.IdOft = oferta.IdOft
				ofertasucursal.IdEmp = sucursal.IdEmp
				ofertasucursal.IdSuc = sucursal.IdSuc
				ofertasucursal.IdCat = oferta.IdCat
				ofertasucursal.Empresa = NombreE
				ofertasucursal.Sucursal = sucursal.Nombre
				ofertasucursal.Descripcion = oferta.Descripcion
				ofertasucursal.Oferta = 'Dummy oferta ' + str(i)
				ofertasucursal.Precio = str(i) + '99.99'
				ofertasucursal.Descuento = '10%'
				ofertasucursal.Url = 'http://localhost:8080/' + sucursal.Nombre + '/' + str(i)
				ofertasucursal.StatusPub = True
				ofertasucursal.lat = sucursal.Latitud
				ofertasucursal.lng = sucursal.Longitud
				ofertasucursal.put()

				for k in range(0,5):
					randnum = random.randrange(20)
					ofertapalabra = OfertaPalabra()
					ofertapalabra.IdEmp = sucursal.IdEmp
					ofertapalabra.IdOft = oferta.IdOft
					ofertapalabra.Palabra = 'palabradummy' + str(randnum)
					ofertapalabra.FechaHora = now
					ofertapalabra.put()

application = webapp.WSGIApplication([
        #('/backend/migrategeo', migrateGeo),
        #('/backend/filldummy', dummyOfertas),
        #('/backend/cleandummy', cleandummy),
        #('/backend/dummysucursal', dummysucursal),
        #('/backend/geogenerate', geogenerate),
	('/backend/generatesearch', generatesearch),
        ], debug=True)

def main():
        run_wsgi_app(application)

if __name__ == '__main__':
        main()
