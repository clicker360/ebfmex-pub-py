from google.appengine.ext import db
from google.appengine.ext import webapp

from models import Sucursal

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
