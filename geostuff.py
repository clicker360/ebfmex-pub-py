from google.appengine.ext import db
from google.appengine.ext import webapp

import geo.geomodel
from models import OfertaSucursal

class Geosuc(geo.geomodel.GeoModel):
	IdSuc = db.StringProperty()

class geogenerate(webapp.RequestHandler):
	def get(self):
		ofsucs = OfertaSucursal.all()
		for ofsuc in ofsucs:
			isgeosucQ = db.GqlQuery("SELECT * FROM Geosuc WHERE IdSuc = :1", ofsuc.IdSuc)
			isgeosuc = isgeosucQ.fetch(1)
			if not isgeosuc or isgeosuc == '' or isgeosuc is None:
				#self.response.out.write("1")
				geosuc = Geosuc(location = db.GeoPt(ofsuc.lat, ofsuc.lng))
				geosuc.IdSuc = ofsuc.IdSuc
				geosuc.put()
			"""else:
				self.response.out.write("Instance already present in Datastore.")"""

class geosucs:
	def getbypoint(lat,lng,dist,nbres):
		outputlist = []
		geosucs = Geosuc.proximity_fetch(Geosuc.all(), geo.geotypes.Point(lat, lng), max_results=nbres, max_distance=dist)
		for geosuc in geosucs:
			outputdict = {'IdSuc': geosuc.IdSuc}
			outputlist.append(outputdict)
