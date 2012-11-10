import datetime, random
from datetime import datetime, timedelta
import json

import logging

from google.appengine.api import urlfetch

from google.appengine.api import taskqueue

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import app_identity

from models import Sucursal, Oferta, OfertaSucursal, Empresa, Categoria, OfertaPalabra, SearchData, Cta, Entidad, Municipio, ShortLogo, ChangeControl
from randString import randLetter, randString
from search import generatesearch, search
from be import gensearch
from sendmail import sendmail

APPID = app_identity.get_default_version_hostname()

urlfetch.set_default_fetch_deadline(60)

H = 6

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

class UpdateSearch(webapp.RequestHandler):
	def token(self):
		token = self.request.get('token')
		if token and str(token) == 'ZWJmbWV4LXB1YnIeCxISX0FoQWRtaW5Yc3JmVG9rZW5fIgZfWFNSRl8M':
			try:
				gminutes = self.request.get('minutes')
				ghours = self.request.get('hours')
				gdays = self.request.get('days')
				if not gminutes:
					gminutes = 0
				else:
					gminutes = int(gminutes)
				if not ghours:
					ghours = 0
				else:
					ghours = int(ghours)
				if not gdays:
					gdays = 0
				else:
					gdays = int(gdays)
			except ValueError:
				gminutes = 30
				ghours = 0
				gdays = 0
			time = datetime.now() - timedelta(days = gdays, hours = ghours, minutes = gminutes)
			self.response.headers['Content-Type'] = 'application/json'

			#self.response.out.write (str(time))
			changecontrol = ChangeControl.all().filter("FechaHora >=", time).filter("Kind =", 'Oferta').filter("Status IN", ["A","M"])
			nbadded = 0
			nbremoved = 0
			for cc in changecontrol:
				#self.response.out.write(cc.Id + '\n')
				if cc.Status != 'B':
					ofertas = Oferta.all().filter("IdOft =", cc.Id)
					for oferta in ofertas:
						if cc.Status == 'M':
							searchdata = SearchData.all().filter("Sid =", str(oferta.key()))
							for sd in searchdata:
								db.delete(sd)
								nbremoved += 1
						desc = oferta.Descripcion.replace('\n',' ').replace('\r',' ').replace('.',' ').replace(',',' ').split(' ')
						nombre = oferta.Oferta.replace('.',' ').replace(',',' ').split(' ')
						for palabra in desc:
							if len(palabra) > 3:
								newsd = SearchData()
								newsd.Enlinea = oferta.Enlinea
								newsd.FechaHora = datetime.now() - timedelta(hours = H)
								newsd.Field = 'Descripcion'
								newsd.IdCat = oferta.IdCat
								newsd.Kind = 'Oferta'
								newsd.Sid = str(oferta.key())
								newsd.Value = palabra.lower()
								newsd.put()
								nbadded += 1
						for palabra in nombre:
							if len(palabra) > 3:
		                                                newsd = SearchData()
       			                                        newsd.Enlinea = oferta.Enlinea
		                                                newsd.FechaHora = datetime.now() - timedelta(hours = H)
		                                                newsd.Field = 'Oferta'
		                                                newsd.IdCat = oferta.IdCat
		                                                newsd.Kind = 'Oferta'
		                                                newsd.Sid = str(oferta.key())
		                                                newsd.Value = palabra.lower()
		                                                newsd.put()
								nbadded += 1
						palabraclave = OfertaPalabra.all().filter("IdOft =", oferta.IdOft)
						for palabra in palabraclave.Palabra:
							if len(palabra) > 3:
								newsd = SearchData()
		                                                newsd.Enlinea = oferta.Enlinea
		                                                newsd.FechaHora = datetime.now() - timedelta(hours = H)
		                                                newsd.Field = 'OfertaPalabra'
		                                                newsd.IdCat = oferta.IdCat
		                                                newsd.Kind = 'Oferta'
		                                                newsd.Sid = str(oferta.key())
		                                                newsd.Value = palabra.Palabra.lower()
		                                                newsd.put()
								nbadded += 1

			logging.info("Finished updating. Added: %s. Removed: %s.", str(nbadded), str(nbremoved))
		else:
			logging.error('Wrong token given.')

class SearchInit(webapp.RequestHandler):
        def post(self):
		for sd in SearchData.all():
			db.delete(sd)

		appid = APPID
		if APPID == 'ebfmex-pub.appspot.com' or APPID == 'ebfmxorg.appspot.com':
			appid = 'movil.' + APPID

                url = 'http://' + appid + '/backend/generatesearch?kind=Oferta&field=Oferta'
		#result = urlfetch.fetch(url)
		url = 'http://' + appid + '/backend/generatesearch?kind=Oferta&field=Descripcion'
		#result = urlfetch.fetch(url)

		#self.redirect('/backend/generatesearch?kind=Oferta&field=Descripcion')
		#self.redirect('/backend/generatesearch?kind=Oferta&field=Oferta')

		for oferta in Oferta.all():
			for palabra in OfertaPalabra.all().filter("IdOft =", oferta.IdOft):
				 newsd = SearchData()
                                 newsd.Enlinea = oferta.Enlinea
                                 newsd.FechaHora = datetime.now() - timedelta(hours = H)
                                 newsd.Field = 'OfertaPalabra'
                                 newsd.IdCat = oferta.IdCat
                                 newsd.Kind = 'Oferta'
                                 newsd.Sid = str(oferta.key())
                                 newsd.Value = palabra.Palabra.lower()
                                 newsd.put()

def gensearch_tr():
	nbofertas = Oferta.all().count()
	batchsize = 10
	batchnumber = 0
	while nbofertas >= 0:
		taskqueue.add(url='/backend/gensearch', params={'kind': 'Oferta', 'field': 'Descripcion', 'batchsize': batchsize, 'batchnumber': batchnumber})
		taskqueue.add(url='/backend/gensearch', params={'kind': 'Oferta', 'field': 'Oferta', 'batchsize': batchsize, 'batchnumber': batchnumber})
		nbofertas -= batchsize
		batchnumber += 1
	taskqueue.add(url='/backend/searchinit')
	logging.info(str(nbofertas) + ' ofertas. Batch size: ' + str(batchsize) + '. Done queueing.')

def updatesearch_tr(token, days, hours, minutes):
	taskqueue.add(url='/backend/updatesearch', params={'token': token, 'days': days, 'hours': hours, 'minutes': minutes})

class SearchInitTask(webapp.RequestHandler):
        def get(self):
                #db.run_in_transaction(gensearch_tr)
		#gensearch_tr
		nbofertas = Oferta.all().count()
	        batchsize = 30
	        batchnumber = 0
		logging.info(str(nbofertas) + ' ofertas. Batch size: ' + str(batchsize) + '. Queueing.')
	        while nbofertas >= 0:
	                taskqueue.add(url='/backend/gensearch', params={'split': 1, 'kind': 'Oferta', 'field': 'Descripcion', 'batchsize': batchsize, 'batchnumber': batchnumber})
	                taskqueue.add(url='/backend/gensearch', params={'split': 1, 'kind': 'Oferta', 'field': 'Oferta', 'batchsize': batchsize, 'batchnumber': batchnumber})
	                nbofertas -= batchsize
	                batchnumber += 1
	        taskqueue.add(url='/backend/searchinit')

class CountSids(webapp.RequestHandler):
	def get(self):
		sdlist = []
		count = 0
		for sd in SearchData.all():
			exists = False
			for oid in sdlist:
				if oid == sd.Sid:
					exists = True
			if exists == False:
				sdlist.append(sd.Sid)	
				count += 1
		self.response.out.write(count)

class CountOfertas(webapp.RequestHandler):
        def get(self):
                olist = []
                count = 0
                for o in Oferta.all():
                	count += 1
                self.response.out.write(count)

class UpdateSearchTask(webapp.RequestHandler):
        def get(self):
		try:
			token = self.request.get('token')
	                gminutes = self.request.get('minutes')
                        ghours = self.request.get('hours')
                        gdays = self.request.get('days')
			if not token:
				token = ''
                        if not gminutes:
	                        gminutes = 0
                        else:
                                gminutes = int(gminutes)
                        if not ghours:
                                ghours = 0
                        else:
                                ghours = int(ghours)
                        if not gdays:
                                gdays = 0
                        else:
                                gdays = int(gdays)
		except ValueError:
			token = ''
                        gminutes = 30
                        ghours = 0
                        gdays = 0
                #db.run_in_transaction(updatesearch_tr(token, gdays, ghours, gminutes))
		taskqueue.add(url='/backend/updatesearch', params={'token': token, 'days': gdays, 'hours': ghours, 'minutes': gminutes})

class ReporteCtas(webapp.RequestHandler):
	def get(self):
		pagina = self.request.get('pagina')
		if not pagina:
			pagina = 1	
		else:
			try:
				pagina = int(pagina)
			except ValueError:
				pagina = 1
		if pagina > 0:
			pagina -= 1
			batchsize = 500
			offset = batchsize * pagina

		self.response.headers['Content-Type'] = 'text/csv'
		#self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
		self.response.out.write("cta.Nombre,cta.Apellidos,cta.Puesto,cta.Email,cta.EmailAlt,cta.Pass,cta.Tel,cta.Cel,cta.FechaHora,cta.CodigoCfm,cta.Status,IdEmp,RFC,Nombre Empresa,Logo,Razon Social,Dir.Calle,Dir.Colonia,Dir.Entidad,Dir.Municipio,Dir.Cp,Dir.Numero Suc,Organiso Emp,Otro Organismo,Reg Org. Empresarial,Url,PartLinea,ExpComer,Descripcion,FechaHora Alta Emp.,emp.Status\n")

		ctas = Cta.all().order("FechaHora")
		for cta in ctas.run(batch_size=20000, limit=500):
			empresas = Empresa.all()
			empresas.ancestor(cta)
			for emp in empresas:
				entidad = ''
				entidades = Entidad.all().filter("CveEnt =", emp.DirEnt)
				for ent in entidades:
					entidad = ent.Entidad
				municipio = ''
				municipios = Municipio.all().filter("CveMun =", emp.DirMun)
				for mun in municipios:
					municipio = mun.Municipio
				haslogo = 'no'
				shortlogos = ShortLogo.all().filter("IdEmp =", emp.IdEmp)
				for sl in shortlogos:
					haslogo = 'si'

				self.response.out.write('"' + cta.Nombre + '","' + cta.Apellidos + '","' +  cta.Puesto + '","' +  cta.Email + '","' + cta.EmailAlt + '","' + cta.Pass + '","' + cta.Tel + '","' + cta.Cel + '","' + str(cta.FechaHora) + '","' + cta.CodigoCfm + '","' + str(cta.Status) + '","' + emp.IdEmp + '","' + emp.RFC + '","' + emp.Nombre + '","' + haslogo + '","' + emp.RazonSoc + '","' + emp.DirCalle + '","' + emp.DirCol + '","' + entidad + '","' + municipio + '","' + emp.DirCp + '","' + emp.NumSuc + '","' + emp.OrgEmp + '","' + emp.OrgEmpOtro + '","' + emp.OrgEmpReg + '","' + emp.Url + '","' + str(emp.PartLinea) + '","' + str(emp.ExpComer) + '","' + str(emp.Desc).replace(u'\u00e1',u'a').replace(u'\u00e9',u'e').replace(u'\u00ed',u'i').replace(u'\u00f3',u'o').replace(u'\u00fa',u'u').replace(u'\u00f1',u'n').replace('\n',' ').replace('\r',' ') + '","' + str(emp.FechaHora) + '","' + str(emp.Status) + '"\n')

class Blacklist(webapp.RequestHandler):
	def get(self):
		ofertaslist = []
		futuredate = datetime.strptime('9999-12-31 23:59:59', '%Y-%m-%d %H:%M:%S')
		blacklist = [
		'dummy',
		'groan',
		'lobortis',
		'ano',
		'boludo',
		'cabron',
		'cabrona',
		'cabronez',
		'caca',
		'cagada',
		'cagadera',
		'cagaderas',
		'cagon',
		'cagoteada',
		'cagotear',
		'cagoteo',
		'chaquero',
		'chaqueteras',
		'chigadera',
		'chigados',
		'chinga',
		'chingadera',
		'chingados',
		'chingon',
		'chingue',
		'chingues',
		'coger',
		'cogido',
		'cogon',
		'cogones',
		'coley',
		'concha',
		'culera',
		'culero',
		'culo',
		'droga',
		'drogadicto',
		'estipideces',
		'estupida',
		'estupideses',
		'estupido',
		'fecal',
		'fuck',
		'fucking',
		'hueva',
		'huevos',
		'jodido',
		'jotadas',
		'joto',
		'joton',
		'maaamaaadaaa',
		'maamaadaa',
		'mamada',
		'mamadera',
		'mamador',
		'mamar',
		'mamdota',
		'mame',
		'mames',
		'mamon',
		'maricon',
		'marik',
		'marika',
		'mierda',
		'mierdero',
		'mion',
		'miona',
		'mrda',
		'nalgas',
		'nalgon',
		'narco',
		'narcotrafico',
		'nomamar',
		'peda',
		'pedo',
		'pendeja',
		'pendejada',
		'pendejo',
		'pene',
		'pinche',
		'pinches',
		'pinchon',
		'pinchuriento',
		'ptm',
		'pucha',
		'puta',
		'putero',
		'putin',
		'teta',
		'tetas',
		'verga',
		'vergisima',
		'vergon',
		'verguisima',
		'verija',
		'verijudo',
		'weba',
		'webones',
		'webos',
		'wtf',
		'zeta',
		'zetas',
		]

		mainlist = []
		ind = 0
		sublist = []
		for palabra in blacklist:
			if ind >= 30:
				mainlist.append(sublist)
				sublist = []
				ind = 0
			sublist.append(palabra)
			ind += 1

		for blacklist in mainlist:
			blacklistedQ = db.GqlQuery("SELECT Sid FROM SearchData WHERE Kind = 'Oferta' AND Value IN :1", blacklist)
			for blacklisted in blacklistedQ:
				oferta = Oferta.get(blacklisted.Sid)
				if oferta.FechaHoraPub < futuredate:
					oferta.FechaHoraPub = futuredate
					oferta.put()
					ofertaslist.append(oferta.IdOft)
		self.response.out.write(json.dumps(ofertaslist))

		receipient = 'thomas@clicker360.com'
		subject = 'Ofertas blacklisted'
		body = 'Esas ofertas se quitaron por causa de tener palabras del blacklist.\nOfertas:\n' + json.dumps(ofertaslist) + '\n\nPalabras:\n' + json.dumps(blacklist)
		
		mail = sendmail(receipient,subject,body)
		mail.send()

application = webapp.WSGIApplication([
        #('/backend/migrategeo', migrateGeo),
        #('/backend/filldummy', dummyOfertas),
        #('/backend/cleandummy', cleandummy),
        #('/backend/dummysucursal', dummysucursal),
        #('/backend/geogenerate', geogenerate),
	('/backend/generatesearch', generatesearch),
	('/backend/updatesearch', UpdateSearch),
	#('/backend/reportectas.csv', ReporteCtas),
	('/backend/searchinit', SearchInit),
	('/backend/gensearch', gensearch),
	('/backend/sit', SearchInitTask),
	('/backend/ust', UpdateSearchTask),
	#('/backend/countsids', CountSids),
	#('/backend/countofertas', CountOfertas),
	('/backend/blacklist', Blacklist),
        ], debug=True)

def main():
	logging.getLogger().setLevel(logging.DEBUG)
        run_wsgi_app(application)

if __name__ == '__main__':
        main()
