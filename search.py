import json
from datetime import datetime, timedelta

from google.appengine.ext import webapp
from google.appengine.ext import db

from models import *

class search(webapp.RequestHandler):
	def get(self):
		gvalue = self.request.get('value')
		gfield = self.request.get('field')
		gkind = self.request.get('kind')
		self.response.headers['Content-Type'] = 'text/plain'
		if not gvalue or gvalue == '':
			errordict = {'error': -1, 'message': 'Correct use: /backend/search?value=<str>[&kind=<str>&field=<str>]'}
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
						resultdict = {'Value': result.Value, 'Kind': result.Kind, 'Field': result.Field}
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
		days = self.request.get('days')
		gid = self.request.get('id')
		gvalue = self.request.get('value')
		self.response.headers['Content-Type'] = 'text/plain'
		if not kindg or not field or kindg == '' or field == '':
			errordict = {'error': -1, 'message': 'Correct use: /backend/generatesearch?kind=<str>&field=<str>'}
			self.response.out.write(json.dumps(errordict))
		elif gid and gid != '' and gvalue and gvalue != '':
			existsQ = db.GqlQuery("SELECT * FROM SearchData WHERE Kind = :kindg AND Sid = :sid AND Field = :field", kindg=kindg, sid=gid, field=field)
			for searchdata in existsQ:
				db.delete(searchdata)
			values = gvalue.replace('.',' ').replace(',',' ').split(' ')
			for value in values:
				if len(value) > 3:
					sd = SearchData()
					sd.Sid = gid
					sd.Kind = kindg
					sd.Field = field
					sd.Value = value
					sd.put()
		else:
			try:
				kindsQ = db.GqlQuery("SELECT * FROM " + kindg)
				kinds = kindsQ.run(batch_size=100000)
				for kind in kinds:
					#self.response.out.write("1")
					values = getattr(kind, field)
					values = values.replace('.',' ').replace(',',' ').split(' ')
					for value in values:
						if len(value) > 3:
							exists = False
							existsQ = db.GqlQuery("SELECT * FROM SearchData WHERE Kind = :kindg AND Sid = :sid AND Field = :field AND Value = :value", kindg=kindg, sid=str(kind.key().id()), field=field, value=value)
							existsR = existsQ.fetch(1)
							for searchdata in existsR:
								exists = True
							if not exists:
								#self.response.out.write("2")
								newsd = SearchData()
								newsd.Sid = str(kind.key().id())
								newsd.Kind = kindg
								newsd.Field = field
								newsd.Value = value
								newsd.FechaHora = datetime.now()
								newsd.put()
			except db.KindError:
				errordict = {'error': -2, 'message': 'Kind ' + kind + ' couldn\'t be found. Careful it is case sensitive.'}
	                        self.response.out.write(json.dumps(errordict))
			except AttributeError:
				errordict = {'error': -2, 'message': 'Kind ' + kindg+ ' doesn\'t have any attribute ' + field + '. Careful it is case sensitive.'}
                                self.response.out.write(json.dumps(errordict))

			if not days or days == '':
                                days = -1
                        else:
                                days = int(days)
			"""if days >= 0:
				changes = ChangeControl.all().filter("Kind =", kindg).filter("Status IN ", ["M","B"])
				if days > 0:
					timestamp = datetime.now() - timedelta(days = days)
					changes.filter("FechaHora >", timestamp)
				for change in changes:"""
