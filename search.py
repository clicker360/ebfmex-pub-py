import json
from datetime import datetime

from google.appengine.ext import webapp
from google.appengine.ext import db

from models import *

class generatesearch(webapp.RequestHandler):
	def get(self):
		kindg = self.request.get('kind')
		field = self.request.get('field')
		self.response.headers['Content-Type'] = 'text/plain'
		if not kindg or not field or kindg == '' or field == '':
			errordict = {'error': -1, 'message': 'Correct use: /backend/generatesearch?kind=<str>&field=<str>'}
			self.response.out.write(json.dumps(errordict))
		else:
			try:
				kindsQ = db.GqlQuery("SELECT * FROM " + kindg)
				kinds = kindsQ.run(batch_size=100000)
				for kind in kinds:
					#self.response.out.write("1")
					values = getattr(kind, field)
					values = values.replace('.',' ').replace(',',' ').split(' ')
					for value in values:
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
