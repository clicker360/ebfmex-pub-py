import os, json

import wsgiref.handlers
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template

from models import Cta
from api import wsoferta, wsofertas, wsofertaxc, wsofertaxp, wsfaq, sucursales, ofertaxsucursal, oxs, wsempresas, changecontrol
from geostuff import geogenerate
from backend import search

class index(webapp.RequestHandler):
	def get(self):
		"""ctasQ = db.GqlQuery("SELECT * FROM Cta ORDER BY FechaHora")
		ctas = ctasQ.run(batch_size=100000)

		template_vars = {
			'ctas': ctas,
		}

		path = os.path.join(os.path.dirname(__file__), 'index.html')
                self.response.out.write(template.render(path, template_vars))"""

		self.response.headers['Content-Type'] = 'text/plain'
		errordict = {'error': -1, 'message': 'Specify an API method (/db, /wsoferta, /wsofertas, /wsofertaxc, /wsofertaxp, /wsfaq) with its variables'}
		self.response.out.write(json.dumps(errordict))

application = webapp.WSGIApplication([
        ('/', index),
	('/wsoferta', wsoferta),
	('/wsofertas', wsofertas),
	('/wsofertaxc', wsofertaxc),
	('/wsofertaxp', wsofertaxp),
	('/wsempresas', wsempresas),
	('/wsfaq', wsfaq),
	('/db', sucursales),
	('/ofertaxsucursal', ofertaxsucursal),
	('/oxs', oxs),
	('/changecontrol', changecontrol),
	('/search', search),
	], debug=True)

def main():
        run_wsgi_app(application)

if __name__ == '__main__':
        main()
