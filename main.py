import os

import wsgiref.handlers
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template

from models import Cta
from api import wsoferta, wsofertas, wsofertaxc, wsofertaxp, wsfaq
from jobs import migrateGeo, dummyOfertas

class index(webapp.RequestHandler):
	def get(self):
		ctasQ = db.GqlQuery("SELECT * FROM Cta ORDER BY FechaHora")
		ctas = ctasQ.run(batch_size=100000)

		template_vars = {
			'ctas': ctas,
		}

		path = os.path.join(os.path.dirname(__file__), 'index.html')
                self.response.out.write(template.render(path, template_vars))

application = webapp.WSGIApplication([
        ('/', index),
	('/wsoferta', wsoferta),
	('/wsofertas', wsofertas),
	('/wsofertaxc', wsofertaxc),
	('/wsofertaxp', wsofertaxp),
	('/wsfaq', wsfaq),
	('/jobs/migrategeo', migrateGeo),
	('/jobs/dummyofertas', dummyOfertas),
	], debug=True)

def main():
        run_wsgi_app(application)

if __name__ == '__main__':
        main()
