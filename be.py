import json
from datetime import datetime, timedelta

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import memcache

from models import *
import models

H = 5

class gensearch(webapp.RequestHandler):
        def post(self):
                kindg = self.request.get('kind')
                field = self.request.get('field')
                gid = self.request.get('id')
                gvalue = self.request.get('value')
                #genlinea = self.request.get('enlinea')
                gcat = self.request.get('categoria')
                self.response.headers['Content-Type'] = 'text/plain'
                if not kindg or not field or kindg == '' or field == '':
                        errordict = {'error': -1, 'message': 'Correct use: /backend/generatesearch?kind=<str>&field=<str>[&id=<int>&value=<str>&enlinea=<int>]'}
                        self.response.out.write(json.dumps(errordict))
                elif gid and gid != '' and gvalue and gvalue != '':
                        try:
                                data = getattr(models, kindg)
                                kdata = data.get(str(gid))
                                existsQ = SearchData.all().filter("Kind = ", kindg).filter("Sid = ",gid).filter("Field = ",field)
                                """if genlinea:
                                        existsQ.filter("Enlinea =", genlinea)"""
                                for searchdata in existsQ:
                                        db.delete(searchdata)
                                values = gvalue.replace('%20',' ').replace('+',' ').replace('.',' ').replace(',',' ').replace('{',' ').replace('}',' ').split(' ')
                                """if genlinea == 'true' or genlinea == 'True' or genlinea == '0':
                                        genlinea = True
                                else:
                                        genlinea = False"""
                                for value in values:
                                        if len(value) > 3:
                                                sd = SearchData()
                                                sd.Sid = gid
                                                sd.Kind = kindg
                                                sd.Field = field
                                                sd.Value = value.lower()
                                                """if genlinea:
                                                        sd.Enlinea = genlinea"""
                                                if gcat:
                                                        sd.IdCat = int(gcat)
                                                sd.FechaHora = datetime.now() - timedelta(hours = H)
                                                sd.put()
                        except db.BadRequestError:
                                errordict = {'error': -2, 'message': 'Inconsistency in kind/ID/value.'}
                                self.response.out.write(json.dumps(errordict))
                        except db.BadKeyError:
                                errordict = {'error': -2, 'message': 'Inconsistency in kind/ID/value.'}
                                self.response.out.write(json.dumps(errordict))
                else:
                        try:
				split = self.request.get('split')
                                if split:
                                        batchsize = self.request.get('batchsize')
                                        if not batchsize:
                                                batchsize = 100
                                        else:
                                                batchsize = int(batchsize)
                                        batchnumber = self.request.get('batchnumber')
                                        if not batchnumber:
                                                batchnumber = 0
                                        else:
                                                batchnumber = int(batchnumber)
                                        offset = batchnumber * batchsize
                                        kindsQ = db.GqlQuery("SELECT * FROM " + kindg)[offset:offset + batchsize]
                                else:
                                        kindsQ = db.GqlQuery("SELECT * FROM " + kindg)
                                for kind in kindsQ:
                                        #self.response.out.write("1")
                                        values = getattr(kind, field)
                                        values = values.replace('\n',' ').replace('\r',' ').replace('.',' ').replace(',',' ').split(' ')
					for value in values:
                                                if len(value) > 3:
                                                        value = value.lower()
                                                        exists = False
                                                        existsQ = SearchData.all().filter("Kind = ",kindg).filter("Sid = ",str(kind.key())).filter("Field = ",field).filter("Value = ", value)
                                                        existsR = existsQ.run(limit=1)
                                                        for searchdata in existsR:
                                                                exists = True
                                                        if not exists:
                                                                #self.response.out.write("2")
                                                                newsd = SearchData()
                                                                newsd.Sid = str(kind.key())
                                                                newsd.Kind = kindg
                                                                newsd.Field = field
                                                                newsd.Value = value
                                                                newsd.FechaHora = datetime.now() - timedelta(hours = H)
                                                                if kindg == 'Oferta':
                                                                        newsd.Enlinea = kind.Enlinea
                                                                        newsd.IdCat = kind.IdCat
                                                                newsd.put()
                        except db.KindError:
                                errordict = {'error': -2, 'message': 'Kind ' + kind + ' couldn\'t be found. Careful it is case sensitive.'}
                                self.response.out.write(json.dumps(errordict))
                        except AttributeError:
                                errordict = {'error': -2, 'message': 'Kind ' + kindg+ ' doesn\'t have any attribute ' + field + '. Careful it is case sensitive.'}
                                self.response.out.write(json.dumps(errordict))

"""if __name__ == '__main__':
    _handlers = [
	(r'/_ah/start', generatesearch),
	, debug=True]
    run_wsgi_app(webapp.WSGIApplication(_handlers))"""
