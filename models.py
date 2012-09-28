from google.appengine.ext import db

class Cta(db.Model):
	Folio = db.IntegerProperty()
	Nombre = db.StringProperty()
	Apellidos = db.StringProperty()
	Puesto = db.StringProperty()
	Email = db.StringProperty()
	EmailAlt = db.StringProperty()
	Pass = db.StringProperty()
	Tel = db.StringProperty()
	Cel = db.StringProperty()
	FechaHora = db.DateTimeProperty()
	UsuarioInt = db.StringProperty()
	CodigoCfm = db.StringProperty()
	Status = db.BooleanProperty()

class Empresa(db.Model):
	IdEmp = db.StringProperty()
	Folio = db.IntegerProperty()
	RFC = db.StringProperty()
	Nombre = db.StringProperty()
	RazonSoc = db.StringProperty()
	DirCalle = db.StringProperty()
	DirCol = db.StringProperty()
	DirEnt = db.StringProperty()
	DirMun = db.StringProperty()
	DirCp = db.StringProperty()
	NumSuc = db.StringProperty()
	OrgEmp = db.StringProperty()
	OrgEmpOtro = db.StringProperty()
	OrgEmpReg = db.StringProperty()
	Url = db.StringProperty()
	Benef = db.IntegerProperty()
	PartLinea = db.IntegerProperty()
	ExpComer = db.IntegerProperty()
	Desc = db.StringProperty()
	FechaHora = db.DateTimeProperty()
	Status = db.BooleanProperty()

class Sucursal(db.Model):
        IdSuc = db.StringProperty()
        IdEmp = db.StringProperty()
        Nombre = db.StringProperty()
        Tel = db.StringProperty()
        DirCalle = db.StringProperty()
        DirCol = db.StringProperty()
        DirEnt = db.StringProperty()
        DirMun = db.StringProperty()
        DirCp = db.StringProperty()
        GeoUrl = db.StringProperty()
        Geo1 = db.StringProperty()
        Geo2 = db.StringProperty()
        Geo3 = db.StringProperty()
        Geo4 = db.StringProperty()
        FechaHora = db.DateTimeProperty()
	Latitud = db.FloatProperty()
	Longitud = db.FloatProperty()

class Oferta(db.Model):
	IdOft = db.StringProperty()
	IdEmp = db.StringProperty()
	IdCat = db.StringProperty()
	Oferta = db.StringProperty()
	Empresa = db.StringProperty()
	Descripcion = db.StringProperty()
	Codigo = db.StringProperty()
	Precio = db.StringProperty()
	Descuento = db.StringProperty()
	Enlinea = db.BooleanProperty()
	Url = db.StringProperty()
	Tarjetas = db.StringProperty()
	Meses = db.StringProperty()
	FechaHoraPub = db.DateTimeProperty()
	StatusPub = db.BooleanProperty()
	FechaHora = db.DateTimeProperty()

class OfertaSucursal(db.Model):
	IdOft = db.StringProperty()
	IdEmp = db.StringProperty()
	IdSuc = db.StringProperty()
	Empresa = db.StringProperty()
	Sucursal = db.StringProperty()
	lat = db.FloatProperty()
	lng = db.FloatProperty()
	Oferta = db.StringProperty()
	Descripcion = db.StringProperty()
	Precio = db.StringProperty()
        Descuento = db.StringProperty()
	Url = db.StringProperty()
	StatusPub = db.BooleanProperty()

class Categoria(db.Model):
	IdCat = db.IntegerProperty()
	Categoria = db.StringProperty()

class OfertaPalabra(db.Model):
	IdSuc = db.StringProperty()
	IdOft = db.StringProperty()
	Palabra = db.StringProperty()
