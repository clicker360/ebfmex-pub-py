from google.appengine.api import mail
from google.appengine.runtime import apiproxy_errors

#Mail sending class. Everything inside's fairly self explanatory
class sendmail:
	def __init__(self, receipient, subject, body):
		self.receipient = receipient
		self.subject = subject
		self.body = body
	def send(self):
		try:
			message = mail.EmailMessage()
			message.sender = "La Mala Noticia <thomas@clicker360.com>"
			message.to = self.receipient
			message.subject = self.subject
			message.body = self.body
			message.send()
		except apiproxy_errors.OverQuotaError, message:
			# Log the error.
			logging.error(message)
			# Display an informative message to the user.
			self.response.out.write('The email could not be sent. '
                          'Please try again later.')
