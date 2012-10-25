import random, string

def randLetter():
        alpha = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvXxYyZz'
        randnum = random.randrange(len(alpha))
        letter = alpha[randnum]
        return letter

def randString(nb,digits=True):
        rands = None
        if digits:
                rands = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for x in range(nb))
        else:
                rands = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for x in range(nb))
        return rands
