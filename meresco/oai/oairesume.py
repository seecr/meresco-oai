from meresco.core import Transparant

class OaiResume(Transparant):

    def __init__(self):
        Transparant.__init__(self)
        self._suspended = []

    def begin(self):
        if self.ctx.tx.name == self._transation_name :
            self.ctx.tx.join(self)

    def addSuspend(self, suspend):
        self._suspended.append(suspend) 

    def commit(self):
        while len(self._suspended) > 0:
            self._suspended.pop().resume()
