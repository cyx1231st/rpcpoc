import eventlet
eventlet.monkey_patch()

from deepsixPOC.dse import deepsix

class serviceB(deepsix.deepSix):
    def __init__(self):
        super(serviceB, self).__init__("serviceb")
        eventlet.spawn(self._service_loop)
        self._counter=0
    
    def _service_loop(self):
        while self.running:
            eventlet.sleep(7)

    def addCount(self):
        self._counter+=1
        eventlet.sleep(1)
        return self._counter


