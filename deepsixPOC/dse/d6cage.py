import eventlet
eventlet.monkey_patch()

from deepsixPOC.dse import deepsix
from deepsixPOC.dse import amqprouter

instances = {}
def singleton(class_):
    global instances
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

@singleton
class d6Cage(deepsix.deepSix):
    def __init__(self):
        name = "d6cage"
        deepsix.deepSix.__init__(
                self, name, dataPath=eventlet.Queue())
        self.table = amqprouter.routeTable()
        self.services = {}
        self._registerservice(self, "DeepSix Cage")
        self.router_greenthread = eventlet.spawn(self._router_loop)

    def createservice(self, svc, desc = "DeepSix Service"):
        self._registerservice(svc, desc)


    def _registerservice(self, svc, desc = ""):
        assert isinstance(svc, deepsix.deepSix)
        svc.dataPath = self.dataPath

        self.services[svc.name] = {}
        self.services[svc.name]['service'] = svc
        self.services[svc.name]['name'] = svc.name
        self.services[svc.name]['description'] = desc
        self.services[svc.name]['inbox'] = svc.inbox

        self.table.add(svc.name, svc.inbox)

    def _router_loop(self):
        while self.running:
            msg = self.dataPath.get()
            
            destinations = self.table.lookup(msg.key)
            if destinations:
                for destination in destinations:
                    destination.put_nowait(msg)

            self.dataPath.task_done()
