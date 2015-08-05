import eventlet
eventlet.monkey_patch()

from deepsixPOC.dse import deepsix

class serviceA(deepsix.deepSix):
    def __init__(self):
        super(serviceA, self).__init__("servicea")
        eventlet.spawn(self._service_loop)
        self._count=0

    def _service_loop(self):
        while self.running:
            # self.sendmessage("serviceb", "greetings from A")
            print "{} rpc attempt No.{}".format(
                    self.name, self._count)
            self._count+=1
            # rpc
            ret, error = self.rpcCall(3, "serviceb", "addCount")
            print "{} rpc result: {}, error: {}".format(
                    self.name, ret, error)
            eventlet.sleep(2)
