import eventlet
from eventlet import greenthread
from eventlet import hubs
from eventlet import event
eventlet.monkey_patch()

import uuid


class deepSix(greenthread.GreenThread):
    def __init__(self, name, dataPath=None):
        hub = hubs.get_hub()
        greenthread.GreenThread.__init__(self, hub.greenlet)
        self.name=name
        self.running = True
        self.inbox = eventlet.Queue()
        self.dataPath = dataPath
        self.msgDispatcher = self._register_message_dispatcher()
        self.rpcrequests = {}

        hub.schedule_call_global(0, self.switch, self._loop, [], {})
    
    def send(self, msg):
        self.dataPath.put_nowait(msg)

    def receive(self, msg):
        if msg.type in self.msgDispatcher:
            self.msgDispatcher[msg.type](msg)
        else:
            assert False, "{} received message of unknown type {}: {}".format(
                    self.name, msg.type, str(msg))

    def sendmessage(self, target, content):
        msg = deepsix.d6msg(
                key=target, replyTo=self.name,
                type="test", dataindex="testmsg",
                body = {"content":content})
        self.send(msg)

    def rpcCall(self, timeout, key, method, *args, **kwargs):
        msg = d6msg(
                key=key, replyTo=self.name,
                type='rpc', dataindex=method,
                body={"args":args, "kwargs":kwargs})
        request_id = msg.corrId
        evt = event.Event()
        self.rpcrequests[request_id]=evt
        self.send(msg)
        try:
            with eventlet.Timeout(timeout):
                (ret, error) = evt.wait()
        except eventlet.Timeout:
            ret, error = None, ("RPC call time out: "
                                "{}.{}({},{}) from {}").format(
                                        key, 
                                        method, 
                                        args, 
                                        kwargs, 
                                        self.name)
        del self.rpcrequests[request_id]
        return ret, error

    def _register_message_dispatcher(self):
        dispatcher = {}
        dispatcher['test']=self.intest
        dispatcher['rpc']=self.inrpc
        dispatcher['rpcrep']=self.inrpcrep 
        return dispatcher

    def inrpc(self, msg):
        (method, args, kwargs) = (
                msg.dataindex,
                msg.body["args"],
                msg.body["kwargs"])
        try:
            method_ = getattr(self, method)
            ret, error = method_(*args, **kwargs), None
        except Exception as e:
            ret, error = None, e
        repmsg = d6msg(
                key=msg.replyTo, replyTo=self.name,
                corrId=msg.corrId, type='rpcrep',
                body={"return":ret, "error":error})
        self.send(repmsg)

    def inrpcrep(self, msg):
        request_id = msg.corrId
        if request_id in self.rpcrequests:
            self.rpcrequests[request_id].send((
                msg.body["return"], msg.body["error"]))

    def intest(self, msg):
        print "{} received a message {}".format(self.name, str(msg))

    def _loop(self):
        while self.running:
            if self.inbox:
                msg = self.inbox.get()
                self.receive(msg)
                self.inbox.task_done()
            else:
                eventlet.sleep(1)

class d6msg(object):
    def __init__(
            self, key="",
            replyTo="", corrId="",
            type="", dataindex="",
            body={}, srcmsg={}):
        self.header = {}
        self.body = body
        self.replyTo = replyTo
        self.type = type

        if srcmsg:
            self.key = srcmsg.replyTo
            self.dataindex = srcmsg.dataindex
            self.corrId = srcmsg.corrId
        else:
            self.key = key
            self.dataindex = dataindex
            if(corrId):
                self.corrId = corrId
            else:
                self.corrId = str(uuid.uuid4())

    def __str__(self):
        return ("<to:{}, from:{}, type:{}, corrId:{},"
                "dataindex:{}, body:{}>").format(
                        self.key, 
                        self.replyTo, 
                        self.type, 
                        self.corrId, 
                        self.dataindex, 
                        str(self.body))
