import eventlet
from eventlet import hubs
eventlet.monkey_patch()

from deepsixPOC.dse import d6cage
from deepsixPOC.services import service_a
from deepsixPOC.services import service_b

def main():
    cage = d6cage.d6Cage()
    sa = service_a.serviceA()
    sb = service_b.serviceB()
    cage.createservice(sa, "service a")
    cage.createservice(sb, "service b")

    hubs.get_hub().switch()

if __name__=='__main__':
    main()
