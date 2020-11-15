import logging

def getLogger(name):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        fmt='%(asctime)s %(levelname)-8s %(module)-18s %(funcName)-10s %(lineno)4s: %(message)s'
    ))
    log = logging.getLogger(name)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    return log

