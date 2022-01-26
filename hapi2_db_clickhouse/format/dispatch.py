import json
from hapi2.config import VARSPACE
from hapi2.format.dispatch import FormatDispatcher

FIELD_DEFAULTS = {
    int: 0,
    float: 0.0,
    str: '',
}

def assign_defaults_json(header,stream):
    """ Assign default values for missing parameters or 
        inserting into Clickhouse """
    clsname = header['content']['class']
    #cls = getattr(models,clsname)
    cls = getattr(VARSPACE['db_backend'].models,clsname)
    for item in stream:
        for key,meta in cls.__keys__:
            if key not in item or item[key] is None:
                item[key] = FIELD_DEFAULTS[meta['type']]
        yield item

def assign_defaults_dotpar(header,stream):
    """ Assign default values for missing parameters or 
        inserting into Clickhouse """
    for item in stream:
        if 'extra' in item:
            item['extra'] = json.dumps(item['extra'])
        yield item

class FormatDispatcher_JSON(FormatDispatcher):
    
    """ Make a child class from FormatDispatcher implementing the
        additional field preparations for Clickhouse """
    
    def getStreamer(self,basedir,header):
        fmt = header['content']['format']
        try:
            stream = self.__REGISTERED_STREAMERS__[fmt](
                basedir=basedir,header=header)
            return assign_defaults_json(header,stream)
        except KeyError:
            raise Exception('unknown format "%s"'%fmt)

class FormatDispatcher_Dotpar(FormatDispatcher):
    
    """ Make a child class from FormatDispatcher implementing the
        additional field preparations for Clickhouse """
    
    def getStreamer(self,basedir,header):
        fmt = header['content']['format']
        try:
            stream = self.__REGISTERED_STREAMERS__[fmt](
                basedir=basedir,header=header)
            return assign_defaults_dotpar(header,stream)
        except KeyError:
            raise Exception('unknown format "%s"'%fmt)
