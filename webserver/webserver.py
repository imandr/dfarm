from webpie import WPApp, WPHandler, Response
from dfarm_api import DiskFarmClient
from dfconfig import DFConfig

class Handler(WPHandler):
    
    def data(self, request, relpath, **args):
        method = request.method
        
        if method.upper() == "GET":
            return self.data_get(equest, relpath, **args)
        elif method.upper() in ("POST", "PUT"):
            return self.data_put(equest, relpath, **args)

            
    def data_get(self, request, relpath, **args):
        dfarm_client = self.App.DiskFarmClient

        info, err = dfarm_client.getInfo(relpath)
        if not info:
            return Response(err, status=400)
        if info.Type != 'f':
            return Response("Not a file", status=403)

        resp = Response(body_file = dfarm_client.open(info, 'r', tmo=10)))
        if info.Size is not None:
            resp.headers["Content-Length"] = info.Size
        return resp
        
    def data_put(self, request, relpath, ncopies=1, **args):
        dfarm_client = self.App.DiskFarmClient

        info = dfarm_client.fileInfo(relpath, None, size = int(request.headers["Content-Length"]))
        lpath = info.Path
        info, err = dfarm_client.createFile(info, ncopies)
        if not info:
            return err, 500
            
        ok, status = dfarm_client.put(info, request.body_file, ncopies = ncopies, tmo=10)
        if ok:
            return "OK"
        else:
            return status, 500
            
class App(WPApp):
    
    def __init__(self, root_class):
        WPApp.__init__(root_class)
        cfg = DFConfig(os.environ['DFARM_CONFIG'])
        self.DiskFarmClient = DiskFarmClient(cfg)
        
        
application = App(Handler)
        
        
            



        