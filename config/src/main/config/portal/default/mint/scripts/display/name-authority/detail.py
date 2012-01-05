#import sys
#print sys.path
#from display.default.result import ResultData
#print " ******", dir(ResultData)
#from display.default.detail import DetailData as DefaultDetailData

#class DetailData(DefaultDetailData):
#    def __activate__(self, context):
#        DefaultDetailData.__activate__(self, context)
#    

from com.googlecode.fascinator.common import JsonConfigHelper

from java.io import InputStreamReader

class DetailData:
    def __activate__(self, context):
        self.services = context["Services"]
        self.formData = context["formData"]
        self.metadata = context["metadata"]
        
        oid = self.metadata.get("id")
        manifest = self.__readManifest(oid)
        self.__manifest = manifest.getJsonMap("manifest")
    
    def getManifest(self):
        return self.__manifest
    
    def __readManifest(self, oid):
        object = self.services.getStorage().getObject(oid)
        sourceId = object.getSourceId()
        payload = object.getPayload(sourceId)
        payloadReader = InputStreamReader(payload.open(), "UTF-8")
        manifest = JsonConfigHelper(payloadReader)
        payloadReader.close()
        payload.close()
        object.close()
        return manifest
    
