from com.googlecode.fascinator.common import JsonConfig

class DescriptionData:
    def __init__(self):
        pass
    
    def __activate__(self, context):
        self.request = context["request"]
        self.portalId = context["portalId"]
        self.request.setAttribute("Content-Type", "application/opensearchdescription+xml")
        
        jc = JsonConfig()
        baseUrl = jc.get("urlBase")
        if baseUrl.endswith("/"):
            baseUrl = baseUrl[:-1]
        self.__baseUrl = baseUrl
    
    def getBaseUrl(self):
        return self.__baseUrl + "/" + self.portalId
    

