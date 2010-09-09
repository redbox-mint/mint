from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfig, JsonConfigHelper

from java.io import ByteArrayInputStream, ByteArrayOutputStream
from java.lang import Exception
from java.util import ArrayList

class LookupData:
    def __init__(self):
        pass
    
    def __activate__(self, context):
        self.log = context["log"]
        self.services = context["Services"]
        self.portalId = context["portalId"]
        self.formData = context["formData"]
        self.request = context["request"]
        #self.request.setAttribute("Content-Type", "application/x-fascinator-lookup+json")
        self.request.setAttribute("Content-Type", "application/json")
        self.__solrData = self.__getSolrData()
        self.__results = self.__solrData.getJsonList("response/docs")
    
        jc = JsonConfig()
        baseUrl = jc.get("urlBase")
        if baseUrl.endswith("/"):
            baseUrl = baseUrl[:-1]
        self.__baseUrl = baseUrl
    
    def getBaseUrl(self):
        return self.__baseUrl + "/" + self.portalId
    
    def getLink(self):
        return ""
    
    def getTotalResults(self):
        return self.__solrData.get("response/numFound")
    
    def getStartIndex(self):
        return self.formData.get("startIndex", "0")
    
    def getItemsPerPage(self):
        return self.formData.get("count", "25")
    
    def getRole(self):
        return "request"
    
    def getSearchTerms(self):
        return self.formData.get("searchTerms", "")
    
    def getStartPage(self):
        index = int(self.getStartIndex())
        perPage = int(self.getItemsPerPage())
        return (index / perPage)
    
    def getResults(self):
        return self.__results
    
    def __getSolrData(self):
        prefix = self.getSearchTerms()
        query = 'dc_title:"%(prefix)s" OR dc_title:%(prefix)s*' % { "prefix" : prefix }
        query2 = 'package_node_title:"%(prefix)s" OR package_node_title:%(prefix)s*' % { "prefix" : prefix }
        
        req = SearchRequest("(%s)^2.5 OR (%s)^0.5" % (query, query2))
        req.addParam("fq", "recordtype:master")
        req.addParam("fq", 'item_type:"object"')
        req.setParam("fl", "score")
        req.setParam("sort", "score desc")
        req.setParam("start", self.getStartIndex())
        req.setParam("rows", self.getItemsPerPage())
        
        try:
            out = ByteArrayOutputStream()
            indexer = self.services.getIndexer()
            indexer.search(req, out)
            return JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        except Exception, e:
            self.log.error("Failed to lookup '{}': {}", prefix, str(e))
        
        return ArrayList

