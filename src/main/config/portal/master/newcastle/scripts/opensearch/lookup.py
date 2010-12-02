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
        return self.getFormData("startIndex", "0")
    
    def getItemsPerPage(self):
        return self.getFormData("count", "25")
    
    def getRole(self):
        return "request"
    
    def getSearchTerms(self):
        return self.getFormData("searchTerms", "")
    
    def getStartPage(self):
        #index = int(self.getStartIndex())
        #perPage = int(self.getItemsPerPage())
        return 0#(index / perPage)
    
    def getResults(self):
        return self.__results
    
    def __getSolrData(self):
        prefix = self.getSearchTerms()
        if prefix:
            query = ""
            parts = prefix.split(" ")
            if len(parts) > 1:
                query = '(firstName:(%(firstName)s OR %(firstName)s*) AND surname:(%(surname)s OR %(surname)s*))^5.0 OR ' % { "firstName": parts[0], "surname": parts[1] }
            query1 = 'dc_title:"%(prefix)s" OR dc_title:%(prefix)s*' % { "prefix" : prefix }
            query2 = 'package_node_title:"%(prefix)s" OR package_node_title:%(prefix)s*' % { "prefix" : prefix }
            query += "(%s)^2.5 OR (%s)^0.5" % (query1, query2)
        else:
            query = "*:*"
        
        req = SearchRequest(query)
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
            result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
            print result
            return result
        except Exception, e:
            e.printStackTrace()
            self.log.error("Failed to lookup '{}': {}", prefix, str(e))
        
        return JsonConfigHelper()
    
    def getFormData(self, name, default):
        value = self.formData.get(name)
        if value is None or value == "":
            return default
        return value
    

