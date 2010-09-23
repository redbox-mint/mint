from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfig, JsonConfigHelper

from java.io import ByteArrayInputStream, ByteArrayOutputStream
from java.lang import Exception

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
        return self.getFormData("searchTerms", None)
    
    def getStartPage(self):
        #index = int(self.getStartIndex())
        #perPage = int(self.getItemsPerPage())
        return 0 #(index / perPage)
    
    def getResults(self):
        return self.__results
    
    def getValue(self, doc, field):
        values = doc.getList(field)
        if not values.isEmpty():
            return values.get(0)
        return ""

    def getValueList(self, doc, field):
        values = doc.getList(field)
        if not values.isEmpty():
            return '["%s"]' % '", "'.join(values)
        return '[]'
    
    def __getSolrData(self):
        prefix = self.getSearchTerms()
        if prefix:
            query = '%(prefix)s OR %(prefix)s*' % { "prefix" : prefix }
        else:
            query = "*:*"
        
        req = SearchRequest(query)
        req.addParam("fq", 'item_type:"object"')
        req.addParam("fq", 'repository_type:"SEO"')
        req.setParam("fl", "score")
        req.setParam("sort", "score desc")
        req.setParam("start", self.getStartIndex())
        req.setParam("rows", self.getItemsPerPage())
        level = self.getFormData("level", None)
        if level is not None:
            if level=="top":
                #query += " AND skos_hasTopConcept:http*"
                query += ' AND dc_identifier:"http://purl.org/anzsrc/seo/#division"'
            else:
                query += ' AND skos_broader:"%s"' % level
        
        try:
            out = ByteArrayOutputStream()
            indexer = self.services.getIndexer()
            indexer.search(req, out)
            return JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        except Exception, e:
            self.log.error("Failed to lookup '{}': {}", prefix, str(e))
        
        return JsonConfigHelper()
    
    def getFormData(self, name, default):
        value = self.formData.get(name)
        if value is None or value == "":
            return default
        return value

