from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common.solr import SolrResult

from java.io import ByteArrayInputStream, ByteArrayOutputStream
from java.lang import Exception

class LookupData:
    def __activate__(self, context):
        self.log = context["log"]
        self.services = context["Services"]
        self.portalId = context["portalId"]
        self.formData = context["formData"]
        
        request = context["request"]
        request.setAttribute("Content-Type", "application/json")
        
        self.__solrData = self.__getSolrData()
        self.__results = self.__solrData.getResults()
        
        baseUrl = context["systemConfig"].getString("", ["urlBase"])
        if baseUrl.endswith("/"):
            baseUrl = baseUrl[:-1]
        self.__baseUrl = baseUrl
    
    def getBaseUrl(self):
        return self.__baseUrl + "/" + self.portalId
    
    def getLink(self):
        return ""
    
    def getTotalResults(self):
        return self.__solrData.getNumFound()
    
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
        return 0 #(index / perPage)
    
    def getResults(self):
        return self.__solrData.getResults()
    
    def getValue(self, doc, field):
        return doc.getFirst(field)
    
    def getValueList(self, doc, field):
        return '["%s"]' % '", "'.join(doc.getList(field)) + ""
    
    def __getSolrData(self):
        prefix = self.getSearchTerms()
        if prefix != "":
            query = 'dc_title:("%(prefix)s" OR "%(prefix)s*")' % { "prefix" : prefix }
        else:
            query = "*:*"
        
        portal = self.services.portalManager.get(self.portalId)
        if portal.searchQuery != "*:*":
            query = query + " AND " + portal.searchQuery
        req = SearchRequest(query)
        req.setParam("fq", 'item_type:"object"')
        if portal.query:
            req.addParam("fq", portal.query)
        req.setParam("fl", "score")
        req.setParam("sort", "score desc, f_dc_title asc")
        req.setParam("start", self.getStartIndex())
        req.setParam("rows", self.getItemsPerPage())
        
        try:
            out = ByteArrayOutputStream()
            indexer = self.services.getIndexer()
            indexer.search(req, out)
            return SolrResult(ByteArrayInputStream(out.toByteArray()))
        except Exception, e:
            self.log.error("Failed to lookup '{}': {}", prefix, e.getMessage())
        
        return SolrResult('{ "response": { "numFound": 0 } }')
    
    def getFormData(self, name, default):
        value = self.formData.get(name)
        if value is None or value == "":
            return default
        return value
