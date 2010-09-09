from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfigHelper

from java.io import ByteArrayInputStream, ByteArrayOutputStream

class SuggestData:
    def __init__(self):
        pass
    
    def __activate__(self, context):
        self.page = context["page"]
        self.services = context["Services"]
        self.formData = context["formData"]
        self.request = context["request"]
        self.request.setAttribute("Content-Type", "application/x-suggestions+json")
    
    def getSuggestionPrefix(self):
        return self.formData.get("query")
    
    def getSearchTerms(self):
        searchTerms = []
        
        prefix = self.getSuggestionPrefix()
        query = '%(prefix)s OR %(prefix)s*' % { "prefix" : prefix }
        req = SearchRequest(query)
        req.addParam("fq", self.page.getPortal().getQuery())
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "50")
        req.setParam("fl", "score,id,dc_title")
        req.setParam("sort", "score desc")
        
        out = ByteArrayOutputStream()
        indexer = self.services.getIndexer()
        indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        docs = result.getJsonList("response/docs")
        for doc in docs:
            dc_title = doc.getList("dc_title").get(0)
            searchTerms.append(dc_title)
        
        return '", "'.join(searchTerms)
    

