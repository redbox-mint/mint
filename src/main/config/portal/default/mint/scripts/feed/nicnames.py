from au.edu.usq.fascinator.common import JsonConfigHelper
from au.edu.usq.fascinator.api.indexer import SearchRequest
from java.io import ByteArrayInputStream, ByteArrayOutputStream


class NicnamesData:
    def __init__(self):
        pass
    
    def __activate__(self, context):
        self.services = context["Services"]
        self.portalId = context["portalId"]
        self.formData = context["formData"]
        self.request = context["request"]
        self.request.setAttribute("Content-Type", "text/xml")
        self.__feed = self.__getFeed()
    
    def getFeed(self):
        return self.__feed
    
    def __getFeed(self):
        portal = self.services.getPortalManager().get(self.portalId)
        req = SearchRequest("*:*")
        req.setParam("facet", "true")
        req.setParam("rows", "1000")
        req.setParam("facet.field", portal.facetFieldList)
        req.setParam("facet.sort", "true")
        req.setParam("facet.limit", str(portal.facetCount))
        req.setParam("sort", "f_dc_title asc")
        req.setParam("fq", 'item_type:"object"')
        portalQuery = portal.getQuery()
        if portalQuery:
            req.addParam("fq", portalQuery)
        
        out = ByteArrayOutputStream()
        self.services.getIndexer().search(req, out)
        return JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
