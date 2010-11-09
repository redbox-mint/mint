from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfigHelper
from java.io import ByteArrayInputStream, ByteArrayOutputStream

class HomeData:
    def __init__(self):
        pass

    def __activate__(self, context):
        self.velocityContext = context

        action = self.vc("formData").get("verb")
        portalName = self.vc("formData").get("value")
        self.vc("sessionState").remove("fq")
        if action == "delete-portal":
            print " * home.py: delete portal %s" % portalName
            Services.portalManager.remove(portalName)
        self.__latest = JsonConfigHelper()
        self.__mine = JsonConfigHelper()
        self.__workflows = JsonConfigHelper()
        self.__result = JsonConfigHelper()
        self.__search()

    # Get from velocity context
    def vc(self, index):
        if self.velocityContext[index] is not None:
            return self.velocityContext[index]
        else:
            print "ERROR: Requested context entry '" + index + "' doesn't exist"
            return None

    def __search(self):
        indexer = Services.getIndexer()
        portalQuery = Services.getPortalManager().get(self.vc("portalId")).getQuery()
        portalSearchQuery = Services.getPortalManager().get(self.vc("portalId")).getSearchQuery()
        
        # Security prep work
        current_user = self.vc("page").authentication.get_username()
        security_roles = self.vc("page").authentication.get_roles_list()
        security_filter = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        security_exceptions = 'security_exception:"' + current_user + '"'
        owner_query = 'owner:"' + current_user + '"'
        security_query = "(" + security_filter + ") OR (" + security_exceptions + ") OR (" + owner_query + ")"
        isAdmin = self.vc("page").authentication.is_admin()

        req = SearchRequest("last_modified:[NOW-1MONTH TO *]")
        req.setParam("fq", 'item_type:"object"')
        req.addParam("fq", "workflow_modified:false")
        if portalQuery:
            req.addParam("fq", portalQuery)
        if portalSearchQuery:
            req.addParam("fq", portalSearchQuery)
        req.setParam("rows", "10")
        req.setParam("sort", "last_modified desc, f_dc_title asc");
        if not isAdmin:
            req.addParam("fq", security_query)
        out = ByteArrayOutputStream()
        indexer.search(req, out)
        self.__latest = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        req = SearchRequest("*:*")
        req.setParam("fq", 'item_type:"object"')
        if portalQuery:
            req.addParam("fq", portalQuery)
        if portalSearchQuery:
            req.addParam("fq", portalSearchQuery)
        req.setParam("facet", "true")
        req.setParam("facet.field", "workflow_step")
        req.setParam("fl", "id")
        req.setParam("rows", "1")
        if not isAdmin:
            req.addParam("fq", security_query)
        out = ByteArrayOutputStream()
        indexer.search(req, out)
        
        #self.vc("sessionState").set("fq", 'item_type:"object"')
        #sessionState.set("query", portalQuery.replace("\"", "'"))
        
        self.__result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
    
    def getLatest(self):
        return self.__latest.getList("response/docs")
    
    def getItemCount(self):
        return self.__result.get("response/numFound")
    
    def getFirstOid(self):
        docs = self.__result.getList("response/docs")
        if not docs.isEmpty():
            return docs.get(0).get("id")
        return None
    
    def getCompletedCount(self):
        wfStep = self.__result.getList("facet_counts/facet_fields/workflow_step")
        confirmed = 0
        for i in range(len(wfStep)):
            if wfStep[i] == "live":
                return wfStep[i+1]
        return 0
    
