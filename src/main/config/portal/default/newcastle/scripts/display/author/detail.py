from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfigHelper

from java.io import ByteArrayInputStream, ByteArrayOutputStream

class DetailData:
    def __activate__(self, context):
        self.services = context["Services"]
        self.metadata = context["metadata"]
        self.log = context["log"]
        self.log.info("metadata:{}", self.metadata)
        sessionState = context["sessionState"]
    
    def getRecords(self):
        req = SearchRequest('dc_creator:"%s"' % self.metadata.getList("dc_title").get(0))
        req.setParam("fq", 'recordtype:"marc"')
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "9999")
        
        # Make sure 'fq' has already been set in the session
        ##security_roles = self.authentication.get_roles_list();
        ##security_query = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        ##req.addParam("fq", security_query)
        
        out = ByteArrayOutputStream()
        indexer = self.services.getIndexer()
        indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        docs = result.getJsonList("response/docs")
        
        return docs
    
    def getAuthorities(self):
        req = SearchRequest('package_node_id:%s' % self.metadata.get("id"))
        req.setParam("fq", 'recordtype:"master"')
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "9999")
        
        # Make sure 'fq' has already been set in the session
        ##security_roles = self.authentication.get_roles_list();
        ##security_query = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        ##req.addParam("fq", security_query)
        
        out = ByteArrayOutputStream()
        indexer = self.services.getIndexer()
        indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        docs = result.getJsonList("response/docs")
        
        return docs
