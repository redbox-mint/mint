from com.googlecode.fascinator.common import JsonSimple
from com.googlecode.fascinator.portal.services import ScriptingServices
from com.googlecode.fascinator.spring import ApplicationContextProvider
from com.googlecode.fascinator.common import JsonObject
from com.googlecode.fascinator.common.storage import StorageUtils
from java.io import ByteArrayInputStream, ByteArrayOutputStream
from com.googlecode.fascinator.api.indexer import SearchRequest
from com.googlecode.fascinator.common.solr import SolrResult

class GetRelationshipsData:

    def __init__(self):
        pass

    def __activate__(self, context):

         try:
             self.log = context["log"]
             self.response = context["response"]
             self.request = context["request"]
             self.systemConfig = context["systemConfig"]
             self.storage = context["Services"].getStorage()
             self.indexer = context["Services"].getIndexer()
             self.sessionState = context["sessionState"]
             self.sessionState.set("username", "admin")

             out = self.response.getPrintWriter("text/plain; charset=UTF-8")
             relationshipMapper = ApplicationContextProvider.getApplicationContext().getBean("relationshipMapper")
             externalCurationMessageBuilder = ApplicationContextProvider.getApplicationContext().getBean("externalCurationMessageBuilder")

             oid = self.request.getParameter("oid")

             if oid is None :
                 identifier = self.request.getParameter("identifier")
                 oid = self.findOidByIdentifier(identifier)


             relationshipMap = relationshipMapper.getRelationshipMap(oid)
             relationshipMapJsonObject = externalCurationMessageBuilder.buildMessage(relationshipMap)
             out.println(relationshipMapJsonObject.toString(True))
             out.close()
         finally:
             self.sessionState.remove("username")


    def findOidByIdentifier(self, identifier):
        query = "known_ids:\"" + identifier + "\"";

        request = SearchRequest(query);
        out = ByteArrayOutputStream();

        # Now search and parse response
        result = None;
        try:
            self.indexer.search(request, out);
            inputStream = ByteArrayInputStream(out.toByteArray());
            result = SolrResult(inputStream);
        except Exception, ex:
            self.log.error("Error searching Solr: ", ex);
            raise ex
            return None;

        # Verify our results
        if (result.getNumFound() == 0) :
            self.log.error("Cannot resolve ID '{}'", identifier);
            return None;

        if (result.getNumFound() > 1) :
            self.log.error("Found multiple OIDs for ID '{}'", identifier);
            return None;


        doc = result.getResults().get(0)
        return doc.getFirst("storage_id")
    
    def getObjectMeta(self, oid):
        digitalObject = StorageUtils.getDigitalObject(self.storage, oid)
        return digitalObject.getMetadata()

    def getObjectMetaJson(self, objectMeta):
        objMetaJson = JsonObject()
        propertyNames = objectMeta.stringPropertyNames()
        for propertyName in propertyNames:
            objMetaJson.put(propertyName, objectMeta.get(propertyName))
        return objMetaJson
