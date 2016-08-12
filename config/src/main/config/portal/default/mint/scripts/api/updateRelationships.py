import re
import traceback

from java.util import Date, HashMap, ArrayList
from java.lang import String, Integer, Long
from java.security import SecureRandom
from java.net import URLDecoder, URLEncoder
from java.io import File
from com.googlecode.fascinator.api.indexer import SearchRequest
from com.googlecode.fascinator.common.solr import SolrResult
from java.io import ByteArrayInputStream, ByteArrayOutputStream
from com.googlecode.fascinator.common.storage import StorageUtils
from com.googlecode.fascinator.api.storage import PayloadType
from com.googlecode.fascinator.common import JsonObject
from com.googlecode.fascinator.spring import ApplicationContextProvider
from com.googlecode.fascinator.common import BasicHttpClient
from org.apache.commons.httpclient.methods import GetMethod
from com.googlecode.fascinator.common import JsonSimple
from org.json.simple import JSONArray
from org.apache.commons.io import FileUtils
from com.googlecode.fascinator.common import FascinatorHome


class UpdateRelationshipsData():
    def __init__(self):
        pass

    def __activate__(self, context):
        self.None = context["log"]
        self.systemConfig = context["systemConfig"]
        self.sessionState = context["sessionState"]
        self.response = context["response"]
        self.request = context["request"]
        self.indexer = context["Services"].getIndexer()
        self.storage = context["Services"].getStorage()
        self.log = context["log"]

        self.sessionState.set("username","admin")
        self.writer = self.response.getPrintWriter("text/plain; charset=UTF-8")

        publishedRecords = self.findPublishedRecords()

        for publishedRecord in publishedRecords:
            self.log.info("processing record "+publishedRecord.getString(None,"storage_id"))
            digitalObject = StorageUtils.getDigitalObject(self.storage, publishedRecord.getString(None,"storage_id"))
            tfPackage = self.getTfPackage(digitalObject)
            metadata = digitalObject.getMetadata()
            configObject = StorageUtils.getDigitalObject(self.storage,metadata.getProperty("jsonConfigOid"))
            payload = configObject.getPayload(metadata.getProperty("jsonConfigPid"))
            inStream = payload.open()
            jsonConfig = JsonSimple(inStream)
            payload.close()
            requiredIdentifiers = jsonConfig.getArray("curation","requiredIdentifiers")

            if requiredIdentifiers is not None:
                pidName = self.systemConfig.getString(None,"curation","identifier-pids",requiredIdentifiers[0])
                pid = metadata.getProperty(pidName)
                identifier = tfPackage.getString(pid,"metadata","dc.identifier")
                relationships = tfPackage.getArray("relationships")
                if relationships is not None:
                    for relationship in relationships:
                        if relationship.get("broker") is None:  
                            if relationship.get("system") is not None and relationship.get("system") !=  self.systemConfig.getString(None,"system"):
                                self.notifyExternalRelationship(relationship,pid,relationship.get("system"),identifier)
                            else:
                                self.updateRelationships(relationship,pid,identifier)

        self.writer.close()
        self.sessionState.remove("username")

    def updateRelationships(self, relationship,pid,identifier):
        oid = self.findOidByIdentifier(relationship.get("identifier"))
        self.writer.println(oid)
        digitalObject = StorageUtils.getDigitalObject(self.storage, oid)
        metadataJsonPayload = digitalObject.getPayload("metadata.json")
        metadataJsonInstream = metadataJsonPayload.open()
        metadataJson = JsonSimple(metadataJsonInstream)
        metadataJsonPayload.close()
        relationships = metadataJson.getArray("relationships")


        found = False
        if relationships is None:
            relationships = JSONArray()
            metadataJson.getJsonObject().put("relationships",relationships)

        for relationship1 in relationships:
             if relationship1.get("identifier") == identifier:
                 relationship1.put("isCurated",True)
                 relationship1.put("curatedPid",pid)
                 found = True

        if not found:
            newRelationship = JsonObject()
            newRelationship.put("isCurated",True)
            newRelationship.put("curatedPid",pid)
            newRelationship.put("relationship",relationship.get("relationship"))
            newRelationship.put("identifier",identifier)
            relationships.add(newRelationship)


        istream = ByteArrayInputStream(String(metadataJson.toString(True)).getBytes())
        StorageUtils.createOrUpdatePayload(digitalObject,"metadata.json",istream)

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


    def notifyExternalRelationship(self, relationship,pid,system,identifier):
        try:
            url = self.systemConfig.getString(None, "curation","external-system-urls","notify-curation",system)
            self.writer.println(url+ "&relationship=isCollectedBy&curatedPid="+pid+"&identifier="+relationship.get("identifier")+"&system="+self.systemConfig.getString(None,"system")+"&sourceIdentifier="+identifier)
            client = BasicHttpClient(url+ "&relationship=isCollectedBy&curatedPid="+pid+"&identifier="+relationship.get("identifier")+"&system="+self.systemConfig.getString(None,"system")+"&sourceIdentifier="+identifier)
            get = GetMethod(url+ "&relationship=isCollectedBy&curatedPid="+pid+"&identifier="+relationship.get("identifier")+"&system="+self.systemConfig.getString(None,"system")+"&sourceIdentifier="+identifier)
            client.executeMethod(get)
            status = get.getStatusCode()
            if status != 200:
                text = get.getStatusText()
                self.log.error(String.format("Error accessing ReDBox: %s %s",status, text));
                return None;

        except Exception, ex:
            return None;

        # Return our results body
        response = None;
        try:
            response = get.getResponseBodyAsString();
        except Exception,ex:
            self.log.error("Error accessing response body: ", ex);
            return None;

        return JsonSimple(response);


    def findPublishedRecords(self):
        req = SearchRequest("published:\"true\"")
        out = ByteArrayOutputStream()
        self.indexer.search(req, out)
        solrResult = SolrResult(ByteArrayInputStream(out.toByteArray()))
        return solrResult.getResults()

    def getTfPackage(self,object):
        payload = None
        inStream = None

        #We don't need to worry about close() calls here
        try:
            payload = object.getPayload("metadata.json")
            inStream = payload.open()
        except Exception, e:
            self.log.error(traceback.format_exc())
            self.log.error("Error during package access", e)
            return None

        # The input stream has now been opened, it MUST be closed
        try:
            __tfpackage = JsonSimple(inStream)
            payload.close()
        except Exception, e:
            self.log.error("Error parsing package contents", e)
            payload.close()
        return __tfpackage
