import md5

from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfigHelper
from au.edu.usq.fascinator.portal.services import PortalManager

from java.io import ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader
from java.lang import Exception, String
from java.util import ArrayList, HashMap

from org.apache.commons.lang import StringEscapeUtils

class NameAuthorityData:
    def __activate__(self, context):
        self.log = context["log"]
        self.services = context["Services"]
        self.formData = context["formData"]
        self.response = context["response"]
        self.defaultPortal = context["defaultPortal"]
        self.__oid = self.formData.get("oid")
        self.log.info("oid={}", self.__oid)
        try:
            # get the package manifest
            self.__manifest = self.__readManifest(self.__oid)
            self.__metadata = self.__getMetadata(self.__oid)
        except Exception, e:
            self.log.error("Failed to load manifest: {}", e.getMessage());
            raise e
        
        try:
            func = self.formData.get("func")
            if func == "link-names":
                authorIds = self.formData.getValues("authorIds")
                print "Linking authors: ", authorIds
                details = self.__getAuthorDetails(authorIds)
                self.log.info(details.toString())
                for detail in details:
                    id = detail.get("id")
                    title = detail.getList("dc_title").get(0)
                    self.__manifest.set("manifest/node-%s/id" % id, id)
                    self.__manifest.set("manifest/node-%s/title" % id, title)
                self.log.info(self.__manifest.toString())
                self.__saveManifest(self.__oid)
        except Exception, e:
            result = '{ status: "error", message: "%s" }' % str(e)
            writer = self.response.getPrintWriter("application/json; charset=UTF-8")
            writer.println(result)
            writer.close()
    
    def __getAuthorDetails(self, authorIds):
        query = "AND id:".join(authorIds)
        req = SearchRequest('id:%s' % query)
        req.setParam("fq", 'recordtype:"author"')
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
        return result.getJsonList("response/docs")
    
    def isLinked(self, oid):
        node = self.__manifest.get("manifest/node-%s" % oid)
        self.log.info("manifest:{}", self.__manifest)
        self.log.info(" ******* nodeid: {}", node)
        return node is not None
    
    def getSuggestedNames(self):
        query = " OR dc_title:".join(self.getPackageTitle().split(" "))
        req = SearchRequest('dc_title:%s' % query)
        req.setParam("fq", 'recordtype:"author"')
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
        
        return result.getJsonList("response/docs")
    
    def __getMetadata(self, oid):
        req = SearchRequest('id:%s' % oid)
        req.setParam("fq", 'item_type:"object"')
        
        # Make sure 'fq' has already been set in the session
        ##security_roles = self.authentication.get_roles_list();
        ##security_query = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        ##req.addParam("fq", security_query)
        
        out = ByteArrayOutputStream()
        indexer = self.services.getIndexer()
        indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        self.log.info("result={}", result.toString())
        return result.getJsonList("response/docs").get(0)
    
    def getMetadata(self):
        return self.__metadata
    
    def getFormData(self, key):
        return self.formData.get(key, "")
    
    def getManifest(self):
        return self.__manifest.getJsonMap("manifest")
    
    def getPackageTitle(self):
        return StringEscapeUtils.escapeHtml(self.formData.get("title", self.__manifest.get("title")))
    
    def getMeta(self, metaName):
        return StringEscapeUtils.escapeHtml(self.formData.get(metaName, self.__manifest.get(metaName)))
    
    def getManifestViewId(self):
        searchPortal = self.__manifest.get("viewId", self.defaultPortal)
        if self.services.portalManager.exists(searchPortal):
            return searchPortal
        else:
            return self.defaultPortal
    
    def getMimeType(self, oid):
        return self.__getContentType(oid) or ""
    
    def getMimeTypeIcon(self, oid):
        #print " *** getMimeTypeIcon(%s)" % oid
        # check for specific icon
        contentType = self.__getContentType(oid)
        iconPath = "images/icons/mimetype/%s/icon.png" % contentType
        resource = self.services.getPageService().resourceExists(self.portalId, iconPath)
        if resource is not None:
            return iconPath
        elif contentType is not None and contentType.find("/") != -1:
            # check for major type
            iconPath = "images/icons/mimetype/%s/icon.png" % contentType[:contentType.find("/")]
            resource = self.services.getPageService().resourceExists(self.portalId, iconPath)
            if resource is not None:
                return iconPath
        # use default icon
        return "images/icons/mimetype/icon.png"
    
    def __getContentType(self, oid):
        #print " *** __getContentType(%s)" % oid
        contentType = ""
        if oid == "blank":
            contentType = "application/x-fascinator-blank-node"
        else:
            object = self.services.getStorage().getObject(oid)
            sourceId = object.getSourceId()
            payload = object.getPayload(sourceId)
            contentType = payload.getContentType()
            payload.close()
            object.close()
        return contentType
    
    def __readManifest(self, oid):
        object = self.services.getStorage().getObject(oid)
        sourceId = object.getSourceId()
        payload = object.getPayload(sourceId)
        payloadReader = InputStreamReader(payload.open(), "UTF-8")
        manifest = JsonConfigHelper(payloadReader)
        payloadReader.close()
        payload.close()
        object.close()
        return manifest
    
    def __addNode(self):
        print self.__manifest.toString()
        return "{}"
    
    def __saveManifest(self, oid):
        object = self.services.getStorage().getObject(oid)
        sourceId = object.getSourceId()
        manifestStr = String(self.__manifest.toString())
        object.updatePayload(sourceId,
                             ByteArrayInputStream(manifestStr.getBytes("UTF-8")))
        object.close()
