import md5

from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfigHelper
from au.edu.usq.fascinator.portal.services import PortalManager

from java.io import ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader
from java.lang import Exception, String
from java.util import ArrayList, HashMap, HashSet, LinkedHashMap

from org.apache.commons.lang import StringEscapeUtils

class NameAuthorityData:
    def __activate__(self, context):
        self.log = context["log"]
        self.services = context["Services"]
        self.formData = context["formData"]
        self.response = context["response"]
        self.defaultPortal = context["defaultPortal"]
        self.__oid = self.formData.get("oid")
        try:
            # get the package manifest
            self.__manifest = self.__readManifest(self.__oid)
            self.__metadata = self.__getMetadata(self.__oid)
        except Exception, e:
            self.log.error("Failed to load manifest: {}", e.getMessage());
            raise e
        
        result = None
        try:
            func = self.formData.get("func")
            if func == "link-names":
                ids = self.formData.getValues("ids")
                records = self.__getAuthorDetails(ids)
                for record in records:
                    id = record.get("id")
                    name = record.getList("dc_title").get(0)
                    title = record.getList("dc_description").get(0)
                    handle = record.getList("handle").get(0)
                    hash = self.getHash(name)
                    self.__manifest.set("manifest/node-%s/title" % (hash), name)
                    self.__manifest.set("manifest/node-%s/children/node-%s/id" % (hash, id), id)
                    self.__manifest.set("manifest/node-%s/children/node-%s/title" % (hash, id), title)
                    if handle:
                        self.__manifest.set("manifest/node-%s/children/node-%s/handle" % (hash, id), handle)
                self.__saveManifest(self.__oid)
                result = '{ status: "ok" }'
            elif func == "unlink-names":
                ids = self.formData.getValues("ids")
                for id in ids:
                    self.__manifest.removePath("manifest/node-%s" % id)
                result = '{ status: "ok" }'
                self.__saveManifest(self.__oid)
            #self.log.info(self.__manifest.toString())
        except Exception, e:
            result = '{ status: "error", message: "%s" }' % str(e)
        if result:
            writer = self.response.getPrintWriter("application/json; charset=UTF-8")
            writer.println(result)
            writer.close()
    
    def getHash(self, data):
        return md5.new(data).hexdigest()
    
    def __getAuthorDetails(self, authorIds):
        query = " OR id:".join(authorIds)
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
        node = self.__manifest.get("manifest//node-%s" % oid)
        #self.log.info("manifest:{}", self.__manifest)
        #self.log.info(" ******* nodeid: {}", node)
        return node is not None
    
    def getSuggestedNames(self):
        # search common forms
        lookupNames = []
        surname = self.__metadata.getList("surname").get(0)
        firstName = self.__metadata.getList("firstName").get(0)
        firstInitial = firstName[0].upper()
        secondName = self.__metadata.getList("secondName")
        if not secondName.isEmpty():
            secondName = secondName.get(0)
        if secondName and secondName != "":
            secondInitial = secondName[0].upper()
            lookupNames.append("%s, %s. %s." % (surname, firstInitial, secondInitial))
            lookupNames.append("%s, %s %s." % (surname, firstName, secondInitial))
            lookupNames.append("%s, %s %s" % (surname, firstName, secondName))
            lookupNames.append("%s %s %s" % (firstName, secondName, surname))
        lookupNames.append("%s, %s." % (surname, firstInitial))
        lookupNames.append("%s, %s" % (surname, firstName))
        lookupNames.append("%s %s" % (firstName, surname))
        query = '" OR dc_title:"'.join(lookupNames)
        
        # general word search from each part of the name
        parts = [p for p in self.getPackageTitle().split(" ") if len(p) > 0]
        query2 = " OR dc_title:".join(parts)
        
        req = SearchRequest('(dc_title:"%s")^2.5 OR (dc_title:%s)^0.5' % (query, query2))
        self.log.info("suggestedNames query={}", req.query)
        req.setParam("fq", 'recordtype:"author"')
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "9999")
        req.setParam("fl", "score")
        req.setParam("sort", "score desc")
        
        # Make sure 'fq' has already been set in the session
        ##security_roles = self.authentication.get_roles_list();
        ##security_query = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        ##req.addParam("fq", security_query)
        
        out = ByteArrayOutputStream()
        indexer = self.services.getIndexer()
        indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        #self.log.info("result={}", result.toString())
        docs = result.getJsonList("response/docs")
        
        map = LinkedHashMap()
        for doc in docs:
            authorName = doc.getList("dc_title").get(0)
            if map.containsKey(authorName):
                authorDocs = map.get(authorName)
            else:
                authorDocs = ArrayList()
                map.put(authorName, authorDocs)
            authorDocs.add(doc)
        
        self.__maxScore = max(1.0, float(result.get("response/maxScore")))
        
        return map
    
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
        #self.log.info("result={}", result.toString())
        return result.getJsonList("response/docs").get(0)
    
    def getRank(self, score):
        return "%.2f" % (min(1.0, float(score)) * 100)
    
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
