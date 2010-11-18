import md5

from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfigHelper
from au.edu.usq.fascinator.portal.services import PortalManager

from java.io import ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader
from java.lang import Exception, String, Boolean, Integer
from java.util import ArrayList, Collections, HashMap, HashSet, LinkedHashMap, TreeMap

from org.apache.commons.lang import StringEscapeUtils
from org.apache.commons.collections import ListUtils

class NameAuthorityData:
    def __activate__(self, context):
        self.log = context["log"]
        self.services = context["Services"]
        self.formData = context["formData"]
        self.response = context["response"]
        self.defaultPortal = context["defaultPortal"]
        self.sessionState = context["sessionState"]
        self.portalId = context["portalId"]
        
        self.__oid = self.formData.get("oid")
        self.__indexer = self.services.getIndexer()
        
        try:
            # get the package manifest
            self.__manifest = self.__readManifest(self.__oid)
            self.__metadata = self.__getMetadata(self.__oid)
            self.__workflowMetadata = self.__readWorkflowMetadata(self.__oid)
        except Exception, e:
            self.log.error("Failed to load manifest: {}", e.getMessage());
            raise e
        
        self.__oidList, self.__nameList = self.__getNavData() 
        self.__unEditedOidList, self.__unEditedNameList = self.__getNavDataUnedited()
        result = None
        try:
            func = self.formData.get("func")
            if func == "link-citation-current-object":
                ids = self.formData.getValues("ids")
                records = self.__getAuthorDetails(ids)
                result = self.__linkNames(records)
            elif func == "unlink-citation-current-object":
                ids = self.formData.getValues("ids")
                result = self.__unlinkCitationForCurrentObject(ids)
            elif func == "search-names":
                query = self.formData.get("query")
                result = self.__searchNames(query)
            elif func == "unlink-citation":
                id = self.formData.getValues("id")
                result = self.__unlinkCitation(id)
        except Exception, e:
            result = '{ status: "error", message: "%s" }' % str(e)
        if result:
            writer = self.response.getPrintWriter("application/json; charset=UTF-8")
            writer.println(result)
            writer.close()
            
    
    def __unlinkCitationForCurrentObject(self, ids):
        # link all records in ids
        if ids:
            records = self.__getAuthorDetails(ids)
            self.__linkNames(records)
        else:
            ids = []
        
        # remove all current link that is not in ids
        nodeList = self.__manifest.getMap("//children")
        for node in nodeList:
            if node[5:] not in ids:
                self.__manifest.removePath("//children/%s" % node)
        
        # check if children node is empty, if yes, remove parent
        nodeList = self.__manifest.getMap("//manifest")
        for node in nodeList:
            childList = self.__manifest.getList("//%s/children" % node)
            for child in childList:
                if child.isEmpty():
                    self.__manifest.removePath("manifest/%s" % node)
        
        self.__saveManifest(self.__oid)
        self.__workflowMetadata.set("modified", "true")
        self.__saveWorkflowMetadata(self.__oid)
        return '{ status: "ok" }'
    
    def __unlinkCitation(self, ids):
        records = self.__getAuthorityRecord(ids)
        
        for record in records:
            authorityStorageId = record.get("storage_id")
            manifest = self.__readManifest(authorityStorageId)
            for id in ids:
                manifest.removePath("//children/node-%s" % id)
                
            # check if children node is empty, if yes, remove parent
            nodeList = manifest.getMap("//manifest")
            for node in nodeList:
                childList = manifest.getList("//%s/children" % node)
                for child in childList:
                    if child.isEmpty():
                        manifest.removePath("manifest/%s" % node)
            self.__saveManifest(authorityStorageId, manifest)
            self.__indexer.index(authorityStorageId)
            self.__indexer.commit()
            
        idList = [str("%s" % item) for item in ids]
        map = {}
        map["a"]=idList
        return map
    
    def getHash(self, data):
        return md5.new(data).hexdigest()
    
    def __getAuthorDetails(self, citationIds):
        query = " OR id:".join(citationIds)
        req = SearchRequest('id:%s' % query)
        req.setParam("fq", 'recordtype:"author"')
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "9999")
        
        # Make sure 'fq' has already been set in the session
        ##security_roles = self.authentication.get_roles_list();
        ##security_query = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        ##req.addParam("fq", security_query)
        
        out = ByteArrayOutputStream()
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        return result.getJsonList("response/docs")
    
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
        
        #filter out the linked citation
        linkedCitations = self.__manifest.getList("//children//id")
        query3 = ""
        if linkedCitations:
            query3 = " OR ".join(linkedCitations)
            query3 = " AND -id:(%s)" % query3
        
        req = SearchRequest('(dc_title:"%s")^2.5 OR (dc_title:%s)^0.5%s' % (query, query2, query3))
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
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        #self.log.info("result={}", result.toString())
        docs = result.getJsonList("response/docs")
        
        exactMatchRecords = LinkedHashMap()
        map = LinkedHashMap()
        
        idList = []
        
        for doc in docs:
            authorName = doc.getList("dc_title").get(0)
            rank = self.getRank(doc.getList("score").get(0))
            id = doc.get("id")
            idList.append(id)
            #try to do automatch
            if float(rank) == 100.00 and self.isModified() == "false":
                if exactMatchRecords.containsKey(authorName):
                    authorMatchDocs = exactMatchRecords.get(authorName)
                else:
                    authorMatchDocs = ArrayList()
                    exactMatchRecords.put(authorName, authorMatchDocs)
                authorMatchDocs.add(doc)
            elif id not in linkedCitations:
                if map.containsKey(authorName):
                    authorDocs = map.get(authorName)
                else:
                    authorDocs = ArrayList()
                    map.put(authorName, authorDocs)
                authorDocs.add(doc)
        
        self.__maxScore = max(1.0, float(result.get("response/maxScore")))
        
        if idList:
            self.__isLinked(idList, map)
        
        # Do not auto save if record is live
        if self.__workflowMetadata.get("modified") == "false":
            self.__autoSaveExactRecord(exactMatchRecords)
        
        return map
    
    def __linkNames(self, records):
        self.__savingLinkRecord(records)
        self.__workflowMetadata.set("modified", "true")
        self.__saveWorkflowMetadata(self.__oid)
        return '{ status: "ok" }'
    
    def __autoSaveExactRecord(self, map):
        if map:
            for authorName in map.keySet():
                authorDocs = map.get(authorName)
                self.__savingLinkRecord(authorDocs)
                
    def __savingLinkRecord(self, docs):
        for doc in docs:
            id = doc.get("id")
            name = doc.getList("dc_title").get(0)
            title = doc.getList("dc_description").get(0)
            handle = doc.getList("handle").get(0)
            faculty = doc.getList("faculty").get(0)
            school = doc.getList("school").get(0)
            hash = self.getHash(name)
            
            self.__manifest.set("manifest/node-%s/title" % (hash), name)
            self.__manifest.set("manifest/node-%s/automatch" % (hash), "true")
            self.__manifest.set("manifest/node-%s/children/node-%s/id" % (hash, id), id)
            self.__manifest.set("manifest/node-%s/children/node-%s/title" % (hash, id), title)
            if handle:
                self.__manifest.set("manifest/node-%s/children/node-%s/handle" % (hash, id), handle)
            if faculty:
                self.__manifest.set("manifest/node-%s/children/node-%s/faculty" % (hash, id), faculty)
            if school:
                self.__manifest.set("manifest/node-%s/children/node-%s/school" % (hash, id), school)
        self.__saveManifest(self.__oid)
        
    
    def __getMetadata(self, oid):
        req = SearchRequest('id:%s' % oid)
        req.setParam("fq", 'item_type:"object"')
        
        # Make sure 'fq' has already been set in the session
        ##security_roles = self.authentication.get_roles_list();
        ##security_query = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        ##req.addParam("fq", security_query)
        
        out = ByteArrayOutputStream()
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        #self.log.info("result={}", result.toString())
        return result.getJsonList("response/docs").get(0)
    
    def isModified(self):
        return self.__workflowMetadata.get("modified") 
    
    def getRank(self, score):
        return "%.2f" % (min(1.0, float(score)) * 100)
    
    def getCitationAffiliation(self, author):
        faculties = [f for f in author.getList("faculty") if f != ""]
        schools = [s for s in author.getList("school") if s != ""]
        all = ListUtils.union(faculties, schools)
        if len(all) > 0:
            return ", ".join(all)
        return None
    
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
    
    def __readWorkflowMetadata(self, oid):
        object = self.services.getStorage().getObject(oid)
        sourceId = object.getSourceId()
        payload = object.getPayload("workflow.metadata")
        payloadReader = InputStreamReader(payload.open(), "UTF-8")
        metadata = JsonConfigHelper(payloadReader)
        payloadReader.close()
        payload.close()
        object.close()
        return metadata
    
    def __saveManifest(self, oid, manifest=None):
        object = self.services.getStorage().getObject(oid)
        sourceId = object.getSourceId()
        if manifest is None:
            manifest = self.__manifest
        manifestStr = String(manifest.toString())
        object.updatePayload(sourceId,
                             ByteArrayInputStream(manifestStr.getBytes("UTF-8")))
        object.close()
        
    def __saveWorkflowMetadata(self, oid):
        object = self.services.getStorage().getObject(oid)
        manifestStr = String(self.__workflowMetadata.toString())
        object.updatePayload("workflow.metadata",
                             ByteArrayInputStream(manifestStr.getBytes("UTF-8")))
        object.close()
        
        self.__indexer.index(oid)
        self.__indexer.commit()

    def __searchNames(self, searchText):
        # search common forms
        lookupNames = []
        
        req = SearchRequest('(dc_title:"%s")^2.5' % searchText)
        self.log.info("searchNames query={}", req.query)
        req.setParam("fq", 'recordtype:"author"')
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "9999")
        req.setParam("fl", "score")
        req.setParam("sort", "score desc")
        
        out = ByteArrayOutputStream()
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        docs = result.getJsonList("response/docs")
        
        #Using this map because velocity gave error if LinkHashedMap is used
        map={}
        idList = []
        count = 0
        for doc in docs:
            authorName = str(doc.getList("dc_title").get(0))
            idList.append(doc.get("id"))
            if map.has_key(authorName):
                docsDic = map.get(authorName)
            else:
                docsDic = {}
                map[authorName] = docsDic
            #hash storageId and authorName
            doc.set("authorHash", self.getHash(authorName))
            doc.set("storageHash", self.getHash(doc.get("storage_id")))
            doc.set("affiliation", self.getCitationAffiliation(doc))
            ##doc.set("linked", Boolean.toString(linked))
            docsDic["%s" % count] = doc
            count +=1
        
        if idList:
            self.__isLinked(idList, map)
        return map
    
    def __isLinked(self, ids, map):
        query = 'package_node_id:("' + '" OR "'.join(ids) + '")'
        req = SearchRequest(query)
        req.setParam("fq", 'recordtype:"master"')
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "9999")
        
        out = ByteArrayOutputStream()
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        currentList = []
        for doc in result.getJsonList("response/docs"):
            currentList.extend(doc.getList("package_node_id"))
        
        if type(map).__name__ == "LinkedHashMap":
            for author in map.keySet():
                authorDocs = map.get(author)
                for doc in authorDocs:
                    if doc.get("id") in currentList:
                        doc.set("linked", "true")
        else:
            for author in map.keys():
                authorList = map[author]
                for count in authorList:
                    doc = authorList[count]
                    if doc.get("id") in currentList:
                        doc.set("linked", "true")
    
    def __getAuthorityRecord(self, ids):
        query = 'package_node_id:("' + '" OR "'.join(ids) + '")'
        req = SearchRequest(query)
        req.setParam("fq", 'recordtype:"master"')
        req.addParam("fq", 'item_type:"object"')
        req.setParam("rows", "9999")
        
        # Make sure 'fq' has already been set in the session
        ##security_roles = self.authentication.get_roles_list();
        ##security_query = 'security_filter:("' + '" OR "'.join(security_roles) + '")'
        ##req.addParam("fq", security_query)
        
        out = ByteArrayOutputStream()
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        
        return result.getJsonList("response/docs")
    
    def getAffiliation(self):
        map = TreeMap()
        
        authors = self.__workflowMetadata.getList("authors")
        
        list = []
        #sort base on the expired date
        for author in authors:
            map.put("%s %s %s" % (author.get("expiry"), author.get("author"), author.get("orgUnitId")), author)
        
        return map
    
    def getAffiliations(self):
        authors = self.__workflowMetadata.getList("authors")
        
        affiliations = {}
        for author in authors:
            key = "(%s) %s" % (author.get("orgUnitId"), author.get("orgUnit"))
            if not affiliations.has_key(key):
                affiliations[key] = {}
            names = affiliations[key]
            expiry = author["expiry"]
            if not names.has_key(expiry):
                names[expiry] = []
            names[expiry].append(author["author"])
        return affiliations
    
    ## Functions for navigation
    def __getNavData(self):
        query = self.sessionState.get("query")
        if query == "":
            query = "*:*"
        req = SearchRequest(query)
        req.setParam("fl", "id dc_title")
        req.setParam("sort", "f_dc_title asc")
        req.setParam("rows", "10000")   ## TODO there could be more than this
        req.setParam("facet", "true")
        req.addParam("facet.field", "workflow_step")
        req.setParam("facet.sort", "false")
        
        pq = self.services.portalManager.get(self.portalId).query
        req.setParam("fq", pq)
        req.addParam("fq", 'item_type:"object"')
        fq = self.sessionState.get("fq")
        if fq:
            for q in fq:
                req.addParam("fq", q)
        
        out = ByteArrayOutputStream()
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        oidList = result.getList("response/docs/id")
        nameList = result.getList("response/docs/dc_title")
        
        wfStep = result.getList("facet_counts/facet_fields/workflow_step")
        self.pending = 0
        self.confirmed = 0
        for i in range(len(wfStep)):
            if wfStep[i] == "pending":
                self.pending = wfStep[i+1]
            if wfStep[i] == "live":
                self.confirmed = wfStep[i+1]
        
        self.total = self.pending + self.confirmed
        return oidList, nameList
    
    def __getNavDataUnedited(self):
        query = self.sessionState.get("query")
        if query == "":
            query = "*:*"
        req = SearchRequest(query)
        req.setParam("fl", "id dc_title")
        req.setParam("sort", "f_dc_title asc")
        req.setParam("rows", "10000")
        pq = self.services.portalManager.get(self.portalId).query
        req.setParam("fq", pq)
        req.addParam("fq", 'item_type:"object"')
        req.addParam("fq", "workflow_modified:false")
        fq = self.sessionState.get("fq")
        if fq:
            for q in fq:
                req.addParam("fq", q)
        out = ByteArrayOutputStream()
        self.__indexer.search(req, out)
        result = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
        oidList = result.getList("response/docs/id")
        nameList = result.getList("response/docs/dc_title")
        return oidList, nameList
    
    def __getIndex(self):
        return self.__oidList.indexOf(self.__oid)
    
    def __getUneditedIndex(self):
        return self.__unEditedOidList.indexOf(self.__oid)
    
    def getNextOid(self):
        i = self.__getIndex()
        if i+1 < self.__oidList.size():
            return self.__oidList.get(i+1)
        return None
    
    def getNextName(self):
        i = self.__getIndex()
        if i+1 < self.__nameList.size():
            return self.__nameList.get(i+1)
        return None
    
    def getPrevOid(self):
        i = self.__getIndex()
        if i > 0:
            return self.__oidList.get(i-1)
        return None
    
    def getPrevName(self):
        i = self.__getIndex()
        if i > 0:
            return self.__nameList.get(i-1)
        return None
    
    def getNextUneditedOid(self):
        i = self.__getUneditedIndex()
        if i+1 < self.__unEditedOidList.size():
            return self.__unEditedOidList.get(i+1)
        return None
    
    def getNextUneditedName(self):
        i = self.__getUneditedIndex()
        if i+1 < self.__unEditedNameList.size():
            return self.__unEditedNameList.get(i+1)
        return None
    
    def getPrevUneditedOid(self):
        i = self.__getUneditedIndex()
        if i > 0:
            return self.__unEditedOidList.get(i-1)
        return None
    
    def getPrevUneditedName(self):
        i = self.__getUneditedIndex()
        if i > 0:
            return self.__unEditedNameList.get(i-1)
        return None
