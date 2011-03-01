import md5, time

from au.edu.usq.fascinator.api.storage import StorageException

from java.util import HashMap

class IndexData:
    def __init__(self):
        pass

    def __activate__(self, context):
        # Prepare variables
        self.index = context["fields"]
        self.indexer = context["indexer"]
        self.object = context["object"]
        self.payload = context["payload"]
        self.params = context["params"]
        self.utils = context["pyUtils"]
        
        self.handle = ""
        self.faculty = ""
        self.school = ""

        # Common data
        self.__newDoc()

        # Real metadata
        if self.itemType == "object":
            self.__previews()
            self.__basicData()
            # Run dc first before marc harvester, try to change in the oaiPmhHarvester to support multiple prefix in 1 harvest
            self.__getHandleFromDC()
            if self.params.get("prefix") == "marc":
                self.__solrMarc()
                self.__processAuthors()

        # Make sure security comes after workflows
        self.__security(self.oid, self.index)

    def __processAuthors(self):
        try:
            payload = self.object.getPayload("metadata.json")
            json = self.utils.getJsonObject(payload.open())
            payload.close()
            
            title = json.get("title")
            
            author100 = json.get("author_100")
            if author100:
                self.__createAuthorRecord(title, author100)
            
            author700 = json.getList("author_700")
            for author in author700:
                self.__createAuthorRecord(title, author)
            
        except StorageException, se:
            print "Failed to read metadata payload: '%s'" % str(e)

    def __createAuthorRecord(self, title, author):
        hash = title.encode("utf-8") + "#" + author.encode("utf-8")
        #print "Creating author record:", hash
        oid = md5.new(hash).hexdigest()
        index = HashMap()
        self.utils.add(index, "id", oid)
        self.utils.add(index, "storage_id", self.oid)
        self.utils.add(index, "item_type", "object")
        self.utils.add(index, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        self.utils.add(index, "harvest_config", self.params.getProperty("jsonConfigOid"))
        self.utils.add(index, "harvest_rules",  self.params.getProperty("rulesOid"))
        self.utils.add(index, "dc_title", author)
        self.utils.add(index, "dc_description", title)
        self.utils.add(index, "recordtype", "author")
        self.utils.add(index, "handle", self.handle)
        self.utils.add(index, "faculty", self.faculty)
        self.utils.add(index, "school", self.school)
        self.utils.add(index, "repository_name", self.params["repository.name"])
        self.utils.add(index, "repository_type", self.params["repository.type"])
        self.utils.add(index, "display_type", "author")
        self.__security(oid, index)
        self.indexer.sendIndexToBuffer(oid, index)

    def __mapVuFind(self, ourField, theirField, map):
        for value in map.getList(theirField):
            if theirField == "faculty":
                self.faculty = value
            if theirField == "school":
                self.school = value
            self.utils.add(self.index, ourField, value)

    def __solrMarc(self):
        ### Index the marc metadata extracted from solrmarc
        try:
            marcPayload = self.object.getPayload("metadata.json")
            marc = self.utils.getJsonObject(marcPayload.open())
            marcPayload.close()
            if marc is not None:
                coreFields = {
                    "id" : "dc_identifier",
                    "recordtype" : "recordtype",
                    "title" : "dc_title",
                    "author_100" : "dc_creator",
                    "author_700" : "dc_creator",
                    "university" : "university",
                    "faculty" : "faculty",
                    "school" : "school"
                }
                for k,v in coreFields.iteritems():
                    self.__mapVuFind(v, k, marc)
                self.utils.add(self.index, "display_type", "marc")
        except StorageException, e:
            print "Could not find MARC data (%s)" % str(e)

        # On the first index after a harvest we need to put the transformer back into
        #  the picture for reharvest actions to work.
        renderer = self.params.getProperty("renderQueue")
        if renderer is not None and renderer == "":
            self.params.setProperty("renderQueue", "solrmarc");
            self.params.setProperty("objectRequiresClose", "true");

    def __newDoc(self):
        self.oid = self.object.getId()
        self.pid = self.payload.getId()
        metadataPid = self.params.getProperty("metaPid", "DC")

        if self.pid == metadataPid:
            self.itemType = "object"
        else:
            self.oid += "/" + self.pid
            self.itemType = "datastream"
            self.utils.add(self.index, "identifier", self.pid)

        self.utils.add(self.index, "id", self.oid)
        self.utils.add(self.index, "storage_id", self.oid)
        self.utils.add(self.index, "item_type", self.itemType)
        self.utils.add(self.index, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        self.utils.add(self.index, "harvest_config", self.params.getProperty("jsonConfigOid"))
        self.utils.add(self.index, "harvest_rules",  self.params.getProperty("rulesOid"))

    def __basicData(self):
        self.utils.add(self.index, "repository_name", self.params["repository.name"])
        self.utils.add(self.index, "repository_type", self.params["repository.type"])

    def __previews(self):
        self.previewPid = None
        for payloadId in self.object.getPayloadIdList():
            try:
                payload = self.object.getPayload(payloadId)
                if str(payload.getType())=="Thumbnail":
                    self.utils.add(self.index, "thumbnail", payload.getId())
                elif str(payload.getType())=="Preview":
                    self.previewPid = payload.getId()
                    self.utils.add(self.index, "preview", self.previewPid)
                elif str(payload.getType())=="AltPreview":
                    self.utils.add(self.index, "altpreview", payload.getId())
            except Exception, e:
                pass

    def __security(self, oid, index):
        roles = self.utils.getRolesWithAccess(oid)
        if roles is not None:
            for role in roles:
                self.utils.add(index, "security_filter", role)
        else:
            # Default to guest access if Null object returned
            schema = self.utils.getAccessSchema("derby");
            schema.setRecordId(oid)
            schema.set("role", "guest")
            self.utils.setAccessSchema(schema, "derby")
            self.utils.add(index, "security_filter", "guest")

    def __getHandleFromDC(self):
        self.utils.registerNamespace("oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/")
        self.utils.registerNamespace("dc", "http://purl.org/dc/elements/1.1/")

        try:
            dcPayload = self.object.getPayload("oai_dc.xml")
            dc = self.utils.getXmlDocument(dcPayload.open())
            dcPayload.close()
            node = dc.selectSingleNode("//dc:identifier[starts-with(text(), 'http://hdl.handle.net')]")
            if node:
                self.handle = node.getTextTrim()
                self.utils.add(self.index, "handle", self.handle)
        except:
           pass
