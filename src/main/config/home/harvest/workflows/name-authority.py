import md5, time

from au.edu.usq.fascinator.api.storage import StorageException
from au.edu.usq.fascinator.common import JsonConfigHelper
from au.edu.usq.fascinator.common.storage import StorageUtils

from java.io import ByteArrayInputStream
from java.lang import Exception, String
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
        self.config = context["jsonConfig"]

        # Common data
        self.__newDoc()

        # Real metadata
        if self.itemType == "object":
            self.__previews()
            self.__basicData()
            self.__metadata()
            self.__nameAuthority()
            self.__processOrgUnits()
            # Some of the above steps may request some
            #  messages be sent, particularly workflows.
            self.__messages()

        # Make sure security comes after workflows
        self.__security(self.oid, self.index)

    def __processOrgUnits(self):
        try:
            payload = self.object.getPayload("workflow.metadata")
            json = self.utils.getJsonObject(payload.open())
            payload.close()
            
            authors = json.getJsonList("authors")
            for author in authors:
                authorName = author.get("author")
                orgUnitId = author.get("orgUnitId")
                orgUnit = author.get("orgUnit")
                expiry = author.get("expiry")
                expiry = time.strptime(expiry, "%d/%m/%Y")
                expiry = time.strftime("%Y-%m-%dT%H:%M:%SZ", expiry)
                oid = md5.new(orgUnitId + "#" + orgUnit).hexdigest()
                index = HashMap()
                self.utils.add(index, "id", oid)
                self.utils.add(index, "storage_id", self.oid)
                self.utils.add(index, "item_type", "object")
                self.utils.add(index, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
                self.utils.add(index, "harvest_config", self.params.getProperty("jsonConfigOid"))
                self.utils.add(index, "harvest_rules",  self.params.getProperty("rulesOid"))
                self.utils.add(index, "dc_title", orgUnit)
                self.utils.add(index, "recordtype", "org_unit")
                self.utils.add(index, "repository_name", self.params["repository.name"])
                self.utils.add(index, "repository_type", self.params["repository.type"])
                self.utils.add(index, "display_type", "org-unit")
                self.utils.add(index, "org_unit_id", orgUnitId)
                self.utils.add(index, "org_unit_label", orgUnit)
                self.utils.add(index, "org_unit_author", authorName)
                self.utils.add(index, "date_org_unit_expiry", expiry)
                self.__security(oid, index)
                self.indexer.sendIndexToBuffer(oid, index)
        except StorageException, se:
            print "Failed to read workflow metadata payload: '%s'" % str(e)

    def __nameAuthority(self):
        # set constant fields
        self.utils.add(self.index, "recordtype", "master")
        self.utils.add(self.index, "display_type", "name-authority")
        
        # index nodes in the package
        try:
            pkgPayload = self.object.getPayload(self.object.getSourceId())
            pkg = JsonConfigHelper(pkgPayload.open())
            pkgPayload.close()
            manifest = pkg.getJsonMap("manifest")
            for key in manifest.keySet():
                node = manifest.get(key)
                nodeTitle = node.get("title")
                self.utils.add(self.index, "package_node_title", nodeTitle)
                children = node.getJsonMap("children")
                for key in children:
                    childNode = children.get(key)
                    nodeId = childNode.get("id")
                    self.utils.add(self.index, "package_node_id", nodeId)
        except Exception, e:
            print "Failed to index package items: %s" % str(e)

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

        self.item_security = []

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
        # Security
        roles = self.utils.getRolesWithAccess(oid)
        if roles is not None:
            # For every role currently with access
            for role in roles:
                # Should show up, but during debugging we got a few
                if role != "":
                    if role in self.item_security:
                        # They still have access
                        self.utils.add(index, "security_filter", role)
                    else:
                        # Their access has been revoked
                        self.__revokeAccess(oid, role)
            # Now for every role that the new step allows access
            for role in self.item_security:
                if role not in roles:
                    # Grant access if new
                    self.__grantAccess(oid, role)
                    self.utils.add(index, "security_filter", role)

        # No existing security
        else:
            if self.item_security is None:
                # Guest access if none provided so far
                self.__grantAccess(oid, "guest")
                self.utils.add(index, "security_filter", role)
            else:
                # Otherwise use workflow security
                for role in self.item_security:
                    # Grant access if new
                    self.__grantAccess(oid, role)
                    self.utils.add(index, "security_filter", role)
        # Ownership
        owner = self.params.getProperty("owner", None)
        if owner is None:
            self.utils.add(index, "owner", "system")
        else:
            self.utils.add(index, "owner", owner)

    def __indexPath(self, name, path, includeLastPart=True):
        parts = path.split("/")
        length = len(parts)
        if includeLastPart:
            length +=1
        for i in range(1, length):
            part = "/".join(parts[:i])
            if part != "":
                if part.startswith("/"):
                    part = part[1:]
                self.utils.add(self.index, name, part)

    def __indexList(self, name, values):
        for value in values:
            self.utils.add(self.index, name, value)

    def __getNodeValues(self, doc, xPath):
        nodes = doc.selectNodes(xPath)
        valueList = []
        if nodes:
            for node in nodes:
                #remove duplicates:
                nodeValue = node.getText()
                if nodeValue not in valueList:
                    valueList.append(node.getText())
        return valueList

    def __grantAccess(self, oid, newRole):
        schema = self.utils.getAccessSchema("derby");
        schema.setRecordId(oid)
        schema.set("role", newRole)
        self.utils.setAccessSchema(schema, "derby")

    def __revokeAccess(self, oid, oldRole):
        schema = self.utils.getAccessSchema("derby");
        schema.setRecordId(oid)
        schema.set("role", oldRole)
        self.utils.removeAccessSchema(schema, "derby")

    def __metadata(self):
        self.titleList = ["New Name Authority"]
        self.descriptionList = []
        self.creatorList = []
        self.creationDate = []
        self.contributorList = []
        self.approverList = []
        self.formatList = ["application/x-fascinator-package"]
        self.fulltext = []
        self.relationDict = {}
        self.customFields = {}

        # Try our data sources, order matters
        self.__workflow()

        # Some defaults if the above failed
        if self.titleList == []:
           self.titleList.append(self.object.getSourceId())
        if self.formatList == []:
            source = self.object.getPayload(self.object.getSourceId())
            self.formatList.append(source.getContentType())

        # Index our metadata finally
        self.__indexList("dc_title", self.titleList)
        self.__indexList("dc_creator", self.creatorList)  #no dc_author in schema.xml, need to check
        self.__indexList("dc_contributor", self.contributorList)
        self.__indexList("dc_description", self.descriptionList)
        self.__indexList("dc_format", self.formatList)
        self.__indexList("dc_date", self.creationDate)
        self.__indexList("full_text", self.fulltext)
        for key in self.customFields:
            self.__indexList(key, self.customFields[key])
        for key in self.relationDict:
            self.__indexList(key, self.relationDict[key])

    def __workflow(self):
        # Workflow data
        WORKFLOW_ID = "name-authority"
        wfChanged = False
        workflow_security = []
        self.message_list = None
        try:
            wfPayload = self.object.getPayload("workflow.metadata")
            wfMeta = JsonConfigHelper(wfPayload.open())
            wfPayload.close()

            # Are we indexing because of a workflow progression?
            targetStep = wfMeta.get("targetStep")
            if targetStep is not None and targetStep != wfMeta.get("step"):
                wfChanged = True
                # Step change
                wfMeta.set("step", targetStep)
                wfMeta.removePath("targetStep")

            # This must be a re-index then
            else:
                targetStep = wfMeta.get("step")

            # Security change
            stages = self.config.getJsonList("stages")
            for stage in stages:
                if stage.get("name") == targetStep:
                    wfMeta.set("label", stage.get("label"))
                    self.item_security = stage.getList("visibility")
                    workflow_security = stage.getList("security")
                    if wfChanged == True:
                        self.message_list = stage.getList("message")

            # Form processing
            formData = wfMeta.getJsonList("formData")
            if formData.size() > 0:
                formData = formData[0]
            else:
                formData = None
            coreFields = ["title", "creator", "contributor", "description", "format", "creationDate"]
            if formData is not None:
                # Core fields
                title = formData.getList("title")
                if title:
                    self.titleList = title
                creator = formData.getList("creator")
                if creator:
                    self.creatorList = creator
                contributor = formData.getList("contributor")
                if contributor:
                    self.contributorList = contributor
                description = formData.getList("description")
                if description:
                    self.descriptionList = description
                format = formData.getList("format")
                if format:
                    self.formatList = format
                creation = formData.getList("creationDate")
                if creation:
                    self.creationDate = creation
                # Non-core fields
                data = formData.getMap("/")
                for field in data.keySet():
                    if field not in coreFields:
                        self.customFields[field] = formData.getList(field)

        except StorageException, e:
            # No workflow payload, time to create
            wfChanged = True
            wfMeta = JsonConfigHelper()
            wfMeta.set("id", WORKFLOW_ID)
            wfMeta.set("step", "pending")
            wfMeta.set("pageTitle", "Uploaded Files - Management")
            stages = self.config.getJsonList("stages")
            for stage in stages:
                if stage.get("name") == "pending":
                    wfMeta.set("label", stage.get("label"))
                    self.item_security = stage.getList("visibility")
                    workflow_security = stage.getList("security")
                    self.message_list = stage.getList("message")

        # Has the workflow metadata changed?
        if wfChanged == True:
            jsonString = String(wfMeta.toString())
            inStream = ByteArrayInputStream(jsonString.getBytes("UTF-8"))
            try:
                StorageUtils.createOrUpdatePayload(self.object, "workflow.metadata", inStream)
            except StorageException, e:
                print " * name-authority.py : Error updating workflow payload"

        self.utils.add(self.index, "workflow_id", wfMeta.get("id"))
        self.utils.add(self.index, "workflow_step", wfMeta.get("step"))
        self.utils.add(self.index, "workflow_step_label", wfMeta.get("label"))
        for group in workflow_security:
            self.utils.add(self.index, "workflow_security", group)

    def __messages(self):
        if self.message_list is not None and len(self.message_list) > 0:
            msg = JsonConfigHelper()
            msg.set("oid", self.oid)
            message = msg.toString()
            for target in self.message_list:
                self.utils.sendMessage(target, message)

