#
# Rules script for ISO 639.2 Language codes
# Data can be downloaded from:
#     http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
#
import md5, time

from au.edu.usq.fascinator.api.storage import StorageException
from au.edu.usq.fascinator.common import JsonConfigHelper
from au.edu.usq.fascinator.common.storage import StorageUtils
from java.io import ByteArrayInputStream
from java.lang import Exception, String
from java.util import HashMap
from org.apache.commons.codec.digest import DigestUtils

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
            self.__basicData()
            self.__metadata()

        # Make sure security comes after workflows
        self.__security(self.oid, self.index)

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
        self.utils.add(self.index, "item_type", self.itemType)
        self.utils.add(self.index, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        self.utils.add(self.index, "harvest_config", self.params.getProperty("jsonConfigOid"))
        self.utils.add(self.index, "harvest_rules",  self.params.getProperty("rulesOid"))
        self.utils.add(self.index, "display_type", "staff")

        self.item_security = []
        
    def __basicData(self):
        self.utils.add(self.index, "repository_name", self.params["repository.name"])
        self.utils.add(self.index, "repository_type", self.params["repository.type"])

    def __metadata(self):
        jsonPayload = self.object.getPayload("metadata.json")
        json = self.utils.getJsonObject(jsonPayload.open())
        jsonPayload.close()
        
        data = json.getMap("data")
        self.utils.add(self.index, "dc_title", "%s %s" % (data.get("firstName"), data.get("surname")))
        self.utils.add(self.index, "dc_description", "%s %s" % (data.get("firstName"), data.get("surname")))
        for key in data.keySet():
            self.utils.add(self.index, key, data.get(key))
    
    def __security(self, oid, index):
        roles = self.utils.getRolesWithAccess(self.oid)
        if roles is not None:
            for role in roles:
                self.utils.add(self.index, "security_filter", role)
        else:
            # Default to guest access if Null object returned
            schema = self.utils.getAccessSchema("derby");
            schema.setRecordId(self.oid)
            schema.set("role", "guest")
            self.utils.setAccessSchema(schema, "derby")
            self.utils.add(self.index, "security_filter", "guest")

    def __indexList(self, name, values):
        for value in values:
            self.utils.add(self.index, name, value)

