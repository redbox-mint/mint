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
        self.utils.add(self.index, "harvest_config", self.params.getProperty("jsonConfigOid"))
        self.utils.add(self.index, "harvest_rules",  self.params.getProperty("rulesOid"))
        
        self.item_security = []
        
    def __basicData(self):
        self.utils.add(self.index, "repository_name", self.params["repository.name"])
        self.utils.add(self.index, "repository_type", self.params["repository.type"])

    def __metadata(self):
        self.utils.registerNamespace("dc", "http://purl.org/dc/terms/")
        
        jsonPayload = self.object.getPayload("metadata.json")
        json = self.utils.getJsonObject(jsonPayload.open())
        jsonPayload.close()
        
        data = json.getMap("data")
        
        geonamesId = data.get("geonameid")
        oid = DigestUtils.md5Hex("http://geonames.org/" + geonamesId)
        self.utils.add(self.index, "dc_identifier", "http://geonames.org/" + geonamesId)
        self.utils.add(self.index, "recordType", "area")
        self.utils.add(self.index, "storage_id", oid)   #Use parent object
        self.utils.add(self.index, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        self.utils.add(self.index, "display_type", "geonames")

        self.utils.add(self.index, "name", data.get("name"))
        self.utils.add(self.index, "dc_title", data.get("name"))
        self.utils.add(self.index, "asciiname", data.get("asciiname"))
        self.utils.add(self.index, "alternatenames", data.get("alternatenames"))
        self.utils.add(self.index, "latitude", data.get("latitude"))
        self.utils.add(self.index, "longitude", data.get("longitude"))
        self.utils.add(self.index, "feature_class", data.get("featureclass"))
        self.utils.add(self.index, "feature_code", data.get("featurecode"))
        self.utils.add(self.index, "country_code", data.get("countrycode"))
        self.utils.add(self.index, "cc2", data.get("cc2"))
        self.utils.add(self.index, "admin1_code", data.get("admin1code"))
        self.utils.add(self.index, "admin2_code", data.get("admin2code"))
        self.utils.add(self.index, "admin3_code", data.get("admin3code"))
        self.utils.add(self.index, "admin4_code", data.get("admin4code"))
        self.utils.add(self.index, "population", data.get("population"))
        self.utils.add(self.index, "elevation", data.get("elevation"))
        self.utils.add(self.index, "gtopo30", data.get("gtopo30"))
        self.utils.add(self.index, "timezone", data.get("timezone"))
        self.utils.add(self.index, "modification_date", data.get("modificationdate"))
                    
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

