#
# Rules file for RDA Parties
#
import time

from com.googlecode.fascinator.api.storage import StorageException
from com.googlecode.fascinator.common import JsonSimple
from com.googlecode.fascinator.common.storage import StorageUtils

from java.lang import Exception
from java.lang import String

from org.apache.commons.io import IOUtils

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
        self.log = context["log"]

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

        self.utils.add(self.index, "storage_id", self.oid)
        if self.pid == metadataPid:
            self.itemType = "object"
        else:
            self.oid += "/" + self.pid
            self.itemType = "datastream"
            self.utils.add(self.index, "identifier", self.pid)

        self.utils.add(self.index, "id", self.oid)
        self.utils.add(self.index, "item_type", self.itemType)
        self.utils.add(self.index, "last_modified", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        self.utils.add(self.index, "harvest_config", self.params.getProperty("jsonConfigOid"))
        self.utils.add(self.index, "harvest_rules", self.params.getProperty("rulesOid"))
        self.utils.add(self.index, "display_type", "parties_people")

    def __checkMetadataPayload(self, identifier):
        # We are just going to confirm the existance of
        # 'metadata.json', or create an empty one if it
        # doesn't exist. Makes curation function for this
        # option and removes some log errors on the details
        # screen.
        try:
            self.object.getPayload("metadata.json")
            # all is good, the above will throw an exception if it doesn't exist
            return
        except Exception:
            self.log.info("Creating 'metadata.json' payload for object '{}'", self.oid)
            # Prep data
            metadata = JsonSimple()
            metadata.getJsonObject().put("recordIDPrefix", "")
            metadata.writeObject("data")
            # The only real data we require is the ID for curation
            idHolder = metadata.writeObject("metadata")
            idHolder.put("dc.identifier", identifier)
            # Store it
            inStream = IOUtils.toInputStream(metadata.toString(True), "UTF-8")
            try:
                StorageUtils.createOrUpdatePayload(self.object, "metadata.json", inStream)
            except StorageException, e:
                self.log.error("Error creating 'metadata.json' payload for object '{}'", self.oid, e)
            return

    def __basicData(self):
        self.utils.add(self.index, "repository_type", self.params["repository.type"])

    def __getRifPayload(self):
        try:
            payload = self.object.getPayload("rif.xml")
            return payload
        except Exception:
            self.log.error("Payload 'rif.xml' not found in object '{}'.", self.oid)
            return None

    def __metadata(self):
        self.utils.registerNamespace("rif", "http://ands.org.au/standards/rif-cs/registryObjects")
        self.utils.registerNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

        rifPayload = self.__getRifPayload()
        if rifPayload is None:
            self.log.error("Error accessing RIF-CS in storage: '{}'", self.oid)
            self.__indexError()
            return
        try:
            rif = self.utils.getXmlDocument(rifPayload.open())
            rifPayload.close()
        except Exception, e:
            self.log.error("Error parsing XML '{}' not accessible", self.oid, e)
            self.__indexError()
            return

        # RIF-CS version
        rif_string = self.__getSingleEntry(rif, "//rif:registryObjects/@xsi:schemaLocation")
        if rif_string is None:
            self.utils.add(self.index, "rifcsVersion", "error")
        else:
            if rif_string.find("rifcs/1.3.0") != -1:
                self.utils.add(self.index, "rifcsVersion", "1.3")
            else:
                if rif_string.find("rifcs/1.2.0") != -1:
                    self.utils.add(self.index, "rifcsVersion", "1.2")
                else:
                    self.utils.add(self.index, "rifcsVersion", "unknown")

        # Required - common
        institution = self.__indexMandatory(rif, "//rif:registryObject/@group", "repository_name")
        data_source = self.__indexMandatory(rif, "//rif:registryObject/rif:originatingSource", "source")
        party_type  = self.__indexMandatory(rif, "//rif:registryObject/rif:party/@type", "party_type")
        identifier  = self.__indexMandatory(rif, "//rif:registryObject/rif:key", "dc_identifier")
        # These just satisfy curation
        self.utils.add(self.index, "known_ids", identifier)
        self.__checkMetadataPayload(identifier)

        # Title
        title_found = False
        title = "Unknown name"
        # Party - Person
        if party_type == "person":
            # Primary
            nameNode = rif.selectSingleNode("//rif:registryObject/rif:party/rif:name[@type='primary'][1]")
            if nameNode is not None:
                primaryName = self.__getMultipartName(nameNode)
                if primaryName is None:
                    primaryName = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[@type='primary'][1]/rif:namePart[not(@type)][1]")
                    if primaryName is not None:
                        title = primaryName
                        self.utils.add(self.index, "title_found", "person-primitive-primary")
                        title_found = True
                    else:
                        primaryName = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[@type='primary'][1]/rif:namePart[1]")
                        if primaryName is not None:
                            title = primaryName
                            self.utils.add(self.index, "title_found", "person-primitive-primary-invalid-type")
                            title_found = True
                else:
                    title = primaryName
                    self.utils.add(self.index, "title_found", "person-normal")
                    title_found = True
            else:
                # Alternate
                nameNode = rif.selectSingleNode("//rif:registryObject/rif:party/rif:name[@type='alternative'][1]")
                if nameNode is not None:
                    altName = self.__getMultipartName(nameNode)
                    if altName is None:
                        altName = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[@type='alternative'][1]/rif:namePart[not(@type)][1]")
                        if altName is not None:
                            title = altName
                            self.utils.add(self.index, "title_found", "person-primitive-alternate")
                            title_found = True
                        else:
                            altName = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[@type='alternative'][1]/rif:namePart[1]")
                            if altName is not None:
                                title = altName
                                self.utils.add(self.index, "title_found", "person-primitive-alternate-invalid-type")
                                title_found = True
                    else:
                        title = altName
                        self.utils.add(self.index, "title_found", "person-alternate")
                        title_found = True
                else:
                    # Anything
                    fallback = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[1]/rif:namePart[1]")
                    if fallback is not None:
                        title = fallback
                        self.utils.add(self.index, "title_found", "person-primitive-any")
                        title_found = True

            self.utils.add(self.index, "title", title)
            self.utils.add(self.index, "dc_title", title)

            if not title_found:
                self.utils.add(self.index, "title_found", "person-none")

            # Needs to put something intelligible in ReDBox
            if party_type == "person":
                self.utils.add(self.index, "Honorific",   "-")
                self.utils.add(self.index, "Given_Name",  title)
                self.utils.add(self.index, "Family_Name", "(RDA Party)")

        else:
            # Party - Others
            if party_type == "group" or party_type == "administrativePosition":
                title = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[@type='primary'][1]/rif:namePart[not(@type)][1]")
                if title is None:
                    fallback = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[@type='primary'][1]/rif:namePart[@type='full'][1]")
                    if fallback is not None:
                        title = fallback
                        self.utils.add(self.index, "title", title)
                        self.utils.add(self.index, "dc_title", title)
                        self.utils.add(self.index, "title_found", party_type+"-invalid-full")
                        title_found = True
                    else:
                        fallback = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[1]/rif:namePart[1]")
                        if fallback is not None:
                            title = fallback
                            self.utils.add(self.index, "title", title)
                            self.utils.add(self.index, "dc_title", title)
                            self.utils.add(self.index, "title_found", party_type+"-primitive-any")
                            title_found = True
                        else:
                            self.utils.add(self.index, "title", "{error}")
                            self.utils.add(self.index, "dc_title", "{error}")
                            self.utils.add(self.index, "title_found", party_type+"-{error}")
                else:
                    self.utils.add(self.index, "title", title)
                    self.utils.add(self.index, "dc_title", title)
                    self.utils.add(self.index, "title_found", party_type+"-normal")
                    title_found = True

            else:
                fallback = self.__getSingleEntry(rif, "//rif:registryObject/rif:party/rif:name[1]/rif:namePart[1]")
                if fallback is not None:
                    title = fallback
                    self.utils.add(self.index, "title", title)
                    self.utils.add(self.index, "dc_title", title)
                    self.utils.add(self.index, "title_found", "invalidType-primitive-any")
                    title_found = True
                else:
                    self.utils.add(self.index, "title", "{error}")
                    self.utils.add(self.index, "dc_title", "{error}")
                    self.utils.add(self.index, "party_type", "{error}")
                    self.utils.add(self.index, "title_found", "invalidType-{error}")

        # Description
        description  = self.__indexOptional(rif, "//rif:registryObject/rif:party/rif:description[@type='brief']", "description")
        if description is None:
            description  = self.__indexOptional(rif, "//rif:registryObject/rif:party/rif:description[@type='full']", "description")
            if description is not None:
                self.utils.add(self.index, "description_type", "full")
        else:
            self.utils.add(self.index, "description_type", "brief")

        if description is None:
            description  = "Record '%s' from '%s' (source: '%s')" % (identifier, institution, data_source)
            self.utils.add(self.index, "description_type", "none")
        self.utils.add(self.index, "dc_description", description)

        # Case sensitivity change, just for the ReDBox lookups
        if party_type == "person":
            self.utils.add(self.index, "Description", description)

    def __indexOptional(self, document, xPath, solrField):
        data = self.__getSingleEntry(document, xPath)
        if data is not None:
            self.utils.add(self.index, solrField, data)
            return data
        return None

    def __indexMandatory(self, document, xPath, solrField):
        data = self.__getSingleEntry(document, xPath)
        if data is None:
            self.log.error("Error indexing '{}' ('{}'). None found '{}'!", [solrField, xPath, self.oid])
            self.utils.add(self.index, solrField, "error")
            return "error"
        else:
            self.utils.add(self.index, solrField, data)
            return data

    def __getMultipleEntries(self, document, xPath):
        list = document.selectNodes(xPath)
        if list is None or list.isEmpty():
            return []
        else:
            result = []
            for entry in list:
                result.append(String(entry.getText()).trim())
            return result

    def __getSingleEntry(self, document, xPath):
        list = document.selectNodes(xPath)
        if list is None or list.isEmpty():
            return None
        else:
            entry = list.get(0)
            if list.size() > 1:
                self.log.warn("Found {} entries ('{}'), only using the first", list.size(), xPath)
            return String(entry.getText()).trim()

    def __getMultipartName(self, node):
        # Honorific
        honorific  = self.__getSingleEntry(node, "//rif:namePart[@type='title'][1]")
        if honorific is not None:
            title = honorific + " "
        else:
            title = ""

        # Given name
        given  = self.__getSingleEntry(node, "//rif:namePart[@type='given'][1]")
        if given is None:
            return None
        title += given

        # Family name
        family = self.__getSingleEntry(node, "//rif:namePart[@type='family'][1]")
        if family is not None:
            title += " " + family

        return title

    def __indexError(self):
        title = "Unknown Title, object '%s'" % self.oid
        self.utils.add(self.index, "title", title)
        self.utils.add(self.index, "dc_title", title)
        description = "Error during harvest/index for item '%s'. Could not access metadata from storage." % self.oid
        self.utils.add(self.index, "description", description)
        self.utils.add(self.index, "dc_description", description)
        return

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
