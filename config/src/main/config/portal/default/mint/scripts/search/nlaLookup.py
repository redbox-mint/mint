from com.googlecode.fascinator.common import JsonObject
from com.googlecode.fascinator.common import JsonSimple
from com.googlecode.fascinator.redbox.sru import NLAIdentity
from com.googlecode.fascinator.redbox.sru import SRUClient

from java.lang import Exception
from org.json.simple import JSONArray

class NlaLookupData:
    def __init__(self):
        pass

    def __activate__(self, context):
        self.request = context["request"]
        self.response = context["response"]
        self.formData = context["formData"]
        self.log = context["log"]

        # Basic response text
        message = JsonSimple()
        self.metadata = message.writeObject(["metadata"])
        self.results  = message.writeArray(["results"])

        # Prepare response Object
        format = self.formData.get("format")
        if format == "json":
            out = self.response.getPrintWriter("application/json; charset=UTF-8")
        else:
            out = self.response.getPrintWriter("text/plain; charset=UTF-8")

        # Success Response
        try:
            self.searchNla()
            out.println(message.toString(True))
            out.close()

        except Exception, ex:
            self.log.error("Error during search: ", ex)

            self.response.setStatus(500)
            message = JsonSimple()
            message.getJsonObject().put("error", ex.getMessage())
            out.println(message.toString(True))
            out.close()

    def searchNla(self):
        # Start row
        start = self.formData.get("start")
        if start is None:
            start = "1"

        # Row limit
        rows = self.formData.get("rows")
        if rows is None:
            rows = "10"

        # Query
        query = ""
        # Test searches
        #query = "pa.surname=\"Smith\""
        #query = "cql.anywhere=\"monash\""

        if query == "":
            # Surname
            surname = self.formData.get("surname")
            if surname is not None:
                query = "pa.surname=\"%s\"" % (surname)

            # Firstname
            firstName = self.formData.get("firstName")
            if firstName is not None:
                if query != "":
                    query += " AND "
                query += "pa.firstname=\"%s\"" % (firstName)

        if query != "":
            query += " AND "
        query += "pa.type=\"person\""

        # Some basic metadata we already know
        self.metadata.put("searchString", query)
        self.metadata.put("startRecord", start)
        self.metadata.put("rowsRequested", rows)

        # Search NLA
        self.log.debug("Submitting query to NLA: '{}', Start: '{}', Rows: '{}'", [query, start, rows])
        sru = SRUClient()
        response =  sru.nlaGetResponseBySearch(query, start, rows)
        self.metadata.put("rowsReturned", response.getRows())
        self.metadata.put("totalHits", response.getTotalResults())

        identities = NLAIdentity.convertNodesToIdentities(response.getResults())
        for id in identities:
            idEntry = JsonObject()
            idEntry.put("nlaId", id.getId())
            idEntry.put("displayName", id.getDisplayName())
            idEntry.put("firstName", id.getFirstName())
            idEntry.put("surname", id.getSurame())
            idEntry.put("institution", id.getInstitution())
            knownIds = JSONArray()
            idEntry.put("knownIdentities", knownIds)

            for knownId in id.getKnownIdentities():
                thisId = JsonObject()
                thisId.put("displayName", knownId.get("displayName"))
                thisId.put("institution", knownId.get("institution"))
                knownIds.add(thisId)

            self.results.add(idEntry)

        return True
