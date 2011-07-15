from com.googlecode.fascinator.api.indexer import SearchRequest
from com.googlecode.fascinator.common.solr import SolrResult

from java.io import ByteArrayInputStream
from java.io import ByteArrayOutputStream

class HandlesData:
    def __init__(self):
        pass

    def __activate__(self, context):
        self.request = context["request"]
        self.response = context["response"]
        self.services = context["Services"]
        self.formData = context["formData"]
        self.results = self.__searchSolr()

        if (self.isCSV()):
            self.response.setHeader("Content-Disposition", "attachment; filename=handles.csv")
            self.response.setHeader("Content-Type", "text/csv")

    def __searchSolr(self):
        query = "handle:http* AND item_type:object";

        req = SearchRequest(query)
        req.setParam("rows", "99999")
        req.setParam("fl",   "id,dc_title,handle,repository_type,repository_name")
        req.setParam("sort", "handle asc")

        req.setParam("facet", "true")
        req.setParam("facet.field", "repository_type,repository_name")

        out = ByteArrayOutputStream()
        self.services.indexer.search(req, out)
        return SolrResult(ByteArrayInputStream(out.toByteArray()))

    def getRowCount(self):
        return self.results.getNumFound()

    def getRows(self):
        return self.results.getResults()

    def isCSV(self):
        csv = self.formData.get("csv")
        if csv is None or csv == "false":
            return False
        return True