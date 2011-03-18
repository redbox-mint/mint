import md5
from au.edu.usq.fascinator.api.indexer import SearchRequest
from au.edu.usq.fascinator.common import JsonConfig, JsonConfigHelper

from java.io import ByteArrayInputStream, ByteArrayOutputStream
from java.util import ArrayList
from java.lang import Exception

class LookupData:
    def __init__(self):
        pass
    
    def __activate__(self, context):
        self.log = context["log"]
        self.services = context["Services"]
        self.portalId = context["portalId"]
        self.formData = context["formData"]

        self.request = context["request"]
        #self.request.setAttribute("Content-Type", "application/x-fascinator-lookup+json")
        self.request.setAttribute("Content-Type", "application/json")
        self.__solrData = self.__getSolrData()
        self.__results = self.__solrData.getJsonList("response/docs")
    
        jc = JsonConfig()
        baseUrl = jc.get("urlBase")
        if baseUrl.endswith("/"):
            baseUrl = baseUrl[:-1]
        self.__baseUrl = baseUrl
    
    def getBaseUrl(self):
        return self.__baseUrl + "/" + self.portalId
    
    def getLink(self):
        return ""
    
    def getTotalResults(self):
        return self.__solrData.get("response/numFound")
    
    def getStartIndex(self):
        return self.getFormData("startIndex", "0")
    
    def getItemsPerPage(self):
        return self.getFormData("count", "25")
    
    def getRole(self):
        return "request"
    
    def getSearchTerms(self):
        return self.getFormData("searchTerms", None)
    
    def getStartPage(self):
        #index = int(self.getStartIndex())
        #perPage = int(self.getItemsPerPage())
        return 0 #(index / perPage)
    
    def getResults(self):
        return self.__results
    
    def getValue(self, doc, field):
        values = doc.getList(field)
        if not values.isEmpty():
            return values.get(0)
        return ""

    def getValueList(self, doc, field):
        values = doc.getList(field)
        if not values.isEmpty():
            return '"%s"' % '", "'.join(values)
        return ''
    
    def getNamespace(self):
        return "http://namespace.adfi.usq.edu.au/grants#"
    
    def __getSolrData(self):
        prefix = self.getSearchTerms()
        print "prefix='%s'" % prefix
        if prefix:
            query = 'dc_title:%(prefix)s OR dc_title:%(prefix)s*' % { "prefix": prefix }
            query += ' OR f_dc_identifier:%(ns)s%(prefix)s OR f_dc_identifier:%(ns)s%(prefix)s*' % \
                { "prefix": prefix, "ns": "http\://example.com/arc/" }
        else:
            query = "*:*"

        portal = self.services.portalManager.get(self.portalId)
        if portal.searchQuery != "*:*" and portal.searchQuery != "":
            query = query + " AND " + portal.searchQuery
        req = SearchRequest(query)
        req.setParam("fq", 'item_type:"object"')
        if portal.query:
            req.addParam("fq", portal.query)
        req.setParam("fl", "score")
        req.setParam("sort", "score desc")
        req.setParam("start", self.getStartIndex())
        req.setParam("rows", self.getItemsPerPage())
        req.setParam("facet", "true")
        req.setParam("facet.field", "repository_name")
        req.setParam("facet.mincount", "1")
        
        ns = self.getNamespace()
        level = self.getFormData("level", None)
        if level and level != "top":
            req.addParam("fq", 'repository_name:"%s"' % level.replace(ns, ""))
        
        try:
            out = ByteArrayOutputStream()
            indexer = self.services.getIndexer()
            indexer.search(req, out)
            results = JsonConfigHelper(ByteArrayInputStream(out.toByteArray()))
            if level == "top":
                narrowerMap = {}
                for doc in results.getJsonList("response/docs"):
                    value = doc.getList("repository_name").get(0)
                    hash = md5.md5(value).hexdigest()
                    if not narrowerMap.has_key(hash):
                        #print value, hash
                        narrowerMap[hash] = []
                    narrowerMap[hash].append(doc.get("id"))
                docs = ArrayList()
                facets = results.getList("facet_counts/facet_fields/repository_name")
                for i in range(0, len(facets), 2):
                    value = facets[i]
                    hash = md5.md5(value).hexdigest()
                    #print value,hash
                    doc = JsonConfigHelper()
                    doc.set("score", "1")
                    doc.set("dc_identifier", "%s%s" % (ns, value))
                    doc.set("skos_inScheme", ns)
                    doc.set("skos_broader", "%s%s" % (ns, value))
                    doc.set("skos_narrower", '", "'.join(narrowerMap[hash]))
                    doc.set("skos_prefLabel", value)
                    docs.add(doc)
                results.removePath("response/docs")
                results.setJsonList("response/docs", docs)
            return results
        except Exception, e:
            self.log.error("Failed to lookup '{}': {}", prefix, str(e))
        
        return JsonConfigHelper()
    
    def getFormData(self, name, default):
        value = self.formData.get(name)
        if value is None or value == "":
            return default
        return value
