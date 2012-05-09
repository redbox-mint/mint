from com.googlecode.fascinator.api.indexer import SearchRequest
from com.googlecode.fascinator.common import JsonObject
from com.googlecode.fascinator.common.messaging import MessagingServices
from com.googlecode.fascinator.common.solr import SolrResult
from com.googlecode.fascinator.messaging import TransactionManagerQueueConsumer
from com.googlecode.fascinator.redbox.sru import SRUClient

from java.io import ByteArrayInputStream
from java.io import ByteArrayOutputStream
from java.lang import Exception

class NlaData:
    def __init__(self):
        self.messaging = MessagingServices.getInstance()

    def __activate__(self, context):
        self.vc = context
        self.log      = self.vc["log"]
        self.services = self.vc["Services"]
        self.writer   = self.vc["response"].getPrintWriter("text/html; charset=UTF-8")
        # We check config now for how to store this
        self.config   = self.vc["systemConfig"]
        self.nlaProperty = self.config.getString("nlaPid", ["curation", "nlaIntegration", "pidProperty"])

        self.process()

    def process(self):
        self.log.debug("NLA housekeeping executing")

        # Find solr records
        result = self.search_solr()
        if result is None:
            return

        # Is there any work to do?
        num = result.getNumFound()
        if num == 0:
            self.writer.println("No records to process")
            self.writer.close()
            return

        # Now loop through each object and process
        sru = SRUClient()
        for record in result.getResults():
            success = self.process_record(record, sru)
            if not success:
                return

        self.writer.println("%s record(s) processed" % num)
        self.writer.close()

    # Process an individual record
    def process_record(self, record, sru):
        try:
            id = record.getFirst("storage_id")
            pid = record.getFirst("pidProperty")

            # TODO
            nlaPid = sru.nlaGetNationalId(pid);
            #nlaPid = sru.nlaGetNationalId("nla.party-915373"); # Debugging. A known NLA ID
            self.log.debug("{} => {} ({})", [id, pid, nlaPid])

            if nlaPid is None:
                self.log.debug("Object '{}' does not yet have a national Identity in NLA", id)
            else:
                self.log.debug("Object '{}' has a new national Identity in NLA ({})", id, nlaPid)

            # Store the NLA ID locally
            object = self.services.getStorage().getObject(id)
            metadata = object.getMetadata()
            metadata.setProperty(self.nlaProperty, nlaPid)
            object.close()

            # Notify the curation manager
            self.send_message(id)
            return True

        except Exception, e:
            self.log.error("Error updating object: ", e)
            self.throw_error("failure updating object: " + e.getMessage())
            return False

    # Send an event notification
    def send_message(self, oid):
        message = JsonObject()
        message.put("oid", oid)
        message.put("task", "curation-confirm")
        self.messaging.queueMessage(
                TransactionManagerQueueConsumer.LISTENER_ID,
                message.toString())

    # Search solr for objects that we are interested in
    def search_solr(self):
        # Build our solr query
        readyForNla = "ready_for_nla:ready"
        nlaPidExists = "nlaId:http*"
        query = readyForNla + " AND NOT " + nlaPidExists
        # Prepare the query
        req = SearchRequest(query)
        req.setParam("facet", "false")
        req.setParam("rows", "20")
        # Run the query
        try:
            out = ByteArrayOutputStream()
            self.services.getIndexer().search(req, out)
            return SolrResult(ByteArrayInputStream(out.toByteArray()))
        except Exception, e:
            self.log.error("Error searching solr: ", e)
            self.throw_error("failure searching solr: " + e.getMessage())
            return None

    def throw_error(self, message):
        self.vc["response"].setStatus(500)
        self.writer.println("Error: " + message)
        self.writer.close()
