from com.googlecode.fascinator.common import JsonSimple
from com.googlecode.fascinator.common.messaging import MessagingServices
from com.googlecode.fascinator.messaging import TransactionManagerQueueConsumer

class CurateData:
    def __init__(self):
        pass

    def __activate__(self, context):
        self.request = context["request"]
        self.response = context["response"]
        self.formData = context["formData"]
        self.log = context["log"]

        oid = self.formData.get("oid")
        self.log.debug("Curation request recieved: '{}'", oid)
        message = JsonSimple()
        message.getJsonObject().put("task", "curation")
        message.getJsonObject().put("oid", oid)

        out = self.response.getPrintWriter("text/plain; charset=UTF-8")
        if self.queueMessage(message.toString()):
            out.println("Request successful. The system will now process.")
        else:
            self.response.setStatus(500)
            out.println("Error sending message, see system logs.")
        out.close()

    def queueMessage(self, msg):
        try:
            ms = MessagingServices.getInstance()
            ms.queueMessage(TransactionManagerQueueConsumer.LISTENER_ID, msg);
            ms.release()
            return True
        except:
            return False
