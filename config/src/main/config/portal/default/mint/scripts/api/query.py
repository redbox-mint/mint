from com.googlecode.fascinator.common import JsonSimple
from com.googlecode.fascinator.portal.services import ScriptingServices
from com.googlecode.fascinator.portal.api.impl import MintStatsAPICallHandlerImpl
from java.lang import Class

class QueryData:

    def __init__(self):
        pass
    def __activate__(self, context):
        self.response = context["response"]
        self.request = context["request"]
        self.systemConfig = context["systemConfig"]
        out = self.response.getPrintWriter("text/plain; charset=UTF-8")
        callType = self.request.getParameter("callType")
        apiClass=self.systemConfig.getObject("api").get(callType)
        className = apiClass.get("className")
        apiCallClass = Class.forName(className)
        apiCallObject = apiCallClass.newInstance()        
    
        setScriptingServiceMethod = apiCallClass.getMethod("setScriptingServices", self.get_class("com.googlecode.fascinator.portal.services.ScriptingServices"))
        setScriptingServiceMethod.invoke(apiCallObject, context['Services'])
        if callType == "mint-stats":
             setScriptingServiceMethod = apiCallClass.getMethod("setConfig", self.get_class("com.googlecode.fascinator.common.JsonSimple"))
             setScriptingServiceMethod.invoke(apiCallObject, JsonSimple(self.systemConfig.getObject("api", "mint-stats")))
             
        handleRequestMethod = apiCallClass.getMethod("handleRequest", 
                                                       self.get_class("org.apache.tapestry5.services.Request"))
        responseString = handleRequestMethod.invoke(apiCallObject, context["request"]);
        out.println(responseString)
        
        out.close()
    
    # Standard Java Class forName seems to have issues at least with Interfaces. 
    # This is an alternative method taken from http://stackoverflow.com/questions/452969/does-python-have-an-equivalent-to-java-class-forname    
    def get_class(self, kls):
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m