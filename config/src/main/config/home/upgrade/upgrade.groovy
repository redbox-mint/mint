/*
 *
 *  Copyright (C) 2016 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License along
 *    with this program; if not, write to the Free Software Foundation, Inc.,
 *    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 * /
 */

import java.io.*;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.FascinatorHome;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.FileUtils;
import com.googlecode.fascinator.common.JsonObject;
import org.json.simple.JSONArray;

println "\n--------------------------------"
println "Mint 1.9 Update Script"
println "--------------------------------\n"

changeList = []

redboxVersion = "1.9-SNAPSHOT";

if(!verifyCorrectUpgradeVersion(redboxVersion)) {
  return;
}

initialConfig = new JsonSimpleConfig();

updateVersionInTfEnv(redboxVersion);

displayUpgradeCompleteMessage();


def updateVersionInTfEnv(redboxVersion){
  println "Update version number"
  println "----------------------------------\n"
  println "If you are using you're own versioning scheme for your institutional build select N for the following question.";
  updateVersionNumber = util.promptUserInput("Would you like to update the version number string to the version " + redboxVersion+"?","[Y/N]","[ynYN]");
  if(updateVersionNumber.toLowerCase() == "y") {
  
  	def lines = FileUtils.readLines(new File("tf_env.sh"));
  	def index = 0;
  	for(line in lines) {
  		if(line.startsWith("export REDBOX_VERSION")) {
  		  line = "export REDBOX_VERSION=\""+redboxVersion+"\"";
  		  lines.set(index, line);
  		}
  		index++;
  	}
  	FileUtils.writeLines(new File("tf_env.sh"),lines);
  	
  	index = 0;
  	lines = FileUtils.readLines(new File("tf_env.bat"));
  	for(line in lines) {
  		if(line.startsWith("set REDBOX_VERSION")) {
  		  line = "set REDBOX_VERSION="+redboxVersion+"";
  		  lines.set(index, line);
  		 }
  		 index++;
  	}
  	FileUtils.writeLines(new File("tf_env.bat"),lines);
  	
  	println "Version string updated.\n"
  }
  
 }

def displayUpgradeCompleteMessage() {
  println "----------------------------------"
  println "Configuration complete"
  println "----------------------------------\n"
  println "All the requested modifications have been made to the configuration. You are now ready to start the application and run the tf_restore script to perform the data migration."
}

def verifyCorrectUpgradeVersion(redboxVersion){
  println "Verifying the Mint installation is the correct version\n"
  if(!new File("lib/mint-"+redboxVersion+".pom").exists()) {
    println "The Mint installation does not appear to be the correct version. Please ensure you have deployed the latest distribution."
    return false;
  }
  return true;
}
