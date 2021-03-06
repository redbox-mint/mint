/* 
 * The Fascinator - Mint Curation Transaction Manager
 * Copyright (C) 2011-2012 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.fascinator.redbox.plugins.curation.mint;

import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.indexer.IndexerException;
import com.googlecode.fascinator.api.indexer.SearchRequest;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transaction.TransactionException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.solr.SolrDoc;
import com.googlecode.fascinator.common.solr.SolrResult;
import com.googlecode.fascinator.common.transaction.GenericTransactionManager;
import com.googlecode.fascinator.messaging.EmailNotificationConsumer;
import com.googlecode.fascinator.messaging.TransactionManagerQueueConsumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements curation boundary logic for Mint. This class is also a replacement
 * for the standard tool chain.
 * 
 * @author Greg Pendlebury
 */
public class CurationManager extends GenericTransactionManager {

	/** Data payload */
	private static String DATA_PAYLOAD_ID = "metadata.json";

	/** Property to set flag for ready to publish */
	private static String READY_PROPERTY = "ready_to_publish";

	/** Property to set flag for publication allowed */
	private static String PUBLISH_PROPERTY = "published";

	/** Format for dates used by the NLA */
	private static String NLA_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	/** Property for storing creation date sent to the NLA */
	private static String NLA_DATE_PROPERTY = "eac_creation_date";

	/** NLA ID Property default */
	private static String NLA_ID_PROPERTY_DEFAULT = "nlaPid";

	/** Property to set flag for ready to send to the NLA */
	private static String NLA_READY_PROPERTY = "ready_for_nla";

	/** Logging **/
	private static Logger log = LoggerFactory.getLogger(CurationManager.class);

	/** Storage */
	private Storage storage;

	/** Solr Index */
	private Indexer indexer;

	/** External URL base */
	private String urlBase;

	/** Curation staff email address */
	private String emailAddress;

	/** Property to store PIDs */
	private String pidProperty;

	/** Send emails or just curate? */
	private boolean manualConfirmation;

	/** URL for our AMQ broker */
	private String brokerUrl;

	/** NLA Integration */
	private boolean nlaIntegrationEnabled;

	/** NLA Integration - Property Name */
	private String nlaIdProperty;

	/** NLA Integration - whether to use the NlaId for relationships */
	private boolean useNlaIdForRelationships;

	/** NLA Integration - Test for execution */
	private Map<String, String> nlaIncludeTest;

	/** NLA Integration - Date Formatting */
	private SimpleDateFormat nlaDate;

	/**
	 * Base constructor
	 * 
	 */
	public CurationManager() {
		super("curation-mint", "Mint Curation Transaction Manager");
	}

	/**
	 * Initialise method
	 * 
	 * @throws TransactionException
	 *             if there was an error during initialisation
	 */
	@Override
	public void init() throws TransactionException {
		JsonSimpleConfig config = getJsonConfig();

		// Load the storage plugin
		String storageId = config.getString("file-system", "storage", "type");
		if (storageId == null) {
			throw new TransactionException("No Storage ID provided");
		}
		storage = PluginManager.getStorage(storageId);
		if (storage == null) {
			throw new TransactionException("Unable to load Storage '" + storageId + "'");
		}
		try {
			storage.init(config.toString());
		} catch (PluginException ex) {
			log.error("Unable to initialise storage layer!", ex);
			throw new TransactionException(ex);
		}

		// Load the indexer plugin
		String indexerId = config.getString("solr", "indexer", "type");
		if (indexerId == null) {
			throw new TransactionException("No Indexer ID provided");
		}
		indexer = PluginManager.getIndexer(indexerId);
		if (indexer == null) {
			throw new TransactionException("Unable to load Indexer '" + indexerId + "'");
		}
		try {
			indexer.init(config.toString());
		} catch (PluginException ex) {
			log.error("Unable to initialise indexer!", ex);
			throw new TransactionException(ex);
		}

		// External facing URL
		urlBase = config.getString(null, "urlBase");
		if (urlBase == null) {
			throw new TransactionException("URL Base in config cannot be null");
		}

		// Where should emails be sent?
		emailAddress = config.getString(null, "curation", "curationEmailAddress");
		if (emailAddress == null) {
			throw new TransactionException("An admin email is required!");
		}

		// Where are PIDs stored?
		pidProperty = config.getString(null, "curation", "pidProperty");
		if (pidProperty == null) {
			throw new TransactionException("An admin email is required!");
		}

		// Do admin staff want to confirm each curation?
		manualConfirmation = config.getBoolean(false, "curation", "curationRequiresConfirmation");

		// Find the address of our broker
		brokerUrl = config.getString(null, "messaging", "url");
		if (brokerUrl == null) {
			throw new TransactionException("Cannot find the message broker.");
		}

		// National Library Integration
		nlaIntegrationEnabled = config.getBoolean(false, "curation", "nlaIntegration", "enabled");
		nlaIdProperty = config.getString(NLA_ID_PROPERTY_DEFAULT, "curation", "nlaIntegration", "pidProperty");
		useNlaIdForRelationships = config.getBoolean(true, "curation", "nlaIntegration", "useNlaIdForRelationships");
		JsonObject nlaIncludeTestNode = config.getObject("curation", "nlaIntegration", "includeTest");
		nlaIncludeTest = new HashMap<String, String>();
		if (nlaIncludeTestNode != null) {
			for (Object key : nlaIncludeTestNode.keySet()) {
				nlaIncludeTest.put((String) key, (String) nlaIncludeTestNode.get(key));
			}
		}
		// NLA Dates should always be UTC
		nlaDate = new SimpleDateFormat(NLA_DATE_FORMAT);
		nlaDate.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	/**
	 * Shutdown method
	 * 
	 * @throws PluginException
	 *             if any errors occur
	 */
	@Override
	public void shutdown() throws PluginException {
		if (storage != null) {
			try {
				storage.shutdown();
			} catch (PluginException pe) {
				log.error("Failed to shutdown storage: {}", pe.getMessage());
				throw pe;
			}
		}
		if (indexer != null) {
			try {
				indexer.shutdown();
			} catch (PluginException pe) {
				log.error("Failed to shutdown indexer: {}", pe.getMessage());
				throw pe;
			}
		}
	}

	/**
	 * This method encapsulates the logic for curation in Mint
	 * 
	 * @param oid
	 *            The object ID being curated
	 * @throws TransactionException 
	 * @returns JsonSimple The response object to send back to the queue
	 *          consumer
	 */
	private JsonSimple curation(JsonSimple message, String task, String oid) throws TransactionException {
		JsonSimple response = new JsonSimple();

		// *******************
		// Collect object data

		// Transformer config
		JsonSimple itemConfig = getConfigFromStorage(oid);
		if (itemConfig == null) {
			log.error("Error accessing item configuration!");
			return new JsonSimple();
		}
		// Object properties
		Properties metadata = getObjectMetadata(oid);
		if (metadata == null) {
			log.error("Error accessing item metadata!");
			return new JsonSimple();
		}
		// Object metadata
		JsonSimple data = getDataFromStorage(oid);
		if (data == null) {
			log.error("Error accessing item data!");
			return new JsonSimple();
		}

		// *******************
		// Validate what we can see

		// Check object state
		boolean curated = false;
		boolean alreadyCurated = itemConfig.getBoolean(false, "curation", "alreadyCurated");
		boolean errors = false;

		// Can we already see this PID?
		String thisPid = null;
		if (metadata.containsKey(pidProperty)) {
			curated = true;
			thisPid = metadata.getProperty(pidProperty);

			// Or does it claim to have one from pre-ingest curation?
		} else {
			if (alreadyCurated) {
				// Make sure we can actually see an ID
				String id = data.getString(null, "metadata", "dc.identifier");
				if (id == null) {
					log.error("Item claims to be curated, but has no" + " 'dc.identifier': '{}'", oid);
					errors = true;

					// Let's fix this so it doesn't show up again
				} else {
					try {
						log.info("Update object properties with ingested" + " ID: '{}'", oid);
						// Metadata writes can be awkward... thankfully this is
						// code that should only ever execute once per object.
						DigitalObject object = storage.getObject(oid);
						metadata = object.getMetadata();
						metadata.setProperty(pidProperty, id);
						object.close();
						metadata = getObjectMetadata(oid);
						curated = true;
						audit(response, oid, "Persitent ID set in properties");

					} catch (StorageException ex) {
						log.error("Error accessing object '{}' in storage: ", oid, ex);
						errors = true;
					}

				}
			}
		}

		// *******************
		// Decision making

		// Errors have occurred, email someone and do nothing
		if (errors) {
			emailObjectLink(response, oid, "An error occurred curating this object, some"
					+ " manual intervention may be required; please see" + " the system logs.");
			audit(response, oid, "Errors during curation; aborted.");
			return response;
		}

		// ***
		// What should happen per task if we have already been curated?
		if (curated) {

			// Happy ending
			if (task.equals("curation-response")) {
				log.info("Confirmation of curated object '{}'.", oid);

				// Send out upstream responses to objects waiting
				JSONArray responses = data.writeArray("responses");
				for (Object thisResponse : responses) {
					JsonSimple json = new JsonSimple((JsonObject) thisResponse);
					String broker = json.getString(brokerUrl, "broker");
					String responseOid = json.getString(null, "oid");
					String responseTask = json.getString(null, "task");
					JsonObject responseObj = createTask(response, broker, responseOid, responseTask);
					// Don't forget to tell them where it came from
					String id = json.getString(null, "quoteId");
					if (id != null) {
						responseObj.put("originId", id);
					}
					responseObj.put("originOid", oid);
					// If NLA Integration is enabled, use the NLA ID instead
					if (nlaIntegrationEnabled && metadata.containsKey(nlaIdProperty) && useNlaIdForRelationships) {
						responseObj.put("curatedPid", metadata.getProperty(nlaIdProperty));
					} else {
						responseObj.put("curatedPid", thisPid);
					}
				}
				//We've responded so let's clear the response list for next time
				responses.clear();
				saveObjectData(data, oid);

				// Set a flag to let publish events that may come in later
				// that this is ready to publish (if not already set)
				if (!metadata.containsKey(READY_PROPERTY)) {
					try {
						DigitalObject object = storage.getObject(oid);
						metadata = object.getMetadata();
						metadata.setProperty(READY_PROPERTY, "ready");
						object.close();
						metadata = getObjectMetadata(oid);
						audit(response, oid, "This object is ready for publication");

					} catch (StorageException ex) {
						log.error("Error accessing object '{}' in storage: ", oid, ex);
						emailObjectLink(response, oid, "This object is ready for publication, but an"
								+ " error occured writing to storage. Please" + " see the system log");
					}

					// Since the flag hasn't been set we also know this is the
					// first time through, so generate some notifications
					emailObjectLink(response, oid,
							"This email is confirming that the object linked" + " below has completed curation.");
					audit(response, oid, "Curation completed.");
				}

				// Schedule a followup to re-index and transform
				createTask(response, oid, "reharvest");
				return response;
			}

			// A response has come back from downstream
			if (task.equals("curation-pending")) {
				String childOid = message.getString(null, "originOid");
				String childId = message.getString(null, "originId");
				String curatedPid = message.getString(null, "curatedPid");

				boolean isReady = false;
				try {
					// False here will make sure we aren't sending out a bunch
					// of requests again.
					isReady = checkChildren(response, data, oid, thisPid, false, childOid, childId, curatedPid);
				} catch (TransactionException ex) {
					log.error("Error updating related objects '{}': ", oid, ex);
					emailObjectLink(response, oid, "An error occurred curating this object, some"
							+ " manual intervention may be required; please see" + " the system logs.");
					audit(response, oid, "Errors curating relations; aborted.");
					return response;
				}

				// If it is ready
				if (isReady) {
					createTask(response, oid, "curation-response");
				}
				return response;
			}

			// The object has finished in-house curation
			if (task.equals("curation-confirm")) {
				// If NLA Integration is required and not completed yet
				if (nlaIntegrationEnabled && !metadata.containsKey(nlaIdProperty)) {
					// Make sure we only run for required datasources (Parties
					// People at this stage)
					boolean sendToNla = false;
					for (String key : nlaIncludeTest.keySet()) {
						String value = metadata.getProperty(key);
						String testValue = nlaIncludeTest.get(key);
						if (value != null && value.equals(testValue)) {
							sendToNla = true;
						}
					}

					if (sendToNla) {
						// Check if we've released the party into the NLA feed
						// yet
						if (!metadata.containsKey(NLA_READY_PROPERTY) || !metadata.containsKey(NLA_DATE_PROPERTY)) {
							try {
								DigitalObject object = storage.getObject(oid);
								metadata = object.getMetadata();
								// Set Date
								metadata.setProperty(NLA_DATE_PROPERTY, nlaDate.format(new Date()));
								// Set Flag
								metadata.setProperty(NLA_READY_PROPERTY, "ready");
								// Cleanup
								object.close();
								metadata = getObjectMetadata(oid);
								audit(response, oid, "This object is ready to go to the NLA");
								// The EAC-CPF Template needs to be updated
								createTask(response, oid, "reharvest");

							} catch (StorageException ex) {
								log.error("Error accessing object '{}' in storage: ", oid, ex);
								emailObjectLink(response, oid, "This object is ready for the NLA, but an"
										+ " error occured writing to storage. Please" + " see the system log");
								return response;
							}

							// Since the flag hasn't been set we also know this
							// is the
							// first time through, so generate some
							// notifications
							emailObjectLink(response, oid,
									"This email is confirming that the object linked"
											+ " below has completed curation and is ready to"
											+ " be harvested by the National Library. NOTE:"
											+ " This object is not ready for publication until"
											+ " after the NLA has harvested it.");
						} else {
							audit(response, oid, "Curation attempt: This object is still waiting on the NLA");
						}
						return response;
					}
				} // Finish NLA

				// The object has finished, work on downstream 'children'
				boolean isReady = false;
				try {
					isReady = checkChildren(response, data, oid, thisPid, true);
				} catch (TransactionException ex) {
					log.error("Error processing related objects '{}': ", oid, ex);
					emailObjectLink(response, oid, "An error occurred curating this object, some"
							+ " manual intervention may be required; please see" + " the system logs.");
					audit(response, oid, "Errors curating relations; aborted.");
					return response;
				}

				// If it is ready on the first pass...
				if (isReady) {
					createTask(response, oid, "curation-response");
				} else {
					// Otherwise we are going to have to wait for children
					audit(response, oid, "Curation complete, but still waiting" + " on relations.");
				}

				return response;
			}

			// Since it is already curated, we are just storing any new
			// relationships / responses and passing things along
			if (task.equals("curation-request") || task.equals("curation-query")) {
				alreadyCurated = message.getBoolean(false, "alreadyCurated");
				try {
					storeRequestData(message, oid);
				} catch (TransactionException ex) {
					log.error("Error storing request data '{}': ", oid, ex);
					emailObjectLink(response, oid, "An error occurred curating this object, some"
							+ " manual intervention may be required; please see" + " the system logs.");
					audit(response, oid, "Errors during curation; aborted.");
					return response;
				}
				// Requests
				if (task.equals("curation-request")) {
					JsonObject taskObj = createTask(response, oid, "curation");
					taskObj.put("alreadyCurated", true);
					return response;

					// Queries
				} else {
					// Rather then push to 'curation-response' we are just
					// sending a single response to the querying object
					JsonSimple respond = new JsonSimple(message.getObject("respond"));
					String broker = respond.getString(brokerUrl, "broker");
					String responseOid = respond.getString(null, "oid");
					String responseTask = respond.getString(null, "task");
					JsonObject responseObj = createTask(response, broker, responseOid, responseTask);
					// Don't forget to tell them where it came from
					responseObj.put("originOid", oid);
					responseObj.put("curatedPid", thisPid);
				}
			}

			// Same as above, but this is a second stage request, let's be a
			// little sterner in case log filtering is occurring
			if (task.equals("curation")) {
				alreadyCurated = message.getBoolean(false, "alreadyCurated");
				log.info("Request to curate ignored. This object '{}' has" + " already been curated.", oid);
				JsonObject taskObj = createTask(response, oid, "curation-confirm");
				taskObj.put("alreadyCurated", true);
				return response;
			}

			// ***
			// What should happen per task if we have *NOT* already been
			// curated?
		} else {
			// Whoops! We shouldn't be confirming or responding to a non-curated
			// item!!!
			if (task.equals("curation-confirm") || task.equals("curation-pending")) {
				emailObjectLink(response, oid,
						"ERROR: Something has gone wrong with curation of this" + " object. The system has received a '"
								+ task + "'" + " event, but the record does not appear to be"
								+ " curated. Please check the system logs for any" + " errors.");
				return response;
			}

			// Standard stuff - a request to curate non-curated data
			if (task.equals("curation-request")) {
				try {
					storeRequestData(message, oid);
				} catch (TransactionException ex) {
					log.error("Error storing request data '{}': ", oid, ex);
					emailObjectLink(response, oid, "An error occurred curating this object, some"
							+ " manual intervention may be required; please see" + " the system logs.");
					audit(response, oid, "Errors during curation; aborted.");
					return response;
				}

				if (manualConfirmation) {
					emailObjectLink(response, oid, "A curation request has been recieved for this"
							+ " object. You can find a link below to approve" + " the request.");
					audit(response, oid, "Curation request received. Pending");
				} else {
					createTask(response, oid, "curation");
				}
				return response;
			}

			// We can't do much here, just store the response address
			if (task.equals("curation-query")) {
				try {
					storeRequestData(message, oid);
				} catch (TransactionException ex) {
					log.error("Error storing request data '{}': ", oid, ex);
					emailObjectLink(response, oid, "An error occurred curating this object, some"
							+ " manual intervention may be required; please see" + " the system logs.");
					audit(response, oid, "Errors during curation; aborted.");
					return response;
				}
				return response;
			}

			// The actual curation event
			if (task.equals("curation")) {
				audit(response, oid, "Object curation requested.");
				List<String> list = itemConfig.getStringList("transformer", "curation");

				// Pass through whichever curation transformer are configured
				if (list != null && !list.isEmpty()) {
					for (String id : list) {
						JsonObject order = newTransform(response, id, oid);
						JsonObject config = (JsonObject) order.get("config");
						// Make sure it even has an override...
						JsonObject override = itemConfig.getObject("transformerOverrides", id);
						if (override != null) {
							config.putAll(override);
						}
					}

				} else {
					log.warn("This object has no configured transformers!");
				}

				// Force an index update after the ID has been created,
				// but before "curation-confirm"
				JsonObject order = newIndex(response, oid);
				order.put("forceCommit", true);

				// Don't forget to come back
				createTask(response, oid, "curation-confirm");
				return response;
			}
		}

		log.error("Invalid message received. Unknown task:\n{}", message.toString(true));
		emailObjectLink(response, oid, "The curation manager has received an invalid curation message"
				+ " for this object. Please see the system logs.");
		return response;
	}

	/**
	 * Look through all known related objects and assess their readiness. Can
	 * optionally send downstream curation requests if required, and update a
	 * relationship based on responses.
	 * 
	 * @param response
	 *            The response currently being built
	 * @param data
	 *            The object's data
	 * @param oid
	 *            The object's ID
	 * @param sendRequests
	 *            True if curation requests should be sent out
	 * @returns boolean True if all 'children' have been curated.
	 * @throws TransactionException
	 *             If an error occurs
	 */
	private boolean checkChildren(JsonSimple response, JsonSimple data, String thisOid, String thisPid,
			boolean sendRequests) throws TransactionException {
		return checkChildren(response, data, thisOid, thisPid, sendRequests, null, null, null);
	}

	/**
	 * Look through all known related objects and assess their readiness. Can
	 * optionally send downstream curation requests if required, and update a
	 * relationship based on responses.
	 * 
	 * @param response
	 *            The response currently being built
	 * @param data
	 *            The object's data
	 * @param oid
	 *            The object's ID
	 * @param sendRequests
	 *            True if curation requests should be sent out
	 * @param childOid
	 * @returns boolean True if all 'children' have been curated.
	 * @throws TransactionException
	 *             If an error occurs
	 */
	private boolean checkChildren(JsonSimple response, JsonSimple data, String thisOid, String thisPid,
			boolean sendRequests, String childOid, String childId, String curatedPid) throws TransactionException {

		boolean isReady = true;
		boolean saveData = false;
		log.debug("Checking Children of '{}'", thisOid);

		JSONArray relations = data.writeArray("relationships");
		for (Object relation : relations) {
			JsonSimple json = new JsonSimple((JsonObject) relation);
			String broker = json.getString(brokerUrl, "broker");
			boolean localRecord = broker.equals(brokerUrl);
			String relatedId = json.getString(null, "identifier");

			// We need to find OIDs to match IDs... for local records
			String relatedOid = json.getString(null, "oid");
			if (relatedOid == null && localRecord) {
				String identifier = json.getString(null, "identifier");
				if (identifier == null) {
					throw new TransactionException("Cannot resolve identifer: " + identifier);
				}
				relatedOid = idToOid(identifier);
				if (relatedOid == null) {
					throw new TransactionException("Cannot resolve identifer: " + identifier);
				}
				((JsonObject) relation).put("oid", relatedOid);
				saveData = true;
			}

			// Are we updating a relationship... and is it this one?
			boolean updatingById = (childId != null && childId.equals(relatedId));
			boolean updatingByOid = (childOid != null && childOid.equals(relatedOid));
			if (curatedPid != null && (updatingById || updatingByOid)) {
				log.debug("Updating...");
				((JsonObject) relation).put("isCurated", true);
				((JsonObject) relation).put("curatedPid", curatedPid);
				saveData = true;
			}

			// Is this relationship using a curated ID?
			boolean isCurated = json.getBoolean(false, "isCurated");
			if (!isCurated) {
				log.debug(" * Needs curation '{}'", relatedOid);
				isReady = false;
				// Only send out curation requests if asked to
				if (sendRequests) {
					JsonObject task;
					broker = json.getString(null, "broker");
					// It is a local object
					if (broker == null) {
						task = createTask(response, relatedOid, "curation-query");
						// Or remote
					} else {
						task = createTask(response, broker, relatedOid, "curation-query");
					}

					// If this record is the authority on the relationship
					// make sure we tell the other object what its relationship
					// back to us should be.
					boolean authority = json.getBoolean(false, "authority");
					if (authority) {
						// Send a full request rather then a query, we need it
						// to propogate through children
						task.put("task", "curation-request");

						// Let the other object know its reverse relationship
						// with us and that we've already been curated.
						String reverseRelationship = json.getString("hasAssociationWith", "reverseRelationship");
						JsonObject relObject = new JsonObject();
						relObject.put("identifier", thisPid);
						relObject.put("curatedPid", thisPid);
						relObject.put("broker", brokerUrl);
						relObject.put("isCurated", true);
						relObject.put("relationship", reverseRelationship);
						// Make sure we send OID to local records
						if (localRecord) {
							relObject.put("oid", thisOid);
						}
						JSONArray newRelations = new JSONArray();
						newRelations.add(relObject);
						task.put("relationships", newRelations);
					}

					// And make sure it knows how to send us curated PIDs
					JsonObject msgResponse = new JsonObject();
					msgResponse.put("broker", brokerUrl);
					msgResponse.put("oid", thisOid);
					msgResponse.put("task", "curation-pending");
					task.put("respond", msgResponse);
				}
			} else {
				log.debug(" * Already curated '{}'", relatedOid);
			}
		}

		// Save our data if we changed it
		if (saveData) {
			saveObjectData(data, thisOid);
		}

		return isReady;
	}

	private String idToOid(String identifier) {
		// Build a query
		String query = "known_ids:\"" + identifier + "\"";
		SearchRequest request = new SearchRequest(query);
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		// Now search and parse response
		SolrResult result = null;
		try {
			indexer.search(request, out);
			InputStream in = new ByteArrayInputStream(out.toByteArray());
			result = new SolrResult(in);
		} catch (Exception ex) {
			log.error("Error searching Solr: ", ex);
			return null;
		}

		// Verify our results
		if (result.getNumFound() == 0) {
			log.error("Cannot resolve ID '{}'", identifier);
			return null;
		}
		if (result.getNumFound() > 1) {
			log.error("Found multiple OIDs for ID '{}'", identifier);
			return null;
		}

		// Return our result
		SolrDoc doc = result.getResults().get(0);
		return doc.getFirst("storage_id");
	}

	/**
	 * Store the important parts of the request data for later use.
	 * 
	 * @param message
	 *            The JsonSimple message to store
	 * @param oid
	 *            The Object to store the message in
	 * @throws TransactionException
	 *             If an error occurred
	 */
	private void storeRequestData(JsonSimple message, String oid) throws TransactionException {

		// Get our incoming data to look at
		JsonObject toRespond = message.getObject("respond");
		JSONArray newRelations = message.getArray("relationships");
		if (toRespond == null && newRelations == null) {
			log.warn("This request requires no responses and specifies" + " no relationships.");
			return;
		}

		// Get from storage
		DigitalObject object = null;
		Payload payload = null;
		InputStream inStream = null;
		try {
			object = storage.getObject(oid);
			payload = object.getPayload(DATA_PAYLOAD_ID);
			inStream = payload.open();
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			throw new TransactionException(ex);
		}

		// Parse existing data
		JsonSimple metadata = null;
		try {
			metadata = new JsonSimple(inStream);
			inStream.close();
		} catch (IOException ex) {
			log.error("Error parsing/reading JSON '{}'", oid, ex);
			throw new TransactionException(ex);
		}

		// Store our new response
		if (toRespond != null) {
			JSONArray responses = metadata.writeArray("responses");
			boolean duplicate = false;
			String newOid = (String) toRespond.get("oid");
			for (Object response : responses) {
				String oldOid = (String) ((JsonObject) response).get("oid");
				if (newOid.equals(oldOid)) {
					log.debug("Ignoring duplicate response request by '{}'" + " on object '{}'", newOid, oid);
					duplicate = true;
				}
			}
			if (!duplicate) {
				log.debug("New response requested by '{}' on object '{}'", newOid, oid);
				responses.add(toRespond);
			}
		}

		// Store relationship(s), with some basic de-duping
		if (newRelations != null) {
			JSONArray relations = metadata.writeArray("relationships");
			for (JsonSimple newRelation : JsonSimple.toJavaList(newRelations)) {
				boolean duplicate = false;
				// Relationships have multiple keys. String comparison of
				// the JSON will catch this sometimes, but a housekeeping
				// job periodically cleans up dupes that make it through.

				// When building the string for comparison is needs to be
				// done before any alterations, so basically as it it was
				// recieved.
				String uniqueString = newRelation.toString();

				// Compare to each existing relationship
				for (JsonSimple relation : JsonSimple.toJavaList(relations)) {
					String storedUnique = relation.getString(null, "uniqueString");
					if (uniqueString.equals(storedUnique)) {
						log.debug("Ignoring duplicate relationship '{}'", oid);
						duplicate = true;
					}
				}

				// Store new entries
				if (!duplicate) {
					log.debug("New relationship added to '{}'", oid);
					newRelation.getJsonObject().put("uniqueString", uniqueString);
					relations.add(newRelation.getJsonObject());
				}
			}
		}

		// Store modifications
		if (toRespond != null || newRelations != null) {
			log.info("Updating object in storage '{}'", oid);
			String jsonString = metadata.toString(true);
			try {
				inStream = new ByteArrayInputStream(jsonString.getBytes("UTF-8"));
				object.updatePayload(DATA_PAYLOAD_ID, inStream);
			} catch (Exception ex) {
				log.error("Unable to store data '{}': ", oid, ex);
				throw new TransactionException(ex);
			}
		}
	}

	/**
	 * Get the requested object ready for publication. This would typically just
	 * involve setting a flag
	 * 
	 * @param message
	 *            The incoming message
	 * @param oid
	 *            The object identifier to publish
	 * @return JsonSimple The response object
	 * @throws TransactionException
	 *             If an error occurred
	 */
	private JsonSimple publish(JsonSimple message, String oid) throws TransactionException {
		log.debug("Publishing '{}'", oid);
		JsonSimple response = new JsonSimple();
		try {
			DigitalObject object = storage.getObject(oid);
			Properties metadata = object.getMetadata();
			// Already published?
			if (!metadata.containsKey(PUBLISH_PROPERTY)) {
				metadata.setProperty(PUBLISH_PROPERTY, "true");
				object.close();
				log.info("Publication flag set '{}'", oid);
				audit(response, oid, "Publication flag set");
			} else {
				log.info("Publication flag is already set '{}'", oid);
			}
		} catch (StorageException ex) {
			throw new TransactionException("Error setting publish property: ", ex);
		}

		// Make a final pass through the curation tool(s),
		// allows for external publication. eg. VITAL
		JsonSimple itemConfig = getConfigFromStorage(oid);
		if (itemConfig == null) {
			log.error("Error accessing item configuration!");
		} else {
			List<String> list = itemConfig.getStringList("transformer", "curation");

			if (list != null && !list.isEmpty()) {
				for (String id : list) {
					JsonObject order = newTransform(response, id, oid);
					JsonObject config = (JsonObject) order.get("config");
					JsonObject overrides = itemConfig.getObject("transformerOverrides", id);
					if (overrides != null) {
						config.putAll(overrides);
					}
				}
			}
		}

		// Don't forget to publish children
		publishRelations(response, oid);
		return response;
	}

	/**
	 * Send out requests to all relations to publish
	 * 
	 * @param oid
	 *            The object identifier to publish
	 * @throws TransactionException 
	 */
	private void publishRelations(JsonSimple response, String oid) throws TransactionException {
		log.debug("Publishing Children of '{}'", oid);

		JsonSimple data = getDataFromStorage(oid);
		if (data == null) {
			log.error("Error accessing item data! '{}'", oid);
			emailObjectLink(response, oid, "An error occured publishing the related objects for this"
					+ " record. Please check the system logs.");
			return;
		}

		JSONArray relations = data.writeArray("relationships");
		for (Object relation : relations) {
			JsonSimple json = new JsonSimple((JsonObject) relation);
			String broker = json.getString(brokerUrl, "broker");
			boolean localRecord = broker.equals(brokerUrl);
			String relatedId = json.getString(null, "identifier");

			// We need to find OIDs to match IDs (only for local records)
			String relatedOid = json.getString(null, "oid");
			if (relatedOid == null && localRecord) {
				String identifier = json.getString(null, "identifier");
				if (identifier == null) {
					log.error("Cannot resolve identifer: '{}'", identifier);
				}
				relatedOid = idToOid(identifier);
				if (relatedOid == null) {
					log.error("Cannot resolve identifer: '{}'", identifier);
				}
			}

			boolean relationPublishRequested = json.getBoolean(false, "relationPublishedRequested");
			boolean authority = json.getBoolean(false, "authority");
			if (authority && !relationPublishRequested) {
				// Is this relationship using a curated ID?
				boolean isCurated = json.getBoolean(false, "isCurated");
				if (isCurated) {
					log.debug(" * Publishing '{}'", relatedId);
					JsonObject task;
					// It is a local object
					if (localRecord) {
						task = createTask(response, relatedOid, "publish");
						// Or remote
					} else {
						task = createTask(response, broker, relatedOid, "publish");
						// We won't know OIDs for remote systems
						task.remove("oid");
						task.put("identifier", relatedId);
					}
					log.error(" * Writing relationPublishedRequested for '{}'", relatedId);
					json.getJsonObject().put("relationPublishedRequested",true);
					saveObjectData(data, oid);
				} else {
					log.debug(" * Ignoring non-curated relationship '{}'", relatedId);
				}
			}
		}
	}

	/**
	 * Processing method
	 * 
	 * @param message
	 *            The JsonSimple message to process
	 * @return JsonSimple The actions to take in response
	 * @throws TransactionException
	 *             If an error occurred
	 */
	@Override
	public JsonSimple parseMessage(JsonSimple message) throws TransactionException {
		log.debug("\n{}", message.toString(true));

		// A standard harvest event
		JsonObject harvester = message.getObject("harvester");
		if (harvester != null) {
			try {
				String oid = message.getString(null, "oid");
				JsonSimple response = new JsonSimple();
				audit(response, oid, "Tool Chain");

				// Standard transformers... ie. not related to curation
				scheduleTransformers(message, response);

				// Solr Index
				newIndex(response, oid);

				// Send a message back here
				createTask(response, oid, "clear-render-flag");
				return response;
			} catch (Exception ex) {
				throw new TransactionException(ex);
			}
		}

		// It's not a harvest, what else could be asked for?
		String task = message.getString(null, "task");
		if (task != null) {
			String oid = message.getString(null, "oid");

			// ######################
			if (task.equals("workflow")) {
				JsonSimple response = new JsonSimple();

				String eventType = message.getString(null, "eventType");
				String newStep = message.getString(null, "newStep");
				// The workflow has altered data, run the tool chain
				if (newStep != null || eventType.equals("ReIndex")) {
					// For housekeeping, we need to alter the
					// Solr index fairly speedily
					boolean quickIndex = message.getBoolean(false, "quickIndex");
					if (quickIndex) {
						JsonObject order = newIndex(response, oid);
						order.put("forceCommit", true);
						try {
							indexer.index(oid);
						} catch (IndexerException e) {
							throw new TransactionException(e);
						}
					}

					// send a copy to the audit log
					JsonObject order = newSubscription(response, oid);
					JsonObject audit = (JsonObject) order.get("message");
					audit.putAll(message.getJsonObject());

					// Then business as usual
					reharvest(response, message);

					// Once the dust settles come back here
					createTask(response, oid, "curation-request");

					// A traditional Subscriber message for audit logs
				} else {
					JsonObject order = newSubscription(response, oid);
					JsonObject audit = (JsonObject) order.get("message");
					audit.putAll(message.getJsonObject());
				}
				return response;
			}

			// ######################
			// Start a reharvest for this object
			if (task.equals("reharvest")) {
				JsonSimple response = new JsonSimple();
				reharvest(response, message);
				return response;
			}

			// ######################
			// Tool chain, clear render flag
			if (task.equals("clear-render-flag")) {
				if (oid != null) {
					clearRenderFlag(oid);
				} else {
					log.error("Cannot clear render flag without an OID!");
				}
			}

			// ######################
			// Curation
			if (task.startsWith("curation")) {
				try {
					if (oid == null) {
						// See if we've been given an identifier before we fail
						String id = message.getString(null, "identifier");
						oid = idToOid(id);
						// We are going to OID inside mint, but when responding
						// we need to remember to quote the identifier
						if (oid != null) {
							message.writeObject("respond").put("quoteId", id);
						}
					}

					if (oid != null) {
						JsonSimple response = curation(message, task, oid);

						// We should always index afterwards
						JsonObject order = newIndex(response, oid);
						order.put("forceCommit", true);
						return response;

					} else {
						log.error("We need an OID to curate!");
					}
				} catch (Exception ex) {
					JsonSimple response = new JsonSimple();
					log.error("Error during curation: ", ex);
					emailObjectLink(response, oid,
							"An unknown error occurred curating this object. " + "Please check the system logs.");
					return response;
				}
			}

			// ######################
			// Publication
			if (task.startsWith("publish")) {
				try {
					if (oid == null) {
						oid = idToOid(message.getString(null, "identifier"));
						// Update out message so the reharvest function gets OID
						if (oid != null) {
							message.getJsonObject().put("oid", oid);
						}
					}
					if (oid != null) {
						JsonSimple response = publish(message, oid);
						// We should always go through the tool chain afterwards
						reharvest(response, message);
						return response;

					} else {
						log.error("We need an OID to publish!");
					}
				} catch (Exception ex) {
					JsonSimple response = new JsonSimple();
					log.error("Error during publication: ", ex);
					emailObjectLink(response, oid,
							"An unknown error occurred publishing this object." + " Please check the system logs.");
					return response;
				}
			}
		}

		// Do nothing
		return new JsonSimple();
	}

	/**
	 * Generate a fairly common list of orders to transform and index an object.
	 * This mirrors the traditional tool chain.
	 * 
	 * @param message
	 *            The response to modify
	 * @param message
	 *            The message we received
	 */
	private void reharvest(JsonSimple response, JsonSimple message) {
		String oid = message.getString(null, "oid");

		try {
			if (oid != null) {
				setRenderFlag(oid);

				// Transformer config
				JsonSimple itemConfig = getConfigFromStorage(oid);
				if (itemConfig == null) {
					log.error("Error accessing item configuration!");
					return;
				}
				itemConfig.getJsonObject().put("oid", oid);

				// Tool chain
				scheduleTransformers(itemConfig, response);
				newIndex(response, oid);
				createTask(response, oid, "clear-render-flag");
			} else {
				log.error("Cannot reharvest without an OID!");
			}
		} catch (Exception ex) {
			log.error("Error during reharvest setup: ", ex);
		}
	}

	/**
	 * Generate an order to send an email to the intended recipient with a link
	 * to an object
	 * 
	 * @param response
	 *            The response to add an order to
	 * @param message
	 *            The message we want to send
	 */
	private void emailObjectLink(JsonSimple response, String oid, String message) {
		String link = urlBase + "default/detail/" + oid;
		String text = "This is an automated message from the ";
		text += "Mint Curation Manager.\n\n" + message;
		text += "\n\nYou can find this object here:\n" + link;
		email(response, oid, text);
	}

	/**
	 * Generate an order to send an email to the intended recipient
	 * 
	 * @param response
	 *            The response to add an order to
	 * @param message
	 *            The message we want to send
	 */
	private void email(JsonSimple response, String oid, String text) {
		JsonObject object = newMessage(response, EmailNotificationConsumer.LISTENER_ID);
		JsonObject message = (JsonObject) object.get("message");
		message.put("to", emailAddress);
		message.put("body", text);
		message.put("oid", oid);
	}

	/**
	 * Generate an order to add a message to the System's audit log
	 * 
	 * @param response
	 *            The response to add an order to
	 * @param oid
	 *            The object ID we are logging
	 * @param message
	 *            The message we want to log
	 */
	private void audit(JsonSimple response, String oid, String message) {
		JsonObject order = newSubscription(response, oid);
		JsonObject messageObject = (JsonObject) order.get("message");
		messageObject.put("eventType", message);
	}

	/**
	 * Generate orders for the list of normal transformers scheduled to execute
	 * on the tool chain
	 * 
	 * @param message
	 *            The incoming message, which contains the tool chain config for
	 *            this object
	 * @param response
	 *            The response to edit
	 * @param oid
	 *            The object to schedule for clearing
	 */
	private void scheduleTransformers(JsonSimple message, JsonSimple response) {
		String oid = message.getString(null, "oid");
		List<String> list = message.getStringList("transformer", "metadata");
		if (list != null && !list.isEmpty()) {
			for (String id : list) {
				JsonObject order = newTransform(response, id, oid);
				// Add item config to message... if it exists
				JsonObject itemConfig = message.getObject("transformerOverrides", id);
				if (itemConfig != null) {
					JsonObject config = (JsonObject) order.get("config");
					config.putAll(itemConfig);
				}
			}
		}
	}

	/**
	 * Clear the render flag for objects that have finished in the tool chain
	 * 
	 * @param oid
	 *            The object to clear
	 */
	private void clearRenderFlag(String oid) {
		try {
			DigitalObject object = storage.getObject(oid);
			Properties props = object.getMetadata();
			props.setProperty("render-pending", "false");
			object.close();
		} catch (StorageException ex) {
			log.error("Error accessing storage for '{}'", oid, ex);
		}
	}

	/**
	 * Set the render flag for objects that are starting in the tool chain
	 * 
	 * @param oid
	 *            The object to set
	 */
	private void setRenderFlag(String oid) {
		try {
			DigitalObject object = storage.getObject(oid);
			Properties props = object.getMetadata();
			props.setProperty("render-pending", "true");
			object.close();
		} catch (StorageException ex) {
			log.error("Error accessing storage for '{}'", oid, ex);
		}
	}

	/**
	 * Create a task. Tasks are basically just trivial messages that will come
	 * back to this manager for later action.
	 * 
	 * @param response
	 *            The response to edit
	 * @param oid
	 *            The object to schedule for clearing
	 * @param task
	 *            The task String to use on receipt
	 * @return JsonObject Access to the 'message' node of this task to provide
	 *         further details after creation.
	 */
	private JsonObject createTask(JsonSimple response, String oid, String task) {
		return createTask(response, null, oid, task);
	}

	/**
	 * Create a task. This is a more detailed option allowing for tasks being
	 * sent to remote brokers.
	 * 
	 * @param response
	 *            The response to edit
	 * @param broker
	 *            The broker URL to use
	 * @param oid
	 *            The object to schedule for clearing
	 * @param task
	 *            The task String to use on receipt
	 * @return JsonObject Access to the 'message' node of this task to provide
	 *         further details after creation.
	 */
	private JsonObject createTask(JsonSimple response, String broker, String oid, String task) {
		JsonObject object = newMessage(response, TransactionManagerQueueConsumer.LISTENER_ID);
		if (broker != null) {
			object.put("broker", broker);
		}
		JsonObject message = (JsonObject) object.get("message");
		message.put("task", task);
		message.put("oid", oid);
		return message;
	}

	/**
	 * Creation of new Orders with appropriate default nodes
	 * 
	 */
	private JsonObject newIndex(JsonSimple response, String oid) {
		JsonObject order = createNewOrder(response, TransactionManagerQueueConsumer.OrderType.INDEXER.toString());
		order.put("oid", oid);
		return order;
	}

	private JsonObject newMessage(JsonSimple response, String target) {
		JsonObject order = createNewOrder(response, TransactionManagerQueueConsumer.OrderType.MESSAGE.toString());
		order.put("target", target);
		order.put("message", new JsonObject());
		return order;
	}

	private JsonObject newSubscription(JsonSimple response, String oid) {
		JsonObject order = createNewOrder(response, TransactionManagerQueueConsumer.OrderType.SUBSCRIBER.toString());
		order.put("oid", oid);
		JsonObject message = new JsonObject();
		message.put("oid", oid);
		message.put("context", "Curation");
		message.put("eventType", "Sending test message");
		message.put("user", "system");
		order.put("message", message);
		return order;
	}

	private JsonObject newTransform(JsonSimple response, String target, String oid) {
		JsonObject order = createNewOrder(response, TransactionManagerQueueConsumer.OrderType.TRANSFORMER.toString());
		order.put("target", target);
		order.put("oid", oid);
		order.put("config", new JsonObject());
		return order;
	}

	private JsonObject createNewOrder(JsonSimple response, String type) {
		JsonObject order = response.writeObject("orders", -1);
		order.put("type", type);
		return order;
	}

	/**
	 * Get the stored harvest configuration from storage for the indicated
	 * object.
	 * 
	 * @param oid
	 *            The object we want config for
	 */
	private JsonSimple getConfigFromStorage(String oid) {
		String configOid = null;
		String configPid = null;

		// Get our object and look for its config info
		try {
			DigitalObject object = storage.getObject(oid);
			Properties metadata = object.getMetadata();
			configOid = metadata.getProperty("jsonConfigOid");
			configPid = metadata.getProperty("jsonConfigPid");
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}

		// Validate
		if (configOid == null || configPid == null) {
			log.error("Unable to find configuration for OID '{}'", oid);
			return null;
		}

		// Grab the config from storage
		try {
			DigitalObject object = storage.getObject(configOid);
			Payload payload = object.getPayload(configPid);
			try {
				return new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error accessing config '{}' in storage: ", configOid, ex);
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing object in storage: ", ex);
		}

		// Something screwed the pooch
		return null;
	}

	/**
	 * Get the stored data from storage for the indicated object.
	 * 
	 * @param oid
	 *            The object we want
	 */
	private JsonSimple getDataFromStorage(String oid) {
		// Get our data from Storage
		Payload payload = null;
		try {
			DigitalObject object = storage.getObject(oid);
			payload = object.getPayload(DATA_PAYLOAD_ID);
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}

		// Parse the JSON
		try {
			try {
				return new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error parsing data '{}': ", oid, ex);
				return null;
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing data '{}' in storage: ", oid, ex);
			return null;
		}
	}

	/**
	 * Get the metadata properties for the indicated object.
	 * 
	 * @param oid
	 *            The object we want config for
	 */
	private Properties getObjectMetadata(String oid) {
		try {
			DigitalObject object = storage.getObject(oid);
			return object.getMetadata();
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}
	}

	/**
	 * Save the provided object data back into storage
	 * 
	 * @param data
	 *            The data to save
	 * @param oid
	 *            The object we want it saved in
	 */
	private void saveObjectData(JsonSimple data, String oid) throws TransactionException {
		// Get from storage
		DigitalObject object = null;
		try {
			object = storage.getObject(oid);
			object.getPayload(DATA_PAYLOAD_ID);
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			throw new TransactionException(ex);
		}

		// Store modifications
		String jsonString = data.toString(true);
		try {
			InputStream inStream = new ByteArrayInputStream(jsonString.getBytes("UTF-8"));
			object.updatePayload(DATA_PAYLOAD_ID, inStream);
		} catch (Exception ex) {
			log.error("Unable to store data '{}': ", oid, ex);
			throw new TransactionException(ex);
		}
	}

}
