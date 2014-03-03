/*
 * The Fascinator - Plugin - Transformer - Ingested Relationships
 * Copyright (C) 2011 University of Southern Queensland
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

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transformer.Transformer;
import com.googlecode.fascinator.api.transformer.TransformerException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transformer is designed to fire only once after each item is ingested.
 * It is primarily concerned with resolving relationships between parties.
 * 
 * @author Greg Pendlebury
 */
public class IngestedRelationshipsTransformer implements Transformer {
	/** Property flag to indicate execution */
	public static String PROPERTY_FLAG = "IngestedRelationshipsTransformer";

	/** Logging **/
	private static Logger log = LoggerFactory
			.getLogger(IngestedRelationshipsTransformer.class);

	/** Configuration */
	private JsonSimpleConfig config;

	/** Storage layer */
	private Storage storage;

	/**
	 * Constructor
	 */
	public IngestedRelationshipsTransformer() {
	}

	/**
	 * Init method from file
	 * 
	 * @param jsonFile
	 * @throws IOException
	 * @throws PluginException
	 */
	@Override
	public void init(File jsonFile) throws PluginException {
		try {
			config = new JsonSimpleConfig(jsonFile);
			reset();
		} catch (IOException e) {
			throw new PluginException("Error reading config: ", e);
		}
	}

	/**
	 * Init method from String
	 * 
	 * @param jsonString
	 * @throws IOException
	 * @throws PluginException
	 */
	@Override
	public void init(String jsonString) throws PluginException {
		try {
			config = new JsonSimpleConfig(jsonString);
			reset();
		} catch (IOException e) {
			throw new PluginException("Error reading config: ", e);
		}
	}

	/**
	 * Reset the transformer in preparation for a new object
	 */
	private void reset() throws TransformerException {
		// First time execution, prepare storage
		if (storage == null) {
			try {
				String storageType = config.getString(null, "storage", "type");
				storage = PluginManager.getStorage(storageType);
				storage.init(JsonSimpleConfig.getSystemFile());
			} catch (Exception ex) {
				throw new TransformerException(ex);
			}
		}
	}

	/**
	 * Transform method
	 * 
	 * @param object
	 *            : DigitalObject to be transformed
	 * @param jsonConfig
	 *            : String containing configuration for this item
	 * @return DigitalObject The object after being transformed
	 * @throws TransformerException
	 */
	@Override
	public DigitalObject transform(DigitalObject in, String jsonConfig)
			throws TransformerException {
		// Read item config and reset before we start
		JsonSimpleConfig itemConfig = null;
		try {
			itemConfig = new JsonSimpleConfig(jsonConfig);
		} catch (IOException ex) {
			throw new TransformerException("Error reading item configuration!",
					ex);
		}
		reset();

		// Test, have we done this before?
		Properties properties = null;
		try {
			properties = in.getMetadata();
		} catch (StorageException ex) {
			throw new TransformerException("Error reading properties: ", ex);
		}
		boolean hasRun = properties.containsKey(PROPERTY_FLAG);
		if (hasRun) {
			return in;
		}

		// Where are we getting out data from?
		String pid = itemConfig.getString(null, "sourcePid");
		JsonSimple data = getJsonFromStorage(in, pid);
		if (data == null) {
			log.error("Failed to retrieve data from storage: '{}'", in.getId());
			return in;
		}
		// Look in config for what relationships to map
		boolean saveChanges = false;
		Map<String, JsonSimple> relations = itemConfig
				.getJsonSimpleMap("relations");
		
		// Fix for REDBOXHELP-22: Drop all existing relationships.
		if (relations != null && relations.size() > 0) {
			data.getJsonObject().remove("relationships");	
		}

		// And loop through them all
		for (String field : relations.keySet()) {
			List<String> path = itemConfig.getStringList("sourcePath");
			if (path == null) {
				// Empty is ok, null is not
				path = new ArrayList<String>();
			}

			// Get our data
			path.add(field);

			// As some fields can be ingested as multi-values, we'll use a List
			List<String> values = null;
			values = data.getStringList(path.toArray());
			if (values == null) {
				String value = data.getString(null, path.toArray());
				if (value != null && !value.equals("")) {
					values = new ArrayList<String>();
					values.add(value);
				}
			}
			if (values != null) {
				// And work out supporting values
				String prefix = relations.get(field).getString(null, "prefix");
				String relation = relations.get(field).getString(
						"hasAssociationWith", "relation");
				String reverseRelation = relations.get(field).getString(
						"hasAssociationWith", "reverseRelation");

				// Now finish up
				if (prefix == null) {
					log.error("Relationship '{}' has incorrect configuration!",
							field);
				} else {
					for (String value : values) {
						if (!value.equals("")) {
							// Add this relationship (if needed)
							if (addWithDuplicateTest(data, prefix + value,
									relation, reverseRelation)) {
								saveChanges = true;
							}
						}
					}
				}
			}
		}

		// If changed, store
		if (saveChanges) {
			saveObjectData(in, data, pid);
			// Value is not important, just having it set
			// Fix for REDBOXHELP-22: Commented out next line to allow so this transformer can update the relationships and primary_group_id
			// properties.setProperty(PROPERTY_FLAG, "hasRun");
			try {
				in.close();
			} catch (StorageException ex) {
				throw new TransformerException("Error updating properties: ",
						ex);
			}
		}

		return in;
	}

	/**
	 * Retrieve and parse the indicated JSON payload from storage
	 * 
	 * @param in
	 *            The incoming object
	 * @param pid
	 *            The payload holding JSON
	 */
	private boolean addWithDuplicateTest(JsonSimple data, String identifier,
			String relationship, String reverseRelationship) {
		// Find what we have already
		boolean found = false;
		JSONArray relations = data.writeArray("relationships");
		for (Object relation : relations) {
			JsonSimple existing = new JsonSimple((JsonObject) relation);
			String existingId = existing.getString(null, "identifier");
			if (existingId != null && existingId.equals(identifier)) {
				found = true;
			}
		}

		// We're done if it already exists
		if (found) {
			return false;
		}

		// Time to add our new relationship to the JSON
		log.info("New relationship ITEM '{}' => '{}'", relationship, identifier);
		JsonObject json = new JsonObject();
		json.put("identifier", identifier);
		json.put("relationship", relationship);
		json.put("reverseRelationship", reverseRelationship);
		json.put("authority", true);
		relations.add(json);
		return true;
	}

	/**
	 * Retrieve and parse the indicated JSON payload from storage
	 * 
	 * @param in
	 *            The incoming object
	 * @param pid
	 *            The payload holding JSON
	 */
	private JsonSimple getJsonFromStorage(DigitalObject in, String pid) {
		// Get our data from Storage
		Payload payload = null;
		try {
			payload = in.getPayload(pid);
		} catch (StorageException ex) {
			log.error("Error accessing payload '{}' in storage: ", pid, ex);
			return null;
		}

		// Parse the JSON
		try {
			try {
				return new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error parsing data '{}': ", pid, ex);
				return null;
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing data '{}' in storage: ", pid, ex);
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
	private void saveObjectData(DigitalObject in, JsonSimple data, String pid)
			throws TransformerException {
		String jsonString = data.toString(true);
		try {
			InputStream inStream = new ByteArrayInputStream(
					jsonString.getBytes("UTF-8"));
			in.updatePayload(pid, inStream);
		} catch (Exception ex) {
			log.error("Unable to store data '{}': ", pid, ex);
			throw new TransformerException(ex);
		}
	}

	/**
	 * Get Transformer ID
	 * 
	 * @return id
	 */
	@Override
	public String getId() {
		return "ingest-relations";
	}

	/**
	 * Get Transformer Name
	 * 
	 * @return name
	 */
	@Override
	public String getName() {
		return "Ingested Relationships Transformer";
	}

	/**
	 * Gets a PluginDescription object relating to this plugin.
	 * 
	 * @return a PluginDescription
	 */
	@Override
	public PluginDescription getPluginDetails() {
		return new PluginDescription(this);
	}

	/**
	 * Shut down the transformer plugin
	 */
	@Override
	public void shutdown() throws PluginException {
		if (storage != null) {
			storage.shutdown();
		}
	}
}
