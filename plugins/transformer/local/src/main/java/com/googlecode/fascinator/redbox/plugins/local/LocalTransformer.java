/*
 * The Fascinator - Plugin - Transformer - Local Curation
 * Copyright (C) 2011 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
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
package com.googlecode.fascinator.redbox.plugins.local;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transformer.Transformer;
import com.googlecode.fascinator.api.transformer.TransformerException;
import com.googlecode.fascinator.common.DummyFileLock;
import com.googlecode.fascinator.common.JsonSimpleConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Create and store Persistent Identifiers based on configurable templates.</p>
 *
 * <h3>Configuration</h3>
 * <p>Keep in mind that each data source can provide and overriding template.</p>
 *
 * <table border="1">
 * <tr>
 *   <th>Option</th>
 *   <th>Description</th>
 *   <th>Required</th>
 *   <th>Default</th>
 * </tr>
 * 
 * <tr>
 *   <td>id</td>
 *   <td>Id of the transformer</td>
 *   <td><b>Yes</b></td>
 *   <td>local</td>
 * </tr>
 * 
 * <tr>
 *   <td>template</td>
 *   <td>The template to evaluate in creating Persistent IDs. Supports placeholder
 * values:
 *     <ul>
 *       <li><b>[[OID]]</b> - The Object ID being transformed</li>
 *       <li><b>[[INC]]</b> - An auto-incrementing number (if configured)</li>
 *     </ul>
 *   </td>
 *   <td><b>No</b></td>
 *   <td>pid:[[OID]]</td>
 * </tr>
 * 
 * <tr>
 *   <td>useIncrements</td>
 *   <td>Boolean flag to decide if auto-incrementing numbers are in use.</td>
 *   <td><b>No</b></td>
 *   <td>false</td>
 * </tr>
 * 
 * <tr>
 *   <td>incrementingFile</td>
 *   <td>If the above flag is set, this File is used to store/read the current
 * value of the sequence.</td>
 *   <td><b>Yes</b> (if 'useIncrements' is set)</td>
 *   <td>N/A</td>
 * </tr>
 * </table>
 * 
 * <p>There is also some related configuration in the Curation Manager that this
 * Transformer looks for under "curation" > "pidProperty". This value decides
 * on the metadata property where the Persistent IDs should be stored.</p>
 * 
 * @author Greg Pendlebury
 */
public class LocalTransformer implements Transformer {
    /** Default configuration for items */
    private static String DEFAULT_TEMPLATE = "pid:[[OID]]";

    /** Lock file for the Incrementing index */
    private static String INDEX_LOCK_FILE = "index.lock";

    /** Logging **/
    private static Logger log = LoggerFactory.getLogger(LocalTransformer.class);

    /** Configuration */
    private JsonSimpleConfig config;

    /** Storage layer */
    private Storage storage;

    /** Flag whether to use incrementing numbers in Ids */
    private boolean useIncrement;

    /** The file on disk to use in storing the incrementing index */
    private File indexFile;

    /** The lock used to mutex against the index file */
    private DummyFileLock indexLock;

    /** Flag for first execution */
    private boolean firstExecution;

    /** Curation PID Property */
    private String pidProperty;

    /**
     * Constructor
     */
    public LocalTransformer() {
        firstExecution = true;
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
        if (firstExecution) {
            if (storage == null) {
                try {
                    String storageType = config.getString(null,
                            "storage", "type");
                    storage = PluginManager.getStorage(storageType);
                    storage.init(JsonSimpleConfig.getSystemFile());
                } catch (Exception ex) {
                    throw new TransformerException(ex);
                }
            }

            // Where are we storing our finished PIDs
            pidProperty = config.getString("localPid",
                    "curation", "pidProperty");
            if (pidProperty == null || "".equals(pidProperty)) {
                throw new TransformerException("No (or invalid) PID property"
                        + " found in config");
            }

            // Is an auto-incrementing number required?
            useIncrement = config.getBoolean(false,
                    "transformerDefaults", "local", "useIncrements");
            if (useIncrement) {
                // Find where we are storing the index
                String path = config.getString(null,
                        "transformerDefaults", "local", "incrementingFile");
                if (path == null) {
                    throw new TransformerException("No auto incrementing" +
                            " path specified, but required!");
                }

                // Check it is 'real'
                indexFile = new File(path);
                if (indexFile == null || !indexFile.exists()) {
                    throw new TransformerException("The auto incrementing " +
                            "file specified does not exist: '" + path +"'");
                }
                // Create a locking file beside the real file
                File lockFile = new File(
                        indexFile.getParentFile(), INDEX_LOCK_FILE);
                try {
                    if (!lockFile.exists()) {
                        lockFile.getParentFile().mkdirs();
                        lockFile.createNewFile();
                    }
                    indexLock = new DummyFileLock(lockFile.getAbsolutePath());
                } catch(IOException ex) {
                    throw new TransformerException(
                            "Error creating lock file: ", ex);
                }

                // Check we can read/use it
                if (!checkIncrementFile()) {
                    throw new TransformerException(
                            "Error on initial check of increment file!");
                }
            }

            // Make sure we don't end up here again
            firstExecution = false;
        }
    }

    /**
     * Acquire a lock on the index file. Method will not return until the lock
     * is acquired or an error is thrown.
     *
     * @throws IOException if any errors occur.
     */
    private void lockIndex() throws IOException {
        //log.debug(" * Locking Index : " + getId());
        indexLock.getLock();
        //log.debug(" * Index locked : " + getId());
    }

    /**
     * Release a lock on the index file. Method will not return until the lock
     * is released or an error is thrown.
     *
     * @throws IOException if any errors occur.
     */
    private void unlockIndex() throws IOException {
        //log.debug(" * Unlocking Index : " + getId());
        indexLock.release();
        //log.debug(" * Index unlocked : " + getId());
    }

    /**
     * Get the next increment from disk, updating the file for next time.
     *
     * @return String: The next increment to use in templates. NULL if errors occur
     */
    private String getNextIncrement() {
        String result = null;

        if (!useIncrement) {
            return null;
        }

        // Lock the index file
        try {
            lockIndex();
        } catch (IOException ex) {
            log.error("Error acquiring file lock: ", ex);
            return null;
        }

        // Do our thing
        try {
            String oldInc = FileUtils.readFileToString(indexFile);
            int inc = Integer.valueOf(oldInc);
            inc++;
            result = String.valueOf(inc);
            FileUtils.writeStringToFile(indexFile, result);
        } catch (Exception ex) {
            log.error("Error reading/updating increment: ", ex);
        }

        // Unlock the index file
        try {
            unlockIndex();
        } catch (IOException ex) {
            log.error("Error releasing file lock: ", ex);
        }

        return result;
    }

    /**
     * Check the contents of the increment file. Will remove spaces and
     * non-printables, but fail if the contents are not an integer.
     *
     * @return booelan: True if the file contains usable content, otherwise False
     */
    private boolean checkIncrementFile() {
        boolean result = true;
        if (!useIncrement) {
            return result;
        }

        // Lock the index file
        try {
            lockIndex();
        } catch (IOException ex) {
            log.error("Error acquiring file lock: ", ex);
            return false;
        }

        // Do our thing
        try {
            String contents = FileUtils.readFileToString(indexFile);

            // Do we need to 'fix' the contents?
            String newContents = contents.replaceAll("\\r|\\n|\\s", "");
            if (!newContents.equals(contents)) {
                log.warn("Removing extra whitespace from increment file!");
                FileUtils.writeStringToFile(indexFile, newContents);
            }

            // Make sure it is a number
            try {
                Integer.valueOf(newContents);
                result = true;
            } catch (Exception ex) {
                log.error("Error parsing integer '{}'; cannot use.",
                        newContents, ex);
                result = false;
            }
        } catch (Exception ex) {
            log.error("Error accessing file: ", ex);
            result = false;
        }

        // Unlock the index file
        try {
            unlockIndex();
        } catch (IOException ex) {
            log.error("Error releasing file lock: ", ex);
            return false;
        }

        return result;
    }

    /**
     * Transform method
     *
     * @param object : DigitalObject to be transformed
     * @param jsonConfig : String containing configuration for this item
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
            throw new TransformerException(
                    "Error reading item configuration!", ex);
        }
        reset();

        // What should the ID look like?
        String template = itemConfig.getString(DEFAULT_TEMPLATE, "template");

        // Do we already have a persitent ID?
        Properties metadata = null;
        try {
            metadata = in.getMetadata();
        } catch (StorageException ex) {
            throw new TransformerException("Error retrieving metadata", ex);
        }
        String pId = metadata.getProperty(pidProperty);
        boolean propertySet = false;
        if (pId != null) {
            propertySet = true;
            log.info("Object already has a Persistent ID: '{}'", pId);

        // A new PID is required
        } else {
            // We should be ready to go now
            try {
                pId = createPid(template, in.getId());
                if (pId == null) {
                    log.error("Error during Persistent ID creation!");
                    return in;
                }
            } catch (Exception ex) {
                log.error("Error during Persistent ID creation: ", ex);
                return in;
            }

            // Success!
            log.info("Persistent ID created: '{}'", pId);
        }

        // Store the output - metadata
        if (!propertySet) {
            metadata.setProperty(pidProperty, pId);
            try {
                in.close();
            } catch (StorageException ex) {
                log.error("Error storing metadata for Persistent ID: ", ex);
            }
        }

        return in;
    }

    /**
     * Create a Persistent ID for the specified Object, using the required
     * template.
     *
     * @param template: The Persistent ID template
     * @param oid: The ID of the object we are transforming
     * @return String: The newly created Persistent ID
     * @throws TransformerException: If any errors occur during the process
     */
    private String createPid(String template, String oid)
            throws TransformerException {
        // Basic, OID in template
        String pid = template.replace("[[OID]]", oid);

        // Optionally more complicated, auto incrementing numbers
        if (template.contains("[[INC]]")) {
            if (useIncrement) {
                String inc = getNextIncrement();
                if (inc == null) {
                    log.error("Error accessing next increment!");
                    return null;
                }
                pid = pid.replace("[[INC]]", inc);
            } else {
                log.error("[[INC]] used in template," +
                        " and increments not configured");
                return null;
            }
        }
        return pid;
    }

    /**
     * Get Transformer ID
     *
     * @return id
     */
    @Override
    public String getId() {
        return "local";
    }

    /**
     * Get Transformer Name
     *
     * @return name
     */
    @Override
    public String getName() {
        return "Local Curation Transformer";
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
