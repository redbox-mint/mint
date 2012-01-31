/*
 * The Fascinator - Plugin - Transformer - Handles
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
package com.googlecode.fascinator.redbox.plugins.handle;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transformer.Transformer;
import com.googlecode.fascinator.api.transformer.TransformerException;
import com.googlecode.fascinator.common.DummyFileLock;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.storage.StorageUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import net.handle.hdllib.AbstractMessage;
import net.handle.hdllib.AbstractResponse;
import net.handle.hdllib.AddValueRequest;
import net.handle.hdllib.AdminRecord;
import net.handle.hdllib.CreateHandleRequest;
import net.handle.hdllib.Encoder;
import net.handle.hdllib.ErrorResponse;
import net.handle.hdllib.HandleException;
import net.handle.hdllib.HandleResolver;
import net.handle.hdllib.HandleValue;
import net.handle.hdllib.ModifyValueRequest;
import net.handle.hdllib.PublicKeyAuthenticationInfo;
import net.handle.hdllib.Util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Create and store handles against a particular name authority. Most of the
 * handle code is a direct implementation of the examples provided at:
 * http://www.handle.net/
 * </p>
 *
 * <p>
 * Download the system and it will be bundled with all source, including samples
 * on handle creation: src.java/net/handle/apps/simple/HDLCreate.java
 * </p>
 *
 * <h3>Configuration</h3>
 * <p>Keep in mind that each data source can provide an overriding values.</p>
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
 *   <td>handle</td>
 * </tr>
 * 
 * <tr>
 *   <td>namingAuthority</td>
 *   <td>Your registered Handle naming authority (or prefix).</td>
 *   <td><b>Yes</b></td>
 *   <td>N/A</td>
 * </tr>
 * 
 * <tr>
 *   <td>privateKeyPath</td>
 *   <td>The path to a binary private key on disk. This is created as part of your Handle Server installation.</td>
 *   <td><b>Yes</b></td>
 *   <td>N/A</td>
 * </tr>
 * 
 * <tr>
 *   <td>passPhrase</td>
 *   <td>Pass phrase to unlock the private key (if it requires such).</td>
 *   <td><b>No</b></td>
 *   <td>N/A</td>
 * </tr>
 * 
 * <tr>
 *   <td>template</td>
 *   <td>The template to evaluate in creating Handle suffixes. Supports
 * placeholder values:
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
 *   <td>urlTemplate</td>
 *   <td>Similar to above, but this template decides how to build the URL this
 * Handle will resolve to. If none is provided, the Handle will not resolve.
 * Supports placeholder values:
 *     <ul>
 *       <li><b>[[OID]]</b> - The Object ID being transformed</li>
 *       <li><b>[[HANDLE]]</b> - The Handle value (this requires a two step creation process).</li>
 *     </ul>
 *   </td>
 *   <td><b>No</b></td>
 *   <td>N/A</td>
 * </tr>
 * 
 * <tr>
 *   <td>updateUrl</td>
 *   <td>Boolean flag used to force an update on URLs in Handles. For
 * performance reasons it is advised that this should only be set to 'true' if
 * the template for resolvable URLs has changed.</td>
 *   <td><b>No</b></td>
 *   <td>false</td>
 * </tr>
 * 
 * <tr>
 *   <td>source</td>
 *   <td>The source payload where description data can be found. The special
 * value of '.tfpackage' can be used to find whichever payload contains the
 * Facsinator package (eg. ReDBox Collections).</td>
 *   <td><b>No</b></td>
 *   <td>metadata.json</td>
 * </tr>
 * 
 * <tr>
 *   <td>description</td>
 *   <td>Instructs the Transformer on how to build a description String from
 * the source metadata. Has two child nodes:
 *     <ul>
 *       <li><b>seperator</b> - A basic String to use in between value.
 * Defaults to an empty String ("").</li>
 *       <li><b>paths</b> - A List of JSON paths (each being a List of Strings)
 * into the metadata in order of how they should be used in the description.
 * Has no default values, but if none are provided, or if none of those
 * provided are not found in the source, no Handle will be generated.</li>
 *     </ul>
 *   </td>
 *   <td><b>No</b></td>
 *   <td>N/A</td>
 * </tr>
 * 
 * <tr>
 *   <td>output</td>
 *   <td>The path to store the resulting Handle in the source metadata. Has two child nodes:
 *     <ul>
 *       <li><b>path</b> - A JSON Path (List of String) where the field will be.</li>
 *       <li><b>field</b> - The final field name to store the Handle in.</li>
 *     </ul>
 *   </td>
 *   <td><b>No</b></td>
 *   <td>Combination of both values defaults to "metadata" > "dc:identifier"</td>
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
 * 
 * <tr>
 *   <td>publishedDomain</td>
 *   <td>The publicly accessible domain you will use to publish your Handles.
 * Technically you can point this to your local Handle server, but it is
 * <b>strongly<b> advised that the default Handle Network domain should be used.</td>
 *   <td><b>No</b></td>
 *   <td>hdl.handle.net</td>
 * </tr>
 * </table>
 * 
 * <p>There is also some related configuration in the Curation Manager that this
 * Transformer looks for under "curation" > "pidProperty". This value decides
 * on the metadata property where the Handle should be stored for the
 * Curation Manager to find it.</p>
 * 
 * @author Greg Pendlebury
 */
public class HandleTransformer implements Transformer {

    /** The web domain to stick in front of our servers */
    private static String HANDLE_DEFAULT_DOMAIN = "hdl.handle.net";

    /** Static values used during handle creation */
    private static int ADMIN_INDEX = 100;
    private static int PUBLIC_INDEX = 300;
    private static int URL_INDEX = 3;
    private static String ADMIN_TYPE = "HS_ADMIN";
    private static String DESC_TYPE = "DESC";
    private static String URL_TYPE = "URL";

    /** Default configuration for items */
    private static String DEFAULT_SOURCE = "metadata.json";
    private static String DEFAULT_TEMPLATE = "[[OID]]";
    private static String[] DEFAULT_OUTPUT = {"metadata", "dc.identifier"};

    /** Lock file for the Incrementing index */
    private static String INDEX_LOCK_FILE = "index.lock";

    /** Logging **/
    private static Logger log = LoggerFactory
            .getLogger(HandleTransformer.class);

    /** Configuration */
    private JsonSimpleConfig config;

    /** Storage layer */
    private Storage storage;

    /** Handle Resolver */
    private HandleResolver resolver;

    /** Keyed authentication data */
    private PublicKeyAuthenticationInfo authentication;

    /** Administrative Record */
    private AdminRecord admin;

    /** Naming Authority */
    private String namingAuthority;

    /** Flag whether to use incrementing numbers in handle creation */
    private boolean useIncrement;

    /** The file on disk to use in storing the incrementing index */
    private File indexFile;

    /** The lock used to mutex against the index file */
    private DummyFileLock indexLock;

    /** Curation PID Property */
    private String pidProperty;

    /** The base URL to prepend to Handles */
    private String handleBaseUrl;

    /**
     * Constructor
     */
    public HandleTransformer() {
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

        // First time execution of some of the Handle details
        if (resolver == null) {
            // Where are we storing our finished PIDs
            pidProperty = config.getString("handle",
                    "curation", "pidProperty");
            if (pidProperty == null || "".equals(pidProperty)) {
                throw new TransformerException("No (or invalid) PID property"
                        + " found in config");
            }

            // Do we have a naming authority? No need to evaluate the
            //  complicated stuff if we don't have this
            namingAuthority = config.getString(null,
                    "transformerDefaults", "handle", "namingAuthority");
            if (namingAuthority == null || namingAuthority.equals("")) {
                throw new TransformerException(
                        "No naming authority specified!");
            }
            // The methods below want the data as a byte array
            byte[] prefix = null;
            try {
                prefix = ("0.NA/" + namingAuthority).getBytes("UTF8");
            } catch(Exception ex) {
                throw new TransformerException(
                        "Error reading naming authority: ", ex);
            }

            // Our basic resolver... processes requests when they are ready
            resolver = new HandleResolver();
            //resolver.traceMessages = true;

            // Private key
            PrivateKey privateKey = null;
            try {
                byte[] key = readPrivateKey();
                byte[] passPhrase = readPassPhrase(key);
                key = Util.decrypt(key, passPhrase);
                privateKey = Util.getPrivateKeyFromBytes(key, 0);
            } catch(Exception ex) {
                throw new TransformerException(
                        "Error during key resolution: ", ex);
            }

            // Create our authentication object for this naming authority
            authentication = new PublicKeyAuthenticationInfo(prefix,
                    PUBLIC_INDEX, privateKey);

            // Set up an administrative record, used to stamp admin rights
            //  on new handles. All those 'true' flags give us full access
            admin = new AdminRecord(prefix, PUBLIC_INDEX,
                    true, true, true, true, true, true,
                    true, true, true, true, true, true);

            // Is an auto-incrementing number required?
            useIncrement = config.getBoolean(false,
                    "transformerDefaults", "handle", "useIncrements");
            if (useIncrement) {
                // Find where we are storing the index
                String path = config.getString(null,
                        "transformerDefaults", "handle", "incrementingFile");
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

            // Work out what the base URL for finished Handles will look like
            String handleDomain = config.getString(HANDLE_DEFAULT_DOMAIN,
                    "transformerDefaults", "handle", "publishedDomain");
            handleBaseUrl = "http://"+handleDomain+"/";
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
                log.error("Error parsing integer '{}'; cannot use.", newContents, ex);
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
     * Create a HandleValue object holding a resolvable URL for the handle
     *
     * @param url: The URL to resolve to
     * @return HandleValue: The instantiated value, NULL if errors occurred.
     */
    private HandleValue getUrlHandleValue(String url) {
        byte[] type = null;
        byte[] urlBytes = null;
        try {
            type = URL_TYPE.getBytes("UTF8");
            urlBytes = url.getBytes("UTF8");
        } catch (Exception ex) {
            log.error("Error creating URL handle value: ", ex);
            return null;
        }

        return createHandleValue(URL_INDEX, type, urlBytes);
    }

    /**
     * Create a HandleValue object holding a public description for the handle
     *
     * @param description: The description to use
     * @return HandleValue: The instantiated value, NULL if errors occurred.
     */
    private HandleValue getDescHandleValue(String description) {
        byte[] type = null;
        byte[] descBytes = null;
        try {
            type = DESC_TYPE.getBytes("UTF8");
            descBytes = description.getBytes("UTF8");
        } catch (Exception ex) {
            log.error("Error creating description handle value: ", ex);
            return null;
        }

        return createHandleValue(PUBLIC_INDEX, type, descBytes);
    }

    /**
     * Create a HandleValue object holding admin data to govern the handle
     *
     * @return HandleValue: The instantiated value, NULL if errors occurred.
     */
    private HandleValue getAdminHandleValue() {
        byte[] type = null;
        try {
            type = ADMIN_TYPE.getBytes("UTF8");
        } catch (Exception ex) {
            // This shouldn't occur, given that ADMIN_TYPE is static, but
            //  we'll return a null response if it ever does;
            log.error("Error creating admin handle value: ", ex);
            return null;
        }

        return createHandleValue(ADMIN_INDEX, type,
                Encoder.encodeAdminRecord(admin));
    }

    /**
     * Create a HandleValue using the index, type and value provided.
     *
     * @param index: The index to assign the value
     * @param type: The type of this value
     * @param value: The data to load into this value
     * @return HandleValue: The instantiated value
     */
    private HandleValue createHandleValue(int index, byte[] type, byte[] value) {
        return new HandleValue(index, type, value,
                // You shouldn't need to change any of this,
                //  see handle.net examples for details.
                HandleValue.TTL_TYPE_RELATIVE, 86400,
                now(), null,
                // Security, all rights except 'public write'
                true, true, true, false);
    }

    /**
     * Trivial wrapper to resolve the current time to an integer
     *
     * @return int: The time now as an integer
     */
    private int now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    /**
     * Reads a private key from the configured location and returns
     * in a byte array
     *
     * @return byte[]: The byte data of the private key
     * @throws TransformerException: If the key is not found or inaccessible
     */
    private byte[] readPrivateKey() throws TransformerException {
        // Make sure it's configured
        String keyPath = config.getString(null,
                "transformerDefaults", "handle", "privateKeyPath");
        if (keyPath == null) {
            throw new TransformerException("No private key provided!");
        }

        // Retrieve it
        try {
            // Access the file
            File file = new File(keyPath);
            if (file == null || !file.exists()) {
                throw new TransformerException(
                        "The private key file does not exist or cannot" +
                        " be found: '" + keyPath + "'");
            }
            FileInputStream stream = new FileInputStream(file);

            // Stream the file into a byte array
            byte[] response = IOUtils.toByteArray(stream);
            stream.close();
            return response;
        } catch (Exception ex) {
            throw new TransformerException("Error accessing file: ", ex);
        }
    }

    /**
     * <p>
     * Confirms that the provided private key actually requires a pass phrase
     * and looks for this in configuration. If not found (but required) an
     * error will be logged, but a null value will be returned. This is a
     * mis-configuration.
     * </p>
     *
     * <p>
     * Using the key will fail in this case and should be appropriately caught,
     * but this method will only through an exception if the configuration is
     * correct, there was just an error during retrieval.
     * </p>
     *
     * @param key: The private keey to check
     * @return byte[]: The byte data of the pass phrase, possibly null
     * @throws TransformerException: If the key is not inaccessible
     */
    private byte[] readPassPhrase(byte[] key) throws TransformerException {
        try {
            if (Util.requiresSecretKey(key)) {
                String password = config.getString(null,
                        "transformerDefaults", "handle", "passPhrase");
                if (password == null) {
                    log.error("The private key requires a pass phrase" +
                            " and none was provided!");
                }
                return password.getBytes("UTF8");
            }
        } catch(Exception ex) {
            throw new TransformerException("Error during key resolution: ", ex);
        }

        // Null is fine if no passphrase is required
        return null;
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

        // Where are we getting out data from?
        String source = itemConfig.getString(DEFAULT_SOURCE, "source");

        // Path information for the decsription
        // Looks something like this:
        //    "description": {
        //        "seperator": " ",
        //        "paths": [
        //            ["data", "Honorific"],
        //            ["data", "Given Name"],
        //            ["data", "Family Name"],
        //        ]
        //    },
        String seperator = itemConfig.getString("", "description", "seperator");
        List<String[]> elementPaths = new ArrayList<String[]>();
        // Find the path to each element of the description
        JSONArray array = itemConfig.getArray("description", "paths");
        for (Object element : array) {
            if (element instanceof JSONArray) {
                List<String> list = JsonSimple.getStringList(
                        (JSONArray) element);
                elementPaths.add(list.toArray(new String[] {}));
            }
        }

        // Where are we putting the data?
        List<String> outputList = itemConfig.getStringList("output", "path");
        String[] outputArray = null;
        if (outputList == null || outputList.isEmpty()) {
            outputArray = DEFAULT_OUTPUT;
        } else {
            outputArray = outputList.toArray(new String[] {});
        }
        String outputField = itemConfig.getString("dc.identifier",
                "output", "field");

        // What should the handle look like?
        String template = itemConfig.getString(DEFAULT_TEMPLATE, "template");

        // Now go get our data
        Payload payload = null;
        JsonSimple json = null;
        try {
            payload = in.getPayload(source);
            json = new JsonSimple(payload.open());
        } catch (StorageException ex) {
            log.error("Error accessing source payload: '{}'", source, ex);
            return in;
        } catch (IOException ex) {
            log.error("Error parsing json from payload: '{}'", source, ex);
            return in;
        } finally {
            if (payload != null) {
                try {
                    payload.close();
                } catch (Exception ex) {
                    log.error("Error closing payload: ", ex);
                }
            }
        }

        // URL Logic - Step 1
        boolean resolvable = false; // Are we resolving at all?
        boolean updateUrl = false;  // Are we forcing an update?
        boolean twoStepUrl = false; // Two-step process? (if URL contains the Handle)
        String urlTemplate = itemConfig.getString(null, "urlTemplate");
        String resolvedUrl = null;
        if (urlTemplate != null && !"".equals(urlTemplate)) {
            resolvable = true;
            updateUrl = itemConfig.getBoolean(updateUrl, "updateUrl");
            if (urlTemplate.contains("[[HANDLE]]")) {
                twoStepUrl = true;
            } else {
                resolvedUrl = resolveUrlTemplate(urlTemplate, in.getId(), null);
            }
        }

        // Do we already have a handle?
        Properties metadata = null;
        try {
            metadata = in.getMetadata();
        } catch (StorageException ex) {
            throw new TransformerException("Error retrieving metadata", ex);
        }
        String handle = metadata.getProperty(pidProperty);
        boolean propertySet = false;
        if (handle != null) {
            propertySet = true;
            log.info("Object already has a handle: '{}'", handle);
            // We are reseting this, since the URL should have been allocated
            //   on first creation. The 'updateUrl' flag needs to be set
            //   before we force another update.
            twoStepUrl = false;

        // A new handle is required
        } else {
            // What are we going to IN the handle
            List<String> descriptionParts = new ArrayList<String>();
            for (String[] path : elementPaths) {
                String part = json.getString(null, (Object[]) path);
                if (part != null) {
                    descriptionParts.add(part);
                } else {
                    log.warn("Description element was empty: '{}'", path);
                }
            }
            if (descriptionParts.isEmpty()) {
                log.error("Couldn't find any description elements!");
                return in;
            }
            String description = StringUtils.join(descriptionParts, seperator);
            if (description == null || description.equals("")) {
                log.error("The description field for this object is empty!");
                return in;
            }

            // We should be ready to go now
            try {
                // URL may or may not be NULL at this point
                handle = createHandle(template, in.getId(), description,
                        resolvedUrl);
                if (handle == null) {
                    log.error("Error during handle creation!");
                    return in;
                }
            } catch (Exception ex) {
                log.error("Error during handle creation: ", ex);
                return in;
            }

            // Success!
            log.info("Succeeded in handle creation: '{}'", handle);
        }

        // This logic below runs for both existing and new handles, to ensure
        //  they are stored appropriately if other transformers/harvesters are
        //  messing with the payloads.

        // Store the output - in payload
        JsonObject outputs = json.writeObject((Object[]) outputArray);
        outputs.put(outputField, handle);
        try {
            byte[] data = json.toString(true).getBytes("UTF-8");
            InputStream stream = new ByteArrayInputStream(data);
            // Write to the object
            StorageUtils.createOrUpdatePayload(in, source, stream);
        } catch (Exception ex) {
            log.error("Error storage JSON payload: ", ex);
            // We can't crash here... still need to try writing metadata
        }

        // Store the output - metadata
        if (!propertySet) {
            metadata.setProperty(pidProperty, handle);
            try {
                in.close();
            } catch (StorageException ex) {
                log.error("Error storing metadata for handle: ", ex);
            }
        }

        // URL Logic - Step 2 - If we need to Add/Update a
        //            resolvable URL to an existing Handle
        if (resolvable && (updateUrl || twoStepUrl)) {
            resolvedUrl = resolveUrlTemplate(urlTemplate, in.getId(), handle);
            if (resolvedUrl == null) {
                log.error("Error making Handle resolvable!");
                return in;
            }
            updateUrl(handle, resolvedUrl);
        }

        return in;
    }

    /**
     * Create a handle for the specified description, using the required
     * template.
     *
     * @param template: The handle suffix template
     * @param oid: The ID of the object we are transforming
     * @param description: The description to allocate to the new handle
     * @return String: The newly created handle, NULL if the suffix is not free
     * @throws TransformerException: If any errors occur during the process
     */
    private String createHandle(String template, String oid,
            String description, String url) throws TransformerException {

        String suffix = resolveTemplate(template, oid);
        if (suffix == null) {
            throw new TransformerException("Error building the handle suffix");
        }

        // Make sure the suffix is even valid
        String handle = namingAuthority + "/" + suffix;
        byte[] handleBytes = null;
        try {
            handleBytes = handle.getBytes("UTF8");
        } catch (Exception ex) {
            throw new TransformerException(
                    "Invalid encoding for Suffix: '" + suffix + "'", ex);
        }

        // Prepare the data going to be used inside the handle
        HandleValue adminVal = getAdminHandleValue();
        HandleValue descVal = getDescHandleValue(description);
        if (adminVal == null || descVal == null) {
            throw new TransformerException("Error creating HandleValues!");
        }

        HandleValue[] values = {adminVal, descVal};
        // Has URL - modify the array
        if (url != null) {
            HandleValue urlVal = getUrlHandleValue(url);
            if (urlVal == null) {
                throw new TransformerException("Error creating HandleValues!");
            }
            values = new HandleValue[] {adminVal, descVal, urlVal};
        }

        // Now prepare the actualy creationg request for sending
        CreateHandleRequest req = new CreateHandleRequest(
                handleBytes, values, authentication);

        // And send
        try {
            log.info("Sending handle create request ...");
            AbstractResponse response = resolver.processRequest(req);
            log.info("... response received.");

            // Success case
            if (response.responseCode != AbstractMessage.RC_SUCCESS) {
                // Failure case... but expected failure
                if (response.responseCode ==
                        AbstractMessage.RC_HANDLE_ALREADY_EXISTS) {
                    log.warn("Handle '{}' already in use", suffix);

                    // If configured, try again
                    if (template.contains("[[INC]]")) {
                        if (useIncrement) {
                            return createHandle(
                                    template, oid, description, url);
                        }
                    }
                }

                // Failure case... unexpected cause
                if (response instanceof ErrorResponse) {
                    throw new TransformerException("Error creating handle: " +
                            ((ErrorResponse) response).toString());

                } else {
                    throw new TransformerException("Unknown error during" +
                            " handle creation. The create API call has" +
                            " failed, but no error response was returned." +
                            " Message: '" +
                            AbstractMessage.getResponseCodeMessage(
                            response.responseCode) + "'");
                }
            }
        } catch (HandleException ex) {
            throw new TransformerException(
                    "Error attempting to create handle:", ex);
        }

        return handleBaseUrl + handle;
    }

    /**
     * Update the URL that the given handle resolves to.
     *
     * @param handle: The handle to update (including handle.net domain).
     * @param newUrl: The new URL to resolve to
     * @throws TransformerException: If any errors occur during the process
     */
    private void updateUrl(String handle, String newUrl)
            throws TransformerException {
        String basicHandle = handle.replace(handleBaseUrl, "");
        byte[] handleBytes = null;
        try {
            handleBytes = basicHandle.getBytes("UTF8");
        } catch (Exception ex) {
            throw new TransformerException(
                    "Invalid encoding for Handle: '" + handle + "'", ex);
        }

        HandleValue urlVal = getUrlHandleValue(newUrl);
        if (urlVal == null) {
            throw new TransformerException("Error creating HandleValues!");
        }

        // Now prepare the actualy creationg request for sending
        ModifyValueRequest req = new ModifyValueRequest(
                handleBytes, urlVal, authentication);

        // And send
        try {
            log.info("Sending handle updating request for URL...");
            AbstractResponse response = resolver.processRequest(req);
            log.info("... response received.");

            // Check for errors
            if (response.responseCode != AbstractMessage.RC_SUCCESS) {
                // Failure case... but expected failure
                if (response.responseCode ==
                        AbstractMessage.RC_VALUES_NOT_FOUND) {
                    log.info("Handle '{}' has no URL, adding", handle);
                    addUrl(handleBytes, urlVal);
                    return;
                }

                if (response instanceof ErrorResponse) {
                    throw new TransformerException("Error creating handle: " +
                            ((ErrorResponse) response).toString());

                } else {
                    throw new TransformerException("Unknown error during" +
                            " handle creation. The create API call has" +
                            " failed, but no error response was returned." +
                            " Message: '" +
                            AbstractMessage.getResponseCodeMessage(
                            response.responseCode) + "'");
                }
            }
        } catch (HandleException ex) {
            throw new TransformerException(
                    "Error attempting to update handle:", ex);
        }
    }

    /**
     * Basic add URL call to follow an update failure against a Handle that
     * doesn't have a URL. The parameter are expected to have already been
     * prepared inside of updateUrl().
     *
     * @param handleBytes: The Handle to update in byte array form
     * @param newUrl: The prepared HandleValue containing the URL
     * @throws TransformerException: If any errors occur during the process
     */
    private void addUrl(byte[] handleBytes, HandleValue newUrl)
            throws TransformerException {
        AddValueRequest req = new AddValueRequest(
                handleBytes, newUrl, authentication);

        // And send
        try {
            log.info("Sending handle ADD request for URL...");
            AbstractResponse response = resolver.processRequest(req);
            log.info("... response received.");

            // Check for errors
            if (response.responseCode != AbstractMessage.RC_SUCCESS) {
                if (response instanceof ErrorResponse) {
                    throw new TransformerException("Error creating handle: " +
                            ((ErrorResponse) response).toString());

                } else {
                    throw new TransformerException("Unknown error during" +
                            " handle creation. The create API call has" +
                            " failed, but no error response was returned." +
                            " Message: '" +
                            AbstractMessage.getResponseCodeMessage(
                            response.responseCode) + "'");
                }
            }
        } catch (HandleException ex) {
            throw new TransformerException(
                    "Error attempting to update handle:", ex);
        }
    }

    /**
     * Resolve the handle template to a suffix. If appropriately configured this
     * includes incrementing the index file.
     *
     * @param template: The handle suffix template
     * @param oid: The ID of the object we are transforming
     * @return String: The next suffix to use
     */
    private String resolveTemplate(String template, String oid) {
        String suffix = template.replace("[[OID]]", oid);
        if (template.contains("[[INC]]")) {
            if (useIncrement) {
                String inc = getNextIncrement();
                if (inc == null) {
                    log.error("Error accessing next increment!");
                    return null;
                }
                suffix = suffix.replace("[[INC]]", inc);
            } else {
                log.error("[[INC]] used in template," +
                        " and increments not configured");
                return null;
            }
        }
        return suffix;
    }

    /**
     * Resolve the URL template to a completed URL. Also validate that it can
     * be used as a URL.
     * 
     * @param template: The handle suffix template
     * @param oid: The ID of the object we are transforming
     * @param handle: The handle this URL will be put into
     * @return String: The complete and valid URL. Null if invalid or errors occur
     */
    private String resolveUrlTemplate(String template, String oid,
            String handle) {
        // Object IDs
        String response = template.replace("[[OID]]", oid);
        // Handle
        if (handle != null) {
            response = response.replace("[[HANDLE]]", handle);
        }
        try {
            // Validate
            URL url = new URL(response);
            return url.toString();
        } catch (MalformedURLException ex) {
            log.error("URL ({}) is invalid: ", response, ex);
            return null;
        }
    }

    /**
     * Get Transformer ID
     *
     * @return id
     */
    @Override
    public String getId() {
        return "handle";
    }

    /**
     * Get Transformer Name
     *
     * @return name
     */
    @Override
    public String getName() {
        return "Handle Transformer";
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
