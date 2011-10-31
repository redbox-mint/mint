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

import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.transformer.Transformer;

import java.io.File;
import java.util.Properties;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalTransformerTest {
    private Storage storage;
    private Transformer local;

    @Before
    public void init() throws Exception {
        storage = PluginManager.getStorage("ram");
        storage.init("{}");
        local = PluginManager.getTransformer("local");
        local.init(new File(getClass().getResource("/testConfig.json")
                .toURI()));
    }

    @After
    public void cleanup() throws Exception {
        if (storage != null) {
            storage.shutdown();
        }
        if (local != null) {
            local.shutdown();
        }
    }

    /**
     * Check that a template is parsed correctly and the property is set
     * 
     * @throws Exception 
     */
    @Test
    public void transformDefault() throws Exception {
        DigitalObject object = storage.createObject("testObject1");

        // Defaults
        object = local.transform(object, "{}");
        Properties metadata = object.getMetadata();
        String pid = metadata.getProperty("testPid");
        Assert.assertEquals("pid:testObject1", pid);

        storage.removeObject(object.getId());
    }

    /**
     * Check that a template is parsed correctly and the property is set
     * 
     * @throws Exception 
     */
    @Test
    public void transformCustom() throws Exception {
        DigitalObject object = storage.createObject("testObject2");

        // Provide override
        object = local.transform(object,
                "{\"template\": \"[[OID]].test.domain.pid\"}");
        Properties metadata = object.getMetadata();
        String pid = metadata.getProperty("testPid");
        Assert.assertEquals("testObject2.test.domain.pid", pid);

        storage.removeObject(object.getId());
    }
}
