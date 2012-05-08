/* 
 * The Fascinator - ReDBox/Mint SRU Client
 * Copyright (C) 2012 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
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
package com.googlecode.fascinator.redbox.sru;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * <p>A light-weight SRU client implementation. Originally written for the purpose
 * of searching the National Library of Australia's Party Infrastructure Project
 * (PIP).</p>
 * 
 * <p>More information/documentation for PIP is <a href="https://wiki.nla.gov.au/display/ARDCPIP/Documentation">available on the NLA wiki</a>.</p>
 * 
 * @author Greg Pendlebury
 * 
 * <p>Credit for some of inspiration has to go to another light-weight implementation
 * available under LGPL we looked at before we started coding: 
 * <a href="http://code.google.com/p/sinciput/source/browse/trunk/sinciput/src/com/technosophos/sinciput/sru/SRUClient.java">SRUClient</a> from 'Sinciput'.</p>
 * 
 */
public class SRUClientTest {
    SRUClient sru;

    @Test
    public void constructorsTest() throws Exception {
        // NLA defaults
        sru = new SRUClient();
        Assert.assertEquals(sru.generateSearchUrl("test='test'"), "http://www.nla.gov.au/apps/srw/search/peopleaustralia?version=1.1&recordSchema=urn%3Aisbn%3A1-931666-33-4&recordPacking=xml&operation=searchRetrieve&query=test%3D%27test%27");

        // Different URL
        sru = new SRUClient("http://www.nla.gov.au/apps/srw/search2/peopleaustralia");
        Assert.assertEquals(sru.generateSearchUrl("test='test'"), "http://www.nla.gov.au/apps/srw/search2/peopleaustralia?version=1.1&recordSchema=urn%3Aisbn%3A1-931666-33-4&recordPacking=xml&operation=searchRetrieve&query=test%3D%27test%27");

        // Switch to Dublin Core
        sru = new SRUClient("http://www.nla.gov.au/apps/srw/search2/peopleaustralia", "info:srw/schema/1/dc-v1.1");
        Assert.assertEquals(sru.generateSearchUrl("test='test'"), "http://www.nla.gov.au/apps/srw/search2/peopleaustralia?version=1.1&recordSchema=info%3Asrw%2Fschema%2F1%2Fdc-v1.1&recordPacking=xml&operation=searchRetrieve&query=test%3D%27test%27");

        // Alter response format away from XML
        sru = new SRUClient("http://www.nla.gov.au/apps/srw/search2/peopleaustralia", "info:srw/schema/1/dc-v1.1", "string");
        Assert.assertEquals(sru.generateSearchUrl("test='test'"), "http://www.nla.gov.au/apps/srw/search2/peopleaustralia?version=1.1&recordSchema=info%3Asrw%2Fschema%2F1%2Fdc-v1.1&recordPacking=string&operation=searchRetrieve&query=test%3D%27test%27");

        // Try a new version
        sru = new SRUClient("http://www.nla.gov.au/apps/srw/search2/peopleaustralia", "info:srw/schema/1/dc-v1.1", "string", "1.2");
        Assert.assertEquals(sru.generateSearchUrl("test='test'"), "http://www.nla.gov.au/apps/srw/search2/peopleaustralia?version=1.2&recordSchema=info%3Asrw%2Fschema%2F1%2Fdc-v1.1&recordPacking=string&operation=searchRetrieve&query=test%3D%27test%27");
    }

    private void printIdentity(NLAIdentity identity) {
        System.out.println("ID: " + identity.getId() + " => " + identity.getDisplayName() + " (" + identity.getInstitution() + ")");
        for (Map<String, String> id : identity.getKnownIdentities()) {
            String a = id.get("displayName");
            String b = id.get("surname");
            String c = id.get("firstName");
            String d = id.get("institution");
            System.out.println("  =>  " + a + " (displayName), " + b + " (surname), " + c + " (firstName), " + d + " (institution)");
        }
        System.out.println("");
    }

    // Simple ID query
    @Test
    public void searchIdTest() throws Exception {
        sru = new SRUClient();
        // Pre-canned response, we are really testing the parser
        sru.testResponseResource("sampleRecord.xml");
        String record = sru.nlaGetNationalId("nla.party-915373");
        Assert.assertEquals(record, "http://nla.gov.au/nla.party-915373");
    }

    // A basic search with nice name from the NLA
    @Test
    public void searchBodyTest() throws Exception {
        sru = new SRUClient();
        // Pre-canned response, we are really testing the parser. Results order etc. will vary over time on live searches
        sru.testResponseResource("sampleSearch.xml");
        List<NLAIdentity> response = sru.nlaGetIdentitiesBySearch("pa.surname=\"Smith\" AND pa.type=\"person\"");
        // Debugging
        for (NLAIdentity id : response) {
            printIdentity(id);
        }
        // Some random tests
        NLAIdentity id = response.get(3);
        Assert.assertEquals(id.getId(), "http://nla.gov.au/nla.party-460536");
        Assert.assertEquals(id.getDisplayName(), "Smith, Fay");
        Assert.assertEquals(id.getInstitution(), "Libraries Australia");
        id = response.get(6);
        Assert.assertEquals(id.getId(), "http://nla.gov.au/nla.party-460537");
        Assert.assertEquals(id.getDisplayName(), "Smith, Keith");
        Assert.assertEquals(id.getInstitution(), "Libraries Australia");
    }

    // Some less trustworthy data. Institutions are not all NLA and some
    //   names are not in the expected form. We also get the results
    //   slightly differently.
    @Test
    public void searchInstitutionTest() throws Exception {
        sru = new SRUClient();
        // Pre-canned response, we are really testing the parser
        sru.testResponseResource("sampleSearchInstitution.xml");
        SRUResponse response =  sru.nlaGetResponseBySearch("bath.possessingInstitution=\"AU-VU:EOAS\" AND pa.type=\"person\"");
        List<NLAIdentity> identities = NLAIdentity.convertNodesToIdentities(response.getResults());
        // Debugging
        for (NLAIdentity id : identities) {
            printIdentity(id);
        }
        // Test counts
        Assert.assertEquals(response.getRows(), 10);
        Assert.assertEquals(response.getTotalResults(), 4870);
        NLAIdentity id = identities.get(0);
        Assert.assertEquals(id.getId(), "http://nla.gov.au/nla.party-1475230");
        Assert.assertEquals(id.getDisplayName(), "Jones, Inigo Owen");
        Assert.assertEquals(id.getInstitution(), "National Library of Australia Party Infrastructure");
        id = identities.get(1);
        Assert.assertEquals(id.getId(), "http://nla.gov.au/nla.party-1475631");
        Assert.assertEquals(id.getDisplayName(), "Booth, John");
        Assert.assertEquals(id.getInstitution(), "Encyclopedia of Australian Science");
        id = identities.get(7);
        Assert.assertEquals(id.getId(), "http://nla.gov.au/nla.party-1476464");
        Assert.assertEquals(id.getDisplayName(), "Bradley, William Albert");
        Assert.assertEquals(id.getInstitution(), "Encyclopedia of Australian Science");
    }

    // People with multiple identities and with multiple maintenance agencies.
    @Test
    public void searchNlaTest() throws Exception {
        sru = new SRUClient();
        // Pre-canned response, we are really testing the parser
        sru.testResponseResource("sampleSearchMonash.xml");
        SRUResponse response =  sru.nlaGetResponseBySearch("cql.anywhere=\"monash\" AND pa.type=\"person\"");
        List<NLAIdentity> identities = NLAIdentity.convertNodesToIdentities(response.getResults());
        // Debugging
        for (NLAIdentity id : identities) {
            printIdentity(id);
        }
        NLAIdentity id = identities.get(0);
        Assert.assertEquals(id.getId(), "http://nla.gov.au/nla.party-458693");
        Assert.assertEquals(id.getDisplayName(), "Blackwood, Robert (Sir)");
        Assert.assertEquals(id.getInstitution(), "Encyclopedia of Australian Science");
        List<Map<String, String>> knownIds = id.getKnownIdentities();
        Assert.assertEquals(knownIds.size(), 4);
        Assert.assertEquals(knownIds.get(0).get("displayName"), "Blackwood, Robert (Sir)");
        Assert.assertEquals(knownIds.get(0).get("institution"), "National Library of Australia Party Infrastructure");
        Assert.assertEquals(knownIds.get(2).get("displayName"), "Blackwood, Robert Rutherford");
        Assert.assertEquals(knownIds.get(2).get("surname"), "Blackwood");
        Assert.assertEquals(knownIds.get(2).get("firstName"), "Robert Rutherford");
        Assert.assertEquals(knownIds.get(2).get("institution"), "Encyclopedia of Australian Science");
        
        id = identities.get(2);
        Assert.assertEquals(id.getId(), "http://nla.gov.au/nla.party-458899");
        Assert.assertEquals(id.getDisplayName(), "Burchill, Elizabeth");
        Assert.assertEquals(id.getInstitution(), "The  Australian Women's Register");
        knownIds = id.getKnownIdentities();
        Assert.assertEquals(knownIds.size(), 4);
        Assert.assertEquals(knownIds.get(0).get("displayName"), "Burchill, Elizabeth");
        Assert.assertEquals(knownIds.get(0).get("institution"), "National Library of Australia Party Infrastructure");
        Assert.assertEquals(knownIds.get(3).get("displayName"), "Burchill, Dora Elizabeth");
        Assert.assertEquals(knownIds.get(3).get("surname"), "Burchill");
        Assert.assertEquals(knownIds.get(3).get("firstName"), "Dora Elizabeth");
        Assert.assertEquals(knownIds.get(3).get("institution"), "Encyclopedia of Australian Science");
    }
}
