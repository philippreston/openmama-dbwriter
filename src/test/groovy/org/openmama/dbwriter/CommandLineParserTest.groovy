/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.openmama.dbwriter

import org.junit.After
import org.junit.Before
import org.junit.Test
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertTrue;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.WARNING;

/**
 *
 * @author Philip Preston (philippreston@mac.com)
 */
class CommandLineParserTest {
    
    def args = [];
    def cmdLine;
    
    @Before
    void setup() {
        cmdLine = new CommandLineParser();
    }
    
    @After
    void tearDown() {
        cmdLine.clear();
    }

    @Test
    void testParse_empty() throws Exception {
        args = [];
        cmdLine.parse(args);

        // Get to here - we shouldn't have bombed
        assertTrue(true);
    }
    @Test(expected = NullPointerException)
    void testParse_null() throws Exception {
        args = null;
        cmdLine.parse(args);

        // Should have bombed by here
        assertTrue("Exception was not raised",false);
    }

    @Test
    void testParse_verbose() throws Exception {
        args = ['-v','-v'];
        cmdLine.parse(args);
        def expected_count = 2;
        def expected_level = FINE;
        def result_count = cmdLine.getVerbose();
        def result_level = cmdLine.getLogLevel();
        assertEquals(expected_count,result_count);
        assertEquals(expected_level,result_level);
    }

    @Test
    void testParse_verbose_default() throws Exception {
        cmdLine.parse(args);
        def expected_count = 0;
        def expected_level = WARNING;
        def result_count = cmdLine.getVerbose();
        def result_level = cmdLine.getLogLevel();
        assertEquals(expected_count,result_count);
        assertEquals(expected_level,result_level);
    }

    @Test
    void testParse_dict_source() throws Exception {
        def expected_source = 'NEW_DICT_SOURCE';
        args = ['-d',expected_source];
        cmdLine.parse(args);
        def result_source = cmdLine.getDictSourceName();
        assertEquals(expected_source,result_source);
    }

    @Test
    void testParse_dict_source_default() throws Exception {
        def expected_source = 'WOMBAT';
        cmdLine.parse(args);
        def result_source = cmdLine.getDictSourceName();
        assertEquals(expected_source,result_source);
    }

    @Test
    void testParse_dict_tport() throws Exception {
        def expected_tport = 'dict_sub';
        args = ['-dict_tport',expected_tport];
        cmdLine.parse(args);
        def result_tport = cmdLine.getDictTransportName();
        assertEquals(expected_tport,result_tport);
    }

    @Test
    void testParse_dict_tport_default() throws Exception {
        cmdLine.parse(args);
        def result_tport = cmdLine.getDictTransportName();
        assertNull(result_tport);
    }

    @Test
    void testParse_dict_file() throws Exception {
        def expected_file = 'file.txt';
        args = ['-dictionary',expected_file];
        cmdLine.parse(args);
        def result_file = cmdLine.getDictFileName();
        assertEquals(expected_file,result_file);
    }

    @Test
    void testParse_dict_file_default() throws Exception {
        cmdLine.parse(args);
        def result_file = cmdLine.getDictFileName();
        assertNull(result_file);
    }


}

