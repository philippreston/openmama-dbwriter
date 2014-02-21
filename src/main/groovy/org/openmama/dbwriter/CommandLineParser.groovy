/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.openmama.dbwriter

import java.util.logging.Level;
import static java.util.logging.Level.*

/**
 *
 * @author Philip Preston (ppreston@nyx.com)
 */
class CommandLineParser {

    String sourceName;
    String transportName;
    String dictSourceName;
    String dictTransportName;
    String dictFileName;
    boolean requireInitial;
    String middleware;
    String database;
    int numberOfThreads;
    Level logLevel;
    boolean quiet;
    int verbose;
    List subjects = [] as ArrayList;
    List fieldList = [] as ArrayList;

    CommandLineParser() {
        // Set defaults 
        sourceName = null;
        transportName = "sub";
        dictSourceName = "WOMBAT";
        dictTransportName = null;
        dictFileName = null;
        requireInitial = true;
        middleware = "wmw";
        database = "cassandra";
        numberOfThreads = 1;
        quiet = false;
        verbose = 0;
        logLevel = WARNING;
    }

    def parse(def args) {
        parse(args, "");
    }

    def parse(def args, def usageString) {

        def cmdLineBuilder = new CliBuilder(usage: usageString);
        cmdLineBuilder.S(args: 1, longOpt: "source", argName: 'Source Name', 'Source name for the publishing source')
        cmdLineBuilder.I(args: 1, 'Not require initial');
        cmdLineBuilder.d(args: 1, longOpt: "dict_source", argName: 'name', 'source name for dictionary');
        cmdLineBuilder.dict_tport(args: 1, argName: 'name', 'transport name for dictionary');
        cmdLineBuilder.dictionary(args: 1, argName: 'file', 'file to load dictionary');
        cmdLineBuilder.s(args: 1, argName: 'subject', 'subject to subscribe to');
        cmdLineBuilder.f(args: 1, argName: 'file', 'file of subjects to subscribe to');
        cmdLineBuilder.t(args: 1, longOpt: "tport", argName: 'name', 'transport name');
        cmdLineBuilder.m(args: 1, longOpt: "middleware", argName: 'name', 'middleware to use name');
        cmdLineBuilder.database(args: 1, argName: 'name', 'database to use name');
        cmdLineBuilder.threads(args: 1, argName: 'number', 'number of numberOfThreads to run');
        cmdLineBuilder.q('quiet');
        cmdLineBuilder.v('verbose');


        // Parse the options
        def options = cmdLineBuilder.parse(args);
        if (options.h) {
            cmdLineBuilder.usage();
            System.exit(1);
        }

        if (options.S)
            sourceName = options.S;

        if (options.I)
            requireInitial = true;

        if (options.d)
            dictSourceName = options.d;

        if (options.dict_tport)
            dictTransportName = options.dict_tport;

        if (options.dictionary)
            dictFileName = options.dictionary;

        if (options.t)
            transportName = options.t;

        if (options.m)
            middleware = options.m;

        if (options.database)
            database = options.database;

        if (options.f) {
            def subjectFile = new File(options.f as String);
            subjectFile.eachLine { subjects.push(it); }
        }

        if (options.nt)
            numberOfThreads = options.nt;

        if (options.s)
            subjects.push(options.s);

        if (options.q) {
            quiet = true;
            logLevel = SEVERE;
        }

        // Just count -v for verbose
        verbose = args.count { it == "-v"; }
        if (verbose > 0) {
            quiet = false;
            logLevel = getLevel(verbose);
        }

        // Anything else should be fields
        def extraArguments = options.arguments();
        if (extraArguments) {
            extraArguments.each {
                field ->
                    fieldList.push(field);
            }
        }
    }

    def clear() {
        sourceName = null;
        transportName = "sub";
        dictSourceName = "WOMBAT";
        dictTransportName = null;
        dictFileName = null;
        requireInitial = true;
        middleware = "wmw";
        database = "cassandra";
        numberOfThreads = 1;
        quiet = false;
        verbose = 0;
        logLevel = WARNING;

        subjects.clear();
        fieldList.clear();
    }

    static def getLevel(def value) {

        def loggingLevel = (value > 7) ? 7:value;

        switch(loggingLevel) {
            case 1:
                return INFO;
            case 2:
                return FINE;
            case 3:
                return FINER;
            case 4:
            case 5:
            case 6:
            case 7:
                return FINEST;
            default:
                return WARNING;
        }
    }

}

