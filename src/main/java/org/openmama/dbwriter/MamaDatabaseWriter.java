/* $Id$
 *
 * OpenMAMA: The open middleware agnostic messaging API
 * Copyright (C) 2011 NYSE Technologies, Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.openmama.dbwriter;

import com.wombat.mama.*;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class MamaDatabaseWriter implements MamaSubscriptionCallback,
                                           MamaTransportListener {

    final public static int EXIT_FAIL = 1;
    final public static int EXIT_SUCCESS = 0;
    final private static Logger logger = Logger.getLogger("org.openmama.dbwriter");


    // OpenMAMA 
    final private List<MamaSubscription> subscriptions;
    final private MamaBridge myBridge;
    final private MamaQueueGroup queueGroup;
    private MamaQueue myDefaultQueue;
    private MamaSource mySource;
    private MamaSource myDictSource;
    private MamaTransport transport;
    private MamaTransport myDictTransport;
    private MamaDictionary dictionary;

    // OpenMAMA - Setup
    final private List<String> subjectList;
    @SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})      // TODO - would be good to add field filtering
    final private List<String> fieldList;
    final private String mySymbolNamespace;
    final private boolean requireInitial;
    final private String myDictTportName;
    final private String dictSource;
    final private String dictFile;
    final private String transportName;
    final private boolean quiet;
    private boolean dictionaryComplete;

    // Database
    final private MamaDatabase database;
    private long count;


    /**
     * Contains the amount of time that the example program will run for, if set
     * to 0 then it will run indefinitely.
     *
     * @param args Command line arguments
     * @throws java.lang.InterruptedException
     */
    public static void main(final String[] args) throws InterruptedException {

        // Parse Command Lines
        final CommandLineParser cmdLine = new CommandLineParser();
        cmdLine.parse(args);

        final MamaDatabaseWriter db_writer = MamaDatabaseWriter.getWriter(cmdLine);
        db_writer.init();

        System.out.println("Type CTRL-C to exit.");
        db_writer.run();


        System.exit(EXIT_SUCCESS);

    }

    // Singleton
    private static MamaDatabaseWriter writer;

    public static MamaDatabaseWriter getWriter(final CommandLineParser cmdLine) {

        if (writer == null) {
            writer = new MamaDatabaseWriter(cmdLine);
        }

        return writer;
    }

    @SuppressWarnings("unchecked")
    private MamaDatabaseWriter(final CommandLineParser cmdLine) {

        subscriptions = new ArrayList<>(100);
        dictionaryComplete = false;
        mySource = null;
        myDictSource = null;
        transport = null;
        myDictTransport = null;
        dictionary = null;
        count = 0L;

        // Get everything from the command line
        dictSource = cmdLine.getDictSourceName();
        dictFile = cmdLine.getDictFileName();
        myDictTportName = cmdLine.getDictTransportName();
        mySymbolNamespace = cmdLine.getSourceName();
        requireInitial = cmdLine.getRequireInitial();
        fieldList = cmdLine.getFieldList();
        subjectList = cmdLine.getSubjects();
        transportName = cmdLine.getTransportName();
        quiet = cmdLine.getQuiet();

        // Load bridge
        myBridge = Mama.loadBridge(cmdLine.getMiddleware());
        queueGroup = new MamaQueueGroup(cmdLine.getNumberOfThreads(), myBridge);
        database = getDatabase(cmdLine.getDatabase());
        Mama.setLogLevel(cmdLine.getLogLevel());
    }

    private MamaDatabase getDatabase(final String name) {

        // Which Database writer
        switch (name.toLowerCase()) {
            case "cassandra":
                return new MamaCassandra();
            case "mongodb":
                return new MamaMongodb();
            default:
                throw new IllegalArgumentException(String.format("%s is not valid database\n", name));
        }

    }

    /**
     * @throws InterruptedException
     */
    public void init() throws InterruptedException {

        initMama();
        initDictionary();
        initDatabase();

    }

    /**
     *
     */
    public void run() {

        try {
            // Subscribe to topics
            for (final String subject : subjectList) {

                final MamaSubscription sub = new MamaSubscription();
                sub.setRequiresInitial(requireInitial);
                sub.createSubscription(this, myDefaultQueue, mySource, subject, null);
            }

            Mama.start(myBridge);
        }
        finally {
            shutdown();
        }

    }

    /**
     *
     */
    public void shutdown() {

        // Shutdown MAMA Subscrtiptions
        for (final MamaSubscription sub : subscriptions) {
            sub.destroy();
        }

        queueGroup.destroyWait();
        Mama.close();
        database.close();
    }

    /**
     *
     */
    private void initMama() {

        Mama.open();
        myDefaultQueue = Mama.getDefaultQueue(myBridge);

        // Create Transports
        transport = new MamaTransport();

        if (myDictTportName != null) {
            myDictTransport = new MamaTransport();
            myDictTransport.create(myDictTportName, myBridge);
            myDictTransport.addTransportListener(this);
        }
        else {
            myDictTransport = transport;
        }

        transport.create(transportName, myBridge);
        transport.addTransportListener(this);

        /* MamaSource for all subscriptions */
        mySource = new MamaSource();
        mySource.setTransport(transport);
        mySource.setSymbolNamespace(mySymbolNamespace);

        /* MamaSource for dictionary subscription */
        myDictSource = new MamaSource();
        myDictSource.setTransport(myDictTransport);
        myDictSource.setSymbolNamespace(dictSource);
    }

    /**
     * @throws InterruptedException
     */
    private void initDictionary() throws InterruptedException {

        // Build from file
        if (dictFile != null) {
            dictionary = new MamaDictionary();
            dictionary.populateFromFile(dictFile);
        }
        else {

            // Build dictionary from subscription
            final MamaDictionaryCallback dictCb = new MamaDictionaryCallback() {

                @Override
                public void onTimeout() {
                    logger.severe(String.format("Timed out getting dictionary"));
                    Mama.stop(myBridge);
                    dictionaryComplete = false;
                }

                @Override
                public void onError(final String string) {
                    logger.severe(String.format("Error getting dictionary: %s", string));
                    Mama.stop(myBridge);
                    dictionaryComplete = false;
                }

                @Override
                public void onComplete() {
                    Mama.stop(myBridge);
                    dictionaryComplete = true;
                }
            };

            final MamaSubscription subscription = new MamaSubscription();
            dictionary = subscription.createDictionarySubscription(dictCb, myDefaultQueue, myDictSource, 10.0, 2);

            // Start dictionary subscription - block
            Mama.start(myBridge);
            if (!dictionaryComplete) {
                logger.severe("Failed to get Dictionary");
                System.exit(EXIT_FAIL);
            }

        }
    }

    private void initDatabase() {

        // Initiliase Database
        database.connect();
        database.clear();

    }

    //<editor-fold defaultstate="folded" desc="Subscription Callbacks">
    @Override
    public void onCreate(final MamaSubscription subscription) {
        subscriptions.add(subscription);
    }

    @Override
    public void onError(final MamaSubscription ms, final short s, final int i, final String string, final Exception excptn) {
        throw new UnsupportedOperationException(
                "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onMsg(final MamaSubscription subscription, final MamaMsg msg) {

        switch (MamaMsgType.typeForMsg(msg)) {
            case MamaMsgType.TYPE_DELETE:
            case MamaMsgType.TYPE_EXPIRE:
                subscription.destroy();
                subscriptions.remove(subscription);
                return;
        }

        switch (MamaMsgStatus.statusForMsg(msg)) {
            case MamaMsgStatus.STATUS_BAD_SYMBOL:
            case MamaMsgStatus.STATUS_EXPIRED:
            case MamaMsgStatus.STATUS_TIMEOUT:
                subscription.destroy();
                subscriptions.remove(subscription);
        }

        // TODO add field filter

        database.writeMsg(msg, dictionary, subscription);

        // Output count
        count++;
        if (count % 1000 == 0 && !quiet)
            System.out.printf("Captured %d messages\n", count);
    }

    @Override
    public void onQuality(final MamaSubscription ms, final short s, final short s1, final Object o) {
        throw new UnsupportedOperationException(
                "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onRecapRequest(final MamaSubscription ms) {
        throw new UnsupportedOperationException(
                "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onGap(final MamaSubscription ms) {
        throw new UnsupportedOperationException(
                "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onDestroy(final MamaSubscription ms) {
        throw new UnsupportedOperationException(
                "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    //</editor-fold>

    //<editor-fold defaultstate="folded" desc="Transport Callbacks">
    @Override
    public void onConnect(final short cause, final Object platformInfo) {
        System.out.println("TRANSPORT CONNECTED!");
    }

    @Override
    public void onDisconnect(final short cause, final Object platformInfo) {
        System.out.println("TRANSPORT DISCONNECTED!");
    }

    @Override
    public void onReconnect(final short cause, final Object platformInfo) {
        System.out.println("TRANSPORT RECONNECTED!");
    }

    @Override
    public void onPublisherDisconnect(final short cause, final Object platformInfo) {
        System.out.println("PUBLISHER DISCONNECTED!");
    }

    @Override
    public void onAccept(final short cause, final Object platformInfo) {
        System.out.println("TRANSPORT ACCEPTED!");
    }

    @Override
    public void onAcceptReconnect(final short cause, final Object platformInfo) {
        System.out.println("TRANSPORT RECONNECT ACCEPTED!");
    }

    @Override
    public void onNamingServiceConnect(final short cause, final Object platformInfo) {
        System.out.println("NSD CONNECTED!");
    }

    @Override
    public void onNamingServiceDisconnect(final short cause, final Object platformInfo) {
        System.out.println("NSD DISCONNECTED!");
    }

    @Override
    public void onQuality(final short cause, final Object platformInfo) {
        System.out.println("TRANSPORT QUALITY!");
        final short quality = transport.getQuality();
        System.out.println("Transport quality is now " + MamaQuality.toString(quality) + ", cause " + MamaDQCause
                .toString(cause) + ", platformInfo: " + platformInfo);
    }
    //</editor-fold>
}
