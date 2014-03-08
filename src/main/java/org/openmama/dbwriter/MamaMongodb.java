/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openmama.dbwriter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.wombat.mama.Mama;
import com.wombat.mama.MamaDictionary;
import com.wombat.mama.MamaException;
import com.wombat.mama.MamaMsg;
import com.wombat.mama.MamaMsgField;
import com.wombat.mama.MamaSubscription;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author Philip Preston (philippreston@mac.com)
 */
public class MamaMongodb implements MamaDatabase {

    final private static Logger logger = Logger.getLogger("org.openmama.dbwriter");

    final private static String MAMA_PROP_NODES = "mama.mongodb.nodes";
    final private static String MAMA_PROP_WRITE_CONCERN = "mama.mongodb.write_concern";
    final private static String DB_NAME = "openmama";
    final private static String DB_COLLECTION_NAME = "tickstore";

    final private List<ServerAddress> servers;
    final private WriteConcern writeConcern;

    private MongoClient client;
    private DBCollection tickstore_c;
    private DB database;

    public MamaMongodb() {

        // Get the servers
        servers = new ArrayList<>(2);
        populateServers(Mama.getProperty(MAMA_PROP_NODES));
        writeConcern = getWriteConcern(Mama.getProperty(MAMA_PROP_WRITE_CONCERN));

    }

    /**
     * @return Write concern for database writes
     */
    private WriteConcern getWriteConcern(final String config) {

        switch (config.toLowerCase()) {
            case "none":
            case "errors_ignored":
            case "errorsignored":
                return WriteConcern.ERRORS_IGNORED;
            case "safe":
            case "acknowledged":
                return WriteConcern.ACKNOWLEDGED;
            case "normal":
            case "unacknowledged":
                return WriteConcern.UNACKNOWLEDGED;
            case "fsync_safe":
            case "fsyncsafe":
            case "fsynced":
                return WriteConcern.FSYNCED;
            case "journal_safe":
            case "journalsafe":
            case "journaled":
                return WriteConcern.JOURNALED;
            case "replicas_safe":
            case "replicassafe":
            case "replica_acknowledged":
            case "replicaacknowledged":
                return WriteConcern.REPLICA_ACKNOWLEDGED;
            case "majority":
                return WriteConcern.MAJORITY;
            default:
                logger.warning(String.format("%s writeTo concern not valid - using UNACKNOWLEDGED", config));
                return WriteConcern.UNACKNOWLEDGED;
        }

    }

    /**
     * @param serverString semi colon delimited list of server address (host:port)
     */
    private void populateServers(final String serverString) {

        if (serverString == null || serverString.isEmpty()) {
            throw new MamaException("No Servers configured for MongoDB");
        }

        for (final String s : serverString.split(",")) {

            String host = "";
            int port = 0;

            try {
                String[] address = s.split(":");
                host = address[0];
                port = Integer.parseInt(address[1]);
                servers.add(new ServerAddress(host, port));
            }
            catch (final UnknownHostException ex) {
                logger.severe(String.format("Server %s:%d was unknown: %s", host, port, ex.getMessage()));
                System.exit(MamaDatabaseWriter.EXIT_FAIL);
            }
            catch(final Exception ex) {
                logger.severe(String.format("Error parsing servers %s", ex.getMessage()));
            }

        }
    }

    @Override
    public void connect() {

        // Get the Database and tickstore_c
        client = new MongoClient(servers);
        database = client.getDB(DB_NAME);
        tickstore_c = database.getCollection(DB_COLLECTION_NAME);

    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeMsg(final MamaMsg msg, final MamaDictionary dictionary, final MamaSubscription subscription) {


        try {

            // Get an object
            DBMessageContainer document = new DBMessageContainer();
            document.setSymbol(subscription.getSymbol());
            document.setSeqNum(msg.getSeqNum());

            for (Iterator<MamaMsgField> iterator = msg.iterator(dictionary); iterator.hasNext(); ) {
                MamaMsgField field = iterator.next();
                document.addField(field.getName(), msg.getFieldAsString(field.getFid(), dictionary));
            }

            document.writeTo(tickstore_c);

        }
        catch (Exception ex) {
            logger.severe("Error on writeMsg: " + ex.getMessage());
        }
    }

    @Override
    public void clear() {

        // Drop the tickstore_c
        if (database.collectionExists(tickstore_c.getName())) {
            tickstore_c.dropIndexes();
            tickstore_c.drop();
        }

        // Create tickstore_c and set writeTo concern
        tickstore_c = database.createCollection(DB_COLLECTION_NAME, null);
        tickstore_c.setWriteConcern(writeConcern);

        // TODO - any indexes as should be created before writing

    }

    @Override
    public void close() {

        // Close the client
        client.close();
    }


    private static class DBMessageContainer {

        final private BasicDBObject document;
        final private BasicDBObject msg_document;

        public DBMessageContainer() {
            document = new BasicDBObject();
            msg_document = new BasicDBObject();
        }

        public void setSymbol(final String symbol) {
            document.put("symbol", symbol);
        }

        public void setSeqNum(final long seqNum) {
            document.put("seq_num", seqNum);
        }

        public void writeTo(final DBCollection collection) {

            if (collection == null) {
                throw new IllegalArgumentException("Collection is null - cannot write");
            }

            // Add the sub document
            document.put("detail", msg_document);
            collection.insert(document);
        }

        public void clear() {
            msg_document.clear();
            document.clear();
        }

        public void addField(final String name, final String fieldAsString) {

            if (name == null) {
                throw new IllegalArgumentException("Field name is null - cannot add to container");
            }

            msg_document.put(name, fieldAsString);
        }
    }

}
