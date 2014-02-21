/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openmama.dbwriter;

import com.mongodb.*;
import com.wombat.mama.*;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author Philip Preston (ppreston@nyx.com)
 */
public class MamaMongodb implements MamaDatabase {

    final private static Logger logger = Logger.getLogger("org.openmama.dbwriter");

    final private static String MAMA_PROP_NODES = "mama.mongodb.nodes";
    final private static String MAMA_PROP_WRITE_CONCERN = "mama.mongodb.write_concern";
    final private static String DB_NAME = "openmama";
    final private static String DB_COLLECTION_NAME = "tickstore";
    final private static int POOL_MAX_SIZE = -1;
    final private static int POOL_MIN_IDLE = 50;
    final private static int POOL_INIT_OBJECTS = 100;


    final private MongoClient client;
    final private List<ServerAddress> servers;
    final private DB database;
    private DBCollection collection;

    private GenericObjectPool<DBMessageContainer> dbMessageContainerPool;

    public MamaMongodb() {

        // Get the servers
        servers = new ArrayList<>(2);
        populateServers(Mama.getProperty(MAMA_PROP_NODES));

        // Get the Database and collection
        client = new MongoClient(servers);
        database = client.getDB(DB_NAME);
        WriteConcern writeConcern = getWriteConcern(Mama.getProperty(MAMA_PROP_WRITE_CONCERN));


        // Create collection and set write concern
        collection = database.createCollection(DB_COLLECTION_NAME, null);
        collection.setWriteConcern(writeConcern);

        // Create Object pool
        dbMessageContainerPool = new GenericObjectPool<>(new PoolableObjectFactory<DBMessageContainer>() {

            @Override
            public DBMessageContainer makeObject() throws Exception {
                return new DBMessageContainer();
            }

            @Override
            public void destroyObject(DBMessageContainer obj) throws Exception {
                obj.clear();
            }

            @Override
            public boolean validateObject(DBMessageContainer obj) {
                return true;
            }

            @Override
            public void activateObject(DBMessageContainer obj) throws Exception {
                // No Action required to activate
            }

            @Override
            public void passivateObject(DBMessageContainer obj) throws Exception {
                // Need to clear contents on return to pool
                obj.clear();
            }

        });

        dbMessageContainerPool.setMaxActive(POOL_MAX_SIZE);
        dbMessageContainerPool.setTestOnReturn(false);
        dbMessageContainerPool.setTestOnReturn(false);
        dbMessageContainerPool.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_GROW);
        dbMessageContainerPool.setMinIdle(POOL_MIN_IDLE);

        try {

            // Preload Object Pool
            for (int i = 0; i < POOL_INIT_OBJECTS; i++) {
                dbMessageContainerPool.addObject();
            }

        }
        catch (Exception ex) {
            logger.severe(String.format("Error initilising object pool: %s\n", ex.getMessage()));
            System.exit(MamaDatabaseWriter.EXIT_FAIL);
        }
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
                logger.warning(String.format("%s write concern not valid - using UNACKNOWLEDGED",config));
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
                throw new MamaException(String.format("Server %s:%d was invalid: %s", host, port, ex.getMessage()));
            }

        }
    }

    @Override
    public void connect() {

        // TODO - any indexes as should be created before writing
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeMsg(final MamaMsg msg,final MamaDictionary dictionary,final MamaSubscription subscription) {


        try {

            // Get an object
            DBMessageContainer container = dbMessageContainerPool.borrowObject();
            container.clear();
            container.setSymbol(subscription.getSymbol());
            container.setSeqNum(msg.getSeqNum());

            for (Iterator<MamaMsgField> iterator = msg.iterator(dictionary); iterator.hasNext(); ) {
                MamaMsgField field = iterator.next();
                container.addField(field.getName(), msg.getFieldAsString(field.getFid(), dictionary));
            }

            container.write(collection);

            // Return an object
            dbMessageContainerPool.returnObject(container);

        }
        catch (Exception ex) {
            logger.severe("Error on writeMsg: " + ex.getMessage());
        }
    }

    @Override
    public void clear() {

        // Drop the full collection
        if (database.collectionExists(collection.getName())) {
            collection.dropIndexes();
            collection.drop();
        }

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

        public void write(final DBCollection collection) {

            if(collection == null) {
                logger.severe("Collection is null - cannot write");
                return;
            }

            // Add the sub document
            document.put("detail",msg_document);
            collection.save(document);
        }

        public void clear() {
            msg_document.clear();
            document.clear();
        }

        public void addField(final String name, final String fieldAsString) {

            if(name == null){
                logger.severe("Field name is null - cannot add to container");
                return;
            }

            msg_document.put(name, fieldAsString);
        }
    }

}
