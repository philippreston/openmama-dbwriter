package org.openmama.dbwriter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.wombat.mama.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MamaCassandra implements MamaDatabase {

    final private static String INSERT_STMT_STR = "INSERT INTO openmama_keyspace.messages (id, fieldmap) VALUES (?, ?);";
    final private static String CREATE_KSPACE_STR = "CREATE KEYSPACE IF NOT EXISTS openmama_keyspace WITH replication = " + "{'class':'SimpleStrategy', 'replication_factor':3}";
    final private static String CREATE_TABLE_STR = "create table IF NOT EXISTS openmama_keyspace.messages " + "(id text, fieldmap map <int,text>, PRIMARY KEY (id))";
    final private static String DROP_KSPACE_STR =  "DROP KEYSPACE IF EXISTS openmama_keyspace;";

    final private Cluster cluster;
    private Session session;
    private PreparedStatement insertStmt;

    public MamaCassandra() {
        String nodes = Mama.getProperty("mama.cassandra.nodes");
        cluster = Cluster.builder().addContactPoints(nodes).build();
    }

    @Override
    public void connect() {
        session = cluster.connect();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeMsg(MamaMsg msg, MamaDictionary dictionary, MamaSubscription subscription) {
        BoundStatement bound = new BoundStatement(insertStmt);
        Map<Integer, String> fieldMap = new HashMap<>(50);
        try {
            for (Iterator<MamaMsgField> iterator = msg.iterator(dictionary); iterator.hasNext(); ) {
                MamaMsgField field = iterator.next();
                fieldMap.put(field.getFid(), msg.getFieldAsString(field.getFid(), dictionary));
            }
            session.executeAsync(bound.bind(subscription.getSymbol() + "-" + msg.getSeqNum(), fieldMap));
        }
        catch (Exception e) {
            System.exit(1);
        }
    }

    @Override
    public void clear() {
        session.execute(DROP_KSPACE_STR);
        session.execute(CREATE_KSPACE_STR);
        session.execute(CREATE_TABLE_STR);
        insertStmt = session.prepare(INSERT_STMT_STR);
    }

    @Override
    public void close() {
        cluster.shutdown();
    }
}
