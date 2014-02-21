package org.openmama.dbwriter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.wombat.mama.Mama;
import com.wombat.mama.MamaDictionary;
import com.wombat.mama.MamaMsg;
import com.wombat.mama.MamaMsgField;
import com.wombat.mama.MamaSubscription;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MamaCassandra implements MamaDatabase {

    final private static String insertStmtStr = "INSERT INTO openmama_keyspace.messages (id, fieldmap) VALUES (?, ?);";

    final private String nodes;
    final private Cluster cluster;
    private Session session;
    private PreparedStatement insertStmt;

    public MamaCassandra() {
        nodes = Mama.getProperty("mama.cassandra.nodes");        
        cluster = Cluster.builder()
                .addContactPoints(nodes)
                .build();
    }

    @Override
    public void connect() {

        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS openmama_keyspace WITH replication "
                        + "= {'class':'SimpleStrategy', 'replication_factor':3};");
        session.execute("create table IF NOT EXISTS openmama_keyspace.messages (id text, fieldmap map <int,text>, PRIMARY KEY (id))");
        insertStmt = session.prepare(insertStmtStr);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeMsg(MamaMsg msg, MamaDictionary dictionary, MamaSubscription subscription) {
        BoundStatement bound = new BoundStatement(insertStmt);
        Map<Integer, String> fieldMap = new HashMap<>(50);
        try {
            for (Iterator<MamaMsgField> iterator = msg.iterator(dictionary); iterator.hasNext();) {
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
        session.execute("DROP KEYSPACE IF EXISTS openmama_keyspace;");
        session.execute("CREATE KEYSPACE openmama_keyspace WITH replication "
                        + "= {'class':'SimpleStrategy', 'replication_factor':3};");
        session.execute("create table openmama_keyspace.messages (id text, fieldmap map <int,text>, PRIMARY KEY (id))");
        this.insertStmt = session.prepare(insertStmtStr);
    }

    @Override
    public void close() {
        cluster.shutdown();
    }
}
