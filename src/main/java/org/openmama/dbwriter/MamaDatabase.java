package org.openmama.dbwriter;

import com.wombat.mama.MamaDictionary;
import com.wombat.mama.MamaMsg;
import com.wombat.mama.MamaSubscription;

public interface MamaDatabase{
    
    /**
     * Will create the connection to the database
     */
    public void connect();
    
    /**
     * Will writeTo a MamaMessage to the database
     * @param msg The Mama Message that will be committed
     * @param dictionary The Mama Dictionary for looking up FIDs and Fields
     * @param subscription The Mama Subscription reference
     */
    public void writeMsg(MamaMsg msg, MamaDictionary dictionary, MamaSubscription subscription);
    
    /**
     * Will clear any previous tables / collections and recreate along with any other setup
     */
    public void clear();
    
    /**
     * Close the connection to the database
     */
    public void close();
    
}
