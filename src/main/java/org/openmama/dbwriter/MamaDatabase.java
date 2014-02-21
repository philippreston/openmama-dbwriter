package org.openmama.dbwriter;

import com.wombat.mama.MamaDictionary;
import com.wombat.mama.MamaMsg;
import com.wombat.mama.MamaSubscription;

public interface MamaDatabase{
    
    /**
     * 
     */
    public void connect();
    
    /**
     * 
     * @param msg The Mama Message that will be committed
     * @param dictionary The Mama Dictionary for looking up FIDs and Fields
     * @param subscription The Mama Subscription reference
     */
    public void writeMsg(MamaMsg msg, MamaDictionary dictionary, MamaSubscription subscription);
    
    /**
     * 
     */
    public void clear();
    
    /**
     * 
     */
    public void close();
    
}
