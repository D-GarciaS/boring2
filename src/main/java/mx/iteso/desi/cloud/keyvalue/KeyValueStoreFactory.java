package mx.iteso.desi.cloud.keyvalue;

import java.net.UnknownHostException;
import mx.iteso.desi.cloud.lp1.Config;

public class KeyValueStoreFactory {
	public static enum STORETYPE {MEM, BERKELEY, DYNAMODB};
	
	public static STORETYPE DEFAULT_TYPE = Config.storeType;

	/**
	 * Create a new key value storage object
	 * 
	 * @param typ Type of the server
	 * @param dbName Name of the database / table
	 * @return
	 * @throws UnknownHostException
	 */
	public static IKeyValueStorage getNewKeyValueStore(STORETYPE typ, 
			String dbName) throws UnknownHostException {
		switch (typ) {
                    case MEM:
			return new MemStorage();
                    case BERKELEY:
			return new BdbStorage(dbName, false);
                    case DYNAMODB:
			return new DynamoDBStorage(dbName);
                    default:
			return null;
		}
	}

	public static IKeyValueStorage getNewKeyValueStore(String dbName) throws UnknownHostException {
                return KeyValueStoreFactory.getNewKeyValueStore(DEFAULT_TYPE, dbName);
        }        
}
