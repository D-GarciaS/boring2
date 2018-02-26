package mx.iteso.desi.cloud.keyvalue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;


public class DynamoDBStorage extends BasicKeyValueStore {
    
    String dbName;
    public static AmazonDynamoDB client;
    
    public static int dbCount;
    public static boolean compress;
    
    DynamoDBMapper myDb;
    
    // Simple autoincrement counter to make sure we have unique entries
    int inx;

    int batchCounter = 0;
    ArrayList<DBPair> itemsBatch = new ArrayList<>(25);

    Set<String> attributesToGet = new HashSet<String>();
    
    public static class DBPair{
        public String keyword;
        public int inx;
        public String value;

        @DynamoDBRangeKey
        /**
         * @return the inx
         */
        public int getInx() {
            return inx;
        }
        /**
         * @param inx the inx to set
         */
        public void setInx(int inx) {
            this.inx = inx;
        }
        @DynamoDBHashKey(attributeName = "keyword")
        /**
         * @return the keyword
         */
        public String getKeyword() {
            return keyword;
        }
        /**
         * @param keyword the keyword to set
         */
        public void setKeyword(String keyword) {
            this.keyword = keyword;
        }
        @DynamoDBAttribute(attributeName = "value")
        /**
         * @return the value
         */
        public String getValue() {
            return value;
        }
        /**
         * @param value the value to set
         */
        public void setValue(String value) {
            this.value = value;
        }
        
    }
    
    private void init(String dbName){
        if(client == null){
            client = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_WEST_2)
            .build();
        }
        
        DynamoDBMapperConfig config;
        config = DynamoDBMapperConfig.builder()
        .withTableNameOverride(
        TableNameOverride.withTableNameReplacement(dbName))
        .build();
        
        myDb = new DynamoDBMapper(client,config);
        
        createTable(myDb.generateCreateTableRequest(DBPair.class));

        dbCount++;   
    }
    
    private void createTable(CreateTableRequest request){
        
        System.out.println("Creando Tabla" + request.getTableName());
        request.setProvisionedThroughput(new ProvisionedThroughput(1L,1L));
        com.amazonaws.services.dynamodbv2.document.DynamoDB dynamoDB = new com.amazonaws.services.dynamodbv2.document.DynamoDB(client);
        try {
            Table t = dynamoDB.createTable(request);   
            t.waitForActive();
            System.out.println("tabla creada");

        } catch (ResourceInUseException e) {
            System.out.println("La tabla ya existia");
        } catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    public DynamoDBStorage(String dbName) {
        System.out.println("calling create dynamo instance");
        this.dbName = dbName;
        init(dbName);
        compress = true;
    }
    
    @Override
    public Set<String> get(String search) {
        // Map<String,String> expressionNames = new HashMap<>();
        // expressionNames.put("#keyword", "keyword");
        // Map<String,String> expressionValues = new HashMap<>();
        // expressionValues.put(":equals");
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS(search));

        DynamoDBQueryExpression<DBPair> queryExpression;
        queryExpression = new DynamoDBQueryExpression<DBPair>()
            .withKeyConditionExpression("keyword = :val1").withExpressionAttributeValues(eav);
        PaginatedQueryList<DBPair> reply = myDb.query(DBPair.class, queryExpression );
        Set<String> result = reply.stream().map( p -> p.value).collect(Collectors.toSet());
        return result;
    }
    
    @Override
    public boolean exists(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public Set<String> getPrefix(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void addToSet(String keyword, String value) {
        batchCounter++;
        inx ++;
        DBPair entry = new DBPair();
        entry.keyword = keyword;
        entry.value = value;
        entry.inx = inx;

        itemsBatch.add(entry);

        if(batchCounter == 25){
            batchCounter = 0;
            myDb.batchSave(itemsBatch);
            itemsBatch.clear();
        }
    }
    
    @Override
    public void put(String keyword, String value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void close() {
        sync();

        dbCount--;
        if(dbCount == 0){
            client.shutdown();
        }
    }
    
    @Override
    public boolean supportsPrefixes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void sync() {
        if(itemsBatch.size() > 0){
            batchCounter = 0;
            myDb.batchSave(itemsBatch);
            itemsBatch.clear();
        }
    }
    
    @Override
    public boolean isCompressible() {
        return false;
    }
    
    @Override
    public boolean supportsMoreThan256Attributes() {
        return true;
    }
    
}
