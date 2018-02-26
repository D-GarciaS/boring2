/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mx.iteso.desi.cloud.lp1;

import com.amazonaws.regions.Regions;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;

/**
 *
 * @author parres
 */
public class Config {

  // The type of key/value store you are using. Initially set to BERKELEY;
  // will be changed to DynamoDB in some phases.
  public static final KeyValueStoreFactory.STORETYPE storeType = KeyValueStoreFactory.STORETYPE.DYNAMODB;
  public static final String pathToDatabase = "/dbd";
    
  // Set to your Amazon REGION tu be used
  public static final Regions amazonRegion = Regions.US_WEST_2;
  
    
  // Restrict the topics that should be indexed. For example, when this is
  // set to 'X', you should only index topics that start with an X.
  // Set this to "A" for local work, and to "Ar" for cloud tests..
  public static final String filter = "Ar";
  
  public static final String titleFileName = "dataset/labels_en.ttl";
  public static final String imageFileName = "dataset/images_en.ttl";
    
}
