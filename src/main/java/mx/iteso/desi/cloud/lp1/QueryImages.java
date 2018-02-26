package mx.iteso.desi.cloud.lp1;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;

public class QueryImages {
  IKeyValueStorage imageStore;
  IKeyValueStorage titleStore;
	
  public QueryImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) 
  {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
	
  public Set<String> query(String word)
  {
    String normalized = PorterStemmer.stem(word).toLowerCase();
    //System.out.println(normalized);
    Set<String> matchedArticles = titleStore.get(normalized);
    //System.out.println(matchedArticles);

    HashSet<String> res =  matchedArticles.stream()
      .map(a -> imageStore.get(a))
      .flatMap( s -> s.stream()).collect(Collectors.toCollection(HashSet::new))
    ;

    return res;
  }
        
  public void close()
  {
    imageStore.close();
    imageStore.close();
  }
	
  public static void main(String args[]) 
  {
    System.out.println("*** Alumno: DAMIAN GARCIA SERRANO (Exp: IS700489 )");
    
    try {
      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
      "images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
      "terms");
      
      QueryImages myQuery = new QueryImages(imageStore, titleStore);

      for (int i=0; i<args.length; i++) {
        System.out.println(args[i]+":");
        Set<String> result = myQuery.query(args[i]);
        Iterator<String> iter = result.iterator();
        while (iter.hasNext()) 
          System.out.println("  - "+iter.next());
      }

      myQuery.close();
      
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to complete the indexing pass -- exiting");
    }
  }
}

