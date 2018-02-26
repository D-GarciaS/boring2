package mx.iteso.desi.cloud.lp1;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.ParseTriples;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;
import mx.iteso.desi.cloud.keyvalue.Triple;

public class IndexImages {
  ParseTriples parser;
  IKeyValueStorage imageStore, titleStore, imagesCache;
  
  public IndexImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) {
    this.imageStore = imageStore;
    this.titleStore = titleStore;
    
    try{
      imagesCache = KeyValueStoreFactory.getNewKeyValueStore(KeyValueStoreFactory.STORETYPE.MEM, "cache");
    }catch(Exception e){
      e.printStackTrace();
    }
  }
  
  public void run(String imageFileName, String titleFileName) throws IOException
  {
    Triple tr;
    parser = new ParseTriples(imageFileName);
        
    int max  = 0;
    System.out.println("Cargando imagenes");
    
    while((tr = parser.getNextTriple()) != null){
      //System.out.println(tr);
      String relType = tr.get(1);
      String articleURL = tr.get(0);
      
      if(!relType.equals("http://xmlns.com/foaf/0.1/depiction"))
      continue;
      
      String articleName = articleURL.substring(articleURL.lastIndexOf('/') + 1);

      if(!articleName.startsWith(Config.filter))
      continue;

        imageStore.addToSet(tr.get(0), tr.get(2));
        imagesCache.addToSet(tr.get(0), tr.get(2));
        max++;
      }
      
      imageStore.sync();

      System.out.println("Imagenes Agregadas " + max);
      
      parser = new ParseTriples(titleFileName);
      
      max = 0 ;
      
      System.out.println("Cargando  titulos");
      
      while((tr = parser.getNextTriple()) != null){

          String relType = tr.get(1);
          String articleURL = tr.get(0);
          
          if(!relType.equals("http://www.w3.org/2000/01/rdf-schema#label"))
          continue;
          
          if(!imagesCache.exists(articleURL))
          continue;
          
          String articulo = tr.get(0);
          List<String> palabras = Arrays.asList(tr.get(2).split(" "));
          long eff = palabras.stream().map(String::toLowerCase).map(PorterStemmer::stem)
            .filter(s -> !s.equals("Invalid term")).peek( s -> {
            titleStore.addToSet(s, articulo);
          }).count();
          
          //max= Integer.max(max,palabras.size());
          max += eff;
        }
        
        System.out.println("Etiquetas agregadas " + max);
      }
      
      public void close() {
        imageStore.close();
        titleStore.close();
      }
      
      public static void main(String args[])
      {
        System.out.println("*** Alumno: DAMIAN GARCIA SERRANO (Exp: IS7000489 )");
        try {
          
          IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
          "images");
          IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
          "terms");
          
          IndexImages indexer = new IndexImages(imageStore, titleStore);
          indexer.run(Config.imageFileName, Config.titleFileName);
          indexer.close();
          
          System.out.println("Indexing completed");
          
          
        } catch (Exception e) {
          e.printStackTrace();
          System.err.println("Failed to complete the indexing pass -- exiting");
        }
      }
    }
    
    