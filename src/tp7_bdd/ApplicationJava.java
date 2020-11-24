/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tp7_bdd;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

/**
 *
 * @author trongvo
 */
public class ApplicationJava {
    Session session;
    Driver driver;
    MongoClient mongoClient;
    // variable pour le lecture du clavier
    Scanner clavier;
    MongoDatabase database;
    MongoCollection<Document> collection;
    MongoCollection<Document> collectionIndexeInverse;
    
    public ApplicationJava(Session session, Driver driver, MongoClient mongoClient){
        this.session = session;
        this.driver = driver;
        this.mongoClient = mongoClient;
        clavier = new Scanner(System.in);
        
        database = mongoClient.getDatabase("dbDocuments");
        collection = database.getCollection("index");
        collectionIndexeInverse =database.getCollection("indexInverse");
    };
    
    public void run(){
        createMongoBD();
        structuremiroir();
        rechercheMongoEnvoyerNeo ();
        closeConnexion();
    }
    
    private void closeConnexion(){
        // Fermeture de la session neo4j
        session.close();
        // Fermeture de la connexion neo4j
        driver.close();
        
        // mongo
        mongoClient.close();
        System.out.println("A BIENTOT!");
    }
    
    private void createMongoBD(){
        //removeAllDocument();
        collection.deleteMany(new BsonDocument());
        
        StatementResult result = session.run( "MATCH (a:Article) RETURN id(a) as id, a.titre order by id(a) asc" );
        // Affichage
        System.out.println("Initiating BD :");
        while ( result.hasNext() ){       
            Record record = result.next();
            String chaine = record.get( "a.titre" ).asString();
            String chaineEnMinuscule = chaine.toLowerCase();
            StringTokenizer st = new StringTokenizer (chaineEnMinuscule, "‘'-:;.,()+[]{}?!= &");
            List<String> motCles = new ArrayList<>();
            while (st.hasMoreTokens()){
                // Récupération du mot suivant
                String mot = st.nextToken().trim();
                motCles.add(mot);
            }
            Document newInput = new Document("idDocument", record.get( "id").asInt())
                    .append("motsCles", motCles);
            collection.insertOne(newInput);
            System.out.println( record.get( "id")+" - "+motCles);
            }
    }
    
    private void removeAllDocument(){
        FindIterable<Document> findIterable = collection.find();
        for (Document document : findIterable) {
          collection.deleteMany(document);
        }
    }
    
    public void structuremiroir(){
           //removeAllDocument();
        collectionIndexeInverse.deleteMany(new BsonDocument());
        
        //Copie des indexes
         FindIterable<Document> documents = collection.find();
            for (Document doc : documents){
                List<String> motCles = new ArrayList<>();
                motCles = (List<String>) doc.get("motsCles");
                for ( String mot : motCles){
                    Document document = findByMotCle(collectionIndexeInverse,mot);
                if (document !=null){
                    UpdateResult updateResult = collectionIndexeInverse.updateOne(
                    Filters.eq("mot", mot), // Condition
                    Updates.push("Documents", doc.get("idDocument")) // Mise à jour
                    );
                    // Affiche le nombre de documents modifiés
                    System.out.println("Nb de documents modifiés : "+updateResult.getModifiedCount());

                }else {
                        List<Integer> idDocuments = new ArrayList<>();
                        idDocuments.add((Integer) doc.get("idDocument"));
                        Document newInput = new Document("mot", mot)
                                .append("Documents",idDocuments);
                        collectionIndexeInverse.insertOne(newInput);
                }
                
                 
                }
               
                    
            }
            }
                
     private static Document findByMotCle( MongoCollection<Document> collection,String motcle){
        Document document = collection.find(Filters.eq("mot", motcle)).first();
        return document;
    }
     
    public void rechercheMongoEnvoyerNeo (){
        Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.println("Enter mot");
        String mot = myObj.nextLine();  // Read user input
        Document docMongo = findByMotCle(collectionIndexeInverse, mot);
        ArrayList<Integer> documents = (ArrayList<Integer>) docMongo.get("Documents");
        String str =  ("MATCH (a:Article)" +
                        "WHERE id(a) in" + documents +
                        "RETURN a.titre order by a.titre asc ") ;
         StatementResult result = session.run( str );
         Integer cpt = 0;
         while ( result.hasNext() ){       
            Record record = result.next();
            String chaine = record.get( "a.titre" ).asString();
             System.out.println(chaine);
            cpt+=1;
         }
          System.out.println("Le nombre d'articles:"+cpt);
        
    }
    
}
