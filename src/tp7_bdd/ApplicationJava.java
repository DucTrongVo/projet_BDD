/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tp7_bdd;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.bson.BsonDocument;
import org.bson.Document;
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
    
    public ApplicationJava(Session session, Driver driver, MongoClient mongoClient){
        this.session = session;
        this.driver = driver;
        this.mongoClient = mongoClient;
        clavier = new Scanner(System.in);
        
        database = mongoClient.getDatabase("dbDocuments");
        collection = database.getCollection("index");
    };
    
    public void run(){
        createMongoBD();
        
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
            StringTokenizer st = new StringTokenizer (chaineEnMinuscule, "‘-:;.()+[]{}?!= ");
            List<String> motCles = new ArrayList<>();
            while (st.hasMoreTokens()){
                // Récupération du mot suivant
                String mot = st.nextToken();
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
}
