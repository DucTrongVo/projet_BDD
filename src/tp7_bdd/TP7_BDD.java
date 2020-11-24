/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tp7_bdd;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

/**
 *
 * @author trongvo
 */
public class TP7_BDD {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        
        // Mongo
        String uri = "mongodb://192.168.56.50:27017";
        MongoClientURI connectionString = new MongoClientURI(uri);
        
        MongoClient mongoClient = new MongoClient(connectionString);
        Thread.sleep(1000);

        // Neo4j
        // Déclaration de driver
        Driver driver = GraphDatabase.driver("bolt://192.168.56.50");
        
        // Démarrer une session
        Session session = driver.session();
        
        ApplicationJava app = new ApplicationJava(session, driver, mongoClient);
        app.run();
    }
     
}
