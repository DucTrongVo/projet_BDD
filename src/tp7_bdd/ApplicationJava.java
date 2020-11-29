/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tp7_bdd;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.Indexes;
import static com.mongodb.client.model.Indexes.descending;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Arrays;
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
 * @author CarlosFL97
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
        top10Auteur();
        recherche();
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
    
    
    public void structuremiroir(){
           //removeAllDocument();
        collectionIndexeInverse.deleteMany(new BsonDocument());
        
        //Copie des indexes
         FindIterable<Document> documents = collection.find();
            for (Document doc : documents){
                List<String> motCles = (List<String>) doc.get("motsCles");
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
    
    public void top10Auteur(){
     String str =  "match(a:Auteur)-[e:Ecrire]->(ar:Article)return count(e),a.nom order by count(e) desc ,a.nom asc limit 10";
     StatementResult result = session.run( str );
     while ( result.hasNext() ){       
            Record record = result.next();
            String chaine = record.get( "count(e)" ).asInt()+" - "+record.get( "a.nom" ).asString();
            System.out.println(chaine);
    }
    }
    
    public void recherche(){
    Scanner clavier = new Scanner(System.in);
    List<String> motscherche= new ArrayList<>();
    System.out.println("Saisir vos mots, enter sans mots pour arrêter)");
    String mot;
    while ((mot=clavier.nextLine()).length()>0){
        motscherche.add(mot);
    }
   
    Bson match = match(Filters.in("mot", motscherche));
    Bson unwind  = unwind( "$Documents");
    Bson group = group("$Documents",sum("nb", 1));
    Bson sort  = sort(descending("nb"));
    Bson limit = limit(10);
    AggregateIterable<Document> documents = collectionIndexeInverse.aggregate(Arrays.asList(match,unwind,group,sort,limit));
    
    int idNeo;
    int nbMots;
    StatementResult result;
    String str;
     Record enr;
     String titre;
    for (Document doc : documents){
            idNeo = doc.getInteger("_id");
            nbMots = doc.getInteger("nb");
            str = "Match (a:Article) where id(a)="+idNeo+ " return a.titre ";
            result = session.run( str );
            enr =result.next();
            titre = enr.get("a.titre").asString();
            System.out.println(idNeo+" - "+titre+" "+nbMots);
    }
      
    }
    
    
}
