package com.mongoProject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import java.util.HashMap;

import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main( String[] args )
	{
		System.out.println( "Hello World!" );
		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("data");
		System.out.println(collection.count());
		final File folder = new File("C:\\Users\\adrie\\Documents\\A2\\Bdd-BigData\\BigData Project\\csv\\");
		mongoimport(collection, folder);
	}

	public static void mongoimport(MongoCollection<Document> collection,final File folder){
		List<String> files = listFilesForFolder(folder);
		Map<String,String> meta = getMeta();
		for(String file : files) {
			if(file.contains(".csv")) {
				System.out.println(file);
				try(Scanner scan = new Scanner(FileSystems.getDefault().getPath("C:\\Users\\adrie\\Documents\\A2\\Bdd-BigData\\BigData Project\\csv\\"+file))){
					String line = scan.nextLine();
					while(scan.hasNextLine()) {
						line = scan.nextLine();
						String site_id = file.replace(".csv", "");
						String[] values=line.split(",");
						String meta_line = meta.get(site_id);
						String[] meta_values = meta_line.split(",");
						Document doc = new Document("timestamp", Integer.parseInt(values[0]))
								.append("dttm_utc", values[1])
								.append("value", Float.parseFloat(values[2]))
								.append("estimated", Float.parseFloat(values[3]))
								.append("anomaly", values[4])
								.append("site", new Document("SITE_ID", Float.parseFloat(meta_values[0]))
										.append("INDUSTRY", meta_values[1])
										.append("SUB_INDUSTRY", meta_values[2])
										.append("SQ_FT", Float.parseFloat(meta_values[3]))
										.append("LAT", Float.parseFloat(meta_values[4]))
										.append("LNG", Float.parseFloat(meta_values[5]))
										.append("TIME_ZONE", meta_values[6])
										.append("TZ_OFFSET", meta_values[7]));
						collection.insertOne(doc);
					}	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static List<String> listFilesForFolder(final File folder) {
		List<String> files = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				files.add(fileEntry.getName());
			}
		}
		return files;
	}
	
	public static Map<String,String> getMeta(){
		Map<String,String> meta = new HashMap<String,String>();
		try(Scanner scan = new Scanner(FileSystems.getDefault().getPath("C:\\Users\\adrie\\Documents\\A2\\Bdd-BigData\\BigData Project\\all_sites.csv"))){
			String line = scan.nextLine();
			while(scan.hasNextLine()) {
				line = scan.nextLine();
				String site_id = line.split(",")[0];
				meta.put(site_id,line);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
		return meta;
	}

}
