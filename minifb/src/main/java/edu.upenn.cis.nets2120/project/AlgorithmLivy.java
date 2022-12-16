package edu.upenn.cis.nets2120.project;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

public class AlgorithmLivy {	
	
	public static void main(String[] args) throws IOException, URISyntaxException, 
		InterruptedException, ExecutionException {
		
		System.out.println("connecting to livy");
		
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://ec2-3-239-128-31.compute-1.amazonaws.com:8998/"))
				  .build();
		
		System.out.println("uploading");
		
		String jar = "target/nets2120-project-0.0.1-SNAPSHOT.jar";
		client.uploadJar(new File(jar)).get();
		
		System.out.println("uploaded");
		
		List<String> results = client.submit(new AdsorptionAlg()).get();
		
		System.out.println(results);
		
		client.stop(true);
	}
}