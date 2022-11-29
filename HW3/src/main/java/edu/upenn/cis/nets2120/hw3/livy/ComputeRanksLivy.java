package edu.upenn.cis.nets2120.hw3.livy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import edu.upenn.cis.nets2120.config.Config;

public class ComputeRanksLivy {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://ec2-44-211-45-220.compute-1.amazonaws.com:8998/"))
				  .build();

		try {
			String jar = "target/nets2120-hw3-0.0.1-SNAPSHOT.jar";
			
		  System.out.printf("Uploading %s to the Spark context...\n", jar);
		  client.uploadJar(new File(jar)).get();
		  
		  String sourceFile = Config.SOCIAL_NET_PATH;

		  System.out.printf("Running SocialRankJob with %s as its input...\n", sourceFile);
		  List<MyPair<Integer,Double>> resultWithBL = client.submit(new SocialRankJob(true, sourceFile)).get();
		  System.out.println("With backlinks: " + resultWithBL);
		  
		  
		  List<MyPair<Integer,Double>> resultWOBL = client.submit(new SocialRankJob(false, sourceFile)).get();
		  System.out.println("Without backlinks: " + resultWOBL);
		  
		  List<Integer> resultWithBLNode = new ArrayList<>();
		  resultWithBL.forEach(t ->{
				resultWithBLNode.add(t.getLeft());
			});
		  
		  List<Integer> resultWOBLNode = new ArrayList<>();
		  resultWOBL.forEach(t ->{
				resultWOBLNode.add(t.getLeft());
			});
		  
		  FileWriter newFile = new FileWriter("results1.txt");
		  
		  List<Integer> NodesInBothLists = new ArrayList<>(resultWithBLNode);
		  
		  NodesInBothLists.retainAll(resultWOBLNode);
		  resultWithBLNode.removeAll(NodesInBothLists);
		  resultWOBLNode.removeAll(NodesInBothLists);
		  
		  
//		  newFile.write(resultWithBLNode + "\n");
		  newFile.write("Nodes exclusive to computations without back-links" + resultWOBLNode+ "\n");
		  
		  newFile.write("Nodes in both lists" + NodesInBothLists + "\n");
		  newFile.close();
		  // TODO write the output to a file that you'll upload.
		} finally {
		  client.stop(true);
		}
	}

}
