package jkamal.ddbmssim.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import jkamal.ddbmssim.io.StreamCollector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

public class ComputationalTiming {	
	private final static String TEST_LOCATION = "C:\\Users\\jkamal\\git\\DDBMSsim\\DDBMSsim\\lib\\native\\hMetis\\1.5.3-win32";
	private static Map<Integer, Integer> vertexMap = null;
	private static Map<Integer, Integer> vertexWeight = null;
	
	public static void main(String[] args) {
		int[] transactional_dimensions = new int[]{10, 20, 30, 40, 50};
		int[] transaction_numbers = new int[]{1000, 2000, 3000, 4000, 5000};
		int[] cluster_size = new int[]{100, 200, 300, 400, 500};
		List<String> arg_list = null;
		int lower = 0;
		int upper = 10000;
		int w_id = 0;
		long startTime;
		long duration;
		
		RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
		
		// Logging
		PrintWriter writer = getWriter(TEST_LOCATION, "log");
		
//		for(int i = 1; i <= 5; i++){//cluster_size
//			for(int j = 1; j <= 5; j++){//transaction_numbers
//				for(int k = 1; k <= 5; k++){//transactional_dimensions
//					// Workload file generation
//					++w_id;
//					System.out.println("["+w_id+"] Generating workload file "+w_id+".txt ...");					
//					startTime = System.nanoTime();					
//			        
//					generateWorkload(w_id, transaction_numbers[j-1], transactional_dimensions[k-1], lower, upper, randomDataGenerator);					
//			        
//					duration = (System.nanoTime() - startTime);			        
//			        System.out.println(">> Workload file "+w_id+".txt has been generated within "+duration + " nano sec.");
//				}
//			}
//		}
		
		int run = 1000;
		while(run!=0){
			w_id = 0;
			for(int i = 1; i <= 5; i++){//cluster_size
				for(int j = 1; j <= 5; j++){//transaction_numbers
					for(int k = 1; k <= 5; k++){//transactional_dimensions			        
						++w_id;
						
				        // Hypergraph clustering
				        arg_list = new ArrayList<String>();
				        arg_list.add("cmd");
						arg_list.add("/c");
						//arg_list.add("start");
						arg_list.add("khmetis");
						arg_list.add(w_id+".txt");
						arg_list.add(Integer.toString(cluster_size[i-1]));	// Nparts
						arg_list.add("5");					// UBfactor(>=5)
						arg_list.add("10");					// Nruns(>=1)
						arg_list.add("1");					// CType(1-5)
						arg_list.add("1");					// OType(1-2) -- 1: Minimizes the hyper edge cut, 2: Minimizes the sum of external degrees (SOED)
						arg_list.add("0");					// Vcycle(0-3)
						arg_list.add("0");					// dbglvl(0+1+2+4+8+16)
						
						String[] arg = arg_list.toArray(new String[arg_list.size()]);					
						ProcessBuilder pb = new ProcessBuilder(arg);
						pb.directory(new File(TEST_LOCATION));
						
						System.out.println("["+w_id+"] Clustering workload file "+w_id+".txt ...");					
						startTime = System.nanoTime();					
				        					
						try {
							final Process p = pb.start();						
							
							new Thread(new Runnable(){
								public void run(){
						        	Scanner stdin = new Scanner(p.getInputStream());
						            while(stdin.hasNextLine()){
						            	System.out.println(stdin.nextLine());
						            }
						            
						            stdin.close();
								}
						    }).start();
							
							// Any error? Or, Any output? 
							//StreamCollector errorStreams = new StreamCollector(p.getErrorStream(), "ERROR");
							//StreamCollector outputStreams = new StreamCollector(p.getInputStream(), "OUTPUT");
							
							// Start stream collectors
							//outputStreams.start();
							//errorStreams.start();
							
							p.waitFor();
							
							//PrintWriter printer = new PrintWriter(outputStreams);
							
						} catch (IOException | InterruptedException e) {						
							e.printStackTrace();
						}
						
						duration = (System.nanoTime() - startTime);			        
				        System.out.println("-->> Workload file "+w_id+".txt has been clustered within "+duration + " nano sec.");
				        
				        // Logging
				        writer.print(transactional_dimensions[k-1]+" "+transaction_numbers[j-1]+" "+cluster_size[i-1]+" "+duration);
				        //writer.print(duration);
				        writer.println();
				        writer.flush();
					}
				}
			}
		}
		
		writer.close();
	}
	
	private static Set<Integer> generateTransaction(int dim, int min, int max, RandomDataGenerator rdg){
		Set<Integer> transaction = new TreeSet<Integer>();
		int step_size = max/dim;
		int lower_limit = min+1;
		int upper_limit = step_size;
		
		while(transaction.size() < dim){			
			transaction.add(rdg.nextInt(lower_limit, upper_limit));
			lower_limit = upper_limit+1;
			upper_limit += step_size;
		}
		
		return transaction;
	}
	
	private static void generateWorkload(int w_id, int tr_numbers, int tr_dimension, 
			int min, int max, RandomDataGenerator rdg){
		
		vertexMap = new TreeMap<Integer, Integer>();
		vertexWeight = new TreeMap<Integer, Integer>();
		int v_id = 0;
		int edges = tr_numbers;
		String content = "";
		
		// Edges
		rdg.reSeed(0);
		for(int i = 0; i < edges; i++){
			Set<Integer> edge = generateTransaction(tr_dimension, min, max, rdg);
			String e = Integer.toString(rdg.nextInt(1, 2))+" "; // Temporal edge weight for two consecutive workload windows			
			
			for(Integer v : edge){
				if(!vertexMap.containsKey(v)){
					++v_id;
					vertexMap.put(v, v_id);
					vertexWeight.put(v_id, 1);
				}else{
					int weight = vertexWeight.get(vertexMap.get(v));
					vertexWeight.remove(vertexMap.get(v));
					vertexWeight.put(vertexMap.get(v), ++weight);
				}
				
				e += Integer.toString(vertexMap.get(v))+" ";
			}
			
			StringUtils.stripEnd(e, null);
			content += e+"\n";
		}
		
		// Vertices
		int new_line = vertexMap.size();
		for(int i = 1; i< vertexMap.size()+1; i++){
			content += Integer.toString(vertexWeight.get(i));
			
			--new_line;			
			if(new_line != 0)
				content += "\n";
		}
		
		// Writing in workload file
		File workloadFile = new File(TEST_LOCATION+"\\"+w_id+".txt");		
		try {
			workloadFile.createNewFile();
			Writer writer = null;			

			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(edges+" "+vertexMap.size()+" "+Integer.toString(1)+""+Integer.toString(1)+"\n"+content);
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}
	}
	
	private static PrintWriter getWriter(String dir, String trace) {		
		File logFile = new File(dir+"\\"+trace+".txt");
		PrintWriter prWriter = null;
		
		try {
			if(!logFile.exists())
				logFile.createNewFile();
			
			try {
				prWriter = new PrintWriter(new BufferedWriter(new FileWriter(logFile)));				
				
			} catch(IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}
		
		return prWriter;
	}
}
