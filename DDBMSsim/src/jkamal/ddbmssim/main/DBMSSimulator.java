/**
 * @author Joarder Kamal
 * 
 * Main Class to run the Database Simulator
 */

package jkamal.ddbmssim.main;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.math3.random.RandomDataGenerator;
import jkamal.ddbmssim.db.DataMovement;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.DatabaseServer;
import jkamal.ddbmssim.graph.GraphMinCut;
import jkamal.ddbmssim.hgraph.HGraphMinCut;
import jkamal.ddbmssim.bootstrap.Bootstrapping;
import jkamal.ddbmssim.io.SimulationMetricsLogger;
import jkamal.ddbmssim.workload.ClusterIdMapper;
import jkamal.ddbmssim.workload.DataPreprocessor;
import jkamal.ddbmssim.workload.TransactionClassifier;
import jkamal.ddbmssim.workload.Workload;
import jkamal.ddbmssim.workload.WorkloadGenerator;

public class DBMSSimulator {	
	public final static int DB_NODES = 3;
	public final static String WORKLOAD_TYPE = "TPC-C";
	public final static int DATA_ROWS = 5200; // 10GB Data (in Size) // 5,200 for a TPC-C Database (scaled down by 1K for individual table row counts)
	public final static int TRANSACTIONS = 1000;
	public final static int SIMULATION_RUNS = 24;
	public final static double PARTITION_SIZE = 1; // 1; 0.1; 0.01
	
	// TPC-C Database table (9) row counts in different scale
	//double[] pk_array = {0.19, 1.92, 1.92, 19.2, 57.69, 5.76, 5.76, 1.73, 5.76};
	//public final static int[] PK_ARRAY = {1, 1, 1, 10, 30, 3, 3, 3, 1}; // 72 (9*8) Equal sized tables
	//public final static int[] PK_ARRAY = {1, 1, 1, 10, 30, 3, 3, 3, 1}; // 48 (6*8) Equal sized tables
	public final static int[] PK_ARRAY = {1, 1, 1, 10, 30, 3, 3, 3, 1}; // 53
	//public final static int[] PK_ARRAY = {1, 10, 10, 100, 300, 30, 30, 30, 9}; // 520
	//public final static int[] PK_ARRAY = {10, 100, 100, 1000, 3000, 300, 300, 300, 90}; //5,200
	//public final static int[] PK_ARRAY = {10, 100, 100, 1000, 3000, 300, 300, 300, 90}; //4,800 (9*800) 9 Equal sized partitions
	//public final static int[] PK_ARRAY = {10, 100, 100, 1000, 3000, 300, 300, 300, 90}; //7,200 (6*800) 6 Equal sized partitions

	//int[] data_row_size_array = {89, 95, 655, 46, 24, 8, 54, 306, 82}; // values are in Bytes
	public final static double[] DATA_ROW_SIZE = {0.000084877, 0.000090599, 0.000624657, 0.000043869, 0.000022888, 0.0000076294, 0.000051498, 0.000291824, 0.000078201}; // values are in MegaBytes

	
	public final static String hMETIS_DIR_LOCATION = "C:\\Users\\jkamal\\git\\DDBMSsim\\DDBMSsim\\lib\\native\\hMetis\\1.5.3-win32";		
	public final static String METIS_DIR_LOCATION = "C:\\Users\\jkamal\\git\\DDBMSsim\\DDBMSsim\\lib\\native\\metis\\3-win32";
	
	public final static String HMETIS = "hmetis";
	public final static String METIS = "pmetis";
	
	public static RandomDataGenerator random_birth;
	public static RandomDataGenerator random_death;
	public static RandomDataGenerator random_data;
	
	private static int global_tr_id;
	
	public static int getGlobal_tr_id() {
		return global_tr_id;
	}

	public static void setGlobal_tr_id(int global_tr_id) {
		DBMSSimulator.global_tr_id = global_tr_id;
	}
	
	public static void incGlobal_tr_id() {
		int id = DBMSSimulator.getGlobal_tr_id();
		DBMSSimulator.setGlobal_tr_id(++id);
	}
	
	public static void main(String[] args) throws IOException {
		random_data = new RandomDataGenerator();				
		
		// Database Server and Tenant Database Creation
		DatabaseServer dbs = new DatabaseServer(0, "test-dbs", DB_NODES);		
		System.out.println("[ACT] Creating Database Server \""+dbs.getDbs_name()+"\" with "+dbs.getDbs_nodes().size()+" Nodes ...");
		
		// Database creation for tenant id-"0" with Range partitioning model with 1GB Partition size	
		Database db = new Database(0, "testdb", 0, dbs, "Range", PARTITION_SIZE);
		System.out.println("[ACT] Creating Database \""+db.getDb_name()+"\" within "+dbs.getDbs_name()+" Database Server ...");		
		
		dbs.getDbs_tenants().add(db);
		
		// Perform Bootstrapping through synthetic Data generation and placing it into appropriate Partition
		System.out.println("[ACT] Started Bootstrapping Process ...");
		System.out.println("[ACT] Generating "+ DATA_ROWS +" synthetic data items ...");

		Bootstrapping bootstrapping = new Bootstrapping();
		bootstrapping.bootstrapping(dbs, db, DATA_ROWS);
		System.out.println("[MSG] Data creation and placement into partitions done.");
		
		// Printing out details after data loading
		dbs.show();
		db.show();
		
		// Data Pre-processing
		DataPreprocessor dataPreprocessor = new DataPreprocessor();
		dataPreprocessor.preprocess(db);
				
		// Workload generation for the entire simulation		
		WorkloadGenerator workloadGenerator = new WorkloadGenerator();		
		workloadGenerator.generateWorkloads(dbs, db, DBMSSimulator.SIMULATION_RUNS);		
		
		// Simulation initialisation
		SimulationMetricsLogger simulation_logger = new SimulationMetricsLogger();
		
		String[] partitioners = {"hgr", "chg", "gr"};
		String[] strategies = {"bs", "s1", "s2"};
		String[] directories = {hMETIS_DIR_LOCATION, METIS_DIR_LOCATION};
		
		// Create 9 databases under 3 partitioning schemes and 3 data movement strategies
		Map<Integer, Set<Database>> db_map = new TreeMap<Integer, Set<Database>>();
		String dir = null;
		for(int i = 0; i < partitioners.length; ++i) {
			Set<Database> db_set = new TreeSet<Database>();
			for(int j = 0; j < strategies.length; ++j) {
				// Creating individual databases
				Database clone_db = new Database(db);
				clone_db.setDb_name(partitioners[i]+"_"+strategies[j]+"_"+"db");
				clone_db.setDb_id((i+1)*(j+1));
				
				System.out.println("@ Creating database "+clone_db.getDb_name());
				
				if(i == 2)
					dir = directories[1];
				else
					dir = directories[0];
				
				// Creating individual log files
				clone_db.setWorkload_log(simulation_logger.getWriter(dir, 
						partitioners[i]+"_"+strategies[j]+"_"+"workload_log"));
				clone_db.setNode_log(simulation_logger.getWriter(dir, 
						partitioners[i]+"_"+strategies[j]+"_"+"node_log"));
				clone_db.setPartition_log(simulation_logger.getWriter(dir, 
						partitioners[i]+"_"+strategies[j]+"_"+"partition_log"));
				
				db_set.add(clone_db);
			}
			//System.out.println("@ i = "+i+" db_set size = "+db_set.size());
			db_map.put(i, db_set);
		}

		// Run simulations
		int simulation_run = 0;
		dir = null;
		while(simulation_run != SIMULATION_RUNS) {			
			Workload workload = workloadGenerator.getWorkload_map().get(simulation_run);
			workload.setMessage("in");
			
			write("============================================================", null);			
			
			int s = 0;
			for(Entry<Integer, Set<Database>> entry : db_map.entrySet()) {				
				for(Database database : entry.getValue()) {
					write("Starting simulation round ("+simulation_run+") for database ("+database.getDb_name()+")", "ACT");
					//System.out.println("@ entry = "+entry.getKey()+" | size = "+entry.getValue().size());
					if(entry.getKey() == 2)
						dir = directories[1];
					else
						dir = directories[0];
					
					runSimulation(database, workload, workloadGenerator, 
							dir, partitioners[entry.getKey()], strategies[s], simulation_logger);
					
					++s;
				}
				
				s = 0;
			}

			++ simulation_run;
		}
		
		// Wrapping Up
		for(Entry<Integer, Set<Database>> entry : db_map.entrySet()) {
			for(Database database : entry.getValue()) {
				close_logFiles(database.getWorkload_log(), database.getNode_log(), database.getPartition_log());
			}
		}
	}
	
	private static void runSimulation(Database db, Workload workload, WorkloadGenerator workloadGenerator, 
			String directory, String partitioner, String strategy, SimulationMetricsLogger simulation_logger) throws IOException{
		ClusterIdMapper cluster_id_mapper = new ClusterIdMapper();		
		DataMovement data_movement = new DataMovement();
		
		write("Started with "+workload.getWrl_totalTransactions()+" transactions from original worklaod.", "MSG");
		
		// Perform workload sampling
		write("Starting workload sampling to remove duplicate transactions ...", "ACT");
		Workload sampled_workload = workloadGenerator.workloadSampling(db, workload);		
		
		// Perform transaction classification
		// Classify the workload transactions based on whether they are distributed or not (Red/Orange/Green List)
		write("Starting workload classification to identify RED and ORANGE transactions ...", "ACT");		
		
		TransactionClassifier transactionClassifier = new TransactionClassifier();
		int target_transactions = transactionClassifier.classifyTransactions(db, sampled_workload);						
		
		// Show details of the sampled workload
		//sampled_workload.show(db, "");
		
		// Assign Shadow HMetis Data Id and generate workload and fix files
		workloadGenerator.assignShadowDataId(db, sampled_workload);
		
		write("Total "+target_transactions+" transactions having "+sampled_workload.getWrl_totalDataObjects()+" data objects have been identified for partitioning.", "MSG");
		
		// Generate workload and fix-files for partitioning
		workloadGenerator.generateWorkloadFile(db, sampled_workload, partitioner);		
		
		// Perform hyper-graph/graph/compressed hyper-graph partitioning
		runPartitioner(db, sampled_workload, partitioner);		

		write("Applying data movement strategies for database ("+db.getDb_name()+") ...", "ACT");
		write("***********************************************************************************************************************", null);
		
		// Log collection before data movement operation
		simulation_logger.setData_hasMoved(false);
		collectLog(simulation_logger, db, sampled_workload, db.getWorkload_log(), db.getNode_log(), db.getPartition_log(), partitioner);
		
		// Mapping cluster id to partition id
		cluster_id_mapper.processPartFile(db, sampled_workload, db.getDb_partitions().size(), directory, partitioner);
		
		//db.show();
		
		// Perform data movement		
		data_movement.performDataMovement(db, sampled_workload, strategy, partitioner);
		
		// Log collection after data movement operation
		simulation_logger.setData_hasMoved(true);
		collectLog(simulation_logger, db, sampled_workload, db.getWorkload_log(), db.getNode_log(), db.getPartition_log(), partitioner);
		
		write("***********************************************************************************************************************", null);
		
	}
	
	private static void runPartitioner(Database db, Workload workload, String partitioner) throws IOException {		
		switch(partitioner) {
		case "hgr":
			// Run hMetis HyperGraph Partitioning
			HGraphMinCut hgraphMinCut = new HGraphMinCut(db, workload, HMETIS, db.getDb_partitions().size(), "hgr"); 		
			hgraphMinCut.runHMetis();

			// Wait for 5 seconds to ensure that the Part files have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			break;
			
		case "chg":			
			HGraphMinCut chgraphMinCut = new HGraphMinCut(db, workload, HMETIS, db.getDb_partitions().size(), "chg"); 		
			chgraphMinCut.runHMetis();

			// Wait for 5 seconds to ensure that the Part files have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			break;
			
		case "gr":
			//==============================================================================================
			// Run Metis Graph Partitioning							
			GraphMinCut graphMinCut = new GraphMinCut(db, workload, METIS, db.getDb_partitions().size()); 		
			graphMinCut.runMetis();
			
			// Wait for 5 seconds to ensure that the Part files have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			break;
		}
	}
	
	private static void close_logFiles(PrintWriter workload_log, PrintWriter node_log, PrintWriter partition_log) {
		// Flush and Close
		workload_log.flush();
		workload_log.close();
		
		node_log.flush();
		node_log.close();
		
		partition_log.flush();
		partition_log.close();
	}
	
	private static void write(String content, String type) {
		if(type == null)
			System.out.println(content);
		else
			System.out.println("["+type+"] "+content);
	}
	
	private static void collectLog(SimulationMetricsLogger logger, Database db, Workload workload
			, PrintWriter wrl_writer, PrintWriter node_writer, PrintWriter partition_writer, String partitioner) {		
		logger.logWorkload(db, workload, wrl_writer, partitioner);
		logger.logNode(db, workload, node_writer);
		logger.logPartition(db, workload, partition_writer);		
	}
}
