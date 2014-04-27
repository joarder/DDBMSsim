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
import java.util.Random;
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
import jkamal.ddbmssim.workload.TransactionClassifier;
import jkamal.ddbmssim.workload.Workload;
import jkamal.ddbmssim.workload.WorkloadFileGenerator;
import jkamal.ddbmssim.workload.WorkloadGenerator;

public class DBMSSimulator {	
	public final static int DB_NODES = 10;
	public final static double NODE_SCALE = 1;
	public final static double PARTITION_SCALE = 1; // 1; 0.1; 0.01
	public final static String WORKLOAD_TYPE = "tpcc";
	
	public final static int NODE_MAX_CAPACITY = 10000; // 10GB Or, equivalently 10 Partitions can be stored in a single node.
	public final static int PARTITION_MAX_CAPACITY = 1000; // in data rows
	
	public final static int TRANSACTIONS = 1000;
	public final static int SIMULATION_RUNS = 100;

	public final static int TPCC_WAREHOUSE = 10; // # of Warehouse, W = 1+	
	public final static double TPCC_Scale = 0.001; // Reflects the total number of Data Rows in each Table; 0.001 = 1/1K
	public final static int[] TPCC_TABLE_TYPE = {0, 0, 1, 1, 1, 2, 1, 1, 2}; // 0: Primary, 1: Secondary, 2: Dependent
	public final static int[][] TPCC_SCHEMA = new int[][]{
											{0, 0, 0, 0, 0, 0, 0, 0, 0}, // Warehouse (Primary:0)
											{0, 0, 0, 0, 0, 0, 0, 0, 0}, // Item (Primary:0)
											{1, 0, 0, 0, 0, 0, 0, 0, 0}, // District (Secondary:1) - Warehouse
											{1, 1, 0, 0, 0, 0, 0, 0, 0}, // Stock (Secondary:1) - Item, Warehouse
											{0, 0, 1, 0, 0, 0, 0, 0, 0}, // Customer (Secondary:1) - District
											{0, 0, 1, 0, 1, 0, 0, 0, 0}, // History (Dependent:2) - Customer, District
											{0, 0, 0, 0, 1, 0, 0, 0, 0}, // Order (Secondary:1) - Customer
											{0, 0, 0, 0, 0, 0, 1, 0, 0}, // New-Order (Secondary:1) - Order
											{0, 0, 0, 1, 0, 0, 1, 0, 0}  // Order-Line (Dependent:2) - Order, Stock											
											};
	public final static double[] TPCC_DATA_ROW_SIZE = { // values are in MegaBytes
													0.000078201, // Warehouse
													0.000043869, // Item
													0.000090599, // District
													0.000291824, // Stock
													0.000084877, // Customer
													0.000624657, // History
													0.0000076294,// Order 
													0.000022888, // New-Order
													0.000051498	 // Order-Line
													};
	
	public final static double[] TPCC_TRANSACTION_PROPORTION = {0.45, 0.43, 0.04, 0.04, 0.04};
	public final static int[][] TPCC_TRANSACTION_DATA_DIST = new int[][]{
											// New-Order Transaction(8) (Read/Write|High Frequency) : Place a new Order 
											// For a randomly selected District and a randomly selected Customer from that District 
											{1, 1, 2, 2, 1, 0, 1, 0, 0},
											// Payment Transaction(5) (Read/Write|High Frequency) : Updates Customer's balance
											// For a randomly selected District and a randomly selected Customer from that District
											{1, 0, 1, 0, 2, 1, 0, 0, 0}, 
											// Order-Status Transaction(3) (Read Only|Low Frequency) : Queries the status of a Customer's last Order
											// For a randomly selected District and a randomly selected Customer from that District
											{0, 0, 0, 0, 1, 0, 1, 0, 1}, 
											// Delivery Transaction(4) (Batch|Low Frequency) : Process a batch of 10 new (not yet delivered) Orders.
											// For a given Warehouse, execute against all of the associated Districts
											{0, 0, 0, 0, 1, 0, 1, 1, 1}, 
											// Stock-Level Transaction(3) (Read Only|Low Frequency) : Determines the number of recently sold Items that have a Stock level below a specified threshold.
											// Examine the level of Stock for a given Item and then for a given unique Warehouse-District pair
											{0, 0, 1, 1, 0, 0, 0, 0, 1}  
											};
	public final static int[][] TPCC_TRANSACTIONAL_CHANGE = new int[][]{
											{0, 0, 0, 0, 0, 0, 1, 0, 0}, // 3: Insert (Order, New-Order, Order-Line)
											{0, 0, 0, 0, 0, 1, 0, 0, 0}, // 1: Insert (History)
											{0, 0, 0, 0, 0, 0, 0, 0, 0}, // 0
											{0, 0, 0, 0, 0, 0, 0, -1, 0},// 1: Delete (New-Order)
											{0, 0, 0, 0, 0, 0, 0, 0, 0}  // 0
											};
	
	public final static String hMETIS_DIR_LOCATION = "C:\\Users\\jkamal\\git\\DDBMSsim\\DDBMSsim\\lib\\native\\hMetis\\1.5.3-win32";		
	public final static String METIS_DIR_LOCATION = "C:\\Users\\jkamal\\git\\DDBMSsim\\DDBMSsim\\lib\\native\\metis\\3-win32";
	public final static String LOG_LOCATION = "C:\\Users\\jkamal\\git\\DDBMSsim\\DDBMSsim\\log";
	public final static String TEST_DATA_LOCATION = "C:\\Users\\Joarder Kamal\\git\\DDBMSsim\\DDBMSsim\\lib\\native\\moa";
	
	public final static String HMETIS = "khmetis";
	public final static String METIS = "kmetis";
	
	public static Random random;
	public static RandomDataGenerator randomDataGenerator;
	
	private static int global_tr_id;
	private static int global_data_id;
	
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
	
	public static int getGlobal_data_id() {
		return global_data_id;
	}

	public static void setGlobal_data_id(int global_data_id) {
		DBMSSimulator.global_data_id = global_data_id;
	}
	
	public static void incGlobal_data_id() {
		int id = DBMSSimulator.getGlobal_data_id();
		DBMSSimulator.setGlobal_data_id(++id);
	}

	public static void main(String[] args) throws IOException {
		random = new Random();
		random.setSeed(0);
		randomDataGenerator = new RandomDataGenerator();				
		
		// Database Server and Tenant Database Creation
		DatabaseServer dbs = new DatabaseServer(0, "simulator", DB_NODES);		
		System.out.println("[ACT] Creating Database Server \""+dbs.getDbs_name()+"\" with "+dbs.getDbs_nodes().size()+" Nodes ...");
		
		// Database creation for tenant id-"0" with Range partitioning model with 1GB Partition size	
		Database db = new Database(0, "tpcc", 0, dbs, "hash");
		System.out.println("[ACT] Creating Database \""+db.getDb_name()+"\" within "+dbs.getDbs_name()+" Database Server ...");		
		
		dbs.setDbs_tenant(db);
		
		// Perform Bootstrapping through synthetic Data generation and placing it into appropriate Partition
		System.out.println("[ACT] Started Bootstrapping Process ...");
		Bootstrapping bootstrapping = new Bootstrapping();
		bootstrapping.bootstrapping(db);
		System.out.println("[MSG] Data creation and placement into partitions have been done.");		
		
		// Printing out details after data loading
		dbs.show();
		db.show();
				
		// Preserve the Base Database Server and Database for cloning in later stage
		DatabaseServer base_dbs = new DatabaseServer(dbs);
		
		// Simulation initialisation
		SimulationMetricsLogger simulation_logger = new SimulationMetricsLogger();
		
		// Workload generation for the entire simulation		
		WorkloadGenerator workloadGenerator = new WorkloadGenerator();		
		workloadGenerator.generateWorkloads(dbs.getDbs_tenant(), DBMSSimulator.SIMULATION_RUNS, simulation_logger);		
		
		String[] partitioners = {"hgr", "chg", "gr"};
		String[] strategies = {"bs", "s1", "s2"};
		String[] directories = {hMETIS_DIR_LOCATION, METIS_DIR_LOCATION, LOG_LOCATION};
		
		// Create 9 databases under 3 partitioning schemes and 3 data movement strategies
		Map<Integer, Set<Database>> db_map = new TreeMap<Integer, Set<Database>>();
		String dir = null;
		String log_dir = directories[2];
		
		for(int i = 0; i < partitioners.length; ++i) {			
			Set<Database> db_set = new TreeSet<Database>();			
			for(int j = 0; j < strategies.length; ++j) {
				// Creating individual database servers and databases
				DatabaseServer clone_dbs = new DatabaseServer(base_dbs);
				clone_dbs.setDbs_name(partitioners[i]+"_"+strategies[j]+"_"+"dbs");
				clone_dbs.setDbs_id((i+1)*(j+1));
				
				Database clone_db = clone_dbs.getDbs_tenant();
				clone_db.setDb_dbs(clone_dbs);
				clone_db.setDb_name(partitioners[i]+"_"+strategies[j]+"_"+"db");
				clone_db.setDb_id((i+1)*(j+1));
				
				System.out.println("@ Creating database server "+clone_dbs.getDbs_name());
				System.out.println("@ Creating database "+clone_db.getDb_name());
				
				if(i == 2)
					dir = directories[1];
				else
					dir = directories[0];
				
				// Creating individual log files
				clone_db.setWorkload_log(simulation_logger.getWriter(log_dir, 
						partitioners[i]+"_"+strategies[j]+"_"+"workload_log"));
				clone_db.setNode_log(simulation_logger.getWriter(log_dir, 
						partitioners[i]+"_"+strategies[j]+"_"+"node_log"));
				clone_db.setPartition_log(simulation_logger.getWriter(log_dir, 
						partitioners[i]+"_"+strategies[j]+"_"+"partition_log"));
				
				db_set.add(clone_db);				
			}
			//System.out.println("@ i = "+i+" db_set size = "+db_set.size());
			db_map.put(i, db_set);
		}

		// Run simulations
		int simulation_run = 1;
		dir = null;
		while(simulation_run != (SIMULATION_RUNS + 1)) {			
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
		// Reapplying Database operations(insert and delete) due to Workload generation
		db.getDb_dbs().show();
		write("Reapplying database operations due to the workload generation process ...", "ACT");
		workload.reapplyDbOperations(db);
		
		// Update Node level load
		db.getDb_dbs().updateNodeLoad();
		db.getDb_dbs().show();
		
		// Initialisation
		WorkloadFileGenerator workloadFileGenerator = new WorkloadFileGenerator();
		ClusterIdMapper cluster_id_mapper = new ClusterIdMapper();
		DataMovement data_movement = new DataMovement();
		
		write("Started with "+workload.getWrl_totalTransactions()+" transactions from original workload.", "MSG");
		
		// Perform workload sampling
		write("Starting workload sampling to remove duplicate transactions ...", "ACT");
		Workload sampled_workload = workload.performSampling(db);		
		sampled_workload.initialise(db);
		//sampled_workload.show(db, "");
		
		// Perform transaction classification
		// Classify the workload transactions based on whether they are distributed or not (Red/Orange/Green List)
		write("Starting workload classification to identify RED and ORANGE transactions ...", "ACT");				
		TransactionClassifier transactionClassifier = new TransactionClassifier();
		int target_transactions = transactionClassifier.classifyTransactions(db, sampled_workload);
				
		// Assign Shadow HMetis Data Id and generate workload and fix files
		workloadFileGenerator.assignShadowDataId(db, sampled_workload);		
		write("Total "+target_transactions+" transactions having "+sampled_workload.getWrl_totalDataObjects()+" data objects have been identified for partitioning.", "MSG");		
		//sampled_workload.show(db, "");
		
		// Generate workload and fix-files for partitioning
		boolean empty = workloadFileGenerator.generateWorkloadFile(db, sampled_workload, partitioner);
		int virtual_data = 0;
		if(partitioner == "chg") {
			virtual_data = workloadFileGenerator.getVirtual_data();
			//System.out.println("-->> virtual data = "+virtual_data);
		}		
		
		if(!empty) {
		// Perform hyper-graph/graph/compressed hyper-graph partitioning
		runPartitioner(db, sampled_workload, partitioner, virtual_data);		

		write("Applying data movement strategies for database ("+db.getDb_name()+") ...", "ACT");
		write("***********************************************************************************************************************", null);
		
		// Log collection before data movement operation
		simulation_logger.setData_hasMoved(false);
		collectLog(simulation_logger, db, sampled_workload, db.getWorkload_log(), db.getNode_log(), db.getPartition_log(), partitioner);
		
		// Mapping cluster id to partition id
		cluster_id_mapper.processPartFile(db, sampled_workload, db.getDb_partitions(), directory, partitioner, virtual_data);		
		//db.show();
		
		// Perform data movement		
		data_movement.performDataMovement(db, sampled_workload, strategy, partitioner);
		
		// Update Node level load
		db.getDb_dbs().updateNodeLoad();
		
		// Log collection after data movement operation
		simulation_logger.setData_hasMoved(true);
		collectLog(simulation_logger, db, sampled_workload, db.getWorkload_log(), db.getNode_log(), db.getPartition_log(), partitioner);
		
		db.getDb_dbs().show();
		
		write("***********************************************************************************************************************", null);
		} else {
			write("Simulation run round aborted for database ("+db.getDb_name()+")","OUT");
			write("***********************************************************************************************************************", null);
		}
	}
	
	private static void runPartitioner(Database db, Workload workload, String partitioner, int virtual_data) throws IOException {		
		switch(partitioner) {
		case "hgr":
			// Run hMetis HyperGraph Partitioning
			HGraphMinCut hgraphMinCut = new HGraphMinCut(db, workload, HMETIS, "hgr", virtual_data); 		
			hgraphMinCut.runHMetis();

			// Wait for 5 seconds to ensure that the Part files have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			break;
			
		case "chg":			
			HGraphMinCut chgraphMinCut = new HGraphMinCut(db, workload, HMETIS, "chg", virtual_data); 		
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
			GraphMinCut graphMinCut = new GraphMinCut(db, workload, METIS); 		
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
		
		logger.logWorkload(workload, wrl_writer, partitioner);
		logger.logNode(db, workload, node_writer);
		logger.logPartition(db, workload, partition_writer);		
	}
}
