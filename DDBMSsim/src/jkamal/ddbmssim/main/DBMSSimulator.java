/**
 * @author Joarder Kamal
 * 
 * Main Class to run the Database Simulator
 */

package jkamal.ddbmssim.main;

import java.io.IOException;
import java.io.PrintWriter;
import org.apache.commons.math3.random.RandomDataGenerator;
import jkamal.ddbmssim.db.DataMovement;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.DatabaseServer;
import jkamal.ddbmssim.hgraph.HGraphMinCut;
import jkamal.ddbmssim.bootstrap.Bootstrapping;
import jkamal.ddbmssim.io.SimulationMetricsLogger;
import jkamal.ddbmssim.workload.HGraphClusters;
import jkamal.ddbmssim.workload.Workload;
import jkamal.ddbmssim.workload.WorkloadGenerator;

public class DBMSSimulator {	
	public final static int DB_SERVERS = 3;
	public final static String WORKLOAD_TYPE = "TPC-C";
	public final static int DATA_OBJECTS = 72; // 10GB Data (in Size)
	public final static int TRANSACTION_NUMS = 10;
	public final static int SIMULATION_RUN_NUMBERS = 3;
	
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
		DatabaseServer dbs = new DatabaseServer(0, "test-dbs", DB_SERVERS);		
		System.out.println("[ACT] Creating Database Server \""+dbs.getDbs_name()+"\" with "+dbs.getDbs_nodes().size()+" Nodes ...");
		
		// Database creation for tenant id-"0" with Range partitioning model with 1GB Partition size
		Database db = new Database(0, "test-db", 0, "Range", 0.01);
		//Database db = new Database(0, "test-db", 0, "Range", 0.1);
		//Database db = new Database(0, "testdb", 0, "Range", 1);
		System.out.println("[ACT] Creating Database \""+db.getDb_name()+"\" within "+dbs.getDbs_name()+" Database Server ...");		
		
		// Perform Bootstrapping through synthetic Data generation and placing it into appropriate Partition
		System.out.println("[ACT] Started Bootstrapping Process ...");
		System.out.println("[ACT] Generating "+ DATA_OBJECTS +" synthetic data items ...");

		Bootstrapping bootstrapping = new Bootstrapping();
		bootstrapping.bootstrapping(dbs, db, DATA_OBJECTS);
		System.out.println("[MSG] Data creation and placement into partitions done.");
		
		// Printing out details after data loading
		dbs.show();
		db.show();
		
		//==============================================================================================
		// Workload generation for the entire simulation		
		WorkloadGenerator workloadGenerator = new WorkloadGenerator();		
		workloadGenerator.generateWorkloads(dbs, db);		
		
		//==============================================================================================
		// Hypergraph Partitioning and Data Movement
		
		HGraphClusters hGraphClusters_bs = new HGraphClusters();
		HGraphClusters hGraphClusters_s1 = new HGraphClusters();
		HGraphClusters hGraphClusters_s2 = new HGraphClusters();
		
		DataMovement bs_dataMovement = new DataMovement();
		DataMovement s1_dataMovement = new DataMovement();
		DataMovement s2_dataMovement = new DataMovement();

		Database bs_db = new Database(db);
		Database s1_db = new Database(db);
		Database s2_db = new Database(db);
		
		SimulationMetricsLogger logger = new SimulationMetricsLogger();
		PrintWriter bs_db_log = logger.getWriter(hMETIS_DIR_LOCATION, "bs_db_log");
		PrintWriter s1_db_log = logger.getWriter(hMETIS_DIR_LOCATION, "s1_db_log");
		PrintWriter s2_db_log = logger.getWriter(hMETIS_DIR_LOCATION, "s2_db_log");
		
		PrintWriter workload_log = logger.getWriter(hMETIS_DIR_LOCATION, "workload_log");		
		PrintWriter partition_log = logger.getWriter(hMETIS_DIR_LOCATION, "partition_log");
		
		int simulation_run = 0;	
		while(simulation_run != DBMSSimulator.SIMULATION_RUN_NUMBERS) {			
			Workload workload = workloadGenerator.getWorkload_map().get(simulation_run);			
			workload.setMessage("in");
			
			//==============================================================================================
			// Run hMetis HyperGraph Partitioning							
			HGraphMinCut minCut = new HGraphMinCut(workload, HMETIS, db.getDb_partitions().size()); 		
			minCut.runHMetis();
			
			// Wait for 5 seconds to ensure that the Part file have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}					
			
		//=== Base Strategy
			// Logging
			logger.setData_movement(false);
			collectLog(logger, bs_db, workload, bs_db_log, workload_log, partition_log);
			
			System.out.println("[ACT] Replaying Workload Capture using Base Strategy ...");
			System.out.println("***********************************************************************************************************************");	
			
			// Read Part file and assign corresponding Data cluster Id			
			hGraphClusters_bs.readPartFile(bs_db, workload, bs_db.getDb_partitions().size());
			
			// Perform Data Movement following One(Cluster)-to-One(Partition) and Many(Cluster)-to-One(Partition)
			System.out.println("[ACT] Base Strategy[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) Peer");
			bs_dataMovement.baseStrategy(bs_db, workload);
			
			logger.setData_movement(true);
			collectLog(logger, bs_db, workload, bs_db_log, workload_log, partition_log);
			
			System.out.println("***********************************************************************************************************************");
			
		//=== Strategy-1
			// Logging
			logger.setData_movement(false);
			collectLog(logger, s1_db, workload, s1_db_log, workload_log, partition_log);
			
			System.out.println("[ACT] Replaying Workload Capture using Strategy-1 ...");
			System.out.println("***********************************************************************************************************************");
			
			// Read Part file and assign corresponding Data cluster Id			
			hGraphClusters_s1.readPartFile(s1_db, workload, s1_db.getDb_partitions().size());
			
			System.out.println("[ACT] Strategy-1[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) [Column Max]");			
			s1_dataMovement.strategy1(s1_db, workload);
			
			logger.setData_movement(true);
			collectLog(logger, s1_db, workload, s1_db_log, workload_log, partition_log);
			
			System.out.println("***********************************************************************************************************************");
			
		//=== Strategy-2
			// Logging
			logger.setData_movement(false);
			collectLog(logger, s2_db, workload, s2_db_log, workload_log, partition_log);
			
			System.out.println("[ACT] Replaying Workload Capture using Strategy-2 ...");
			System.out.println("***********************************************************************************************************************");

			// Read Part file and assign corresponding Data cluster Id			
			hGraphClusters_s2.readPartFile(s2_db, workload, s2_db.getDb_partitions().size());
			
			System.out.println("[ACT] Strategy-2[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Unique Partition) [Sub Matrix Max]");			
			s2_dataMovement.strategy2(s2_db, workload);
			
			logger.setData_movement(true);
			collectLog(logger, s2_db, workload, s2_db_log, workload_log, partition_log);
			
			System.out.println("***********************************************************************************************************************");
			
			++ simulation_run;
		}
		
		// End Logging
		bs_db_log.flush();
		s1_db_log.flush();
		s2_db_log.flush();
		workload_log.flush();
		partition_log.flush();
		
		bs_db_log.close();
		s1_db_log.close();
		s2_db_log.close();
		workload_log.close();
		partition_log.close();
	}
	
	private static void collectLog(SimulationMetricsLogger logger, Database db, Workload workload
			, PrintWriter db_writer, PrintWriter wrl_writer, PrintWriter part_writer) {
		logger.logDb(db, workload, db_writer);
		logger.logWorkload(db, workload, wrl_writer);
		logger.logPartition(db, workload, part_writer);	
	}
}
