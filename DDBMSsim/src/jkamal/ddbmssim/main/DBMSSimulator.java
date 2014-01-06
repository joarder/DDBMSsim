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
import jkamal.ddbmssim.graph.GraphMinCut;
import jkamal.ddbmssim.hgraph.HGraphMinCut;
import jkamal.ddbmssim.bootstrap.Bootstrapping;
import jkamal.ddbmssim.io.SimulationMetricsLogger;
import jkamal.ddbmssim.workload.ClusterIdMapper;
import jkamal.ddbmssim.workload.Workload;
import jkamal.ddbmssim.workload.WorkloadGenerator;

public class DBMSSimulator {	
	public final static int DB_SERVERS = 3;
	public final static String WORKLOAD_TYPE = "TPC-C";
	public final static int DATA_OBJECTS = 5200; // 10GB Data (in Size) // 5,200 for a TPC-C Database (scaled down by 1K for individual table row counts)
	public final static int TRANSACTION_NUMS = 1000;
	public final static int SIMULATION_RUN_NUMBERS = 24;
	
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
		
		//Database creation for tenant id-"0" with Range partitioning model with 1GB Partition size
		//Database db = new Database(0, "test-db", 0, "Range", 0.01);
		//Database db = new Database(0, "test-db", 0, "Range", 0.1);
		Database db = new Database(0, "testdb", 0, "Range", 1);
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
		// Hypergraph/Compressed Hypergraph/Graph Partitioning and Data Movement		
		ClusterIdMapper cluster_id_mapper = new ClusterIdMapper();		
		DataMovement data_movement = new DataMovement();

		// For HyperGraph Partitioning
		Database hgr_bs_db = new Database(db);
		Database hgr_s1_db = new Database(db);
		Database hgr_s2_db = new Database(db);
		// For Compressed HyperGraph Partitioning
		Database chg_bs_db = new Database(db);
		Database chg_s1_db = new Database(db);
		Database chg_s2_db = new Database(db);
		// For Graph Partitioning
		Database gr_bs_db = new Database(db);
		Database gr_s1_db = new Database(db);
		Database gr_s2_db = new Database(db);
		
		SimulationMetricsLogger sim_logger = new SimulationMetricsLogger();
		// For HyperGraph Partitioning		
		PrintWriter hgr_bs_db_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "hgr_bs_db_log");
		PrintWriter hgr_s1_db_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "hgr_s1_db_log");
		PrintWriter hgr_s2_db_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "hgr_s2_db_log");
		PrintWriter hgr_partition_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "hgr_partition_log");
		PrintWriter hgr_workload_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "hgr_workload_log");
		// For Compressed HyperGraph Partitioning		
		PrintWriter chg_bs_db_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "chg_bs_db_log");
		PrintWriter chg_s1_db_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "chg_s1_db_log");
		PrintWriter chg_s2_db_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "chg_s2_db_log");
		PrintWriter chg_partition_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "chg_partition_log");
		PrintWriter chg_workload_log = sim_logger.getWriter(hMETIS_DIR_LOCATION, "chg_workload_log");
		// For Graph Partitioning		
		PrintWriter gr_bs_db_log = sim_logger.getWriter(METIS_DIR_LOCATION, "gr_bs_db_log");
		PrintWriter gr_s1_db_log = sim_logger.getWriter(METIS_DIR_LOCATION, "gr_s1_db_log");
		PrintWriter gr_s2_db_log = sim_logger.getWriter(METIS_DIR_LOCATION, "gr_s2_db_log");		
		PrintWriter gr_partition_log = sim_logger.getWriter(METIS_DIR_LOCATION, "gr_partition_log");
		PrintWriter gr_workload_log = sim_logger.getWriter(METIS_DIR_LOCATION, "gr_workload_log");		
		
		int simulation_run = 0;	
		while(simulation_run != SIMULATION_RUN_NUMBERS) {			
			Workload workload = workloadGenerator.getWorkload_map().get(simulation_run);			
			workload.setMessage("in");
			//workload.calculateDTImapct(db);
			//workload.calculateDTPercentage();
			
			//==============================================================================================
			// Run hMetis HyperGraph Partitioning
			HGraphMinCut hgraphMinCut = new HGraphMinCut(workload, HMETIS, db.getDb_partitions().size(), "hgr"); 		
			hgraphMinCut.runHMetis();

			// Wait for 5 seconds to ensure that the Part files have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
			
			//==============================================================================================
			// Run hMetis HyperGraph Partitioning for Compressed Hypergraph
			HGraphMinCut chgraphMinCut = new HGraphMinCut(workload, HMETIS, db.getDb_partitions().size(), "chg"); 		
			chgraphMinCut.runHMetis();

			// Wait for 5 seconds to ensure that the Part files have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			//==============================================================================================
			// Run Metis Graph Partitioning							
			GraphMinCut graphMinCut = new GraphMinCut(workload, METIS, db.getDb_partitions().size()); 		
			graphMinCut.runMetis();
			
			// Wait for 5 seconds to ensure that the Part files have been generated properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}					
			
		//=== Base Strategy
			// Logging
			sim_logger.setData_movement(false);
			collectLog(sim_logger, hgr_bs_db, workload, hgr_bs_db_log, hgr_workload_log, hgr_partition_log, "hgr");
			collectLog(sim_logger, chg_bs_db, workload, chg_bs_db_log, chg_workload_log, chg_partition_log, "chg");
			collectLog(sim_logger, gr_bs_db, workload, gr_bs_db_log, gr_workload_log, gr_partition_log, "gr");
			
			System.out.println("[ACT] Replaying Workload Capture using Base Strategy ...");
			System.out.println("***********************************************************************************************************************");	
			
			// Read Part file and assign corresponding Data cluster Id			
			cluster_id_mapper.processPartFile(hgr_bs_db, workload, hgr_bs_db.getDb_partitions().size(), hMETIS_DIR_LOCATION, "hgr");			
			cluster_id_mapper.processPartFile(chg_bs_db, workload, chg_bs_db.getDb_partitions().size(), hMETIS_DIR_LOCATION, "chg");
			cluster_id_mapper.processPartFile(gr_bs_db, workload, gr_bs_db.getDb_partitions().size(), METIS_DIR_LOCATION, "gr");
			
			// Perform Data Movement following One(Cluster)-to-One(Partition) and Many(Cluster)-to-One(Partition)
			System.out.println("[ACT] Base Strategy[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) Peer | Using Hypergraph Partitioning");
			data_movement.baseStrategy(hgr_bs_db, workload, "hgr");
			System.out.println("=======================================================================================================================");
			System.out.println("[ACT] Base Strategy[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) Peer | Using Compressed Hypergraph Partitioning");
			data_movement.baseStrategy(chg_bs_db, workload, "chg");
			System.out.println("=======================================================================================================================");
			System.out.println("[ACT] Base Strategy[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) Peer | Using Graph Partitioning");
			data_movement.baseStrategy(gr_bs_db, workload, "gr");
			
			sim_logger.setData_movement(true);
			collectLog(sim_logger, hgr_bs_db, workload, hgr_bs_db_log, hgr_workload_log, hgr_partition_log, "hgr");
			collectLog(sim_logger, chg_bs_db, workload, chg_bs_db_log, chg_workload_log, chg_partition_log, "chg");
			collectLog(sim_logger, gr_bs_db, workload, gr_bs_db_log, gr_workload_log, gr_partition_log, "gr");
			
			System.out.println("***********************************************************************************************************************");
			
			// Wait for 5 seconds to ensure that the files have been written out properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
			
		//=== Strategy-1
			// Logging
			workload.setMessage("in");
			sim_logger.setData_movement(false);
			collectLog(sim_logger, hgr_s1_db, workload, hgr_s1_db_log, hgr_workload_log, hgr_partition_log, "hgr");
			collectLog(sim_logger, chg_s1_db, workload, chg_s1_db_log, chg_workload_log, chg_partition_log, "chg");
			collectLog(sim_logger, gr_s1_db, workload, gr_s1_db_log, gr_workload_log, gr_partition_log, "gr");
			
			System.out.println("[ACT] Replaying Workload Capture using Strategy-1 ...");
			System.out.println("***********************************************************************************************************************");
			
			// Read Part file and assign corresponding Data cluster Id			
			cluster_id_mapper.processPartFile(hgr_s1_db, workload, hgr_s1_db.getDb_partitions().size(), hMETIS_DIR_LOCATION, "hgr");
			cluster_id_mapper.processPartFile(chg_s1_db, workload, chg_s1_db.getDb_partitions().size(), hMETIS_DIR_LOCATION, "chg");
			cluster_id_mapper.processPartFile(gr_s1_db, workload, gr_s1_db.getDb_partitions().size(), METIS_DIR_LOCATION, "gr");
			
			System.out.println("[ACT] Strategy-1[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) [Column Max] | Using Hypergraph Partitioning");
			data_movement.strategy1(hgr_s1_db, workload, "hgr");
			System.out.println("=======================================================================================================================");
			System.out.println("[ACT] Strategy-1[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) [Column Max] | Using Compressed Hypergraph Partitioning");
			data_movement.strategy1(chg_s1_db, workload, "chg");
			System.out.println("=======================================================================================================================");
			System.out.println("[ACT] Strategy-1[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Partition) [Column Max] | Using Graph Partitioning");
			data_movement.strategy1(gr_s1_db, workload, "gr");
			
			sim_logger.setData_movement(true);
			collectLog(sim_logger, hgr_s1_db, workload, hgr_s1_db_log, hgr_workload_log, hgr_partition_log, "hgr");
			collectLog(sim_logger, chg_s1_db, workload, chg_s1_db_log, chg_workload_log, chg_partition_log, "chg");
			collectLog(sim_logger, gr_s1_db, workload, gr_s1_db_log, gr_workload_log, gr_partition_log, "gr");
			
			System.out.println("***********************************************************************************************************************");
			
			// Wait for 5 seconds to ensure that the files have been written out properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		//=== Strategy-2
			// Logging
			workload.setMessage("in");
			sim_logger.setData_movement(false);
			collectLog(sim_logger, hgr_s2_db, workload, hgr_s2_db_log, hgr_workload_log, hgr_partition_log, "hgr");
			collectLog(sim_logger, chg_s2_db, workload, chg_s2_db_log, chg_workload_log, chg_partition_log, "chg");
			collectLog(sim_logger, gr_s2_db, workload, gr_s2_db_log, gr_workload_log, gr_partition_log, "gr");
			
			System.out.println("[ACT] Replaying Workload Capture using Strategy-2 ...");
			System.out.println("***********************************************************************************************************************");

			// Read Part file and assign corresponding Data cluster Id			
			cluster_id_mapper.processPartFile(hgr_s2_db, workload, hgr_s2_db.getDb_partitions().size(), hMETIS_DIR_LOCATION, "hgr");
			cluster_id_mapper.processPartFile(chg_s2_db, workload, chg_s2_db.getDb_partitions().size(), hMETIS_DIR_LOCATION, "chg");
			cluster_id_mapper.processPartFile(gr_s2_db, workload, gr_s2_db.getDb_partitions().size(), METIS_DIR_LOCATION, "gr");
			
			System.out.println("[ACT] Strategy-2[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Unique Partition) [Sub Matrix Max] | Using Hypergraph Partitioning");			
			data_movement.strategy2(hgr_s2_db, workload, "hgr");
			System.out.println("=======================================================================================================================");
			System.out.println("[ACT] Strategy-2[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Unique Partition) [Sub Matrix Max] | Using Compressed Hypergraph Partitioning");			
			data_movement.strategy2(chg_s2_db, workload, "chg");
			System.out.println("=======================================================================================================================");
			System.out.println("[ACT] Strategy-2[Simulation Round-"+simulation_run+"] :: One(Cluster)-to-One(Unique Partition) [Sub Matrix Max] | Using Graph Partitioning");
			data_movement.strategy2(gr_s2_db, workload, "gr");
			
			sim_logger.setData_movement(true);
			collectLog(sim_logger, hgr_s2_db, workload, hgr_s2_db_log, hgr_workload_log, hgr_partition_log, "hgr");
			collectLog(sim_logger, chg_s2_db, workload, chg_s2_db_log, chg_workload_log, chg_partition_log, "chg");
			collectLog(sim_logger, gr_s2_db, workload, gr_s2_db_log, gr_workload_log, gr_partition_log, "gr");
			
			System.out.println("***********************************************************************************************************************");
			
			// Wait for 5 seconds to ensure that the files have been written out properly
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			++ simulation_run;
		}
		
		// End Logging
		hgr_bs_db_log.flush();
		hgr_s1_db_log.flush();
		hgr_s2_db_log.flush();
		hgr_workload_log.flush();
		hgr_partition_log.flush();
		
		chg_bs_db_log.flush();
		chg_s1_db_log.flush();
		chg_s2_db_log.flush();
		chg_workload_log.flush();
		chg_partition_log.flush();
		
		gr_bs_db_log.flush();
		gr_s1_db_log.flush();
		gr_s2_db_log.flush();
		gr_workload_log.flush();
		gr_partition_log.flush();
		
		hgr_bs_db_log.close();
		hgr_s1_db_log.close();
		hgr_s2_db_log.close();
		hgr_workload_log.close();
		hgr_partition_log.close();
				
		chg_bs_db_log.close();
		chg_s1_db_log.close();
		chg_s2_db_log.close();
		chg_workload_log.close();
		chg_partition_log.close();
		
		gr_bs_db_log.close();
		gr_s1_db_log.close();
		gr_s2_db_log.close();
		gr_workload_log.close();
		gr_partition_log.close();
	}
	
	private static void collectLog(SimulationMetricsLogger logger, Database db, Workload workload
			, PrintWriter db_writer, PrintWriter wrl_writer, PrintWriter part_writer, String type) {
		logger.logDb(db, workload, db_writer);
		logger.logWorkload(db, workload, wrl_writer, type);
		logger.logPartition(db, workload, part_writer);	
	}
}
