/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.TreeMap;
import jkamal.ddbmssim.db.Database;

public class Workload implements Comparable<Workload> {
	private int wrl_id;
	private String wrl_label;
	private int wrl_db_id;		
	
	private Map<Integer, ArrayList<Transaction>> wrl_tr_map;
	
	private int wrl_tr_types; // Represents the number of Transaction types. e.g. for AuctionMark it is 10
	private int[] wrl_tr_prop;
	private int wrl_tr_red;
	private int wrl_tr_green;
	private int wrl_tr_orange;
	
	private int wrl_init_total_tr;
	private int wrl_total_tr;
	private int wrl_total_data;
	
	private Map<Integer, Set<Integer>> wrl_dataTransactionsInvolved;
	
	private int wrl_tr_borning;
	private double wrl_tr_birth_rate;
	private int[] wrl_tr_birth_prop;
	
	private int wrl_tr_dying;	
	private double wrl_tr_death_rate;	
	private int[] wrl_tr_death_prop;		

	private Map<Integer, Integer> wrl_dataId_shadowId_map;
	private Map<Integer, Integer> wrl_hg_dataId_clusterId_map;
	private Map<Integer, Integer> wrl_chg_dataId_clusterId_map;
	private Map<Integer, Integer> wrl_chg_virtualDataId_clusterId_map;
	private Map<Integer, Integer> wrl_gr_dataId_clusterId_map;	
	
	private String wrl_hg_workload_file = null;
	private String wrl_hg_fix_file = null;
	private String wrl_chg_workload_file = null;
	private String wrl_chg_fix_file = null;
	private String wrl_gr_workload_file = null;
		
	private double wrl_mean_dti;
	private int wrl_dt_nums;
	private int[] wrl_dt_nums_typewise;
	private double wrl_percentage_dt;
	
	private double wrl_hg_percentage_intra_dmv;
	private double wrl_hg_percentage_inter_dmv;
	private int wrl_hg_intra_dmv;
	private int wrl_hg_inter_dmv;

	private double wrl_chg_percentage_intra_dmv;
	private double wrl_chg_percentage_inter_dmv;
	private int wrl_chg_intra_dmv;
	private int wrl_chg_inter_dmv;
	
	private double wrl_gr_percentage_intra_dmv;
	private double wrl_gr_percentage_inter_dmv;
	private int wrl_gr_intra_dmv;
	private int wrl_gr_inter_dmv;
	
	private boolean wrl_has_dmv;
	private String wrl_dmv_strategy;
	private String message = null;
	
	private HashMap<Integer, ArrayList<Integer>> db_operations;	
	private int db_operation_count;	
	
	public Workload(int id, int trTypes, int db_id) {
		this.setWrl_id(id);
		this.setWrl_label("W"+id);
		this.setWrl_database_id(db_id);
		
		this.setWrl_transactionTypes(trTypes);
		this.setWrl_transactionProp(new int[this.getWrl_transactionTypes()]);
		this.setWrl_tr_red(0);
		this.setWrl_tr_orange(0);
		this.setWrl_tr_green(0);
		this.setWrl_transactionMap(new TreeMap<Integer, ArrayList<Transaction>>());
		this.setWrl_initTotalTransactions(0);
		this.setWrl_totalTransaction(0);
		this.setWrl_totalDataObjects(0);				
		
		this.setWrl_transactionBorning(0);
		this.setWrl_transactionDying(0);
		this.setWrl_transactionBirthRate(0.0d);
		this.setWrl_transactionDeathRate(0.0d);
		this.setWrl_transactionBirthProp(new int[this.getWrl_transactionTypes()]);
		this.setWrl_transactionDeathProp(new int[this.getWrl_transactionTypes()]);				
		
		this.setWrl_dataId_shadowId_map(new TreeMap<Integer, Integer>());
		this.setWrl_hg_dataId_clusterId_map(new TreeMap<Integer, Integer>());
		this.setWrl_chg_dataId_clusterId_map(new TreeMap<Integer, Integer>());
		this.setWrl_chg_virtualDataId_clusterId_map(new TreeMap<Integer, Integer>());
		this.setWrl_gr_dataId_clusterId_map(new TreeMap<Integer, Integer>());
		
		this.setWrl_hGraphWorkloadFile("workload.txt");
		this.setWrl_hGraphFixFile("fixfile.txt");		
		this.setWrl_chGraphWorkloadFile("workload.txt");
		this.setWrl_chGraphFixFile("fixfile.txt");
		this.setWrl_graphWorkloadFile("workload.txt");
		
		this.setWrl_distributedTransactions(0);
		//this.setWrl_dt_nums_typewise(new int[this.getWrl_transactionTypes()]);
		this.setWrl_meanDTI(0.0);		
		this.setWrl_percentageDistributedTransactions(0.0);
		
		this.setWrl_hg_percentageIntraNodeDataMovements(0.0);
		this.setWrl_hg_percentageInterNodeDataMovements(0.0);
		this.setWrl_hg_intraNodeDataMovements(0);
		this.setWrl_hg_interNodeDataMovements(0);
		
		this.setWrl_chg_percentageIntraNodeDataMovements(0.0);
		this.setWrl_chg_percentageInterNodeDataMovements(0.0);
		this.setWrl_chg_intraNodeDataMovements(0);
		this.setWrl_chg_interNodeDataMovements(0);
		
		this.setWrl_gr_percentageIntraNodeDataMovements(0.0);
		this.setWrl_gr_percentageInterNodeDataMovements(0.0);
		this.setWrl_gr_intraNodeDataMovements(0);
		this.setWrl_gr_interNodeDataMovements(0);
		
		this.setWrl_hasDataMoved(false);
		this.setWrl_data_movement_strategy(null);
		this.setMessage(" (Initial Stage) ");
		
		this.setDb_operations(new HashMap<Integer, ArrayList<Integer>>());
		this.setDb_operation_count(0);
	}

	// Copy Constructor
	public Workload(Workload workload) {
		this.setWrl_id(workload.getWrl_id());
		this.setWrl_label(workload.getWrl_label());
		this.setWrl_database_id(workload.getWrl_database_id());	
		
		this.setWrl_transactionTypes(workload.getWrl_transactionTypes());
		
		int[] cloneTransactionProp = new int[this.wrl_tr_types];		
		System.arraycopy(workload.getWrl_transactionProportions(), 0, cloneTransactionProp, 0, workload.getWrl_transactionProportions().length);
		this.setWrl_transactionProp(cloneTransactionProp);
		
		this.setWrl_tr_red(workload.getWrl_tr_red());
		this.setWrl_tr_orange(workload.getWrl_tr_orange());
		this.setWrl_tr_green(workload.getWrl_tr_green());
		
		this.setWrl_transactionBorning(workload.getWrl_transactionBorning());
		this.setWrl_transactionDying(workload.getWrl_transactionDying());		
		this.setWrl_transactionBirthRate(workload.getWrl_transactionBirthRate());
		this.setWrl_transactionDeathRate(workload.getWrl_transactionDeathRate());
		
		int[] cloneTransactionBirthProp = new int[this.wrl_tr_types];		
		System.arraycopy(workload.getWrl_transactionBirthProp(), 0, cloneTransactionBirthProp, 0, workload.getWrl_transactionBirthProp().length);
		this.setWrl_transactionBirthProp(cloneTransactionBirthProp);				
		
		int[] cloneTransactionDeathProp = new int[this.wrl_tr_types];		
		System.arraycopy(workload.getWrl_transactionDeathProportions(), 0, cloneTransactionDeathProp, 0, workload.getWrl_transactionDeathProportions().length);
		this.setWrl_transactionDeathProp(cloneTransactionDeathProp);
		
		Map<Integer, ArrayList<Transaction>> cloneTransactionMap = new TreeMap<Integer, ArrayList<Transaction>>();
		int cloneTransactionType;		
		ArrayList<Transaction> cloneTransactionList;
		Transaction cloneTransaction;		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			cloneTransactionType = entry.getKey();
			cloneTransactionList = new ArrayList<Transaction>();
			for(Transaction tr : entry.getValue()) {
				cloneTransaction = new Transaction(tr);
				cloneTransactionList.add(cloneTransaction);
			}
			cloneTransactionMap.put(cloneTransactionType, cloneTransactionList);
		}
		this.setWrl_transactionMap(cloneTransactionMap);
		this.setWrl_initTotalTransactions(workload.getWrl_initTotalTransactions());
		this.setWrl_totalTransaction(workload.getWrl_totalTransactions());						
		this.setWrl_totalDataObjects(workload.getWrl_totalDataObjects());				
		
		//Shadow Id
		Map<Integer, Integer> clone_dataId_shadowId_map = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : workload.getWrl_dataId_shadowId_map().entrySet())
			clone_dataId_shadowId_map.put(entry.getKey(), entry.getValue());
		this.setWrl_dataId_shadowId_map(clone_dataId_shadowId_map);
		
		//HyperGraph
		Map<Integer, Integer> clone_dataId_clusterId_map = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : workload.getWrl_hg_dataId_clusterId_map().entrySet())
			clone_dataId_clusterId_map.put(entry.getKey(), entry.getValue());
		this.setWrl_hg_dataId_clusterId_map(clone_dataId_clusterId_map);
		
		//Compressed HyperGraph
		Map<Integer, Integer> clone_chg_dataId_clusterId_map = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : workload.getWrl_chg_dataId_clusterId_map().entrySet())
			clone_chg_dataId_clusterId_map.put(entry.getKey(), entry.getValue());
		this.setWrl_chg_dataId_clusterId_map(clone_chg_dataId_clusterId_map);
		
		Map<Integer, Integer> clone_chg_virtualDataId_clusterId_map = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : workload.getWrl_chg_virtualDataId_clusterId_map().entrySet())
			clone_chg_virtualDataId_clusterId_map.put(entry.getKey(), entry.getValue());
		this.setWrl_chg_virtualDataId_clusterId_map(clone_chg_virtualDataId_clusterId_map);
		
		//Graph
		Map<Integer, Integer> clone_gr_dataId_clusterId_map = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : workload.getWrl_gr_dataId_clusterId_map().entrySet())
			clone_gr_dataId_clusterId_map.put(entry.getKey(), entry.getValue());
		this.setWrl_gr_dataId_clusterId_map(clone_gr_dataId_clusterId_map);
		
		//HyperGraph Files
		this.setWrl_hGraphWorkloadFile(workload.getWrl_hGraphWorkloadFile());
		this.setWrl_hGraphFixFile(workload.getWrl_hGraphFixFile());
		//Compressed HyperGraph Files
		this.setWrl_chGraphWorkloadFile(workload.getWrl_chGraphWorkloadFile());
		this.setWrl_chGraphFixFile(workload.getWrl_chGraphFixFile());
		//Graph Files
		this.setWrl_graphWorkloadFile(workload.getWrl_graphWorkloadFile());
		
		this.setWrl_distributedTransactions(workload.getWrl_distributedTransactions());
		this.setWrl_meanDTI(workload.getWrl_meanDTI());		
		this.setWrl_percentageDistributedTransactions(workload.getWrl_percentageDistributedTransactions());
		//Hypergraph
		this.setWrl_hg_percentageIntraNodeDataMovements(workload.getWrl_hg_percentageIntraNodeDataMovements());
		this.setWrl_hg_percentageInterNodeDataMovements(workload.getWrl_hg_percentageInterNodeDataMovements());
		this.setWrl_hg_intraNodeDataMovements(workload.getWrl_hgr_intraNodeDataMovements());
		this.setWrl_hg_interNodeDataMovements(workload.getWrl_hgr_interNodeDataMovements());
		//Compressed Hypergraph				
		this.setWrl_chg_percentageIntraNodeDataMovements(workload.getWrl_chg_percentageIntraNodeDataMovements());
		this.setWrl_chg_percentageInterNodeDataMovements(workload.getWrl_chg_percentageInterNodeDataMovements());
		this.setWrl_chg_intraNodeDataMovements(workload.getWrl_chg_intraNodeDataMovements());
		this.setWrl_chg_interNodeDataMovements(workload.getWrl_chg_interNodeDataMovements());
		//Graph
		this.setWrl_gr_percentageIntraNodeDataMovements(workload.getWrl_gr_percentageIntraNodeDataMovements());
		this.setWrl_gr_percentageInterNodeDataMovements(workload.getWrl_gr_percentageInterNodeDataMovements());
		this.setWrl_gr_intraNodeDataMovements(workload.getWrl_gr_intraNodeDataMovements());
		this.setWrl_gr_interNodeDataMovements(workload.getWrl_gr_interNodeDataMovements());
		
		this.setWrl_hasDataMoved(workload.isWrl_hasDataMoved());
		this.setWrl_data_movement_strategy(workload.getWrl_data_movement_strategy());
		this.setMessage(workload.getMessage());
		
		HashMap<Integer, ArrayList<Integer>> clone_db_operations = new HashMap<Integer, ArrayList<Integer>>();
		ArrayList<Integer> clone_op_param_list;
		for(Entry<Integer, ArrayList<Integer>> entry : workload.getDb_operations().entrySet()){
			clone_op_param_list = new ArrayList<Integer>();
			for(Integer clone_value : entry.getValue()) 
				clone_op_param_list.add(clone_value);
			
			clone_db_operations.put(entry.getKey(), clone_op_param_list);
		}
		this.setDb_operations(clone_db_operations);
		
		this.setDb_operation_count(workload.getDb_operation_count());
		//this.setWrl_dt_nums_typewise(new int[this.getWrl_transactionTypes()]);
	}

	public int getWrl_id() {
		return wrl_id;
	}

	public void setWrl_id(int id) {
		this.wrl_id = id;
	}

	public int getWrl_database_id() {
		return wrl_db_id;
	}

	public String getWrl_label() {
		return wrl_label;
	}

	public void setWrl_label(String label) {
		this.wrl_label = label;
	}

	public void setWrl_database_id(int wrl_database_id) {
		this.wrl_db_id = wrl_database_id;
	}

	public int getWrl_transactionTypes() {
		return wrl_tr_types;
	}

	public void setWrl_transactionTypes(int wrl_type) {
		this.wrl_tr_types = wrl_type;
	}

	public Map<Integer, ArrayList<Transaction>> getWrl_transactionMap() {
		return wrl_tr_map;
	}

	public void setWrl_transactionMap(Map<Integer, ArrayList<Transaction>> wrl_transactionMap) {
		this.wrl_tr_map = wrl_transactionMap;
	}

	public int[] getWrl_transactionProportions() {
		return wrl_tr_prop;
	}

	public void setWrl_transactionProp(int[] wrl_transactionProp) {
		this.wrl_tr_prop = wrl_transactionProp;
	}

	public int getWrl_tr_red() {
		return wrl_tr_red;
	}

	public void setWrl_tr_red(int wrl_tr_red) {
		this.wrl_tr_red = wrl_tr_red;
	}

	public int getWrl_tr_green() {
		return wrl_tr_green;
	}

	public void setWrl_tr_green(int wrl_tr_green) {
		this.wrl_tr_green = wrl_tr_green;
	}

	public int getWrl_tr_orange() {
		return wrl_tr_orange;
	}

	public void setWrl_tr_orange(int wrl_tr_orange) {
		this.wrl_tr_orange = wrl_tr_orange;
	}

	public void incWrl_transactionProportions(int pos) {
		++this.getWrl_transactionProportions()[pos];
	}
	
	public void incWrl_transactionProportions(int pos, int val) {
		int value = this.getWrl_transactionProportions()[pos];
		value += val;
		this.getWrl_transactionProportions()[pos] = value;
	}
	
	public void decWrl_transactionProportions(int pos) {		
		--this.getWrl_transactionProportions()[pos];		
	}
	
	public void decWrl_transactionProportions(int pos, int val) {
		int value = this.getWrl_transactionProportions()[pos];
		value -= val;
		this.getWrl_transactionProportions()[pos] = value;
	}

	public int getWrl_transactionBorning() {
		return wrl_tr_borning;
	}

	public void setWrl_transactionBorning(int wrl_transactionBorning) {
		this.wrl_tr_borning = wrl_transactionBorning;
	}

	public int getWrl_transactionDying() {
		return wrl_tr_dying;
	}

	public void setWrl_transactionDying(int wrl_transactionDying) {
		this.wrl_tr_dying = wrl_transactionDying;
	}

	public double getWrl_transactionBirthRate() {
		return wrl_tr_birth_rate;
	}

	public void setWrl_transactionBirthRate(double wrl_transactionBirthRate) {
		this.wrl_tr_birth_rate = wrl_transactionBirthRate;
	}

	public double getWrl_transactionDeathRate() {
		return wrl_tr_death_rate;
	}

	public void setWrl_transactionDeathRate(double wrl_transactionDeathRate) {
		this.wrl_tr_death_rate = wrl_transactionDeathRate;
	}

	public int[] getWrl_transactionBirthProp() {
		return wrl_tr_birth_prop;
	}

	public void setWrl_transactionBirthProp(int[] wrl_transactionBirthProp) {
		this.wrl_tr_birth_prop = wrl_transactionBirthProp;
	}

	public int[] getWrl_transactionDeathProportions() {
		return wrl_tr_death_prop;
	}

	public void setWrl_transactionDeathProp(int[] wrl_transactionDeathProp) {
		this.wrl_tr_death_prop = wrl_transactionDeathProp;
	}

	public int getWrl_totalTransactions() {
		return wrl_total_tr;
	}

	public void setWrl_totalTransaction(int wrl_totalTransaction) {
		this.wrl_total_tr = wrl_totalTransaction;
	}
	
	public int getWrl_initTotalTransactions() {
		return wrl_init_total_tr;
	}

	public void setWrl_initTotalTransactions(int wrl_initTotalTransactions) {
		this.wrl_init_total_tr = wrl_initTotalTransactions;
	}

	public void incWrl_totalTransaction() {
		int totalTransaction = this.getWrl_totalTransactions();		
		++totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}
	
	public void decWrl_totalTransactions() {
		int totalTransaction = this.getWrl_totalTransactions();		
		--totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}

	public int getWrl_totalDataObjects() {
		return wrl_total_data;
	}

	public void setWrl_totalDataObjects(int wrl_totalData) {
		this.wrl_total_data = wrl_totalData;
	}

	public Map<Integer, Set<Integer>> getWrl_dataInvolvedInTransactions() {
		return wrl_dataTransactionsInvolved;
	}

	public void setWrl_dataTransactionsInvolved(
			Map<Integer, Set<Integer>> wrl_dataTransactionsInvolved) {
		this.wrl_dataTransactionsInvolved = wrl_dataTransactionsInvolved;
	}

	public int getWrl_hgr_intraNodeDataMovements() {
		return wrl_hg_intra_dmv;
	}

	public void setWrl_hg_intraNodeDataMovements(int wrl_intraNodeDataMovements) {
		this.wrl_hg_intra_dmv = wrl_intraNodeDataMovements;
	}

	public int getWrl_hgr_interNodeDataMovements() {
		return wrl_hg_inter_dmv;
	}

	public void setWrl_hg_interNodeDataMovements(int wrl_interNodeDataMovements) {
		this.wrl_hg_inter_dmv = wrl_interNodeDataMovements;
	}

	
	public Map<Integer, Integer> getWrl_dataId_shadowId_map() {
		return wrl_dataId_shadowId_map;
	}

	public void setWrl_dataId_shadowId_map(
			Map<Integer, Integer> wrl_dataId_shadowId_map) {
		this.wrl_dataId_shadowId_map = wrl_dataId_shadowId_map;
	}

	public Map<Integer, Integer> getWrl_hg_dataId_clusterId_map() {
		return wrl_hg_dataId_clusterId_map;
	}

	public void setWrl_hg_dataId_clusterId_map(
			Map<Integer, Integer> wrl_dataId_clusterId_map) {
		this.wrl_hg_dataId_clusterId_map = wrl_dataId_clusterId_map;
	}

	public Map<Integer, Integer> getWrl_gr_dataId_clusterId_map() {
		return wrl_gr_dataId_clusterId_map;
	}

	public void setWrl_gr_dataId_clusterId_map(
			Map<Integer, Integer> wrl_gr_dataId_clusterId_map) {
		this.wrl_gr_dataId_clusterId_map = wrl_gr_dataId_clusterId_map;
	}
	
	public String getWrl_hGraphWorkloadFile() {
		return this.wrl_hg_workload_file;
	}

	public void setWrl_hGraphWorkloadFile(String wrl_workload_file) {
		this.wrl_hg_workload_file = wrl_workload_file;
	}

	public String getWrl_hGraphFixFile() {
		return this.wrl_hg_fix_file;
	}

	public void setWrl_hGraphFixFile(String wrl_fixfile) {
		this.wrl_hg_fix_file = wrl_fixfile;
	}
	
	public String getWrl_graphWorkloadFile() {
		return wrl_gr_workload_file;
	}

	public void setWrl_graphWorkloadFile(String wrl_graph_workload_file) {
		this.wrl_gr_workload_file = wrl_graph_workload_file;
	}

	public double getWrl_meanDTI() {
		return wrl_mean_dti;
	}

	public void setWrl_meanDTI(double wrl_dti) {
		this.wrl_mean_dti = wrl_dti;
	}
	
	public int getWrl_distributedTransactions() {
		return wrl_dt_nums;
	}

	public void setWrl_distributedTransactions(int wrl_dt_nums) {
		this.wrl_dt_nums = wrl_dt_nums;
	}

	public int[] getWrl_dt_nums_typewise() {
		return wrl_dt_nums_typewise;
	}

	public void setWrl_dt_nums_typewise(int[] wrl_dt_nums_typewise) {
		this.wrl_dt_nums_typewise = wrl_dt_nums_typewise;
	}

	public double getWrl_percentageDistributedTransactions() {
		return wrl_percentage_dt;
	}

	public void setWrl_percentageDistributedTransactions(double wrl_percentage_dt) {
		this.wrl_percentage_dt = wrl_percentage_dt;
	}

	public double getWrl_hg_percentageIntraNodeDataMovements() {
		return wrl_hg_percentage_intra_dmv;
	}

	public void setWrl_hg_percentageIntraNodeDataMovements(double wrl_percentage_pdmv) {
		this.wrl_hg_percentage_intra_dmv = wrl_percentage_pdmv;
	}
		
	public double getWrl_hg_percentageInterNodeDataMovements() {
		return wrl_hg_percentage_inter_dmv;
	}

	public void setWrl_hg_percentageInterNodeDataMovements(double wrl_percentage_ndmv) {
		this.wrl_hg_percentage_inter_dmv = wrl_percentage_ndmv;
	}
	
	// Compressed Hypergraph

	public Map<Integer, Integer> getWrl_chg_dataId_clusterId_map() {
		return wrl_chg_dataId_clusterId_map;
	}

	public void setWrl_chg_dataId_clusterId_map(
			Map<Integer, Integer> wrl_chg_dataId_clusterId_map) {
		this.wrl_chg_dataId_clusterId_map = wrl_chg_dataId_clusterId_map;
	}

	public Map<Integer, Integer> getWrl_chg_virtualDataId_clusterId_map() {
		return wrl_chg_virtualDataId_clusterId_map;
	}

	public void setWrl_chg_virtualDataId_clusterId_map(
			Map<Integer, Integer> wrl_chg_dataId_virtualDataId_map) {
		this.wrl_chg_virtualDataId_clusterId_map = wrl_chg_dataId_virtualDataId_map;
	}

	public String getWrl_chGraphWorkloadFile() {
		return wrl_chg_workload_file;
	}

	public void setWrl_chGraphWorkloadFile(String wrl_chg_workload_file) {
		this.wrl_chg_workload_file = wrl_chg_workload_file;
	}

	public String getWrl_chGraphFixFile() {
		return wrl_chg_fix_file;
	}

	public void setWrl_chGraphFixFile(String wrl_chg_fix_file) {
		this.wrl_chg_fix_file = wrl_chg_fix_file;
	}

	public double getWrl_chg_percentageIntraNodeDataMovements() {
		return wrl_chg_percentage_intra_dmv;
	}

	public void setWrl_chg_percentageIntraNodeDataMovements(double wrl_chg_percentage_pdmv) {
		this.wrl_chg_percentage_intra_dmv = wrl_chg_percentage_pdmv;
	}

	public double getWrl_chg_percentageInterNodeDataMovements() {
		return wrl_chg_percentage_inter_dmv;
	}

	public void setWrl_chg_percentageInterNodeDataMovements(double wrl_chg_percentage_ndmv) {
		this.wrl_chg_percentage_inter_dmv = wrl_chg_percentage_ndmv;
	}

	public int getWrl_chg_intraNodeDataMovements() {
		return wrl_chg_intra_dmv;
	}

	public void setWrl_chg_intraNodeDataMovements(int wrl_chg_intraNodeDataMovements) {
		this.wrl_chg_intra_dmv = wrl_chg_intraNodeDataMovements;
	}

	public int getWrl_chg_interNodeDataMovements() {
		return wrl_chg_inter_dmv;
	}

	public void setWrl_chg_interNodeDataMovements(int wrl_chg_interNodeDataMovements) {
		this.wrl_chg_inter_dmv = wrl_chg_interNodeDataMovements;
	}

	// Graph
	public String getWrl_gr_workload_file() {
		return wrl_gr_workload_file;
	}

	public void setWrl_gr_workload_file(String wrl_gr_workload_file) {
		this.wrl_gr_workload_file = wrl_gr_workload_file;
	}

	public double getWrl_gr_percentageIntraNodeDataMovements() {
		return wrl_gr_percentage_intra_dmv;
	}

	public void setWrl_gr_percentageIntraNodeDataMovements(double wrl_gr_percentage_pdmv) {
		this.wrl_gr_percentage_intra_dmv = wrl_gr_percentage_pdmv;
	}

	public double getWrl_gr_percentageInterNodeDataMovements() {
		return wrl_gr_percentage_inter_dmv;
	}

	public void setWrl_gr_percentageInterNodeDataMovements(double wrl_gr_percentage_ndmv) {
		this.wrl_gr_percentage_inter_dmv = wrl_gr_percentage_ndmv;
	}

	public int getWrl_gr_intraNodeDataMovements() {
		return wrl_gr_intra_dmv;
	}

	public void setWrl_gr_intraNodeDataMovements(int wrl_gr_intraNodeDataMovements) {
		this.wrl_gr_intra_dmv = wrl_gr_intraNodeDataMovements;
	}

	public int getWrl_gr_interNodeDataMovements() {
		return wrl_gr_inter_dmv;
	}

	public void setWrl_gr_interNodeDataMovements(int wrl_gr_interNodeDataMovements) {
		this.wrl_gr_inter_dmv = wrl_gr_interNodeDataMovements;
	}

	public boolean isWrl_hasDataMoved() {
		return wrl_has_dmv;
	}

	public void setWrl_hasDataMoved(boolean wrl_hasDataMoved) {
		this.wrl_has_dmv = wrl_hasDataMoved;
	}
	
	public String getWrl_data_movement_strategy() {
		return wrl_dmv_strategy;
	}

	public void setWrl_data_movement_strategy(String wrl_data_movement_strategy) {
		this.wrl_dmv_strategy = wrl_data_movement_strategy;
	}
	
	// Calculate the percentage of Data movements within the Workload (after running Strategy-Base/1/2)
	public void hg_CalculateIntraNodeDataMovementPercentage(int intra_node_movements) {
		double percentage = ((double)intra_node_movements/this.getWrl_totalDataObjects())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_hg_percentageIntraNodeDataMovements(percentage);
	}
	
	public void hg_CalculateInterNodeDataMovementPercentage(int inter_node_movements) {
		int counts = this.getWrl_totalDataObjects();		
		double percentage = ((double)inter_node_movements/counts)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_hg_percentageInterNodeDataMovements(percentage);
	}
	
	// For Compressed Hypergraph
	public void chg_CalculateIntraNodeDataMovementPercentage(int intra_node_movements) {
		double percentage = ((double)intra_node_movements/this.getWrl_totalDataObjects())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_chg_percentageIntraNodeDataMovements(percentage);
	}
	
	public void chg_CalculateInterNodeDataMovementPercentage(int inter_node_movements) {
		int counts = this.getWrl_totalDataObjects();		
		double percentage = ((double)inter_node_movements/counts)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_chg_percentageInterNodeDataMovements(percentage);
	}
	
	// Calculate the percentage of Data movements within the Workload (after running Strategy-Base/1/2)
	public void gr_CalculateIntraNodeDataMovementPercentage(int intra_node_movements) {
		double percentage = ((double)intra_node_movements/this.getWrl_totalDataObjects())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_gr_percentageIntraNodeDataMovements(percentage);
	}
	
	public void gr_CalculateInterNodeDataMovementPercentage(int inter_node_movements) {
		int counts = this.getWrl_totalDataObjects();		
		double percentage = ((double)inter_node_movements/counts)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_gr_percentageInterNodeDataMovements(percentage);
	}	

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}	
	
	public HashMap<Integer, ArrayList<Integer>> getDb_operations() {
		return db_operations;
	}

	public void setDb_operations(HashMap<Integer, ArrayList<Integer>> db_insert) {
		this.db_operations = db_insert;
	}

	public int getDb_operation_count() {
		return db_operation_count;
	}

	public void setDb_operation_count(int insert_count) {
		this.db_operation_count = insert_count;
	}
	
	public void incDb_operation_count() {
		int count = this.getDb_operation_count();
		this.setDb_operation_count(++count);
	}
	
	// Preserves the Database operations (insert and delete) due to workload generation
	public void preserveDbOperations(int operation, int table_id, int data_id) {		
		ArrayList<Integer> op_param_list = new ArrayList<Integer>();		
		op_param_list.add(operation);
		op_param_list.add(table_id);
		op_param_list.add(data_id);
		this.incDb_operation_count();
		//System.out.println("@ "+this.db_operation_count+"|"+operation+"|"+table_id+"|"+data_id);
		this.getDb_operations().put(this.getDb_operation_count(), op_param_list);
	}
	
	//Re-applies Database operations (insert and delete) due to Workload generation 
	public void reapplyDbOperations(Database db){
		int insert = 0;
		int delete = 0;
		
		for(Entry<Integer, ArrayList<Integer>> entry : this.getDb_operations().entrySet()) {
			int operation = entry.getValue().get(0);
			int table_id = entry.getValue().get(1);
			int data_id = entry.getValue().get(2);
			//System.out.println(">> "+entry.getKey()+"|"+operation+"|"+table_id+"|"+data_id);
			
			switch(operation) {
				case 1:
					db.insertData(table_id, data_id);
					++insert;
					break;
					
				case -1:
					db.deleteData(table_id, data_id);					
					++delete;
					break;
			}
		}
		
		System.out.println("[OUT] Total "+insert+" data rows have been inserted to adopt the workload originated changes");
		System.out.println("[OUT] Total "+delete+" data rows have been deleted to adopt the workload originated changes.");
	}	
	
	// Workload initialisation after sampling
	public void initialise(Database db) {
		this.setWrl_dt_nums_typewise(new int[this.getWrl_transactionTypes()]);
		this.setWrl_dataTransactionsInvolved(new TreeMap<Integer, Set<Integer>>());
		Set<Integer> dataSet = new TreeSet<Integer>();
		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {			
			for(Transaction transaction : entry.getValue()) {							
				for(Integer data_id : transaction.getTr_dataSet()) {
					if(!dataSet.contains(data_id)) {
						dataSet.add(data_id);
						
						Set<Integer> transactionSet = new TreeSet<Integer>();
						transactionSet.add(transaction.getTr_id());
						
						this.getWrl_dataInvolvedInTransactions().put(data_id, transactionSet);
					} else {
						this.getWrl_dataInvolvedInTransactions().get(data_id).add(transaction.getTr_id());
					}					
				}
			}
		}
		
		this.calculateDTandDTI(db);
	}
	
	// Workload Sampling
	public Workload performSampling(Database db) {
		Workload sampled_workload = new Workload(this);
		int removed_count = 0;
		
		Map<Integer, Set<Integer>> removable_transaction_map = new TreeMap<Integer, Set<Integer>>();		
		for(Entry<Integer, ArrayList<Transaction>> entry : sampled_workload.getWrl_transactionMap().entrySet()) {		
			Set<Integer> removed_transactions = new TreeSet<Integer>();
			for(Transaction transaction : entry.getValue()) {
				for(Transaction tr : entry.getValue()) {
					if(transaction.getTr_id() != tr.getTr_id()) {						
						if(transaction.getTr_dataSet().equals(tr.getTr_dataSet()) 
								&& tr.getTr_frequency() == 1) {
							
							transaction.incTr_frequency();
							removed_transactions.add(tr.getTr_id());				
							++removed_count;
						}
					}
				}
			} // end -- for()-Transaction
			
			removable_transaction_map.put(entry.getKey(), removed_transactions);
		} // end -- for()-Transaction Types
		
		// i -- Transaction types
		for(int i = 0; i < sampled_workload.getWrl_transactionTypes(); i++)
			sampled_workload.removeTransactions(removable_transaction_map.get(i), i, false);						
		
		System.out.println("[MSG] Total "+removed_count+" duplicate transactions have been removed from the workload.");
		return sampled_workload;
	}	
	
	
	
	// Search and return a target Transaction from the Workload
	public Transaction getTransaction(int transaction_id) {		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {			
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_id() == transaction_id)
					return transaction;
			}
		}
		
		return null;
	}
	
	// Remove a set of transactions from the Workload
	public void removeTransactions(Set<Integer> removed_transactions, int i, boolean flag) {
		HashMap<Integer, TreeSet<Integer>> _dataSetMap = new HashMap<Integer, TreeSet<Integer>>();
		
		for(int tr_id : removed_transactions) {
			Transaction transaction = this.getTransaction(tr_id);
			
			Set<Integer> dataSet = new TreeSet<Integer>();
			dataSet = transaction.getTr_dataSet();
			_dataSetMap.put(tr_id, (TreeSet<Integer>) dataSet);						
			
			if(flag) {
				this.releaseTransactionData(tr_id);				
			}
			
			this.getWrl_transactionMap().get(i).remove(transaction); // Removing Object
			
			this.decWrl_totalTransactions();				
			this.decWrl_transactionProportions(i);		
		}
	}
	
	// Only works for Transaction Classification which executes after workload sampling
	public void releaseTransactionData(int tr_id) {
		Transaction transaction = this.getTransaction(tr_id);
		//System.out.println("@ Removing T"+transaction.getTr_id());
		for(Integer data_id : transaction.getTr_dataSet()) {
			//System.out.println("@ Removing T"+transaction.getTr_id()+" | d"+data.getData_id());
			this.getWrl_dataInvolvedInTransactions().get(data_id).remove(tr_id);
		}
	}
	
	// Returns a list of transaction ids which contain the searched data id (Only at the time of new Transaction generation)
	public ArrayList<Integer> getTransactionListForSearchedData(int data_id) {
		ArrayList<Integer> transactionList = new ArrayList<Integer>();
		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_dataSet().contains(data_id))
					transactionList.add(transaction.getTr_id());
			}
		}
		
		return transactionList;
	}
	
	// Remove a data id from the given list of transactions (Only at the time of new Transaction generation)
	public void removeDataFromTransactions(int data_id, ArrayList<Integer> transactionList) {		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_dataSet().contains(data_id)) {
					transaction.getTr_dataSet().remove(data_id);
					//System.out.println("@ d"+data_id+" has been removed from T"+transaction.getTr_id());					
				}
			}
		}
	}	
	
	private void incDTbyTypes(int i) {		
		int val = this.getWrl_dt_nums_typewise()[i];
		this.getWrl_dt_nums_typewise()[i] = ++val;
	}

	// Calculate the percentage of Distributed Transactions within the Workload (before and after the Data movements)
	public void calculateDTandDTI(Database db) {
		this.setWrl_dt_nums_typewise(new int[this.getWrl_transactionTypes()]);
		int dt_nums = 0;
		int dti_sum = 0;		
		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				transaction.calculateDTCost(db);
				
				if(transaction.getTr_dtCost() > 0) {
					++dt_nums;
					
					transaction.calculateDTImapct();
					dti_sum += transaction.getTr_dtImpact();
					
					int i = transaction.getTr_type();
					this.incDTbyTypes(i);
				}
			} // end -- for()-Transaction		
		} // end -- for()-
		
		this.setWrl_distributedTransactions(dt_nums);
		
		double percentage = ((double)dt_nums/(double)this.getWrl_totalTransactions())*100.0;
		double mean_dti = ((double)dti_sum/(double)this.getWrl_distributedTransactions());
		
		this.setWrl_percentageDistributedTransactions(percentage);
		this.setWrl_meanDTI(mean_dti);
	}
		
	public void printWrl_transactionProp(int[] array) {
		int size = array.length;
		
		System.out.print("{");
		for(int val : array) {			
			System.out.print(Integer.toString(val));
			
			--size;			
			if(size != 0)
				System.out.print(", ");
		}		
		System.out.print("}");		
	}
	
	public void show(Database db, String type) {				
		System.out.println("[OUT] Workload details for simulation round "+this.getWrl_id());
		System.out.print("      "+this.toString() +" having a distribution of ");				
		this.printWrl_transactionProp(this.getWrl_transactionProportions());
				
		System.out.println("\n      -----------------------------------------------------------------------------------------------------------------");
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				System.out.print("     ");
				transaction.show(db);
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types						
				
		System.out.println("      -----------------------------------------------------------------------------------------------------------------");							
		System.out.println("      # Total Distributed Transactions: "+this.getWrl_distributedTransactions()+" ("+this.getWrl_percentageDistributedTransactions()+"%)");
		System.out.println("      # Mean Impact: "+this.getWrl_meanDTI());
		
		switch(type) {
		case "hgr":
			if(this.isWrl_hasDataMoved()) {
				System.out.println("      # Intra-Node Data Movements: "+this.getWrl_hgr_intraNodeDataMovements());
				System.out.println("      # Inter-Node Data Movements: "+this.getWrl_hgr_interNodeDataMovements());
			}		
			break;
	
		case "chg":
			if(this.isWrl_hasDataMoved()) {
				System.out.println("      # Intra-Node Data Movements: "+this.getWrl_chg_intraNodeDataMovements());
				System.out.println("      # Inter-Node Data Movements: "+this.getWrl_chg_interNodeDataMovements());
			}		
			break;
			
		case "gr":
			if(this.isWrl_hasDataMoved()) {
				System.out.println("      # Intra-Node Data Movements: "+this.getWrl_gr_intraNodeDataMovements());
				System.out.println("      # Inter-Node Data Movements: "+this.getWrl_gr_interNodeDataMovements());
			}		
			break;
		
		default:
			break;
		}
	}
	
	@Override
	public String toString() {	
		return (this.wrl_label+" ["+this.getWrl_totalTransactions()+" transactions (containing "+this.getWrl_totalDataObjects()
				+" unique data) ");//+this.getWrl_transactionTypes());//+" types having a distribution of ");//+this.printWrl_transactionProp()+"]");
	}

	@Override
	public int compareTo(Workload workload) {
		int compare = ((int)this.wrl_id < (int)workload.wrl_id) ? -1: ((int)this.wrl_id > (int)workload.wrl_id) ? 1:0;
		return compare;
	}
}