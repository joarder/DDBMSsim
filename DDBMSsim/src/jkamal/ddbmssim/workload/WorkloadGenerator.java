/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.DatabaseServer;
import jkamal.ddbmssim.main.DBMSSimulator;
import org.apache.commons.lang3.StringUtils;

public class WorkloadGenerator {	
	private Map<Integer, Workload> workload_map;
	private Map<Integer, Workload> sampled_workload_map;
	
	public WorkloadGenerator() {
		this.setWorkload_map(new TreeMap<Integer, Workload>());
		this.setSampled_workload_map(new TreeMap<Integer, Workload>());	
	}	
	
	public Map<Integer, Workload> getWorkload_map() {
		return workload_map;
	}

	public void setWorkload_map(Map<Integer, Workload> workload_map) {
		this.workload_map = workload_map;
	}
	
	public Map<Integer, Workload> getSampled_workload_map() {
		return sampled_workload_map;
	}

	public void setSampled_workload_map(Map<Integer, Workload> workload_map) {
		this.sampled_workload_map = workload_map;
	}
	
	// Workload Initialisation
	public Workload workloadInitialisation(Database db, String workload_name, int workload_id) {
		// Workload Details : http://oltpbenchmark.com/wiki/index.php?title=Workloads
		switch(workload_name) {
			case "AuctionMark":
				return(new Workload(workload_id, 10, db.getDb_id()));			
			case "Epinions":
				return(new Workload(workload_id, 9, db.getDb_id()));
			case "SEATS":
				return(new Workload(workload_id, 6, db.getDb_id()));
			case "TPC-C":
				return(new Workload(workload_id, 5, db.getDb_id()));
			case "YCSB":
				return(new Workload(workload_id, 6, db.getDb_id()));
			}		
		
		return null;
	}
	
	// Generates Workloads for the entire simulation
	public void generateWorkloads(DatabaseServer dbs, Database db, int simulation_run_numbers) throws IOException {
		Workload workload = null;
		int workload_id = 0;
		
		while(workload_id != simulation_run_numbers) {
			System.out.println("--------------------------------------------------------------------------");
			System.out.println("[ACT] Starting workload generation for simulation round "+workload_id+"...");
			if(workload_id != 0) {
				workload = new Workload(this.getWorkload_map().get(workload_id -1));
				workload.setWrl_id(workload_id);
				workload.setWrl_label("W"+workload_id);
				
				System.out.println("[MSG] Total "+workload.getWrl_totalTransactions()+" transactions are present in the workload.");
				
				// Setting Death Rate
				workload.setWrl_transactionDying((int) ((int) workload.getWrl_totalTransactions() * 0.5));				
				workload.setWrl_transactionDeathRate(0.5); // fixed rate

				// Setting Birth Rate
				workload.setWrl_transactionBorning((int) ((int) workload.getWrl_totalTransactions() * 0.5));
				workload.setWrl_transactionBirthRate(0.5); // fixed rate
				
				// === Death Management === 						
				workload.setWrl_transactionDeathProp(transactionPropGen(workload.getWrl_transactionTypes(), 
						workload.getWrl_transactionDying()));
				
				// Reducing Old Workload Transactions			
				TransactionReducer transactionReducer = new TransactionReducer();
				int old_tr = transactionReducer.reduceTransaction(db, workload);
				
				System.out.println("[ACT] Varying current workload by reducing "+old_tr+" old transactions ...");
				this.print(workload);

				// === Birth Management ===								
				workload.setWrl_transactionBirthProp(transactionPropGen(workload.getWrl_transactionTypes(), 
						workload.getWrl_transactionBorning()));
				
				// Generating New Workload Transactions						
				TransactionGenerator transactionGenerator = new TransactionGenerator();
				int new_tr = transactionGenerator.generateTransaction(db, workload, DBMSSimulator.getGlobal_tr_id());	
				
				System.out.println("[ACT] Varying current workload by generating "+new_tr+" new transactions ...");
				this.print(workload);				
				
				workload.reInitialise(db);
			} else {
				// === Workload Generation Round 0 ===
				workload = this.workloadInitialisation(db, DBMSSimulator.WORKLOAD_TYPE, workload_id);				
				workload.setWrl_initTotalTransactions(DBMSSimulator.TRANSACTIONS);				
				workload.setWrl_transactionProp(transactionPropGen(workload.getWrl_transactionTypes(), 
						DBMSSimulator.TRANSACTIONS));
				
				// Generating New Workload Transactions						
				TransactionGenerator transactionGenerator = new TransactionGenerator();
				transactionGenerator.generateTransaction(db, workload, DBMSSimulator.getGlobal_tr_id());
				workload.reInitialise(db);
			}						
				
			System.out.println("[OUT] Initially "+workload.getWrl_totalTransactions()+" transactions have been " +
					"gathered for the target workload of simulation round "+workload_id);
			
			this.print(workload);
			
			//workload.show(db, "");
			
			// Clone the Workload
			Workload cloned_workload = new Workload(workload);
			this.getWorkload_map().put(workload_id, cloned_workload);

			++workload_id;
		}
	}
	
	// Workload Sampling
	public Workload workloadSampling(Database db, Workload workload) {
		Workload sampled_workload = new Workload(workload);
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
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++)
			sampled_workload.removeTransactions(db, sampled_workload.getWrl_transactionMap().get(i), removable_transaction_map.get(i), i);						
		
		System.out.println("[MSG] Total "+removed_count+" duplicate transactions have been removed from the workload.");
		
		sampled_workload.refresh(db);
		
		return sampled_workload;
	}
	
	// Generates Transaction Proportions based on the Zipfian Ranking
	public int[] transactionPropGen(int ranks, int elements) {		
		int proportionArray[] = new int[ranks];
		int rankArray[] = zipfLawDistributionGeneration(ranks, elements);
		
		// TR Rankings {T1, T2, T3, T4, T5} = {5, 4, 1, 2, 3}; 1 = Higher, 5 = Lower
		int begin = 0;
		int end = (rankArray.length - 1);
		for(int i = 0; i < proportionArray.length; i++) {
			if(i < 2) {
				proportionArray[i] = rankArray[end];
				-- end;
			} else {
				proportionArray[i] = rankArray[begin];
				++ begin;
			}			
			//System.out.println("@debug >> TR-"+(i+1)+" | Counts = "+propArray[i]);
		}
		
		return proportionArray;
	}	
	
	// Generates Zipfian Ranking for Transactions
	public int[] zipfLawDistributionGeneration(int ranks, int elements) {
		double prop[] = new double[ranks];
		int finalProp[] = new int[ranks];
		
		double sum = 0.0d;
		for(int rank = 0; rank < ranks; rank++) {
			prop[rank] = elements / (rank+1); // exponent value is always 1
			sum += prop[rank];
		}
			
		double amplification = elements/sum;		
		int finalSum = 0;
		for(int rank = 0; rank < ranks; rank++) {
			finalProp[rank] = (int) (prop[rank] * amplification);
			finalSum += finalProp[rank];
		}				
		
		finalProp[0] += (elements - finalSum); // Adjusting the difference by adding it to the highest rank proportion
		finalSum += (elements - finalSum);
		
		return finalProp;
	}
	
	// Assigns Shadow HMetis Data Id for Hypergraph partitioning
	public void assignShadowDataId(Database db, Workload workload) {
		// Cleanup
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.search(data_id);
					
					if(data.isData_hasShadowId()) {					
						data.setData_shadowId(-1);
						data.setData_hasShadowId(false);
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
		
		
		int shadow_id = 1;		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.search(data_id);
					
					if(!data.isData_hasShadowId()) {
						workload.getWrl_dataId_shadowId_map().put(data.getData_id(), shadow_id);
						
						data.setData_shadowId(shadow_id);
						data.setData_hasShadowId(true);								
						++shadow_id;					
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
		
		workload.setWrl_totalDataObjects(shadow_id - 1);
	}
	
	public void generateWorkloadFile(Database db, Workload workload, String partitioner) throws IOException {
		switch(partitioner) {
		case "hgr":
			this.generateHGraphWorkloadFile(db, workload);
			this.generateHGraphFixFile(db, workload);
			break;
			
		case "chg":
			this.generateCHGraphWorkloadFile(db, workload);
			this.generateCHGraphFixFile(db, workload);
			break;
			
		case "gr":
			this.generateGraphWorkloadFile(db, workload);
			break;
		}
	}
	
	// Generates Workload File for Hypergraph partitioning
	private void generateHGraphWorkloadFile(Database db, Workload workload) {
		File workloadFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_hGraphWorkloadFile());
		
		Data trData = null;
		int hyper_edges = workload.getWrl_totalTransactions();		
		int vertices = workload.getWrl_totalDataObjects();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;						
		
		try {
			workloadFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(hyper_edges+" "+vertices+" "+hasTransactionWeight+""+hasDataWeight+"\n");
				//System.out.println(hyper_edges+" "+vertices+" "+hasTransactionWeight+""+hasDataWeight);
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {
							writer.write(transaction.getTr_frequency()+" ");
							//System.out.println("@@ "+transaction.getTr_frequency()+"* ");
							Iterator<Integer> data =  transaction.getTr_dataSet().iterator();
							while(data.hasNext()) {
								trData = db.search(data.next());
								
								writer.write(Integer.toString(trData.getData_shadowId()));							
								
								if(data.hasNext())
									writer.write(" "); 
							} // end -- while() loop
							
							writer.write("\n");						
						} // end -- if()-Transaction Class
					} // end -- for()-Transaction
				} // end -- for()-Transaction-Types

				// Writing Data Weight
				Set<Integer> uniqueDataSet = new TreeSet<Integer>();
				int newline = 0;
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {
							
							Iterator<Integer> data =  transaction.getTr_dataSet().iterator();							
							
							while(data.hasNext()) {
								trData = db.search(data.next());								
								
								if(!uniqueDataSet.contains(trData.getData_shadowId())) {
									++newline;
									
									writer.write(Integer.toString(trData.getData_weight()));	
									//System.out.println("@@ "+trData.getData_weight());
									//writer.write(Integer.toString(1));
									
									if(newline != vertices)
										writer.write("\n");										
									
									uniqueDataSet.add(trData.getData_shadowId());
								}
							}							
						}
					}
				}
				
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}										
	}
	
	// Generates Fix Files (Determines whether a Data is movable from its current Partition or not) 
	// for Hypergraph partitioning
	private void generateHGraphFixFile(Database db, Workload workload) {
		File fixFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_hGraphFixFile());
		
		Data trData = null;
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				Set<Integer> uniqueDataSet = new TreeSet<Integer>();
				int newline = 0;
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {							
							Iterator<Integer> data =  transaction.getTr_dataSet().iterator();							
							
							while(data.hasNext()) {
								trData = db.search(data.next());								
								
								if(!uniqueDataSet.contains(trData.getData_shadowId())) {
									++newline;
									
									if(trData.isData_isMoveable())									
										writer.write(Integer.toString(trData.getData_partitionId()));
									else
										writer.write(Integer.toString(-1));
									
									if(newline != workload.getWrl_totalDataObjects())
										writer.write("\n");										
									
									uniqueDataSet.add(trData.getData_shadowId());
								}
							}
						}
					}					
				}
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}		
	}
	
	// Generates Workload File for Graph partitioning
	private void generateGraphWorkloadFile(Database db, Workload workload) throws IOException {
		File workloadFile = new File(DBMSSimulator.METIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_graphWorkloadFile());
		
		Data trData = null;
		Data trInvolvedData = null;
		Set<Integer> dataIdSet = null;
		Set<Integer> dataSet = new TreeSet<Integer>();
		String content = "";
		int edges = 0; // Total number of edges need to be determined
		int vertices = workload.getWrl_totalDataObjects();		
		int hasVertexWeight = 1;
		int hasEdgeWeight = 1;
		int new_line = vertices;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_class() != "green") {
					
					Iterator<Integer> data_id_itr =  transaction.getTr_dataSet().iterator();
					while(data_id_itr.hasNext()) {
						trData = db.search(data_id_itr.next());
											
						if(!dataSet.contains(trData.getData_id())) {
							dataSet.add(trData.getData_id());
							String str = Integer.toString(trData.getData_weight())+" ";
							//String str = Integer.toString(trData.getData_shadowId())+"-";
							//String str = "";
							dataIdSet = new TreeSet<Integer>();
							
							if(trData.getData_transactions_involved().size() != 0) {					
								for(Integer transaction_id : trData.getData_transactions_involved()) {
									Transaction tr = workload.getTransaction(transaction_id);
									
									if(tr != null) {
										for(int trInvolvedDataId : tr.getTr_dataSet()) {
											trInvolvedData = db.search(trInvolvedDataId);													
											
											if(!dataIdSet.contains(trInvolvedDataId) && trInvolvedData.getData_id() != trData.getData_id()) {
												str += Integer.toString(trInvolvedData.getData_shadowId())+" ";							
												str += tr.getTr_weight()+" ";
												
												++edges;
												
												dataIdSet.add(trInvolvedData.getData_id());
											}
										}
									}
								}															
							}
							
							content += StringUtils.stripEnd(str, null);
							
							--new_line;
							
							if(new_line != 0)
								content += "\n";
						}																
					} // end -- while() loop																				
				} // end -- if()-Transaction Class
			} // end -- for()-Transaction
		} // end -- for()-Transaction-Types
		
		
		try {
			workloadFile.createNewFile();
			Writer writer = null;			

			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(vertices+" "+(edges/2)+" "+hasVertexWeight+""+hasEdgeWeight+"\n"+content);
				//writer.write(vertices+" "+(edges/2)+"\n"+content);
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}
	}
	
	private int simpleHash(int x, int nums) {
		System.out.println("@debug >> x = "+x+" nums = "+nums);
		return (x % nums);
	} 
	
	// Generates Workload File for (Compressed) Hypergraph partitioning
	private void generateCHGraphWorkloadFile(Database db, Workload workload) {
		Map<Integer, Set<Integer>> virtual_vertexMap = new TreeMap<Integer, Set<Integer>>();
		Map<Integer, Integer> virtual_vertexWeightMap = new TreeMap<Integer, Integer>();
		Set<Integer> virtual_vertex_dataSet = null;		
		int virtual_vertex_id = 0;
		
		Data trData = null;
		Set<Integer> dataSet = new TreeSet<Integer>();
		Set<Integer> virtual_dataSet = new TreeSet<Integer>();		
		
		System.out.println("@debug >> Red = "+workload.getWrl_tr_red()+" | Orange = "+workload.getWrl_tr_orange()
				+" | div = "+workload.getWrl_totalDataObjects()/2);

		// Creating Virtual Nodes
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_class() != "green") {
					Iterator<Integer> data_id_itr =  transaction.getTr_dataSet().iterator();
					while(data_id_itr.hasNext()) {												
						trData = db.search(data_id_itr.next());
						
						if(!dataSet.contains(trData.getData_id())) {
							dataSet.add(trData.getData_id());
							
							//String pk_id = trData.getData_pk().substring(trData.getData_pk().length() - 1);
							int i = simpleHash(trData.getData_pk(), (workload.getWrl_totalDataObjects()/2));							
							//System.out.println("@ "+trData.toString()+" | hashed at ("+i+")");
							
							if(!virtual_dataSet.contains(i)) {
								virtual_dataSet.add(i);
								
								//virtual_vertex_id = i;
								
								virtual_vertex_dataSet = new TreeSet<Integer>();								
								virtual_vertex_dataSet.add(trData.getData_id());								
								virtual_vertexMap.put(i, virtual_vertex_dataSet);
								virtual_vertexWeightMap.put(i, trData.getData_weight());
								
								trData.setData_virtual_node_id(i);
							} else {																
								virtual_vertexMap.get(i).add(trData.getData_id());
								
								int weight = virtual_vertexWeightMap.get(i) + trData.getData_weight();
								virtual_vertexWeightMap.remove(i);
								virtual_vertexWeightMap.put(i, weight);
								
								trData.setData_virtual_node_id(i);
							}							
							
							System.out.println("@debug >> "+trData.toString()+" | hashed at ("+i+") | vid = "+trData.getData_virtual_node_id());
						}
					}
				}
			}
		}
		
		//Check -- Found OK
		for(Entry<Integer, Set<Integer>> entry : virtual_vertexMap.entrySet()) {
			System.out.print(" vid = "+entry.getKey()+" {");
			
			for(Integer j : entry.getValue()) {
				System.out.print(j+", ");
			}
			
			System.out.print("}\n");
		}
		//Check -- Found OK
		for(Entry<Integer, Integer> entry : virtual_vertexWeightMap.entrySet())
			System.out.println(" vid = "+entry.getKey()+" | weight = "+entry.getValue());
		
		// Creating Virtual Edges
		Map<Integer, Set<Integer>> virtual_edgeMap = new TreeMap<Integer, Set<Integer>>();
		Map<Integer, Integer> virtual_edgeWeightMap = new TreeMap<Integer, Integer>();
		Set<Integer> virtual_edge_nodeSet = null;
		//Set<Integer> dataSet2 = new TreeSet<Integer>();
		Set<Integer> edgeSet = new TreeSet<Integer>();
		int virtual_edge_id = 1;
		boolean virtual_edge_weight = false;
		Transaction tr = null;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_class() != "green") {					
					Iterator<Integer> data_id_itr2 =  transaction.getTr_dataSet().iterator();
					while(data_id_itr2.hasNext()) {												
						trData = db.search(data_id_itr2.next());						
						
						//if(!dataSet2.contains(trData.getData_id())) {
							//dataSet2.add(trData.getData_id());		
							//System.out.println("@debug >> "+trData.toString()+" | "+trData.getData_virtual_node_id());
							
							virtual_vertex_id = trData.getData_virtual_node_id();
							
							// Virtual Edge - Vertices
							if(!edgeSet.contains(virtual_edge_id)) {								
								edgeSet.add(virtual_edge_id);
								
								virtual_edge_nodeSet = new TreeSet<Integer>();
								virtual_edge_nodeSet.add(virtual_vertex_id);
								virtual_edgeMap.put(virtual_edge_id, virtual_edge_nodeSet);
								//System.out.println("@debug >> v-eid= "+virtual_edge_id+" | v-vid = "+virtual_vertex_id);
							} else {
								virtual_edgeMap.get(virtual_edge_id).add(virtual_vertex_id);
								//System.out.println("@debug >> v-eid= "+virtual_edge_id+" | v-vid = "+virtual_vertex_id);
							}
														
							// Virtual Edge Weight
							int combined_weight = 0;
							for(Integer involved_tr_id : trData.getData_transactions_involved()) {
								tr = workload.getTransaction(involved_tr_id);
								//combined_weight += tr.getTr_weight();
								combined_weight += tr.getTr_frequency();
								//System.out.println("@debug >> "+tr.toString()+" | combined_weight = "+combined_weight);
							}
						
							//Check
							//System.out.println("%% Map Show %%");
							//for(Entry<Integer, Integer> entry1 : virtual_edgeWeightMap.entrySet())
								//System.out.println(" eid = "+entry1.getKey()+" | weight == "+entry1.getValue());
							
							//System.out.println("%%");
							
							if(virtual_edge_weight) {								
								//System.out.println("@debug >> Before v-eid = "+virtual_edge_id);
								int weight = virtual_edgeWeightMap.get(virtual_edge_id);
								weight += combined_weight;
								virtual_edgeWeightMap.remove(virtual_edge_id);
								virtual_edgeWeightMap.put(virtual_edge_id, weight);
								//System.out.println("@debug >> After v-eid = "+virtual_edge_id+" | w = "+weight);
							} else {
								virtual_edgeWeightMap.put(virtual_edge_id, combined_weight);
								//System.out.println("@debug >> *** v-eid = "+virtual_edge_id+" | cw = "+combined_weight);
								virtual_edge_weight = true;
							}
						//}
					}
				}
				
				//System.out.println("*** >> "+virtual_edge_id);
				if(virtual_edgeMap.get(virtual_edge_id) != null) {
					if(virtual_edgeMap.get(virtual_edge_id).size() == 1) {
						edgeSet.remove(virtual_edge_id);
						virtual_edgeMap.remove(virtual_edge_id);
						virtual_edgeWeightMap.remove(virtual_edge_id);
						virtual_edge_weight = false;
						//System.out.println("*** >> Removed v-eid = "+virtual_edge_id);
					}
				} else {
					++virtual_edge_id;
					virtual_edge_weight = false;
				}
			}
		}			
		
		//Check -- Found OK
		for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
			System.out.print(" eid = "+entry.getKey()+"{");
			
			for(Integer j : entry.getValue()) {
				System.out.print(j+", ");
			}
			
			System.out.print("}\n");
		}
		//Check -- Found OK
		for(Entry<Integer, Integer> entry : virtual_edgeWeightMap.entrySet())
			System.out.println(" eid = "+entry.getKey()+" | weight = "+entry.getValue());
				
		
		// Creating Compressed Hypergraph File
		File workloadFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_chGraphWorkloadFile());
				
		int compressed_hyper_edges = virtual_edgeMap.size();
		int compressed_vertices = virtual_vertexMap.size();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;						
		
		try {
			workloadFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(compressed_hyper_edges+" "+compressed_vertices+" "+hasTransactionWeight+""+hasDataWeight+"\n");
				
				for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
					writer.write(virtual_edgeWeightMap.get(entry.getKey())+" ");
							
					Iterator<Integer> virtual_id_itr =  entry.getValue().iterator();
					while(virtual_id_itr.hasNext()) {						
						writer.write(Integer.toString(virtual_id_itr.next()));							
						
						if(virtual_id_itr.hasNext())
							writer.write(" "); 
					}
					
					writer.write("\n");		
				}

				// Writing Data Weight
				int newline = 0;
				
				for(Entry<Integer, Integer> entry : virtual_vertexWeightMap.entrySet()) {
						writer.write(Integer.toString(entry.getValue()));																			
						
						if(newline != (virtual_vertexWeightMap.size()-1))
							writer.write("\n");
						
						++newline;
				}
				
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}										
	}
	
	private void generateCHGraphFixFile(Database db, Workload workload) {
		File fixFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_chGraphFixFile());
		
		Data trData = null;
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				Set<Integer> uniqueDataSet = new TreeSet<Integer>();
				int newline = 0;
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {							
							Iterator<Integer> data =  transaction.getTr_dataSet().iterator();							
							
							while(data.hasNext()) {
								trData = db.search(data.next());								
								
								if(!uniqueDataSet.contains(trData.getData_shadowId())) {
									++newline;
									
									if(trData.isData_isMoveable())									
										writer.write(Integer.toString(trData.getData_partitionId()));
									else
										writer.write(Integer.toString(-1));
									
									if(newline != workload.getWrl_totalDataObjects())
										writer.write("\n");										
									
									uniqueDataSet.add(trData.getData_shadowId());
								}
							}
						}
					}					
				}
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}		
	}
	
	// Printing the Workload contents
	private void print(Workload workload) {
		System.out.print("[MSG] Total "+workload.getWrl_totalTransactions()+" transactions of "
				+workload.getWrl_transactionTypes()+" types having a distribution of ");
		
		workload.printWrl_transactionProp(workload.getWrl_transactionProportions());
		
		System.out.println(" are currently in the workload.");
	}	
}
