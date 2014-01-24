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
		TransactionClassifier workloadClassifier = new TransactionClassifier();
		int workload_id = 0;
		
		while(workload_id != simulation_run_numbers) {
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
				
				this.refreshWorkload(db, workload);
			} else {
				// === Workload Generation Round 0 ===
				workload = this.workloadInitialisation(db, DBMSSimulator.WORKLOAD_TYPE, workload_id);				
				workload.setWrl_initTotalTransactions(DBMSSimulator.TRANSACTIONS);				
				workload.setWrl_transactionProp(transactionPropGen(workload.getWrl_transactionTypes(), 
						DBMSSimulator.TRANSACTIONS));
				
				// Generating New Workload Transactions						
				TransactionGenerator transactionGenerator = new TransactionGenerator();
				transactionGenerator.generateTransaction(db, workload, DBMSSimulator.getGlobal_tr_id());
				
				this.refreshWorkload(db, workload);
			}						
				
			System.out.println("[OUT] Initially "+workload.getWrl_totalTransactions()+" transactions have been " +
					"gathered for the target workload of simulation round "+workload_id);
			
			// Clone the Workload
			//Workload cloned_workload = new Workload(workload);
			//this.getWorkload_map().put(workload_id, cloned_workload);
			
			// Workload Sampling
			System.out.println("[ACT] Starting workload sampling to remove duplicate transactions ...");
			Workload sampled_workload = this.workloadSampling(db, workload);			
			
			// Classify the Workload Transactions based on whether they are Distributed or not (Red/Orange/Green List)
			workloadClassifier.classifyTransactions(db, workload);
			
			// Workload Sampling
			Workload sampled_worklaod = this.workloadSampling(db, workload);
			
			// Assign Shadow HMetis Data Id and generate workload and fix files
			this.assignShadowDataId(db, workload);	
			
			this.generateHGraphWorkloadFile(db, workload);
			this.generateHGraphFixFile(db, workload);
			
			this.generateCHGraphWorkloadFile(db, workload);
			this.generateCHGraphFixFile(db, workload);
			
			this.generateGraphWorkloadFile(db, workload);
			
			workload.show(db, "");
			
			// Clone the Sampled Workload
			//Workload cloned_sampled_workload = new Workload(sampled_workload);
			//this.getSampled_workload_map().put(workload_id, cloned_sampled_workload);	
			
			Workload cloned_workload = new Workload(sampled_workload);
			this.getWorkload_map().put(workload_id, cloned_workload);

			++workload_id;
		}
	}
	
	// Workload Sampling
	private Workload workloadSampling(Database db, Workload workload) {
		int removed_count = 0;
		Map<Integer, Set<Integer>> removable_transaction_map = new TreeMap<Integer, Set<Integer>>();		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {		
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
			workload.removeTransactions(db, workload.getWrl_transactionMap().get(i), removable_transaction_map.get(i), i);						
		
		System.out.println("[MSG] Total "+removed_count+" duplicate transactions have been removed from the workload.");
		
		return sampled_workload;
	}
	
	// Refresh Workload Transactions and Data
	public void refreshWorkload(Database db, Workload workload) {
		Map<Integer, Integer> dataFrequencyTracker = new TreeMap<Integer, Integer>();		
		Map<Integer, Set<Integer>> dataInvolvedTransactionsTracker = new TreeMap<Integer, Set<Integer>>();
		Set<Integer> involvedTransactions = null;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				transaction.setTr_frequency(1); // resetting Transaction frequency				
				transaction.calculateTr_weight();
				transaction.generateTransactionCost(db);								
				
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.search(data_id);
					
					// Remove already removed Transaction Ids from Data-Transaction involved List
					Set<Integer> toBeRemovedTransactionSet = new TreeSet<Integer>();
					for(Integer tr : data.getData_transactions_involved()) {
						
						if(workload.getTransaction(tr) == null)
							toBeRemovedTransactionSet.add(tr);
					}						
					
					Iterator<Integer> iterator = toBeRemovedTransactionSet.iterator();
					while(iterator.hasNext()) {
						int toBeRemovedTransaction = iterator.next();
						data.getData_transactions_involved().remove((Object)toBeRemovedTransaction); // removing object					
					}
					
					// Refresh Data Frequency and recalculate Weight
					if(!dataFrequencyTracker.containsKey(data.getData_id())) {
						data.setData_frequency(1);
						data.calculateData_weight();
						
						dataFrequencyTracker.put(data.getData_id(), data.getData_frequency());
					} else {
						data.incData_frequency(dataFrequencyTracker.get(data.getData_id()));
						data.calculateData_weight();
						
						dataFrequencyTracker.remove(data.getData_id());
						dataFrequencyTracker.put(data.getData_id(), data.getData_frequency());
					}
					
					// 
					if(!dataInvolvedTransactionsTracker.containsKey(data.getData_id())) {
						involvedTransactions = new TreeSet<Integer>();
						involvedTransactions.add(transaction.getTr_id());
						dataInvolvedTransactionsTracker.put(data.getData_id(), involvedTransactions);
					} else {
						dataInvolvedTransactionsTracker.get(data.getData_id()).add(transaction.getTr_id());
					}
				}
			}
		}
		
		// Refresh the whole Workload with the updated Data frequency
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.search(data_id);
					
					data.setData_frequency(dataFrequencyTracker.get(data.getData_id()));
					data.calculateData_weight();
					
					Set<Integer> dataTransactions = new TreeSet<Integer>();
					dataTransactions = dataInvolvedTransactionsTracker.get(data.getData_id());
					
					data.setData_transactions_involved(dataTransactions);
				}
			}
		}
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
	
	// Generates Workload File for Hypergraph partitioning
	public void generateHGraphWorkloadFile(Database db, Workload workload) {
		File workloadFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+workload.getWrl_hGraphWorkloadFile());
		
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
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {
							writer.write(transaction.getTr_weight()+" ");
							
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
	public void generateHGraphFixFile(Database db, Workload workload) {
		File fixFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+workload.getWrl_hGraphFixFile());
		
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
	public void generateGraphWorkloadFile(Database db, Workload workload) throws IOException {
		File workloadFile = new File(DBMSSimulator.METIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+workload.getWrl_graphWorkloadFile());
		
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
		return (x % nums);
	} 
	
	// Generates Workload File for (Compressed) Hypergraph partitioning
	public void generateCHGraphWorkloadFile(Database db, Workload workload) {
		Map<Integer, Set<Integer>> virtual_vertexMap = new TreeMap<Integer, Set<Integer>>();
		Map<Integer, Integer> virtual_vertexWeightMap = new TreeMap<Integer, Integer>();
		Set<Integer> virtual_vertex_dataSet = null;		
		int virtual_vertex_id = 0;
		
		Data trData = null;
		Set<Integer> dataSet = new TreeSet<Integer>();
		Set<Integer> virtual_dataSet = new TreeSet<Integer>();		
		
		//System.out.println("@debug >> Red = "+workload.getWrl_tr_red()+" | Orange = "+workload.getWrl_tr_orange()
				//+" | div = "+workload.getWrl_totalDataObjects()/2);

		// Creating Virtual Nodes
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_class() != "green") {
					Iterator<Integer> data_id_itr =  transaction.getTr_dataSet().iterator();
					while(data_id_itr.hasNext()) {												
						trData = db.search(data_id_itr.next());
						
						if(!dataSet.contains(trData.getData_id())) {
							dataSet.add(trData.getData_id());
							
							String pk_id = trData.getData_pk().substring(trData.getData_pk().length() - 1);
							int i = simpleHash(Integer.parseInt(pk_id), (workload.getWrl_totalDataObjects()/2));							
							
							if(!virtual_dataSet.contains(i)) {
								virtual_dataSet.add(i);
								
								++virtual_vertex_id;
								
								virtual_vertex_dataSet = new TreeSet<Integer>();								
								virtual_vertex_dataSet.add(trData.getData_id());								
								virtual_vertexMap.put(virtual_vertex_id, virtual_vertex_dataSet);
								virtual_vertexWeightMap.put(virtual_vertex_id, trData.getData_weight());
							} else {																
								virtual_vertexMap.get(virtual_vertex_id).add(trData.getData_id());
								
								int weight = virtual_vertexWeightMap.get(virtual_vertex_id) + trData.getData_weight();
								virtual_vertexWeightMap.remove(virtual_vertex_id);
								virtual_vertexWeightMap.put(virtual_vertex_id, weight);
							}
							
							trData.setData_virtual_node_id(virtual_vertex_id);
							
							//System.out.println("@debug >> "+trData.toString()+" | hashed at ("+i+") | vid = "+virtual_vertex_id);
						}
					}
				}
			}
		}
		
		//Check -- Found OK
		//for(Entry<Integer, Set<Integer>> entry : virtual_vertexMap.entrySet()) {
			//System.out.print(" vid = "+entry.getKey()+" {");
			
			//for(Integer j : entry.getValue()) {
				//System.out.print(j+", ");
			//}
			
			//System.out.print("}\n");
		//}
		//Check -- Found OK
		//for(Entry<Integer, Integer> entry : virtual_vertexWeightMap.entrySet())
			//System.out.println(" vid = "+entry.getKey()+" | weight = "+entry.getValue());
		
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
								tr = workload.search(involved_tr_id);
								combined_weight += tr.getTr_weight();
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
		//for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
			//System.out.print(" eid = "+entry.getKey()+"{");
			
			//for(Integer j : entry.getValue()) {
				//System.out.print(j+", ");
			//}
			
			//System.out.print("}\n");
		//}
		//Check -- Found OK
		//for(Entry<Integer, Integer> entry : virtual_edgeWeightMap.entrySet())
			//System.out.println(" eid = "+entry.getKey()+" | weight = "+entry.getValue());
				
		
		// Creating Compressed Hypergraph File
		File workloadFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+workload.getWrl_chGraphWorkloadFile());
				
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
	
	public void generateCHGraphFixFile(Database db, Workload workload) {
		File fixFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+workload.getWrl_chGraphFixFile());
		
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
	public void print(Workload workload) {
		System.out.print("[MSG] Total "+workload.getWrl_totalTransactions()+" transactions of "
				+workload.getWrl_transactionTypes()+" types having a distribution of ");
		
		workload.printWrl_transactionProp(workload.getWrl_transactionProportions());
		
		System.out.println(" are currently in the workload.");
	}	
}
