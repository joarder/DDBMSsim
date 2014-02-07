/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.util;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.main.DBMSSimulator;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;
 
public class PitmanYor {
	private double _d; // 0 <= d < 1
	private double _alpha; // Concentration parameter, alpha > -d
	Map<Integer, ArrayList<Double>> betaMap;
	Map<Integer, ArrayList<Double>> piMap;

	public PitmanYor(double d, double alpha) {
		this.set_d(d);
		this.set_alpha(alpha);
		this.setBetaMap(new TreeMap<Integer, ArrayList<Double>>());
		this.setPiMap(new TreeMap<Integer, ArrayList<Double>>());
	}
	
	public double get_d() {
		return _d;
	}

	public void set_d(double d) {
		this._d = d;
	}

	public double get_alpha() {
		return _alpha;
	}

	public void set_alpha(double alpha) {
		this._alpha = alpha;
	}
	
	public Map<Integer, ArrayList<Double>> getBetaMap() {
		return betaMap;
	}

	public void setBetaMap(Map<Integer, ArrayList<Double>> betaMap) {
		this.betaMap = betaMap;
	}

	public Map<Integer, ArrayList<Double>> getPiMap() {
		return piMap;
	}

	public void setPiMap(Map<Integer, ArrayList<Double>> piMap) {
		this.piMap = piMap;
	}

	private double scale(double x, double a, double b, double min, double max) {
		return ((((b - a) * (x - min))/(max - min)) + a);
	}	
	
	public void generateDataPopularity(int n) {
		
		for(int i = 0; i < DBMSSimulator.PK_ARRAY.length; i++) {
			
		}
						
		// Step-1
		ArrayList<Double> betaList = null;
		RandomDataGenerator rdg = new  RandomDataGenerator();
		//DBMSSimulator.randomDataGenerator.reSeed(0);
		BetaDistribution betaDistribution = new BetaDistribution(0.5, 0.5);
		
		double beta_i = 0.0;
		double d = this.get_d();
		double alpha = this.get_alpha(); 
		
		for(int i = 1; i <= n; i++) {
			betaList = new ArrayList<Double>();

			//beta_i = DBMSSimulator.randomDataGenerator.nextBeta((1 - d), (alpha + i * d)); // beta_i = Beta((1-d), (alpha + i*d))
			beta_i = rdg.nextBeta((1 - d), (alpha + i * d)); // beta_i = Beta((1-d), (alpha + i*d))
			betaList.add(beta_i);
			betaList.add(betaDistribution.cumulativeProbability(((double)i/(double)n)));
			
			this.getBetaMap().put(i, betaList);			
		} 

		// Step-2
		//Map<Integer, ArrayList<Double>> piMap = new TreeMap<Integer, ArrayList<Double>>();
		ArrayList<Double> piList = new ArrayList<Double>();
		double max = Integer.MIN_VALUE;
		double min = Integer.MAX_VALUE;
		
		// for i=1; pi_1 = beta_1 
		piList.add(this.getBetaMap().get(1).get(0));		
		this.getPiMap().put(1, piList);
		
		// for i = 2:n
		for(int i = 2; i <= n; i++) {
			double sum = 0.0;
			piList = new ArrayList<Double>();
			
			for(int l = 1; l <= i-1; l++) {
				sum += (1- this.getBetaMap().get(l).get(0));
			}
			
			double pi_i = this.getBetaMap().get(i).get(0) * sum;
			
			if(pi_i > max)
				max = pi_i;
			
			if(pi_i < min)
				min = pi_i;
			
			piList.add(pi_i);
			this.getPiMap().put(i, piList);
		}		
				
		//System.out.println(">> "+max+" | "+min);
		
		// scale pi_i values between 0 to 1		
		for(Entry<Integer, ArrayList<Double>> entry : this.getPiMap().entrySet()) {
			entry.getValue().add(this.scale(entry.getValue().get(0), 0, 1, min, max));
		}
		
		// Print
		for(Entry<Integer, ArrayList<Double>> entry : this.getBetaMap().entrySet())
			System.out.println(
					entry.getKey()
					//+"|"+entry.getValue().get(0)
					+"|"+entry.getValue().get(1)
					+"|"+piMap.get(entry.getKey()).get(0));
					//+"|"+piMap.get(entry.getKey()).get(1));
				
		System.out.println("--------------------------");
		double rand = 0.0;
		for(int i = 1; i <= n+10; i++) {
			//System.out.println("@ i="+i);
			boolean smallest = false;
			//rand = rdg.nextUniform(0.0, 1.0, true);
			rand = rdg.nextBeta(0.5, 0.5);
			System.out.print(i+"|"+rand);
			
			for(int j = 1; j < this.getBetaMap().size(); j++) {
				//System.out.println("@ j="+j);
				double d_j = this.getBetaMap().get(j).get(1);
				double d_j1 = this.getBetaMap().get(j+1).get(1);
				
				//System.out.println("@ d_j="+d_j+" | d_j1="+d_j1);
				
				if(d_j <= rand && rand < d_j1){					
					int data_i = (int) this.getBetaMap().keySet().toArray()[j-1];
					int data_i1 = (int) this.getBetaMap().keySet().toArray()[j];
					
					if(this.getBetaMap().get(data_i).get(1) > this.getBetaMap().get(data_i1).get(1)) {
						System.out.print("|"+(j)+"\n");
						break;
					} else {
						System.out.print("|"+(j+1)+"\n");
						break;
					}
				} else if(d_j > rand)
					smallest = true;
			}
			
			if(smallest)
				System.out.print("|1"+"\n");
		}
	}
	

	public static void main(String[] args) {
		// py(d, alpha) -- py(1/a, 0.5) where a = 2 and a is the power-law parameter (a > 1)
		int n = 10; // total number of data objects
		PitmanYor py = new PitmanYor(0.5, 0.5); // d = 0.5, alpha = 0.5
		py.generateDataPopularity(n);
	}
}