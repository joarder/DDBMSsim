/**
 * source: https://github.com/yungsters/rain-workload-toolkit
 */

package jkamal.ddbmssim.util;

import java.util.Random;

public class Pareto 
{
	private double _alpha = 0.0;
	private double _beta = 0.0;
	private Random _random;

	public Pareto( double alpha, double beta )
	{
		this._alpha = alpha;
		this._beta = beta;
		this._random = new Random();
	}

	public Pareto( double alpha, double beta, Random rng )
	{
		this._alpha = alpha;
		this._beta = beta;
		this._random = rng;
	}

	// Courtesy: http://www.sitmo.com/eq/521 - Generating Pareto distributed random number
	// This seems incorrect since the numbers generated can be less than the minimum (beta)
	public double nextDouble_Old()
	{
		double rndValU = this._random.nextDouble();
		double next = this._beta/( -1 * ( Math.pow( Math.log( rndValU ), 1/this._alpha ) ) );
		return next;
	}

	// Courtesy: http://en.wikipedia.org/wiki/Pareto_distribution
	public double nextDouble()
	{
		double rndValU = this._random.nextDouble();
		double next = this._beta/( Math.pow( rndValU, 1/this._alpha ) );
		return next;
	}

	public static void main(String[] args) 
	{
		double total = 0.0;
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;

		int iterations = 1000;
		Pareto dist = new Pareto( 1, 1000 );
		for( int i = 0; i < iterations; i ++ )
		{
			double val = dist.nextDouble();
			if( val < min )
				min = val;
			if( val > max )
				max = val;
			total += val;
			//System.out.println( val );
		}

		System.out.println( "Avg: " + (total/(double)iterations) );
		System.out.println( "Min: " + min );
		System.out.println( "Max: " + max );
	}
}