/**
 * source: https://github.com/yungsters/rain-workload-toolkit
 */

package jkamal.ddbmssim.util;

import java.util.Random;

public class Zipf 
{
	long _lowerBound = 0;
	long _upperBound = 0;
	double _a = 0.0;
	double _r = 0.0;
	private Random _random;

	private boolean _first = true;
	private double _c = 0; // Normalization constant

	public Zipf( double a, double r, long L, long H )
	{
		this._a = a;
		this._r = r;
		this._lowerBound = L;
		this._upperBound = H + 1;
		this._random = new Random();
	}

	public Zipf( double a, double r, long L, long H, Random rng )
	{
		this._a = a;
		this._r = r;
		this._lowerBound = L;
		this._upperBound = H + 1;
		this._random = rng;
	}

	public double nextDouble2()
	{

		// Look at the gap between the upper and lower bound.
		// We will be generating
		long N = this._upperBound - this._lowerBound;

		if( this._first == true )
		{
			for( int i = 1; i <= N; i++ )
				this._c = this._c + (1.0 / Math.pow( (double) i, this._a ) );

			this._c = 1.0 / this._c;
			this._first = false;
		}

		double rndU = 0.0;
		double sumProb = 0.0;

		while( rndU == 0.0 )
			rndU = this._random.nextDouble();

		// Map the random value to cdf
		for( int i = 1; i <= N; i++ )
		{
			sumProb += this._c / Math.pow( (double) i , this._a );
			if( sumProb >= rndU )
				return i + this._lowerBound;
		}

		return 0;
	}

	public double nextDouble()
	{
		double k = -1;
		do 
		{
			// Sample from zipf
			k = this.sampleZipf();
			//System.out.println(k);
		} while (k > this._upperBound);
		//System.out.println(k);

		return Math.abs( (Double.valueOf((k+1)*this._r)).hashCode() ) % (this._upperBound-this._lowerBound) + this._lowerBound;
		//return (k % (this._upperBound - this._lowerBound) ) + this._lowerBound;
	}

	// Courtesy: http://osdir.com/ml/lib.gsl.general/2008-05/msg00057.html
	// and http://cg.scs.carleton.ca/~luc/chapter_ten.pdf
	private double sampleZipf() 
	{
		double b = Math.pow(2, this._a-1);
		double u, v, x, t = 0.0;
		do 
		{
			u = this._random.nextDouble();
			v = this._random.nextDouble();
			x = Math.floor( Math.pow(u,-1.0/(this._a-1.0)));
			t = Math.pow(1.0+1.0/x, this._a-1.0);
		} while ( v*x*(t-1.0)/(b-1.0) > t/b );
		return x;
	}

	/**
	 * @param args
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) 
	{
		double total = 0.0;
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;

		//double a = 1.001;
		//double r = 3.456;

		int iterations = 1000;
		//Zipf dist = new Zipf( 1.001, 3.456, 1, 1000 );
		Zipf dist = new Zipf( 1.001, 3.456, 1, 10000000 );
		//Zipf dist = new Zipf( 1.001, 3.456, 1, 5 );
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

		//System.out.println( "Avg: " + (total/(double)iterations) );
		//System.out.println( "Min: " + min );
		//System.out.println( "Max: " + max );
	}

}