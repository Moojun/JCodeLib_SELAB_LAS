public class DiffTestBefore {
	
	public double maxdiv(int a, int b) {
		if(a == 0 || b == 0) {
			if(a > 0 || b > 0)
				return Double.POSITIVE_INFINITY;
			else
				return Double.NEGATIVE_INFINITY; 
		}
		
		if(a >= b) {
			return (double)a / b;
		} else {
			return (double)b / a;
		}
	}

}
