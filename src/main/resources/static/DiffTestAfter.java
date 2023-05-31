public class DiffTestBefore {

	public double maxdiv(int a, int b) {
		if(a == 0 || b == 0) {
			return a > 0 || b > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;			
		}
		
		double x = (double) a;
		
		return a > b ? x/b : b/x;
	}
}
