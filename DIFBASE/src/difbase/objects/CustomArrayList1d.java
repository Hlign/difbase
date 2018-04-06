package difbase.objects;
import java.util.ArrayList;
import java.util.Collection;

public class CustomArrayList1d extends ArrayList<String>{

	private static final long serialVersionUID = 1275029724018208705L;

	//heritage de la classe ArrayList permettant d'avoir la clause Values d'un insert juste avec un toString
	@Override
	public String toString() {
		String result = "( '";
		for(String element : this){
			result+= element.replaceAll("'", "''")+"', '";
		}		
		return result.substring(0, result.length()-3)+")"; 
	}

	public CustomArrayList1d() {
		super();
		// TODO Auto-generated constructor stub
	}

	public CustomArrayList1d(Collection<? extends String> c) {
		super(c);
		// TODO Auto-generated constructor stub
	}

	public CustomArrayList1d(int initialCapacity) {
		super(initialCapacity);
		// TODO Auto-generated constructor stub
	}
	
}
