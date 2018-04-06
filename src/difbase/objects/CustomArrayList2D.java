package difbase.objects;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.stream.Collectors;

import difbase.main.Main;

public class CustomArrayList2D {

	private ArrayList<ArrayList<String>> data;
	private ArrayList<Integer> primaryKey;
	private String SqlType;
	private Difftype name;


	public CustomArrayList2D() {

	}
	public CustomArrayList2D(ArrayList<ArrayList<String>> data, ArrayList<Integer> primaryKey, String sqlType, Difftype name) {
		super();
		this.data = data;
		this.primaryKey = primaryKey;
		this.SqlType = sqlType;
		this.setName(name);
	}
	public ArrayList<ArrayList<String>> getData() {
		return data;
	}
	public void setData(ArrayList<ArrayList<String>> data) {
		this.data = data;
	}
	public ArrayList<Integer> getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(ArrayList<Integer> primaryKey) {
		this.primaryKey = primaryKey;
	}
	public String getSqlType() {
		return SqlType;
	}
	public void setSqlType(String sqlType) {
		SqlType = sqlType;
	}
	public void WriteSqlFile() throws IOException {
		switch (name) {
		case REQ_ONLY_IN_T1:
			this.WriteInsertFileFromDev2Prod();
			break;
		case REQ_ONLY_IN_T2:
			this.WriteInsertFileFromProd2Dev();
			break;
		case LINES_ONLY_IN_T1:
			this.WriteInsertFileFromDev2Prod();
			break;
		case LINES_ONLY_IN_T2:
			this.WriteInsertFileFromProd2Dev();
			break;
		case LINES_2_UPDATE:
			this.WriteUpdateFile();
			break;
		}
	}
	private void WriteInsertFileFromDev2Prod() throws IOException{
		String FieldNames = Main.columnNames.toString().replace('[', '(').replace(']', ')');
		ArrayList<String> lines2Insert = new ArrayList<>();
		ArrayList<String> lines2Revert = new ArrayList<>();
		ArrayList<ArrayList<String>> allLines =  new ArrayList<>();
		if(this.data.get(0).size()==1){
			for(ArrayList<String> row : this.data){
				for(ArrayList<String> grouped: Main.t1.get(row.get(0))){
					ArrayList<String> tmp = new ArrayList<>();
					for(int index : Main.grpBy){
						tmp.add(grouped.get(index));
					}
					allLines.add(tmp);
				}
			}
		}else{
			allLines = this.data;
		}
		for(ArrayList<String> line : allLines){
			String where = " WHERE 1=1 ";
			for(int i = 0; i<line.size() ; i++){
				where+= " AND "+ Main.columnNames.get(Main.grpBy.get(i))+" = '"+line.get(i).replace("'", "''")+"'";
			}
			CustomArrayList1d stringifier = new CustomArrayList1d(Main.t1.get(line.get(0))
					.stream().filter(x -> x.containsAll(line))
					.collect(Collectors.toList()).get(0));
			lines2Revert.add("DELETE FROM "+ Main.nomTable + where + ";");
			lines2Insert.add("INSERT INTO "+ Main.nomTable + " " + FieldNames + " VALUES " + stringifier.toString() +";");
		}
		WriteFileFromArray(lines2Insert,"Insert.sql");
		WriteFileFromArray(lines2Revert, "Revert.sql");
	}
	private void WriteInsertFileFromProd2Dev() throws IOException{
		String FieldNames = Main.columnNames.toString().replace('[', '(').replace(']', ')');
		ArrayList<String> lines2Insert = new ArrayList<>();
		ArrayList<String> lines2Revert = new ArrayList<>();
		ArrayList<ArrayList<String>> allLines =  new ArrayList<>();
		if(this.data.get(0).size()==1){
			for(ArrayList<String> row : this.data){
				for(ArrayList<String> grouped: Main.t2.get(row.get(0))){
					ArrayList<String> tmp = new ArrayList<>();
					for(int index : Main.grpBy){
						tmp.add(grouped.get(index));
					}
					allLines.add(tmp);
				}
			}
		}else{
			allLines = this.data;
		}
		for(ArrayList<String> line : allLines){
			String where = " WHERE 1=1 ";
			for(int i = 0; i<line.size() ; i++){
				where+= " AND "+ Main.columnNames.get(Main.grpBy.get(i))+" = '"+line.get(i).replace("'", "''")+"'";
			}
			CustomArrayList1d stringifier = new CustomArrayList1d(Main.t2.get(line.get(0))
					.stream().filter(x -> x.containsAll(line))
					.collect(Collectors.toList()).get(0));
			lines2Revert.add("DELETE FROM "+ Main.nomTable + where + ";");
			lines2Insert.add("INSERT INTO "+ Main.nomTable + " " + FieldNames + " VALUES " + stringifier.toString()+";");
		}
		WriteFileFromArray(lines2Insert,"Insert.sql");
		WriteFileFromArray(lines2Revert, "Revert.sql");
	}
	private void WriteUpdateFile() throws IOException{
		ArrayList<String> update2Dev = new ArrayList<>();
		ArrayList<String> update2Prod = new ArrayList<>();
		for(ArrayList<String> row : this.data){
			String where = "WHERE 1 = 1 ";
			String requestD2P = "UPDATE "+ Main.nomTable+" SET ";
			String requestP2D = "UPDATE "+ Main.nomTable+" SET ";
			ArrayList<String> rowP2D = new  ArrayList<>();
			ArrayList<String> rowD2P = new ArrayList<>();
			for(int i= 1; i< Main.columnNames.size()+1; i++){
				rowD2P.add(row.get(i));
				rowP2D.add(row.get(i + Main.columnNames.size()+1));
			}
			for(int i = 0; i<Main.columnNames.size(); i++){
				if(!rowD2P.get(i).equals(rowP2D.get(i)) && !Main.grpBy.contains(i)){
					requestD2P += Main.columnNames.get(i)+" = '"+ rowD2P.get(i).replace("'", "''")+"', ";
					requestP2D += Main.columnNames.get(i)+" = '"+ rowP2D.get(i).replace("'", "''")+"', ";
				}
				else if(Main.grpBy.contains(i)){
					where += " AND "+ Main.columnNames.get(i)+" = '"+ rowD2P.get(i).replace("'", "''")+"'";
				}
			}
			requestD2P = requestD2P.substring(0, requestD2P.length()-2)+" "+where+";";
			requestP2D = requestP2D.substring(0, requestP2D.length()-2)+" "+where+";";
			update2Dev.add(requestP2D);
			update2Prod.add(requestD2P);
		}
		WriteFileFromArray(update2Dev,"_prod_vers_dev.sql");
		WriteFileFromArray(update2Prod, "_dev_vers_prod.sql");
	}
	private void WriteFileFromArray(ArrayList<String> data, String suffixe) throws IOException{
		Main.CreateDirectory(Main.SQLLocation+Main.nomTable);
		String FileName = Main.SQLLocation+Main.nomTable+"\\"+Main.nomTable+"_"+name.name()+"_"+suffixe;
		File file = new File(FileName);
		//file.mkdirs();
		FileWriter writer = new FileWriter(file);
		PrintWriter printer = new PrintWriter(writer);
		for(String line : data){
			printer.println(line);
		}	
		printer.close();
		writer.close();	    
	}

	public Difftype getName() {
		return name;
	}

	public void setName(Difftype name) {
		this.name = name;
	}

}
