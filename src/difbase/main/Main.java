package difbase.main;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import difbase.objects.CustomArrayList2D;
import difbase.objects.Difftype;

public class Main {

	public static HashMap<String,ArrayList<ArrayList<String>>> t1;
	public static HashMap<String,ArrayList<ArrayList<String>>> t2;
	public static ArrayList<String> columnNames = new ArrayList<>();
	public static String nomTable;
	public static ArrayList<Integer> grpBy;
	public static String TablesLocation;
	public static String CompLocation;
	public static String SQLLocation;

	public static void main(String[] args) throws Exception {
		nomTable = args[0];
		grpBy = new ArrayList<>();
		for(int i= 1; i< args.length ; i++){
			grpBy.add(Integer.parseInt(args[i]));
		}
		String configPath = Thread.currentThread().getContextClassLoader().getResource("").getPath()+"DIFFBASE.properties";
		Properties appProps = new  Properties();
		appProps.load(new FileInputStream(configPath));
		TablesLocation=appProps.getProperty("TablesLocation");
		CompLocation=appProps.getProperty("ComparisonTablesOutputDirectory");
		SQLLocation=appProps.getProperty("SQLOutputDirectory");
		System.out.println("extraction des données issues de la base T1");
		t1 = excelToHashMap(TablesLocation+nomTable+"\\"+nomTable+"tst.xlsx", grpBy.get(0));
		System.out.println("extraction des données issues de la T2");
		t2 = excelToHashMap(TablesLocation+nomTable+"\\"+nomTable+"prd.xlsx", grpBy.get(0));		
		//intersection des deux tables a comparer selon l'index de groupby
		System.out.println("extraction des lignes identiques dans les deux environnements");
		final ArrayList<String> groupsIntersect = (ArrayList<String>) t1.keySet().stream().filter(x->t2.containsKey(x)).collect(Collectors.toList());

		//lignes uniquement dans t1
		//print
		System.out.println("extraction des lignes présentes uniquement en T1");
		final ArrayList<String> onlyInt1 = (ArrayList<String>) t1.keySet().stream().filter(x->!groupsIntersect.contains(x)).collect(Collectors.toList());
		//lignes uniquement dans t2
		//print
		System.out.println("extraction des lignes présentes uniquement en T2");
		final ArrayList<String> onlyInt2 = (ArrayList<String>) t2.keySet().stream().filter(x->!groupsIntersect.contains(x)).collect(Collectors.toList());
		ArrayList<ArrayList<String>> only1 = new ArrayList<>();
		ArrayList<ArrayList<String>> only2 = new ArrayList<>();

		for(String v: onlyInt1){
			ArrayList<String> row = new ArrayList<>();
			row.add(v);
			only1.add(row);
		}
		for(String v: onlyInt2){
			ArrayList<String> row = new ArrayList<>();
			row.add(v);
			only2.add(row);
		}
		ArrayList<ArrayList<String>> t1unliket2 = new ArrayList<>();
		ArrayList<ArrayList<String>> t2unliket1 = new ArrayList<>();
		System.out.println("extraction des lignes qui different d'une table a l'autre");
		for(String key : groupsIntersect){
			t1unliket2.addAll((ArrayList<ArrayList<String>>) t1.get(key).stream().filter(x -> !t2.get(key).contains(x)).collect(Collectors.toList()));
			t2unliket1.addAll((ArrayList<ArrayList<String>>) t2.get(key).stream().filter(x -> !t1.get(key).contains(x)).collect(Collectors.toList()));
		}
		final HashMap<String,ArrayList<ArrayList<String>>> formattedt1unliket2 =twoDimArray2HashMap(t1unliket2, grpBy);		
		final HashMap<String,ArrayList<ArrayList<String>>> formattedt2unliket1 =twoDimArray2HashMap(t2unliket1, grpBy);		

		final ArrayList<String> group2Intersect = (ArrayList<String>) formattedt1unliket2.keySet().stream().filter(x->formattedt2unliket1.containsKey(x)).collect(Collectors.toList());
		//print
		final ArrayList<ArrayList<String>> unlikeButInBoth = new ArrayList<>();
		System.out.println("extraction des lignes présente dans les deux tables ayant des différences");
		for(String key : group2Intersect){
			ArrayList<String> tmp = new ArrayList<>();
			tmp.add("t1");
			tmp.addAll(formattedt1unliket2.get(key).get(0));
			tmp.add("t2");
			tmp.addAll(formattedt2unliket1.get(key).get(0));
			unlikeButInBoth.add(tmp);
		}

		System.out.println("extraction des lignes présente uniquement dans T1 appartenant a une requete présente dans les deux tables");
		ArrayList<String> reqInBothChampInt1 = (ArrayList<String>) formattedt1unliket2.keySet().stream().filter(x->!group2Intersect.contains(x)).collect(Collectors.toList());
		System.out.println("extraction des lignes présente uniquement dans T2 appartenant a une requete présente dans les deux tables");
		ArrayList<String> reqInBothChampInt2 = (ArrayList<String>) formattedt2unliket1.keySet().stream().filter(x->!group2Intersect.contains(x)).collect(Collectors.toList());
		ArrayList<ArrayList<String>> RIBCI1 = new ArrayList<>();
		ArrayList<ArrayList<String>> RIBCI2 = new ArrayList<>();
		System.out.println("Formattage des données extraites");

		for(String val : reqInBothChampInt1){
			ArrayList<String> row = new ArrayList<>();
			for(String v : val.split("!!!")){
				if(!v.equals("a")){
					row.add(v);
				}
			}
			RIBCI1.add(row);
		}
		for(String val : reqInBothChampInt2){
			ArrayList<String> row = new ArrayList<>();
			for(String v : val.split("!!!")){
				if(!v.equals("a")){
					row.add(v);
				}
			}
			RIBCI2.add(row);
		}
		ArrayList<CustomArrayList2D> finaltab = new ArrayList<>();
		finaltab.add(new CustomArrayList2D(only1,grpBy,"Insert", Difftype.REQ_ONLY_IN_T1));
		finaltab.add(new CustomArrayList2D(only2,grpBy,"Insert", Difftype.REQ_ONLY_IN_T2));
		finaltab.add(new CustomArrayList2D(unlikeButInBoth,grpBy,"Update", Difftype.LINES_2_UPDATE));
		finaltab.add(new CustomArrayList2D(RIBCI1,grpBy,"Insert", Difftype.LINES_ONLY_IN_T1));
		finaltab.add(new CustomArrayList2D(RIBCI2,grpBy,"Insert", Difftype.LINES_ONLY_IN_T2));
		System.out.println("Ecriture des requetes SQL permettant d'annuler les différences");
		for (CustomArrayList2D tab : finaltab) {
			if(!tab.getData().isEmpty()){
				tab.WriteSqlFile();
			}
		}
		System.exit(0);
	}

	public static void writeComparison(ArrayList<CustomArrayList2D> finalTab,String nomTable) throws Exception{
		XSSFWorkbook book = new XSSFWorkbook();
		int sheetNum = 0;
		int tabsWithData = 0;
		for(CustomArrayList2D tab : finalTab){
			if(!tab.getData().isEmpty()){
				tabsWithData++;
				book.createSheet(tab.getName().name());
				XSSFSheet sheet = book.getSheetAt(sheetNum++);
				int rowNum = 0;
				for (ArrayList<String> row : tab.getData()){
					Row line = sheet.createRow(rowNum++);
					int cellNum = 0;
					for(String cell : row){
						Cell cel = line.createCell(cellNum++);
						cel.setCellValue(cell);
					}
				}
			}
		}
		if( tabsWithData !=0){
			CreateDirectory(CompLocation);
			FileOutputStream os = new FileOutputStream(CompLocation+"Compa"+nomTable+".xlsx");
			book.write(os);
			book.close();
		}
		else{
			book.close();
			throw new Exception("Les deux tables sont similaires");
		}
	}

	public static HashMap<String,ArrayList<ArrayList<String>>>  excelToHashMap(String path, int groupByIndex) throws IOException{
		File myFile = new File(path);
		FileInputStream fis = new FileInputStream(myFile); 
		XSSFWorkbook myWorkBook = new XSSFWorkbook (fis); // Return first sheet from the XLSX workbook 
		XSSFSheet mySheet = myWorkBook.getSheetAt(0); // Get iterator to all the rows in current sheet 
		Iterator<Row> rowIterator = mySheet.iterator(); // Traversing over each row of XLSX file 
		Row header = rowIterator.next();
		if(columnNames.size()==0){
			for(Cell c : header){
				switch (c.getCellType()) {
				case Cell.CELL_TYPE_STRING:
					columnNames.add(c.getRichStringCellValue()+"");
					break;
				case Cell.CELL_TYPE_NUMERIC:
					columnNames.add(c.getNumericCellValue()+"");
					break;
				case Cell.CELL_TYPE_BOOLEAN:
					columnNames.add(c.getBooleanCellValue()+"");
					break;
				case Cell.CELL_TYPE_BLANK:
					columnNames.add(" ");
					break;
				default :
					columnNames.add(" ");
					break;
				}
			}
		}
		ArrayList<ArrayList<String>> data = new ArrayList<>();
		//mySheet.getLastRowNum();
		while (rowIterator.hasNext()) { 
			Row row = rowIterator.next(); // For each row, iterate through each columns 
			ArrayList<String> dRow = new ArrayList<String>();	
			for (int count = 0; count < columnNames.size(); count++) {
				if(row.getLastCellNum()<columnNames.size()){
				}
				Cell cell = row.getCell(count, Row.RETURN_BLANK_AS_NULL);
				if (cell == null) {
					dRow.add("");
					continue;
				}
				switch (cell.getCellType()) {
				case Cell.CELL_TYPE_STRING:
					dRow.add(cell.getRichStringCellValue()+"");
					break;
				case Cell.CELL_TYPE_NUMERIC:
					dRow.add(cell.getNumericCellValue()+"");
					break;
				case Cell.CELL_TYPE_BOOLEAN:
					dRow.add(cell.getBooleanCellValue()+"");
					break;
				default:
					break;
				}			
			}
			data.add(dRow);
		}
		myWorkBook.close();
		ArrayList<String> groupBy = new ArrayList<>();
		for(ArrayList<String> row : data){
			if (!groupBy.contains(row.get(groupByIndex))){
				groupBy.add(row.get(groupByIndex));
			}
		}
		HashMap<String,ArrayList<ArrayList<String>>> map = new HashMap<>();
		for(String nomreq : groupBy){
			map.put(nomreq, (ArrayList<ArrayList<String>>) data.stream().filter(x -> x.get(groupByIndex).equals(nomreq)).collect(Collectors.toList()));
		}
		return map;
	}

	public static HashMap<String,ArrayList<ArrayList<String>>> twoDimArray2HashMap(ArrayList<ArrayList<String>> data, ArrayList<Integer> grpBy){	
		HashMap<String,ArrayList<ArrayList<String>>> map = new HashMap<>();
		map.put("a", data);
		for(int grpIndex : grpBy){
			HashMap<String,ArrayList<ArrayList<String>>> tmpmap = new HashMap<>();
			ArrayList<String> groupBy = new ArrayList<>();
			for(String key : map.keySet()){
				for(ArrayList<String> row : map.get(key)){
					if (!groupBy.contains(key + "!!!" + row.get(grpIndex))){
						groupBy.add(key + "!!!" + row.get(grpIndex));
					}
				}
				for(String nomreq : groupBy){
					tmpmap.put(nomreq, (ArrayList<ArrayList<String>>) data.stream().filter(x -> x.get(grpIndex).equals(nomreq.split("!!!")[nomreq.split("!!!").length-1])).collect(Collectors.toList()));
				}
			}
			map = tmpmap;
		}
		return map;
	}
	public static void CreateDirectory(String Path){
		File dir = new File(Path);
		if (!dir.exists()){
			dir.mkdirs();
		}
	}
}

