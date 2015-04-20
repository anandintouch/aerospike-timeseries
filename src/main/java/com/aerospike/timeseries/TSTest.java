package com.aerospike.timeseries;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;


public class TSTest {
	private AerospikeClient client;
    private static final String TEST_NAMESPACE = "test";
    private static final String TEST_SET = "wcset";
	private static final String host = "127.0.0.1";
    private static final int port = 3000;
    
    boolean skipRecursivePaths = false;

	
    public TSTest(AerospikeClient aInClient)
    {
        client = aInClient;
    }
    
    public void loadRecords(){
       
        try {
        	 deleteAll();
			 createRecords();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }

    
	private void createRecords() throws Exception {
		
		WritePolicy lPolicy = new WritePolicy();
        lPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        /*lPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        lPolicy.generation = 0;*/
        
        for (Map<?, ?> lRecord : Records.RECORDS)
        {
            @SuppressWarnings("unchecked")
            Bin[] lBins = Util.getBins((Map<String, Object>) lRecord);
            Key lKey = getKey(lRecord);
            client.put(lPolicy, lKey, lBins);
        }
        System.out.println("Records inserted !!");
        System.out.println("\n");

	}
	
    private static Key getKey(Map<?, ?> aInRecord) throws AerospikeException
    {
        return new Key(TEST_NAMESPACE, TEST_SET, Value.get(aInRecord.get("ProductId")));
    }
    
    @SuppressWarnings("unchecked")
	public Object readRecord(Policy policy,Key key) throws AerospikeException{
    	Object receivedRecord = null;
    	
		Record record = client.get(null, key);
		 
		if(record !=null){
			receivedRecord = record.bins;
		}
		return receivedRecord;
	
    }
    
    public void updateRecord(String productId,List<Bin> binNameValue) throws AerospikeException {
    	
    	WritePolicy lPolicy = new WritePolicy();
    	client.put(lPolicy, new Key(TEST_NAMESPACE,TEST_SET,productId), binNameValue.toArray(new Bin[binNameValue.size()]));
    	    	
    }
    
    @SuppressWarnings("unchecked")
	public void readAndUpdatePriceData(String productId,Date currentDate,String priceData){
    	Date winBeginDate = null;
    	Object lastBinUpdated = null;
    	List<Bin> finalBinList = new ArrayList<Bin>();
    	WritePolicy lPolicy = new WritePolicy();
    	String valueToCarryOver = null;
    	
    	try {
    		
        	Map<String, Object> result = (Map<String, Object>) readRecord(null,new Key(TEST_NAMESPACE,TEST_SET, productId));
        	System.out.println("Record value before update: "+result.toString());
        	
        	//for(String key : result.keySet()){
        		//System.out.println("Map size -"+result.keySet().size()+" Keys-"+key +" Values-"+result.get(key));
        		
        		if(result.get("Win_Begin_Dt") != null){
        			
        			try {
    					winBeginDate = Util.sdf.parse(result.get("Win_Begin_Dt").toString());
    					
    				} catch (ParseException e) {
    					
    					e.printStackTrace();
    				}
        		}
        		
        		if(result.get("LastBinUpdated") != null ){
        			lastBinUpdated = result.get("LastBinUpdated");
        		}
        		
        	//}

        	if(currentDate.after(winBeginDate) || currentDate.equals(winBeginDate)){
               	String binToBeUpdated = Util.findBinToUpdate(currentDate);
            	
            	// Check if holes(no price update on previous days)
            	if(Util.checkIfHoles(lastBinUpdated, currentDate,binToBeUpdated)){
            		valueToCarryOver = result.get(lastBinUpdated.toString()).toString();
            		List<Bin> refillBinList =Util.findBinToReplenish(currentDate,lastBinUpdated, binToBeUpdated,valueToCarryOver);
            		finalBinList.addAll(refillBinList);
            	}
            	
            	// Get the list of Bins to update
            	finalBinList.addAll(getListOfBinToUpdate(currentDate,winBeginDate,priceData,binToBeUpdated));
            	
            	//Update the record
            	updateRecord(productId, finalBinList);
            	
            	System.out.println("Record Updated successfully !");
            	
            	Map<String, Object> updateResult = (Map<String, Object>) readRecord(null,new Key(TEST_NAMESPACE,TEST_SET, "P1"));
            	System.out.println("Record value after update: "+updateResult.toString());
        	}else if(currentDate.before(winBeginDate)){
        		System.out.println("Product cannot be updated, current date '"+currentDate+"' is before window begin date '"+winBeginDate+"'");
        	}
        	/*else if(currentDate.equals(winBeginDate)) {
        		System.out.println("Product cannot be updated, current date '"+currentDate+"' is equal to window begin date '"+winBeginDate+"'");
        	}*/
        	System.out.println("\n");

    		
    	} catch(AerospikeException aEx) {
    		aEx.printStackTrace();
    	}
    	
    }

    
    public Record searchRecord(String prodId,Date currentDate,int lastNumberOfDays){
    	List<String> binList = new ArrayList<String>();
    	Record record = null;
    	
    	int givenDay = Util.getSpecificDay(currentDate);

    	int counter = 0;
    	int binNum =0;
    	int startLoop =0;
    	
    	 if(lastNumberOfDays <= Util.DAYS_COUNTER) {
    		 if(givenDay>Util.DAYS_COUNTER){
    			 binNum = Util.calculateBinNum(givenDay);
    			 if(lastNumberOfDays > binNum){
        			 startLoop = lastNumberOfDays;
        		 }
    			 
    			 if(binNum > Util.DAYS_COUNTER){
        			 System.out.println("No data found for the given number of days");
        	     }else{
        	    	 startLoop = binNum;
        	    	 for(int i=startLoop; ;i--){
         				counter ++;
         				
         				if(i==0){
         					i = Util.DAYS_COUNTER;
         				}
         				binList.add("Bin"+i);
         				if(counter == lastNumberOfDays){
         	     			break;
         	     		}
         	  		 }
        	     }
    		 }else{
    			 for(int i=givenDay; ;i--){
      				counter ++;
      				if(i==0 && lastNumberOfDays> givenDay){
     					//i = Util.DAYS_COUNTER;
      					break;
     				}
      				binList.add("Bin"+i);
      				if(counter == lastNumberOfDays){
      	     			break;
      	     		}
      	  		 }
    		 }
    		
    	 }else{
         	System.out.println("No data found ");
         }
    	 
         record = client.get(null, new Key(TEST_NAMESPACE,TEST_SET, prodId), binList.toArray(new String[binList.size()]));

		return record;
    }
    
    public Object getProductUpdateDetail(String prodId){
    	
    	Map<String, Object> result = (Map<String, Object>) readRecord(null,new Key(TEST_NAMESPACE,TEST_SET, prodId));
    	Object updateDate = null;
    	String price = null;
    	
 		if(result !=null){
 			if(result.get("Last_Update_Dt") != null){
 				updateDate = result.get("Last_Update_Dt");
 			}
 			
 			if(result.get("LastBinUpdated") != null){
 				price = result.get((result.get("LastBinUpdated"))).toString();
 			}
 			System.out.println("Product detail: Updated Date- "+ updateDate+" Price data-"+price);
 		}

 		
		return prodId;
    	
    }
    
    public static List<Bin> getListOfBinToUpdate(Date currentDate, Date windowBeginDate,String priceData,
    																				String binToBeUpdated){
    	//Find Bin to update 
    	List<Bin>  binList = new ArrayList<Bin>();
    	Bin lBin ;
    	
    	// Add Bin for price update
    	//String bin = Util.findBinToUpdate(currentDate, windowBeginDate);
    	binList.add(new Bin(binToBeUpdated, priceData));
    	
    	//Add bin for window begin date update
    	String winBeginDate = Util.sdf.format(Util.calculateNewWindowBeginDate(currentDate, windowBeginDate));
    	binList.add(new Bin("Win_Begin_Dt", winBeginDate));
    	
    	//Add Last Bin updated
    	binList.add(new Bin("LastBinUpdated",binToBeUpdated));
    	
    	//Add product update date
    	binList.add(new Bin("Last_Update_Dt",Util.sdf.format(currentDate)));
    	
		return binList;
    	
    }

    
    private void deleteAll() throws AerospikeException
    {
        WritePolicy lPolicy = new WritePolicy();
        lPolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
       // lPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
       // lPolicy.generation = 1;
        for (Map<?, ?> lRecord : Records.RECORDS)
        {
            client.delete(lPolicy, getKey(lRecord));
        }
        System.out.println("Records deleted !!");
    }
	
    private void searchAPI(String prodId,Date currentDate,int lastNumberOfDays){
    	Record searchResult = searchRecord(prodId,currentDate,3); 
    	if(searchResult !=null){
	    	Map<String,Object> searchResults = searchResult.bins;
	    	
	        	for(String s : searchResults.keySet()){
	        		System.out.println("Key-"+s+" Value-"+searchResults.get(s));
	        	}
    	}
    }

	public static void main(String[] args) {

        System.out.println("Host connecting to-"+host);
        
        ClientPolicy lPolicy = new ClientPolicy();
        lPolicy.failIfNotConnected = true;

        TSTest wTest = new TSTest(new AerospikeClient(lPolicy, host, port));
       // wTest.loadRecords();
        
        Calendar givenDate = new GregorianCalendar(2015, 2, 4); // Jan starts from 0
        givenDate.setTimeInMillis(givenDate.getTimeInMillis());
    	
    	String priceData = "$40";
    	String prodId = "P1";
    	Date currentDate = new Date(givenDate.getTimeInMillis());
    	
    	long sT = System.currentTimeMillis();
    	wTest.readAndUpdatePriceData(prodId, currentDate, priceData);
    	long eT = System.currentTimeMillis();
		System.out.println("Product Key =" + prodId + " took, time=" + (eT-sT) + "ms");
		
    	wTest.searchAPI(prodId,currentDate,3); 
    	
    	System.out.println("\n");
    	wTest.getProductUpdateDetail("P1");
    	
    	
	}

}
