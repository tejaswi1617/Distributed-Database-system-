import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TransactionHelper
{
    public static LinkedHashMap<String, ArrayList<String>> transactions = new LinkedHashMap<String, ArrayList<String>>();
    public static LinkedHashMap<String, ArrayList<String>> lockedTables = new LinkedHashMap<String,ArrayList<String>>();
    public static String currentTransaction="";

    public static void writeToTransactionFile(String query,String tableName) throws IOException {

        transactions.get(currentTransaction).add(query);
        lockedTables.get(currentTransaction).add(tableName);
        System.out.println(transactions);
    }
    public static boolean setTransaction(String transaction)
    {
        if(transactions.containsKey(transaction)){
            currentTransaction= transaction.trim();
            return true;
        }
        else
        {
            return false;
        }
    }
    public static boolean writeTransaction(String transactionName)
    {
        if(transactions.containsKey(transactionName) == false)
        {
            transactions.put(transactionName.trim(), new ArrayList<String>());
            lockedTables.put(transactionName.trim(), new ArrayList<String>());
            return true;
        }
        System.out.println("Transaction has been already under execution");
        return false;
    }

    public static boolean checkfortablelocked(String tableName)
    {
        for(String key:transactions.keySet()){
            if(key.equals(currentTransaction)==true)
            {
                break;
            }
            if(lockedTables.containsKey(key))
            {
                if(lockedTables.get(key).contains(tableName))
                {
                    return true;
                }
            }

        }
        return false;
    }
    public static void commitTransaction(String transactionName) throws IOException {
        if (transactions.containsKey(transactionName)) {
            ArrayList<String> backuplockedtables = lockedTables.get(transactionName);
            lockedTables.get(transactionName).clear();
            boolean result = false;
            if(transactions.get(transactionName).size()==0)
            {
                result=true;
            }
            for (int i = transactions.get(transactionName).size() - 1; i >= 0; i--) {

                result = Main.executeTransaction(transactions.get(transactionName).get(i));
                System.out.println(transactions.get(transactionName).get(i) + " ------run successfully:" + result);
                if(transactions.get(transactionName).get(i).contains("select"))
                {
                    break;
                }

            }
            if (result == false) {
                for (int i = 0; i < backuplockedtables.size(); i++) {
                    lockedTables.get(transactionName).add(backuplockedtables.get(i));
                    if(transactions.get(transactionName).get(i).contains("select"))
                    {
                        break;
                    }
                }
            } else {

                lockedTables.remove(transactionName);
                transactions.get(transactionName).clear();
                transactions.remove(transactionName);
            }
        }
        else {
                System.out.println("Transaction name is  invalid");
            }
        }

    public static void deleteTransactionFile(String transactionName)
    {
        transactions.get(transactionName).clear();
        lockedTables.get(transactionName).clear();
        transactions.remove(transactionName);
        lockedTables.remove(transactionName);
    }
}
