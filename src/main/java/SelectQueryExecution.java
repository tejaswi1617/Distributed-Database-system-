import java.util.*;

public class SelectQueryExecution {
    private static String dataPath;
    private static String tableName;
    private static String[] columns;
    private static boolean isLocal;
    private void displaySomeColumnswithWhere(HashMap<String, ArrayList<String>> tableMap, SelectQueryParser parse) throws Exception {
        String whereColumn = parse.getWhereColumn();
        readMetaData read = new readMetaData();
        MetaData metaData = read.readmetaData(isLocal).get(tableName);
        String whereColumnDataType = metaData.getColumns().get(whereColumn);
        if (whereColumnDataType.equals("text")) {
            performOperationOnStringForColumnSpecified(tableMap, parse);
        } else {
            performOperationOnIntegerForColumnSpecified(tableMap, parse);
        }
    }

    private void performOperationOnIntegerForColumnSpecified(HashMap<String, ArrayList<String>> tableMap, SelectQueryParser parse) throws Exception {
        ArrayList<String> keyToDisplay = new ArrayList<>();
        String whereValue = parse.getWhereValue();
        columns = parse.getColumns();
        String whereColumn = parse.getWhereColumn();
        String whereOperation = parse.getWhereOperation();
        if (checkMetaData(whereColumn, parse.getTableName()))//check if where column exists
        {
            for (String column : Arrays.asList(columns)) {
                if (tableMap.containsKey(column)) {
                    keyToDisplay.add(column);
                } else {
                    throw new Exception("Check Column Name");
                }
            }
            if (!keyToDisplay.isEmpty()) {
                for (String keys : keyToDisplay) {
                    System.out.print(keys + "\t\t");
                }
                System.out.print("\n");
                try {
                    if (whereOperation.equals("=")) {
                        for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                            if (Float.parseFloat(tableMap.get(whereColumn).get(j)) == Float.parseFloat(whereValue)) {
                                for (int i = 0; i < keyToDisplay.size(); i++) {
                                    System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                                }
                            } else {
                                continue;
                            }
                            System.out.print("\n");
                        }
                    } else if (whereOperation.equals("<")) {
                        for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                            if (Float.parseFloat(tableMap.get(whereColumn).get(j)) < Float.parseFloat(whereValue)) {
                                for (int i = 0; i < keyToDisplay.size(); i++) {
                                    System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                                }
                            } else {
                                continue;
                            }
                            System.out.print("\n");
                        }
                    } else if (whereOperation.equals(">")) {
                        for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                            if (Float.parseFloat(tableMap.get(whereColumn).get(j)) > Float.parseFloat(whereValue)) {
                                for (int i = 0; i < keyToDisplay.size(); i++) {
                                    System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                                }
                            } else {
                                continue;
                            }
                            System.out.print("\n");
                        }
                    } else if (whereOperation.equals(">=")) {
                        for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                            if (Float.parseFloat(tableMap.get(whereColumn).get(j)) >= Float.parseFloat(whereValue)) {
                                for (int i = 0; i < keyToDisplay.size(); i++) {
                                    System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                                }
                            } else {
                                continue;
                            }
                            System.out.print("\n");
                        }
                    } else if (whereOperation.equals("<=")) {
                        for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                            if (Float.parseFloat(tableMap.get(whereColumn).get(j)) <= Float.parseFloat(whereValue)) {
                                for (int i = 0; i < keyToDisplay.size(); i++) {
                                    System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                                }
                            } else {
                                continue;
                            }
                            System.out.print("\n");
                        }
                    } else {
                        throw new Exception("Where condition value specified is wrong.");
                    }
                } catch (Exception e) {
                    System.out.println("Where operation performed on wrong data type.");
                }
            }
        } else {
            throw new Exception("Where column doesnt exist.");
        }
    }

    private void performOperationOnStringForColumnSpecified(HashMap<String, ArrayList<String>> tableMap, SelectQueryParser parse) throws Exception {
        ArrayList<String> keyToDisplay = new ArrayList<>();
        String whereValue = parse.getWhereValue();
        columns = parse.getColumns();
        String whereColumn = parse.getWhereColumn();
        String whereOperation = parse.getWhereOperation();
        if (checkMetaData(whereColumn, parse.getTableName()))//check if where column exists
        {
            for (String column : Arrays.asList(columns)) {
                if (tableMap.containsKey(column)) {
                    keyToDisplay.add(column);
                } else {
                    throw new Exception("Check Column Name");
                }
            }
            if (whereValue.contains("\"")) {
                whereValue = whereValue.replace("\"", "");
            }
            if (!keyToDisplay.isEmpty()) {
                for (String keys : keyToDisplay) {
                    System.out.print(keys + "\t\t");
                }
                System.out.print("\n");
                if (whereOperation.equals("=")) {
                    for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                        if (tableMap.get(whereColumn).get(j).equals(whereValue)) {
                            for (int i = 0; i < keyToDisplay.size(); i++) {
                                System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                            }
                        } else {
                            continue;
                        }
                        System.out.print("\n");
                    }
                } else {
                    System.out.println("Incorrect operation.");
                }
            }
        }
        else {
            throw new Exception("Where column doesnt exist.");
        }
    }

    private void displayAllColumnsWithWhere(HashMap<String, ArrayList<String>> tableMap, SelectQueryParser parse) throws Exception {
        String whereColumn = parse.getWhereColumn();
        readMetaData read = new readMetaData();
        MetaData metaData = read.readmetaData(isLocal).get(tableName);
        if (metaData.getColumns().containsKey(whereColumn)) {
            String whereColumnDataType = metaData.getColumns().get(whereColumn);
            if (whereColumnDataType.equals("text")) {
                performOperationOnString(tableMap, parse);
            } else {
                performOperationOnInteger(tableMap, parse);
            }
        } else {
            throw new Exception("Where column doesnt exist.");
        }
    }

    private void performOperationOnInteger(HashMap<String, ArrayList<String>> tableMap, SelectQueryParser parse) throws Exception {
        ArrayList<String> keyToDisplay = new ArrayList<>();
        String whereColumn = parse.getWhereColumn();
        String whereOperation = parse.getWhereOperation();
        String whereValue = parse.getWhereValue();

        if (checkMetaData(whereColumn, parse.getTableName()))//check if where column exists
        {
            for (String keys : tableMap.keySet()) {
                System.out.print(keys + "\t\t");
            }
            System.out.print("\n");
            for (String keys : tableMap.keySet()) //add columns to display in o/p
            {
                keyToDisplay.add(keys);
            }
            //check(works for string equality
            try
            {
                if (whereOperation.equals("=")) {
                    for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                        if (Float.parseFloat(tableMap.get(whereColumn).get(j)) == Float.parseFloat(whereValue)) {
                            for (int i = 0; i < keyToDisplay.size(); i++) {
                                System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                            }
                        } else {
                            continue;
                        }
                        System.out.print("\n");
                    }
                } else if (whereOperation.equals("<")) {
                    for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                        if (Float.parseFloat(tableMap.get(whereColumn).get(j)) < Float.parseFloat(whereValue)) {
                            for (int i = 0; i < keyToDisplay.size(); i++) {
                                System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                            }
                        } else {
                            continue;
                        }
                        System.out.print("\n");
                    }
                } else if (whereOperation.equals("<=")) {
                    for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                        if (Float.parseFloat(tableMap.get(whereColumn).get(j)) <= Float.parseFloat(whereValue)) {
                            for (int i = 0; i < keyToDisplay.size(); i++) {
                                System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                            }
                        } else {
                            continue;
                        }
                        System.out.print("\n");
                    }
                } else if (whereOperation.equals(">")) {
                    for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                        if (Float.parseFloat(tableMap.get(whereColumn).get(j)) > Float.parseFloat(whereValue)) {
                            for (int i = 0; i < keyToDisplay.size(); i++) {
                                System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                            }
                        } else {
                            continue;
                        }
                        System.out.print("\n");
                    }
                } else if (whereOperation.equals(">=")) {
                    for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                        if (Float.parseFloat(tableMap.get(whereColumn).get(j)) >= Float.parseFloat(whereValue)) {
                            for (int i = 0; i < keyToDisplay.size(); i++) {
                                System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                            }
                        } else {
                            continue;
                        }
                        System.out.print("\n");
                    }
                }else {
                    throw new Exception("Where condition operation specified is wrong.");
                }
            }
            catch (Exception e)
            {
                System.out.println("Where value datatype is wrong.");
            }
        } else {
            throw new Exception("Where column does not exist.");
        }
    }


    private void performOperationOnString(HashMap<String, ArrayList<String>> tableMap, SelectQueryParser parse) {
        ArrayList<String> keyToDisplay = new ArrayList<>();
        String whereColumn = parse.getWhereColumn();
        String whereOperation = parse.getWhereOperation();
        String whereValue = parse.getWhereValue();

        if (checkMetaData(whereColumn, parse.getTableName()))//check if where column exists
        {
            for (String keys : tableMap.keySet()) {
                System.out.print(keys + "\t\t");
            }
            System.out.print("\n");
            for (String keys : tableMap.keySet()) //add columns to display in o/p
            {
                keyToDisplay.add(keys);
            }
            //check(works for string equality
            if (whereOperation.equals("=")) {
                if (whereValue.contains("\"")) {
                    whereValue = whereValue.replace("\"", "");
                }

                for (int j = 0; j < tableMap.get(keyToDisplay.get(0)).size(); j++) {
                    if (tableMap.get(whereColumn).get(j).equals(whereValue)) {
                        for (int i = 0; i < keyToDisplay.size(); i++) {
                            System.out.print(tableMap.get(keyToDisplay.get(i)).get(j) + "\t\t");
                        }
                    } else {
                        continue;
                    }
                    System.out.print("\n");
                }
            } else {
                System.out.println("Incorrect operation.");
            }
        }
    }

    private void displayAllColumns(HashMap<String, ArrayList<String>> tableMap) {
        ArrayList<String> key = new ArrayList<>();
        for (String keys : tableMap.keySet()) {
            System.out.print(keys + "\t\t");
        }
        System.out.print("\n");
        for (String keys : tableMap.keySet()) {
            key.add(keys);
        }
        for (int j = 0; j < tableMap.get(key.get(0)).size(); j++) {
            for (int i = 0; i < key.size(); i++) {
                System.out.print(tableMap.get(key.get(i)).get(j) + "\t\t");
            }
            System.out.print("\n");
        }
    }

    private void displaySomeColumnswithoutWhere(LinkedHashMap<String, ArrayList<String>> tableMap, String[] columns) throws Exception {
        ArrayList<String> key = new ArrayList<>();
        for (String column : Arrays.asList(columns)) {
            if (tableMap.containsKey(column)) {
                key.add(column);
            } else {
                throw new Exception("Check Column Name");
            }
        }
        if (!key.isEmpty()) {
            for (String keys : key) {
                System.out.print(keys + "\t\t");
            }

            System.out.print("\n");

            for (int j = 0; j < tableMap.get(key.get(0)).size(); j++) {
                for (int i = 0; i < key.size(); i++) {
                    System.out.print(tableMap.get(key.get(i)).get(j) + "\t\t");
                }
                System.out.print("\n");
            }
        }
    }

    private boolean checkMetaData(String key, String tableName) {
        boolean result = false;
        readMetaData m = new readMetaData();
        HashMap<String, MetaData> meta = m.readmetaData(isLocal);
        if (meta.containsKey(tableName)) {
            MetaData meta1 = meta.get(tableName);
            HashMap<String, String> cols = meta1.getColumns();
            if (cols.containsKey(key)) {
                result = true;
            }
        }
        return result;
    }

    public boolean executeSelect(SelectQueryParser parse, boolean isLocal) throws Exception {
        tableName = parse.getTableName();
        SelectQueryExecution.isLocal = isLocal;
        boolean resultSet=false;
        if(TransactionHelper.checkfortablelocked(tableName)==true){
            System.out.println("sorry"+tableName+" is used by another transaction");
            TransactionHelper.writeToTransactionFile(parse.getOriginalSql(),tableName);
            return false;
        }
        if (isLocal) {
            dataPath = ConfigurationSetup.getDataPathLocal();
        } else {
            dataPath = ConfigurationSetup.getDataPathRemote();
        }
        readTable reader = new readTable();
        if (reader.checkTableFile(isLocal, tableName)) {
            LinkedHashMap<String, ArrayList<String>> tableMap = reader.readTableData(tableName,isLocal);
            if (tableMap.isEmpty()) {
                throw new Exception("No entries");
            } else {
                if (!parse.getWhereColumn().isEmpty() && parse.isAll()) {
                    try {
                        displayAllColumnsWithWhere(tableMap, parse);
                        resultSet=true;
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                } else if (!parse.getWhereColumn().isEmpty() && !parse.isAll()) {
                    try {
                        displaySomeColumnswithWhere(tableMap, parse);
                        resultSet=true;
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                } else if (parse.isAll()) {
                    displayAllColumns(tableMap);
                    resultSet=true;
                } else if (parse.getWhereColumn().isEmpty() && !parse.isAll()) {
                    try {
                        displaySomeColumnswithoutWhere(tableMap, parse.getColumns());
                        resultSet=true;
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }

            }
        } else {
            System.out.println("Table not found.");
        }
        return resultSet;
    }

}
