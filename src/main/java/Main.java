import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class Main {
    static int totalTransaction = 0;
    static HashMap<String, List<String>> gdd;

    public static void main(String[] args) throws Exception {
        ConfigurationSetup.setupConfiguration();
        Scanner scanner = new Scanner(System.in);
        String query = null;
        CredentialsValidation validator = new CredentialsValidation();
        ConfigurationSetup.setAutoCommitToTrue();
        readGDD.readgdd();
        gdd = GDD.getInstance();
        if (validator.isValidUser()) {
            try {
                do {
                    System.out.println("Enter query or type exit to quit");
                    query = scanner.nextLine();
                    long startTime = System.nanoTime();
                    if (query.equals("exit")) {
                        break;
                    }
                    execute(query);
                    long endTime = System.nanoTime();
                    long executionTime = (endTime - startTime) / 1000000;
                    System.out.println("execution time in milliseconds is: " + executionTime);
                } while (true);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    static void execute(String query) {
        Scanner sc = new Scanner(System.in);
        boolean result;
        String transaction = "";
        String[] queryOperations = query.split(" ");
        try {
            if (queryOperations[0].toLowerCase().equals("begin")) {
                totalTransaction = totalTransaction + 1;
                ConfigurationSetup.setAutoCommitToFalse();
                TransactionHelper.writeTransaction(queryOperations[1].trim());
            } else if (queryOperations[0].toLowerCase().equals("commit")) {
                totalTransaction = totalTransaction - 1;
                ConfigurationSetup.setAutoCommitToTrue();
                TransactionHelper.commitTransaction(queryOperations[1].trim());
                ConfigurationSetup.setAutoCommitToFalse();
            } else if (queryOperations[0].toLowerCase().equals("rollback")) {
                totalTransaction = totalTransaction - 1;
                TransactionHelper.deleteTransactionFile(queryOperations[1].trim());
                ConfigurationSetup.setAutoCommitToTrue();
            } else if (totalTransaction > 0) {
                System.out.println("Enter transaction Name");
                transaction = sc.nextLine();
                TransactionHelper.setTransaction(transaction.trim());
            }

            switch (queryOperations[0].toLowerCase()) {
                case "insert":
                    insertHelper insertData = new insertHelper();
                    insertData.insertHelperClass(query);
                    break;

                case "drop":
                    DropTableParse dropTableParse = new DropTableParse();
                    dropTableParse.dropTable(query);
                    break;

                case "delete":
                    deleteParser deleteFromTable = new deleteParser();
                    deleteFromTable.deleteRecord(query);
                    break;

                case "update":
                    updateQueryParser updateTable = new updateQueryParser();
                    updateTable.updateTable(query);
                    break;

                case "select":
                    SelectQueryParser parser = new SelectQueryParser();
                    readGDD.readgdd();
                    gdd = GDD.getInstance();
                    if (parser.isValid(query)) {
                        if (gdd.get("local").contains(parser.getTableName())) {
                            SelectQueryExecution execution = new SelectQueryExecution();
                            execution.executeSelect(parser, true);
                        } else if (gdd.get("remote").contains(parser.getTableName())) {
                            SelectQueryExecution execution = new SelectQueryExecution();
                            execution.executeSelect(parser, false);
                        } else {
                            System.out.println("Table does not exist.");
                        }
                    } else {
                        throw new Exception("Invalid Syntax");
                    }
                    break;
                case "begin":
                    break;
                case "commit":
                    break;
                case "rollback":
                    break;
                case "local":
                case "remote":
                    CreateQueryParse createParser = new CreateQueryParse();
                    createParser.createTable(query);
                    break;
                case "erd":
                    System.out.println("Enter file path you want to create ERD:");
                    String url = sc.nextLine();
                    System.out.println("Enter file name with txt extension");
                    String fileName = sc.nextLine();
                    generateERD generateerd = new generateERD();
                    result = generateerd.generateERDOfDB(url, fileName);
                    System.out.println(result);
                    break;
                default:
                    System.out.println("Invalid query");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    static boolean executeTransaction(String query) {
        Scanner scanner = new Scanner(System.in);
        boolean result = false;
        String[] queryOperations = query.split(" ");
        try {
            switch (queryOperations[0].toLowerCase()) {
                case "insert":
                    insertHelper insertData = new insertHelper();
                    result = insertData.insertHelperClass(query);
                    break;

                case "drop":
                    DropTableParse dropTableParse = new DropTableParse();
                    dropTableParse.dropTable(query);
                    break;

                case "delete":
                    deleteParser deleteFromTable = new deleteParser();
                    result = deleteFromTable.deleteRecord(query);
                    break;

                case "update":
                    updateQueryParser updateTable = new updateQueryParser();
                    result = updateTable.updateTable(query);
                    break;

                case "select":
                    SelectQueryParser parser = new SelectQueryParser();
                    if (parser.isValid(query)) {
                        if (gdd.get("local").contains(parser.getTableName())) {
                            SelectQueryExecution execution = new SelectQueryExecution();
                            result = execution.executeSelect(parser, true);
                        } else if (gdd.get("remote").contains(parser.getTableName())) {
                            SelectQueryExecution execution = new SelectQueryExecution();
                            result = execution.executeSelect(parser, false);
                        } else {
                            System.out.println("Table does not exist.");
                        }
                    } else {
                        throw new Exception("Invalid Syntax");
                    }
                    break;
                default:
                    System.out.println("Invalid query");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

