//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//
//public class insertQueryParserTest
//{
//    @Test
//    public void isValidInsertQuery_true()
//    {
//        String query = "insert into customer values(1,'tejaswi',22.5);";
//        insertParser inseryQuery = new insertParser();
//        String[] data = query.split("values");
//        try {
//            Assertions.assertTrue(inseryQuery.insertQueryParserChecker(data));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void queryWithInvalidTextDataType()
//    {
//        String query = "insert into customer values(1,'tejaswi,22.5);";
//        insertParser inseryQuery = new insertParser();
//        String[] data = query.split("values");
//        try {
//            Assertions.assertFalse(inseryQuery.insertQueryParserChecker(data));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void queryWithInvalidIntDataType()
//    {
//        String query = "insert into customer values(1b,'tejaswi',22.5);";
//        insertParser inseryQuery = new insertParser();
//        String[] data = query.split("values");
//        try {
//            Assertions.assertFalse(inseryQuery.insertQueryParserChecker(data));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void queryWithInvalidFloatDataType()
//    {
//        String query = "insert into customer values(1,'tejaswi',22.5b);";
//        insertParser inseryQuery = new insertParser();
//        String[] data = query.split("values");
//        try {
//            Assertions.assertFalse(inseryQuery.insertQueryParserChecker(data));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//}
