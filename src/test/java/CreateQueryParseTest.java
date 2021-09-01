import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateQueryParseTest
{
    @Test
    public void isValid_True() throws Exception {
        String sql="CREATE TABLE Persons (PersonID int , LastName text , FirstName " +
                "text , Address text , City text , rate float);";
        CreateQueryParse parse=new CreateQueryParse();
        boolean result=parse.isValid(sql);
        Assertions.assertTrue(result);
        Assertions.assertEquals("persons",parse.getTableName());
    }

    @Test
    public void isValidNoBrackets_False() throws Exception {
        String sql="CREATE TABLE Persons PersonID int , LastName text , FirstName " +
                "text , Address text , City text , rate float;";
        CreateQueryParse parse=new CreateQueryParse();
        boolean result=parse.isValid(sql);
        Assertions.assertFalse(result);
    }

    @Test
    public void isValidNoSemiColon_False() throws Exception {
        String sql="CREATE TABLE (Persons PersonID int , LastName text , FirstName " +
                "text , Address text , City text , rate float)";
        CreateQueryParse parse=new CreateQueryParse();
        boolean result=parse.isValid(sql);
        Assertions.assertFalse(result);
    }

    @Test
    public void isValidWrongDataType_False() throws Exception {
        String sql="CREATE TABLE (Persons PersonID integer , LastName text , FirstName " +
                "text , Address text , City text , rate float);";
        CreateQueryParse parse=new CreateQueryParse();
        boolean result=parse.isValid(sql);
        Assertions.assertFalse(result);
    }

    @Test
    public void isValidMissingKeyword_False() throws Exception {
        String sql="CREATE (Persons PersonID integer , LastName text , FirstName " +
                "text , Address text , City text , rate float);";
        CreateQueryParse parse=new CreateQueryParse();
        boolean result=parse.isValid(sql);
        Assertions.assertFalse(result);
    }

}
