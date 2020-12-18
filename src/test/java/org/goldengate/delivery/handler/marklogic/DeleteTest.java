package org.goldengate.delivery.handler.marklogic;

import oracle.goldengate.datasource.GGDataSource;
import oracle.goldengate.delivery.handler.marklogic.util.HashUtil;
import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;

public class DeleteTest extends AbstractGGTest {

    protected static Integer PK_VALUE = 6985724;
    protected static String ID = HashUtil.hash(PK_VALUE.toString());
    protected static String EXPECTED_URI = MessageFormat.format("/my_org/ogg_test/new_table/{0}.json", ID);

    @Test
    public void testDelete() throws IOException {
        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withScn(100L)
            .withPrimaryKeyColumn("VAL_PK", (long)PK_VALUE)
            .withColumn("VAL_ONE", "insertOne")
            .withColumn("VAL_TWO", "insertTwo")
            .withColumn("VAL_THREE", "insertThree")
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        Map<String, Object> document = readDocument(EXPECTED_URI, builder.getMarklogicHandler().getProperties());
        Map<String, Object> headers = getHeaders(document);
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(instance.get("valPk"), PK_VALUE);
        Assert.assertEquals(headers.get("deleted"), false);
        Assert.assertEquals(headers.get("scn"), 100);
        Assert.assertEquals(instance.get("valOne"), "insertOne");
        Assert.assertEquals(instance.get("valTwo"), "insertTwo");
        Assert.assertEquals(instance.get("valThree"), "insertThree");


        builder = GGInputBuilder.newDelete(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withScn(150L)
            .withPrimaryKeyColumn("VAL_PK", (long)PK_VALUE, true)
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        document = readDocument(EXPECTED_URI, builder.getMarklogicHandler().getProperties());
        headers = getHeaders(document);
        instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(headers.get("operation"), "delete");
        Assert.assertEquals(headers.get("deleted"), true);
        Assert.assertEquals(headers.get("scn"), 150);
        Assert.assertEquals(headers.get("deletedAtScn"), 150);
        Assert.assertEquals(instance.get("valPk"), PK_VALUE);
        Assert.assertNull(instance.get("valOne"));
        Assert.assertNull(instance.get("valTwo"));
        Assert.assertNull(instance.get("valThree"));


        builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withScn(110L)
            .withPrimaryKeyColumn("VAL_PK", (long)PK_VALUE)
            .withColumn("VAL_ONE", "four1")
            .withColumn("VAL_TWO", "four2")
            .withColumn("VAL_THREE", "four3")
            .withColumn("VAL_FOUR", "four4")
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        document = readDocument(EXPECTED_URI, builder.getMarklogicHandler().getProperties());
        headers = getHeaders(document);
        instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(headers.get("operation"), "delete");
        Assert.assertEquals(headers.get("deleted"), true);
        Assert.assertEquals(headers.get("scn"), 150);
        Assert.assertEquals(headers.get("deletedAtScn"), 150);
        Assert.assertEquals(instance.get("valPk"), PK_VALUE);
        Assert.assertNull(instance.get("valOne"));
        Assert.assertNull(instance.get("valTwo"));
        Assert.assertNull(instance.get("valThree"));
        Assert.assertNull(instance.get("valFour"));


        builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withScn(200L)
            .withPrimaryKeyColumn("VAL_PK", (long)PK_VALUE)
            .withColumn("VAL_TWO", "200-2")
            .withColumn("VAL_THREE", "200-3")
            .withColumn("VAL_FOUR", "200-4")
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        document = readDocument(EXPECTED_URI, builder.getMarklogicHandler().getProperties());
        headers = getHeaders(document);
        instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(headers.get("operation"), "insert");
        Assert.assertEquals(headers.get("deleted"), false);
        Assert.assertEquals(headers.get("scn"), 200);
        Assert.assertEquals(headers.get("deletedAtScn"), 150);
        Assert.assertEquals(instance.get("valPk"), PK_VALUE);
        Assert.assertNull(instance.get("valOne"));
        Assert.assertEquals(instance.get("valTwo"), "200-2");
        Assert.assertEquals(instance.get("valThree"), "200-3");
        Assert.assertEquals(instance.get("valFour"), "200-4");


        builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withScn(160L)
            .withPrimaryKeyColumn("VAL_PK", (long)PK_VALUE)
            .withColumn("VAL_ONE", "160-1")
            .withColumn("VAL_TWO", "160-2")
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        document = readDocument(EXPECTED_URI, builder.getMarklogicHandler().getProperties());
        headers = getHeaders(document);
        instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(headers.get("operation"), "insert");
        Assert.assertEquals(headers.get("deleted"), false);
        Assert.assertEquals(headers.get("scn"), 200);
        Assert.assertEquals(headers.get("deletedAtScn"), 150);
        Assert.assertEquals(instance.get("valPk"), PK_VALUE);
        Assert.assertEquals(instance.get("valOne"), "160-1");
        Assert.assertEquals(instance.get("valTwo"), "200-2");
        Assert.assertEquals(instance.get("valThree"), "200-3");
        Assert.assertEquals(instance.get("valFour"), "200-4");


        builder = GGInputBuilder.newDelete(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withScn(170L)
            .withPrimaryKeyColumn("VAL_PK", (long)PK_VALUE, true)
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        document = readDocument(EXPECTED_URI, builder.getMarklogicHandler().getProperties());
        headers = getHeaders(document);
        instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(headers.get("operation"), "insert"); // insert is still the latest operation
        Assert.assertEquals(headers.get("deleted"), false); // there was an insert in the future (scn 200)
        Assert.assertEquals(headers.get("scn"), 200);
        Assert.assertEquals(headers.get("deletedAtScn"), 170);
        Assert.assertEquals(instance.get("valPk"), PK_VALUE);
        Assert.assertNull(instance.get("valOne"));
        Assert.assertEquals(instance.get("valTwo"), "200-2");
        Assert.assertEquals(instance.get("valThree"), "200-3");
        Assert.assertEquals(instance.get("valFour"), "200-4");
    }
}
