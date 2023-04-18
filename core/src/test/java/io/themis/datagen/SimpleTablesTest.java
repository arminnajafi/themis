package io.themis.datagen;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimpleTablesTest {

    @Test
    public void testNestedSchemaDDL() {
        Assertions.assertThat(SimpleTables.USERS.ddlSchemaPart())
                .isEqualTo("id char(36)," +
                        "first_name varchar(20)," +
                        "middle_name varchar(20)," +
                        "last_name varchar(20)," +
                        "age int," +
                        "gender boolean," +
                        "contact_info struct<email:varchar(50),phone:varchar(20)>," +
                        "emergency_contacts map<varchar(60),varchar(20)>," +
                        "additional_info array<string>");
    }
}
