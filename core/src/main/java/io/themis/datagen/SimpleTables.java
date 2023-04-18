package io.themis.datagen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.themis.column.ArrayColumn;
import io.themis.partition.BucketTransform;
import io.themis.partition.HourTransform;
import io.themis.partition.IdentityTransform;
import io.themis.column.VarcharColumn;
import io.themis.Table;
import io.themis.column.BooleanColumn;
import io.themis.column.CharColumn;
import io.themis.column.DecimalColumn;
import io.themis.column.IntColumn;
import io.themis.column.MapColumn;
import io.themis.column.StringColumn;
import io.themis.column.StructColumn;
import io.themis.column.TimestampLocalTzColumn;

import java.util.Map;

public class SimpleTables {
    public static final Table PRODUCTS = new Table(
            "products",
            Lists.newArrayList(
                    new CharColumn(1, "id", true, 36),
                    new VarcharColumn(2, "category", true, 20),
                    new StringColumn(3, "item", false),
                    new DecimalColumn(4, "price", false, 8, 2),
                    new StringColumn(5, "description", false)
            ),
            Lists.newArrayList(
                    new BucketTransform(1, 4),
                    new IdentityTransform(2)
            ),
            Lists.newArrayList(1));

    public static final Table USERS = new Table(
            "users",
            Lists.newArrayList(
                    new CharColumn(1, "id", true, 36),
                    new VarcharColumn(2, "first_name", true, 20),
                    new VarcharColumn(3, "middle_name", false, 20),
                    new VarcharColumn(4, "last_name", true, 20),
                    new IntColumn(5, "age", false),
                    new BooleanColumn(6, "gender", true),
                    new StructColumn(7, "contact_info", false, ImmutableList.of(
                            new VarcharColumn(8, "email", false, 50),
                            new VarcharColumn(9, "phone", false, 20)
                    )),
                    new MapColumn(10, "emergency_contacts", false,
                            new VarcharColumn(11, "name", true, 60),
                            new VarcharColumn(12, "phone", true, 20)),
                    new ArrayColumn(13, "additional_info", false,
                            new StringColumn(14, "info", true))
            ),
            Lists.newArrayList(
                    new BucketTransform(1, 4)
            ),
            Lists.newArrayList(1));

    public static final Table ORDERS = new Table(
            "orders",
            Lists.newArrayList(
                    new CharColumn(1, "id", true, 36),
                    new CharColumn(2, "product_id", true, 36),
                    new CharColumn(3, "user_id", true, 36),
                    new VarcharColumn(4, "status", true, 10),
                    new TimestampLocalTzColumn(5, "updated_at", true, 6),
                    new StringColumn(6, "comments", false)
            ),
            Lists.newArrayList(
                    new BucketTransform(1, 4),
                    new HourTransform(5)
            ),
            Lists.newArrayList(1));

    public static final Map<String, Table> TABLES = ImmutableMap.<String, Table>builder()
            .put(PRODUCTS.name(), PRODUCTS)
            .put(USERS.name(), USERS)
            .put(ORDERS.name(), ORDERS)
            .build();
}
