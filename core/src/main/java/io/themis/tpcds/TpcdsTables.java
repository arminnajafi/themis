package io.themis.tpcds;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.themis.partition.IdentityTransform;
import io.themis.column.VarcharColumn;
import io.themis.Table;
import io.themis.column.CharColumn;
import io.themis.column.Column;
import io.themis.column.DateColumn;
import io.themis.column.DecimalColumn;
import io.themis.column.IntColumn;
import io.themis.partition.PartitionTransform;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf
 */
public class TpcdsTables {

    public static Table CATALOG_SALES = new Table(
            "catalog_sales",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "cs_sold_date_sk", false))
                    .add(new IntColumn(2, "cs_sold_time_sk", false))
                    .add(new IntColumn(3, "cs_ship_date_sk", false))
                    .add(new IntColumn(4, "cs_bill_customer_sk", false))
                    .add(new IntColumn(5, "cs_bill_cdemo_sk", false))
                    .add(new IntColumn(6, "cs_bill_hdemo_sk", false))
                    .add(new IntColumn(7, "cs_bill_addr_sk", false))
                    .add(new IntColumn(8, "cs_ship_customer_sk", false))
                    .add(new IntColumn(9, "cs_ship_cdemo_sk", false))
                    .add(new IntColumn(10, "cs_ship_hdemo_sk", false))
                    .add(new IntColumn(11, "cs_ship_addr_sk", false))
                    .add(new IntColumn(12, "cs_call_center_sk", false))
                    .add(new IntColumn(13, "cs_catalog_page_sk", false))
                    .add(new IntColumn(14, "cs_ship_mode_sk", false))
                    .add(new IntColumn(15, "cs_warehouse_sk", false))
                    .add(new IntColumn(16, "cs_item_sk", true))
                    .add(new IntColumn(17, "cs_promo_sk", false))
                    .add(new IntColumn(18, "cs_order_number", true))
                    .add(new IntColumn(19, "cs_quantity", false))
                    .add(new DecimalColumn(20, "cs_wholesale_cost", false, 7, 2))
                    .add(new DecimalColumn(21, "cs_list_price", false, 7, 2))
                    .add(new DecimalColumn(22, "cs_sales_price", false, 7, 2))
                    .add(new DecimalColumn(23, "cs_ext_discount_amt", false, 7, 2))
                    .add(new DecimalColumn(24, "cs_ext_sales_price", false, 7, 2))
                    .add(new DecimalColumn(25, "cs_ext_wholesale_cost", false, 7, 2))
                    .add(new DecimalColumn(26, "cs_ext_list_price", false, 7, 2))
                    .add(new DecimalColumn(27, "cs_ext_tax", false, 7, 2))
                    .add(new DecimalColumn(28, "cs_coupon_amt", false, 7, 2))
                    .add(new DecimalColumn(29, "cs_ext_ship_cost", false, 7, 2))
                    .add(new DecimalColumn(30, "cs_net_paid", false, 7, 2))
                    .add(new DecimalColumn(31, "cs_net_paid_inc_tax", false, 7, 2))
                    .add(new DecimalColumn(32, "cs_net_paid_inc_ship", false, 7, 2))
                    .add(new DecimalColumn(33, "cs_net_paid_inc_ship_tax", false, 7, 2))
                    .add(new DecimalColumn(34, "cs_net_profit", false, 7, 2))
                    .build(),
            ImmutableList.<PartitionTransform>builder()
                    .add(new IdentityTransform(1))
                    .build(),
            ImmutableList.of(16, 18));

    public static Table CATALOG_RETURNS = new Table(
            "catalog_returns",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "cr_returned_date_sk", false))
                    .add(new IntColumn(2, "cr_returned_time_sk", false))
                    .add(new IntColumn(3, "cr_item_sk", true))
                    .add(new IntColumn(4, "cr_refunded_customer_sk", false))
                    .add(new IntColumn(5, "cr_refunded_cdemo_sk", false))
                    .add(new IntColumn(6, "cr_refunded_hdemo_sk", false))
                    .add(new IntColumn(7, "cr_refunded_addr_sk", false))
                    .add(new IntColumn(8, "cr_returning_customer_sk", false))
                    .add(new IntColumn(9, "cr_returning_cdemo_sk", false))
                    .add(new IntColumn(10, "cr_returning_hdemo_sk", false))
                    .add(new IntColumn(11, "cr_returning_addr_sk", false))
                    .add(new IntColumn(12, "cr_call_center_sk", false))
                    .add(new IntColumn(13, "cr_catalog_page_sk", false))
                    .add(new IntColumn(14, "cr_ship_mode_sk", false))
                    .add(new IntColumn(15, "cr_warehouse_sk", false))
                    .add(new IntColumn(16, "cr_reason_sk", false))
                    .add(new IntColumn(17, "cr_order_number", true))
                    .add(new IntColumn(18, "cr_return_quantity", false))
                    .add(new DecimalColumn(19, "cr_return_amount", false, 7, 2))
                    .add(new DecimalColumn(20, "cr_return_tax", false, 7, 2))
                    .add(new DecimalColumn(21, "cr_return_amt_inc_tax", false, 7, 2))
                    .add(new DecimalColumn(22, "cr_fee", false, 7, 2))
                    .add(new DecimalColumn(23, "cr_return_ship_cost", false, 7, 2))
                    .add(new DecimalColumn(24, "cr_refunded_cash", false, 7, 2))
                    .add(new DecimalColumn(25, "cr_reversed_charge", false, 7, 2))
                    .add(new DecimalColumn(26, "cr_store_credit", false, 7, 2))
                    .add(new DecimalColumn(27, "cr_net_loss", false, 7, 2))
                    .build(),
            ImmutableList.<PartitionTransform>builder()
                    .add(new IdentityTransform(1))
                    .build(),
            ImmutableList.of(3, 17));

    public static Table INVENTORY = new Table(
            "inventory",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "inv_date_sk", true))
                    .add(new IntColumn(2, "inv_item_sk", true))
                    .add(new IntColumn(3, "inv_warehouse_sk", true))
                    .add(new IntColumn(4, "inv_quantity_on_hand", false))
                    .build(),
            ImmutableList.<PartitionTransform>builder()
                    .add(new IdentityTransform(1))
                    .build(),
            ImmutableList.of(1, 2, 3));

    public static Table STORE_SALES = new Table(
            "store_sales",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "ss_sold_date_sk", false))
                    .add(new IntColumn(2, "ss_sold_time_sk", false))
                    .add(new IntColumn(3, "ss_item_sk", true))
                    .add(new IntColumn(4, "ss_customer_sk", false))
                    .add(new IntColumn(5, "ss_cdemo_sk", false))
                    .add(new IntColumn(6, "ss_hdemo_sk", false))
                    .add(new IntColumn(7, "ss_addr_sk", false))
                    .add(new IntColumn(8, "ss_store_sk", false))
                    .add(new IntColumn(9, "ss_promo_sk", false))
                    .add(new IntColumn(10, "ss_ticket_number", true))
                    .add(new IntColumn(11, "ss_quantity", false))
                    .add(new DecimalColumn(12, "ss_wholesale_cost", false, 7, 2))
                    .add(new DecimalColumn(13, "ss_list_price", false, 7, 2))
                    .add(new DecimalColumn(14, "ss_sales_price", false, 7, 2))
                    .add(new DecimalColumn(15, "ss_ext_discount_amt", false, 7, 2))
                    .add(new DecimalColumn(16, "ss_ext_sales_price", false, 7, 2))
                    .add(new DecimalColumn(17, "ss_ext_wholesale_cost", false, 7, 2))
                    .add(new DecimalColumn(18, "ss_ext_list_price", false, 7, 2))
                    .add(new DecimalColumn(19, "ss_ext_tax", false, 7, 2))
                    .add(new DecimalColumn(20, "ss_coupon_amt", false, 7, 2))
                    .add(new DecimalColumn(21, "ss_net_paid", false, 7, 2))
                    .add(new DecimalColumn(22, "ss_net_paid_inc_tax", false, 7, 2))
                    .add(new DecimalColumn(23, "ss_net_profit", false, 7, 2))
                    .build(),
            ImmutableList.<PartitionTransform>builder()
                    .add(new IdentityTransform(1))
                    .build(),
            ImmutableList.of(3, 10));

    public static Table STORE_RETURNS = new Table(
            "store_returns",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "sr_returned_date_sk", false))
                    .add(new IntColumn(2, "sr_return_time_sk", false))
                    .add(new IntColumn(3, "sr_item_sk", true))
                    .add(new IntColumn(4, "sr_customer_sk", false))
                    .add(new IntColumn(5, "sr_cdemo_sk", false))
                    .add(new IntColumn(6, "sr_hdemo_sk", false))
                    .add(new IntColumn(7, "sr_addr_sk", false))
                    .add(new IntColumn(8, "sr_store_sk", false))
                    .add(new IntColumn(9, "sr_reason_sk", false))
                    .add(new IntColumn(10, "sr_ticket_number", true))
                    .add(new IntColumn(11, "sr_return_quantity", false))
                    .add(new DecimalColumn(12, "sr_return_amt", false, 7, 2))
                    .add(new DecimalColumn(13, "sr_return_tax", false, 7, 2))
                    .add(new DecimalColumn(14, "sr_return_amt_inc_tax", false, 7, 2))
                    .add(new DecimalColumn(15, "sr_fee", false, 7, 2))
                    .add(new DecimalColumn(16, "sr_return_ship_cost", false, 7, 2))
                    .add(new DecimalColumn(17, "sr_refunded_cash", false, 7, 2))
                    .add(new DecimalColumn(18, "sr_reversed_charge", false, 7, 2))
                    .add(new DecimalColumn(19, "sr_store_credit", false, 7, 2))
                    .add(new DecimalColumn(20, "sr_net_loss", false, 7, 2))
                    .build(),
            ImmutableList.<PartitionTransform>builder()
                    .add(new IdentityTransform(1))
                    .build(),
            ImmutableList.of(3, 10));

    public static Table WEB_SALES = new Table(
            "web_sales",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "ws_sold_date_sk", false))
                    .add(new IntColumn(2, "ws_sold_time_sk", false))
                    .add(new IntColumn(3, "ws_ship_date_sk", false))
                    .add(new IntColumn(4, "ws_item_sk", true))
                    .add(new IntColumn(5, "ws_bill_customer_sk", false))
                    .add(new IntColumn(6, "ws_bill_cdemo_sk", false))
                    .add(new IntColumn(7, "ws_bill_hdemo_sk", false))
                    .add(new IntColumn(8, "ws_bill_addr_sk", false))
                    .add(new IntColumn(9, "ws_ship_customer_sk", false))
                    .add(new IntColumn(10, "ws_ship_cdemo_sk", false))
                    .add(new IntColumn(11, "ws_ship_hdemo_sk", false))
                    .add(new IntColumn(12, "ws_ship_addr_sk", false))
                    .add(new IntColumn(13, "ws_web_page_sk", false))
                    .add(new IntColumn(14, "ws_web_site_sk", false))
                    .add(new IntColumn(15, "ws_ship_mode_sk", false))
                    .add(new IntColumn(16, "ws_warehouse_sk", false))
                    .add(new IntColumn(17, "ws_promo_sk", false))
                    .add(new IntColumn(18, "ws_order_number", true))
                    .add(new IntColumn(19, "ws_quantity", false))
                    .add(new DecimalColumn(20, "ws_wholesale_cost", false, 7, 2))
                    .add(new DecimalColumn(21, "ws_list_price", false, 7, 2))
                    .add(new DecimalColumn(22, "ws_sales_price", false, 7, 2))
                    .add(new DecimalColumn(23, "ws_ext_discount_amt", false, 7, 2))
                    .add(new DecimalColumn(24, "ws_ext_sales_price", false, 7, 2))
                    .add(new DecimalColumn(25, "ws_ext_wholesale_cost", false, 7, 2))
                    .add(new DecimalColumn(26, "ws_ext_list_price", false, 7, 2))
                    .add(new DecimalColumn(27, "ws_ext_tax", false, 7, 2))
                    .add(new DecimalColumn(28, "ws_coupon_amt", false, 7, 2))
                    .add(new DecimalColumn(29, "ws_ext_ship_cost", false, 7, 2))
                    .add(new DecimalColumn(30, "ws_net_paid", false, 7, 2))
                    .add(new DecimalColumn(31, "ws_net_paid_inc_tax", false, 7, 2))
                    .add(new DecimalColumn(32, "ws_net_paid_inc_ship", false, 7, 2))
                    .add(new DecimalColumn(33, "ws_net_paid_inc_ship_tax", false, 7, 2))
                    .add(new DecimalColumn(34, "ws_net_profit", false, 7, 2))
                    .build(),
            ImmutableList.<PartitionTransform>builder()
                    .add(new IdentityTransform(1))
                    .build(),
            ImmutableList.of(4, 18));

    public static Table WEB_RETURNS = new Table(
            "web_returns",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "wr_returned_date_sk", false))
                    .add(new IntColumn(2, "wr_returned_time_sk", false))
                    .add(new IntColumn(3, "wr_item_sk", true))
                    .add(new IntColumn(4, "wr_refunded_customer_sk", false))
                    .add(new IntColumn(5, "wr_refunded_cdemo_sk", false))
                    .add(new IntColumn(6, "wr_refunded_hdemo_sk", false))
                    .add(new IntColumn(7, "wr_refunded_addr_sk", false))
                    .add(new IntColumn(8, "wr_returning_customer_sk", false))
                    .add(new IntColumn(9, "wr_returning_cdemo_sk", false))
                    .add(new IntColumn(10, "wr_returning_hdemo_sk", false))
                    .add(new IntColumn(11, "wr_returning_addr_sk", false))
                    .add(new IntColumn(12, "wr_web_page_sk", false))
                    .add(new IntColumn(13, "wr_reason_sk", false))
                    .add(new IntColumn(14, "wr_order_number", true))
                    .add(new IntColumn(15, "wr_return_quantity", false))
                    .add(new DecimalColumn(16, "wr_return_amt", false, 7, 2))
                    .add(new DecimalColumn(17, "wr_return_tax", false, 7, 2))
                    .add(new DecimalColumn(18, "wr_return_amt_inc_tax", false, 7, 2))
                    .add(new DecimalColumn(19, "wr_fee", false, 7, 2))
                    .add(new DecimalColumn(20, "wr_return_ship_cost", false, 7, 2))
                    .add(new DecimalColumn(21, "wr_refunded_cash", false, 7, 2))
                    .add(new DecimalColumn(22, "wr_reversed_charge", false, 7, 2))
                    .add(new DecimalColumn(23, "wr_account_credit", false, 7, 2))
                    .add(new DecimalColumn(24, "wr_net_loss", false, 7, 2))
                    .build(),
            ImmutableList.<PartitionTransform>builder()
                    .add(new IdentityTransform(1))
                    .build(),
            ImmutableList.of(3, 14));

    public static Table CALL_CENTER = new Table(
            "call_center",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "cc_call_center_sk", true))
                    .add(new CharColumn(2, "cc_call_center_id", true, 16))
                    .add(new DateColumn(3, "cc_rec_start_date", false))
                    .add(new DateColumn(4, "cc_rec_end_date", false))
                    .add(new IntColumn(5, "cc_closed_date_sk", false))
                    .add(new IntColumn(6, "cc_open_date_sk", false))
                    .add(new VarcharColumn(7, "cc_name", false, 50))
                    .add(new VarcharColumn(8, "cc_class", false, 50))
                    .add(new IntColumn(9, "cc_employees", false))
                    .add(new IntColumn(10, "cc_sq_ft", false))
                    .add(new CharColumn(11, "cc_hours", false, 20))
                    .add(new VarcharColumn(12, "cc_manager", false, 40))
                    .add(new IntColumn(13, "cc_mkt_id", false))
                    .add(new CharColumn(14, "cc_mkt_class", false, 50))
                    .add(new VarcharColumn(15, "cc_mkt_desc", false, 100))
                    .add(new VarcharColumn(16, "cc_market_manager", false, 40))
                    .add(new IntColumn(17, "cc_division", false))
                    .add(new VarcharColumn(18, "cc_division_name", false, 50))
                    .add(new IntColumn(19, "cc_company", false))
                    .add(new CharColumn(20, "cc_company_name", false, 50))
                    .add(new CharColumn(21, "cc_street_number", false, 10))
                    .add(new VarcharColumn(22, "cc_street_name", false, 60))
                    .add(new CharColumn(23, "cc_street_type", false, 15))
                    .add(new CharColumn(24, "cc_suite_number", false, 10))
                    .add(new VarcharColumn(25, "cc_city", false, 60))
                    .add(new VarcharColumn(26, "cc_county", false, 30))
                    .add(new CharColumn(27, "cc_state", false, 2))
                    .add(new CharColumn(28, "cc_zip", false, 10))
                    .add(new VarcharColumn(29, "cc_country", false, 20))
                    .add(new DecimalColumn(30, "cc_gmt_offset", false, 5, 2))
                    .add(new DecimalColumn(31, "cc_tax_percentage", false, 5, 2))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table CATALOG_PAGE = new Table(
            "catalog_page",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "cp_catalog_page_sk", true))
                    .add(new CharColumn(2, "cp_catalog_page_id", true, 16))
                    .add(new IntColumn(3, "cp_start_date_sk", false))
                    .add(new IntColumn(4, "cp_end_date_sk", false))
                    .add(new VarcharColumn(5, "cp_department", false, 50))
                    .add(new IntColumn(6, "cp_catalog_number", false))
                    .add(new IntColumn(7, "cp_catalog_page_number", false))
                    .add(new VarcharColumn(8, "cp_description", false, 100))
                    .add(new VarcharColumn(9, "cp_type", false, 100))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table CUSTOMER = new Table(
            "customer",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "c_customer_sk", true))
                    .add(new CharColumn(2, "c_customer_id", true, 16))
                    .add(new IntColumn(3, "c_current_cdemo_sk", false))
                    .add(new IntColumn(4, "c_current_hdemo_sk", false))
                    .add(new IntColumn(5, "c_current_addr_sk", false))
                    .add(new IntColumn(6, "c_first_shipto_date_sk", false))
                    .add(new IntColumn(7, "c_first_sales_date_sk", false))
                    .add(new CharColumn(8, "c_salutation", false, 10))
                    .add(new CharColumn(9, "c_first_name", false, 20))
                    .add(new CharColumn(10, "c_last_name", false, 30))
                    .add(new CharColumn(11, "c_preferred_cust_flag", false, 1))
                    .add(new IntColumn(12, "c_birth_day", false))
                    .add(new IntColumn(13, "c_birth_month", false))
                    .add(new IntColumn(14, "c_birth_year", false))
                    .add(new VarcharColumn(15, "c_birth_country", false, 20))
                    .add(new CharColumn(16, "c_login", false, 13))
                    .add(new CharColumn(17, "c_email_address", false, 50))
                    .add(new IntColumn(18, "c_last_review_date", false))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table CUSTOMER_ADDRESS = new Table(
            "customer_address",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "ca_address_sk", true))
                    .add(new CharColumn(2, "ca_address_id", true, 16))
                    .add(new CharColumn(3, "ca_street_number", false, 10))
                    .add(new VarcharColumn(4, "ca_street_name", false, 60))
                    .add(new CharColumn(5, "ca_street_type", false, 15))
                    .add(new CharColumn(6, "ca_suite_number", false, 10))
                    .add(new VarcharColumn(7, "ca_city", false, 60))
                    .add(new VarcharColumn(8, "ca_county", false, 30))
                    .add(new CharColumn(9, "ca_state", false, 2))
                    .add(new CharColumn(10, "ca_zip", false, 10))
                    .add(new VarcharColumn(11, "ca_country", false, 20))
                    .add(new DecimalColumn(12, "ca_gmt_offset", false, 5, 2))
                    .add(new CharColumn(13, "ca_location_type", false, 20))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table CUSTOMER_DEMOGRAPHICS = new Table(
            "customer_demographics",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "cd_demo_sk", true))
                    .add(new CharColumn(2, "cd_gender", false, 1))
                    .add(new CharColumn(3, "cd_marital_status", false, 1))
                    .add(new CharColumn(4, "cd_education_status", false, 20))
                    .add(new IntColumn(5, "cd_purchase_estimate", false))
                    .add(new CharColumn(6, "cd_credit_rating", false, 10))
                    .add(new IntColumn(7, "cd_dep_count", false))
                    .add(new IntColumn(8, "cd_dep_employed_count", false))
                    .add(new IntColumn(9, "cd_dep_college_count", false))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table DATE_DIM = new Table(
            "date_dim",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "d_date_sk", true))
                    .add(new CharColumn(2, "d_date_id", true, 16))
                    .add(new DateColumn(3, "d_date", true))
                    .add(new IntColumn(4, "d_month_seq", false))
                    .add(new IntColumn(5, "d_week_seq", false))
                    .add(new IntColumn(6, "d_quarter_seq", false))
                    .add(new IntColumn(7, "d_year", false))
                    .add(new IntColumn(8, "d_dow", false))
                    .add(new IntColumn(9, "d_moy", false))
                    .add(new IntColumn(10, "d_dom", false))
                    .add(new IntColumn(11, "d_qoy", false))
                    .add(new IntColumn(12, "d_fy_year", false))
                    .add(new IntColumn(13, "d_fy_quarter_seq", false))
                    .add(new IntColumn(14, "d_fy_week_seq", false))
                    .add(new CharColumn(15, "d_day_name", false, 9))
                    .add(new CharColumn(16, "d_quarter_name", false, 6))
                    .add(new CharColumn(17, "d_holiday", false, 1))
                    .add(new CharColumn(18, "d_weekend", false, 1))
                    .add(new CharColumn(19, "d_following_holiday", false, 1))
                    .add(new IntColumn(20, "d_first_dom", false))
                    .add(new IntColumn(21, "d_last_dom", false))
                    .add(new IntColumn(22, "d_same_day_ly", false))
                    .add(new IntColumn(23, "d_same_day_lq", false))
                    .add(new CharColumn(24, "d_current_day", false, 1))
                    .add(new CharColumn(25, "d_current_week", false, 1))
                    .add(new CharColumn(26, "d_current_month", false, 1))
                    .add(new CharColumn(27, "d_current_quarter", false, 1))
                    .add(new CharColumn(28, "d_current_year", false, 1))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table HOUSEHOLD_DEMOGRAPHICS = new Table(
            "household_demographics",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "hd_demo_sk", true))
                    .add(new IntColumn(2, "hd_income_band_sk", false))
                    .add(new CharColumn(3, "hd_buy_potential", false, 15))
                    .add(new IntColumn(4, "hd_dep_count", false))
                    .add(new IntColumn(5, "hd_vehicle_count", false))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table INCOME_BAND = new Table(
            "income_band",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "ib_income_band_sk", true))
                    .add(new IntColumn(2, "ib_lower_bound", false))
                    .add(new IntColumn(3, "ib_upper_bound", false))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table ITEM = new Table(
            "item",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "i_item_sk", true))
                    .add(new CharColumn(2, "i_item_id", true, 16))
                    .add(new DateColumn(3, "i_rec_start_date", false))
                    .add(new DateColumn(4, "i_rec_end_date", false))
                    .add(new VarcharColumn(5, "i_item_desc", false, 200))
                    .add(new DecimalColumn(6, "i_current_price", false, 7, 2))
                    .add(new DecimalColumn(7, "i_wholesale_cost", false, 7, 2))
                    .add(new IntColumn(8, "i_brand_id", false))
                    .add(new CharColumn(9, "i_brand", false, 50))
                    .add(new IntColumn(10, "i_class_id", false))
                    .add(new CharColumn(11, "i_class", false, 50))
                    .add(new IntColumn(12, "i_category_id", false))
                    .add(new CharColumn(13, "i_category", false, 50))
                    .add(new IntColumn(14, "i_manufact_id", false))
                    .add(new CharColumn(15, "i_manufact", false, 50))
                    .add(new CharColumn(16, "i_size", false, 20))
                    .add(new CharColumn(17, "i_formulation", false, 20))
                    .add(new CharColumn(18, "i_color", false, 20))
                    .add(new CharColumn(19, "i_units", false, 10))
                    .add(new CharColumn(20, "i_container", false, 10))
                    .add(new IntColumn(21, "i_manager_id", false))
                    .add(new CharColumn(22, "i_product_name", false, 50))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table PROMOTION = new Table(
            "promotion",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "p_promo_sk", true))
                    .add(new CharColumn(2, "p_promo_id", true, 16))
                    .add(new IntColumn(3, "p_start_date_sk", false))
                    .add(new IntColumn(4, "p_end_date_sk", false))
                    .add(new IntColumn(5, "p_item_sk", false))
                    .add(new DecimalColumn(6, "p_cost", false, 15, 2))
                    .add(new IntColumn(7, "p_response_target", false))
                    .add(new CharColumn(8, "p_promo_name", false, 50))
                    .add(new CharColumn(9, "p_channel_dmail", false, 1))
                    .add(new CharColumn(10, "p_channel_email", false, 1))
                    .add(new CharColumn(11, "p_channel_catalog", false, 1))
                    .add(new CharColumn(12, "p_channel_tv", false, 1))
                    .add(new CharColumn(13, "p_channel_radio", false, 1))
                    .add(new CharColumn(14, "p_channel_press", false, 1))
                    .add(new CharColumn(15, "p_channel_event", false, 1))
                    .add(new CharColumn(16, "p_channel_demo", false, 1))
                    .add(new VarcharColumn(17, "p_channel_details", false, 100))
                    .add(new CharColumn(18, "p_purpose", false, 15))
                    .add(new CharColumn(19, "p_discount_active", false, 1))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table REASON = new Table(
            "reason",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "r_reason_sk", true))
                    .add(new CharColumn(2, "r_reason_id", true, 16))
                    .add(new CharColumn(3, "r_reason_desc", false, 100))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table SHIP_MODE = new Table(
            "ship_mode",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "sm_ship_mode_sk", true))
                    .add(new CharColumn(2, "sm_ship_mode_id", true, 16))
                    .add(new CharColumn(3, "sm_type", false, 30))
                    .add(new CharColumn(4, "sm_code", false, 10))
                    .add(new CharColumn(5, "sm_carrier", false, 20))
                    .add(new CharColumn(6, "sm_contract", false, 20))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table STORE = new Table(
            "store",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "s_store_sk", true))
                    .add(new CharColumn(2, "s_store_id", true, 16))
                    .add(new DateColumn(3, "s_rec_start_date", false))
                    .add(new DateColumn(4, "s_rec_end_date", false))
                    .add(new IntColumn(5, "s_closed_date_sk", false))
                    .add(new VarcharColumn(6, "s_store_name", false, 50))
                    .add(new IntColumn(7, "s_number_employees", false))
                    .add(new IntColumn(8, "s_floor_space", false))
                    .add(new CharColumn(9, "s_hours", false, 20))
                    .add(new VarcharColumn(10, "s_manager", false, 40))
                    .add(new IntColumn(11, "s_market_id", false))
                    .add(new VarcharColumn(12, "s_geography_class", false, 100))
                    .add(new VarcharColumn(13, "s_market_desc", false, 100))
                    .add(new VarcharColumn(14, "s_market_manager", false, 40))
                    .add(new IntColumn(15, "s_division_id", false))
                    .add(new VarcharColumn(16, "s_division_name", false, 50))
                    .add(new IntColumn(17, "s_company_id", false))
                    .add(new VarcharColumn(18, "s_company_name", false, 50))
                    .add(new VarcharColumn(19, "s_street_number", false, 10))
                    .add(new VarcharColumn(20, "s_street_name", false, 60))
                    .add(new CharColumn(21, "s_street_type", false, 15))
                    .add(new CharColumn(22, "s_suite_number", false, 10))
                    .add(new VarcharColumn(23, "s_city", false, 60))
                    .add(new VarcharColumn(24, "s_county", false, 30))
                    .add(new CharColumn(25, "s_state", false, 2))
                    .add(new CharColumn(26, "s_zip", false, 10))
                    .add(new VarcharColumn(27, "s_country", false, 20))
                    .add(new DecimalColumn(28, "s_gmt_offset", false, 5, 2))
                    .add(new DecimalColumn(29, "s_tax_percentage", false, 5, 2))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table TIME_DIM = new Table(
            "time_dim",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "t_time_sk", true))
                    .add(new CharColumn(2, "t_time_id", true, 16))
                    .add(new IntColumn(3, "t_time", true))
                    .add(new IntColumn(4, "t_hour", false))
                    .add(new IntColumn(5, "t_minute", false))
                    .add(new IntColumn(6, "t_second", false))
                    .add(new CharColumn(7, "t_am_pm", false, 2))
                    .add(new CharColumn(8, "t_shift", false, 20))
                    .add(new CharColumn(9, "t_sub_shift", false, 20))
                    .add(new CharColumn(10, "t_meal_time", false, 20))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table WAREHOUSE = new Table(
            "warehouse",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "w_warehouse_sk", true))
                    .add(new CharColumn(2, "w_warehouse_id", true, 16))
                    .add(new VarcharColumn(3, "w_warehouse_name", false, 20))
                    .add(new IntColumn(4, "w_warehouse_sq_ft", false))
                    .add(new CharColumn(5, "w_street_number", false, 10))
                    .add(new VarcharColumn(6, "w_street_name", false, 20))
                    .add(new CharColumn(7, "w_street_type", false, 15))
                    .add(new CharColumn(8, "w_suite_number", false, 10))
                    .add(new VarcharColumn(9, "w_city", false, 60))
                    .add(new VarcharColumn(10, "w_county", false, 30))
                    .add(new CharColumn(11, "w_state", false, 2))
                    .add(new CharColumn(12, "w_zip", false, 10))
                    .add(new VarcharColumn(13, "w_country", false, 20))
                    .add(new DecimalColumn(14, "w_gmt_offset", false, 5, 2))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table WEB_PAGE = new Table(
            "web_page",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "wp_web_page_sk", true))
                    .add(new CharColumn(2, "wp_web_page_id", true, 16))
                    .add(new DateColumn(3, "wp_rec_start_date", false))
                    .add(new DateColumn(4, "wp_rec_end_date", false))
                    .add(new IntColumn(5, "wp_creation_date_sk", false))
                    .add(new IntColumn(6, "wp_access_date_sk", false))
                    .add(new CharColumn(7, "wp_autogen_flag", false, 1))
                    .add(new IntColumn(8, "wp_customer_sk", false))
                    .add(new VarcharColumn(9, "wp_url", false, 100))
                    .add(new CharColumn(10, "wp_type", false, 50))
                    .add(new IntColumn(11, "wp_char_count", false))
                    .add(new IntColumn(12, "wp_link_count", false))
                    .add(new IntColumn(13, "wp_image_count", false))
                    .add(new IntColumn(14, "wp_max_ad_count", false))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static Table WEB_SITE = new Table(
            "web_site",
            ImmutableList.<Column>builder()
                    .add(new IntColumn(1, "web_site_sk", true))
                    .add(new CharColumn(2, "web_site_id", true, 16))
                    .add(new DateColumn(3, "web_rec_start_date", false))
                    .add(new DateColumn(4, "web_rec_end_date", false))
                    .add(new VarcharColumn(5, "web_name", false, 50))
                    .add(new IntColumn(6, "web_open_date_sk", false))
                    .add(new IntColumn(7, "web_close_date_sk", false))
                    .add(new VarcharColumn(8, "web_class", false, 50))
                    .add(new VarcharColumn(9, "web_manager", false, 40))
                    .add(new IntColumn(10, "web_mkt_id", false))
                    .add(new VarcharColumn(11, "web_mkt_class", false, 50))
                    .add(new VarcharColumn(12, "web_mkt_desc", false, 100))
                    .add(new VarcharColumn(13, "web_market_manager", false, 40))
                    .add(new IntColumn(14, "web_company_id", false))
                    .add(new CharColumn(15, "web_company_name", false, 50))
                    .add(new CharColumn(16, "web_street_number", false, 10))
                    .add(new VarcharColumn(17, "web_street_name", false, 60))
                    .add(new CharColumn(18, "web_street_type", false, 15))
                    .add(new CharColumn(19, "web_suite_number", false, 10))
                    .add(new VarcharColumn(20, "web_city", false, 60))
                    .add(new VarcharColumn(21, "web_county", false, 30))
                    .add(new CharColumn(22, "web_state", false, 2))
                    .add(new CharColumn(23, "web_zip", false, 10))
                    .add(new VarcharColumn(24, "web_country", false, 20))
                    .add(new DecimalColumn(25, "web_gmt_offset", false, 5, 2))
                    .add(new DecimalColumn(26, "web_tax_percentage", false, 5, 2))
                    .build(),
            ImmutableList.of(),
            ImmutableList.of(1));

    public static final Map<String, Table> TABLES = ImmutableMap.<String, Table>builder()
            .put(CATALOG_SALES.name(), CATALOG_SALES)
            .put(CATALOG_RETURNS.name(), CATALOG_RETURNS)
            .put(INVENTORY.name(), INVENTORY)
            .put(STORE_SALES.name(), STORE_SALES)
            .put(STORE_RETURNS.name(), STORE_RETURNS)
            .put(WEB_SALES.name(), WEB_SALES)
            .put(WEB_RETURNS.name(), WEB_RETURNS)
            .put(CALL_CENTER.name(), CALL_CENTER)
            .put(CATALOG_PAGE.name(), CATALOG_PAGE)
            .put(CUSTOMER.name(), CUSTOMER)
            .put(CUSTOMER_ADDRESS.name(), CUSTOMER_ADDRESS)
            .put(CUSTOMER_DEMOGRAPHICS.name(), CUSTOMER_DEMOGRAPHICS)
            .put(DATE_DIM.name(), DATE_DIM)
            .put(HOUSEHOLD_DEMOGRAPHICS.name(), HOUSEHOLD_DEMOGRAPHICS)
            .put(INCOME_BAND.name(), INCOME_BAND)
            .put(ITEM.name(), ITEM)
            .put(PROMOTION.name(), PROMOTION)
            .put(REASON.name(), REASON)
            .put(SHIP_MODE.name(), SHIP_MODE)
            .put(STORE.name(), STORE)
            .put(TIME_DIM.name(), TIME_DIM)
            .put(WAREHOUSE.name(), WAREHOUSE)
            .put(WEB_PAGE.name(), WEB_PAGE)
            .put(WEB_SITE.name(), WEB_SITE)
            .build();

    public static final Map<String, Table> UNPARTITIONED_TABLES = TABLES
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().withPartitionKeys(ImmutableList.of())));

    public static final Map<Table, Integer> SF1_ROW_COUNTS = ImmutableMap.<Table, Integer>builder()
            .put(TpcdsTables.CATALOG_SALES, 1441548)
            .put(TpcdsTables.CATALOG_RETURNS, 144067)
            .put(TpcdsTables.INVENTORY, 11745000)
            .put(TpcdsTables.STORE_SALES, 2880404)
            .put(TpcdsTables.STORE_RETURNS, 287514)
            .put(TpcdsTables.WEB_SALES, 719384)
            .put(TpcdsTables.WEB_RETURNS, 71763)
            .put(TpcdsTables.CALL_CENTER, 6)
            .put(TpcdsTables.CATALOG_PAGE, 11718)
            .put(TpcdsTables.CUSTOMER, 100000)
            .put(TpcdsTables.CUSTOMER_ADDRESS, 50000)
            .put(TpcdsTables.CUSTOMER_DEMOGRAPHICS, 1920800)
            .put(TpcdsTables.DATE_DIM, 73049)
            .put(TpcdsTables.HOUSEHOLD_DEMOGRAPHICS, 7200)
            .put(TpcdsTables.INCOME_BAND, 20)
            .put(TpcdsTables.ITEM, 18000)
            .put(TpcdsTables.PROMOTION, 300)
            .put(TpcdsTables.REASON, 35)
            .put(TpcdsTables.SHIP_MODE, 20)
            .put(TpcdsTables.STORE, 12)
            .put(TpcdsTables.TIME_DIM, 86400)
            .put(TpcdsTables.WAREHOUSE, 5)
            .put(TpcdsTables.WEB_PAGE, 60)
            .put(TpcdsTables.WEB_SITE, 30)
            .build();

    public static final Map<Table, Integer> SF1_P1000_ID1_ROW_COUNTS = ImmutableMap.<Table, Integer>builder()
            .put(TpcdsTables.CATALOG_SALES, 1441548)
            .put(TpcdsTables.CATALOG_RETURNS, 144067)
            .put(TpcdsTables.INVENTORY, 11745)
            .put(TpcdsTables.STORE_SALES, 2880404)
            .put(TpcdsTables.STORE_RETURNS, 287514)
            .put(TpcdsTables.WEB_SALES, 719384)
            .put(TpcdsTables.WEB_RETURNS, 71763)
            .put(TpcdsTables.CALL_CENTER, 6)
            .put(TpcdsTables.CATALOG_PAGE, 11718)
            .put(TpcdsTables.CUSTOMER, 100000)
            .put(TpcdsTables.CUSTOMER_ADDRESS, 50000)
            .put(TpcdsTables.CUSTOMER_DEMOGRAPHICS, 1921)
            .put(TpcdsTables.DATE_DIM, 73049)
            .put(TpcdsTables.HOUSEHOLD_DEMOGRAPHICS, 7200)
            .put(TpcdsTables.INCOME_BAND, 20)
            .put(TpcdsTables.ITEM, 18000)
            .put(TpcdsTables.PROMOTION, 300)
            .put(TpcdsTables.REASON, 35)
            .put(TpcdsTables.SHIP_MODE, 20)
            .put(TpcdsTables.STORE, 12)
            .put(TpcdsTables.TIME_DIM, 86400)
            .put(TpcdsTables.WAREHOUSE, 5)
            .put(TpcdsTables.WEB_PAGE, 60)
            .put(TpcdsTables.WEB_SITE, 30)
            .build();

    public static final Map<Table, Integer> SF1_P1000_ID2_ROW_COUNTS = ImmutableMap.<Table, Integer>builder()
            .put(TpcdsTables.CATALOG_SALES, 0)
            .put(TpcdsTables.CATALOG_RETURNS, 0)
            .put(TpcdsTables.INVENTORY, 11745)
            .put(TpcdsTables.STORE_SALES, 0)
            .put(TpcdsTables.STORE_RETURNS, 0)
            .put(TpcdsTables.WEB_SALES, 0)
            .put(TpcdsTables.WEB_RETURNS, 0)
            .put(TpcdsTables.CALL_CENTER, 0)
            .put(TpcdsTables.CATALOG_PAGE, 0)
            .put(TpcdsTables.CUSTOMER, 0)
            .put(TpcdsTables.CUSTOMER_ADDRESS, 0)
            .put(TpcdsTables.CUSTOMER_DEMOGRAPHICS, 1921)
            .put(TpcdsTables.DATE_DIM, 0)
            .put(TpcdsTables.HOUSEHOLD_DEMOGRAPHICS, 0)
            .put(TpcdsTables.INCOME_BAND, 0)
            .put(TpcdsTables.ITEM, 0)
            .put(TpcdsTables.PROMOTION, 0)
            .put(TpcdsTables.REASON, 0)
            .put(TpcdsTables.SHIP_MODE, 0)
            .put(TpcdsTables.STORE, 0)
            .put(TpcdsTables.TIME_DIM, 0)
            .put(TpcdsTables.WAREHOUSE, 0)
            .put(TpcdsTables.WEB_PAGE, 0)
            .put(TpcdsTables.WEB_SITE, 0)
            .build();

    public static final List<String[]> CALL_CENTER_DATA = Lists.newArrayList(
            new String[]{"1", "AAAAAAAABAAAAAAA", "1998-01-01", null, null, "2450952", "NY Metro",
                    "large", "2", "1138", "8AM-4PM", "Bob Belcher", "6", "More than other authori",
                    "Shared others could not count fully dollars. New members ca", "Julius Tran",
                    "3", "pri", "6", "cally", "730", "Ash Hill", "Boulevard", "Suite 0", "Midway",
                    "Williamson County", "TN", "31904", "United States", "-5", "0.11"},
            new String[]{"2", "AAAAAAAACAAAAAAA", "1998-01-01", "2000-12-31", null, "2450806", "Mid Atlantic",
                    "medium", "6", "2268", "8AM-8AM", "Felipe Perkins", "2", "A bit narrow forms matter animals. Consist",
                    "Largely blank years put substantially deaf, new others. Question", "Julius Durham",
                    "5", "anti", "1", "ought", "984", "Center Hill", "Way", "Suite 70", "Midway",
                    "Williamson County", "TN", "31904", "United States", "-5", "0.12"},
            new String[]{"3", "AAAAAAAACAAAAAAA", "2001-01-01", null, null, "2450806", "Mid Atlantic",
                    "medium", "6", "4134", "8AM-4PM", "Mark Hightower", "2", "Wrong troops shall work sometimes in a opti",
                    "Largely blank years put substantially deaf, new others. Question", "Julius Durham",
                    "1", "ought", "2", "able", "984", "Center Hill", "Way", "Suite 70", "Midway",
                    "Williamson County", "TN", "31904", "United States", "-5", "0.01"},
            new String[]{"4", "AAAAAAAAEAAAAAAA", "1998-01-01", "2000-01-01", null, "2451063", "North Midwest",
                    "medium", "1", "649", "8AM-4PM", "Larry Mccray", "2", "Dealers make most historical, direct students",
                    "Rich groups catch longer other fears; future,", "Matthew Clifton",
                    "4", "ese", "3", "pri", "463", "Pine Ridge", "RD", "Suite U", "Midway",
                    "Williamson County", "TN", "31904", "United States", "-5", "0.05"},
            new String[]{"5", "AAAAAAAAEAAAAAAA", "2000-01-02", "2001-12-31", null, "2451063", "North Midwest",
                    "small", "3", "795", "8AM-8AM", "Larry Mccray", "2", "Dealers make most historical, direct students",
                    "Blue, due beds come. Politicians would not make far thoughts. Specifically new horses partic", "Gary Colburn",
                    "4", "ese", "3", "pri", "463", "Pine Ridge", "RD", "Suite U", "Midway",
                    "Williamson County", "TN", "31904", "United States", "-5", "0.12"},
            new String[]{"6", "AAAAAAAAEAAAAAAA", "2002-01-01", null, null, "2451063", "North Midwest",
                    "medium", "7", "3514", "8AM-4PM", "Larry Mccray", "5", "Silly particles could pro",
                    "Blue, due beds come. Politicians would not make far thoughts. Specifically new horses partic", "Gary Colburn",
                    "5", "anti", "3", "pri", "463", "Pine Ridge", "RD", "Suite U", "Midway",
                    "Williamson County", "TN", "31904", "United States", "-5", "0.11"});
}
