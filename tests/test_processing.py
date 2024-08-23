import sys
import os
#import dataframe from polars
from polars import DataFrame as PolarsDataFrame
import polars as pl
from polars import Datetime
import numpy as np
from sqlalchemy.orm import declarative_base
from datetime import datetime
import unittest


# Adding the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
curr_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
data_dir = os.path.join(curr_dir, "data")
# WORKING_DIR = "/customer_orders_analysis/data"

from unittest.mock import patch 
from src.processing import (load_data,
                            transform_product_category_df,
                            transform__df,
                            process_fact_table,
                            process_dim_table_df,
                            get_top_sellers
                            )


orders_df_path = os.path.join(data_dir, "olist_order_items_dataset.csv")
order_items_df_path = os.path.join(data_dir, "olist_orders_dataset.csv")
customers_df_path = os.path.join(data_dir, "olist_customers_dataset.csv")
order_payments_df_path = os.path.join(data_dir, "olist_order_payments_dataset.csv")
products_df_path = os.path.join(data_dir, "olist_products_dataset.csv")
sellers_df_path = os.path.join(data_dir, "olist_sellers_dataset.csv")
product_category_df_path = os.path.join(data_dir, "product_category_name_translation.csv")

orders_df = load_data(orders_df_path)
order_items_df = load_data(order_items_df_path)
customers_df = load_data(customers_df_path)
order_payments_df = load_data(order_payments_df_path)
products_df = load_data(products_df_path)
sellers_df = load_data(sellers_df_path)
product_category_df = load_data(product_category_df_path)

class TestLoadData(unittest.TestCase):
    
    def test_load_data_none(self):
        "Test load_data function with None file_path"
        file_path = None
        result = load_data(file_path)
        self.assertEqual(result, "Invalid file path or format provided")
    
    def test_load_data_invalid(self):
        "Test load_data function with invalid file_path"
        file_path = "invalid_path"
        result = load_data(file_path)
        self.assertEqual(result, "File path does not exist or is not in the right format")
    
    def test_load_data_valid(self):
        "Test load_data function with valid file_path"

        file_path = os.path.join(data_dir, "olist_customers_dataset.csv")
        result = load_data(file_path).is_empty()  
        self.assertEqual(result, False)



class TestTransformProductCategoryDF(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.sample_data = pl.DataFrame({
            "product_category_name": ["category1", "category2", "category3"],
            "product_category_name_english": ["cat1", "cat2", "cat3"]
        })

    def test_with_correct_df(self):
        "Test with a valid product category df"
        file_path = os.path.join(data_dir, "product_category_name_translation.csv")
        df = load_data(file_path)
        result_df = transform_product_category_df(df)

        #check if the dataframe has the same no. of rows
        df_rows = len(df)
        result_rows = len(result_df)
        self.assertEqual(df_rows, result_rows)

        # Check if the result is a Polars DataFrame
        check_result = result_df.is_empty()
        self.assertEqual(check_result, False)

        # Check if the new column 'product_category_id' is added
        self.assertIn("product_category_id", result_df.columns)

        # Check if the new product_category_id column has the correct data type
        self.assertEqual(result_df["product_category_id"].dtype, pl.Int64)

        # Check if the new product_category_id column has the correct values
        expected_ids = pl.Series("product_category_id", 
                                 np.arange(1, len(df) + 1))
        equal_val = expected_ids == result_df["product_category_id"]
        
        self.assertEqual(equal_val.all(), True)

        # Check if the original product_category columns are preserved before the transformation
        original_columns = set(self.sample_data.columns)
        result_columns = set(result_df.columns) - {"product_category_id"}
        self.assertEqual(original_columns, result_columns)

    def test_empty_dataframe(self):
        "Test the transform category function parameter with an empty DataFrame"
        empty_df = pl.DataFrame()
        result_df = transform_product_category_df(empty_df)
        self.assertEqual(result_df, "Dataframe is empty")
    
    def test_with_string(self):
        "Test the transform category function parameter with a string type"

        result = transform_product_category_df("string")
        self.assertEqual(result, "Please provide a valid dataframe")
    
    def test_with_none(self):
        "Test the transform category function parameter with a None type"

        result = transform_product_category_df(None)
        self.assertEqual(result, "Please provide a valid dataframe")

class TestTransformDF(unittest.TestCase):
    
        def setUp(self):
            # Sample data for testing
            self.sample_data = pl.DataFrame({
                "product_id": ["prod1", "prod2", "prod3"],
                "product_category_name": ["category1", "category2", "category3"],
                "product_name_lenght": ["name1", "name2", "name3"],
                "product_description_lenght": ["desc1", "desc2", "desc3"],
                "product_photos_qty": ["1", "2", "3"],
                "product_weight_g": [1, 2, 3],
                "product_length_cm": [1, 2, 3],
                "product_height_cm": [1, 2, 3],
                "product_width_cm": [1, 2, 3]
            })
    
        def test_with_correct_df(self):
            "Test with a valid product df"
            file_path = os.path.join(data_dir, "olist_products_dataset.csv")
            df = load_data(file_path)
            result_df = transform__df(df)
    
            #check if the dataframe has the same no. of rows
            df_rows = len(df)
            result_rows = len(result_df)
            self.assertEqual(df_rows, result_rows)
            
            # Check if the result is a Polars DataFrame
            check_result = result_df.is_empty()
            self.assertEqual(check_result, False)
    
            # Check if the new column 'product_id' is added
            self.assertIn("id", result_df.columns)
         
            # Check if the new product_id column has the correct data type
            self.assertEqual(result_df["id"].dtype, pl.Int64)
    
            # Check if the new id column has the correct values
            expected_ids = pl.Series("id", 
                                    np.arange(1, len(df) + 1))
            equal_val = expected_ids == result_df["id"]
            
            self.assertEqual(equal_val.all(), True)
    
            # Check if the original product columns are preserved before the transformation
            original_columns = set(self.sample_data.columns)
            result_columns = set(result_df.columns) - {"id"}
            self.assertEqual(original_columns, result_columns)
    
        def test_empty_dataframe(self):
            "Test the transform category function parameter with an empty DataFrame"
            empty_df = pl.DataFrame()
            result_df = transform__df(empty_df)
            self.assertEqual(result_df, "Dataframe is empty")
        
        def test_with_string(self):
            "Test the transform category function parameter with a string type"
    
            result = transform__df("string")
            self.assertEqual(result, "Please provide a valid dataframe")
        
        def test_with_none(self):
            "Test the transform category function parameter with a None type"
    
            result = transform__df(None)
            self.assertEqual(result, "Please provide a valid dataframe")
        
class TestProcessDimTableDF(unittest.TestCase):
    def setUp(self):
        #sample data for testing
        self.mock_db_table = [
            MockDBRow(
                id=1, name="Ayomide", job="Data Engineer"
            ),
            MockDBRow(
                id=2, name="Corper", job="Data Scientist"
            ),
            MockDBRow(
                id=3, name="James", job="Software Engineer"
            )
        ]

    def test_with_valid_input(self):
        with patch('src.processing.process_dim_table_df', 
                   return_value = pl.DataFrame({
                       "id": [1, 2, 3],
                       "name": ["Ayomide", "Corper", "James"],
                       "job": ["Data Engineer", "Data Scientist", "Software Engineer"]
                   })) as mock_process_dim_table_df:
            
            result = process_dim_table_df(self.mock_db_table)
            # Check if the result is a Polars DataFrame
            self.assertIsInstance(result, pl.DataFrame)
            #check if the dataframe has the correct shape
            self.assertEqual(result.shape, (3,3))

            #check if the dataframe has the correct columns
            self.assertListEqual(result.columns, ["id", "name", "job"])

            #check if the dataframe has the correct values
            expected_df = pl.DataFrame({
                "id": [1, 2, 3],
                "name": ["Ayomide", "Corper", "James"],
                "job": ["Data Engineer", "Data Scientist", "Software Engineer"]
            })
            for col in result.columns:
                self.assertListEqual(result[col].to_list(), expected_df[col].to_list())

    def test_empty_list_tables(self):
        "Test the process_dim_table_df function parameter with an empty db table"
        empty_list = []
        result_df = process_dim_table_df(empty_list)
        self.assertEqual(result_df, "Please provide a valid sqlalchemy table")
    
    def test_with_string(self):
        "Test the process_dim_table_df function parameter with a string type"

        result = process_dim_table_df("string")
        self.assertEqual(result, "Please provide a valid sqlalchemy table")
    
    def test_with_none(self):
        "Test the process_dim_table_df function parameter with a None type"

        result = process_dim_table_df(None)
        self.assertEqual(result, "Please provide a valid sqlalchemy table")
    
class MockDBRow:
    def __init__(self, id, name, job):
        self.id = id
        self.name = name
        self.job = job
    
    @property
    def __table__(self):
        return self

    @property
    def columns(self):
        return MockColumns(["id", "name", "job"])

class MockColumns:
    def __init__(self, column_names):
        self.column_names = column_names
    
    def keys(self):
        return self.column_names

class TestProcessFactTable(unittest.TestCase):

    def test_with_correct_data(self):
        "Test the process_fact_table function with with the valid list of dataframes"

        list_of_dfs = [orders_df,order_items_df, customers_df, order_payments_df, products_df, sellers_df, product_category_df]
        result = process_fact_table(list_of_dfs)
        self.assertIsInstance(result, pl.DataFrame)

    def test_empty_list(self):
        "Test the process_fact_table function with an empty list"
        empty_list = []
        result = process_fact_table(empty_list)
        self.assertEqual(result, "Ensure you have a list of just polars dataframes")
    
    def test_invalid_list(self):
        "Test the process_fact_table function with a list containing invalid data"
        invalid_list = ["string", "string", "string",
                         "string", "string", "string", "string"]
        result = process_fact_table(invalid_list)
        self.assertEqual(result, "Ensure you have a list of just polars dataframes")
    
    def test_none_type(self):
        "Test the process_fact_table function with a None type"
        result = process_fact_table(None)
        self.assertEqual(result, "Please provide a list of polars dataframes")
    
    def test_with_string(self):
        "Test the process_fact_table function with a string type"
        result = process_fact_table("string")
        self.assertEqual(result, "Please provide a list of polars dataframes")
    
    def test_with_int(self):
        "Test the process_fact_table function with an integer type"
        result = process_fact_table(123)
        self.assertEqual(result, "Please provide a list of polars dataframes")


class TestGetTopSellers(unittest.TestCase):
    def setUp(self):
        # Sample data for testing
        self.sample_data = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "order_id": ["order1", "order2", "order3", "order4"],
            "customer_id": ["customer1", "customer2", "customer3", "customer4"],
            "order_status": ["status1", "status2", "status3", "status4"],
            "order_purchase_timestamp": [datetime(2021,1,1), datetime(2021,1,2), datetime(2021,1,3), datetime(2021,1,4)],
            "order_approved_at": [datetime(2021,1,1), datetime(2021,1,2), datetime(2021,1,3), datetime(2021,1,4)],
            "order_delivered_carrier_date": [datetime(2021,1,1), datetime(2021,1,2), datetime(2021,1,3), datetime(2021,1,4)],
            "order_delivered_customer_date": [datetime(2021,1,2), datetime(2021,1,3), datetime(2021,1,4), datetime(2021,1,5)],
            "order_estimated_delivery_date": [datetime(2021,1,3), datetime(2021,1,4), datetime(2021,1,5), datetime(2021,1,6)],
            "customer_unique_id": ["unique1", "unique2", "unique3", "unique4"],
            "customer_zip_code_prefix": [1, 2, 3, 4],
            "customer_city": ["city1", "city2", "city3", "city4"],
            "customer_state": ["state1", "state2", "state3", "state4"],
            "order_item_id": [1, 2, 3, 4],
            "product_id": ["prod1", "prod2", "prod3", "prod4"],
            "seller_id": ["seller1", "seller2", "seller3", "seller4"],
            "shipping_limit_date": [datetime(2021,1,1), datetime(2021,1,2), datetime(2021,1,3), datetime(2021,1,4)],
            "price": [1.0, 2.0, 3.0, 4.0],
            "freight_value": [1, 2, 3, 4],
            "product_category_name": ["category1", "category2", "category3", "category4"],
            "seller_zip_code_prefix": [1, 2, 3, 4],
            "seller_city": ["city1", "city2", "city3", "city4"],
            "seller_state": ["state1", "state2", "state3", "state4"],
            "product_category_name_english": ["cat1", "cat2", "cat3", "cat4"]
        })
    
    def test_with_correct_data(self):
        "use the setup data above"
        result = get_top_sellers(self.sample_data)

        print("result_columns", result.columns)

        # Check if the result is a Polars DataFrame
        self.assertIsInstance(result, pl.DataFrame)

        # Check if the dataframe has the correct shape
        self.assertEqual(result.shape, (4, 3))

        # Check if the dataframe has the correct columns
        self.assertListEqual(result.columns, ["id","seller_id", "Total_sales"])

        # Check if the dataframe has the correct values
        expected_df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "seller_id": ["seller1", "seller2", "seller3", "seller4"],
            "Total_sales": [1.0, 2.0, 3.0, 4.0]
        })  

        for col in result.columns:
            self.assertEqual(set(result[col].to_list()), set(expected_df[col].to_list()))

    def test_empty_dataframe(self):
        "Test the get_top_sellers function parameter with an empty DataFrame"
        empty_df = pl.DataFrame()
        result_df = get_top_sellers(empty_df)
        self.assertEqual(result_df, "Dataframe is empty, Provide the valid Fact table")
    
    def test_with_string(self):
        "Test the get_top_sellers function parameter with a string type"

        result = get_top_sellers("string")
        self.assertEqual(result, "Please provide the valid Fact table")

    def test_with_none(self):
        "Test the get_top_sellers function parameter with a None type"

        result = get_top_sellers(None)
        self.assertEqual(result, "Please provide the valid Fact table")

if __name__ == "__main__":
    unittest.main()