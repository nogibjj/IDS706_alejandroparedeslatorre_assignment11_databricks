import os
import pytest
from unittest.mock import patch, MagicMock
from lib.extract_transform_load import ETL

@pytest.fixture(scope="module")
def etl():
    etl_instance = ETL()
    yield etl_instance
    etl_instance.spark.stop()

def test_extract(etl):
    """Test the extract method to ensure data is fetched and loaded into a Spark DataFrame."""
    # Patch the requests.get method to avoid actual API calls
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"name": "Test University", "country": "United States", "alpha_two_code": "US",
             "state-province": "California", "domains": ["test.edu"], "web_pages": ["http://test.edu"]}
        ]
        mock_get.return_value = mock_response

        etl.extract()
        assert etl.df_spark is not None, "Extract failed: df_spark is None."
        assert etl.df_spark.count() > 0, "Extract failed: DataFrame is empty."
        assert "name" in etl.df_spark.columns, "Extract failed: Expected column 'name' not found."

def test_transform(etl):
    """Test the transform method to ensure data transformation is applied correctly."""
    # Run extract first to initialize df_spark with data
    etl.extract()
    
    # Apply transformation
    etl.transform()
    
    # Check columns after transformation
    transformed_columns = ["unique_id", "name", "country", "country_code", "state_province", "domains", "web_pages"]
    for col in transformed_columns:
        assert col in etl.df_spark.columns, f"Transform failed: Column '{col}' not found after transformation."

    # Check if 'unique_id' has been correctly generated
    sample_row = etl.df_spark.select("unique_id", "name", "country").first()
    expected_unique_id = f"{sample_row['name']}_{sample_row['country']}"
    assert sample_row['unique_id'] == expected_unique_id, "Transform failed: 'unique_id' column not created as expected."

def test_spark_transform(etl):
    """Test the transform method to ensure it runs without errors."""
    
    try:
        # Run extract first to initialize df_spark with data
        etl.extract()
        # Apply the transformation
        etl.transform()
        print("Transformation ran successfully.")
    except Exception as e:
        # If there is an error, print the error message
        print(f"Transformation failed: {e}")
        raise


def test_log_output(etl):
    """Test the log_output method to ensure logs are written correctly to the markdown file."""
    log_file = "pyspark_output.md"
    if os.path.exists(log_file):
        os.remove(log_file)  # Remove the file if it exists

    etl.log_output("Test Operation", "This is a test output.")
    
    assert os.path.exists(log_file), "Log output failed: Log file not created."
    with open(log_file, "r") as file:
        content = file.read()
        assert "## Test Operation" in content, "Log output failed: Operation title not found in log."
        assert "This is a test output." in content, "Log output failed: Output message not found in log."

if __name__ == "__main__":
    pytest.main()
