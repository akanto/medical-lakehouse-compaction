# tests/test_spark_session.py
def test_spark_session_local_mode(spark):
    assert spark is not None
    assert spark.conf.get("spark.sql.catalog.dicom.type") == "hadoop"
