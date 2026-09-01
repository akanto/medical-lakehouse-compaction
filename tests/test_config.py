from medical_lakehouse_compaction.config import load_profile


def test_load_dev_profile():
    cfg = load_profile("conf/profiles/dev.yaml")
    assert cfg["warehouse"] == "s3a://dicom-lakehouse/warehouse"
    assert cfg["endpoint"] == "http://localhost:9000"
    assert cfg["n_series"] == 10


def test_profile_has_required_keys():
    cfg = load_profile("conf/profiles/dev.yaml")
    for key in ["warehouse", "endpoint", "access_key", "secret_key", "n_series"]:
        assert key in cfg, f"missing key: {key}"
