"""Tests for DICOM reader — pydicom Dataset to Iceberg row dict conversion."""
import json
import numpy as np
import pytest
from pydicom.uid import generate_uid
from tests.conftest import make_dicom_slice
from medical_lakehouse_compaction.ingestion.dicom_reader import dicom_to_row


@pytest.fixture
def sample_ds():
    return make_dicom_slice(
        series_uid="1.2.3.4.5",
        sop_uid="1.2.3.4.5.1",
        instance_number=3,
        rows=32, cols=32,
    )


def test_required_fields_present(sample_ds):
    row = dicom_to_row(sample_ds, "/data/slice.dcm")
    for field in ["series_instance_uid", "sop_instance_uid", "patient_id",
                  "study_instance_uid", "instance_number", "slice_location",
                  "rows", "columns", "pixel_spacing", "image_position_patient",
                  "pixel_data", "source_path"]:
        assert field in row, f"missing field: {field}"


def test_pixel_data_roundtrips(sample_ds):
    row = dicom_to_row(sample_ds, "/data/slice.dcm")
    original = sample_ds.pixel_array.astype(np.int16)
    recovered = np.frombuffer(row["pixel_data"], dtype=np.int16).reshape(32, 32)
    np.testing.assert_array_equal(original, recovered)


def test_pixel_spacing_is_json(sample_ds):
    row = dicom_to_row(sample_ds, "/data/slice.dcm")
    spacing = json.loads(row["pixel_spacing"])
    assert len(spacing) == 2
    assert all(isinstance(v, float) for v in spacing)


def test_instance_number_is_int(sample_ds):
    row = dicom_to_row(sample_ds, "/data/slice.dcm")
    assert row["instance_number"] == 3
    assert isinstance(row["instance_number"], int)


def test_source_path_preserved(sample_ds):
    row = dicom_to_row(sample_ds, "/data/slice.dcm")
    assert row["source_path"] == "/data/slice.dcm"


def test_slice_location_missing_falls_back_to_instance_number(sample_ds):
    del sample_ds.SliceLocation
    row = dicom_to_row(sample_ds, "/data/slice.dcm")
    assert row["slice_location"] == 3.0


def test_slice_location_present_but_empty_falls_back(sample_ds):
    # Real TCIA files can carry the tag with an empty value → pydicom None
    sample_ds.SliceLocation = None
    row = dicom_to_row(sample_ds, "/data/slice.dcm")
    assert row["slice_location"] == 3.0
