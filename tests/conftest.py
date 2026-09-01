# tests/conftest.py
import os
import pytest
import numpy as np
import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import generate_uid
from medical_lakehouse_compaction.spark_session import create_spark_session

# Slice dimensions every synthetic fixture uses. Tests that assert on decoded
# or transferred bytes derive from these rather than hardcoding them: the
# dimensions have changed before, and a hardcoded assertion did not follow.
FIXTURE_ROWS = 32
FIXTURE_COLS = 32

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    session = create_spark_session({"warehouse": warehouse, "endpoint": None})
    yield session
    session.stop()

def make_dicom_slice(series_uid: str, sop_uid: str, instance_number: int,
                     patient_id: str = "TEST001",
                     rows: int = FIXTURE_ROWS, cols: int = FIXTURE_COLS) -> Dataset:
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    ds.file_meta.MediaStorageSOPInstanceUID = sop_uid
    ds.file_meta.TransferSyntaxUID = "1.2.840.10008.1.2.1"
    ds.PatientID = patient_id
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = series_uid
    ds.SOPInstanceUID = sop_uid
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    ds.InstanceNumber = instance_number
    ds.SliceLocation = float(instance_number * 2.5)
    ds.Rows = rows
    ds.Columns = cols
    ds.PixelSpacing = [1.0, 1.0]
    ds.ImagePositionPatient = [0.0, 0.0, instance_number * 2.5]
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 1
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    pixel_array = np.random.randint(-1000, 1000, (rows, cols), dtype=np.int16)
    ds.PixelData = pixel_array.tobytes()
    return ds

@pytest.fixture
def sample_dicom_dir(tmp_path):
    """2 series × 5 slices each — minimal for testing ingestion."""
    series_uids = [generate_uid(), generate_uid()]
    for s_uid in series_uids:
        series_dir = tmp_path / s_uid
        series_dir.mkdir()
        for i in range(5):
            sop_uid = generate_uid()
            ds = make_dicom_slice(s_uid, sop_uid, i)
            pydicom.dcmwrite(str(series_dir / f"slice_{i:03d}.dcm"), ds)
    return tmp_path
