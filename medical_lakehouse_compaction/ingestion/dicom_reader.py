"""DICOM reader — convert pydicom Dataset to Iceberg row dict."""
import json
import numpy as np
import pydicom
from pydicom.dataset import Dataset


def dicom_to_row(ds: Dataset, source_path: str) -> dict:
    """Convert pydicom Dataset to a dict suitable for Iceberg ingestion.

    Args:
        ds: pydicom Dataset object.
        source_path: Source file path (preserved in the row).

    Returns:
        Dictionary with standardized field names and serialized values.
    """
    pixel_array = ds.pixel_array.astype(np.int16)

    # SliceLocation may be absent OR present-but-empty (None) in real TCIA
    # data; getattr's default only covers the absent case.
    slice_location = getattr(ds, "SliceLocation", None)
    if slice_location is None:
        slice_location = ds.InstanceNumber

    return {
        "series_instance_uid": str(ds.SeriesInstanceUID),
        "sop_instance_uid": str(ds.SOPInstanceUID),
        "patient_id": str(ds.PatientID),
        "study_instance_uid": str(ds.StudyInstanceUID),
        "instance_number": int(ds.InstanceNumber),
        "slice_location": float(slice_location),
        "rows": int(ds.Rows),
        "columns": int(ds.Columns),
        "pixel_spacing": json.dumps([float(v) for v in ds.PixelSpacing]),
        "image_position_patient": json.dumps([float(v) for v in ds.ImagePositionPatient]),
        "pixel_data": pixel_array.tobytes(),
        "source_path": source_path,
    }


def read_dicom_file(path: str) -> tuple[Dataset, str]:
    """Read a DICOM file and return the Dataset with its path.

    Args:
        path: Path to the DICOM file.

    Returns:
        Tuple of (pydicom Dataset, source path).
    """
    return pydicom.dcmread(path), path
