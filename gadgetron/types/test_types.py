import pytest
from .serialization import NDArray
from . import serialization
from .recon_data import SamplingDescription
from .image_array import ImageArray
from .acquisition_bucket import AcquisitionBucket, AcquisitionBucketStats
from ..external import readers
from ..external import writers
import ismrmrd
import numpy as np
import dataclasses
from typing import List, Optional
from io import BytesIO


@dataclasses.dataclass
class TestClass:
    data: NDArray[np.float32]
    mdata: NDArray[np.float64]
    headers: List[ismrmrd.AcquisitionHeader]


def test_dataclass():
    buffer = BytesIO()

    a = TestClass(np.zeros((2, 2), dtype=np.float32), np.ones((1, 1, 3), dtype=np.float64), [])
    serialization.write(buffer, a, TestClass)
    buffer.seek(0)
    b = serialization.read(buffer, TestClass)

    assert np.equal(a.data, b.data, casting='no').all()
    assert np.equal(a.mdata, b.mdata, casting='no').all()
    assert a.headers == b.headers


def test_acquisition():
    data = np.array(np.random.normal(0, 10, size=(12, 128)), dtype=np.complex64)
    traj = np.array(np.random.normal(0, 10, size=(128, 2)), dtype=np.float32)

    a = ismrmrd.Acquisition.from_array(data, traj)

    buffer = BytesIO()

    serialization.write(buffer, a, ismrmrd.Acquisition)
    buffer.seek(0)
    b = serialization.read(buffer, ismrmrd.Acquisition)
    assert a == b


def test_optional():
    a = np.array(np.random.random((1, 2, 3)), dtype=np.complex128)
    buffer = BytesIO()

    serialization.write(buffer, a, Optional[NDArray[np.complex128]])
    buffer.seek(0)
    b = serialization.read(buffer, Optional[NDArray[np.complex128]])

    assert np.equal(a, b, casting='no').all()


def test_samplingdescription():
    a = SamplingDescription()

    buffer = BytesIO()
    serialization.write(buffer, a, SamplingDescription)
    buffer.seek(0)
    b = serialization.read(buffer, SamplingDescription)

    for field in a._fields_:
        assert (np.array(getattr(a,field[0])) == np.array(getattr(b,field[0]))).all()


def test_imagearray():

    a = ImageArray()
    a.acq_headers = np.array([ismrmrd.AcquisitionHeader() for k in range(20)],dtype=np.object)
    buffer = BytesIO()
    serialization.write(buffer,a,ImageArray)
    buffer.seek(0)
    b = serialization.read(buffer,ImageArray)

    assert np.equal(a.data,b.data,casting='no').all()
    assert np.equal(a.headers,b.headers,casting='no').all()
    assert a.meta == b.meta


