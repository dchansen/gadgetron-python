
import re
import requests
import urllib.parse as url

import pickle
import datetime
from typing import Union

class Storage:
    """ Access the Gadgetron Storage Server for persistent data storage.

    Configure storage access by setting the GADGETRON_STORAGE_SERVER environment variable,
    or by supplying an address when you call `gadgetron.external.listen`. Gadgetron will
    ensure that the address is configured correctly when it starts external gadgets.
    """

    default_serializers = {
        'pickle': pickle.dumps
    }

    default_deserializers = {
        'pickle': pickle.loads
    }

    def __init__(self, address, **ids):

        self.address = address
        self.meta_url = url.urljoin(address, '/v1/data')
        self.data_url = url.urljoin(address, '/v1/blobs')

        self.scanner = GenericSpace(self, 'scanner', ids.get('scanner_id')) if 'scanner_id' in ids else ErrorSpace('Scanner')
        self.session = GenericSpace(self, 'session', ids.get('session_id')) if 'session_id' in ids else ErrorSpace('Session')
        self.measurement = MeasurementSpace(self)

    def fetch(self, *, space, subject, key, deserializer):

        deserializer = self.default_deserializers.get(deserializer, deserializer)

        data = {
            'storagespace': space,
            'subject': subject,
            'key': key
        }

        response = requests.get(self.meta_url, json=data)
        response.raise_for_status()

        return Iterable(self, deserializer, response.json())

    def store(self, *, space, subject, key, value, serializer, storage_duration=datetime.timedelta(hours=48)):

        serializer = self.default_serializers.get(serializer, serializer)

        data = {
            'storagespace': space,
            'subject': subject,
            'key': key,
            'storage_duration': duration_to_string(storage_duration)
        }

        response = requests.post(self.meta_url, json=data)
        response.raise_for_status()

        storage_path = response.json().get('storage_path')

        response = requests.patch(url.urljoin(self.address, storage_path), data=serializer(value))
        response.raise_for_status()


class GenericSpace:

    def __init__(self, storage, space, subject):
        self.storage = storage
        self.subject = subject
        self.space = space

    def fetch(self, key, *, deserializer):
        return self.storage.fetch(
            space=self.space,
            subject=self.subject,
            key=key,
            deserializer=deserializer
        )

    def store(self, key, value, *, serializer, storage_duration=datetime.timedelta(hours=48)):
        return self.storage.store(
            space=self.space,
            subject=self.subject,
            key=key,
            value=value,
            serializer=serializer,
            storage_duration=storage_duration
        )


class MeasurementSpace:

    def __init__(self, storage):
        self.storage = storage

    def fetch(self, measurement_id, key, *, deserializer):
        return self.storage.fetch(
            space='measurement',
            subject=measurement_id,
            key=key,
            deserializer=deserializer
        )

    def store(self, measurement_id, key, value, *, serializer, storage_duration=datetime.timedelta(hours=48)):
        return self.storage.store(
            space='measurement',
            subject=measurement_id,
            key=key,
            value=value,
            serializer=serializer,
            storage_duration=storage_duration
        )


class ErrorSpace:

    def __init__(self, descriptor):
        self.message = f"{descriptor} Storage Space is unavailable - no appropriate id provided."

    def fetch(self, *_, **__):
        raise RuntimeError(self.message)

    def store(self, *_, **__):
        raise RuntimeError(self.message)


class Iterable:

    def __init__(self, storage, deserializer, blobs):
        self.deserializer = deserializer
        self.storage = storage
        self.blobs = blobs

    def __getitem__(self, idx):
        response = requests.get(url.urljoin(self.storage.address, self.blobs[idx].get('storage_path')))
        response.raise_for_status()

        return self.deserializer(response.content)

    def __len__(self):
        return len(self.blobs)


def ids_from_header(header):

    _measurement_id_pattern = re.compile(r"(?P<scanner>.*?)_(?P<patient>.*?)_(?P<study>.*?)_(.*)")

    def parse_measurement_id(key):
        m = re.match(_measurement_id_pattern, header.measurementInformation.measurementID)
        if m:
            return m[key]

    def patient_id():
        if header.subjectInformation:
            if header.subjectInformation.patientID:
                return header.subjectInformation.patientID
        return parse_measurement_id('patient')

    def study_id():
        if header.studyInformation:
            if header.studyInformation.studyID:
                return header.studyInformation.studyID
        return parse_measurement_id('study')

    def session_id():
        return '/'.join([patient_id(), study_id()])

    def scanner_id():
        if header.acquisitionSystemInformation:
            if header.acquisitionSystemInformation.stationName:
                return header.acquisitionSystemInformation.stationName
        return parse_measurement_id('scanner')

    return {
        'session_id': session_id(),
        'scanner_id': scanner_id()
    }


def duration_to_string(duration: Union[datetime.timedelta,float] ):
    if type(duration) is not  datetime.timedelta:
        duration = datetime.timedelta(seconds=duration)

    hours = duration.days*24+ duration.seconds//(60*60)
    remaining_seconds = duration.seconds - (duration.seconds//(60*60))*60*60
    minutes = remaining_seconds//(60)
    seconds = float(remaining_seconds -  minutes*60) + duration.microseconds*1e-6

    return f"{hours}:{minutes}:{seconds}"
       



