
import os
import re
import logging
import requests
import urllib.parse as url

import pickle


class Storage:

    default_serializers = {
        'pickle': pickle.dumps
    }

    default_deserializers = {
        'pickle': pickle.loads
    }

    def __init__(self, address=None, **ids):

        if address is None:
            address = self.get_default_address()

        self.address = address
        self.meta_url = url.urljoin(address, '/v1/data')
        self.data_url = url.urljoin(address, '/v1/blobs')

        self.scanner = GenericSpace(self, 'scanner', ids.get('scanner_id')) if 'scanner_id' in ids else ErrorSpace('Scanner')
        self.session = GenericSpace(self, 'session', ids.get('session_id')) if 'session_id' in ids else ErrorSpace('Session')
        self.measurement = MeasurementSpace(self)

    @classmethod
    def get_default_address(cls):
        if os.environ.get('GADGETRON_STORAGE_ADDRESS') is not None:
            return os.environ.get('GADGETRON_STORAGE_ADDRESS')

        raise RuntimeError("No storage address provided.")

    @classmethod
    def from_connection(cls, connection):
        return Storage(address=_address_from_config(connection.config), **_ids_from_config(connection.config))

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

    def store(self, *, space, subject, key, value, serializer):

        serializer = self.default_serializers.get(serializer, serializer)

        data = {
            'storagespace': space,
            'subject': subject,
            'key': key,
            'storage_duration': '48:00:00'  # 48 hours
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

    def store(self, key, value, *, serializer):
        return self.storage.store(
            space=self.space,
            subject=self.subject,
            key=key,
            value=value,
            serializer=serializer
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

    def store(self, measurement_id, key, value, *, serializer):
        return self.storage.store(
            space='measurement',
            subject=measurement_id,
            key=key,
            value=value,
            serializer=serializer
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


def _address_from_config(config):
    for elm in config.findall('.//gadgetron/storage/address'):
        return elm.text


def _ids_from_config(config):
    ids = {}

    for elm in config.findall('.//gadgetron/storage/spaces/scanner'):
        ids.update(scanner_id=elm.text)

    for elm in config.findall('.//gadgetron/storage/spaces/patient'):
        ids.update(patient_id=elm.text)

    return ids

