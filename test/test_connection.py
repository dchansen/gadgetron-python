
import socket
from ismrmrd.xsd import ismrmrdHeader
import ismrmrd
import gadgetron
import numpy as np
from gadgetron.external import Connection
import concurrent.futures as cf
import xml.etree.ElementTree as xml
import random_data as rd

def client(socket, testdata):
    config = xml.fromstring("<_/>")
    header = ismrmrd.xsd.CreateFromDocument(sample_header)
    with Connection.initiate_connection(socket,config,header) as connection:
        connection.send(testdata)
        connection.close()
        id,item = next(connection)

    assert item == testdata

def parrot_server(socket):
    with Connection(socket) as connection:
        for item in connection:
            connection.send(item)

def run_connection_test(testdata):
    sock1, sock2 = socket.socketpair()
    sock1.setblocking(True)
    sock2.setblocking(True)
    with cf.ProcessPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(client, sock1,testdata)
        future2 = executor.submit(parrot_server, sock2)
        future1.result()
        future2.result()

def test_acquisitions():
    acq = rd.create_random_acquisition()
    run_connection_test(acq)

def test_images():
    img = rd.create_random_image()
    run_connection_test(img)

def test_waveforms():
    wav = rd.create_random_waveform()
    run_connection_test(wav)



sample_header = '''<?xml version="1.0"?>
<ismrmrdHeader xmlns="http://www.ismrm.org/ISMRMRD" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema" xsi:schemaLocation="http://www.ismrm.org/ISMRMRD ismrmrd.xsd">
  <subjectInformation>
    <patientName>phantom</patientName>
    <patientWeight_kg>70.3068</patientWeight_kg>
  </subjectInformation>
  <acquisitionSystemInformation>
    <systemVendor>SIEMENS</systemVendor>
    <systemModel>Avanto</systemModel>
    <systemFieldStrength_T>1.494</systemFieldStrength_T>
    <receiverChannels>32</receiverChannels>
    <relativeReceiverNoiseBandwidth>0.79</relativeReceiverNoiseBandwidth>
  </acquisitionSystemInformation>
  <experimentalConditions>
    <H1resonanceFrequency_Hz>63642459</H1resonanceFrequency_Hz>
  </experimentalConditions>
  <encoding>
    <trajectory>cartesian</trajectory>
    <encodedSpace>
      <matrixSize>
        <x>256</x>
        <y>140</y>
        <z>80</z>
      </matrixSize>
      <fieldOfView_mm>
        <x>600</x>
        <y>328.153125</y>
        <z>160</z>
      </fieldOfView_mm>
    </encodedSpace>
    <reconSpace>
      <matrixSize>
        <x>128</x>
        <y>116</y>
        <z>64</z>
      </matrixSize>
      <fieldOfView_mm>
        <x>300</x>
        <y>271.875</y>
        <z>128</z>
      </fieldOfView_mm>
    </reconSpace>
    <encodingLimits>
      <kspace_encoding_step_1>
        <minimum>0</minimum>
        <maximum>83</maximum>
        <center>28</center>
      </kspace_encoding_step_1>
      <kspace_encoding_step_2>
        <minimum>0</minimum>
        <maximum>45</maximum>
        <center>20</center>
      </kspace_encoding_step_2>
      <slice>
        <minimum>0</minimum>
        <maximum>0</maximum>
        <center>0</center>
      </slice>
      <set>
        <minimum>0</minimum>
        <maximum>0</maximum>
        <center>0</center>
      </set>
    </encodingLimits>
    <parallelImaging>
    <accelerationFactor>
      <kspace_encoding_step_1>1</kspace_encoding_step_1>
      <kspace_encoding_step_2>1</kspace_encoding_step_2>
    </accelerationFactor>
    <calibrationMode>other</calibrationMode>
  </parallelImaging>
  </encoding>
  
  <sequenceParameters>
    <TR>4.6</TR>
    <TE>2.35</TE>
    <TI>300</TI>
  </sequenceParameters>
</ismrmrdHeader>'''

