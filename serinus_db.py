import os
import django
from datetime import datetime
from django.utils.timezone import make_aware
from uuid import uuid4

class SerinusDB:

    # TODO: You must add path of adapter root either here or via PATH to virtualenv
    # ADAPTER_DIR = '''FULL_PATH_TO\\enerc_serinus_adapter'''

    def __init__(self):
        #    sys.path.append(ADAPTER_DIR)
        self.id_pair = dict()

        # Setting up the environment to use the serinus adapter settings
        os.environ['DJANGO_SETTINGS_MODULE'] = 'enerc_serinus_adapter.settings'
        django.setup()

        # Attempt to import the model from enerc serinus adapter
        from serinus.models import \
            TemperatureRecord, HumidityRecord, NoiseRecord, \
            LightRecord, CO2Record, MovementRecord, Sensor, MetaData, SensorConfig

        assert hasattr(TemperatureRecord, 'objects')
        assert hasattr(CO2Record, 'objects')
        assert hasattr(MovementRecord, 'objects')
        assert hasattr(NoiseRecord, 'objects')
        assert hasattr(LightRecord, 'objects')
        assert hasattr(HumidityRecord, 'objects')
        assert hasattr(SensorConfig, 'objects')

        # saving the Record objects to a class-local variable
        self.TemperatureRecord = TemperatureRecord
        self.HumidityRecord = HumidityRecord
        self.NoiseRecord = NoiseRecord
        self.LightRecord = LightRecord
        self.CO2Record = CO2Record
        self.MovementRecord = MovementRecord

        # abstract models
        self.Sensor = Sensor
        self.MetaData = MetaData

        # populate the oid:ipv4 key pair dict
        self.SensorConfig = SensorConfig
        sensors = self.SensorConfig.objects.all()

        for sensor in sensors:
            self.id_pair[sensor.origin_id] = sensor.vicinity_oid

        print('sensors: %d' % len(self.id_pair))

    def save(self, record):
        sensor_type = record['Sensor']
        rssi = record['RSSI']
        voltage = record['voltage']
        sensor = self.Sensor(rssi=rssi, voltage=voltage, sensor_type=sensor_type)

        system_id = record['System ID']
        origin_id = record['Origin ID']
        origin_network_level = record['Origin Network Level']
        packet_type = record['packet type']
        hop_counter = record['Hop Counter']
        gateway_mac = record['GW_MAC']
        latency_counter = record['Latency counter']

        software_version = record['software ver']
        hardware_version = record['Hardware version']
        message_counter = record['Message counter']

        # create a new uuid4 if no entry in the key:pair dict
        # update DB and Dict.
        if origin_id not in self.id_pair:
            vicinity_oid = uuid4()
            self.id_pair[origin_id] = vicinity_oid
            s = self.SensorConfig(origin_id=origin_id, vicinity_oid=vicinity_oid)
            s.save()

        vicinity_oid = self.id_pair[origin_id]

        meta_data = self.MetaData(
            software_version=software_version,
            hardware_version=hardware_version,
            message_counter=message_counter,
            system_id=system_id,
            origin_id=origin_id,
            origin_network_level=origin_network_level,
            packet_type=packet_type,
            hop_counter=hop_counter,
            gateway_mac=gateway_mac,
            latency_counter=latency_counter,
            vicinity_oid=vicinity_oid
        )

        value = record['Value']

        datetime_obj = make_aware(datetime.fromtimestamp(record['Timestamp']))
        timestamp = datetime_obj

        if sensor_type == 'CO2':
            r = self.CO2Record(sensor=sensor, meta_data=meta_data, value=value, timestamp=timestamp)
        elif sensor_type == 'Movement':
            r = self.MovementRecord(sensor=sensor, meta_data=meta_data, value=value, timestamp=timestamp)
        elif sensor_type == 'Humidity':
            r = self.HumidityRecord(sensor=sensor, meta_data=meta_data, value=value, timestamp=timestamp)
        elif sensor_type == 'Temperature':
            r = self.TemperatureRecord(sensor=sensor, meta_data=meta_data, value=value, timestamp=timestamp)
        elif sensor_type == 'Light':
            r = self.LightRecord(sensor=sensor, meta_data=meta_data, value=value, timestamp=timestamp)
        elif sensor_type == 'Noise Ai1':
            r = self.NoiseRecord(sensor=sensor, meta_data=meta_data, value=value, timestamp=timestamp)
        else:
            print('Unsupported sensor type: \'%s\'' % sensor_type)
            return

        r.save()
