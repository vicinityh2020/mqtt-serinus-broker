import os
import django


class SerinusDB:

    # TODO: You must add path of adapter root either here or via PATH to virtualenv
    # ADAPTER_DIR = '''FULL_PATH_TO\\enerc_serinus_adapter'''

    def __init__(self):
        #    sys.path.append(ADAPTER_DIR)

        # Setting up the environment to use the serinus adapter settings
        os.environ['DJANGO_SETTINGS_MODULE'] = 'enerc_serinus_adapter.settings'
        django.setup()

        # Attempt to import the model from enerc serinus adapter
        from serinus.models import DecimalRecord, CO2Record, MovementRecord, Sensor, MetaData
        assert hasattr(DecimalRecord, 'objects')
        assert hasattr(CO2Record, 'objects')
        assert hasattr(MovementRecord, 'objects')

        # saving the Record object to a class-local variable
        self.DecimalRecord = DecimalRecord
        self.CO2Record = CO2Record
        self.MovementRecord = MovementRecord
        self.Sensor = Sensor
        self.MetaData = MetaData

    def save(self, record):
        sensor_type = record['Sensor']
        rssi = record['RSSI']
        voltage = record['voltage']

        sv = record['software ver']
        mc = record['Message counter']

        value = record['Value']

        meta_data = self.MetaData(software_version=sv, message_counter=mc)
        sensor = self.Sensor(rssi=rssi, voltage=voltage, sensor_type=sensor_type)

        if sensor_type == 'CO2':
            r = self.CO2Record(sensor=sensor, meta_data=meta_data, value=value)
            r.save()
        elif sensor_type == 'Movement':
            r = self.MovementRecord(sensor=sensor, meta_data=meta_data, value=value)
            r.save()
        else:
            r = self.DecimalRecord(sensor=sensor, meta_data=meta_data, value=value)
            r.save()
