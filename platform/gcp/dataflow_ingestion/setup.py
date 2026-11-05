from setuptools import setup
setup(name='dp-kp010-raw-v2',version='0.1.0',packages=['kp010'],package_dir={'kp010':'.'},install_requires=['avro==1.12.0','confluent-kafka[avro]'])
