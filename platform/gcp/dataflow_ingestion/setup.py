from setuptools import setup
setup(name='dp-payments-raw-v1',version='0.1.0',packages=['dataflow_ingestion'],package_dir={'dataflow_ingestion':'.'},install_requires=['avro==1.12.0','confluent-kafka[avro]'])
