file = open("commands.sql","w") 

topic='log-input'

root_stream_name = 'log_stream'
create_stream = 'create stream {} (\"$table\" VARCHAR, type VARCHAR, tenantid VARCHAR, sourcesystem VARCHAR, mg VARCHAR, managementgroupname VARCHAR, timegenerated VARCHAR, computer VARCHAR, rawdata VARCHAR, resulttype VARCHAR, resultdescription VARCHAR, resourceid VARCHAR, operationname VARCHAR, category VARCHAR, level VARCHAR, durationms VARCHAR, properties_s STRUCT<deviceId VARCHAR, protocol VARCHAR, authType VARCHAR, maskedIpAddress VARCHAR, statusCode VARCHAR>, location_s VARCHAR, subscriptionid VARCHAR, resourcegroup VARCHAR, resourceprovider VARCHAR, resource VARCHAR, resourcetype VARCHAR, _resourceid VARCHAR, operationversion VARCHAR, resultsignature VARCHAR, calleripaddress VARCHAR, correlationid VARCHAR, metricname VARCHAR, total VARCHAR, count VARCHAR, maximum VARCHAR, minimum VARCHAR, average VARCHAR, timegrain VARCHAR, unitname VARCHAR, remoteipcountry VARCHAR, remoteiplatitude VARCHAR, remoteiplongitude VARCHAR, maliciousip VARCHAR, indicatorthreattype VARCHAR, description VARCHAR, tlplevel VARCHAR, confidence VARCHAR, severity VARCHAR, firstreporteddate VARCHAR, lastreporteddatetime VARCHAR, isactive VARCHAR, reportreferencelink VARCHAR, additionalinformation VARCHAR) WITH (KAFKA_TOPIC=\'{}\', VALUE_FORMAT=\'JSON\', PARTITIONS=1, REPLICAS=1);\n'.format(root_stream_name, topic)
file.write(create_stream)

filer_stream_name='log_stream_filtered'
filter_stream='create stream {} as select timegenerated, properties_s->deviceid, properties_s->maskedIpAddress, properties_s->protocol, properties_s->authtype, operationname from {};\n'.format(filer_stream_name, root_stream_name)
file.write(filter_stream)

keyed_stream_name='log_stream_keyed'
keyed_stream='create stream {} as select * FROM {} PARTITION BY properties_s__deviceid;\n'.format(keyed_stream_name, filer_stream_name)
file.write(keyed_stream)

device_connect_name='log_stream_device_connects'
device_connect='create stream {} as select *, 1 as KEYCOL from {} where operationname=\'deviceConnect\';\n'.format(device_connect_name, keyed_stream_name)
file.write(device_connect)

device_count_table_name='log_table_device_count'
device_count_table='create table {} as select PROPERTIES_S__DEVICEID, OPERATIONNAME, KEYCOL, COUNT(*) AS COUNT FROM {} window tumbling (size 30 seconds) group by PROPERTIES_S__DEVICEID, OPERATIONNAME, KEYCOL;\n'.format(device_count_table_name, device_connect_name)
file.write(device_count_table)

device_count_stream_name='log_stream_device_count'
device_count_stream='create stream {} (PROPERTIES_S__DEVICEID varchar, OPERATIONNAME varchar, KEYCOL int, COUNT BIGINT) with (kafka_topic=\'{}\', value_format=\'JSON\', partitions=1, replicas=1);\n'.format(device_count_stream_name, device_count_table_name.upper())
file.write(device_count_stream)

file.close() 
