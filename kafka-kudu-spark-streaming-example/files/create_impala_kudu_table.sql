CREATE EXTERNAL TABLE `traffic_conditions` STORED AS KUDU
TBLPROPERTIES(
    'kudu.table_name' = 'traffic_conditions',
    'kudu.master_addresses' = '<kudu master fqdn>:7051')
