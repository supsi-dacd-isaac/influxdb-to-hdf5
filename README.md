# influxdb-to-hdf5
Script to gather times-series from an InfluxDB server (https://www.influxdata.com/) and save the datasets in HDF5 files (https://www.hdfgroup.org/HDF5/).

**Requirements:**  
* Python>=2.7.11
* numpy>=1.12.0
* h5py>=2.7.0
* influxdb>=4.0.0
* jsonschema>=2.6.0

**Usage:** 
<pre>python bridge.py -c conf/example.json</pre>

**Configuration of a single HDF5 file:** 

<pre>
{
  "exporting_parameters":
  [
    {
      "host": "host.example.com",
      "port": 8086,
      "db": "db_name",
      "user": "username",
      "password": "xxx",
      "epoch": "s",
      "query": "SELECT mean(value) AS mean, sum(flag) AS flag FROM meteo WHERE stand='TSGAB' AND signal='Gglob_hor' AND time>='2017-03-10T12:00:00Z' GROUP BY time(10m), stand, signal",
      "hdf5_file": "hdf5/data_tags.h5",
      "chunk_factor": "0.5",
      "compression_type": "gzip",
      "compression_level": "5"
    }
  ]
}
</pre>

<pre>
host: InfluxDB host
port: InfluxDB port (default 8086)
db: InfluxDB database name
user: InfluxDB user
password: InfluxDB password
query: InfluxQL query (https://docs.influxdata.com/influxdb/v1.2/guides/querying_data)
hdf5_file: path of the hdf5 file
chunk_factor: Chunking factor (ex: dataset length = 1000, chunk_factor=0.5 => dataset stored in HDF5 with two chunks of 500 samples), (chunk_factor = 1.0 => no chunking)
compression_type: None | gzip | szip
compression_level: [0:8] for gzip, [0:16] for szip
</pre>

