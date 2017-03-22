# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #

import sys
import logging
import argparse
import json

import h5py
import numpy as np

from influxdb import InfluxDBClient

# --------------------------------------------------------------------------- #
# Functions
# --------------------------------------------------------------------------- #


def check_hdf5_config(cfg, log):
    """ Check HDF5 configuration
    @:param cfg: configuration data
    @:type cfg: dictionary
    @:param log: logger
    @:type log: logger object
    """
    if cfg['compression_type'] not in ['gzip', 'szip', 'None']:
        log.error("Compression type not available (None | gzip | szip)")
        sys.exit(-3)
    if cfg['compression_type'] == 'gzip' and (int(cfg['compression_level']) < 0 or int(cfg['compression_level']) > 8):
        log.error("Compression level not available ([0:8] for gzip")
        sys.exit(-4)
    if cfg['compression_type'] == 'szip' and (int(cfg['compression_level']) < 0 or int(cfg['compression_level']) > 16):
        log.error("Compression level not available ([0:16] for szip")
        sys.exit(-5)
    if float(cfg['chunk_factor']) < 0 or float(cfg['chunk_factor']) > 1:
        log.error("Chunk_factor not in the interval [0:1]")
        sys.exit(-6)


def create_hdf5_ds(hdf5_file,
                   dset_name,
                   tags,
                   columns,
                   data_size,
                   data_chunks,
                   data,
                   compression_type,
                   compression_level):
    """ Check a dataset in a HDF5 file
    @:param hdf5_file: HDF5 file
    @:type hdf5_file: h5py file object
    @:param dset_name: name of the dataset
    @:type dset_name: string
    @:param tags: tags
    @:type tags: dictionary
    @:param columns: columns (fields in InfluxDB)
    @:type columns: dictionary
    @:param data_size: size of the dataset
    @:type data_size: list
    @:param data_chunks: chunks of the dataset
    @:type data_chunks: list
    @:param data: data
    @:type data: NumPy array of array
    @:param compression_type: compression type ( None: no compression | gzip | szip)
    @:type compression_type: string
    @:param compression_level: compression level ([0:8] for gzip | [0:16] for szip)
    @:type compression_level: int
    """
    # Define the dataset dimensions
    dims = (data_size[0], data_size[1])
    space_id = h5py.h5s.create_simple(dims)

    # Set dataset properties
    dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    if compression_type == 'gzip':
        dcpl.set_deflate(compression_level)
    elif compression_type == 'szip':
        dcpl.set_szip(32, compression_level)
    dcpl.set_chunk((data_chunks[0], data_chunks[1]))

    # Create dataset in the file
    dset = h5py.h5d.create(hdf5_file, dset_name, h5py.h5t.IEEE_F64BE, space_id, dcpl)

    # Define the dataset attributes (strings datatype)
    for i in range(0, len(columns)):
        attr_str = columns[i]
        npd_type = '|S' + str(len(attr_str))
        attr_data = np.empty((1,), npd_type)
        attr_data[0] = attr_str

        ft = h5py.h5t.FORTRAN_S1.copy()
        ft.set_size(len(attr_str))

        space = h5py.h5s.create_simple((1,))
        attr = h5py.h5a.create(dset, 'field_%i' % i, ft, space)
        attr.write(attr_data)

    if tags is not None:
        for tag_k in tags:
            attr_str = tags[tag_k]
            npd_type = '|S' + str(len(attr_str))
            attr_data = np.empty((1,), npd_type)
            attr_data[0] = attr_str

            ft = h5py.h5t.FORTRAN_S1.copy()
            ft.set_size(len(attr_str))

            space = h5py.h5s.create_simple((1,))
            attr = h5py.h5a.create(dset, tag_k, ft, space)
            attr.write(attr_data)

    # Write dataset on file
    dset.write(h5py.h5s.ALL, h5py.h5s.ALL, data)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', help='JSON configuration file')

    args = arg_parser.parse_args()
    configs = json.loads(open(args.c).read())
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=None)

    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")

    for config in configs['exporting_parameters']:
        check_hdf5_config(cfg=config, log=logger)

        # --------------------------------------------------------------------------- #
        # InfluxDB connection
        # --------------------------------------------------------------------------- #
        logger.info('InfluxDB connection -> socket=%s:%s; user=%s, db=%s' % (config['host'], config['port'],
                                                                             config['user'], config['db']))
        try:
            influxdb_client = InfluxDBClient(host=config['host'], port=int(config['port']), database=config['db'],
                                             username=config['user'], password=config['password'])
        except Exception as e:
            logger.error("EXCEPTION: " + str(e))
            sys.exit(2)
        logger.info("Connection OK")

        # Send query request to InfluxDB
        logger.info("Query: %s" % config['query'])
        try:
            rs = influxdb_client.query(query=config['query'], epoch=config['epoch'])
        except Exception as e:
            logger.error("EXCEPTION: " + str(e))
            sys.exit(2)

        # HDF5 file creation
        hdf5_file_obj = h5py.h5f.create(config['hdf5_file'])
        # Cycling on the series
        if 'series' in rs.raw.keys():
            logger.info("Creation file %s, compression=%s, level=%s, chunk_factor=%s" % (config['hdf5_file'],
                                                                                         config['compression_type'],
                                                                                         config['compression_level'],
                                                                                         config['chunk_factor']))
            cnt_series = 1
            for elem in rs.raw['series']:

                data_np = np.array(elem['values'], dtype=np.float)
                size = [len(data_np), len(elem['values'][0])]
                # Chunking done only on time dimension
                chunks = [int(len(data_np)*float(config['chunk_factor'])), len(elem['values'][0])]

                dset_desc = ''
                if 'tags' in elem.keys():
                    for tag_key in elem['tags']:
                        dset_desc += elem['tags'][tag_key] + '__'
                    dset_desc = dset_desc[:-2]
                    logger.info("Dataset '%s' creation" % dset_desc)
                    create_hdf5_ds(hdf5_file=hdf5_file_obj,
                                   dset_name=dset_desc,
                                   tags=elem['tags'],
                                   columns=elem['columns'],
                                   data_size=size,
                                   data_chunks=chunks,
                                   data=data_np,
                                   compression_type=config['compression_type'],
                                   compression_level=int(config['compression_level']))
                else:
                    dset_desc = 'dset_%03d' % cnt_series
                    logger.info("Dataset '%s' creation" % dset_desc)
                    create_hdf5_ds(hdf5_file=hdf5_file_obj,
                                   dset_name=dset_desc,
                                   tags=None,
                                   columns=elem['columns'],
                                   data_size=size,
                                   data_chunks=chunks,
                                   data=data_np,
                                   compression_type=config['compression_type'],
                                   compression_level=int(config['compression_level']))
                cnt_series += 1
            # Close the file
            hdf5_file_obj.close()

            # Delete InfluxDB client
            del influxdb_client
        else:
            logger.info('No data found')

    # --------------------------------------------------------------------------- #
    # Exiting program
    # --------------------------------------------------------------------------- #
    logger.info("Exiting program")
