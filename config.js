var servers = exports.servers = {
  storage: {
    datadir: './var'
  },
  hiveserver: {
    version: 1, // or 2
    host: 'localhost',
    port: 10000,
    support_database: true,
    default_database: 'default',
    setup_queries: []
  },
  huahinmanager: {
    enable: true,
    host: 'localhost',
    port: 9010,
    mapreduce: 'MRv1' // or 'YARN'
  }
};
