var servers = exports.servers = {
  listen: 3000,
  fetch_lines: 1000,
  query_timeout: null, // seconds. (null:shib will wait query response infinitely).
  setup_queries: [],
  storage: {
    datadir: './var'
  },
  executer: {
    name: 'hiveserver', // or 'hiveserver2' (not implemented) or 'huahinmanager' (not implemented)
    host: 'localhost',
    port: 10000,
    support_database: true,
    default_database: 'default'
  },
  monitor: null
  /*
  monitor: {
    name : 'huahin_mrv1', // or 'huahin_yarn'
    host: 'localhost',
    port: 9010
  }
  */
};
