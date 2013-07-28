var servers = exports.servers = {
  listen: 3010,
  fetch_lines: 1000,
  query_timeout: null, // seconds. (null:shib will wait query response infinitely).
  setup_queries: [],
  storage: {
    datadir: './var'
  },
  executer: {
    name: 'hiveserver', // or 'hiveserver2' (not implemented) or 'huahinmanager' (not implemented)
    host: '10.132.5.163',
    port: 10003,
    support_database: true,
    default_database: 'nicodata'
  },
  monitor: {
    name : 'huahin_mrv1', // or 'huahin_yarn'
    host: '10.132.5.163',
    port: 9010
  }
};
