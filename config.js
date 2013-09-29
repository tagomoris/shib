var servers = exports.servers = {
    listen: 3018,
    fetch_lines: 1000,
    query_timeout: null, // seconds. (null:shib will wait query response infinitely).
    setup_queries: [],
    enviroment:"dev",
    storage: {
	datadir: './var'
    },
    executer: {
	name: 'hiveserver', // or 'hiveserver2' (not implemented) or 'huahinmanager' (not implemented)
	host: 'ec2-54-250-26-144.ap-northeast-1.compute.amazonaws.com',
	port: 10004,
	support_database: true,
	default_database: 'nicodata'
    },
    monitor: {
	name : 'huahin_mrv1', // or 'huahin_yarn'
	host: 'ec2-54-250-26-144.ap-northeast-1.compute.amazonaws.com',
	port: 9010
    },
};
