module.exports = {
  apps: [{
    name: 'zkmessage-queue',
    script: 'server.js',
    exec_mode: 'cluster',
    instances: '4',
    instance_var: 'INSTANCE_ID',
    env: {
      NODE_ENV: 'test'
    }
  }]
};