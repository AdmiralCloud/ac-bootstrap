/**
 * Takes the object that has .log and .config available and add .redis functions to it.
 * 
 */

const async = require('async')
const _ = require('lodash') 
const Redis = require('ioredis')

module.exports = (acapi, options, cb) => {
  const headlineLength = _.get(acapi.config, 'headlineLength', 60)
  const padLength = _.get(acapi.config, 'padLength', 25)
  const bootstrapping = _.get(options, 'bootstrapping', true)

  acapi.log.info('')
  acapi.log.info(_.pad(' REDIS ', headlineLength, '*'))
  
  // init multiple instances for different purposes
  acapi.redis = {}
  let lastInvoked = 0
  async.eachSeries(_.get(acapi.config, 'redis.databases'), (database, itDone) => {
    if (_.get(database, 'ignoreBootstrap')) return itDone()
    let name = _.get(database, 'name')
    let server = _.find(acapi.config.redis.servers, { server: _.get(database, 'server') })
    if (!server) return itDone({ message: 'serverConfigurationMissingForRedis' })

    let redisBaseOptions = {
      host: _.get(server, 'host'),
      port: acapi.config.localRedis ? 6379 : _.get(server, 'port'),
      db: _.get(database, 'db')
    }
    if (_.get(acapi.config, 'redis.retryStrategy')) _.set(redisBaseOptions, 'retry_strategy', _.get(acapi.config, 'redis.retryStrategy'))
    acapi.redis[name] = new Redis(redisBaseOptions)

    acapi.redis[name].on('error', (err) => {
      if (lastInvoked === 0 || lastInvoked < new Date().getTime()) {
        acapi.log.error('REDIS Problem for %s - %s', name, err)
        lastInvoked = new Date().getTime() + _.get(acapi.config, 'redis.errorInterval', 5000) // 5000 = interval in ms
      }
    })

    acapi.redis[name]._readyCheck((err) => {
      if (err) {
        acapi.log.error('Bootstrap.initRedis:ready failed for %s with %s', name, err)
        if (bootstrapping) return itDone(err)
      }

      acapi.log.info('%s: %s', _.padEnd('Name', padLength), name)
      acapi.log.info('%s: %s %s', _.padEnd('Host/Port', padLength), redisBaseOptions.host, redisBaseOptions.port)
      acapi.log.info('%s: %s', _.padEnd('DB', padLength), database.db.toString())
      acapi.log.info('%s: %s', _.padEnd('Connection', padLength), '\x1b[32mSuccessful\x1b[0m')
      if (acapi.config.environment !== 'test') return itDone()

      // better debugging in testmode
      acapi.redis[name].on('connect', () => {
        acapi.log.debug('Connected to Redis %s', name)
      })

      acapi.redis[name].on('close', () => {
        acapi.log.debug('Closed connected to Redis %s', name)
      })

      // flush redis in testmode
      acapi.redis[name].flushdb((err) => {
        acapi.log.info('%s: %s', _.padEnd('Flushed', padLength), '\x1b[32mSuccessful\x1b[0m')
        return itDone(err)
      })
    })
  }, (err) => {
    if (bootstrapping) return cb(err)
    if (err) acapi.log.error('Bootstrap.initRedis:failed with %j', err)
    process.exit(0)
  })

}