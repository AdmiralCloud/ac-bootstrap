const Queue = require('bull')
const _ = require('lodash')
const Redis = require('ioredis')
const async = require('async')

const functionName = _.padEnd('AC-Bull', 10)

module.exports = function(acapi) {

  const scope = (params) => {
    _.set(acapi.config, 'bull.redis.database.name', _.get(params, 'redis.config', 'jobProcessing'))
  }

    /**
   * Ingests the job list and return the queue name for the environment. ALways use when preparing/using the name.
   * @param jobList STRING name of the list
   */

  const prepareQueue =  (params) => {
    const jobList = _.get(params, 'jobList')
    const configPath = _.get(params, 'configPath', 'bull')
    const jobListConfig = _.find(_.get(acapi.config, configPath + '.jobLists'), { jobList }) 
    if (!jobListConfig) return false

    const queueName = _.get(params, 'customJobList.environment', (acapi.config.environment + (acapi.config.localDevelopment ? acapi.config.localDevelopment : ''))) + '.' + jobList
    return { queueName, jobListConfig }
  }

  const init = function(params, cb) {
    acapi.aclog.headline({ headline: 'bull' })

    // prepare some vars for this scope of this module
    this.scope(params)

    const redisServer = _.find(acapi.config.redis.servers, { server: _.get(params, 'redis.server', 'jobProcessing') })
    const redisConfig = _.find(acapi.config.redis.databases, { name: _.get(acapi.config, 'bull.redis.database.name') })
    const port = acapi.config.localRedis ? 6379 : _.get(redisServer, 'port')

    let redisConf = {
      host: _.get(redisServer, 'host', 'localhost'),
      port,
      db: _.get(redisConfig, 'db', 3)
    }

    acapi.aclog.serverInfo(redisConfig)

    const opts = {
      createClient: (type) => {
        switch (type) {
          case 'client':
            return new Redis(redisConf)
          case 'subscriber':
            return new Redis(redisConf)
          default:
            return new Redis(redisConf)
        }
      }
    }

    // create a bull instance for every jobList, to allow concurrency
    _.forEach(_.get(params, 'jobLists'), jobList => {
      const { queueName } = this.prepareQueue(jobList)
      acapi.aclog.listing({ field: 'Queue', value: queueName })

      acapi.bull[queueName] = new Queue(queueName, opts)
      if (_.get(params, 'activateListeners')) {
        if (_.get(jobList, 'listening')) {
          // this job's listener is on this API
          acapi.bull[queueName].on('global:completed', _.get(params, 'handlers.global:completed')[_.get(jobList, 'jobList')])
          acapi.bull[queueName].on('global:failed', this.handleFailedJobs.bind(this, queueName))  
          acapi.aclog.listing({ field: '', value: 'Listener activated' })
        }
        if (_.get(jobList, 'worker')) {
          // this job's worker is on this API (BatchProcessCollector[jobList])
          _.get(params, 'worker')[_.get(jobList, 'jobList')]()
          acapi.aclog.listing({ field: '', value: 'Worker activated' })
        }
        if (_.get(jobList, 'autoClean')) {
          acapi.bull[queueName].clean(_.get(jobList, 'autoClean', _.get(acapi.config, 'bull.autoClean')))
        }
      }
    })
    return cb()
  }

  const handleFailedJobs = (jobList, jobId, err) => {
    const functionIdentifier = _.padEnd(jobList, _.get(acapi.config, 'bull.log.functionIdentifierLength'))
    acapi.log.error('%s | %s | # %s | Job Failed %j', functionName, functionIdentifier, jobId, err)
  }

  /**
   * Adds a job to a given bull queue
   *
   * @param jobList STRING The jobList to use (bull queue)
   * @param params OBJ Job Parameters
   * @param params.addToWatchList BOOL If true (default) add key to customer watch list
   *
   */

 const addJob = function(jobList, params, cb) {
    const { queueName } = this.prepareQueue({ jobList, configPath: _.get(params, 'configPath'), customJobList: _.get(params, 'customJobList') })
    if (!queueName) return cb({ message: 'jobListNotDefined', additionalInfo: { jobList } })
    const functionIdentifier = _.padEnd('addJob', _.get(acapi.config, 'bull.log.functionIdentifierLength'))

    const name = _.get(params, 'name') // named job
    const jobPayload = _.get(params, 'jobPayload')
    const jobOptions = _.get(params, 'jobOptions', {})
    if (_.get(jobPayload, 'jobId')) {
      _.set(jobOptions, 'jobId', _.get(jobPayload, 'jobId'))
    }

    const identifier = _.get(params, 'identifier') // e.g. customerId
    const identifierId = _.get(jobPayload, identifier)
    if (!identifierId) {
      acapi.log.warn('%s | %s | Job has no identifier %j', functionName, functionIdentifier, params)    
    }
    const addToWatchList = _.get(acapi.config, 'bull.jobListWatchKey') && _.get(params, 'addToWatchList', true)
    const watchKeyParts = []
    if (acapi.config.localDevelopment) watchKeyParts.push(acapi.config.localDevelopment)
    if (identifierId) watchKeyParts.push(identifierId)
    const jobListWatchKey = _.get(acapi.config, 'bull.jobListWatchKey') + _.join(watchKeyParts, ':')
    
    if (!acapi.bull[queueName]) return cb({ message: 'bullNotAvailableForQueueName', additionalInfo: { queueName } })
    //acapi.log.error('195 %j %j %j %j %j', queueName, name, jobPayload, jobOptions, addToWatchList)

    let jobId
    async.series({
      addJob: (done) => {
        if (name) {
          acapi.bull[queueName].add(name, jobPayload, jobOptions).then(job => {
            jobId = _.get(job, 'id')
            return done()
          }).catch(err => {
            acapi.log.error('% s | Name %s | Adding job failed %j', queueName, name, err)
          })
        }
        else {
          acapi.bull[queueName].add(jobPayload, jobOptions).then(job => {
            jobId = _.get(job, 'id')
            return done()
          }).catch(err => {
            acapi.log.error('% s | Adding job failed %j', queueName, err)
          })
        }
      },
      addKeyToWatchList: (done) => {
        if (!addToWatchList || !_.isObject(acapi.redis[_.get(acapi.config, 'bull.redis.database.name')])) return done()
        const redisKey = acapi.config.environment + jobListWatchKey
        acapi.redis[_.get(acapi.config, 'bull.redis.database.name')].hset(redisKey, jobId, queueName, done)
      }
    }, (err) => {
      return cb(err, { jobId })
    })
  }

  return {
    init,
    scope,
    prepareQueue,
    handleFailedJobs,
    addJob
  }

}