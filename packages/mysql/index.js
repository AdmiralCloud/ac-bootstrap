/**
 * Takes the object that has .log and .config available and add .redis functions to it.
 * 
 */

const async = require('async')
const _ = require('lodash') 

const mysql = require('mysql')
const crypto = require('crypto')

const knownCertificates = [
  { name: 'rds-ca-2015-eu-central-1', provider: 'aws', fingerPrint: '63:0F:29:07:BA:DB:16:6B:3F:01:11:3D:D2:B8:94:2C:6C:DA:99:B3:4F:E3:81:E8:7C:01:FC:15:9F:0D:AC:63' }
]

module.exports = (acapi, options, cb) => {
  const bootstrapping = _.get(options, 'bootstrapping', true)

  acapi.aclog.headline({ headline: 'mysql' })

  function getHash(content, inputEncoding = "utf8", outputEncoding="base64") {
    const shasum = crypto.createHash("sha256")
    shasum.update(content, inputEncoding)
    return shasum.digest(outputEncoding)
}
  
  // init multiple instances for different purposes
  acapi.mysql = {}
  async.eachSeries(_.get(acapi.config, 'database.servers'), (db, itDone) => {
    if (_.get(db, 'ignoreBootstrap')) return itDone()
    
    let connection = _.pick(db, ['host', 'user', 'password', 'database', 'timezone', 'ssl'])
    if (acapi.config.localDatabase) {
      _.set(connection, 'port', _.get(acapi.config, 'localDatabase.port'))
    }
    acapi.mysql[_.get(db, 'server')] = mysql.createPool(_.merge(connection, {
      multipleStatements: true,
      connectionLimit: 5
    }))
    acapi.mysql[_.get(db, 'server')].getConnection((err,r) => {
      if (err) {
        if (bootstrapping) return itDone(err)
        acapi.log.error('Bootstrap.initMySQL:connect %s failed %j', _.get(db, 'name'), err)
      }
      acapi.aclog.serverInfo(connection)
      if (!_.get(acapi.config, 'database.certificateCheck') || !_.get(db, 'ssl')) return itDone()

      const certs = _.get(r, 'config.ssl.ca')
      _.forEach(certs, cert => {
        cert.split('\n').filter(line => !line.includes("-----")).map(line => line.trim() ).join('')
        // [NODE VERSION] openssl x509 -noout -fingerprint -sha256 -inform pem -in cert.crt
        let fingerPrint = getHash(cert, 'base64', 'hex').toUpperCase()
        fingerPrint = fingerPrint.match(/.{1,2}/g).join(":") 
        const matchedCertificate = _.find(knownCertificates, { fingerPrint })
        acapi.aclog.listing({ field: 'Certificate', value: _.get(matchedCertificate, 'name') })
        acapi.aclog.listing({ field: '', value: fingerPrint })
      })
      return itDone()
    })
  }, (err) => {
    if (bootstrapping) return cb(err)
    if (err) acapi.log.error('Bootstrap.initRedis:failed with %j', err)
    process.exit(0)
  })

}