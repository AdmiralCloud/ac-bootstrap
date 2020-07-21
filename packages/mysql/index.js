/**
 * Takes the object that has .log and .config available and add .redis functions to it.
 * 
 */

const async = require('async')
const _ = require('lodash') 

const mysql = require('mysql')
const crypto = require('crypto')

const knownCertificates = [
  { name: 'rds-ca-2019-eu-central-1', provider: 'aws', fingerPrint: '0A:7D:2F:10:8E:F8:FA:AE:86:CF:9A:55:3D:B0:95:B6:52:35:B9:A3:94:D0:18:99:C1:A6:4F:85:8E:10:80:95' },
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
      _.forOwn(acapi.config.localDatabase, (val, key) => {
        _.set(connection, key, val)
      })
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
        cert = cert.split('\n').filter(line => !line.includes("-----")).map(line => line.trim() ).join('')
        // [NODE VERSION] openssl x509 -noout -fingerprint -sha256 -inform pem -in cert.crt
        let fingerPrint = getHash(cert, 'base64', 'hex').toUpperCase()
        fingerPrint = fingerPrint.match(/.{1,2}/g).join(":") 
        const matchedCertificate = _.find(knownCertificates, { fingerPrint })
        if (matchedCertificate) {
          let value
          if (_.get(db, 'expectedCertificateName')) {
            // show only matching
            if (_.get(db, 'expectedCertificateName') === _.get(matchedCertificate, 'name')) {
              value = _.get(matchedCertificate, 'name') + ' | Expected'
             }
          }
          else {
            value = _.get(matchedCertificate, 'name')
          }
          if (value) {
            acapi.aclog.listing({ field: 'Certificate', value })          
          }
        }
      })
      acapi.aclog.listing({ field: '', value: '' })          
      return itDone()
    })
  }, (err) => {
    if (bootstrapping) return cb(err)
    if (err) acapi.log.error('Bootstrap.initRedis:failed with %j', err)
    process.exit(0)
  })

}