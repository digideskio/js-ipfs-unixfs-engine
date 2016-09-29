'use strict'

const traverse = require('pull-traverse')
const pull = require('pull-stream')
const isIPFS = require('is-ipfs')

const util = require('./util')
const switchType = util.switchType
const cleanMultihash = util.cleanMultihash

const dirExporter = require('./exporters/dir')
const fileExporter = require('./exporters/file')

module.exports = (hash, dagService, options) => {
  if (!isIPFS.multihash(hash)) {
    return pull.error(new Error('not valid multihash'))
  }

  hash = cleanMultihash(hash)
  options = options || {}

  function visitor (item) {
    return pull(
      dagService.getStream(item.hash),
      pull.map((node) => switchType(
        node,
        () => dirExporter(node, item.path, dagService),
        () => fileExporter(node, item.path, dagService)
      )),
      pull.flatten()
    )
  }

  // Traverse the DAG
  return pull(
    dagService.getStream(hash),
    pull.map((node) => switchType(
      node,
      () => traverse.widthFirst({path: hash, hash}, visitor),
      () => fileExporter(node, hash, dagService)
    )),
    pull.flatten()
  )
}
