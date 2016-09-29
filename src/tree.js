'use strict'

const mh = require('multihashes')
const UnixFS = require('ipfs-unixfs')
const merkleDAG = require('ipfs-merkle-dag')
const mapValues = require('async/mapValues')

const getSizeAndHash = require('./util').getSizeAndHash

const DAGLink = merkleDAG.DAGLink
const DAGNode = merkleDAG.DAGNode

module.exports = (files, dagService, source, cb) => {
  // file struct
  // {
  //   path: // full path
  //   multihash: // multihash of the dagNode
  //   size: // cumulative size
  //   dataSize: // dagNode size
  // }

  // 1) convert files to a tree
  // for each path, split, add to a json tree and in the end the name of the
  // file points to an object that is has a key multihash and respective value
  // { foo: { bar: { baz.txt: <multihash> }}}
  // the stop condition is if the value is not an object
  const fileTree = {}
  files.forEach((file) => {
    let splitted = file.path.split('/')
    if (splitted.length === 1) {
      return // adding just one file
      // fileTree[file.path] = bs58.encode(file.multihash).toString()
    }
    if (splitted[0] === '') {
      splitted = splitted.slice(1)
    }
    let tmpTree = fileTree

    for (let i = 0; i < splitted.length; i++) {
      if (!tmpTree[splitted[i]]) {
        tmpTree[splitted[i]] = {}
      }
      if (i === splitted.length - 1) {
        tmpTree[splitted[i]] = file.multihash
      } else {
        tmpTree = tmpTree[splitted[i]]
      }
    }
  })

  if (Object.keys(fileTree).length === 0) {
    return cb()// no dirs to be created
  }

  // 2) create a index for multihash: { size, dataSize } so
  // that we can fetch these when creating the merkle dag nodes

  const mhIndex = {}

  files.forEach((file) => {
    mhIndex[mh.toB58String(file.multihash)] = {
      size: file.size,
      dataSize: file.dataSize
    }
  })

  // 3) expand leaves recursively
  // create a dirNode
  // Object.keys
  // If the value is an Object
  //   create a dir Node
  //   Object.keys
  //   Once finished, add the result as a link to the dir node
  // If the value is not an object
  //   add as a link to the dirNode
  function traverse (tree, path, done) {
    const keys = Object.keys(tree)

    mapValues(tree, (node, key, cb) => {
      if (typeof node === 'object' && !Buffer.isBuffer(node)) {
        traverse(node, path ? `${path}/${key}` : key, cb)
      } else {
        cb(null, node)
      }
    }, (err, tmpTree) => {
      if (err) {
        return done(err)
      }

      // at this stage, all keys are multihashes
      // create a dir node
      // add all the multihashes as links
      // return this new node multihash

      const d = new UnixFS('directory')
      const n = new DAGNode()

      keys.forEach((key) => {
        const b58mh = mh.toB58String(tmpTree[key])
        const l = new DAGLink(
          key, mhIndex[b58mh].size, tmpTree[key])
        n.addRawLink(l)
      })

      n.data = d.marshal()

      dagService.put(n, (err) => {
        if (err) {
          source.push(new Error('failed to store dirNode'))
        } else if (path) {
          getSizeAndHash(n, (err, stats) => {
            if (err) {
              return source.push(err)
            }
            source.push({
              path: path,
              multihash: stats.multihash,
              size: stats.size
            })
          })
        }
        finish()
      })

      function finish () {
        if (!path) {
          return done()
        }

        getSizeAndHash(n, (err, stats) => {
          if (err) {
            return done(err)
          }

          mhIndex[mh.toB58String(stats.multihash)] = {
            size: stats.size
          }
          done(null, stats.multihash)
        })
      }
    })
  }

  traverse(fileTree, null, cb)
}
