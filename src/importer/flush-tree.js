'use strict'

const mh = require('multihashes')
const UnixFS = require('ipfs-unixfs')
const CID = require('cids')
const dagPB = require('ipld-dag-pb')
const asyncEach = require('async/each')

const DAGLink = dagPB.DAGLink
const DAGNode = dagPB.DAGNode

module.exports = (files, ipldResolver, source, callback) => {
  // 1) convert files to a tree
  const fileTree = createTree(files)

  if (Object.keys(fileTree).length === 0) {
    return callback()// no dirs to be created
  }

  // 2) create sizeIndex
  const sizeIndex = createSizeIndex(files)

  // 3) bottom up flushing
  traverse(fileTree, sizeIndex, null, ipldResolver, source, callback)
}

/*
 * createTree
 *
 * received an array of files with the format:
 * {
 *    path: // full path
 *    multihash: // multihash of the dagNode
 *    size: // cumulative size
 * }
 *
 * returns a JSON object that represents a tree where branches are the paths
 * and the leaves are objects with file names and respective multihashes, such
 * as:
 *   {
 *     foo: {
 *       bar: {
 *         baz.txt: <multihash>
 *       }
 *     }
 *   }
 */
function createTree (files) {
  const fileTree = {}

  files.forEach((file) => {
    let splitted = file.path.split('/')
    if (splitted.length === 1) {
      return // adding just one file
    }
    if (splitted[0] === '') {
      splitted = splitted.slice(1)
    }
    var tmpTree = fileTree

    for (var i = 0; i < splitted.length; i++) {
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

  return fileTree
}

/*
 * create a size index that goes like:
 * { <multihash>: <size> }
 */
function createSizeIndex (files) {
  const sizeIndex = {}

  files.forEach((file) => {
    sizeIndex[mh.toB58String(file.multihash)] = file.size
  })

  return sizeIndex
}

/*
 * expand the branches recursively (depth first), flush them first
 * and then traverse through the bottoum up, flushing everynode
 *
 * Algorithm tl;dr;
 *  create a dirNode
 *  Object.keys
 *    If the value is an Object
 *      create a dir Node
 *      Object.keys
 *    Once finished, add the result as a link to the dir node
 *  If the value is not an object
 *    add as a link to the dirNode
 */
function traverse (tree, sizeIndex, path, ipldResolver, source, done) {
  const keys = Object.keys(tree)

  let tmp = tree

  asyncEach(keys, (key, cb) => {
    if (isLeaf(tmp[key])) {
      cb()
    } else {
      path = path ? path + '/' + key : key
      console.log('->', path)

      traverse(tmp[key], sizeIndex, path, ipldResolver, source, (err, multihash) => {
        if (err) {
          return done(err)
        }
        tmp[key] = multihash
        cb()
      })
    }
  }, () => {
    next(tmp, done)
  })

  function next (tree, cb) {
    // at this stage, all keys are multihashes
    // create a dir node
    // add all the multihashes as links
    // return this new node multihash

    const keys = Object.keys(tree)

    const ufsDir = new UnixFS('directory')
    const node = new DAGNode(ufsDir.marshal())

    keys.forEach((key) => {
      const b58mh = mh.toB58String(tree[key])
      const link = new DAGLink(key, sizeIndex[b58mh], tree[key])
      node.addRawLink(link)
    })

    console.log('0---->', path)
    node.multihash((err, multihash) => {
      if (err) {
        return cb(err)
      }
      node.size((err, size) => {
        if (err) {
          return cb(err)
        }

        sizeIndex[mh.toB58String(multihash)] = size
        console.log('1---->', path)

        ipldResolver.put({
          node: node,
          cid: new CID(multihash)
        }, (err) => {
          if (err) {
            source.push(new Error('failed to store dirNode'))
          } else if (path) {
            console.log('2---->', path)
            source.push({
              path: path,
              multihash: multihash,
              size: size
            })
          }

          cb(null, multihash)
        })
      })
    })
  }
}

function isLeaf (value) {
  return !(typeof value === 'object' && !Buffer.isBuffer(value))
}
