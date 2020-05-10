/** shape-tree-fetch - ShapeTree implemented as a replacement for fetch.
 */

const Errors = require('./rdf-errors');

const NoShapeTrees = process.env.SHAPETREE !== 'fetch';

module.exports = function (nextFetch, filesystem) {
  return async function (url, options = {}) {
    if (!('method' in options))
      options.method = 'GET';
    const requestUrl = new URL(url.href.replace(/^\//, ''))
    const rstat = await rstatOrNull(requestUrl)
    const links = parseLinks(options.headers && options.headers.link ? options.headers.link.join('') : '');
    switch (options.method) {
    case 'GET':
    case 'DELETE':
      return nextFetch(url, options);
      
    case 'POST': {
      // Make sure POSTed URL exists.
      throwIfNotFound(rstat, requestUrl, 'POST');
      // Store a new resource or create a new ShapeTree
      const postedContainer = NoShapeTrees
            ? await new ShapeTree.Container(requestUrl).ready
            : await ShapeTree.loadContainer(requestUrl);

      const isPlantRequest = !!links.shapeTree;
      if (isPlantRequest) {
        console.warn('client PLANT');
      }
    }
    default:
      console.warn(`would intercept ${options.method} ${url.href} ${JSON.stringify(options)}`);
      return nextFetch(url, options);
    }
  }

  /* !! redundant against test-suite/servers/LDP.js
   */
  function throwIfNotFound (rstat, url, method) {
    if (rstat)
      return;
    const error = new Errors.NotFoundError(url, 'queried resource', `${method} ${url.pathname}`);
    error.status = 404;
    throw error;
  }

  /* !! redundant against test-suite/servers/LDP.js
   */
  async function rstatOrNull (url) {
    try {
      return await filesystem.rstat(url);
    } catch (e) {
      return null;
    }
  }
}

/* !! redundant against test-suite/servers/LDP.js
 * returns e.g. {"type": "http://...#Container", "rel": "..."}
 */
function parseLinks (linkHeader) {
  if (!linkHeader) return {};
  const components = linkHeader.split(/<(.*?)> *; *rel *= *"(.*?)" *,? */);
  components.shift(); // remove empty match before pattern captures.
  const ret = {  };
  for (i = 0; i < components.length; i+=3)
    ret[components[i+1]] = components[i];
  return ret
  /* functional equivalent is tedious to maintain:
  return linkHeader.split(/(?:<(.*?)> *; *rel *= *"(.*?)" *,? *)/).filter(s => s).reduce(
    (acc, elt) => {
      if (acc.val) {
        acc.map[elt] = acc.val;
        return {map: acc.map, val: null};
      } else {
        return {map: acc.map, val: elt}
      }
    }, {map:{}, val:null}
  ).map
  */
}

