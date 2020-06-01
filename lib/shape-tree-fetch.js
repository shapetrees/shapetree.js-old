/** shape-tree-fetch - ShapeTree implemented as a replacement for fetch.
 */

// Logging
const Debug = require('debug');
const Log = Debug('LDP');

const ResponseCodes = require('statuses').STATUS_CODES;
const Errors = require('./rdf-errors');
const Prefixes = require('./prefixes');
const NoShapeTrees = process.env.SHAPETREE !== 'fetch';

module.exports = function (filesystem, rdfInterface, nextFetch, baseUrl, ldpConf) {
  const ShapeTree = require('./shape-tree')(filesystem, rdfInterface, nextFetch);
  const Ecosystem = new (require('../ecosystems/simple-apps'))(filesystem, ShapeTree, rdfInterface);
  Ecosystem.baseUrl = baseUrl;
  Ecosystem.appsUrl = new URL(ldpConf.apps + '/', baseUrl);
  Ecosystem.cacheUrl = new URL(ldpConf.cache + '/', baseUrl);

  return async function (url, options = {}) {
    if (!('method' in options))
      options.method = 'GET';
    const resp = new FakeResponse(url);
    try {
      const requestUrl = new URL(url.href.replace(/^\//, ''))
      const rstat = await rstatOrNull(requestUrl)
      const links = parseLinks(options.headers && options.headers.link ? options.headers.link.join('') : '');
      switch (options.method) {
        
      case 'POST': {
        // Make sure POSTed URL exists.
        throwIfNotFound(rstat, requestUrl, options.method);
        // Store a new resource or create a new ShapeTree
        const postedContainer = NoShapeTrees
              ? await new ShapeTree.Container(requestUrl).ready
              : await ShapeTree.loadContainer(requestUrl);

        const ldpType = links.type.substr(Prefixes.ns_ldp.length); // links.type ? links.type.substr(Prefixes.ns_ldp.length) : null;
        const requestedName = (options.headers.slug || ldpType) + (ldpType === 'Container' ? '/' : '');
        const payload = options.body.toString('utf8');
        const mediaType = options.headers['content-type'];

        const isPlantRequest = !!links.shapeTree;
        if (isPlantRequest) {

          // Parse payload early so we can throw before creating a ShapeTree instance.
          const payloadGraph = await rdfInterface.parseRdf(payload, requestUrl, mediaType);

          // Create ShapeTree instance and tell ecosystem about it.
          const shapeTreeUrl = new URL(links.shapeTree, requestUrl); // !! should respect anchor per RFC5988 §5.2
          const [location, respBody, respMediaType]
                = await Ecosystem.plantShapeTreeInstance(shapeTreeUrl, postedContainer, requestedName.replace(/\/$/, ''), payloadGraph);

          resp.headers.set('Location', location.href);
          resp.status = 201; // Should ecosystem be able to force a 304 Not Modified ?
          resp.headers.set('Content-type', respMediaType);
          resp._text = respBody;
          return Promise.resolve(resp);
        } else {
          if (!(postedContainer instanceof ShapeTree.ManagedContainer))
            return nextFetch(url, options);

          // Validate the posted data according to the ShapeTree rules.
          const approxLocation = new URL(requestedName, requestUrl);
          const entityUrl = new URL(links.root, approxLocation); // !! should respect anchor per RFC5988 §5.2
          const [payloadGraph, dirMaker] =
                await postedContainer.validatePayload(payload, approxLocation, mediaType, ldpType, entityUrl);

          const ret = await nextFetch(url, options);
          try {
            if (ldpType === 'Container') {
              // If it's a Container, instantiate nested Containers
              const location = new URL(ret.headers.get('location'));
              const created = await ShapeTree.loadContainer(location);
              await dirMaker(created);
              const newShapeTreeInstancePath = postedContainer.shapeTreeInstancePath + location.pathname.substr(postedContainer.url.pathname.length);
              await created.asManagedContainer(postedContainer.shapeTreeUrl, newShapeTreeInstancePath)
              await created.write();
            }
          } catch (e) {
            await nextFetch(url, { method: 'DELETE' });
            throw e;
          }
          return ret;
        }
      }

      case 'PUT': {
        // Store a new resource or create a new ShapeTree
        const parentUrl = new URL(requestUrl.pathname.endsWith('/') ? '..' : '.', requestUrl);
        const pstat = rstatOrNull(parentUrl);
        await throwIfNotFound(pstat, requestUrl, options.method);
        const postedContainer = NoShapeTrees
              ? await new ShapeTree.Container(parentUrl).ready
              : await ShapeTree.loadContainer(parentUrl);
        if (!(postedContainer instanceof ShapeTree.ManagedContainer))
          return nextFetch(url, options);

        const ldpType = requestUrl.pathname.endsWith('/') ? 'Container' : 'Resource';
        let location = requestUrl;

        {

          // Validate the posted data according to the ShapeTree rules.
          const entityUrl = new URL(links.root, location); // !! should respect anchor per RFC5988 §5.2
          const payload = options.body.toString('utf8');
          const mediaType = options.headers['content-type'];
          const [payloadGraph, dirMaker] =
                await postedContainer.validatePayload(payload, location, mediaType, ldpType, entityUrl);
          const ret = await nextFetch(url, options);
          if (ldpType === 'Container')
            // If it's a Container, instantiate nested Containers
            await dirMaker(await ShapeTree.loadContainer(url));

          return ret;
        }
      }

      case 'DELETE': {
        return nextFetch(url, options);
      }

      case 'GET': {
        return nextFetch(url, options);
      }

      default:
        console.warn(`shape-tree-fetch would intercept ${options.method} ${url.href} ${JSON.stringify(options)}`);
        return nextFetch(url, options);
      }
    } catch (e) {
      /* istanbul ignore else */
      if (e instanceof Errors.ManagedError) {
        /* istanbul ignore if */
        if (e.message.match(/^\[object Object\]$/))
          console.warn('fix up error invocation for:\n', e.stack);
      } else {
        console.warn('unmanaged exception: ' + (e.stack || e.message))
        e.status = e.status || 500;
      }
      return errorResponse(e, url);
    }
  }

/** Create (plant) a ShapeTree instance.
 */
async function plantShapeTreeInstance (shapeTreeUrl, postedContainer, location) {
  Log('plant', shapeTreeUrl.href)

  // Ask ecosystem if we can re-use an old ShapeTree instance.
  const reusedLocation = Ecosystem.reuseShapeTree(postedContainer, shapeTreeUrl);
  if (reusedLocation) {
    location = reusedLocation;
    Log('plant reusing', location.pathname.substr(1));
  } else {
    Log('plant creating', location.pathname.substr(1));

    // Populate a ShapeTree object.
    const shapeTree = new ShapeTree.RemoteShapeTree(shapeTreeUrl);
    await shapeTree.fetch();

    // Create and register ShapeTree instance.
    await shapeTree.instantiateStatic(shapeTree.getRdfRoot(), location, '.', postedContainer);
    Ecosystem.indexInstalledShapeTree(postedContainer, location, shapeTreeUrl);
    await postedContainer.write();
  }
  return location;
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

async function firstAvailableFile (parentUrl, slug, type) {
  const base = slug || type; // e.g. Container-3 if no slug
  const optSlash = type === 'Container' ? '/' : '';
  let unique = 0;
  let tested;
  while (await incrementAndTest())
    ++unique;
  return tested;

  async function incrementAndTest () {
    const nth = unique > 0 // e.g. -3
          ? '-' + unique
          : '';
    tested = base + nth + optSlash; // e.g. mySlug-3/
    const testUrl = new URL(tested, parentUrl);
    return filesystem.rstat(testUrl).then(stat => true, e => false);
  }
}

}

class Headers extends Map {
  get (name) { return super.get(name.toLowerCase()) || null; }
  set (name, value) { super.set(name.toLowerCase(), value); }
}

/* !! semi-redundant against ecosystems/simple-apps.js
 */
class FakeResponse {
  constructor (url, text = '', type = 'text/plain') {
    this.url = url;
    this.headers = new Headers();
    this.ok = true;
    this._status = 200;
    this.statusText = "OK";
    this.bodyUsed = false;
    this._text = text;
    if (text) {
      this.headers.set('content-length', text.length);
      this.headers.set('content-type', type);
    }
  }
  get status () { return this._status; }
  set status (status) { this._status = status; this.statusText = ResponseCodes[status]; }
  text () { return Promise.resolve(this._text); }
  get body() { throw Error('FakeResponse.body is not implemented'); }
};

function errorResponse (e, url) {
  const json = {
    message: e.message,
    error: e,
    stack: e.stack
  }
  const ret = new FakeResponse(url, JSON.stringify(json))
  ret.headers.set('Content-type', 'application/json');
  ret.status = e.status;
  return ret;
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

