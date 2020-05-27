/** SimpleApps - a simple Solid ecosystem
 *
 * stores ShapeTree indexes in parent containers e.g. /Public or /SharedData
 *
 * This class provides:
 *   initialize - create a hierarchy with /apps, /cache and /shared
 *   indexInstalledShapeTree - assert that a local URL is an instance of a ShapeTree
 *   unindexInstalledShapeTree - remove assertion that a local URL is an instance of a ShapeTree
 *   reuseShapeTree - look in an LDPC for instances of a footprint
 *   registerInstance - register a new ShapeTree instance
 *   parseInstatiationPayload - parse payload when planting a ShapeTree
 */

const Fs = require('fs');
const Fetch = require('node-fetch');
const Log = require('debug')('simpleApps');
const Errors = require('../lib/rdf-errors');
const Mutex = require('../lib/mutex');
const Prefixes = require('../lib/prefixes');
const { DataFactory } = require("n3");
const { namedNode, literal, defaultGraph, quad } = DataFactory;

class FakeResponse {
  constructor (url, headers, text) {
    this._url = url;
    this.headers = headers;
    this._text = text;
    this.ok = true;
  }
  text () { return Promise.resolve(this._text); }
};

class simpleApps {
  constructor (fileSystem, shapeTrees, rdfInterface) {
    this.fileSystem = fileSystem;
    this.shapeTrees = shapeTrees;
    this._rdfInterface = rdfInterface;
    this._mutex = new Mutex();
  }

  initialize (baseUrl, LdpConf) {
    this.baseUrl = baseUrl;
    this.appsUrl = new URL(LdpConf.apps + '/', baseUrl);
    this.cacheUrl = new URL(LdpConf.cache + '/', baseUrl);
    const _simpleApps = this;
    let containerHierarchy =
        {path: '/', title: "DocRoot Container", children: [
          {path: LdpConf.apps + '/', title: "Applications Container"},
          {path: LdpConf.cache + '/', title: "Cache Container"},
          {path: LdpConf.shared + '/', title: "Shared Data Container"},
        ]};
    return createContainers(containerHierarchy, baseUrl);

    /** recursively create a container and any children containers.
     * @param spec - {path, title, children: [...]}
     * @param path - relative path from parent, e.g. ./ or ./Apps
     * @param title - text for the dc:title property
     * @param children - option list of specs of child containers
     * @param parentUrl - URL of parent container, e.g. URL('http://localhost/')
     */
    async function createContainers (spec, parentUrl)  {
      const container = await new _simpleApps.shapeTrees.Container(new URL(spec.path, parentUrl), spec.title).ready;
      spec.container = container; // in case someone needs them later.
      if (spec.children) {
        await Promise.all(spec.children.map(async child => {
          await createContainers(child, container.url);
          container.addMember(new URL(child.path, parentUrl).href, null);
        }));
        await container.write();
      }
      return spec;
    }
  }

  /** indexInstalledShapeTree - assert that instanceUrl is an instance of shapeTreeUrl
   * @param parent: ShapeTree.ManagedContainer
   * @param instanceUrl: URL
   * @param shapeTreeUrl: URL
   */
  indexInstalledShapeTree (parent, instanceUrl, shapeTreeUrl) {
    parent.graph.addQuad(namedNode(instanceUrl.href), namedNode(Prefixes.ns_tree + 'shapeTreeRoot'), namedNode(shapeTreeUrl.href));
    parent.prefixes['tree'] = Prefixes.ns_tree;
  }

  /** unindexInstalledShapeTree - remove assertion that instanceUrl is an instance of shapeTreeUrl
   * @param parent: ShapeTree.ManagedContainer
   * @param instanceUrl: URL
   * @param shapeTreeUrl: URL
   */
  unindexInstalledShapeTree (parent, instanceUrl, shapeTreeUrl) {
    parent.graph.removeQuad(namedNode(instanceUrl.href), namedNode(Prefixes.ns_tree + 'shapeTreeRoot'), namedNode(shapeTreeUrl.href));
  }

  /** reuseShapeTree - look in an LDPC for instances of a footprint
   * @param parent: ShapeTree.ManagedContainer
   * @param shapeTree: ShapeTree.RemoteShapeTree
   */
  reuseShapeTree (parent, shapeTreeUrl) {
    const q = this._rdfInterface.zeroOrOne(parent.graph, null, namedNode(Prefixes.ns_tree + 'shapeTreeRoot'), namedNode(shapeTreeUrl.href));
    return q ? new URL(q.subject.value) : null;
  }


  /** Install (plant) a ShapeTree instance.
   * @param shapeTreeUrl: URL of ShapeTree to be planted
   * @param postedContainer: parent Container where instance should appear
   * @param requestedName: url of created ShapeTree instance
   * @param payloadGraph: RDF payload POSTed in the plant request
   */
  async plantShapeTreeInstance (shapeTreeUrl, postedContainer, requestedName, payloadGraph) {
    Log('plant', shapeTreeUrl.href)
    const appData = this.parseInstatiationPayload(payloadGraph);
    let location;

    // Ask ecosystem if we can re-use an old ShapeTree instance.
    location = this.reuseShapeTree(postedContainer, shapeTreeUrl);
    if (location) {
      Log('plant reused', location.pathname.substr(1));
    } else {

      // Populate a ShapeTree object.
      const shapeTree = new this.shapeTrees.RemoteShapeTree(shapeTreeUrl);
      await shapeTree.fetch();

      const unlock = await this._mutex.lock();
      const tmp = await (await postedContainer.nestContainer(requestedName, `Application Container`));
      const newContainer = await tmp.asManagedContainer(shapeTreeUrl, '.'); // don't move asMC to RemoteShapeTree.instantiateStatic()
      await newContainer.write();
      location = newContainer.url;
      unlock();
      // Create and register ShapeTree instance.
      await shapeTree.instantiateStatic(shapeTree.getRdfRoot(), location, '.', postedContainer, newContainer);
      this.indexInstalledShapeTree(postedContainer, location, shapeTreeUrl);
      await postedContainer.write();
      Log('plant creating', location.pathname.substr(1));
    }

    // The ecosystem consumes the payload and provides a response.
    const [responseGraph, prefixes] = await this.registerInstance(appData, shapeTreeUrl, location);
    const rebased = await this._rdfInterface.serializeTurtle(responseGraph, postedContainer.url, prefixes);
    return [location, rebased, 'text/turtle'];
  }

  /** registerInstance - register a new ShapeTree instance
   * @param appData: RDFJS DataSet
   * @param shapeTree: ShapeTree.RemoteShapeTree
   * @param instanceUrl: location of the ShapeTree instance
   */
  async registerInstance(appData, shapeTreeUrl, instanceUrl) {
    const apps = await new this.shapeTrees.Container(this.appsUrl, `Applications Directory`, null, null).ready;
    const app = await new this.shapeTrees.Container(new URL(appData.name + '/', this.appsUrl), appData.name + ` Directory`, null, null).ready;
    apps.addMember(app.url, shapeTreeUrl);
    await apps.write();
    const prefixes = {
      ldp: Prefixes.ns_ldp,
      tree: Prefixes.ns_tree,
      xsd: Prefixes.ns_xsd,
      dcterms: Prefixes.ns_dc,
    }
    const appFileText = Object.entries(prefixes).map(p => `PREFIX ${p[0]}: <${p[1]}>`).join('\n') + `
<> tree:installedIn
  [ tree:app <${appData.planted}> ;
    tree:shapeTreeRoot <${shapeTreeUrl.href}> ;
    tree:shapeTreeInstancePath <${instanceUrl.href}> ;
  ] .
<${appData.planted}> tree:name "${appData.name}" .
`    // could add tree:instantiationDateTime "${new Date().toISOString()}"^^xsd:dateTime ;
    const toAdd = await this._rdfInterface.parseTurtle(appFileText, app.url, prefixes);
    app.merge(toAdd);
    Object.assign(app.prefixes, prefixes);
    await app.write();
    return [toAdd, prefixes];
  }

  /** parse payload when planting a ShapeTree
   * @param graph: RDFJS Store
   */
  parseInstatiationPayload (graph) {
    const planted = this._rdfInterface.one(graph, null, namedNode(Prefixes.ns_ldp + 'app'), null).object;
    const name = this._rdfInterface.one(graph, planted, namedNode(Prefixes.ns_ldp + 'name'), null).object;
    return {
      planted: planted.value,
      name: name.value
    };
  }

  /** a caching wrapper for fetch
   */
  async fetch (url, /* istanbul ignore next */opts = {}) {
    const prefixes = {};
    const cacheUrl = new URL(cacheName(url.href), this.cacheUrl);
    if (!await this.fileSystem.exists(cacheUrl)) {
      // The first time this url was seen, put the mime type and payload in the cache.

      Log('cache miss on', url.href, '/', cacheUrl.href)
      const resp = await Errors.getOrThrow(Fetch, url);
      const text = await resp.text();
      const headers = Array.from(resp.headers).filter(
        // Hack: remove date and time to reduce visible churn in cached contents.
        pair => !pair[0].match(/date|time/),
      ).reduce(
        (map, pair) => map.set(pair[0], pair[1]),
        new Map()
      );
      // Is there any real return on treating the cacheDir as a Container?

      // const image = `${mediaType}\n\n${text}`;
      const image = Array.from(headers).map(
        pair => `${escape(pair[0])}: ${escape(pair[1])}`
      ).join('\n')+'\n\n' + text;
      await this.fileSystem.write(cacheUrl, image);
      Log('cached', url.href, 'size:', text.length, 'type:', headers.get('content-type'), 'in', cacheUrl.href)
      // return resp;
      return new FakeResponse(url, headers, text);
    } else {
      // Pull mime type and payload from cache.

      const image = await this.fileSystem.read(cacheUrl);
      // const [mediaType, text] = image.match(/([^\n]+)\n\n(.*)/s).slice(1);
      const eoh = image.indexOf('\n\n');
      const text = image.substr(eoh + 2);
      const headers = image.substr(0, eoh).split(/\n/).reduce((map, line) => {
        const [key, val] = line.match(/^([^:]+): (.*)$/).map(decodeURIComponent).slice(1);
        return map.set(key, val);
      }, new Map());
      Log('cache hit on ', url.href, 'size:', text.length, 'type:', headers.get('content-type'), 'in', cacheUrl.href)
      return new FakeResponse(url, headers, text);
    }
  }
};

/** private function to calculate cache names.
 */
function cacheName (url) {
  const copy = new URL(url);
  copy.hash = ''; // All fragments share the same cache entry.
  return copy.href.replace(/[^a-zA-Z0-9_-]/g, '');
}

module.exports = simpleApps;
