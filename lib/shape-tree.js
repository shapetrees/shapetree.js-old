/** Simple structure to associate resource hiearchies with shapes and media types
 *
 * This library provides:
 * * Container - an LDPC.
 * * ManagedContainer - an LDPC under ShapeTree control.
 * * loadContainer - loads either a Container or a ManagedContainer.
 * * RemoteShapeTree - a parsed ShapeTree structure.
 * @module ShapeTree
 */
function ShapeTreeFunctions (storage, rdfInterface, cachingFetch) {

// Logging
const Debug = require('debug');
const Log = Debug('				shape-tree');
const Details = Log.extend('details');

const Path = require('path');
const Relateurl = require('relateurl');
const N3 = require("n3");
const { namedNode, literal, defaultGraph, quad } = N3.DataFactory;
const Errors = require('./rdf-errors');
const Mutex = require('./mutex');
const Prefixes = require('./prefixes');
const UriTemplate = require('uri-template-lite').URI.Template;
const ShExCore = require('@shexjs/core')
const ShExPath = require('../../../../shexSpec/shex.js/packages/shex-path')
const ShExParser = require('@shexjs/parser')

/** Container - an LDPC
 * @param url: URL of Container
 * @param title: dc:title of container | an N3.Store for already-read graphs.
 */
class Container {
  constructor (url, title) {
    if (!(url instanceof URL))
      throw Error(`url ${url} must be an instance of URL`);
    if (!(url.pathname.endsWith('/')))
      throw Error(`url ${url} must end with '/'`);
    if (url.pathname.endsWith('//'))
      throw Error(`url ${url} ends with '//'`);
    this._classDetails = Details.extend(`Container(<${url.pathname}>, ${title instanceof Array ? `[n3.Store() with ${title[0].size} quads, ${JSON.stringify(title[1])}]` : `"${title}"`})`);
    const funcDetails = this._classDetails.extend('new');
    funcDetails('');
    this.url = url;
    this.prefixes = title instanceof Array ? title[1] : {};
    this._mutex = new Mutex()
    this.graph = title instanceof Array ? title[0] : new N3.Store();
    this.subdirs = [];

    this.ready = title instanceof Array ? Promise.resolve(this) : loadOrCreate.call(this);

    async function loadOrCreate () {
      const unlock = await this._mutex.lock();
      funcDetails('storage.ensureContainer()', url.pathname);
      const [newDir, containerGraph] = await storage.ensureContainer(this.url, this.prefixes, title);
      this.newDir = newDir;
      this.graph.addQuads(containerGraph.getQuads());
      unlock();
      return /*this*/ new Promise((res, rej) => { // !!DELME sleep for a bit to surface bugs
        setTimeout(() => {
          res(this)
        }, 20);
      });
    }
  }

  setTitle (title) {
    this.graph.getQuads(
      namedNode(this.url.href), namedNode(Prefixes.ns_dc + 'title'), null
    ).map(
      q => this.graph.removeQuad(q)
    );
    this.graph.addQuad(namedNode(this.url.href), namedNode(Prefixes.ns_dc + 'title'),
                       literal(title));
  }

  /** virtual nest:URL - add a Container in this Container
   * @param slug:string - requested name of new Resource
   * @param payload - contents of document
   * @param mediaType - media type of payload
   * @returns created Container object
   */
  async nest (slug, payload, mediaType) {
    const prefixes = {};
    const funcDetails = this._classDetails.extend(`nest("%{slug}", ${payload.length} bytes, "${mediaType})`);
    funcDetails('storage.invent(<%s>, "%s", %d characters, "%s")', this.url.pathname, slug, payload.length, mediaType);
    const [name] = await storage.invent(this.url, slug, payload, mediaType);
    console.assert(name.match(/^[^/]+$/));
    const url = new URL(name, this.url);
    return url;
  }

  /** virtual nest:Container - add a Container in this Container
   * @param slug:string - requested name of new Container
   * @param title:string - title of new Container
   * @returns created Container object
   */
  async nestContainer (slug, title) {
    const prefixes = {};
    const containerTitle = `unmanaged Container in ${this.url.pathname}`;

    const funcDetails = this._classDetails.extend(`nestContainer("${slug}", "${title})`);
    funcDetails('storage.inventContainer(<%s>, "%s", "%s", %s)', this.url.pathname, slug, containerTitle, JSON.stringify(prefixes));
    const [name, graph] = await storage.inventContainer(this.url, slug, containerTitle, prefixes);
    console.assert(name.match(/^[^/]+\/$/));
    const url = new URL(name, this.url);
    funcDetails('new Container(%s, [...])', this.url.pathname);
    const ret = new Container(url, [graph, prefixes]); // kinda inefficient
    ret.setTitle(`unmanaged Container ${url.pathname}`);
    await ret.write();
    return ret;
  }

  /** PUT or POST to an unmanaged LDPC
   * @param {string} payload - HTTP request body
   * @param {URL} location - resource being created or replaced
   * @param {string} mediaType - media type of payload
   * @param {string} ldpType - "Resource"|"Container"|"NonRDFSource"
   * @param {URL} entityUrl - unused
   */
  async validatePayload (payload, location, mediaType, ldpType/*, entityUrl*/) {
    let payloadGraph = null;
    const prefixes = {};

    if (ldpType == 'NonRDFSource') {
      ;
    } else {
      payloadGraph = await rdfInterface.parseRdf(payload, location, mediaType, prefixes);
    }
    // Return a trivial lambda for creating a single Container.
    return [payloadGraph, async serversNewContainer => {
      Object.assign(serversNewContainer.prefixes, prefixes); // inject the parsed prefixes
      return serversNewContainer;
    }];
  }

  async write () {
    const unlock = await this._mutex.lock();
    this._classDetails.extend(`write()`);
    await storage.writeContainer(this.url, this.graph, this.prefixes).then(
      x => { unlock(); return x; },
      e => /* istanbul ignore next */ { unlock(); throw e; }
    );
    return this
  }

  async remove () {
    const unlock = await this._mutex.lock();
    return storage.removeContainer(this.url).then(
      x => { unlock(); return x; },
      e => /* istanbul ignore next */ { unlock(); throw e; }
    );
  }

  async merge (payload, base) {
    // istanbul ignore next
    const g2 = payload instanceof N3.Store ? payload : await rdfInterface.parseTurtle(payload, base, this.prefixes);
    this.graph.addQuads(g2.getQuads());
    return this
  }

  addMember (location, shapeTreeUrl) {
    this.graph.addQuad(namedNode(this.url.href), namedNode(Prefixes.ns_ldp + 'contains'), namedNode(location));
    return this
  }

  removeMember (location, shapeTreeUrl) {
    this.graph.removeQuad(namedNode(this.url.href), namedNode(Prefixes.ns_ldp + 'contains'), namedNode(location));
    return this
  }

  addSubdirs (addUs) {
    this.subdirs.push(...addUs);
    return this
  }

  async plantShapeTreeInstance (shapeTreeUrl, requestedName) {
    Log('plant', shapeTreeUrl.href)
    const funcDetails = Details.extend(`plantShapeTreeInstance(<${shapeTreeUrl.href}>), Container(<${this.url.pathname}>), "${requestedName}")`);
    funcDetails('');
    let location;

      // Populate a ShapeTree object.
      funcDetails('ShapeTrees.RemoteShapeTree(<%s>)', shapeTreeUrl.href);
      const shapeTree = new RemoteShapeTree(shapeTreeUrl);
      await shapeTree.fetch();

    // const unlock = await this._mutex.lock();
      const appContainerTitle = 'Application Container';
      funcDetails('this(<%s>).nestContainer(<%s>, "%s")', this.url.pathname, requestedName, appContainerTitle)
      const tmp = await (await this.nestContainer(requestedName, appContainerTitle));
      funcDetails(`Container(${tmp.url.pathname}).asManagedContainer(${shapeTreeUrl.pathname}, '.')`)
      const newContainer = await tmp.asManagedContainer(shapeTreeUrl, '.', tmp.url); // don't move asMC to RemoteShapeTree.instantiateStatic()
      funcDetails('setTitle()');
      newContainer.setTitle(`root of Container for ${shapeTree.url}`);
      await newContainer.write();
      location = newContainer.url;
    // unlock();

      // Create and register ShapeTree instance.
      funcDetails(`shapeTree(<${shapeTree.url.href}>).instantiateStatic(${JSON.stringify(shapeTree.getRdfRoot())}, <${location.pathname}>, '.', this(<${this.url.pathname}>), Container(<${newContainer.url.pathname}>))`);
      await shapeTree.instantiateStatic(shapeTree.getRdfRoot(), location, '.', this, newContainer);

    return location;
  }

  /** asManagedContainer - add necessary triples to make this a ManagedContainer
   * @param shapeTreeUrl: the URL of the ShapeTree that defines this instance.
   * @param shapeTreeInstancePath: the path from the root of this ShapeTree instance.
   */
  async asManagedContainer (shapeTreeUrl, shapeTreeInstancePath, validationRoot) {
    const mdGraph = await mergeTreeMetadata(this.url, shapeTreeUrl, shapeTreeInstancePath, validationRoot, this.prefixes);
    return this instanceof ManagedContainer
      ? this
      : await new ManagedContainer(this.url, [this.graph, this.prefixes, mdGraph]).ready;
  }
}

  async function mergeTreeMetadata (describedUrl, shapeTreeUrl, shapeTreeInstancePath, validationRoot, prefixes) {
    const mdUrl = new URL(await storage.getMetaDataFilePath(describedUrl), describedUrl);
    const fromMd = Relateurl.relate(mdUrl.href, describedUrl.href);
    const mdGraph = await storage.readMetaData(describedUrl, {});
    const instanceRootRel = shapeTreeInstancePath.substr(0, shapeTreeInstancePath.lastIndexOf('/'));
    // const validationRootRel = Relateurl.relate(mdUrl.href, validationRoot.href);
    const validationRootStr = validationRoot === null ? '' : ` ;
   tree:validationRoot <${validationRoot.href}>`
    const mdNode = namedNode(mdUrl.href);
    (['shapeTreeRoot', 'shapeTreeInstancePath', 'shapeTreeInstanceRoot'])
      .forEach(
        ln =>
          mdGraph.removeQuads(mdGraph.getQuads(mdNode, namedNode(Prefixes.ns_tree + ln), null))
      )
    const c = `
@prefix dcterms: <http://purl.org/dc/terms/>.
@prefix ldp: <http://www.w3.org/ns/ldp#>.
@prefix tree: <${Prefixes.ns_tree}>.

<${fromMd}>
   tree:shapeTreeRoot <${shapeTreeUrl.href}> ;
   tree:shapeTreeInstancePath "${shapeTreeInstancePath}" ;
   tree:shapeTreeInstanceRoot <${instanceRootRel ? Path.relative(instanceRootRel, '') : '.'}>${validationRootStr} .
`;
    const s = await rdfInterface.parseTurtle(c, describedUrl, prefixes);
    mdGraph.addQuads(s.getQuads());
    storage.writeMetaData(describedUrl, mdGraph, {tree: Prefixes.ns_tree});
    return mdGraph;
  }

/** ManagedContainer - an LDPC with shapeTrees
 * @param url: URL of Container
 * @param title: dc:title of container | a [N3.Store, prefixex] for already-read graphs (hack).
 * @param shapeTreeUrl: the URL of the ShapeTree that defines this instance.
 * @param shapeTreeInstancePath: the path from the root of this ShapeTree instance.
 */
class ManagedContainer extends Container {
  constructor (url, title, shapeTreeUrl, shapeTreeInstancePath) {
    super(url, title);
    this._classDetails = Details.extend(`ManagedContainer(<${url.pathname}>, ${title instanceof Array ? `[n3.Store() with ${title[0].size} quads, ${JSON.stringify(title[1])}]` : `"${title}", <${shapeTreeUrl.pathname}>, "${shapeTreeInstancePath}"`})`);
    const funcDetails = this._classDetails.extend('new');
    funcDetails('');
    if (title instanceof Array) {
      parseShapeTreeInstance.call(this, title[2]); // the metadata graph
    } else {
      if (!(shapeTreeUrl instanceof URL))
        throw Error(`shapeTreeUrl ${shapeTreeUrl} must be an instance of URL`);
      this,shapeTreeUrl = shapeTreeUrl;
      this.shapeTreeInstancePath = shapeTreeInstancePath;
      this.shapeTreeInstanceRoot = null;
      this.shapeTreeInstanceRoot = new URL(Path.relative(shapeTreeInstancePath, ''), shapeTreeUrl);
      this.ready = this.ready.then(() => loadOrCreate.call(this));
    }

    async function loadOrCreate () {
      const unlock = await this._mutex.lock();
      if (this.newDir) {
        funcDetails(`this.asManagedContainer(<${shapeTreeUrl.href}>, "${shapeTreeInstancePath}")`);
        await this.asManagedContainer(shapeTreeUrl, shapeTreeInstancePath, this.url);
        funcDetails('storage.writeContainer(<%s>, n3.Store() with %d quads, %s)', this.url.pathname, this.graph.size, JSON.stringify(this.prefixes));
        await storage.writeContainer(this.url, this.graph, this.prefixes);
      } else {
        parseShapeTreeInstance.call(this, await storage.readMetaData(new URL(await storage.getMetaDataFilePath(this.url), this.url), {}));
      }
      unlock();
      return /*this*/ new Promise((res, rej) => { // !!DELME sleep for a bit to surface bugs
        setTimeout(() => {
          res(this);
        }, 20);
      });
    }

    function parseShapeTreeInstance (metaDataGraph) {
      this.shapeTreeInstanceRoot = asUrl(metaDataGraph, this.url, 'shapeTreeInstanceRoot');
      this.shapeTreeInstancePath = asLiteral(metaDataGraph, this.url, 'shapeTreeInstancePath');
      this.shapeTreeUrl = asUrl(metaDataGraph, this.url, 'shapeTreeRoot');
    }
  }

  async nest (slug, payload, mediaType, step, validationRootRel) {
    const url = await super.nest(slug, payload, mediaType);
    const added = url.href.substr(this.url.href.length);
    const shapeTree = await this.getRootedShapeTree();
    await mergeTreeMetadata(url, new URL(step.node.value), pathAppend(this.shapeTreeInstancePath, added), validationRootRel === null ? null : new URL(validationRootRel, url), {});
    return url;
  }

  /** virtual nestContainer:Container - add a Container in this Container
   * @param url:URL - URL of new Container
   * @returns created Container object
   */
  async nestContainer (slug, title, validationRootRel) {
    const prefixes = {};
    const containerTitle = `managed Container in ${this.url.pathname}`;
    const funcDetails = this._classDetails.extend(`nestContainer("${slug}", "${title}")`);
    funcDetails('storage.inventContainer(<%s>, "%s", "%s", %s)', this.url.pathname, slug, containerTitle, JSON.stringify(prefixes));
    const [name, graph] = await storage.inventContainer(this.url, slug, containerTitle, prefixes);
    console.assert(name.match(/^[^/]+\/$/));
    const url = new URL(name, this.url);
    const newInstancePath = pathAppend(this.shapeTreeInstancePath, name);
    const validationRoot = new URL(validationRootRel, url)
    funcDetails('new Container(%s, [...]).asManagedContainer(<%s>, "%s", "%s")', this.shapeTreeUrl.href, newInstancePath, validationRoot.href);
    const ret = await new Container(url, [graph, prefixes]).asManagedContainer(this.shapeTreeUrl, newInstancePath, validationRoot); // kinda inefficient
    ret.setTitle(`nested Container for ${newInstancePath} in <${this.shapeTreeUrl}>`);
    // console.warn(215, ret.graph.getQuads().map(q => '\n' + (['subject', 'predicate', 'object']).map(pos => q[pos].value).join(' ')).join(''))
    await ret.write();
    return ret;
  }

  async getRootedShapeTree () {
    const mdUrl = new URL(await storage.getMetaDataFilePath(this.url), this.url);
    const fromMd = Relateurl.relate(mdUrl.href, this.url.href);
    const mdGraph = await storage.readMetaData(this.url);
    const path = rdfInterface.one(mdGraph, namedNode(this.url.href), namedNode(Prefixes.ns_tree + 'shapeTreeInstancePath'), null).object.value;
    const root = rdfInterface.one(mdGraph, namedNode(this.url.href), namedNode(Prefixes.ns_tree + 'shapeTreeRoot'), null).object.value;
    return new RemoteShapeTree(new URL(root), path)
  }

  /** Validate member of this Container according to step in ShapeTree.
   * @param {string} payload - HTTP request body
   * @param {URL} location - resource being created or replaced
   * @param {string} mediaType - media type of payload
   * @param {string} ldpType - "Resource"|"Container"|"NonRDFSource"
   * @param {URL} entityUrl - initial focus for validation
   */
  async validatePayload (payload, location, mediaType, ldpType, entityUrl) {
    const _ManagedContainer = this;
    let payloadGraph = null;
    const prefixes = {};

    // Get ShapeTree object from the container we're POSTing to.
    const shapeTree = await this.getRootedShapeTree();
    await shapeTree.fetch();

    // Find the corresponding step.
    const resourceName = location.pathname.substr(this.url.pathname.length);
    const pathWithinShapeTree = pathAppend(shapeTree.path, resourceName);
    const step = shapeTree.matchingStep(shapeTree.getRdfRoot(), resourceName);
    console.assert(!step.name); // can't post to static resources.
    Log.extend('ManagedContainer')('validate %s payload (%d bytes) with %s', mediaType, payload.length, step.matchesUriTemplate.value);

    // Validate the payload
    if (ldpType !== step.type)
      throw new Errors.ManagedError(`Resource POSTed with link type=${ldpType} while ${step.node.value} expects a ${step.type}`, 422);
    if (ldpType == 'NonRDFSource') {
      // if (step.validatedBy)
      //   throw new Errors.ShapeTreeStructureError(this.url, `POST of NonRDFSource to ${rdfInterface.renderRdfTerm(step.node)} which has a tree:validatedBy property`);
    } else {
      if (!step.validatedBy)
        // @@issue: is a step allowed to not have a validatedBy?
        throw new Errors.ShapeTreeStructureError(this.url, `${rdfInterface.renderRdfTerm(step.node)} has no tree:validatedBy property`);
      payloadGraph = await rdfInterface.parseRdf(payload, location, mediaType, prefixes);
      await shapeTree.validate(step.validatedBy.value, payloadGraph, entityUrl.href);
    }

    // Return a lambda for creating a containers mandated by the ShapeTree.
    return [payloadGraph, async serversNewContainer => {
      const dir = await shapeTree.instantiateStatic(step.node, location, pathWithinShapeTree, _ManagedContainer, serversNewContainer)
      Object.assign(dir.prefixes, prefixes, dir.prefixes); // inject the parsed prefixes
      return dir;
    }, step];
  }
}

/* append ShapeTree instance paths
 * first arg may be '.'
 */
function pathAppend () {
  const [base, ...rest] = Array.from(arguments);
  return [].concat.call([base === '.' ? '' : base], rest).join('');
}

function asUrl (g, s, p) {
  const ret = rdfInterface.zeroOrOne(g, namedNode(s.href), namedNode(Prefixes.ns_tree + p), null);
  return ret ? new URL(ret.object.value) : null;
}

function asLiteral (g, s, p) {
  const ret = rdfInterface.zeroOrOne(g, namedNode(s.href), namedNode(Prefixes.ns_tree + p), null);
  return ret ? ret.object.value : null;
}

/** loadContainer - read an LDPC from the storage
 * @param url: URL of Container
 */
async function loadContainer (url) {
  const prefixes = {};
  const containerGraph = await storage.readContainer(url, prefixes);
  const mdUrl = new URL(await storage.getMetaDataFilePath(url), url);
  const fromMd = Relateurl.relate(mdUrl.href, url.href);
  const mdGraph = await storage.readMetaData(url, prefixes);
  return asUrl(mdGraph, url, 'shapeTreeInstanceRoot')
    ? new ManagedContainer(url, [containerGraph, prefixes, mdGraph])
    : new Container(url, [containerGraph, prefixes]);
}

class RemoteResource {
  constructor (url) {
    if (!(url instanceof URL)) throw Error(`url ${url} must be an instance of URL`);
    this._classDetails = Details.extend(`RemoteResource(<${url.href}>)`);
    this._classDetails('new')
    this.url = url;
    this.prefixes = {};
    this.graph = null;
  }

  async fetch () {
    this._classDetails(`fetch(<${this.url}>)`);
    const resp = await cachingFetch(this.url);
    const mediaType = resp.headers.get('content-type').split(/; */)[0];
    const text = await resp.text();

    switch (mediaType) {
    case 'application/ld+json':
      // parse the JSON-LD into n-triples
      this.graph = await rdfInterface.parseJsonLd(text, this.url);
      break;
    case 'text/turtle':
      this.graph = await rdfInterface.parseTurtle (text, this.url, this.prefixes);
      break;
    default:
      /* istanbul ignore next */throw Error(`unknown media type ${mediaType} when parsing ${this.url.href}`)
    }
    return this;
  }
}

/** reference to a ShapeTree stored remotely
 *
 * @param url: URL string locating ShapeTree
 * @param path: refer to a specific node in the ShapeTree hierarchy
 *
 * A ShapeTree has contents:
 *     [] a rdf:ShapeTreeRoot, ldp:BasicContainer ; tree:contains
 *
 * The contents may be ldp:Resources:
 *         [ a ldp:Resource ;
 *           tree:matchesUriTemplate "{labelName}.ttl" ;
 *           tree:validatedBy gh:LabelShape ] ],
 * or ldp:Containers, which may either have
 * n nested static directories:
 *         [ a ldp:BasicContainer ;
 *           rdfs:label "repos" ;
 *           tree:contains ... ] ,
 *         [ a ldp:BasicContainer ;
 *           rdfs:label "users" ;
 *           tree:contains ... ]
 * or one dynamically-named member:
 *         [ a ldp:BasicContainer ;
 *           tree:matchesUriTemplate "{userName}" ;
 *           tree:validatedBy gh:UserShape ;
 *           tree:contains ]
 */
class RemoteShapeTree extends RemoteResource {
  constructor (url, path = '.') {
    super(url);
    this._classDetails = Details.extend(`RemoteShapeTree(<${url.href}>, "${path}")`);
    this._classDetails('new')
    this.path = path
  }

  static async get (url) {
    const docString = noHash(url).href
    if (RemoteShapeTree.cache && docString in RemoteShapeTree.cache)
      return RemoteShapeTree.cache[docString]
    const ret = await new RemoteShapeTree(url).fetch()
    if (RemoteShapeTree.cache)
      RemoteShapeTree.cache[docString] = ret
    return ret
  }

  async fetch () {
    await super.fetch()
    this.ids = {} // @id index for entire ShapeTree
    this.tree = await this.parse() // add '@id', 'expectsType', 'contains', 'validatedBy' properties
    return this
  }

  async parse () {
    const _RemoteShapeTree = this
    const p1 = new Promise((acc, rej) => {
      setTimeout(() => acc(5), 200)
    })
    const contains = namedNode(Prefixes.ns_tree + 'contains')
    const root = this.graph.getQuads(null, contains, null).find(
      q => this.graph.getQuads(null, contains, q.subject).length === 0
    ).subject;

    const str = sz => one(sz).value
    const url = sz => new URL(one(sz).value)
    const lst = sz => sz.map(s => new URL(s.value))
    const cnt = sz => visitStep(stepRules, sz, true)
    const ref = sz => visitStep(referenceRules, sz, false)
    const one = sz => sz.length === 1
          ? sz[0]
          : (() => {throw new Errors.ShapeTreeStructureError(this.url, `Expected one object, got [${sz.map(s => s.value).join(', ')}]`)})()
    const referenceRules = [
      { predicate: Prefixes.ns_tree + 'treeStep'    , attr: 'treeStep'    , f: url },
      { predicate: Prefixes.ns_tree + 'shapePath'   , attr: 'shapePath'   , f: str },
    ]
    const stepRules = [
      { predicate: Prefixes.ns_tree + 'expectsType', attr: 'expectsType', f: url },
      { predicate: Prefixes.ns_rdfs + 'label'       , attr: 'name'        , f: str },
      { predicate: Prefixes.ns_tree + 'matchesUriTemplate' , attr: 'matchesUriTemplate' , f: str },
      { predicate: Prefixes.ns_tree + 'validatedBy'       , attr: 'validatedBy'       , f: url },
      { predicate: Prefixes.ns_tree + 'supports'       , attr: 'supports'       , f: lst },
      { predicate: Prefixes.ns_tree + 'contains'    , attr: 'contains'    , f: cnt },
      { predicate: Prefixes.ns_tree + 'references'  , attr: 'references'  , f: ref },
    ]

    const subjects = this.graph.getQuads(null, null, null).reduce((acc, q) => {
      if (q.subject.termType === 'NamedNode') {
        const key = q.subject.value
        if (!(key in acc))
          acc[key] = q.subject
      }
      return acc
    }, {})
    for (const key in subjects) {
      // try {
        const step = visitStep(stepRules, [subjects[key]], true)[0]
        if (Object.keys(step).length > 1)
          _RemoteShapeTree.ids[step['@id'].href] = step
      // } catch (e) {
      //   console.warn('WHY:', e); // non-ShapeTree node in the ShapeTree graph. I guess that's OK.
      // }
    }
    const indexes =
          this.graph.getQuads(namedNode(noHash(this.url).href), null, null)
          .map(q => new URL(q.object.value))
    if (indexes.length > 0)
      _RemoteShapeTree.hasShapeTreeDecoratorIndex = indexes
    return _RemoteShapeTree.ids[root.value]

    function visitStep (rules, subjects, includeId) {
      return subjects.map(s => {
        const byPredicate = _RemoteShapeTree.graph.getQuads(s, null, null).reduce((acc, q) => {
          const p = q.predicate.value
          if (!(p in acc))
            acc[p] = []
          acc[p].push(q.object)
          return acc
        }, {})
        const ret = includeId
              ? {'@id': new URL(s.value)}
              : {}
        return rules.reduce((acc, rule) => {
          const p = rule.predicate
          if (p in byPredicate)
            acc[rule.attr] = rule.f(byPredicate[p])
          return acc
        }, ret)
      })
    }

    // handy function, not currently used
    function shorten999 (urlStr) {
      for (const prefix in Prefixes) {
        const ns = Prefixes[prefix]
        if (urlStr.startsWith(ns)) {
          const localname = urlStr.substr(ns.length)
          if (localname.match(/^[a-zA-Z]+$/))
            return prefix === ''
            ? localname
            : prefix + ':' + localname
        }
      }
      if (new URL(_RemoteShapeTree.url.href).host === new URL(urlStr).host) // https://github.com/stevenvachon/relateurl/issues/28
        return Relateurl.relate(_RemoteShapeTree.url.href, urlStr, { output: Relateurl.ROOT_PATH_RELATIVE })
      return urlStr
    }
  }

  /** getRdfRoot - Walk through the path elements to find the target node.
   */
  getRdfRoot () {
    return this.path.split(/\//).reduce((node, name) => {
      if (name === '.' || name === '')
        return node;
      // Get the contents of the node being examined
      const cqz = this.graph.getQuads(node, namedNode(Prefixes.ns_tree + 'contains'), null);
      // Find the element which either
      return cqz.find(
        q =>
          // matches the current label in the path
        this.graph.getQuads(q.object, namedNode(Prefixes.ns_rdfs + 'label'), literal(name)).length === 1
          ||
          // or has a matchesUriTemplate (so it should be the sole element in the contents)
        this.graph.getQuads(q.object, namedNode(Prefixes.ns_tree + 'matchesUriTemplate'), null).length === 1
      ).object
    }, namedNode(this.url.href));
  }

  /** firstChild - return the first contents.
   * @returns: { type, name, matchesUriTemplate, validatedBy, contains }
   */
  matchingStep (shapeTreeNode, slug) {
    const contains = this.graph.getQuads(shapeTreeNode, namedNode(Prefixes.ns_tree + 'contains'))
          .map(q => q.object);
    const choices = contains
          .filter(
            step => !slug ||
              new UriTemplate(
                this.graph.getQuads(step, namedNode(Prefixes.ns_tree + 'matchesUriTemplate'))
                  .map(q2 => q2.object.value)[0]
              ).match(slug)
          );
    if (choices.length === 0)
      throw new Errors.UriTemplateMatchError(slug, [], `No match in ${shapeTreeNode.value} ${contains.map(t => t.value).join(', ')}`);
    /* istanbul ignore if */
    if (choices.length > 1) // @@ Could have been caught by static analysis of ShapeTree.
      throw new Errors.UriTemplateMatchError(slug, [], `Ambiguous match against ${contains.map(t => t.value).join(', ')}`);
    const g = this.graph;
    const typeNode = obj('expectsType')
    const ret = {
      node: choices[0],
      typeNode: typeNode,
      name: obj('name'),
      matchesUriTemplate: obj('matchesUriTemplate'),
      validatedBy: obj('validatedBy'),
      contains: this.graph.getQuads(choices[0], Prefixes.ns_tree + 'contains', null).map(t => t.object)
    };
    /* istanbul ignore else */ if (typeNode)
      ret.type = typeNode.value.replace(Prefixes.ns_ldp, '');
    return ret;

    function obj (property) {
      const q = rdfInterface.zeroOrOne(g, choices[0], namedNode(Prefixes.ns_tree + property), null);
      return q ? q.object : null;
    }
  }


  /** instantiateStatic - make all containers implied by the ShapeTree.
   * @param {RDFJS:node} stepNode - subject of ldp:contains arcs of the LDP-Cs to be created.
   * @param {URL} rootUrl - root of the resource hierarchy (path === '/') @@ change to ManagedContainer?
   * @param {string} pathWithinShapeTree. e.g. "repos/someOrg/someRepo"
   * @param {Container} parent - Container object for parent to be updated with new member
   * @param {Container} container - optional pre-existing Container object for root of new tree
   *   WARNING: in the interest of API simplicity, container will be removed if instantiateStatic fails.
   */
  async instantiateStatic (stepNode, rootUrl, pathWithinShapeTree, parent, container = null) {
    const funcDetails = this._classDetails.extend(`instantiateStatic(<${stepNode.value}>, <${rootUrl.pathname}>, "${pathWithinShapeTree}", ${parent.url.pathname}, ...)`);
    funcDetails('');
    let ret;
    if (container) {
      ret = container;
      // We could move the `await container.asManagedContainer(this.url,
      // pathWithinShapeTree)` from plantShapeTreeInstance but that would be
      // redundant in the common path of P*Ting to a ManagedContainer.
    } else {
      const containerTitle = `nested Container ${pathWithinShapeTree} in <${this.url}>`;
      funcDetails('new ManagedContainer(<%s>, "%s", <%s>, "%s")', rootUrl.pathname, containerTitle, this.url.href, pathWithinShapeTree);
      ret = await new ManagedContainer(rootUrl,
                                       containerTitle,
                                       this.url, pathWithinShapeTree).ready;
      parent.addMember(ret.url.href, stepNode.url);
    }

    try {
      const contains = this.graph.getQuads(stepNode, Prefixes.ns_tree + 'contains', null).map(q => q.object);
      const subdirPromises = contains.reduce((acc, nested) => {
        const labelQ = rdfInterface.zeroOrOne(this.graph, nested, namedNode(Prefixes.ns_rdfs + 'label'), null);
        if (!labelQ)
          return acc; // not a static subdir of the stepNode
        const toAdd = labelQ.object.value;
        funcDetails('new RemoteShapeTree(<%s>, "%s")', this.url.href, Path.join(pathWithinShapeTree, toAdd, '/'));
        const step = new RemoteShapeTree(this.url, Path.join(pathWithinShapeTree, toAdd, '/'));
        step.graph = this.graph; // no need to fetch it again so assign graph rather than calling .fetch()
        const nestedUrl = new URL(Path.join(toAdd, '/'), rootUrl);
        funcDetails('step.instantiateStatic(<%s>, <%s>, "%s", ManagedContainer(<%s>, ...))', nested, nestedUrl.pathname, step.path, ret.url.pathname);
        const nestedContainer = step.instantiateStatic(nested, nestedUrl, step.path, ret);
        return acc.concat(nestedContainer);
      }, []);
      const subdirs = await Promise.all(subdirPromises);
      ret.addSubdirs(subdirs);
      if (!container)
        await parent.write();
      return ret
    } catch (e) {
      await ret.remove(); // remove supplied or newly-created the Container
      if (!container)
        parent.removeMember(ret.url.href, stepNode.url);
      if (e instanceof Errors.ManagedError)
        throw e;
      throw new Errors.ShapeTreeStructureError(rootUrl.href, e.message);
    }
  }

  async validate (shape, payloadGraph, node) {
    // shape is a URL with a fragement. shapeBase is that URL without the fragment.
    const shapeBase = noHash(new URL(shape));
    let schemaResp = await Errors.getOrThrow(cachingFetch, shapeBase); // throws if unresolvable
    // const schemaType = schemaResp.headers.get('content-type').split(/; */)[0];
    const schemaPrefixes = {};
    const schema = ShExParser.construct(shapeBase.href, schemaPrefixes, {})
          .parse(await schemaResp.text());
    const v = ShExCore.Validator.construct(schema);
    let res
    try {
      Log.extend('ShapeTree')('validate graph (%d triples) with ShapeMap <%s>@<%s>', payloadGraph.size, node, shape);
      res = v.validate(ShExCore.Util.makeN3DB(payloadGraph), node, shape);
    } catch (e) {
      throw new Errors.MissingShapeError(shape, e.message);
    }
    if ('errors' in res) {
      // We could log this helpful server-side debugging info:
      //   console.warn(ShExCore.Util.errsToSimple(res).join('\n'));
      //   console.warn(`<${node}>@<${shape}>`);
      //   console.warn(payloadGraph.getQuads().map(q => (['subject', 'predicate', 'object']).map(pos => q[pos].value).join(' ')).join('\n'));
      throw new Errors.ValidationError(node, shape, ShExCore.Util.errsToSimple(res).join('\n'));
    }
  }

  /**
   * recursively get the list of referenced ShapeTree steps
   * @TODO:
   *   use generator?
   *   make efficient (capture and proxy nested iterators in .next())
   *   add referrers stack a la: { referrers: [<#org>, <#repo>, <#issue>, <#comment>] }
   * @returns {Iterable}
   */
  async *walkReferencedTrees (control = RemoteShapeTree.DEFAULT, via = []) {if (control instanceof URL) console.warn(Error(`HERE`).stack)
    const _RemoteShapeTree = this
    yield* walkLocalTree(this.url, control, via)

    // Iterate over this ShapeTree.
    async function *walkLocalTree (from, control, via = []) {
      if (!(from.href in _RemoteShapeTree.ids))
        throw new Errors.ShapeTreeStructureError(from, `not found in [${Object.keys(_RemoteShapeTree.ids)}]`);
      const step = _RemoteShapeTree.ids[from.href]

      // Queue contents and references.
      if ('contains' in step)
        for (const i in step.contains) {
          const r = step.contains[i]

          // Steps have URLs so reference by id.
          const result = {type: 'contains', target: r['@id']}
          if (control & RemoteShapeTree.REPORT_CONTAINS) // Only report references (for now).
            control = defaultControl(yield { result, via }, control)

          if (control & RemoteShapeTree.RECURSE_CONTAINS)
            yield *visit(r['@id'], result)
        }

      if ('references' in step)
        for (const i in step.references) {
          const r = step.references[i]

          // References don't have URLs so so include verbatim.
          const result = {type: 'reference', target: r}
          if (control & RemoteShapeTree.REPORT_REERENCES)
            control = defaultControl(yield { result, via }, control)

          if (control & RemoteShapeTree.RECURSE_REERENCES)
            yield *visit(r['treeStep'], result)
        }

      async function *visit (stepName, result) {
        // Some links may be URLs out of this RemoteShapeTree.
        if (!(via.find(v => (v.type === 'contains'
                             ? v.target
                             : v.target.treeStep)
                       .href === stepName.href)))
          yield* noHash(stepName).href === noHash(_RemoteShapeTree.url).href

            // (optimization) In-tree links can recursively call this generator.
            ? walkLocalTree(stepName, control, via.concat(result))

            // (general case) Parse a new RemoteShapeTree.
            : (await RemoteShapeTree.get(stepName))
          .walkReferencedTrees(control, via.concat(result))
      }
    }
  }
}
RemoteShapeTree.REPORT_CONTAINS = 0x1
RemoteShapeTree.REPORT_REERENCES = 0x2
RemoteShapeTree.RECURSE_CONTAINS = 0x4
RemoteShapeTree.RECURSE_REERENCES = 0x8
RemoteShapeTree.DEFAULT = 0xE

  /**
   * recursively get the list of resources referenced by instance data corresponding to ShapeTree references.
   * @TODO:
   *   use generator?
   *   make efficient (capture and proxy nested iterators in .next())
   *   add referrers stack a la: { referrers: [<#org>, <#repo>, <#issue>, <#comment>] }
   * @returns {Iterable}
   */
  async function* walkReferencedResources (focus, control = RemoteShapeTree.DEFAULT, via = []) {
    // console.warn(focus.href, via)
    const dataResp = await Errors.getOrThrow(cachingFetch, noHash(focus));
    const dataPrefixes = {}
    const dataGraph = await rdfInterface.parseRdf(await dataResp.text(), noHash(focus), 'text/turtle', dataPrefixes);

    const mdPrefixes = {}
    const focusMetaData = await getMetaData(focus, mdPrefixes)
    const tree = await (new RemoteShapeTree(focusMetaData.shapeTreeRoot)).fetch()
    const step = tree.ids[focusMetaData.shapeTreeRoot.href]
    const validatedBy = step.validatedBy.href
    const shapeBase = noHash(new URL(validatedBy));
    let schemaResp = await Errors.getOrThrow(cachingFetch, shapeBase);
    // const schemaType = schemaResp.headers.get('content-type').split(/; */)[0];
    const schema = ShExParser.construct(shapeBase.href, {}, {index: true})
          .parse(await schemaResp.text());
    const st = await RemoteShapeTree.get(focusMetaData.shapeTreeRoot)

    // Set up a ShEx semantic action to push referees into a queue
    const pathResolver = new ShExPath(schema, {
      base: shapeBase.href,
      prefixes: schema._prefixes
    })
    // pick some consistent name. could be random.
    const FollowName = 'http://shex.io/extensions/Follow/#'
    const references = st.ids[focusMetaData.shapeTreeRoot.href].references || []
    const queue = []
    references.map(ref => {
      const tes = pathResolver.search(ref.shapePath)
      tes.map(te => {
        te.semActs = [
          {
            type: 'SemAct',
            name: FollowName,
            code: JSON.stringify(ref)
          }
        ]
      })
      return tes
    })
    const v = ShExCore.Validator.construct(schema);
    v.semActHandler.register(
      FollowName,
      {
        dispatch: function (code, ctx, extensionStorage) {
          // @@ selection criteria could be set by `control`
          // if (!(via.find(v => v.resource.href === ctx.object)))
          queue.push({resource: new URL(ctx.object), reference: JSON.parse(code)})
        }
      }
    )

    Log.extend('ShapeTree')('validate graph (%d triples) with ShapeMap <%s>@<%s>', dataGraph.size, focus.href, validatedBy);
    let res = v.validate(ShExCore.Util.makeN3DB(dataGraph), focus.href, validatedBy);

    for (const i in queue) {
      const match = queue[i]

      // References don't have URLs so so include verbatim.
      const result = {type: 'reference', target: match.reference, resource: match.resource}
      if (control & RemoteShapeTree.REPORT_REERENCES)
        control = defaultControl(yield { result, via }, control)

      if (control & RemoteShapeTree.RECURSE_REERENCES)
        yield* walkReferencedResources(match.resource, control, via.concat(result))
    }
  }

  function defaultControl (newValue, oldValue) {
    return newValue === undefined
      ? oldValue
      : newValue
  }

  async function getMetaData (focus, prefixes) {
    const g = await storage.readMetaData(focus, prefixes)
    if (g.size === 0)
      throw Error(`No metadata for ${focus.href}`)
    const s = namedNode(noHash(focus).href)
    return {
      shapeTreeRoot: new URL(rdfInterface.one(g, s, namedNode(Prefixes.ns_tree + 'shapeTreeRoot'), null).object.value),
      shapeTreeInstancePath: rdfInterface.one(g, s, namedNode(Prefixes.ns_tree + 'shapeTreeInstancePath'), null).object.value,
      shapeTreeInstanceRoot: new URL(rdfInterface.one(g, s, namedNode(Prefixes.ns_tree + 'shapeTreeInstanceRoot'), null).object.value),
      validationFocus: new URL(rdfInterface.one(g, s, namedNode(Prefixes.ns_tree + 'validationRoot'), null).object.value),
    }
  }

  function noHash (url) {
    const u = url.href
    return new URL(u.substr(0, u.length - url.hash.length))
  }

  const fsHash = storage.hashCode();
  /* istanbul ignore if */if (ShapeTreeFunctions[fsHash])
    return ShapeTreeFunctions[fsHash];

  return ShapeTreeFunctions[fsHash] = {
    RemoteResource,
    RemoteShapeTree,
    Container,
    ManagedContainer,
    loadContainer,
    mergeTreeMetadata,
    walkReferencedResources,
  }
}

module.exports = ShapeTreeFunctions;
