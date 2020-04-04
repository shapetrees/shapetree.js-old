const ns = {
  link: $rdf.Namespace('http://www.w3.org/2007/ont/link#'),
  http: $rdf.Namespace('http://www.w3.org/2007/ont/http#'),
  httph: $rdf.Namespace('http://www.w3.org/2007/ont/httph#'),  // headers
  rdf: $rdf.Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#'),
  rdfs: $rdf.Namespace('http://www.w3.org/2000/01/rdf-schema#'),
  dc: $rdf.Namespace('http://purl.org/dc/elements/1.1/'),
  dcterms: $rdf.Namespace('http://purl.org/dc/terms/'),
  ldp: $rdf.Namespace('http://www.w3.org/ns/ldp#'),
  media: $rdf.Namespace('http://www.w3.org/ns/iana/media-types/'),
  posix: $rdf.Namespace('http://www.w3.org/ns/posix/stat#'),
}

// Log the user in and out on click
const popupUri = 'popup.html';
$('#login  button').click(() => solid.auth.popupLogin({ popupUri }));
$('#logout button').click(() => solid.auth.logout());
$('#image').hide()

// Update components to match the user's login status
solid.auth.trackSession(session => {
  const loggedIn = !!session;
  $('#login').toggle(!loggedIn);
  $('#logout').toggle(loggedIn);
  if (loggedIn) {
    $('#user').text(session.webId);
    // Use the user's WebID as default profile
    if (!$('#profile').val())
      $('#profile').val(session.webId);
  }
});

class FootprintManager {
  constructor () {
    this.flush()
  }

  flush () {
    // In principe, fetcher's cache should serve here.
    // Including cache code to measure and verify that hypothesis.
    this.known = {}
  }

  makeFetcher (store) {
    console.warn(Array.from(arguments))
    const _FootprintManager = this
    const fetcher = new $rdf.Fetcher(store)
    const oldFetcher = fetcher.webOperation
    const oldPFP = fetcher.pendingFetchPromise
    fetcher.webOperation = function () {
      const argArray = Array.from(arguments)
      console.warn('webOperation', argArray)
      return oldFetcher.apply(fetcher, argArray)
    }
    fetcher.pendingFetchPromise = function () {
      const argArray = Array.from(arguments)
      if (argArray[2].credentials === 'omit') {
        console.warn('skipping pendingFetchPromise retry ', argArray)
        return oldPFP.apply(fetcher, argArray)
      }
      console.warn('pendingFetchPromise', argArray)
      const parentUri = new URL('.', new URL(argArray[0].uri || argArray[0]))
            .href
      const parentPromise =
            fetcher._fetch(parentUri)
            parentUri in _FootprintManager.known ?
            Promise.resolve(_FootprintManager.known[parentUri]) :
            oldPFP.apply(fetcher,
                           [parentUri, parentUri,
                            fetcher.initFetchOptions(parentUri, {})])
            .then(resp => cacheMe(resp, parentUri))

      return Promise.all([
        parentPromise,
        oldPFP.apply(fetcher, argArray)]).then(both => {
          const [parent, resource] = both
          console.warn([parent, resource])
          return resource
      })
    }
    return fetcher

    function cacheMe (resp, uri) {
      if (resp.ok)
        _FootprintManager.known[uri] = resp
      return resp
    }
  }
}

const TheMan = new FootprintManager() // don't let the man keep you down

const RdfTypes = ['text/turtle', 'application/json']
const NeedsBody = ['PUT', 'POST']
const NeedsSlug = ['POST']

const [Method, MediaType, Location, Data, Slug, Image, Directory, Result]
      = [$('#method'), $('#media-type'), $('#location'), $('#data'), $('#slug'), $('#image'), $('#directory'), $('#result')]

showData() // hide image and directory table
const Args = window.location.search.substr(1).split(/&/).reduce((acc, pair) => {
  const [attr, val] = pair.split(/=/).map(decodeURIComponent)
  acc[attr] = val
  return acc
}, {})
if ('location' in Args) {
  Location.val(Args.location)
  if (Args.immediate)
    process(Args.location)
}

$('#fetch').click(evt => {
  const members = $('input[name=member]:checked').prop("checked", false).get()
  if (members.length > 0)
    members.map(m => process(m.getAttribute('value')))
  else
    process(Location.val())
})

async function process (docuri) {
  docuri = docuri.replace(/^</, '').replace(/>$/, '').split('#')[0] // remove <>s and #
  window.history.pushState( {} , 'Footprint user ' + docuri, '?location=' +  encodeURIComponent(docuri))

  const store = $rdf.graph()
  const fetcher = TheMan.makeFetcher(store) // new $rdf.Fetcher(store)
  // fetcher.timeout = 30000
  // ([Location, Data]).forEach(elt => elt.removeClass('error')) 3TF doesn't this work?
  MediaType.removeClass('error');
  Data.removeClass('error');

  try {
    let response
    if (Method.val() === 'STOMP') {
    } else if (Method.val() === 'GET') {
      // GET may be invoked by either webOperation or load (which
      // calls pendingFetchPromise)
      response = await fetcher.load(docuri)
    } else {
      const fetchOpts = Object.assign(
        {contentType: MediaType.val(), acceptString: MediaType.val() },
        NeedsBody.indexOf(Method.val()) !== -1 ? {data: Data.val()} : {},
        NeedsSlug.indexOf(Method.val()) !== -1 ? {headers: {'slug': Slug.val()}} : {}
      )
      response = await fetcher.webOperation(Method.val(), docuri, fetchOpts)
    }
    const links = response.headers.get('link')
          ? parseLinkHeader(response.headers.get('link'))
          : null
    // console.warn(response)

    const elts = [{name: 'content-type', elt: MediaType},
                  {name: 'location', elt: Location}]
    elts.forEach(tuple => {
      const val = response.headers.get(tuple.name)
      if (val !== null)
        tuple.elt.val(val)
    })
    if (response.headers.get('content-type').startsWith('image/')) {
      Image.attr('src', docuri)
      Data.hide()
      Image.show()
      Directory.parent().hide()
      Data.prev().click(showData)
    } else if (links && links.find(
      l => l.uri === 'http://www.w3.org/ns/ldp#BasicContainer'
        && l.rels.rel === 'type'
    ) && store.match(null, ns.ldp('contains'), null).length > 0) { // only works after fetcher.load()
      const td = elt => $('<td/>').append(elt)
      Directory.empty().append(parseContainer(store, docuri.length).map(
        m => $('<tr/>').append(td($('<input/>', {
          type: 'checkbox',
          name: 'member',
          value: new URL(m.name, docuri).href
        })), td($('<a/>', {
          href: `?location=${encodeURIComponent(new URL(m.name, docuri).href)}`
        }).text(m.name)), (["role", "media", "size", "modified"]).map(
          k => td(m[k])
        ))
      ))
      Data.hide()
      Image.hide()
      Directory.parent().show()
      Data.val(response.responseText)
      Data.prev().click(showData)
    } else {
      Data.val(response.responseText)
      showData()
    }

    let resultText = ''
    for (var pair of response.headers.entries())
      resultText += ((pair[0] === 'link')
                     ? (pair[0] + ': ' + JSON.stringify(parseLinkHeader(pair[1]), null, 2) + '\n')
                     : (pair[0] + ': ' + pair[1] + '\n'))
    if ('req' in response) {
      resultText += serialize(arcsOut(store, response.req), docuri)
      const remainder = (({ responseText, req, ...o }) => o)(response)
      console.assert(Object.keys(remainder).length === 0)
    } else {
      resultText += JSON.stringify((({ responseText, ...o }) => o)(response), null, 2)
    }
    Result.text(resultText)
  } catch (e) {
    console.warn(e);
    ([MediaType, Data]).forEach(elt => elt.addClass('error'))
    Data.val(e)
    MediaType.val('text/plain')
  }
}

function showData (evt) {
  Data.show()
  Image.hide()
  Directory.parent().hide()
  Data.prev().off('click', showData);
}

function parseContainer (store, trim) {
  const entries = store.match(null, ns.ldp('contains'), null).map(q => q.object)
  return entries.map(s => ({
    name: s.value.substr(trim),
    role: val(s, ns.rdf('type'), ns.ldp('')),
    media: val(s, ns.rdf('type'), ns.media(''), v => v.replace(/#.*$/, '')),
    size: val(s, ns.posix('size')),
    modified: val(s, ns.dcterms('modified')),
  }))

  function val (s, p, oStem, f) {
    const values = store.match(s, p, null).map(q => q.object.value)
    const filtered = oStem
          ? values.filter(
            v => v.startsWith(oStem.value)
          ).map(
            v => v.substr(oStem.value.length)
          )
          : values
    const x = f ? filtered.map(f) : filtered
    return x.join(',')
  }
}

function serialize (sts, base) {
  const store = $rdf.graph()
  const sz = $rdf.Serializer(store)
  sz.suggestNamespaces(store.namespaces)
  sz.setBase(base)
  return sz.statementsToN3(sts)
}

function arcsOut (store, node, seen = []) {
  seen.push(node)
  const ret = store.match(node)
  ret.forEach(arc => {
    if (seen.indexOf(arc.object) === -1)
      [].push.apply(ret, arcsOut(store, arc.object, seen))
  })
  return ret
}

function parseLinkHeader (linkHeaderString) {
  return linkHeaderString.split(/,\s*(?=<)/).map(link => {
    const [undefined, uri, relsString] = link.match(/^<([^>]+)>(;.*)$/)
    const rels = { }
    relsString.replace(/;\s*([^=]+)="([^"]+)"/g, function (a, b, c) {
      rels[b] = c
      return ''
    })
    return { uri, rels }
  })
}
