const FOAF = $rdf.Namespace('http://xmlns.com/foaf/0.1/');

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
      const parentUri = new URL('.', new URL(argArray[0].uri || argArray[0]))
            .href
      const parentPromise = parentUri in _FootprintManager.known
            ? Promise.resolve(_FootprintManager.known[parentUri])
            : oldPFP.apply(fetcher,
                           [parentUri, parentUri,
                            fetcher.initFetchOptions(parentUri, {})])
            .then(resp => cacheMe(resp, parentUri))

      console.warn('pendingFetchPromise', argArray)
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

$('#fetch').click(async evt => {
  const [method, mediaType, location, data, slug, image, result]
        = [$('#method'), $('#media-type'), $('#location'), $('#data'), $('#slug'), $('#image'), $('#result')]
  const store = $rdf.graph()
  const fetcher = TheMan.makeFetcher(store) // new $rdf.Fetcher(store)
  // fetcher.timeout = 30000
  // ([location, data]).forEach(elt => elt.removeClass('error')) 3TF doesn't this work?
  mediaType.removeClass('error');
  data.removeClass('error');

  const docuri = location.val().split('#')[0].replace(/^</, '').replace(/>$/, '')

  try {
    if (method.val() === 'GET') {
      // callers may invoke either webOperation or load (which calls pendingFetchPromise)
      const response = await fetcher.load(docuri)
      console.warn(response)
      for (var pair of response.headers.entries())
        console.log(pair[0]+ ': '+ pair[1]);

      const ct = fetcher.store.sym('http://www.w3.org/2007/ont/httph#content-type')
      const linkResp = fetcher.store.sym('http://www.w3.org/2007/ont/link#response')
      const contentType = fetcher.store.anyStatementMatching(
        fetcher.store.anyStatementMatching(response.req, linkResp).object, ct)
            .object.value

      mediaType.val(contentType)
      if (contentType.startsWith('image/')) {
        image.attr('src', docuri)
        data.hide()
        image.show()
        $('#data').parent().click(hideImage)
      } else {
        data.val(response.responseText)
        hideImage()
      }
      result.text(serialize(arcsOut(store, response.req), docuri))
      const remainder = (({ responseText, req, ...o }) => o)(response)
      console.assert(Object.keys(remainder).length === 0)
    } else if (method.val() === 'STOMP') {
    } else {
      const fetchOpts = Object.assign(
        {contentType: mediaType.val(), acceptString: mediaType.val() },
        NeedsBody.indexOf(method.val()) !== -1 ? {data: data.val()} : {},
        NeedsSlug.indexOf(method.val()) !== -1 ? {headers: {'Slug': slug.val()}} : {}
      )

      const response = await fetcher.webOperation(method.val(), docuri, fetchOpts)
      for (var pair of response.headers.entries())
        console.log(pair[0]+ ': '+ pair[1]);

      ([{name: 'content-type', elt: location},
        {name: 'location', elt: location}]).forEach(tuple => {
          const val = response.headers.get(tuple.name)
          if (val !== null)
            tuple.elt.val(val)
        })
      data.val(response.responseText)
      result.text(JSON.stringify((({ responseText, ...o }) => o)(response), null, 2))
    }
  } catch (e) {
    console.warn(e);
    ([mediaType, data]).forEach(elt => elt.addClass('error'))
    data.val(e)
    mediaType.val('text/plain')
  }

  function hideImage (evt) {
    data.show()
    image.hide()
    $('#data').parent().off('click', hideImage);
  }
});

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

