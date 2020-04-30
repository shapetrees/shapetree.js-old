// const shapetree = require('../../../shapetrees/util/shapetree.js')

console.log(blues.Blueprints)

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
const popupUri = 'popup.html'
$('#login  button').click(() => solid.auth.popupLogin({ popupUri }))
$('#logout button').click(() => solid.auth.logout())

// Update components to match the user's login status
solid.auth.trackSession(session => {
  const loggedIn = !!session
  $('#login').toggle(!loggedIn)
  $('#logout').toggle(loggedIn)
  if (loggedIn) {
    $('#user').text(session.webId)
    // Use the user's WebID as default profile
    if (!$('#profile').val())
      $('#profile').val(session.webId)
  }
})

class ShapeTreeManager {
  constructor (bypass) {
    this.flush()
    this.bypass = bypass
  }

  flush () {
    // In principe, fetcher's cache should serve here.
    // Including cache code to measure and verify that hypothesis.
    this.known = {}
  }

  makeFetcher (store) {
    console.warn(Array.from(arguments))
    const _ShapeTreeManager = this
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
      if (_ShapeTreeManager.bypass()) {
        console.warn('bypassing pendingFetchPromise')
        return oldPFP.apply(fetcher, argArray)
      }
      console.warn('pendingFetchPromise', argArray)
      const parentUri = new URL('.', new URL(argArray[0].uri || argArray[0]))
            .href
      const parentPromise =
            fetcher._fetch(parentUri)
            parentUri in _ShapeTreeManager.known ?
            Promise.resolve(_ShapeTreeManager.known[parentUri]) :
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
        _ShapeTreeManager.known[uri] = resp
      return resp
    }
  }
}

const RdfTypes = ['text/turtle', 'application/json']
const Needs = {
  turtle: ['PUT', 'POST'],
  slug: ['POST']
}

const Ctls = ([
  'manifest', 'view', 'data', 'turtle', 'image', 'directory', 'hex', 'json', 'location', 'intercept', 'shapetree', 'intercept', 'mediatype', 'slug', 'method', 'result'
]).reduce((acc, key) => {
  acc[key] = $('#' + key)
  return acc
}, {})

const TheMan = new ShapeTreeManager( // don't let the man keep you down
  () => !Ctls.intercept.is(':checked') // respect intercept button
)

Ctls.view.on('change', evt => {
  Ctls.data.children().each((idx, elt) => {
    const jelt = $(elt);
    const id = jelt.prop('id');
    const showMe = Ctls.view.val();
    if (id === showMe)
      jelt.show();
    else
      jelt.hide();
  })
});
// Switch to Turtle view onload.
Ctls.view.val('turtle').trigger('change')

const Args = window.location.search.substr(1).split(/&/).reduce((acc, pair) => {
  const [attr, val] = pair.split(/=/).map(decodeURIComponent)
  acc[attr] = val
  return acc
}, {});
if ('manifest' in Args) {
  fetch(Args.manifest).then(
    async resp => {
      Ctls.manifest.append(
        $('<span/>', {class: 'source'}).append(
          $('<a/>', {href: Args.manifest}).text('loaded manifest')
        )
      );

      const j = await resp.json()
      const ul = $('<ul/>')
      for(let label in j) {
        const li = $('<li/>').append($('<button/>').text(label).on('click', evt => {
          Ctls.mediatype.removeClass('error')
          Ctls.turtle.removeClass('error')
          for (let key in j[label])
            Ctls[key].val(j[label][key]).change()
        }))
        ul.append(li)
      }
      Ctls.manifest.append(ul)
    },
    e => {
      Ctls.manifest.addClass('error').text(e.stack || e)
    }
  )
}
if ('location' in Args) {
  Ctls.location.val(Args.location)
  if (Args.immediate)
    process(Args.location)
}

$("body").keydown(function (e) { // keydown because we need to preventDefault
  var code = e.keyCode || e.charCode; // standards anyone?
  if (e.ctrlKey && (code === 10 || code === 13)) {
    var at = $(":focus");
    $("#fetch").focus().click();
    at.focus();
    return false; // same as e.preventDefault();
  } else {
    return true;
  }
});

$('#fetch').click(evt => {
  const members = $('input[name=member]:checked').prop("checked", false).get()
  if (members.length > 0)
    members.map(m => process(m.getAttribute('value')))
  else
    process(Ctls.location.val())
})

async function process (docuri) {
  docuri = docuri.replace(/^</, '').replace(/>$/, '').split('#')[0] // remove <>s and #
  Args.location = docuri
  window.history.pushState( {} , 'ShapeTree user ' + docuri, '?' + Object.keys(Args).map(k => `${k}=${encodeURIComponent(Args[k])}`).join('&'))

  const store = $rdf.graph()
  const fetcher = TheMan.makeFetcher(store) // new $rdf.Fetcher(store)
  // fetcher.timeout = 30000
  // ([Ctls.location, Ctls.turtle]).forEach(elt => elt.removeClass('error')) 3TF doesn't this work?
  Ctls.mediatype.removeClass('error')
  Ctls.turtle.removeClass('error')

  try {
    let response
    if (Ctls.method.val() === 'PLANT') {
      const link = ['<http://www.w3.org/ns/ldp#Container>; rel="type"',
                    `<${Ctls.shapetree.val()}>; rel="shapeTree"`];
      const fetchOpts = {
        contentType: Ctls.mediatype.val(),
        acceptString: Ctls.mediatype.val(),
        data: Ctls.turtle.val(),
        headers: {
          slug: Ctls.slug.val(),
          link: link
        }
      }
      response = await fetcher.webOperation('POST', docuri, fetchOpts)
    } else if (Ctls.method.val() === 'GET') {
      // GET may be invoked by either webOperation or load (which
      // calls pendingFetchPromise)
      response = await fetcher.load(docuri)
    } else {
      const fetchOpts = Object.assign(
        {contentType: Ctls.mediatype.val(), acceptString: Ctls.mediatype.val() },
        Needs.turtle.indexOf(Ctls.method.val()) !== -1 ? {turtle: Ctls.turtle.val()} : {},
        Needs.slug.indexOf(Ctls.method.val()) !== -1 ? {headers: {slug: Ctls.slug.val()}} : {}
      )
      response = await fetcher.webOperation(Ctls.method.val(), docuri, fetchOpts)
    }
    const links = response.headers.get('link')
          ? parseLinkHeader(response.headers.get('link'))
          : null
    // console.warn(response)

    const elts = [{name: 'content-type', elt: Ctls.mediatype},
                  {name: 'location', elt: Ctls.location}]
    elts.forEach(tuple => {
      const val = response.headers.get(tuple.name)
      if (val !== null)
        tuple.elt.val(val)
    })
    const contentType = response.headers.get('content-type')
    if (!contentType) {
      Ctls.hex.val('no contents')
      Ctls.view.val('hex').trigger('change')
    } else if (contentType && contentType.startsWith('image/')) {
      Ctls.image.attr('src', docuri)
      Ctls.view.val('image').trigger('change')
    } else if (links && links.find(
      l => l.uri === 'http://www.w3.org/ns/ldp#BasicContainer'
        && l.rels.rel === 'type'
    ) && store.match(null, ns.ldp('contains'), null).length > 0) { // store is populated by fetcher.load(), not webOperation()
      const td = elt => $('<td/>').append(elt)
      Ctls.directory.find('tbody').empty().append(parseContainer(store, docuri.length).map(
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
      Ctls.view.val('directory').trigger('change')
      Ctls.turtle.val(response.responseText)
    } else if (contentType.startsWith('text/turtle')) {
      Ctls.turtle.val(response.responseText)
      Ctls.view.val('turtle').trigger('change')
    } else if (contentType.startsWith('application/json')) {
      Ctls.json.val(JSON.stringify(await response.json(), null, 2))
      Ctls.view.val('json').trigger('change')
    } else {
      const buffer = await response.arrayBuffer();
      const hex = [...new Uint8Array (buffer)]
            .map (b => b.toString (16).padStart (2, '0'))
            .join (' ');
      // let hex = '';
      // const bytes = new Uint8Array( buffer );
      // const len = bytes.byteLength;
      // for (var i = 0; i < len; i++)
      //   hex += String.fromCharCode(bytes[i]);
      Ctls.hex.val(hex)
      Ctls.view.val('hex').trigger('change')
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
    Ctls.result.text(resultText)
  } catch (e) {
    console.warn(e)
    let text = ''
    try {
      text = await e.response.text();
      if ('response' in e) {
        const contentType = e.response.headers.get('content-type')
        if (contentType && contentType.startsWith('application/json'))
          text = JSON.stringify(JSON.parse(text), null, 2)
      }
    } catch (e) {  }
    Ctls.view.val('turtle').trigger('change')
    // ([Ctls.mediatype, Ctls.turtle]).forEach(elt => elt.addClass('error'))
    Ctls.mediatype.addClass('error')
    Ctls.turtle.addClass('error')
    Ctls.turtle.val(e + '\n' + text)
    Ctls.mediatype.val('text/plain')
  }
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
