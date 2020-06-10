/** Permissive Fetch which allows (ignores) self-signed certificates.
 * @module fetchSelfSigned
 * @implements fetch
 */

const Https = new require("https");
const PermissiveAgent = new Https.Agent({
  rejectUnauthorized: false
});

module.exports = function fetchSelfSigned (nextFetch) {
  return function (url, options = {}) {
    return url.protocol === 'https:'
      ? nextFetch(url, Object.assign({
        agent: PermissiveAgent
      }, options))
      : nextFetch(url, options);
  }
}
