Visit this site at [i3x.dev](https://www.i3x.dev)

## Hosting

This site is hosted on GitHub Pages. Related i3X content lives on its own subdomains:

- SDK docs: [sdk.i3x.dev](https://sdk.i3x.dev) (formerly `/sdk`)
- Conformance tests: [conformance.i3x.dev](https://conformance.i3x.dev) (formerly `/conformance`)

The old paths are kept as redirects. `sdk/index.html` and `conformance/index.html` handle the base paths, and `404.html` forwards deep links (e.g. `/sdk/faq` to `sdk.i3x.dev/faq`) with JavaScript. GitHub Pages can't send server-side redirects, so this is done in the page.
