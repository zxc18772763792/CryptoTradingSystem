# Plotly.js

`plotly-2.35.2.min.js` is the unmodified full Plotly.js 2.35.2 bundle from
`plotly==5.24.1` (`plotly/package_data/plotly.min.js`). It matches the previous
CDN asset's SHA-384 integrity value:

```
sha384-cCVCZkAjYNxaYKbM8lsArLznDF/SvMFr1jcZrvOpSTCa0W40ZAdLzHCEulnUa5i7
```

The dashboard and news page serve this file locally to support charts without
external CDN access. Keep the full build: the application also uses heatmaps,
candlesticks, and other trace types.

Upstream: https://github.com/plotly/plotly.js/tree/v2.35.2

The MIT license is in `plotly-2.35.2.LICENSE`; bundled dependency notices are in
`plotly.min.js.LICENSE.txt`. Preserve these when updating the bundle, and update
the versioned filename, asset registry, and template integrity values together.
