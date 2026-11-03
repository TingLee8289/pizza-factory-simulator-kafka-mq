# Vendored third-party assets

No runtime CDN or remote fetch: every asset below is committed to the repo.

| package | version | source | file | sha256 |
|---------|---------|--------|------|--------|
| chart.js | 4.5.1 | https://registry.npmjs.org/chart.js/-/chart.js-4.5.1.tgz (tarball sha512 checked against `npm view chart.js@4.5.1 dist.integrity`) | chart.umd.min.js (package/dist/chart.umd.min.js, global `Chart`) | 48444a82d4edcb5bec0f1965faacdde18d9c17db3063d042abada2f705c9f54a |
| three | 0.186.1 | https://registry.npmjs.org/three/-/three-0.186.1.tgz (tarball sha512 checked against `npm view three@0.186.1 dist.integrity`) | three.module.js (package/build/three.module.js, ES module entry, imports ./three.core.js) | 9052042d676cb0fdc1ddfefe193053f34b7ac0513a616fdac4535d49987812ea |
| three | 0.186.1 | same tarball | three.core.js (package/build/three.core.js, required by three.module.js) | 9edde002b066a9a05676a6127f67735b62baf399bdea529f2f7e31657da769e6 |

Only the listed dist files were extracted from each tarball in a scratch directory; nothing from the tarballs was executed. For three, both `three.module.js` and `three.core.js` are required: the importmap (`"three": "/static/vendor/three.module.js"`) loads the core relative to the module.
