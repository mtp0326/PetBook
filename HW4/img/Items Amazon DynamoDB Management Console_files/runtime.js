(()=>{"use strict";var e,t,r,n,o,a={},i={};function s(e){var t=i[e];if(void 0!==t)return t.exports;var r=i[e]={id:e,loaded:!1,exports:{}};return a[e].call(r.exports,r,r.exports,s),r.loaded=!0,r.exports}s.m=a,s.amdD=function(){throw new Error("define cannot be used indirect")},e=[],s.O=(t,r,n,o)=>{if(!r){var a=1/0;for(u=0;u<e.length;u++){for(var[r,n,o]=e[u],i=!0,d=0;d<r.length;d++)(!1&o||a>=o)&&Object.keys(s.O).every((e=>s.O[e](r[d])))?r.splice(d--,1):(i=!1,o<a&&(a=o));if(i){e.splice(u--,1);var l=n();void 0!==l&&(t=l)}}return t}o=o||0;for(var u=e.length;u>0&&e[u-1][2]>o;u--)e[u]=e[u-1];e[u]=[r,n,o]},s.n=e=>{var t=e&&e.__esModule?()=>e.default:()=>e;return s.d(t,{a:t}),t},r=Object.getPrototypeOf?e=>Object.getPrototypeOf(e):e=>e.__proto__,s.t=function(e,n){if(1&n&&(e=this(e)),8&n)return e;if("object"==typeof e&&e){if(4&n&&e.__esModule)return e;if(16&n&&"function"==typeof e.then)return e}var o=Object.create(null);s.r(o);var a={};t=t||[null,r({}),r([]),r(r)];for(var i=2&n&&e;"object"==typeof i&&!~t.indexOf(i);i=r(i))Object.getOwnPropertyNames(i).forEach((t=>a[t]=()=>e[t]));return a.default=()=>e,s.d(o,a),o},s.d=(e,t)=>{for(var r in t)s.o(t,r)&&!s.o(e,r)&&Object.defineProperty(e,r,{enumerable:!0,get:t[r]})},s.f={},s.e=e=>Promise.all(Object.keys(s.f).reduce(((t,r)=>(s.f[r](e,t),t)),[])),s.u=e=>({81:"date-fns-id-index-js",132:"language-fr-properties",216:"vendors",220:"date-fns-ja-index-js",231:"date-fns-ko-index-js",243:"awssdk",266:"language-de-properties",296:"partiqlPage",316:"language-ko-properties",328:"web-vitals",333:"date-fns-pt-BR-index-js",366:"date-fns-fr-index-js",370:"language-pt_BR-properties",400:"date-fns-de-index-js",496:"language-zh_CN-properties",543:"ace-builds",560:"language-ja-properties",569:"language-es-properties",610:"date-fns-it-index-js",647:"language-zh_TW-properties",665:"date-fns-zh-CN-index-js",712:"date-fns-en-US-index-js",788:"date-fns-es-index-js",903:"language-it-properties",928:"date-fns-zh-TW-index-js",956:"ace-editor",978:"language-id-properties"}[e]+".js"),s.g=function(){if("object"==typeof globalThis)return globalThis;try{return this||new Function("return this")()}catch(e){if("object"==typeof window)return window}}(),s.o=(e,t)=>Object.prototype.hasOwnProperty.call(e,t),n={},o="@amzn/dynamodb-console-assets:",s.l=(e,t,r,a)=>{if(n[e])n[e].push(t);else{var i,d;if(void 0!==r)for(var l=document.getElementsByTagName("script"),u=0;u<l.length;u++){var f=l[u];if(f.getAttribute("src")==e||f.getAttribute("data-webpack")==o+r){i=f;break}}i||(d=!0,(i=document.createElement("script")).charset="utf-8",i.timeout=120,s.nc&&i.setAttribute("nonce",s.nc),i.setAttribute("data-webpack",o+r),i.src=e),n[e]=[t];var p=(t,r)=>{i.onerror=i.onload=null,clearTimeout(c);var o=n[e];if(delete n[e],i.parentNode&&i.parentNode.removeChild(i),o&&o.forEach((e=>e(r))),t)return t(r)},c=setTimeout(p.bind(null,void 0,{type:"timeout",target:i}),12e4);i.onerror=p.bind(null,i.onerror),i.onload=p.bind(null,i.onload),d&&document.head.appendChild(i)}},s.r=e=>{"undefined"!=typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(e,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(e,"__esModule",{value:!0})},s.nmd=e=>(e.paths=[],e.children||(e.children=[]),e),s.p="/",(()=>{var e={666:0};s.f.j=(t,r)=>{var n=s.o(e,t)?e[t]:void 0;if(0!==n)if(n)r.push(n[2]);else if(666!=t){var o=new Promise(((r,o)=>n=e[t]=[r,o]));r.push(n[2]=o);var a=s.p+s.u(t),i=new Error;s.l(a,(r=>{if(s.o(e,t)&&(0!==(n=e[t])&&(e[t]=void 0),n)){var o=r&&("load"===r.type?"missing":r.type),a=r&&r.target&&r.target.src;i.message="Loading chunk "+t+" failed.\n("+o+": "+a+")",i.name="ChunkLoadError",i.type=o,i.request=a,n[1](i)}}),"chunk-"+t,t)}else e[t]=0},s.O.j=t=>0===e[t];var t=(t,r)=>{var n,o,[a,i,d]=r,l=0;if(a.some((t=>0!==e[t]))){for(n in i)s.o(i,n)&&(s.m[n]=i[n]);if(d)var u=d(s)}for(t&&t(r);l<a.length;l++)o=a[l],s.o(e,o)&&e[o]&&e[o][0](),e[o]=0;return s.O(u)},r=self.webpackChunk_amzn_dynamodb_console_assets=self.webpackChunk_amzn_dynamodb_console_assets||[];r.forEach(t.bind(null,0)),r.push=t.bind(null,r.push.bind(r))})(),s.nc=void 0})();
//# sourceMappingURL=runtime.js.map