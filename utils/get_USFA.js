function _e(P, w, e, t, i, o) {
    return w = ue(ue(w, P), ue(t, o)),
    ue(w << i | w >>> 32 - i, e)
}
function $(P, w, e, t, i, o, h) {
    return _e(w & e | ~w & t, P, w, i, o, h)
}
function ee(P, w, e, t, i, o, h) {
    return _e(w & t | e & ~t, P, w, i, o, h)
}

function te(P, w, e, t, i, o, h) {
    return _e(w ^ e ^ t, P, w, i, o, h)
}
function ie(P, w, e, t, i, o, h) {
    return _e(e ^ (w | ~t), P, w, i, o, h)
}

function $e(P) {
    let w = [], e;
    for (e = 0; e < 64; e += 4)
        w[e >> 2] = P.charCodeAt(e) + (P.charCodeAt(e + 1) << 8) + (P.charCodeAt(e + 2) << 16) + (P.charCodeAt(e + 3) << 24);
    return w
}

let Ne = "0123456789abcdef".split("");
function et(P) {
    let w = ""
      , e = 0;
    for (; e < 4; e++)
        w += Ne[P >> e * 8 + 4 & 15] + Ne[P >> e * 8 & 15];
    return w
}

function Be(P) {
    return tt(Qe(P))
}

function tt(P) {
    for (let w = 0; w < P.length; w++)
        P[w] = et(P[w]);
    return P.join("")
}

function Qe(P) {
    let w = P.length, e = [1732584193, -271733879, -1732584194, 271733878], t;
    for (t = 64; t <= P.length; t += 64)
        Ie(e, $e(P.substring(t - 64, t)));
    P = P.substring(t - 64);
    let i = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for (t = 0; t < P.length; t++)
        i[t >> 2] |= P.charCodeAt(t) << (t % 4 << 3);
    if (i[t >> 2] |= 128 << (t % 4 << 3),
    t > 55)
        for (Ie(e, i),
        t = 0; t < 16; t++)
            i[t] = 0;
    return i[14] = w * 8,
    Ie(e, i),
    e
}

function Ie(P, w) {
    let e = P[0]
      , t = P[1]
      , i = P[2]
      , o = P[3];
    e = $(e, t, i, o, w[0], 7, -680876936),
    o = $(o, e, t, i, w[1], 12, -389564586),
    i = $(i, o, e, t, w[2], 17, 606105819),
    t = $(t, i, o, e, w[3], 22, -1044525330),
    e = $(e, t, i, o, w[4], 7, -176418897),
    o = $(o, e, t, i, w[5], 12, 1200080426),
    i = $(i, o, e, t, w[6], 17, -1473231341),
    t = $(t, i, o, e, w[7], 22, -45705983),
    e = $(e, t, i, o, w[8], 7, 1770035416),
    o = $(o, e, t, i, w[9], 12, -1958414417),
    i = $(i, o, e, t, w[10], 17, -42063),
    t = $(t, i, o, e, w[11], 22, -1990404162),
    e = $(e, t, i, o, w[12], 7, 1804603682),
    o = $(o, e, t, i, w[13], 12, -40341101),
    i = $(i, o, e, t, w[14], 17, -1502002290),
    t = $(t, i, o, e, w[15], 22, 1236535329),
    e = ee(e, t, i, o, w[1], 5, -165796510),
    o = ee(o, e, t, i, w[6], 9, -1069501632),
    i = ee(i, o, e, t, w[11], 14, 643717713),
    t = ee(t, i, o, e, w[0], 20, -373897302),
    e = ee(e, t, i, o, w[5], 5, -701558691),
    o = ee(o, e, t, i, w[10], 9, 38016083),
    i = ee(i, o, e, t, w[15], 14, -660478335),
    t = ee(t, i, o, e, w[4], 20, -405537848),
    e = ee(e, t, i, o, w[9], 5, 568446438),
    o = ee(o, e, t, i, w[14], 9, -1019803690),
    i = ee(i, o, e, t, w[3], 14, -187363961),
    t = ee(t, i, o, e, w[8], 20, 1163531501),
    e = ee(e, t, i, o, w[13], 5, -1444681467),
    o = ee(o, e, t, i, w[2], 9, -51403784),
    i = ee(i, o, e, t, w[7], 14, 1735328473),
    t = ee(t, i, o, e, w[12], 20, -1926607734),
    e = te(e, t, i, o, w[5], 4, -378558),
    o = te(o, e, t, i, w[8], 11, -2022574463),
    i = te(i, o, e, t, w[11], 16, 1839030562),
    t = te(t, i, o, e, w[14], 23, -35309556),
    e = te(e, t, i, o, w[1], 4, -1530992060),
    o = te(o, e, t, i, w[4], 11, 1272893353),
    i = te(i, o, e, t, w[7], 16, -155497632),
    t = te(t, i, o, e, w[10], 23, -1094730640),
    e = te(e, t, i, o, w[13], 4, 681279174),
    o = te(o, e, t, i, w[0], 11, -358537222),
    i = te(i, o, e, t, w[3], 16, -722521979),
    t = te(t, i, o, e, w[6], 23, 76029189),
    e = te(e, t, i, o, w[9], 4, -640364487),
    o = te(o, e, t, i, w[12], 11, -421815835),
    i = te(i, o, e, t, w[15], 16, 530742520),
    t = te(t, i, o, e, w[2], 23, -995338651),
    e = ie(e, t, i, o, w[0], 6, -198630844),
    o = ie(o, e, t, i, w[7], 10, 1126891415),
    i = ie(i, o, e, t, w[14], 15, -1416354905),
    t = ie(t, i, o, e, w[5], 21, -57434055),
    e = ie(e, t, i, o, w[12], 6, 1700485571),
    o = ie(o, e, t, i, w[3], 10, -1894986606),
    i = ie(i, o, e, t, w[10], 15, -1051523),
    t = ie(t, i, o, e, w[1], 21, -2054922799),
    e = ie(e, t, i, o, w[8], 6, 1873313359),
    o = ie(o, e, t, i, w[15], 10, -30611744),
    i = ie(i, o, e, t, w[6], 15, -1560198380),
    t = ie(t, i, o, e, w[13], 21, 1309151649),
    e = ie(e, t, i, o, w[4], 6, -145523070),
    o = ie(o, e, t, i, w[11], 10, -1120210379),
    i = ie(i, o, e, t, w[2], 15, 718787259),
    t = ie(t, i, o, e, w[9], 21, -343485551),
    P[0] = ue(e, P[0]),
    P[1] = ue(t, P[1]),
    P[2] = ue(i, P[2]),
    P[3] = ue(o, P[3])
}


function ie(P, w, e, t, i, o, h) {
    return _e(e ^ (w | ~t), P, w, i, o, h)
}

let ue = function(P, w) {
    return P + w & 4294967295
};


const Ve = /^(https*:\/\/[\w-\.]*)(\/.*\.(jpeg|jpg|png))\?*(.*)$/;
function je(P) {
    return {
        hex_chr_y11: "0134cdef".split(""),
        hg: P.xy11_web
    }
}
function qe(P, w) {
    let e = w.join("-");
    const t = P.x;
    return t && (e = ""),
    t.CK.toLowerCase() + e
}
function Ze() {
    return ( () => Le.xy11_web)()
}
function Xe(P) {
    const w = P.split("_");
    return [w[0], "fc", w[1]]
}
function Ke(P) {
    const {hg: w, hex_chr_y11: e} = je(P)
      , t = "lt_" + qe({
        x: P,
        hg: w
    }, e) + Ze();
    return Xe(t).join("")
}
const Ye = ["d", "o", "t"].join("").toUpperCase()
  , Le = {
    CK: Ye
};
Le.xy11_web = "net";


function Je(P) {
    if (!P)
        return P;
    let w = P.match(Ve);
    if (!w)
        return P;
    let e = w[1]
      , t = w[2]
      , i = w[4];
    i && (i = "".concat(i, "&"));
    // let h = Math.floor(new Date().getTime() / 1e3)
    let h = 1763383278
      , n = 0
      , r = 0
      , s = "".concat(t, "-").concat(h, "-").concat(n, "-").concat(r, "-").concat(Ke(Le))
      , l = Be(s);
    return "".concat(e).concat(t, "?").concat(i, "auth_key=").concat(h, "-").concat(n, "-").concat(r, "-").concat(l)
}

// console.log(Je("https://cag-ac.ltfc.net/cagstore/673df804e7502048b9867b18/17/1_0.jpg"));
module.exports.init = function (arg1) {
    // 调用函数，并返回
    console.log(Je(arg1));
};

module.exports.init(process.argv[3]);
// module.exports.init = function (P) {
//     console.log(Je(P));
// };

// module.exports.init(process.argv[3]);
