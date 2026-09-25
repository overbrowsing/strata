/* ------------------- Hyparquet------------------- */

var hyparquet=(()=>{var Le=Object.defineProperty;var en=Object.getOwnPropertyDescriptor;var tn=Object.getOwnPropertyNames;var nn=Object.prototype.hasOwnProperty;var rn=(e,t)=>{for(var r in t)Le(e,r,{get:t[r],enumerable:!0})},on=(e,t,r,n)=>{if(t&&typeof t=="object"||typeof t=="function")for(let i of tn(t))!nn.call(e,i)&&i!==r&&Le(e,i,{get:()=>t[i],enumerable:!(n=en(t,i))||n.enumerable});return e};var fn=e=>on(Le({},"__esModule",{value:!0}),e);var Wn={};rn(Wn,{parquetMetadata:()=>re,parquetMetadataAsync:()=>G,parquetRead:()=>Re,parquetReadObjects:()=>tt,parquetSchema:()=>R});var Be=["BOOLEAN","INT32","INT64","INT96","FLOAT","DOUBLE","BYTE_ARRAY","FIXED_LEN_BYTE_ARRAY"],O=["PLAIN","GROUP_VAR_INT","PLAIN_DICTIONARY","RLE","BIT_PACKED","DELTA_BINARY_PACKED","DELTA_LENGTH_BYTE_ARRAY","DELTA_BYTE_ARRAY","RLE_DICTIONARY","BYTE_STREAM_SPLIT"],ot=["REQUIRED","OPTIONAL","REPEATED"],ft=["UTF8","MAP","MAP_KEY_VALUE","LIST","ENUM","DECIMAL","DATE","TIME_MILLIS","TIME_MICROS","TIMESTAMP_MILLIS","TIMESTAMP_MICROS","UINT_8","UINT_16","UINT_32","UINT_64","INT_8","INT_16","INT_32","INT_64","JSON","BSON","INTERVAL"],st=["UNCOMPRESSED","SNAPPY","GZIP","LZO","BROTLI","LZ4","ZSTD","LZ4_RAW"],de=["DATA_PAGE","INDEX_PAGE","DICTIONARY_PAGE","DATA_PAGE_V2"],at=["UNORDERED","ASCENDING","DESCENDING"],lt=["SPHERICAL","VINCENTY","THOMAS","ANDOYER","KARNEY"];function me(e){let t=pe(e);if(t.type===1)return{type:"Point",coordinates:Se(e,t)};if(t.type===2)return{type:"LineString",coordinates:Ne(e,t)};if(t.type===3)return{type:"Polygon",coordinates:ct(e,t)};if(t.type===4){let r=[];for(let n=0;n<t.count;n++)r.push(Se(e,pe(e)));return{type:"MultiPoint",coordinates:r}}else if(t.type===5){let r=[];for(let n=0;n<t.count;n++)r.push(Ne(e,pe(e)));return{type:"MultiLineString",coordinates:r}}else if(t.type===6){let r=[];for(let n=0;n<t.count;n++)r.push(ct(e,pe(e)));return{type:"MultiPolygon",coordinates:r}}else if(t.type===7){let r=[];for(let n=0;n<t.count;n++)r.push(me(e));return{type:"GeometryCollection",geometries:r}}else throw new Error(`Unsupported geometry type: ${t.type}`)}function pe(e){let{view:t}=e,r=t.getUint8(e.offset++)===1,n=t.getUint32(e.offset,r);e.offset+=4;let i=n%1e3,o=Math.floor(n/1e3),f=0;i>1&&i<=7&&(f=t.getUint32(e.offset,r),e.offset+=4);let s=2;return o&&s++,o===3&&s++,{littleEndian:r,type:i,dim:s,count:f}}function Se(e,t){let r=[];for(let n=0;n<t.dim;n++){let i=e.view.getFloat64(e.offset,t.littleEndian);e.offset+=8,r.push(i)}return r}function Ne(e,t){let r=[];for(let n=0;n<t.count;n++)r.push(Se(e,t));return r}function ct(e,t){let{view:r}=e,n=[];for(let i=0;i<t.count;i++){let o=r.getUint32(e.offset,t.littleEndian);e.offset+=4,n.push(Ne(e,{...t,count:o}))}return n}var ut=new TextDecoder,M={timestampFromMilliseconds(e){return new Date(Number(e))},timestampFromMicroseconds(e){return new Date(Number(e/1000n))},timestampFromNanoseconds(e){return new Date(Number(e/1000000n))},dateFromDays(e){return new Date(e*864e5)},stringFromBytes(e){return e&&ut.decode(e)},jsonFromBytes(e){return e&&JSON.parse(ut.decode(e))},geometryFromBytes(e){return e&&me({view:new DataView(e.buffer,e.byteOffset,e.byteLength),offset:0})},geographyFromBytes(e){return e&&me({view:new DataView(e.buffer,e.byteOffset,e.byteLength),offset:0})},uuidFromBytes(e){if(!e)return;let t=Array.from(e,r=>r.toString(16).padStart(2,"0")).join("");return t.slice(0,8)+"-"+t.slice(8,12)+"-"+t.slice(12,16)+"-"+t.slice(16,20)+"-"+t.slice(20,32)}};function Pe(e,t,r,n){if(t&&r.endsWith("_DICTIONARY")){let i=e;e instanceof Uint8Array&&!(t instanceof Uint8Array)&&(i=new t.constructor(e.length));for(let o=0;o<e.length;o++)i[o]=t[e[o]];return i}else return Oe(e,n)}function Oe(e,t){let{element:r,parsers:n,utf8:i=!0,schemaPath:o}=t,{type:f,converted_type:s,logical_type:l}=r,d=r.repetition_type!=="REQUIRED";if(o?.some(a=>a.element.logical_type?.type==="VARIANT")&&f==="BYTE_ARRAY"&&s!=="UTF8"&&l?.type!=="STRING")return e;if(s==="DECIMAL"){let u=10**-(r.scale||0),m=new Array(e.length);for(let p=0;p<m.length;p++)e[p]instanceof Uint8Array?m[p]=Me(e[p])*u:m[p]=Number(e[p])*u;return m}if(!s&&f==="INT96")return Array.from(e).map(a=>n.timestampFromNanoseconds(sn(a)));if(s==="DATE")return Array.from(e).map(a=>n.dateFromDays(a));if(s==="TIMESTAMP_MILLIS")return Array.from(e).map(a=>n.timestampFromMilliseconds(a));if(s==="TIMESTAMP_MICROS")return Array.from(e).map(a=>n.timestampFromMicroseconds(a));if(s==="JSON")return e.map(a=>n.jsonFromBytes(a));if(s==="BSON")throw new Error("parquet bson not supported");if(s==="INTERVAL")throw new Error("parquet interval not supported");if(l?.type==="GEOMETRY")return e.map(a=>n.geometryFromBytes(a));if(l?.type==="GEOGRAPHY")return e.map(a=>n.geographyFromBytes(a));if(l?.type==="UUID")return e.map(a=>n.uuidFromBytes(a));if(s==="UTF8"||l?.type==="STRING"||i&&f==="BYTE_ARRAY")return e.map(a=>n.stringFromBytes(a));if(s==="UINT_64"||l?.type==="INTEGER"&&l.bitWidth===64&&!l.isSigned){if(e instanceof BigInt64Array)return new BigUint64Array(e.buffer,e.byteOffset,e.length);let a=d?new Array(e.length):new BigUint64Array(e.length);for(let u=0;u<a.length;u++)a[u]=e[u];return a}if(s==="UINT_32"||l?.type==="INTEGER"&&l.bitWidth===32&&!l.isSigned){if(e instanceof Int32Array)return new Uint32Array(e.buffer,e.byteOffset,e.length);let a=d?new Array(e.length):new Uint32Array(e.length);for(let u=0;u<a.length;u++)a[u]=e[u]<0?4294967296+e[u]:e[u];return a}if(l?.type==="FLOAT16")return Array.from(e).map(Ue);if(l?.type==="TIMESTAMP"){let{unit:a}=l,u=n.timestampFromMilliseconds;a==="MICROS"&&(u=n.timestampFromMicroseconds),a==="NANOS"&&(u=n.timestampFromNanoseconds);let m=new Array(e.length);for(let p=0;p<m.length;p++)m[p]=u(e[p]);return m}return e}function Me(e){if(!e.length)return 0;let t=0n;for(let n of e)t=t*256n+BigInt(n);let r=e.length*8;return t>=2n**BigInt(r-1)&&(t-=2n**BigInt(r)),Number(t)}function sn(e){let t=(e>>64n)-2440588n,r=e&0xffffffffffffffffn;return t*86400000000000n+r}function Ue(e){if(!e)return;let t=e[1]<<8|e[0],r=t>>15?-1:1,n=t>>10&31,i=t&1023;return n===0?r*2**-14*(i/1024):n===31?i?NaN:r*(1/0):r*2**(n-15)*(1+i/1024)}function dt(e,t,r){let n=e[t],i=[],o=1;if(n.num_children)for(;i.length<n.num_children;){let f=e[t+o],s=dt(e,t+o,[...r,f.name]);o+=s.count,i.push(s)}return{count:o,element:n,children:i,path:r}}function _e(e,t){let r=dt(e,0,[]),n=[r];for(let i of t){let o=r.children.find(f=>f.element.name===i);if(!o)throw new Error(`parquet schema element not found: ${t}`);n.push(o),r=o}return n}function he(e){let t=[];function r(n){if(n.children.length)for(let i of n.children)r(i);else t.push(n.path.join("."))}return r(e),t}function De(e){let t=0;for(let{element:r}of e)r.repetition_type==="REPEATED"&&t++;return t}function ne(e){let t=0;for(let{element:r}of e.slice(1))r.repetition_type!=="REQUIRED"&&t++;return t}function pt(e){if(!e||e.element.converted_type!=="LIST"||e.children.length>1)return!1;let t=e.children[0];return!(t.children.length>1||t.element.repetition_type!=="REPEATED")}function mt(e){if(!e||e.element.converted_type!=="MAP"||e.children.length>1)return!1;let t=e.children[0];return!(t.children.length!==2||t.element.repetition_type!=="REPEATED"||t.children.find(i=>i.element.name==="key")?.element.repetition_type==="REPEATED"||t.children.find(i=>i.element.name==="value")?.element.repetition_type==="REPEATED")}function Ce(e){if(e.length!==2)return!1;let[,t]=e;return!(t.element.repetition_type==="REPEATED"||t.children.length)}function N(e){let t={},r=0;for(;e.offset<e.view.byteLength;){let n=e.view.getUint8(e.offset++),i=n&15;if(i===0)break;let o=n>>4;r=o?r+o:_t(e),t[`field_${r}`]=$e(e,i)}return t}function $e(e,t){switch(t){case 1:return!0;case 2:return!1;case 3:return e.view.getInt8(e.offset++);case 4:case 5:return _t(e);case 6:return ge(e);case 7:{let r=e.view.getFloat64(e.offset,!0);return e.offset+=8,r}case 8:{let r=U(e),n=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,r);return e.offset+=r,n}case 9:{let r=e.view.getUint8(e.offset++),n=r&15,i=r>>4;i===15&&(i=U(e));let o=n===1||n===2,f=new Array(i);for(let s=0;s<i;s++)f[s]=o?$e(e,3)===1:$e(e,n);return f}case 12:return N(e);default:throw new Error(`thrift unhandled type: ${t}`)}}function U(e){let t=0,r=0;for(;;){let n=e.view.getUint8(e.offset++);if(t|=(n&127)<<r,!(n&128))return t;r+=7}}function an(e){let t=0n,r=0n;for(;;){let n=e.view.getUint8(e.offset++);if(t|=BigInt(n&127)<<r,!(n&128))return t;r+=7n}}function _t(e){let t=U(e);return t>>>1^-(t&1)}function ge(e){let t=an(e);return t>>1n^-(t&1n)}function ht(e,t){let r=new Map,n=t?.find(({key:o})=>o==="geo")?.value,i=(n&&JSON.parse(n)?.columns)??{};for(let[o,f]of Object.entries(i)){if(f.encoding!=="WKB")continue;let s=f.edges==="spherical"?"GEOGRAPHY":"GEOMETRY",l=f.crs?.id??f.crs?.ids?.[0],d=l?`${l.authority}:${l.code.toString()}`:void 0;r.set(o,{type:s,crs:d})}for(let o=1;o<e.length;o++){let{logical_type:f,name:s,num_children:l,type:d}=e[o];if(l){o+=l;continue}d==="BYTE_ARRAY"&&!f&&(e[o].logical_type=r.get(s))}}var ln=1<<19,cn=new TextDecoder;function P(e){return e&&cn.decode(e)}async function G(e,{parsers:t,initialFetchSize:r=ln,geoparquet:n=!0}={}){if(!e||!(e.byteLength>=0))throw new Error("parquet expected AsyncBuffer");let i=Math.max(0,e.byteLength-r),o=await e.slice(i,e.byteLength),f=new DataView(o);if(f.getUint32(o.byteLength-4,!0)!==827474256)throw new Error("parquet file invalid (footer != PAR1)");let s=f.getUint32(o.byteLength-8,!0);if(s>e.byteLength-8)throw new Error(`parquet metadata length ${s} exceeds available buffer ${e.byteLength-8}`);if(s+8>r){let l=e.byteLength-s-8,d=await e.slice(l,i),c=new ArrayBuffer(s+8),a=new Uint8Array(c);return a.set(new Uint8Array(d)),a.set(new Uint8Array(o),i-l),re(c,{parsers:t,geoparquet:n})}else return re(o,{parsers:t,geoparquet:n})}function re(e,{parsers:t,geoparquet:r=!0}={}){if(!(e instanceof ArrayBuffer))throw new Error("parquet expected ArrayBuffer");let n=new DataView(e),i={...M,...t};if(n.byteLength<8)throw new Error("parquet file is too short");if(n.getUint32(n.byteLength-4,!0)!==827474256)throw new Error("parquet file invalid (footer != PAR1)");let o=n.byteLength-8,f=n.getUint32(o,!0);if(f>n.byteLength-8)throw new Error(`parquet metadata length ${f} exceeds available buffer ${n.byteLength-8}`);let s=o-f,d=N({view:n,offset:s}),c=d.field_1,a=d.field_2.map(h=>({type:Be[h.field_1],type_length:h.field_2,repetition_type:ot[h.field_3],name:P(h.field_4),num_children:h.field_5,converted_type:ft[h.field_6],scale:h.field_7,precision:h.field_8,field_id:h.field_9,logical_type:un(h.field_10)})),u=a.filter(h=>h.type),m=d.field_3,p=d.field_4.map(h=>({columns:h.field_1.map((_,w)=>({file_path:P(_.field_1),file_offset:_.field_2,meta_data:_.field_3&&{type:Be[_.field_3.field_1],encodings:_.field_3.field_2?.map(A=>O[A]),path_in_schema:_.field_3.field_3.map(P),codec:st[_.field_3.field_4],num_values:_.field_3.field_5,total_uncompressed_size:_.field_3.field_6,total_compressed_size:_.field_3.field_7,key_value_metadata:_.field_3.field_8?.map(A=>({key:P(A.field_1),value:P(A.field_2)})),data_page_offset:_.field_3.field_9,index_page_offset:_.field_3.field_10,dictionary_page_offset:_.field_3.field_11,statistics:dn(_.field_3.field_12,u[w],i),encoding_stats:_.field_3.field_13?.map(A=>({page_type:de[A.field_1],encoding:O[A.field_2],count:A.field_3})),bloom_filter_offset:_.field_3.field_14,bloom_filter_length:_.field_3.field_15,size_statistics:_.field_3.field_16&&{unencoded_byte_array_data_bytes:_.field_3.field_16.field_1,repetition_level_histogram:_.field_3.field_16.field_2,definition_level_histogram:_.field_3.field_16.field_3},geospatial_statistics:_.field_3.field_17&&{bbox:_.field_3.field_17.field_1&&{xmin:_.field_3.field_17.field_1.field_1,xmax:_.field_3.field_17.field_1.field_2,ymin:_.field_3.field_17.field_1.field_3,ymax:_.field_3.field_17.field_1.field_4,zmin:_.field_3.field_17.field_1.field_5,zmax:_.field_3.field_17.field_1.field_6,mmin:_.field_3.field_17.field_1.field_7,mmax:_.field_3.field_17.field_1.field_8},geospatial_types:_.field_3.field_17.field_2}},offset_index_offset:_.field_4,offset_index_length:_.field_5,column_index_offset:_.field_6,column_index_length:_.field_7,crypto_metadata:_.field_8,encrypted_column_metadata:_.field_9})),total_byte_size:h.field_2,num_rows:h.field_3,sorting_columns:h.field_4?.map(_=>({column_idx:_.field_1,descending:_.field_2,nulls_first:_.field_3})),file_offset:h.field_5,total_compressed_size:h.field_6,ordinal:h.field_7})),g=d.field_5?.map(h=>({key:P(h.field_1),value:P(h.field_2)})),y=P(d.field_6);return r&&ht(a,g),{version:c,schema:a,num_rows:m,row_groups:p,key_value_metadata:g,created_by:y,metadata_length:f}}function R({schema:e}){return _e(e,[])[0]}function un(e){return e?.field_1?{type:"STRING"}:e?.field_2?{type:"MAP"}:e?.field_3?{type:"LIST"}:e?.field_4?{type:"ENUM"}:e?.field_5?{type:"DECIMAL",scale:e.field_5.field_1,precision:e.field_5.field_2}:e?.field_6?{type:"DATE"}:e?.field_7?{type:"TIME",isAdjustedToUTC:e.field_7.field_1,unit:gt(e.field_7.field_2)}:e?.field_8?{type:"TIMESTAMP",isAdjustedToUTC:e.field_8.field_1,unit:gt(e.field_8.field_2)}:e?.field_10?{type:"INTEGER",bitWidth:e.field_10.field_1,isSigned:e.field_10.field_2}:e?.field_11?{type:"NULL"}:e?.field_12?{type:"JSON"}:e?.field_13?{type:"BSON"}:e?.field_14?{type:"UUID"}:e?.field_15?{type:"FLOAT16"}:e?.field_16?{type:"VARIANT",specification_version:e.field_16.field_1}:e?.field_17?{type:"GEOMETRY",crs:P(e.field_17.field_1)}:e?.field_18?{type:"GEOGRAPHY",crs:P(e.field_18.field_1),algorithm:lt[e.field_18.field_2]}:e}function gt(e){if(e.field_1)return"MILLIS";if(e.field_2)return"MICROS";if(e.field_3)return"NANOS";throw new Error("parquet time unit required")}function dn(e,t,r){return e&&{max:Y(e.field_1,t,r),min:Y(e.field_2,t,r),null_count:e.field_3,distinct_count:e.field_4,max_value:Y(e.field_5,t,r),min_value:Y(e.field_6,t,r),is_max_value_exact:e.field_7,is_min_value_exact:e.field_8}}function Y(e,t,r){let{type:n,converted_type:i,logical_type:o}=t;if(e===void 0)return e;if(n==="BOOLEAN")return e[0]===1;if(n==="BYTE_ARRAY")return r.stringFromBytes(e);let f=new DataView(e.buffer,e.byteOffset,e.byteLength);if(n==="FLOAT"&&f.byteLength===4)return f.getFloat32(0,!0);if(n==="DOUBLE"&&f.byteLength===8)return f.getFloat64(0,!0);if(n==="INT32"&&i==="DECIMAL"&&f.byteLength===4)return f.getInt32(0,!0)*10**-(t.scale||0);if(n==="INT64"&&i==="DECIMAL"&&f.byteLength===8)return Number(f.getBigInt64(0,!0))*10**-(t.scale||0);if(n==="INT32"&&i==="DATE")return r.dateFromDays(f.getInt32(0,!0));if(n==="INT64"&&i==="TIMESTAMP_MILLIS")return r.timestampFromMilliseconds(f.getBigInt64(0,!0));if(n==="INT64"&&i==="TIMESTAMP_MICROS")return r.timestampFromMicroseconds(f.getBigInt64(0,!0));if(n==="INT64"&&o?.type==="TIMESTAMP"&&o?.unit==="NANOS")return r.timestampFromNanoseconds(f.getBigInt64(0,!0));if(n==="INT64"&&o?.type==="TIMESTAMP"&&o?.unit==="MICROS")return r.timestampFromMicroseconds(f.getBigInt64(0,!0));if(n==="INT64"&&o?.type==="TIMESTAMP")return r.timestampFromMilliseconds(f.getBigInt64(0,!0));let s=i?.startsWith("UINT_")||o?.type==="INTEGER"&&!o.isSigned;return n==="INT32"&&s&&f.byteLength===4?f.getUint32(0,!0):n==="INT64"&&s&&f.byteLength===8?f.getBigUint64(0,!0):n==="INT32"&&f.byteLength===4?f.getInt32(0,!0):n==="INT64"&&f.byteLength===8?f.getBigInt64(0,!0):i==="DECIMAL"?Me(e)*10**-(t.scale||0):o?.type==="FLOAT16"?Ue(e):o?.type==="UUID"?r.uuidFromBytes(e):e}function yt(e,t,r=void 0){let n={...M,...r},i=N(e);return{null_pages:i.field_1,min_values:i.field_2.map(o=>Y(o,t,n)),max_values:i.field_3.map(o=>Y(o,t,n)),boundary_order:at[i.field_4],null_counts:i.field_5,repetition_level_histograms:i.field_6,definition_level_histograms:i.field_7}}function ie(e){let t=N(e);return{page_locations:t.field_1.map(r=>({offset:r.field_1,compressed_page_size:r.field_2,first_row_index:r.field_3})),unencoded_byte_array_data_bytes:t.field_2}}var b=0xffffffffffffffffn,j=0x9e3779b185ebca87n,oe=0xc2b2ae3d27d4eb4fn,wt=0x165667b19e3779f9n,Et=0x85ebca77c2b2ae63n,At=0x27d4eb2f165667c5n;function $(e,t){return(e<<t|e>>64n-t)&b}function K(e,t){return e=e+t*oe&b,e=$(e,31n),e*j&b}function ye(e,t){return e^=K(0n,t),e*j+Et&b}function D(e,t=0n){let r=new DataView(e.buffer,e.byteOffset,e.byteLength),n=e.byteLength,i=0,o;if(n>=32){let f=t+j+oe&b,s=t+oe&b,l=t,d=t-j&b;for(;i+32<=n;)f=K(f,r.getBigUint64(i,!0)),i+=8,s=K(s,r.getBigUint64(i,!0)),i+=8,l=K(l,r.getBigUint64(i,!0)),i+=8,d=K(d,r.getBigUint64(i,!0)),i+=8;o=$(f,1n)+$(s,7n)+$(l,12n)+$(d,18n)&b,o=ye(o,f),o=ye(o,s),o=ye(o,l),o=ye(o,d)}else o=t+At&b;for(o=o+BigInt(n)&b;i+8<=n;)o^=K(0n,r.getBigUint64(i,!0)),o=$(o,27n)*j+Et&b,i+=8;for(i+4<=n&&(o^=BigInt(r.getUint32(i,!0))*j&b,o=$(o,23n)*oe+wt&b,i+=4);i<n;)o^=BigInt(r.getUint8(i))*At&b,o=$(o,11n)*j&b,i+=1;return o^=o>>33n,o=o*oe&b,o^=o>>29n,o=o*wt&b,o^=o>>32n,o}var pn=new TextEncoder,mn=new Uint32Array([1203114875,1150766481,2284105051,2729912477,1884591559,770785867,2667333959,1550580529]);function _n(e,t){return Number((e>>32n)*BigInt(t)>>32n)}function hn(e){let t=new Uint32Array(8),r=Number(e&0xffffffffn)|0;for(let n=0;n<8;n++)t[n]=1<<(Math.imul(r,mn[n])>>>27);return t}function qe(e,t){let r=_n(t,e.length>>3)<<3,n=hn(t);for(let i=0;i<8;i++)if((e[r+i]&n[i])===0)return!1;return!0}function It(e){let t=N(e),r=t.field_1;if(typeof r!="number"||r<=0||r%32!==0||!t.field_2?.field_1||!t.field_3?.field_1||!t.field_4?.field_1)return;let{view:n,offset:i}=e;if(i+r>n.byteLength)throw new Error(`parquet bloom filter truncated: need ${r} bytes, have ${n.byteLength-i}`);let o=new Uint32Array(r>>2);for(let f=0;f<o.length;f++)o[f]=n.getUint32(i+f*4,!0);return e.offset=i+r,{numBytes:r,blocks:o}}function ke(e,t){if(e==null)return;let{type:r,converted_type:n,logical_type:i}=t;if(r==="BOOLEAN")return typeof e!="boolean"?void 0:D(new Uint8Array([e?1:0]));if(r==="FLOAT"){if(typeof e!="number")return;let o=new ArrayBuffer(4);return new DataView(o).setFloat32(0,e,!0),D(new Uint8Array(o))}if(r==="DOUBLE"){if(typeof e!="number")return;let o=new ArrayBuffer(8);return new DataView(o).setFloat64(0,e,!0),D(new Uint8Array(o))}if(r==="INT32"){if(n==="DATE"||n==="DECIMAL"||n==="TIME_MILLIS"||i?.type==="DATE"||i?.type==="TIME"||i?.type==="DECIMAL"||typeof e!="number"||!Number.isInteger(e))return;let o=new ArrayBuffer(4);return new DataView(o).setInt32(0,e|0,!0),D(new Uint8Array(o))}if(r==="INT64"){if(n==="TIMESTAMP_MILLIS"||n==="TIMESTAMP_MICROS"||n==="TIME_MICROS"||n==="DECIMAL"||i?.type==="TIMESTAMP"||i?.type==="TIME"||i?.type==="DECIMAL")return;let o;if(typeof e=="bigint")o=e;else if(typeof e=="number"&&Number.isSafeInteger(e))o=BigInt(e);else return;let f=new ArrayBuffer(8);return new DataView(f).setBigUint64(0,BigInt.asUintN(64,o),!0),D(new Uint8Array(f))}if(r==="BYTE_ARRAY")return n==="JSON"||n==="BSON"||n==="DECIMAL"||i?.type==="JSON"||i?.type==="BSON"||i?.type==="VARIANT"||i?.type==="GEOMETRY"||i?.type==="GEOGRAPHY"?void 0:typeof e=="string"?D(pn.encode(e)):e instanceof Uint8Array?D(e):void 0;if(r==="FIXED_LEN_BYTE_ARRAY")return n==="DECIMAL"||n==="INTERVAL"||i?.type==="DECIMAL"||i?.type==="UUID"||i?.type==="FLOAT16"||i?.type==="GEOMETRY"||i?.type==="GEOGRAPHY"?void 0:e instanceof Uint8Array?D(e):void 0}function xt(e){let t=new Set;return Fe(e,t),t}function Fe(e,t){if(e){if("$and"in e&&Array.isArray(e.$and)){for(let r of e.$and)Fe(r,t);return}if("$or"in e&&Array.isArray(e.$or)){for(let r of e.$or)Fe(r,t);return}if(!("$nor"in e))for(let[r,n]of Object.entries(e))r.startsWith("$")||(typeof n=="object"&&n!==null&&!Array.isArray(n)?("$eq"in n||"$in"in n)&&t.add(r):t.add(r))}}function Ye(e,t){for(let n=0;n<t.length;n+=1e4)e.push(...t.slice(n,n+1e4))}function T(e,t,r=!0){if(r?e===t:e==t)return!0;if(!e||!t||typeof e!="object"||typeof t!="object")return!1;if(e instanceof Uint8Array&&t instanceof Uint8Array){if(e.length!==t.length)return!1;for(let i=0;i<e.length;i++)if(e[i]!==t[i])return!1;return!0}if(e instanceof Date||t instanceof Date)return e instanceof Date&&t instanceof Date&&e.getTime()===t.getTime();if(Array.isArray(e)&&Array.isArray(t)){if(e.length!==t.length)return!1;for(let i=0;i<e.length;i++)if(!T(e[i],t[i],r))return!1;return!0}let n=Object.keys(e);if(n.length!==Object.keys(t).length)return!1;for(let i of n)if(!T(e[i],t[i],r))return!1;return!0}function Ge(e){if(!e)return[];if(e.length===1)return e[0];let t=[];for(let r of e)Ye(t,r);return t}var bt=new TextEncoder;function J(e){if(!e)return[];let t=[];return"$and"in e&&Array.isArray(e.$and)?t.push(...e.$and.flatMap(J)):"$or"in e&&Array.isArray(e.$or)?t.push(...e.$or.flatMap(J)):"$nor"in e&&Array.isArray(e.$nor)?t.push(...e.$nor.flatMap(J)):t.push(...Object.keys(e)),[...new Set(t)]}function Ae(e){return[...new Set(J(e).map(t=>t.split(".")[0]))]}function F(e,t,r=!0){return"$and"in t&&Array.isArray(t.$and)?t.$and.every(n=>F(e,n,r)):"$or"in t&&Array.isArray(t.$or)?t.$or.some(n=>F(e,n,r)):"$nor"in t&&Array.isArray(t.$nor)?!t.$nor.some(n=>F(e,n,r)):Object.entries(t).every(([n,i])=>{let o=wn(e,n);return typeof i!="object"||i===null||Array.isArray(i)?T(o,i,r):Object.entries(i||{}).every(([f,s])=>f==="$gt"?o!=null&&o>s:f==="$gte"?o!=null&&o>=s:f==="$lt"?o!=null&&o<s:f==="$lte"?o!=null&&o<=s:f==="$eq"?T(o,s,r):f==="$ne"?!T(o,s,r):f==="$in"?Array.isArray(s)&&vt(o,s,r):f==="$nin"?Array.isArray(s)&&!vt(o,s,r):f==="$not"?!F({value:o},{value:s},r):!0)})}function vt(e,t,r){return t.some(n=>T(e,n,r)||Array.isArray(e)&&e.some(i=>T(i,n,r)))}function Q({rowGroup:e,physicalColumns:t,filter:r,strict:n=!0,bloomFilters:i,schemaElements:o}){if(!r)return!1;if("$and"in r&&Array.isArray(r.$and))return r.$and.some(f=>Q({rowGroup:e,physicalColumns:t,filter:f,strict:n,bloomFilters:i,schemaElements:o}));if("$or"in r&&Array.isArray(r.$or))return r.$or.every(f=>Q({rowGroup:e,physicalColumns:t,filter:f,strict:n,bloomFilters:i,schemaElements:o}));if("$nor"in r&&Array.isArray(r.$nor))return!1;for(let[f,s]of Object.entries(r)){let l=t.indexOf(f);if(l===-1)continue;let d=e.columns[l].meta_data?.statistics,{min:c,max:a,min_value:u,max_value:m,null_count:p}=d||{},g=u!==void 0?u:c,y=m!==void 0?m:a,h=g!==void 0&&y!==void 0,_=i?.[f],w=o?.[f],A=F({value:null},{value:s},n)&&(p===void 0||p>0);if(h&&!A&&Lt(s,g,y,n,w))return!0;for(let[I,x]of Object.entries(s||{}))if(_&&w){if(I==="$eq"){let E=ke(x,w);if(E!==void 0&&!qe(_.blocks,E))return!0}if(I==="$in"&&Array.isArray(x)&&x.length>0){let E=!0;for(let L of x){let v=ke(L,w);if(v===void 0||qe(_.blocks,v)){E=!1;break}}if(E)return!0}}}return!1}function Lt(e,t,r,n,i){if(t===void 0||r===void 0)return!1;let o=i?.type==="FLOAT"||i?.type==="DOUBLE"||i?.logical_type?.type==="FLOAT16";for(let[f,s]of Object.entries(e||{})){let l=Z(t,s,n,i),d=Z(r,s,n,i),a=!(t instanceof Uint8Array||r instanceof Uint8Array)&&(i?.type!=="BYTE_ARRAY"||typeof s=="string"&&[...s].every(u=>u.charCodeAt(0)<=127));if(f==="$gt"&&a&&d!==void 0&&d<=0||f==="$gte"&&a&&d!==void 0&&d<0||f==="$lt"&&a&&l!==void 0&&l>=0||f==="$lte"&&a&&l!==void 0&&l>0)return!0;if(f==="$eq"){let u=Z(s,t,n,i),m=Z(s,r,n,i);if(u!==void 0&&u<0||m!==void 0&&m>0)return!0}if(f==="$ne"&&!o&&T(t,r,n)&&T(t,s,n)||f==="$in"&&Array.isArray(s)&&s.every(u=>{let m=Z(u,t,n,i),p=Z(u,r,n,i);return m!==void 0&&m<0||p!==void 0&&p>0})||f==="$nin"&&!o&&Array.isArray(s)&&T(t,r,n)&&s.some(u=>T(t,u,n)))return!0}return!1}function Z(e,t,r,n){if(n?.type==="BYTE_ARRAY")return typeof e!="string"||typeof t!="string"?void 0:Tt(bt.encode(e),bt.encode(t));if(e instanceof Uint8Array||t instanceof Uint8Array)return!(e instanceof Uint8Array)||!(t instanceof Uint8Array)?void 0:Tt(e,t);if(e<t)return-1;if(e>t)return 1;if(T(e,t,r))return 0}function Tt(e,t){let r=Math.min(e.length,t.length);for(let n=0;n<r;n++){if(e[n]<t[n])return-1;if(e[n]>t[n])return 1}return e.length<t.length?-1:e.length>t.length?1:0}function gn(e,t){return F({value:null},{value:e},t)}function we(e,t,r,n=!0){if(!e)return;if("$and"in e&&Array.isArray(e.$and)){let o;for(let f of e.$and)o=Rt(o,we(f,t,r,n));return o}if("$or"in e&&Array.isArray(e.$or)){let o=[];for(let f of e.$or){let s=we(f,t,r,n);if(!s)return;o=yn(o,s)}return o}if("$nor"in e&&Array.isArray(e.$nor))return;let i;for(let[o,f]of Object.entries(e)){let s=t[o];if(!s)continue;let l=gn(f,n),d=[];for(let c=0;c<s.pageStarts.length;c++){let a=s.pageStarts[c],u=c+1<s.pageStarts.length?s.pageStarts[c+1]:r,m=s.nullCounts?.[c],p=l&&(m===void 0||m>0);if(!(!s.nullPages[c]&&!p&&Lt(f,s.minValues[c],s.maxValues[c],n,s.element))){let y=d[d.length-1];y&&y[1]===a?y[1]=u:d.push([a,u])}}i=Rt(i,d)}return i}function Rt(e,t){if(!e)return t;if(!t)return e;let r=[],n=0,i=0;for(;n<e.length&&i<t.length;){let o=Math.max(e[n][0],t[i][0]),f=Math.min(e[n][1],t[i][1]);o<f&&r.push([o,f]),e[n][1]<t[i][1]?n++:i++}return r}function yn(e,t){let r=[],n=0,i=0;for(;n<e.length||i<t.length;){let o=i>=t.length||n<e.length&&e[n][0]<=t[i][0]?e[n++]:t[i++],f=r[r.length-1];f&&o[0]<=f[1]?f[1]=Math.max(f[1],o[1]):r.push([o[0],o[1]])}return r}function wn(e,t){let r=e;for(let n of t.split("."))r=r?.[n];return r}var An=1<<21;function St(e){let{metadata:t,rowStart:r=0,columns:n,useOffsetIndex:i=!1}=e;if(!t)throw new Error("parquetPlan requires metadata");let o=[],f=[],s=[],l=Nt(e);for(let d of l.groups){let c=Pt({...d,columns:n,useOffsetIndex:i});o.push(...c.groups),f.push(...c.fetches),s.push(...c.indexes)}return f.push(...s),{metadata:t,rowStart:r,rowEnd:l.rowEnd,columns:n,fetches:f,groups:o}}function Nt({metadata:e,rowStart:t=0,rowEnd:r=1/0,columns:n,filter:i,filterStrict:o=!0,bloomFiltersByGroup:f,schemaElements:s,pageRangesByGroup:l,pageLocationsByGroup:d}){if(!e)throw new Error("parquetPlan requires metadata");let c=R(e),a=he(c),u=i?{...Ut(c),...s}:s,m=[],p=0;for(let g=0;g<e.row_groups.length;g++){let y=e.row_groups[g],h=Number(y.num_rows),_=p+h;if(h>0&&_>t&&p<r&&!Q({rowGroup:y,physicalColumns:a,filter:i,strict:o,bloomFilters:f?.[g],schemaElements:u})){let w=Math.max(t-p,0),A=Math.min(r-p,h),I=l?.[g],x=d?.[g],E=I?I.map(([L,v])=>[Math.max(L,w),Math.min(v,A)]).filter(([L,v])=>L<v):[[w,A]];E.length>1&&(E=y.columns.every(v=>{let q=v.meta_data?.path_in_schema[0],z=v.meta_data?.path_in_schema.join(".");return n&&q&&!n.includes(q)?!0:!!(v.offset_index_offset&&v.offset_index_length)||!!(z&&x?.[z])})?En(E,y,n,x):[[E[0][0],E[E.length-1][1]]]),E.length&&m.push({rowGroup:y,groupIndex:g,groupStart:p,groupRows:h,ranges:E,pageRanges:I,pageLocations:x})}p=_}return{groups:m,rowEnd:isFinite(r)?r:p}}function Pt({rowGroup:e,groupStart:t,groupRows:r,ranges:n,columns:i,useOffsetIndex:o=!1,pageRanges:f,pageLocations:s}){let l=[],d=[],c=[],a=n.length>1||n[0][0]>0||n[0][1]<r;for(let p of e.columns){let g=p.meta_data;if(p.file_path)throw new Error("parquet file_path not supported");if(!g)throw new Error("parquet column metadata is undefined");if(i&&!i.includes(g.path_in_schema[0]))continue;let y=g.dictionary_page_offset||g.data_page_offset,h=Number(y),_=Number(y+g.total_compressed_size),w=s?.[g.path_in_schema.join(".")];if(w&&a)l.push({columnMetadata:g,pageLocations:w,range:{startByte:h,endByte:_}});else if((o||f)&&p.offset_index_offset&&p.offset_index_length&&a){let A=Number(p.offset_index_offset);l.push({columnMetadata:g,offsetIndex:{startByte:A,endByte:A+p.offset_index_length},range:{startByte:Number(y),endByte:_}})}else l.push({columnMetadata:g,range:{startByte:h,endByte:_}})}let u;for(let p of l)"pageLocations"in p||("offsetIndex"in p?c.push(p.offsetIndex):i?d.push(p.range):u&&p.range.endByte-u.startByte<=An?u.endByte=p.range.endByte:(u&&d.push(u),u={...p.range}));return u&&d.push(u),{groups:n.map(([p,g])=>({chunks:l,rowGroup:e,groupStart:t,groupRows:r,selectStart:p,selectEnd:g})),fetches:d,indexes:c}}function En(e,t,r,n){let i=t.columns.filter(f=>!r||r.includes(f.meta_data?.path_in_schema[0]||"")).map(f=>n?.[f.meta_data?.path_in_schema.join(".")||""]),o=[];for(let f of e){let s=o[o.length-1],l=s&&i.some(d=>{if(!d)return!0;let c=Bt(s,d,Number(t.num_rows)),a=Bt(f,d,Number(t.num_rows));return c[0]<=a[1]&&a[0]<=c[1]});s&&l?s[1]=f[1]:o.push([...f])}return o}function Bt([e,t],r,n){let i=1/0,o=-1/0;for(let f=0;f<r.length;f++){let s=Number(r[f].first_row_index);(f+1<r.length?Number(r[f+1].first_row_index):n)>e&&s<t&&(i=Math.min(i,f),o=f)}return[i,o]}async function Ot({file:e,metadata:t,filter:r,filterStrict:n=!0}){let i=t.row_groups.map(()=>({})),o=xt(r);if(o.size===0)return i;let f=he(R(t)),s=[];return t.row_groups.forEach((l,d)=>{if(!Q({rowGroup:l,physicalColumns:f,filter:r,strict:n}))for(let c of o){let a=f.indexOf(c);if(a===-1)continue;let u=l.columns[a]?.meta_data;if(!u?.bloom_filter_offset||!u.bloom_filter_length)continue;let m=Number(u.bloom_filter_offset),p=m+u.bloom_filter_length;s.push((async()=>{let g=await e.slice(m,p),y=It({view:new DataView(g),offset:0});y&&(i[d][c]=y)})())}}),s.length&&await Promise.all(s),i}async function Mt({file:e,metadata:t,filter:r,filterStrict:n=!0,rowStart:i=0,rowEnd:o=1/0,columns:f,bloomFiltersByGroup:s,schemaElements:l,parsers:d}){let c=t.row_groups.map(()=>{}),a=t.row_groups.map(()=>({}));if(r&&"$nor"in r&&Array.isArray(r.$nor))return{pageRangesByGroup:c,pageLocationsByGroup:a};let u=J(r);if(!u.length)return{pageRangesByGroup:c,pageLocationsByGroup:a};let m=R(t),p=he(m),g={...Ut(m),...l},y=[],h=[],_=[],w=0;if(t.row_groups.forEach((A,I)=>{let x=Number(A.num_rows),E=w+x,L=x>0&&E>i&&w<o;if(w=E,!L||Q({rowGroup:A,physicalColumns:p,filter:r,strict:n,bloomFilters:s?.[I],schemaElements:g}))return;let v={},q=0,z=new Set,nt=!1;for(let B of u){let W=p.indexOf(B);if(W===-1)continue;let S=A.columns[W];if(!S?.meta_data||!S.column_index_offset||!S.column_index_length||!S.offset_index_offset||!S.offset_index_length)continue;let k=g[B];if(!k)continue;nt=!0,z.add(B);let H=Number(S.column_index_offset),te=Number(S.offset_index_offset),le=H+S.column_index_length,ce=te+S.offset_index_length;y.push({startByte:H,endByte:le},{startByte:te,endByte:ce}),q++,h.push(async rt=>{let[Jt,Qt]=await Promise.all([rt.slice(H,le),rt.slice(te,ce)]),ue=yt({view:new DataView(Jt),offset:0},k,d),it=ie({view:new DataView(Qt),offset:0});a[I][B]=it.page_locations,v[B]={minValues:ue.min_values,maxValues:ue.max_values,nullPages:ue.null_pages,nullCounts:ue.null_counts,pageStarts:it.page_locations.map(Xt=>Number(Xt.first_row_index)),element:k}})}if(nt)for(let B of A.columns){let W=B.meta_data;if(!W)continue;let S=W.path_in_schema[0],ae=W.path_in_schema.join(".");if(f&&!f.includes(S)||z.has(ae)||!B.offset_index_offset||!B.offset_index_length)continue;z.add(ae);let k=Number(B.offset_index_offset),H=k+B.offset_index_length;y.push({startByte:k,endByte:H}),q++,h.push(async te=>{let le=await te.slice(k,H),ce=ie({view:new DataView(le),offset:0});a[I][ae]=ce.page_locations})}q&&_.push({rgIdx:I,groupRows:x,columnPages:v})}),h.length){let A=je(e,{fetches:In(y)});await Promise.all(h.map(I=>I(A)));for(let{rgIdx:I,groupRows:x,columnPages:E}of _)c[I]=we(r,E,x,n)}return{pageRangesByGroup:c,pageLocationsByGroup:a}}function In(e){let t=e.map(n=>({...n})).sort((n,i)=>n.startByte-i.startByte||n.endByte-i.endByte),r=[];for(let n of t){let i=r[r.length-1];i&&n.startByte<=i.endByte?i.endByte=Math.max(i.endByte,n.endByte):r.push(n)}return r}function Ut(e){let t={};function r(n){if(n.children.length)for(let i of n.children)r(i);else t[n.path.join(".")]=n.element}return r(e),t}function je(e,{fetches:t}){let r=t.map(({startByte:n,endByte:i})=>e.slice(n,i));return{byteLength:e.byteLength,slice(n,i=e.byteLength){let o=t.findIndex(({startByte:f,endByte:s})=>f<=n&&i<=s);if(o<0)return e.slice(n,i);if(t[o].startByte!==n||t[o].endByte!==i){let f=n-t[o].startByte,s=i-t[o].startByte;return r[o]instanceof Promise?r[o].then(l=>l.slice(f,s)):r[o].slice(f,s)}else return r[o]}}}var ze=new TextDecoder,Dt=new WeakMap;function We(e,t=M){if(Array.isArray(e))return e.map(r=>We(r,t));if(typeof e!="object")return e;if("metadata"in e){let r=xn(e.metadata),n=e.typed_value&&Ee(e.typed_value,r,t),i=e.value&&fe(Ie(e.value),r,t);return n&&i?{...i,...n}:n??i}return e}function Ee(e,t,r){if(e instanceof Date)return e;if(e&&typeof e=="object"&&!Array.isArray(e)&&!(e instanceof Uint8Array)){if("typed_value"in e&&e.typed_value!==null&&e.typed_value!==void 0)return Ee(e.typed_value,t,r);if("value"in e&&e.value instanceof Uint8Array)return fe(Ie(e.value),t,r);if("typed_value"in e||"value"in e)return null;let n={};for(let[i,o]of Object.entries(e))t.dictionary.includes(i)&&(n[i]=Ee(o,t,r));return n}return e instanceof Uint8Array?fe(Ie(e),t,r):Array.isArray(e)?e.map(n=>Ee(n,t,r)):e}function Ie(e){return{view:new DataView(e.buffer,e.byteOffset,e.byteLength),offset:0}}function xn(e){let t=Dt.get(e.buffer);t||(t=new Map,Dt.set(e.buffer,t));let r=`${e.byteOffset}:${e.byteLength}`,n=t.get(r);if(n)return n;let i=Ie(e),o=i.view.getUint8(i.offset++),f=o&15;if(f!==1)throw new Error(`parquet unsupported variant metadata version: ${f}`);let s=(o>>4&1)===1,l=(o>>6&3)+1,d=V(i,l),c=new Array(d+1);for(let p=0;p<c.length;p++)c[p]=V(i,l);let a=i.offset,u=new Array(d);for(let p=0;p<d;p++){let g=c[p],y=c[p+1],h=new Uint8Array(e.buffer,e.byteOffset+a+g,y-g);u[p]=ze.decode(h)}let m={dictionary:u,sorted:s};return t.set(r,m),m}function V(e,t){let r=0;for(let n=0;n<t;n++)r|=e.view.getUint8(e.offset+n)<<n*8;return e.offset+=t,r}function fe(e,t,r){let n=e.view.getUint8(e.offset++),i=n&3,o=n>>2;if(i===0)return bn(e,o,r);if(i===2)return vn(e,o,t,r);if(i===3)return Tn(e,o,t,r);let f=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,o);return e.offset+=o,ze.decode(f)}function bn(e,t,r){switch(t){case 0:return null;case 1:return!0;case 2:return!1;case 3:{let n=e.view.getInt8(e.offset);return e.offset+=1,n}case 4:{let n=e.view.getInt16(e.offset,!0);return e.offset+=2,n}case 5:{let n=e.view.getInt32(e.offset,!0);return e.offset+=4,n}case 6:{let n=e.view.getBigInt64(e.offset,!0);return e.offset+=8,n}case 7:{let n=e.view.getFloat64(e.offset,!0);return e.offset+=8,n}case 8:return Ve(e,4);case 9:return Ve(e,8);case 10:return Ve(e,16);case 11:{let n=e.view.getInt32(e.offset,!0);return e.offset+=4,r.dateFromDays(n)}case 12:case 13:{let n=e.view.getBigInt64(e.offset,!0);return e.offset+=8,r.timestampFromMicroseconds(n)}case 14:{let n=e.view.getFloat32(e.offset,!0);return e.offset+=4,n}case 15:return Ct(e);case 16:{let n=Ct(e);return ze.decode(n)}case 17:{let n=e.view.getBigInt64(e.offset,!0);return e.offset+=8,n}case 18:case 19:{let n=e.view.getBigInt64(e.offset,!0);return e.offset+=8,r.timestampFromNanoseconds(n)}case 20:{let n=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,16);e.offset+=16;let i=Array.from(n,o=>o.toString(16).padStart(2,"0")).join("");return`${i.slice(0,8)}-${i.slice(8,12)}-${i.slice(12,16)}-${i.slice(16,20)}-${i.slice(20)}`}default:throw new Error(`parquet unsupported variant primitive type: ${t}`)}}function vn(e,t,r,n){let i=(t&3)+1,o=(t>>2&3)+1,s=t>>4&1?V(e,4):e.view.getUint8(e.offset++),l=new Array(s);for(let a=0;a<s;a++)l[a]=V(e,o);let d=new Array(s+1);for(let a=0;a<d.length;a++)d[a]=V(e,i);let c={};for(let a=0;a<s;a++){let u=r.dictionary[l[a]],m={view:e.view,offset:e.offset+d[a]};c[u]=fe(m,r,n)}return e.offset+=d[d.length-1],c}function Tn(e,t,r,n){let i=t&3,o=t>>2&1,f=i+1,s=V(e,o?4:1),l=new Array(s+1);for(let a=0;a<l.length;a++)l[a]=V(e,f);let d=e.offset,c=new Array(s);for(let a=0;a<s;a++){let u={view:e.view,offset:d+l[a]};c[a]=fe(u,r,n)}return e.offset=d+l[l.length-1],c}function Ve(e,t){let r=e.view.getUint8(e.offset);e.offset+=1;let n;if(t===4)n=BigInt(e.view.getInt32(e.offset,!0)),e.offset+=4;else if(t===8)n=e.view.getBigInt64(e.offset,!0),e.offset+=8;else{let i=e.view.getBigUint64(e.offset,!0);n=e.view.getBigInt64(e.offset+8,!0)<<64n|i,e.offset+=16}return Number(n)*10**-r}function Ct(e){let t=e.view.getUint32(e.offset,!0);e.offset+=4;let r=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,t);return e.offset+=t,r}function He(e,t,r,n,i){let o=ne(i);if(!t?.length&&!r.length){if(!o||!n.length)return n;t=new Array(n.length).fill(o)}let f=t?.length||r.length,s=i.map(({element:p})=>p.repetition_type),l=0,d=[e],c=e,a=0,u=0,m=0;if(r[0])for(;a<s.length-2&&m<r[0];)a++,s[a]!=="REQUIRED"&&(c=c.at(-1),d.push(c),u++),s[a]==="REPEATED"&&m++;for(let p=0;p<f;p++){let g=t?.length?t[p]:o,y=r[p];for(;a&&(y<m||s[a]!=="REPEATED");)s[a]!=="REQUIRED"&&(d.pop(),u--),s[a]==="REPEATED"&&m--,a--;for(c=d.at(-1);(a<s.length-2||s[a+1]==="REPEATED")&&(u<g||s[a+1]==="REQUIRED");){if(a++,s[a]!=="REQUIRED"){let h=[];c.push(h),c=h,d.push(h),u++}s[a]==="REPEATED"&&m++}g===o?c.push(n[l++]):a===s.length-2?c.push(null):c.push([])}if(!e.length)for(let p=0;p<o;p++){let g=[];c.push(g),c=g}return e}function X(e,t,r,n=0){let i=t.path.join("."),o=t.element.repetition_type==="OPTIONAL",f=o?n+1:n;if(pt(t)){let s=t.children[0],l=f;s.children.length===1&&(s=s.children[0],l++),X(e,s,r,l);let d=s.path.join("."),c=e.get(d);if(!c)throw new Error("parquet list column missing values");o&&xe(c,n),e.set(i,c),e.delete(d);return}if(mt(t)){let s=t.children[0].element.name;X(e,t.children[0].children[0],r,f+1),X(e,t.children[0].children[1],r,f+1);let l=e.get(`${i}.${s}.key`),d=e.get(`${i}.${s}.value`);if(!l)throw new Error("parquet map column missing keys");if(!d)throw new Error("parquet map column missing values");if(l.length!==d.length)throw new Error("parquet map column key/value length mismatch");let c=$t(l,d,f);o&&xe(c,n),e.delete(`${i}.${s}.key`),e.delete(`${i}.${s}.value`),e.set(i,c);return}if(t.children.length){let s=t.element.repetition_type==="REQUIRED"?n:n+1,l={};for(let c of t.children){X(e,c,r,s);let a=e.get(c.path.join("."));if(!a)throw new Error("parquet struct missing child data");l[c.element.name]=a}for(let c of t.children)e.delete(c.path.join("."));let d=Ft(l,s);t.element.logical_type?.type==="VARIANT"&&(d=We(d,r)),o&&xe(d,n),e.set(i,d)}}function xe(e,t){for(let r=0;r<e.length;r++)t?xe(e[r],t-1):e[r]=e[r][0]}function $t(e,t,r){let n=[];for(let i=0;i<e.length;i++)if(r)n.push($t(e[i],t[i],r-1));else if(e[i]){let o={};for(let f=0;f<e[i].length;f++){let s=t[i][f];o[e[i][f]]=s===void 0?null:s}n.push(o)}else n.push(void 0);return n}function Ft(e,t){let r=Object.keys(e),n=e[r[0]]?.length,i=[];for(let o=0;o<n;o++){let f={};for(let s of r){if(e[s].length!==n)throw new Error("parquet struct parsing error");f[s]=e[s][o]}t?i.push(Ft(f,t-1)):i.push(f)}return i}function ee(e,t,r){let n=r instanceof Int32Array,i=U(e),o=U(e);U(e);let f=ge(e),s=0;r[s++]=n?Number(f):f;let l=i/o;for(;s<t;){let d=ge(e),c=new Uint8Array(o);for(let a=0;a<o;a++)c[a]=e.view.getUint8(e.offset++);for(let a=0;a<o&&s<t;a++){let u=BigInt(c[a]);if(u){let m=0n,p=l,g=(1n<<u)-1n;for(;p&&s<t;){let y=BigInt(e.view.getUint8(e.offset))>>m&g;for(m+=u;m>=8;)m-=8n,e.offset++,m&&(y|=BigInt(e.view.getUint8(e.offset))<<u-m&g);let h=d+y;f+=h,r[s++]=n?Number(f):f,p--}p&&(e.offset+=Math.ceil((p*Number(u)+Number(m))/8))}else for(let m=0;m<l&&s<t;m++)f+=d,r[s++]=n?Number(f):f}}}function Ke(e,t,r){let n=new Int32Array(t);ee(e,t,n);for(let i=0;i<t;i++)r[i]=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,n[i]),e.offset+=n[i]}function qt(e,t,r){let n=new Int32Array(t);ee(e,t,n);let i=new Int32Array(t);ee(e,t,i);for(let o=0;o<t;o++){let f=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,i[o]);n[o]?(r[o]=new Uint8Array(n[o]+i[o]),r[o].set(r[o-1].subarray(0,n[o])),r[o].set(f,n[o])):r[o]=f,e.offset+=i[o]}}function C(e,t,r,n){n===void 0&&(n=e.view.getUint32(e.offset,!0),e.offset+=4);let i=e.offset,o=0;for(;o<r.length;){let f=U(e);if(f&1)o=Ln(e,f,t,r,o);else{let s=f>>>1;Rn(e,s,t,r,o),o+=s}}e.offset=i+n}function Rn(e,t,r,n,i){let o=r+7>>3,f=0;for(let s=0;s<o;s++)f|=e.view.getUint8(e.offset++)<<(s<<3);for(let s=0;s<t;s++)n[i+s]=f}function Ln(e,t,r,n,i){let o=t>>1<<3,f=(1<<r)-1,s=0;if(e.offset<e.view.byteLength)s=e.view.getUint8(e.offset++);else if(f)throw new Error(`parquet bitpack offset ${e.offset} out of range`);let l=8,d=0;for(;o;)d>8?(d-=8,l-=8,s>>>=8):l-d<r?(s|=e.view.getUint8(e.offset)<<l,e.offset++,l+=8):(i<n.length&&(n[i++]=s>>d&f),o--,d+=r);return i}function Ze(e,t,r,n){let i=Bn(r,n),o=new Uint8Array(t*i);for(let f=0;f<i;f++)for(let s=0;s<t;s++)o[s*i+f]=e.view.getUint8(e.offset++);if(r==="FLOAT")return new Float32Array(o.buffer);if(r==="DOUBLE")return new Float64Array(o.buffer);if(r==="INT32")return new Int32Array(o.buffer);if(r==="INT64")return new BigInt64Array(o.buffer);if(r==="FIXED_LEN_BYTE_ARRAY"){let f=new Array(t);for(let s=0;s<t;s++)f[s]=o.subarray(s*i,(s+1)*i);return f}throw new Error(`parquet byte_stream_split unsupported type: ${r}`)}function Bn(e,t){switch(e){case"INT32":case"FLOAT":return 4;case"INT64":case"DOUBLE":return 8;case"FIXED_LEN_BYTE_ARRAY":if(!t)throw new Error("parquet byteWidth missing type_length");return t;default:throw new Error(`parquet unsupported type: ${e}`)}}function se(e,t,r,n){if(r===0)return[];if(t==="BOOLEAN")return Sn(e,r);if(t==="INT32")return Nn(e,r);if(t==="INT64")return Pn(e,r);if(t==="INT96")return On(e,r);if(t==="FLOAT")return Mn(e,r);if(t==="DOUBLE")return Un(e,r);if(t==="BYTE_ARRAY")return Dn(e,r);if(t==="FIXED_LEN_BYTE_ARRAY"){if(!n)throw new Error("parquet missing fixed length");return Cn(e,r,n)}else throw new Error(`parquet unhandled type: ${t}`)}function Sn(e,t){let r=new Array(t);for(let n=0;n<t;n++){let i=e.offset+(n/8|0),o=n%8,f=e.view.getUint8(i);r[n]=(f&1<<o)!==0}return e.offset+=Math.ceil(t/8),r}function Nn(e,t){let r=(e.view.byteOffset+e.offset)%4?new Int32Array(be(e.view.buffer,e.view.byteOffset+e.offset,t*4)):new Int32Array(e.view.buffer,e.view.byteOffset+e.offset,t);return e.offset+=t*4,r}function Pn(e,t){let r=(e.view.byteOffset+e.offset)%8?new BigInt64Array(be(e.view.buffer,e.view.byteOffset+e.offset,t*8)):new BigInt64Array(e.view.buffer,e.view.byteOffset+e.offset,t);return e.offset+=t*8,r}function On(e,t){let r=new Array(t);for(let n=0;n<t;n++){let i=e.view.getBigInt64(e.offset+n*12,!0),o=e.view.getInt32(e.offset+n*12+8,!0);r[n]=BigInt(o)<<64n|i}return e.offset+=t*12,r}function Mn(e,t){let r=(e.view.byteOffset+e.offset)%4?new Float32Array(be(e.view.buffer,e.view.byteOffset+e.offset,t*4)):new Float32Array(e.view.buffer,e.view.byteOffset+e.offset,t);return e.offset+=t*4,r}function Un(e,t){let r=(e.view.byteOffset+e.offset)%8?new Float64Array(be(e.view.buffer,e.view.byteOffset+e.offset,t*8)):new Float64Array(e.view.buffer,e.view.byteOffset+e.offset,t);return e.offset+=t*8,r}function Dn(e,t){let r=new Array(t);for(let n=0;n<t;n++){let i=e.view.getUint32(e.offset,!0);e.offset+=4,r[n]=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,i),e.offset+=i}return r}function Cn(e,t,r){let n=new Array(t);for(let i=0;i<t;i++)n[i]=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,r),e.offset+=r;return n}function be(e,t,r){let n=new ArrayBuffer(r);return new Uint8Array(n).set(new Uint8Array(e,t,r)),n}var $n=[0,255,65535,16777215,4294967295];function kt(e,t,r,n,i){for(let o=0;o<i;o++)r[n+o]=e[t+o]}function Yt(e,t){let r=e.byteLength,n=t.byteLength,i=0,o=0;for(;i<r;){let f=e[i];if(i++,f<128)break}if(n&&i>=r)throw new Error("invalid snappy length header");for(;i<r;){let f=e[i],s=0;if(i++,i>=r)throw new Error("missing eof marker");if((f&3)===0){let l=(f>>>2)+1;if(l>60){if(i+3>=r)throw new Error("snappy error literal pos + 3 >= inputLength");let d=l-60;l=e[i]+(e[i+1]<<8)+(e[i+2]<<16)+(e[i+3]<<24),l=(l&$n[d])+1,i+=d}if(i+l>r)throw new Error("snappy error literal exceeds input length");kt(e,i,t,o,l),i+=l,o+=l}else{let l=0;switch(f&3){case 1:s=(f>>>2&7)+4,l=e[i]+(f>>>5<<8),i++;break;case 2:if(r<=i+1)throw new Error("snappy error end of input");s=(f>>>2)+1,l=e[i]+(e[i+1]<<8),i+=2;break;case 3:if(r<=i+3)throw new Error("snappy error end of input");s=(f>>>2)+1,l=e[i]+(e[i+1]<<8)+(e[i+2]<<16)+(e[i+3]<<24),i+=4;break;default:break}if(l===0||isNaN(l))throw new Error(`invalid offset ${l} pos ${i} inputLength ${r}`);if(l>o)throw new Error("cannot copy from before start of buffer");kt(t,o-l,t,o,s),o+=s}}if(o!==n)throw new Error("premature end of input")}function Gt(e,t,{type:r,element:n,schemaPath:i}){let o=new DataView(e.buffer,e.byteOffset,e.byteLength),f={view:o,offset:0},s,l=Fn(f,t,i),{definitionLevels:d,numNulls:c}=qn(f,t,i),a=t.num_values-c;if(t.encoding==="PLAIN")s=se(f,r,a,n.type_length);else if(t.encoding==="PLAIN_DICTIONARY"||t.encoding==="RLE_DICTIONARY"||t.encoding==="RLE"){let u=r==="BOOLEAN"?1:o.getUint8(f.offset++);u?(s=new Array(a),r==="BOOLEAN"?(C(f,u,s),s=s.map(m=>!!m)):C(f,u,s,o.byteLength-f.offset)):s=new Uint8Array(a)}else if(t.encoding==="BYTE_STREAM_SPLIT")s=Ze(f,a,r,n.type_length);else if(t.encoding==="DELTA_BINARY_PACKED")s=r==="INT32"?new Int32Array(a):new BigInt64Array(a),ee(f,a,s);else if(t.encoding==="DELTA_LENGTH_BYTE_ARRAY")s=new Array(a),Ke(f,a,s);else throw new Error(`parquet unsupported encoding: ${t.encoding}`);return{definitionLevels:d,repetitionLevels:l,dataPage:s}}function Fn(e,t,r){if(r.length>1){let n=De(r);if(n){let i=new Array(t.num_values);return C(e,Te(n),i),i}}return[]}function qn(e,t,r){let n=ne(r);if(!n)return{definitionLevels:[],numNulls:0};let i=new Array(t.num_values);C(e,Te(n),i);let o=t.num_values;for(let f of i)f===n&&o--;return o===0&&(i.length=0),{definitionLevels:i,numNulls:o}}function ve(e,t,r,n){let i,o=n?.[r];if(r==="UNCOMPRESSED")i=e;else if(o)i=o(e,t);else if(r==="SNAPPY")i=new Uint8Array(t),Yt(e,i);else throw new Error(`parquet unsupported compression codec: ${r}`);if(i?.length!==t)throw new Error(`parquet decompressed page length ${i?.length} does not match header ${t}`);return i}function jt(e,t,r){let i={view:new DataView(e.buffer,e.byteOffset,e.byteLength),offset:0},{type:o,element:f,schemaPath:s,codec:l,compressors:d}=r,c=t.data_page_header_v2;if(!c)throw new Error("parquet data page header v2 is undefined");let a=kn(i,c,s);i.offset=c.repetition_levels_byte_length;let u=Yn(i,c,s),m=t.uncompressed_page_size-c.definition_levels_byte_length-c.repetition_levels_byte_length,p=e.subarray(i.offset);c.is_compressed!==!1&&(p=ve(p,m,l,d));let g=new DataView(p.buffer,p.byteOffset,p.byteLength),y={view:g,offset:0},h,_=c.num_values-c.num_nulls;if(c.encoding==="PLAIN")h=se(y,o,_,f.type_length);else if(c.encoding==="RLE")h=new Array(_),C(y,1,h),h=h.map(w=>!!w);else if(c.encoding==="PLAIN_DICTIONARY"||c.encoding==="RLE_DICTIONARY"){let w=g.getUint8(y.offset++);h=new Array(_),C(y,w,h,m-1)}else if(c.encoding==="DELTA_BINARY_PACKED")h=o==="INT32"?new Int32Array(_):new BigInt64Array(_),ee(y,_,h);else if(c.encoding==="DELTA_LENGTH_BYTE_ARRAY")h=new Array(_),Ke(y,_,h);else if(c.encoding==="DELTA_BYTE_ARRAY")h=new Array(_),qt(y,_,h);else if(c.encoding==="BYTE_STREAM_SPLIT")h=Ze(y,_,o,f.type_length);else throw new Error(`parquet unsupported encoding: ${c.encoding}`);return{definitionLevels:u,repetitionLevels:a,dataPage:h}}function kn(e,t,r){let n=De(r);if(!n)return[];let i=new Array(t.num_values);return C(e,Te(n),i,t.repetition_levels_byte_length),i}function Yn(e,t,r){let n=ne(r);if(n){let i=new Array(t.num_values);return C(e,Te(n),i,t.definition_levels_byte_length),i}}function Te(e){return 32-Math.clz32(e)}function Je(e,{groupStart:t,selectStart:r,selectEnd:n},i,o){let{pathInSchema:f,schemaPath:s}=i,l=Ce(s),d=[],c,a,u=0,m=0,p=o&&(()=>{a&&o({pathInSchema:f,columnData:a,rowStart:t+u-a.length,rowEnd:t+u})});for(;(l?u<n:e.offset<e.view.byteLength-1)&&!(e.offset>=e.view.byteLength-1);){let g=Gn(e);if(g.type==="DICTIONARY_PAGE"){let{data:y}=Vt(e,g,i,c,void 0,0);y&&(c=Oe(y,i))}else{let y=a?.length||0,h=Vt(e,g,i,c,a,r-u);h.skipped?(d.length||(m+=h.skipped),u+=h.skipped):h.data&&a===h.data?u+=h.data.length-y:h.data&&h.data.length&&(p?.(),d.push(h.data),u+=h.data.length,a=h.data)}}return p?.(),{data:d,skipped:m}}function Vt(e,t,r,n,i,o){let{type:f,element:s,schemaPath:l,codec:d,compressors:c}=r,a=new Uint8Array(e.view.buffer,e.view.byteOffset+e.offset,t.compressed_page_size);if(e.offset+=t.compressed_page_size,t.type==="DATA_PAGE"){let u=t.data_page_header;if(!u)throw new Error("parquet data page header is undefined");if(o>u.num_values&&Ce(l))return{skipped:u.num_values};let m=ve(a,Number(t.uncompressed_page_size),d,c),{definitionLevels:p,repetitionLevels:g,dataPage:y}=Gt(m,u,r),h=Pe(y,n,u.encoding,r),_=Array.isArray(i)?i:[];return{skipped:0,data:He(_,p,g,h,l)}}else if(t.type==="DATA_PAGE_V2"){let u=t.data_page_header_v2;if(!u)throw new Error("parquet data page header v2 is undefined");if(o>u.num_rows)return{skipped:u.num_values};let{definitionLevels:m,repetitionLevels:p,dataPage:g}=jt(a,t,r),y=Pe(g,n,u.encoding,r),h=Array.isArray(i)?i:[];return{skipped:0,data:He(h,m,p,y,l)}}else if(t.type==="DICTIONARY_PAGE"){let u=t.dictionary_page_header;if(!u)throw new Error("parquet dictionary page header is undefined");let m=ve(a,Number(t.uncompressed_page_size),d,c),p={view:new DataView(m.buffer,m.byteOffset,m.byteLength),offset:0};return{skipped:0,data:se(p,f,u.num_values,s.type_length)}}else throw new Error(`parquet unsupported page type: ${t.type}`)}function Gn(e){let t=N(e),r=de[t.field_1],n=t.field_2,i=t.field_3,o=t.field_4,f=t.field_5&&{num_values:t.field_5.field_1,encoding:O[t.field_5.field_2],definition_level_encoding:O[t.field_5.field_3],repetition_level_encoding:O[t.field_5.field_4],statistics:t.field_5.field_5&&{max:t.field_5.field_5.field_1,min:t.field_5.field_5.field_2,null_count:t.field_5.field_5.field_3,distinct_count:t.field_5.field_5.field_4,max_value:t.field_5.field_5.field_5,min_value:t.field_5.field_5.field_6}},s=t.field_6,l=t.field_7&&{num_values:t.field_7.field_1,encoding:O[t.field_7.field_2],is_sorted:t.field_7.field_3},d=t.field_8&&{num_values:t.field_8.field_1,num_nulls:t.field_8.field_2,num_rows:t.field_8.field_3,encoding:O[t.field_8.field_4],definition_levels_byte_length:t.field_8.field_5,repetition_levels_byte_length:t.field_8.field_6,is_compressed:t.field_8.field_7===void 0?!0:t.field_8.field_7,statistics:t.field_8.field_8};return{type:r,uncompressed_page_size:n,compressed_page_size:i,crc:o,data_page_header:f,index_page_header:s,dictionary_page_header:l,data_page_header_v2:d}}function Wt(e,{metadata:t},r){let n=[];for(let i of r.chunks){let{path_in_schema:o}=i.columnMetadata,f=_e(t.schema,o),s={pathInSchema:o,element:f[f.length-1].element,schemaPath:f,...e,...i.columnMetadata,parsers:{...M,...e.parsers}},{startByte:l,endByte:d}=i.range;"pageLocations"in i?n.push({pathInSchema:o,data:zt(e,r,i,i.pageLocations,s)}):"offsetIndex"in i?n.push({pathInSchema:o,data:Promise.resolve(e.file.slice(i.offsetIndex.startByte,i.offsetIndex.endByte)).then(c=>{let a=ie({view:new DataView(c),offset:0}).page_locations;return zt(e,r,i,a,s)})}):n.push({pathInSchema:o,data:Promise.resolve(e.file.slice(l,d)).then(c=>{let a={view:new DataView(c),offset:0};return Je(a,r,s,e.onPage)})})}return{groupStart:r.groupStart,groupRows:r.groupRows,selectStart:r.selectStart,selectEnd:r.selectEnd,asyncColumns:n}}async function zt(e,t,r,n,i){let{data_page_offset:o,dictionary_page_offset:f}=r.columnMetadata,{selectStart:s,selectEnd:l}=t,{startByte:d,endByte:c}=r.range,a=-1,u=f||o<n[0].offset;for(let _=0;_<n.length;_++){let w=n[_],A=Number(w.first_row_index),I=_+1<n.length?Number(n[_+1].first_row_index):t.groupRows;a<0&&I>s&&(d=Number(w.offset),a=A),A<l&&(c=Number(w.offset)+w.compressed_page_size)}a<0&&(a=0);let m;if(u&&a){let _=Number(n[0].offset)-r.range.startByte,[w,A]=await Promise.all([e.file.slice(r.range.startByte,Number(n[0].offset)),e.file.slice(d,c)]),I=new Uint8Array(_+A.byteLength);I.set(new Uint8Array(w,0,_)),I.set(new Uint8Array(A),_),m=new DataView(I.buffer)}else u?m=new DataView(await e.file.slice(r.range.startByte,c)):m=new DataView(await e.file.slice(d,c));let p={view:m,offset:0},g=a?{...t,groupStart:t.groupStart+a,selectStart:t.selectStart-a,selectEnd:t.selectEnd-a}:t,{data:y,skipped:h}=Je(p,g,i,e.onPage);return{data:y,skipped:a+h}}async function Qe({asyncColumns:e},t,r,n,i){let o=await Promise.all(e.map(a=>a.data.then(({skipped:u,data:m})=>({skipped:u,data:Ge(m)})))),f=r-t;if(i==="object"){let a=Array(f);for(let u=0;u<f;u++){let m={};for(let p=0;p<e.length;p++){let{data:g,skipped:y}=o[p];m[e[p].pathInSchema[0]]=g[t+u-y]}a[u]=m}return a}let s=e.map(a=>a.pathInSchema[0]).filter(a=>!n||n.includes(a)),l=n??s,d=l.map(a=>e.findIndex(u=>u.pathInSchema[0]===a)),c=Array(f);for(let a=0;a<f;a++){let u=Array(e.length);for(let m=0;m<l.length;m++){let p=d[m];if(p<0)throw new Error(`parquet column not found: ${l[m]}`);let{data:g,skipped:y}=o[p];u[m]=g[t+a-y]}c[a]=u}return c}function Xe(e,t,r){let{asyncColumns:n}=e,i={...M,...r},o=[];for(let f of t.children)if(f.children.length){let s=n.filter(l=>l.pathInSchema[0]===f.element.name);if(!s.length)continue;o.push({pathInSchema:f.path,data:(async()=>{let l=await Promise.all(s.map(p=>p.data)),d=new Map,c=l.map(({data:p})=>Ge(p)),a=Math.max(e.selectStart??0,...l.map(p=>p.skipped)),u=Math.min(e.selectEnd??1/0,...l.map((p,g)=>p.skipped+c[g].length));for(let p=0;p<s.length;p++){let g=a-l[p].skipped,y=Math.max(0,u-a);d.set(s[p].pathInSchema.join("."),c[p].slice(g,g+y))}X(d,f,i);let m=d.get(f.element.name);if(!m)throw new Error("parquet column data not assembled");return{data:[m],skipped:a}})()})}else{let s=n.find(l=>l.pathInSchema[0]===f.element.name);s&&o.push(s)}return{...e,asyncColumns:o}}async function Ht(e){let t=await jn(e);return{options:t,plan:St(t)}}async function jn(e){let t=e.metadata??await G(e.file,e),r=R(t).children.map(f=>f.element.name),i=Ae(e.filter).filter(f=>!r.includes(f));if(i.length)throw new Error(`parquet filter columns not found: ${i.join(", ")}`);if(e.columns){let f=e.columns.filter(s=>!r.includes(s));if(f.length)throw new Error(`parquet column not found: ${f[0]}`)}let o={...e,metadata:t};return o=await Vn(o),o=await zn(o),o}function Kt(e,t){let r={...e,file:je(e.file,t)};return t.groups.map(n=>Wt(r,t,n))}async function Vn(e){if(!e.useBloomFilters||!e.filter||!e.metadata)return e;let t=R(e.metadata),r={};for(let i of t.children)r[i.element.name]=i.element;let n=await Ot({file:e.file,metadata:e.metadata,filter:e.filter,filterStrict:e.filterStrict});return{...e,bloomFiltersByGroup:n,schemaElements:r}}async function zn(e){if(!e.usePageIndex||!e.filter||!e.metadata)return e;let{pageRangesByGroup:t,pageLocationsByGroup:r}=await Mt({file:e.file,metadata:e.metadata,filter:e.filter,filterStrict:e.filterStrict,rowStart:e.rowStart,rowEnd:e.rowEnd,columns:e.columns,bloomFiltersByGroup:e.bloomFiltersByGroup,schemaElements:e.schemaElements,parsers:e.parsers});return{...e,pageRangesByGroup:t,pageLocationsByGroup:r}}var Zt=Symbol("rowIndex");async function Re(e){e.metadata??(e.metadata=await G(e.file,e));let{rowStart:t=0,rowEnd:r,columns:n,onChunk:i,onComplete:o,rowFormat:f,filter:s,filterStrict:l=!0}=e;if(s&&f!=="object")throw new Error('parquet filter requires rowFormat: "object"');if(e.includeRowIndex&&f!=="object")throw new Error('parquet includeRowIndex requires rowFormat: "object"');let d=Ae(s),c=n;if(n&&d.length){let _=new Set(n),w=d.filter(A=>!_.has(A));w.length&&(c=[...n,...w])}let a=c===n?e:{...e,columns:c},u=await Ht(a),m=u.options,p=c!==n,g=Kt(m,u.plan);if(!o&&!i){await et(g);return}if(!m.metadata)throw new Error("parquet requires metadata");let y=R(m.metadata),h=g.map(_=>Xe(_,y,e.parsers));if(i)for(let _ of h)for(let w of _.asyncColumns)w.data.then(({data:A,skipped:I})=>{let x=_.groupStart+I;for(let E of A)i({columnName:w.pathInSchema[0],columnData:E,rowStart:x,rowEnd:x+E.length}),x+=E.length},()=>{});if(o){await et(h);let _=[];for(let w of h){let A=w.selectStart??Math.max(t-w.groupStart,0),I=w.selectEnd??Math.min((r??1/0)-w.groupStart,w.groupRows),x=f==="object"?await Qe(w,A,I,c,"object"):await Qe(w,A,I,n,"array");if(e.includeRowIndex)for(let E=0;E<x.length;E++)Object.defineProperty(x[E],Zt,{value:w.groupStart+A+E});if(s){for(let E of x)if(F(E,s,l)){if(p&&n)for(let L of d)n.includes(L)||delete E[L];_.push(E)}}else Ye(_,x)}o(_)}else await et(h)}async function et(e){let t=e.flatMap(i=>i.asyncColumns.map(o=>o.data)),n=(await Promise.allSettled(t)).find(i=>i.status==="rejected");if(n)throw n.reason}function tt(e){return new Promise((t,r)=>{Re({...e,rowFormat:"object",onComplete:t}).catch(r)})}return fn(Wn);})();

/* ------------------- Config ------------------- */

const DEFAULT_DATA_DIR = "assets/data/ed.ac.uk/";
const REPO_TREE_API =
  "https://api.github.com/repos/overbrowsing/strata/git/trees/HEAD?recursive=1";
const REGISTRY_BASE =
  "https://raw.githubusercontent.com/overbrowsing/web-archive.txt/main/registry";
const REGISTRY_API =
  "https://api.github.com/repos/overbrowsing/web-archive.txt/contents/registry";
const REPLAY_FALLBACK = {
  no_toolbar: ["no_toolbar", "rewritten", "raw"],
  rewritten: ["rewritten", "no_toolbar", "raw"],
  raw: ["raw", "no_toolbar", "rewritten"],
};

const DAY_MS = 86400000;
const MONTH_MS = 30.44 * DAY_MS;
const YEAR_MS = 365.25 * DAY_MS;
const TAU = Math.PI * 2;

const MIN_PX_YEAR = 2;
const MAX_PX_YEAR = 50000;
const MIN_ROW = 0.05;
const MAX_ROW = 32;
const MAX_CANVAS_DIM = 16000;
const AXIS_H = 40;
const OVERVIEW_H = 72;
const CHROME_H = AXIS_H + OVERVIEW_H;
const BAND_H = 30;
const EDGE_PAD = 5;
const DRAW_BUFFER = 100;
const TICK_TARGET_PX = 90;
const INTRO_MS = 15000;

const DOT_RADIUS = 4;
const DOT_HIT_RADIUS = 20;
const DOT_MIN_GAP = 8;
const DOT_ZOOM_MIN = 100;
const DOT_MIN_ROW_H = 15;
const LABEL_MIN_ROW_H = 15;

const ERA_ZOOM = [0, 70, 150];
const ERA_MIN_WIDTH = 120;
const ERA_LANE_H = 18;
const ERA_LANE_GAP = 6;
const ERA_RESERVE_ZOOM = 80;

const ERAS = [
  {
    start: "1991-01-01",
    end: "1995-12-31",
    label: "Early Web",
    zoom: 0,
    lane: 0,
  },
  { start: "1993-04-30", end: "1995-12-31", label: "Mosaic", zoom: 1, lane: 1 },
  {
    start: "1994-10-01",
    end: "1999-12-31",
    label: "GeoCities",
    zoom: 2,
    lane: 2,
  },
  {
    start: "1995-01-01",
    end: "2001-03-31",
    label: "Browser Wars",
    zoom: 0,
    lane: 0,
  },
  {
    start: "1995-08-09",
    end: "2001-12-31",
    label: "Dot-com Boom",
    zoom: 1,
    lane: 1,
  },
  {
    start: "1996-01-01",
    end: "2003-12-31",
    label: "Animated GIFs",
    zoom: 2,
    lane: 2,
  },
  { start: "1997-01-01", end: "2004-12-31", label: "Flash", zoom: 1, lane: 3 },
  {
    start: "1998-09-04",
    end: "2010-12-31",
    label: "Google Search",
    zoom: 1,
    lane: 0,
  },
  {
    start: "1999-01-01",
    end: "2008-12-31",
    label: "Portal Era",
    zoom: 2,
    lane: 1,
  },
  {
    start: "2003-01-01",
    end: "2008-12-31",
    label: "Blogs & RSS",
    zoom: 1,
    lane: 2,
  },
  {
    start: "2004-01-01",
    end: "2013-12-31",
    label: "Web 2.0",
    zoom: 0,
    lane: 0,
  },
  {
    start: "2004-02-01",
    end: "2016-12-31",
    label: "Social Networks",
    zoom: 1,
    lane: 1,
  },
  {
    start: "2005-02-14",
    end: "2020-12-31",
    label: "YouTube Era",
    zoom: 2,
    lane: 2,
  },
  { start: "2005-01-01", end: "2012-12-31", label: "AJAX", zoom: 2, lane: 3 },
  {
    start: "2007-06-29",
    end: "2016-12-31",
    label: "Touch Web",
    zoom: 0,
    lane: 0,
  },
  {
    start: "2008-07-10",
    end: "2020-12-31",
    label: "App Economy",
    zoom: 1,
    lane: 1,
  },
  { start: "2008-09-02", end: "2018-12-31", label: "Chrome", zoom: 2, lane: 2 },
  {
    start: "2010-01-01",
    end: "2017-12-31",
    label: "Responsive Design",
    zoom: 1,
    lane: 3,
  },
  {
    start: "2013-01-01",
    end: "2020-12-31",
    label: "Mobile-first Web",
    zoom: 0,
    lane: 0,
  },
  {
    start: "2013-01-01",
    end: "2021-12-31",
    label: "Infinite Scroll",
    zoom: 2,
    lane: 1,
  },
  {
    start: "2014-01-01",
    end: "2021-12-31",
    label: "HTTPS Everywhere",
    zoom: 1,
    lane: 2,
  },
  {
    start: "2015-01-01",
    end: "2022-12-31",
    label: "Single-page Apps",
    zoom: 2,
    lane: 3,
  },
  {
    start: "2016-01-01",
    end: "2022-12-31",
    label: "Progressive Web Apps",
    zoom: 2,
    lane: 4,
  },
  {
    start: "2016-01-01",
    end: "2024-12-31",
    label: "Creator Economy",
    zoom: 1,
    lane: 5,
  },
  {
    start: "2020-01-01",
    end: "2023-12-31",
    label: "Remote-first Internet",
    zoom: 1,
    lane: 1,
  },
  {
    start: "2020-01-01",
    end: "2023-12-31",
    label: "NFT Boom",
    zoom: 2,
    lane: 2,
  },
  { start: "2021-01-01", end: "2024-12-31", label: "Web3", zoom: 1, lane: 3 },
  {
    start: "2021-01-01",
    end: "2024-12-31",
    label: "Crypto Mainstream",
    zoom: 2,
    lane: 4,
  },
  { start: "2021-01-01", end: "2026-12-31", label: "AI Web", zoom: 0, lane: 0 },
  {
    start: "2022-11-30",
    end: "2026-12-31",
    label: "Generative AI",
    zoom: 1,
    lane: 1,
  },
  {
    start: "2023-01-01",
    end: "2026-12-31",
    label: "LLM Search",
    zoom: 2,
    lane: 2,
  },
  {
    start: "2024-01-01",
    end: "2026-12-31",
    label: "AI Agents",
    zoom: 2,
    lane: 3,
  },
].map(era => ({
  ...era,
  startMs: Date.parse(era.start),
  endMs: Date.parse(era.end),
}));

const TOP_ERAS = ERAS.filter(era => era.zoom === 0)
  .sort((a, b) => a.startMs - b.startMs)
  .map((era, i, sorted) => ({
    label: era.label,
    startMs: era.startMs,
    endMs: i + 1 < sorted.length ? sorted[i + 1].startMs : Infinity,
  }));

const VIEW_LABELS = {
  "timeline-date": "View: Timeline (Chronological)",
  "timeline-life": "View: Timeline (Lifespan)",
  "timeline-captures": "View: Timeline (Captures)",
  "timeline-archive": "View: Timeline (Web Archive)",
  ridgeline: "View: Era Ridgeline",
  growth: "View: Growth: Born vs Died",
  survival: "View: Survival Curves (Kaplan-Meier)",
  agepyramid: "View: Age Pyramid",
};

const EMPTY_MESSAGE =
  "No hosts match the current filters — clear a filter or search to see this view.";

const CONTROLS = [
  ["Drag", "Pan"],
  ["⌘/Ctrl + scroll, + / -", "Zoom time"],
  ["Alt + scroll, [ / ]", "Resize rows"],
  ["0", "Reset view"],
  ["E", "Toggle eras"],
  ["F", "Exhibition mode"],
  ["← / →", "Previous / next capture"],
  ["Esc", "Close / exit"],
];

/* ------------------- DOM ------------------- */

const $ = id => document.getElementById(id);

const viewport = $("viewport");
const surface = $("surface");
const plotWrap = $("plot");
const plotCanvas = $("plot-canvas");
const axisCanvas = $("axis-canvas");
const overviewCanvas = $("overview-canvas");
const plotCtx = plotCanvas.getContext("2d");
const axisCtx = axisCanvas.getContext("2d");
const overviewCtx = overviewCanvas.getContext("2d");
const bands = $("bands");
const dotHighlight = $("dot-highlight");
const placard = $("placard");

const searchInput = $("search");
const sortSelect = $("sort");
const viewDropdown = $("view");
const viewTrigger = $("view-trigger");
const viewTriggerLabel = viewTrigger.querySelector("span");
const viewPanel = $("view-panel");

const ridgelineView = $("ridgeline");
const ridgelineCanvas = $("ridgeline-canvas");
const growthView = $("growth");
const growthCanvas = $("growth-canvas");
const survivalView = $("survival");
const survivalCanvas = $("survival-canvas");
const ageView = $("age");
const ageCanvas = $("age-canvas");

const archivesDropdown = $("archives");
const archivesTrigger = $("archives-trigger");
const archivesLabel = archivesTrigger.querySelector("span");
const archivesPanel = $("archives-panel");
const archivesList = $("archives-list");
const archivesAll = $("archives-all");
const archivesNone = $("archives-none");

const uploadButton = $("upload");
const folderInput = $("folder-input");
const dropHint = $("drop-hint");

const viewerEl = $("viewer");
const viewerFrame = $("viewer-frame");
const viewerHostEl = $("viewer-host");
const viewerArchiveEl = $("viewer-archive");
const viewerDateEl = $("viewer-date");
const viewerCountEl = $("viewer-count");
const viewerExternal = $("viewer-external");
const viewerStatus = $("viewer-status");
const viewerPrevBtn = $("viewer-prev");
const viewerNextBtn = $("viewer-next");
const viewerCloseBtn = $("viewer-close");

const aboutTrigger = $("about-trigger");
const aboutOverlay = $("about");
const aboutClose = $("about-close");
const reportTrigger = $("report-trigger");
const reportOverlay = $("report");
const reportClose = $("report-close");
const reportBody = $("report-body");

const dpr = window.devicePixelRatio || 1;
const cssToken = name =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const LAUREL = cssToken("--laurel");
const SEAWEED = cssToken("--seaweed");
const LIVE = cssToken("--live");
const SANS = getComputedStyle(document.body).fontFamily;
const FONT_SM = `11px ${SANS}`;

/* ------------------- State ------------------- */

let hosts = [];
let mergedHosts = [];
let visible = [];
let currentRows = [];
let allArchives = [];
let selectedArchives = new Set();

let viewMode = "timeline-date";
let fullStart = 0;
let fullEnd = 0;
let pxPerYear = 40;
let rowHeight = 6;
let plotFullHeight = 0;
let labelReserve = 60;
let erasVisible = true;

let populationCurve = [];
let captureHistogram = [];
let maxPopulation = 1;
let maxMonthlyCaptures = 1;

let playingIntro = false;
let introClipX = null;
let introId = 0;
let introRedraw = null;
let pendingIntro = null;
let aboutDismissed = false;
let dragDepth = 0;
let scrollFrame = null;
let dragging = false;
let dragMoved = false;
let dragX = 0;
let dragY = 0;

let viewerHost = null;
let viewerMementos = [];
let viewerIndex = -1;
let viewerRequestId = 0;

/* ------------------- Utilities ------------------- */

const HTML_ESCAPES = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
};
const esc = value => String(value).replace(/[&<>"']/g, c => HTML_ESCAPES[c]);
const ink = alpha => `rgba(${LAUREL},${alpha})`;
const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
const pad2 = n => String(n).padStart(2, "0");
const bump = (map, key) => map.set(key, (map.get(key) || 0) + 1);

const toWayback = ms => {
  const d = new Date(ms);
  return (
    `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}${pad2(d.getUTCDate())}` +
    `${pad2(d.getUTCHours())}${pad2(d.getUTCMinutes())}${pad2(d.getUTCSeconds())}`
  );
};

const prettyDate = ms => {
  return new Date(ms).toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
};

const prettyDateTime = ms => {
  return new Date(ms).toLocaleString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
};

const formatSpan = ms => {
  const days = Math.max(1, Math.floor(ms / DAY_MS));
  const years = Math.floor(days / 365);
  const months = Math.floor((days % 365) / 30);
  const plural = (n, unit) => `${n} ${unit}${n === 1 ? "" : "s"}`;
  if (years || months)
    return [years && plural(years, "year"), months && plural(months, "month")]
      .filter(Boolean)
      .join(" ");
  return plural(days % 30, "day");
};

const median = nums => {
  if (!nums.length) return 0;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

const sizeCanvas = (canvas, width, height) => {
  const largest = Math.max(width, height) * dpr;
  const scale =
    largest > MAX_CANVAS_DIM ? dpr * (MAX_CANVAS_DIM / largest) : dpr;
  canvas.width = Math.max(1, Math.round(width * scale));
  canvas.height = Math.max(1, Math.round(height * scale));
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(scale, 0, 0, scale, 0, 0);
  return ctx;
};

/* ------------------- web-archive.txt registry ------------------- */

const descriptorCache = new Map();
const replayCache = new Map();
let registryIdsPromise = null;

const tomlSection = (text, name) => {
  const lines = text.split(/\r?\n/);
  const start = lines.findIndex(line => line.trim() === `[${name}]`);
  if (start < 0) return "";
  const end = lines.findIndex((line, i) => i > start && /^\s*\[/.test(line));
  return lines.slice(start + 1, end < 0 ? undefined : end).join("\n");
};

const parseArchiveDescriptor = text => {
  const archive = tomlSection(text, "archive");
  const replaySection = tomlSection(text, "replay");
  const id = archive.match(/^\s*id\s*=\s*"([^"]+)"/m)?.[1] ?? null;
  const nameValue =
    archive.match(/^\s*name\s*=\s*(\[[\s\S]*?\]|"[^"]*")\s*$/m)?.[1] ?? "";
  const names = (nameValue.match(/"([^"]*)"/g) || []).map(q => q.slice(1, -1));
  const replay = {};
  for (const mode of ["rewritten", "no_toolbar", "raw"]) {
    const match = replaySection.match(
      new RegExp(`^\\s*${mode}\\s*=\\s*"([^"]*)"`, "m")
    );
    if (match) replay[mode] = match[1];
  }
  return { id, names, replay };
};

const registryIds = () => {
  registryIdsPromise ??= fetch(REGISTRY_API)
    .then(res => {
      if (!res.ok) throw new Error(`registry listing responded ${res.status}`);
      return res.json();
    })
    .then(entries => entries.filter(e => e.type === "dir").map(e => e.name))
    .catch(err => {
      console.warn("Could not list the web-archive.txt registry:", err);
      return [];
    });
  return registryIdsPromise;
};

const fetchDescriptor = id => {
  if (!descriptorCache.has(id)) {
    descriptorCache.set(
      id,
      fetch(`${REGISTRY_BASE}/${id}/web-archive.txt`)
        .then(res => {
          if (!res.ok) throw new Error(res.status);
          return res.text();
        })
        .then(parseArchiveDescriptor)
        .catch(err => {
          console.warn(
            `Could not load web-archive.txt descriptor for "${id}":`,
            err
          );
          return null;
        })
    );
  }
  return descriptorCache.get(id);
};

const normaliseLabel = s =>
  ` ${s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()} `;

const resolveReplay = (archiveId, archiveLabel) => {
  const id = (archiveId || "").toLowerCase();
  const key = id ? `id:${id}` : (archiveLabel || "").toLowerCase();
  if (!key) return Promise.resolve(null);
  if (!replayCache.has(key))
    replayCache.set(key, lookupReplay(id, archiveLabel));
  return replayCache.get(key);
};

const lookupReplay = async (id, label) => {
  if (id) {
    const descriptor = await fetchDescriptor(id);
    if (descriptor) return { id, replay: descriptor.replay };
  }
  if (!label) return null;

  const direct = await fetchDescriptor(label.toLowerCase());
  if (direct) return { id: label.toLowerCase(), replay: direct.replay };

  const wanted = normaliseLabel(label);
  for (const registryId of await registryIds()) {
    const descriptor = await fetchDescriptor(registryId);
    const names = descriptor
      ? [descriptor.id, ...descriptor.names].filter(Boolean)
      : [];
    if (names.some(name => normaliseLabel(name).includes(wanted))) {
      return { id: registryId, replay: descriptor.replay };
    }
  }
  return null;
};

const targetURL = url => {
  if (/^https?:\/\//i.test(url)) return url;
  return url.includes("/") ? `https://${url}` : `https://${url}/`;
};

const mementoURL = (resolved, ts, url, mode = "no_toolbar") => {
  const datetime = ts.padEnd(14, "0");
  const template = (REPLAY_FALLBACK[mode] || REPLAY_FALLBACK.no_toolbar)
    .map(m => resolved?.replay?.[m])
    .find(Boolean);
  if (template) {
    return template
      .replace("{datetime}", datetime)
      .replace("{collection}", "")
      .replace("{url}", targetURL(url));
  }
  const suffix = { raw: "id_", no_toolbar: "if_" }[mode] || "";
  return `https://web.archive.org/web/${datetime}${suffix}/${targetURL(url)}`;
};

/* ------------------- Pluto ingest ------------------- */

const Pluto = (() => {
  const ALIVE_STATES = new Set(["S0", "S1", "S2"]);
  const TABLE_PATTERNS = {
    urls: /(^|\/)urls\/[^/]+\.parquet$/i,
    captures: /(^|\/)captures\/[^/]+\.parquet$/i,
    events: /(^|\/)events\/[^/]+\.parquet$/i,
  };
  const SUMMARY_PATTERN = /(^|\/)summary\.parquet$/i;
  const REGISTRY_PATTERN = /(^|\/)registry\/([^/]+)\/web-archive\.txt$/i;

  const bareId = id => (id || "").replace(/^archive:/i, "").toLowerCase();
  const stripScheme = url => (url || "").replace(/^https?:\/\//i, "");
  const summaryArchive = row => row.last_archive || row.first_archive || null;

  const toMs = value => {
    if (value == null) return null;
    const ms = (
      value instanceof Date
        ? value
        : new Date(typeof value === "bigint" ? Number(value) : value)
    ).getTime();
    return Number.isNaN(ms) ? null : ms;
  };

  const readRows = async file => {
    return hyparquet.parquetReadObjects({ file: await file.arrayBuffer() });
  };

  const readAll = async files => {
    return (await Promise.all(files.map(readRows))).flat();
  };

  const classify = entries => {
    const tables = { urls: [], captures: [], events: [] };
    const registry = new Map();
    let summary = null;
    let matched = false;

    for (const { path, file } of entries) {
      const table = Object.keys(TABLE_PATTERNS).find(name =>
        TABLE_PATTERNS[name].test(path)
      );
      const registryMatch = path.match(REGISTRY_PATTERN);
      if (table) tables[table].push(file);
      else if (SUMMARY_PATTERN.test(path)) summary = file;
      else if (registryMatch)
        registry.set(registryMatch[2].toLowerCase(), file);
      else continue;
      matched = true;
    }
    return { tables, summary, registry, matched };
  };

  const archiveNames = async (ids, registryFiles) => {
    const names = new Map();
    await Promise.all(
      [...new Set(ids)].filter(Boolean).map(async id => {
        const key = bareId(id);
        const localFile = registryFiles.get(key);
        let name = null;
        if (localFile) {
          try {
            name = parseArchiveDescriptor(await localFile.text()).names[0];
          } catch {}
        }
        name ||= (await fetchDescriptor(key))?.names[0];
        names.set(id, name || key.toUpperCase());
      })
    );
    return names;
  };

  const statesFromSummary = rows => {
    return new Map(rows.map(row => [row.url_id, row.current_state]));
  };

  const statesFromEvents = rows => {
    const latest = new Map();
    for (const event of rows) {
      if (!event.event_type?.startsWith("state_")) continue;
      const endMs = toMs(event.event_end) ?? -Infinity;
      const prev = latest.get(event.url_id);
      if (!prev || endMs >= prev.endMs)
        latest.set(event.url_id, { endMs, state: event.event_type.slice(6) });
    }
    return new Map([...latest].map(([urlId, { state }]) => [urlId, state]));
  };

  const rowsFromCaptures = (urlRows, captureRows, states, names) => {
    const originals = new Map(
      urlRows.map(u => [u.url_id, u.original_url || u.canonical_url || ""])
    );
    const groups = new Map();
    for (const capture of captureRows) {
      const key = `${capture.url_id}\u0000${capture.archive}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(capture);
    }

    const rows = [];
    for (const captures of groups.values()) {
      const { url_id: urlId, archive } = captures[0];
      const original = originals.get(urlId) || "";
      const archiveId = bareId(archive);
      const archiveName =
        names.get(archive) || (archiveId || "Unspecified").toUpperCase();
      const mementos = captures
        .map(c => ({
          ms: toMs(c.capture_time),
          url: c.memento_url || original,
        }))
        .filter(m => m.ms !== null)
        .sort((a, b) => a.ms - b.ms)
        .map(m => ({
          ...m,
          ts: toWayback(m.ms),
          archive: archiveName,
          archiveId,
        }));
      if (!mementos.length) continue;

      rows.push({
        host: stripScheme(original) || urlId,
        url: original,
        firstMs: mementos[0].ms,
        lastMs: mementos[mementos.length - 1].ms,
        captures: captures.length,
        live: ALIVE_STATES.has(states.get(urlId)),
        archive: archiveName,
        archiveId,
        mementos,
      });
    }
    return rows;
  };

  const rowsFromSummary = (summaryRows, names) => {
    const rows = [];
    for (const row of summaryRows) {
      const firstMs = toMs(row.first_appearance);
      const lastMs = toMs(row.last_appearance);
      if (firstMs === null || lastMs === null) continue;
      const archive = summaryArchive(row);
      rows.push({
        host: stripScheme(row.original_url || row.url_id),
        url: row.original_url || "",
        firstMs,
        lastMs,
        captures: Number(row.n_captures ?? 0),
        live: ALIVE_STATES.has(row.current_state),
        archive: archive
          ? names.get(archive) || bareId(archive).toUpperCase()
          : "Unspecified",
        archiveId: bareId(archive),
        mementos: [],
      });
    }
    return rows;
  };

  const loadFolder = async entries => {
    const { tables, summary, registry, matched } = classify(entries);
    if (!matched) return null;

    const summaryRows = summary ? await readRows(summary) : null;
    let states = summaryRows ? statesFromSummary(summaryRows) : new Map();

    if (tables.captures.length) {
      const [urlRows, captureRows] = await Promise.all([
        readAll(tables.urls),
        readAll(tables.captures),
      ]);
      if (!states.size && tables.events.length)
        states = statesFromEvents(await readAll(tables.events));
      const names = await archiveNames(
        captureRows.map(c => c.archive),
        registry
      );
      const rows = rowsFromCaptures(urlRows, captureRows, states, names);
      if (rows.length) return rows;
    }

    if (summaryRows?.length) {
      return rowsFromSummary(
        summaryRows,
        await archiveNames(summaryRows.map(summaryArchive), registry)
      );
    }
    return [];
  };

  const loadFile = async file => {
    if (!/\.parquet$/i.test(file.name || "")) return null;
    const rows = await readRows(file);
    if (
      !rows.length ||
      !("url_id" in rows[0]) ||
      !("current_state" in rows[0])
    ) {
      throw new Error(
        "This .parquet file doesn't look like a Pluto summary.parquet (expected columns like " +
          "url_id and current_state). For full per-archive detail, use “Upload Folder” on " +
          "Pluto's whole output folder instead."
      );
    }
    return rowsFromSummary(
      rows,
      await archiveNames(rows.map(summaryArchive), new Map())
    );
  };

  const isPlutoFile = path => {
    return (
      Object.values(TABLE_PATTERNS).some(pattern => pattern.test(path)) ||
      SUMMARY_PATTERN.test(path) ||
      REGISTRY_PATTERN.test(path)
    );
  };

  return { loadFolder, loadFile, isPlutoFile };
})();
/* ------------------- Data loading ------------------- */

const listFromServer = async (dirURL, visited = new Set()) => {
  if (visited.has(dirURL) || visited.size > 50) return [];
  visited.add(dirURL);
  const res = await fetch(dirURL);
  if (!res.ok || !res.headers.get("content-type")?.includes("text/html"))
    return [];

  const doc = new DOMParser().parseFromString(await res.text(), "text/html");
  const children = new Set();
  for (const link of doc.querySelectorAll("a[href]")) {
    const url = new URL(link.getAttribute("href"), dirURL);
    url.search = url.hash = "";
    if (url.href.startsWith(dirURL) && url.href !== dirURL)
      children.add(url.href);
  }

  const nested = await Promise.all(
    [...children].map(href =>
      /\.(parquet|txt)$/i.test(href)
        ? [href]
        : listFromServer(href.endsWith("/") ? href : `${href}/`, visited)
    )
  );
  return nested.flat();
};

const listFromGitHub = async dir => {
  const res = await fetch(REPO_TREE_API);
  if (!res.ok) throw new Error(`GitHub tree listing responded ${res.status}`);
  const { tree } = await res.json();
  return tree
    .filter(item => item.type === "blob" && item.path.startsWith(dir))
    .map(item => item.path.slice(dir.length));
};

const listDataFiles = async dir => {
  const base = new URL(dir, location.href).href;
  const fromServer = (await listFromServer(base).catch(() => []))
    .map(href => decodeURIComponent(href.slice(base.length)))
    .filter(Pluto.isPlutoFile);
  if (fromServer.length) return fromServer;

  const fromGitHub = (await listFromGitHub(dir).catch(() => [])).filter(
    Pluto.isPlutoFile
  );
  return fromGitHub.length ? fromGitHub : ["summary.parquet"];
};

const loadDefaultData = async () => {
  try {
    const paths = await listDataFiles(DEFAULT_DATA_DIR);
    const entries = await Promise.all(
      paths.map(async path => {
        const res = await fetch(DEFAULT_DATA_DIR + path);
        if (!res.ok) throw new Error(`${path} responded ${res.status}`);
        return {
          path,
          file: new File([await res.blob()], path.split("/").pop()),
        };
      })
    );
    const rows = await Pluto.loadFolder(entries);
    if (rows?.length) applyHostRows(rows);
  } catch (err) {
    console.error("Could not load the default Pluto data:", err);
  }
};

const applyHostRows = rows => {
  hosts = rows.map((row, id) => ({
    id,
    host: row.host,
    firstMs: row.firstMs,
    lastMs: row.lastMs,
    captures: row.captures,
    live: row.live,
    archive: row.archive,
    mementos: row.mementos.length
      ? row.mementos
      : [row.firstMs, row.lastMs].map(ms => ({
          ms,
          ts: toWayback(ms),
          archive: row.archive,
          archiveId: row.archiveId,
          url: row.url,
        })),
  }));

  stopIntro();
  mergedHosts = mergeByHost(hosts);
  fullStart = Infinity;
  fullEnd = -Infinity;
  for (const host of hosts) {
    if (host.firstMs < fullStart) fullStart = host.firstMs;
    if (host.lastMs > fullEnd) fullEnd = host.lastMs;
  }

  buildArchiveFilter();
  buildOverviewData();
  computeLabelReserve();
  computeVisible();
  fitInitialScale();
  layout();
  playIntro();
};

const mergeByHost = rows => {
  const byHost = new Map();
  for (const row of rows) {
    const merged = byHost.get(row.host);
    if (!merged) {
      byHost.set(row.host, {
        ...row,
        id: row.host,
        archives: [row.archive],
        mementos: [...row.mementos],
      });
      continue;
    }
    merged.firstMs = Math.min(merged.firstMs, row.firstMs);
    merged.lastMs = Math.max(merged.lastMs, row.lastMs);
    merged.captures += row.captures;
    merged.live ||= row.live;
    if (!merged.archives.includes(row.archive))
      merged.archives.push(row.archive);
    merged.mementos.push(...row.mementos);
  }
  const merged = [...byHost.values()];
  for (const host of merged) host.mementos.sort((a, b) => a.ms - b.ms);
  return merged;
};

const buildOverviewData = () => {
  const events = [];
  const monthly = new Map();
  for (const host of mergedHosts) {
    events.push(
      { t: host.firstMs, delta: 1 },
      { t: host.lastMs + 1, delta: -1 }
    );
    for (const memento of host.mementos) bump(monthly, memento.ts.slice(0, 6));
  }
  events.sort((a, b) => a.t - b.t);

  let count = 0;
  populationCurve = events.map(({ t, delta }) => ({
    t,
    count: (count += delta),
  }));
  captureHistogram = [...monthly]
    .map(([key, total]) => ({
      monthStart: new Date(+key.slice(0, 4), +key.slice(4, 6) - 1, 1).getTime(),
      count: total,
    }))
    .sort((a, b) => a.monthStart - b.monthStart);

  maxPopulation = populationCurve.reduce((max, p) => Math.max(max, p.count), 1);
  maxMonthlyCaptures = captureHistogram.reduce(
    (max, b) => Math.max(max, b.count),
    1
  );
};

const computeLabelReserve = () => {
  plotCtx.font = FONT_SM;
  let widest = 0;
  for (const name of new Set(hosts.map(h => h.host)))
    widest = Math.max(widest, plotCtx.measureText(name).width);
  labelReserve = widest + 35 + EDGE_PAD;
};

const loadUpload = async load => {
  try {
    const rows = await load();
    if (!rows) return;
    applyHostRows(rows);
    resetView();
  } catch (err) {
    console.error("Could not load upload:", err);
    alert(err.message || "Could not load that data.");
  }
};

const loadEntries = entries => {
  if (!entries.length) return;
  loadUpload(async () => {
    const single = entries.length === 1 && !entries[0].path.includes("/");
    const rows = single
      ? await Pluto.loadFile(entries[0].file)
      : await Pluto.loadFolder(entries);
    if (!rows) {
      alert(
        "That doesn't look like Pluto data — choose Pluto's results folder, or drop its summary.parquet."
      );
      return null;
    }
    if (!rows.length) {
      alert("No usable rows found in that folder.");
      return null;
    }
    return rows;
  });
};

const entriesFromDrop = async items => {
  const entries = [];
  const readEntries = reader =>
    new Promise((resolve, reject) => reader.readEntries(resolve, reject));
  const walk = async entry => {
    if (entry.isFile) {
      const file = await new Promise((resolve, reject) =>
        entry.file(resolve, reject)
      );
      entries.push({ path: entry.fullPath.replace(/^\//, ""), file });
      return;
    }
    const reader = entry.createReader();
    for (
      let batch = await readEntries(reader);
      batch.length;
      batch = await readEntries(reader)
    ) {
      await Promise.all(batch.map(walk));
    }
  };
  await Promise.all(items.map(walk));
  return entries;
};

const isFileDrag = e => e.dataTransfer?.types.includes("Files");

uploadButton.addEventListener("click", () => folderInput.click());

folderInput.addEventListener("change", () => {
  const entries = [...folderInput.files].map(file => ({
    path: file.webkitRelativePath || file.name,
    file,
  }));
  folderInput.value = "";
  loadEntries(entries);
});

window.addEventListener("dragenter", e => {
  if (!isFileDrag(e)) return;
  e.preventDefault();
  dragDepth++;
  dropHint.hidden = false;
});

window.addEventListener("dragover", e => {
  if (isFileDrag(e)) e.preventDefault();
});

window.addEventListener("dragleave", e => {
  if (isFileDrag(e) && --dragDepth <= 0) {
    dragDepth = 0;
    dropHint.hidden = true;
  }
});

window.addEventListener("drop", async e => {
  if (!isFileDrag(e)) return;
  e.preventDefault();
  dragDepth = 0;
  dropHint.hidden = true;
  const items = [...e.dataTransfer.items]
    .map(item => item.webkitGetAsEntry?.())
    .filter(Boolean);
  const files = [...e.dataTransfer.files].map(file => ({
    path: file.name,
    file,
  }));
  loadEntries(items.length ? await entriesFromDrop(items) : files);
});

/* ------------------- Filter and sort ------------------- */

const SORTS = {
  date: (a, b) => a.firstMs - b.firstMs,
  life: (a, b) => b.lastMs - b.firstMs - (a.lastMs - a.firstMs),
  captures: (a, b) => b.captures - a.captures,
  archive: (a, b) =>
    a.archive.localeCompare(b.archive) || a.firstMs - b.firstMs,
};

const inSelectedArchives = host => {
  return host.archives
    ? host.archives.some(a => selectedArchives.has(a))
    : selectedArchives.has(host.archive);
};

const computeVisible = () => {
  const query = searchInput.value.trim().toLowerCase();
  const source = sortSelect.value === "archive" ? hosts : mergedHosts;
  visible = source.filter(
    h =>
      inSelectedArchives(h) && (!query || h.host.toLowerCase().includes(query))
  );
  visible.sort(SORTS[sortSelect.value] || SORTS.date);
};

const refresh = () => {
  computeVisible();
  searchInput.placeholder = `Search (${visible.length} hosts)`;
  renderActiveView();
};

searchInput.addEventListener("input", refresh);

/* ------------------- View switcher ------------------- */

const ALT_VIEWS = {
  ridgeline: { el: ridgelineView, render: () => renderRidgeline() },
  growth: { el: growthView, render: () => renderGrowth() },
  survival: { el: survivalView, render: () => renderSurvival() },
  agepyramid: { el: ageView, render: () => renderAgePyramid() },
};

const renderActiveView = () => {
  (ALT_VIEWS[viewMode]?.render ?? layout)();
};

const setViewMode = mode => {
  viewMode = mode;
  viewPanel.hidden = true;
  viewTriggerLabel.textContent = VIEW_LABELS[mode] || "View";
  for (const item of viewPanel.querySelectorAll(".item")) {
    const radio = item.querySelector("input");
    radio.checked = radio.value === mode;
    item.classList.toggle("selected", radio.checked);
  }

  const isTimeline = mode.startsWith("timeline-");
  if (isTimeline) sortSelect.value = mode.slice("timeline-".length);
  viewport.hidden = !isTimeline;
  for (const [key, view] of Object.entries(ALT_VIEWS))
    view.el.hidden = key !== mode;
  refresh();
};

viewPanel.addEventListener("change", e => {
  if (e.target.matches("input[type=radio]")) setViewMode(e.target.value);
});

/* ------------------- Row layout ------------------- */

const buildRowPlan = () => {
  const rows = [];
  const grouped = sortSelect.value === "archive";
  visible.forEach((host, i) => {
    rows.push({ type: "host", host, h: rowHeight });
    if (grouped && visible[i + 1]?.archive !== host.archive) {
      rows.push({ type: "band", label: host.archive, h: BAND_H });
    }
  });
  return rows;
};

const rowsHeight = rows => {
  return rows.reduce((sum, row) => sum + row.h, 0);
};

const positionRows = (rows, height) => {
  let bottom = height;
  for (const row of rows) {
    row.bottom = bottom;
    row.top = bottom - row.h;
    row.y = bottom - row.h / 2;
    bottom = row.top;
  }
  return rows;
};

const firstRowIndexAtOrBelow = (rows, y) => {
  let lo = 0;
  let hi = rows.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (rows[mid].top <= y) hi = mid;
    else lo = mid + 1;
  }
  return lo;
};

const rowAt = y => {
  const row = currentRows[firstRowIndexAtOrBelow(currentRows, y)];
  return row && y < row.bottom && row.type === "host" ? row : null;
};

const dotNear = (row, x) => {
  return row?.dots?.find(dot => Math.abs(dot.x - x) <= DOT_HIT_RADIUS) ?? null;
};

/* ------------------- Scale ------------------- */

const dateToX = ms => EDGE_PAD + ((ms - fullStart) / YEAR_MS) * pxPerYear;
const xToDate = x => fullStart + ((x - EDGE_PAD) / pxPerYear) * YEAR_MS;
const surfaceWidth = () =>
  Math.max(viewport.clientWidth, dateToX(fullEnd) + labelReserve);
const viewSize = () => [
  Math.max(1, viewport.clientWidth),
  Math.max(1, viewport.clientHeight - CHROME_H),
];

const fitRowHeight = count => {
  const available = Math.max(
    0,
    viewport.clientHeight - CHROME_H - eraReserveHeight()
  );
  return clamp(available / Math.max(1, count), MIN_ROW, MAX_ROW);
};

const fitInitialScale = () => {
  const years = (fullEnd - fullStart) / YEAR_MS;
  pxPerYear = clamp(
    (viewport.clientWidth - EDGE_PAD * 2) / years,
    MIN_PX_YEAR,
    MAX_PX_YEAR
  );
  rowHeight = fitRowHeight(visible.length);
};

/* ------------------- Axis ticks ------------------- */

const computeTicks = (rangeStart, rangeEnd) => {
  const viewStart = Math.max(fullStart, rangeStart);
  const viewEnd = Math.min(fullEnd, rangeEnd);
  const pxPerDay = pxPerYear / 365.25;
  const pxPerMonth = pxPerYear / 12;
  const ticks = [];

  if (pxPerDay >= TICK_TARGET_PX) {
    const step =
      [1, 2, 5, 10, 15].find(s => s * pxPerDay >= TICK_TARGET_PX) || 15;
    const stepMs = step * DAY_MS;
    const anchor = new Date(fullStart).setHours(0, 0, 0, 0);
    const skip = Math.max(0, Math.floor((viewStart - anchor) / stepMs) - 1);
    for (let t = anchor + skip * stepMs; t <= viewEnd; t += stepMs) {
      if (t < fullStart) continue;
      ticks.push({
        x: dateToX(t),
        label: new Date(t).toLocaleDateString("en-GB", {
          day: "numeric",
          month: "short",
        }),
      });
    }
    return ticks;
  }

  if (pxPerMonth >= TICK_TARGET_PX) {
    const step = [1, 2, 3, 6].find(s => s * pxPerMonth >= TICK_TARGET_PX) || 6;
    const start = new Date(fullStart);
    const view = new Date(viewStart);
    const monthsIn =
      (view.getFullYear() - start.getFullYear()) * 12 +
      view.getMonth() -
      start.getMonth();
    const skip = Math.max(0, Math.floor(monthsIn / step) - 1);
    for (let m = start.getMonth() + skip * step; ; m += step) {
      const t = new Date(start.getFullYear(), m, 1).getTime();
      if (t > viewEnd) break;
      if (t < fullStart) continue;
      ticks.push({
        x: dateToX(t),
        label: new Date(t).toLocaleDateString("en-GB", {
          month: "short",
          year: "numeric",
        }),
      });
    }
    return ticks;
  }

  const step =
    [1, 2, 5, 10, 25, 50, 100].find(s => s * pxPerYear >= TICK_TARGET_PX) ||
    100;
  const lastYear = new Date(fullEnd).getFullYear();
  for (
    let y = Math.floor(new Date(fullStart).getFullYear() / step) * step;
    y <= lastYear;
    y += step
  ) {
    ticks.push({ x: dateToX(new Date(y, 0, 1).getTime()), label: String(y) });
  }
  return ticks;
};

const ticksInView = (scrollLeft, viewW) => {
  return computeTicks(
    xToDate(scrollLeft - DRAW_BUFFER),
    xToDate(scrollLeft + viewW + DRAW_BUFFER)
  );
};

/* ------------------- Timeline rendering ------------------- */

const sizeCanvases = () => {
  const [viewW, viewH] = viewSize();
  sizeCanvas(axisCanvas, viewW, AXIS_H);
  sizeCanvas(overviewCanvas, viewW, OVERVIEW_H);
  sizeCanvas(plotCanvas, viewW, viewH);
};

const layout = () => {
  if (playingIntro) return introRedraw();
  const rows = buildRowPlan();
  const width = surfaceWidth();
  const height = Math.max(
    viewport.clientHeight - CHROME_H,
    rowsHeight(rows) + eraReserveHeight()
  );
  currentRows = positionRows(rows, height);
  plotFullHeight = height;

  surface.style.width = `${width}px`;
  plotWrap.style.height = `${height}px`;

  sizeCanvases();
  redraw();
  renderBandLabels();
};

const redraw = () => {
  const [viewW, viewH] = viewSize();
  const { scrollLeft, scrollTop } = viewport;
  const width = surfaceWidth();
  const ticks = ticksInView(scrollLeft, viewW);
  drawAxis(ticks, viewW, scrollLeft, width);
  drawOverview(viewW, scrollLeft, width);
  drawPlot(ticks, viewW, viewH, scrollLeft, scrollTop);
};

const drawAxis = (ticks, viewW, scrollLeft, width) => {
  const labelPad = 4;
  axisCtx.clearRect(0, 0, viewW, AXIS_H);
  axisCtx.save();
  axisCtx.translate(-scrollLeft, 0);
  axisCtx.font = FONT_SM;
  axisCtx.fillStyle = ink(0.5);
  axisCtx.strokeStyle = ink(0.2);
  axisCtx.beginPath();
  for (const { x } of ticks) {
    axisCtx.moveTo(x + 0.5, 10);
    axisCtx.lineTo(x + 0.5, 1);
  }
  axisCtx.stroke();
  for (const { x, label } of ticks) {
    const textW = axisCtx.measureText(label).width;
    axisCtx.fillText(
      label,
      clamp(x - textW / 2, labelPad, width - textW - labelPad),
      24
    );
  }
  axisCtx.restore();
};

const drawOverview = (viewW, scrollLeft, width) => {
  overviewCtx.clearRect(0, 0, viewW, OVERVIEW_H);
  if (!populationCurve.length) return;

  const baselineY = OVERVIEW_H - 6;
  const curveH = baselineY - 18;
  const barMaxH = curveH * 0.4;
  const countToY = count => baselineY - (count / maxPopulation) * curveH;
  const firstX = dateToX(populationCurve[0].t);

  overviewCtx.save();
  overviewCtx.translate(-scrollLeft, 0);

  overviewCtx.fillStyle = ink(0.12);
  captureHistogram.forEach((bucket, i) => {
    const x = dateToX(bucket.monthStart);
    const nextX =
      i < captureHistogram.length - 1
        ? dateToX(captureHistogram[i + 1].monthStart)
        : x + 4;
    const barH = (bucket.count / maxMonthlyCaptures) * barMaxH;
    overviewCtx.fillRect(x, baselineY - barH, Math.max(1, nextX - x - 1), barH);
  });

  const tracePath = () => {
    let y = countToY(0);
    overviewCtx.beginPath();
    overviewCtx.moveTo(firstX, y);
    for (const point of populationCurve) {
      const x = dateToX(point.t);
      overviewCtx.lineTo(x, y);
      y = countToY(point.count);
      overviewCtx.lineTo(x, y);
    }
    overviewCtx.lineTo(width, y);
  };

  tracePath();
  overviewCtx.lineTo(width, baselineY);
  overviewCtx.lineTo(firstX, baselineY);
  overviewCtx.closePath();
  overviewCtx.fillStyle = ink(0.08);
  overviewCtx.fill();

  tracePath();
  overviewCtx.strokeStyle = ink(0.6);
  overviewCtx.lineWidth = 1.2;
  overviewCtx.stroke();
  overviewCtx.restore();
};

const drawPlot = (ticks, viewW, viewH, scrollLeft, scrollTop) => {
  plotCtx.clearRect(0, 0, viewW, viewH);
  plotCtx.save();
  plotCtx.translate(-scrollLeft, -scrollTop);

  if (erasVisible) drawEras();
  plotCtx.strokeStyle = ink(0.2);
  plotCtx.lineWidth = 1;
  plotCtx.beginPath();
  for (const { x } of ticks) {
    plotCtx.moveTo(x + 0.5, 0);
    plotCtx.lineTo(x + 0.5, plotFullHeight);
  }
  plotCtx.stroke();

  const top = scrollTop - DRAW_BUFFER;
  const bottom = scrollTop + viewH + DRAW_BUFFER;
  const width = surfaceWidth();
  plotCtx.font = FONT_SM;
  plotCtx.lineCap = "round";
  for (
    let i = firstRowIndexAtOrBelow(currentRows, bottom);
    i < currentRows.length;
    i++
  ) {
    const row = currentRows[i];
    if (row.bottom < top) break;
    if (row.type === "band") drawBandRow(row, width);
    else drawHostRow(row);
  }
  plotCtx.restore();
};

const drawBandRow = (row, width) => {
  plotCtx.strokeStyle = ink(1);
  plotCtx.lineWidth = 1;
  plotCtx.beginPath();
  plotCtx.moveTo(0, row.top);
  plotCtx.lineTo(width, row.top + 10);
  plotCtx.stroke();
};

const renderBandLabels = () => {
  bands.innerHTML = currentRows
    .filter(row => row.type === "band")
    .map(
      row => `<div class="band" style="top:${row.top}px;height:${row.h}px">
        <span>${esc(row.label)}</span>
      </div>`
    )
    .join("");
};

const fillDots = (dots, radius, color) => {
  plotCtx.beginPath();
  for (const { x, y } of dots) {
    plotCtx.moveTo(x + radius, y);
    plotCtx.arc(x, y, radius, 0, TAU);
  }
  plotCtx.fillStyle = color;
  plotCtx.fill();
};

const drawHostRow = row => {
  const { host, y, h: rowH } = row;
  const startX = dateToX(host.firstMs);
  const fullEndX = dateToX(host.lastMs);
  const endX = Math.max(
    startX + 1.5,
    playingIntro && introClipX !== null
      ? Math.min(fullEndX, introClipX)
      : fullEndX
  );
  const density = Math.min(1, 0.25 + Math.log10(host.captures + 1) / 2);

  plotCtx.strokeStyle = ink(density * 0.45);
  plotCtx.lineWidth = Math.max(1, rowH - Math.max(0, (rowH - 8) * 0.6));
  plotCtx.beginPath();
  plotCtx.moveTo(startX, y);
  plotCtx.lineTo(endX, y);
  plotCtx.stroke();

  row.dots = [];
  if (!playingIntro && pxPerYear >= DOT_ZOOM_MIN && rowH >= DOT_MIN_ROW_H) {
    let lastX = -Infinity;
    for (const memento of host.mementos) {
      const x = dateToX(memento.ms);
      if (x - lastX < DOT_MIN_GAP) continue;
      lastX = x;
      row.dots.push({ x, y, memento });
    }
    fillDots(row.dots, DOT_RADIUS + 1, `rgb(${SEAWEED})`);
    fillDots(row.dots, DOT_RADIUS, ink(1));
  }

  if (rowH < LABEL_MIN_ROW_H) return;
  const labelX = endX + 15;
  plotCtx.fillStyle = ink(0.8);
  plotCtx.fillText(host.host, labelX, y + 4);

  if (host.live) {
    plotCtx.beginPath();
    plotCtx.arc(
      labelX + plotCtx.measureText(host.host).width + 10,
      y,
      2.5,
      0,
      TAU
    );
    plotCtx.fillStyle = `rgb(${LIVE})`;
    plotCtx.fill();
  }
};

const eraVisible = era => {
  return (
    pxPerYear >= ERA_ZOOM[era.zoom] &&
    dateToX(era.endMs) - dateToX(era.startMs) >= ERA_MIN_WIDTH
  );
};

const eraReserveHeight = () => {
  if (!erasVisible || pxPerYear >= ERA_RESERVE_ZOOM) return 0;
  let maxLane = -1;
  for (const era of ERAS)
    if (eraVisible(era)) maxLane = Math.max(maxLane, era.lane);
  return maxLane < 0 ? 0 : (maxLane + 1) * ERA_LANE_H + ERA_LANE_GAP;
};

const drawEras = () => {
  plotCtx.font = FONT_SM;
  plotCtx.lineWidth = 1;
  for (const era of ERAS) {
    if (!eraVisible(era)) continue;
    const startX = dateToX(era.startMs);
    const laneY = era.lane * ERA_LANE_H;

    plotCtx.fillStyle = ink(0.05);
    plotCtx.fillRect(
      startX,
      laneY,
      dateToX(era.endMs) - startX,
      ERA_LANE_H - 4
    );
    plotCtx.fillStyle = ink(0.35);
    plotCtx.fillText(era.label, startX + 6, laneY + 11);
    plotCtx.strokeStyle = ink(0.1);
    plotCtx.beginPath();
    plotCtx.moveTo(startX, laneY);
    plotCtx.lineTo(startX, plotFullHeight);
    plotCtx.stroke();
  }
};

/* ------------------- Intro animation ------------------- */

const stopIntro = () => {
  introId++;
  playingIntro = false;
  introClipX = null;
  introRedraw = null;
  pendingIntro = null;
};

const playIntro = () => {
  const id = ++introId;
  const sweep = [...mergedHosts]
    .sort((a, b) => a.firstMs - b.firstMs)
    .map(host => ({ type: "host", host, h: 0 }));
  let progress = 0;
  let start = 0;

  const revealedCount = ms => {
    let lo = 0;
    let hi = sweep.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (sweep[mid].host.firstMs <= ms) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  };

  const placeRows = () => {
    const introTime = fullStart + (fullEnd - fullStart) * progress;
    const rows = sweep.slice(0, revealedCount(introTime));
    const rowH = fitRowHeight(rows.length);
    for (const row of rows) row.h = rowH;
    introClipX = dateToX(introTime);
    plotFullHeight = Math.max(
      viewport.clientHeight - CHROME_H,
      rows.length * rowH + eraReserveHeight()
    );
    currentRows = positionRows(rows, plotFullHeight);
    plotWrap.style.height = `${plotFullHeight}px`;
  };

  const frame = now => {
    if (id !== introId) return;
    progress = Math.min(1, (now - start) / INTRO_MS);
    placeRows();
    const [w, h] = viewSize();
    drawPlot(
      ticksInView(viewport.scrollLeft, w),
      w,
      h,
      viewport.scrollLeft,
      viewport.scrollTop
    );
    if (progress < 1) return requestAnimationFrame(frame);
    stopIntro();
    layout();
  };

  playingIntro = true;
  bands.innerHTML = "";
  introRedraw = () => {
    surface.style.width = `${surfaceWidth()}px`;
    sizeCanvases();
    placeRows();
    redraw();
  };
  introRedraw();

  const begin = () => {
    start = performance.now();
    requestAnimationFrame(frame);
  };
  if (aboutDismissed) begin();
  else pendingIntro = begin;
};

/* ------------------- Zoom ------------------- */

const zoomTime = (factor, clientX) => {
  const offset = clientX - viewport.getBoundingClientRect().left;
  const anchor = offset + viewport.scrollLeft;
  const before = surfaceWidth();
  pxPerYear = clamp(pxPerYear * factor, MIN_PX_YEAR, MAX_PX_YEAR);
  layout();
  viewport.scrollLeft = anchor * (surfaceWidth() / before) - offset;
};

const zoomRows = (factor, clientY) => {
  const offset = clientY - plotCanvas.getBoundingClientRect().top;
  const anchor = offset + viewport.scrollTop;
  const before = plotFullHeight;
  rowHeight = clamp(rowHeight * factor, MIN_ROW, MAX_ROW);
  layout();
  viewport.scrollTop = anchor * (plotFullHeight / before) - offset;
};

const resetView = () => {
  searchInput.value = "";
  placard.style.display = "none";
  setAllArchives(true);
  setViewMode("timeline-date");
  fitInitialScale();
  layout();
  requestAnimationFrame(() =>
    viewport.scrollTo({ left: 0, top: 0, behavior: "instant" })
  );
};
/* ------------------- Pointer input ------------------- */

const plotPoint = e => {
  const rect = plotCanvas.getBoundingClientRect();
  return {
    x: e.clientX - rect.left + viewport.scrollLeft,
    y: e.clientY - rect.top + viewport.scrollTop,
  };
};

const hideHover = () => {
  placard.style.display = "none";
  dotHighlight.style.display = "none";
};

viewport.addEventListener(
  "scroll",
  () => {
    if (scrollFrame) return;
    scrollFrame = requestAnimationFrame(() => {
      scrollFrame = null;
      redraw();
    });
  },
  { passive: true }
);

viewport.addEventListener(
  "wheel",
  e => {
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      zoomTime(e.deltaY < 0 ? 1.12 : 1 / 1.12, e.clientX);
    } else if (e.altKey) {
      e.preventDefault();
      zoomRows(e.deltaY < 0 ? 1.15 : 1 / 1.15, e.clientY);
    }
  },
  { passive: false }
);

window.addEventListener(
  "wheel",
  e => {
    if (e.ctrlKey || e.metaKey) e.preventDefault();
  },
  { passive: false }
);

for (const type of ["gesturestart", "gesturechange", "gestureend"]) {
  document.addEventListener(type, e => e.preventDefault());
}

viewport.addEventListener("mousedown", e => {
  dragging = true;
  dragMoved = false;
  dragX = e.clientX;
  dragY = e.clientY;
  viewport.classList.add("dragging");
  dotHighlight.style.display = "none";
});

window.addEventListener("mousemove", e => {
  if (!dragging) return;
  dragMoved = true;
  viewport.scrollLeft -= e.clientX - dragX;
  viewport.scrollTop -= e.clientY - dragY;
  dragX = e.clientX;
  dragY = e.clientY;
});

window.addEventListener("mouseup", () => {
  dragging = false;
  viewport.classList.remove("dragging");
});

viewport.addEventListener("mousemove", e => {
  if (dragging) return hideHover();

  const { x, y } = plotPoint(e);
  const row = rowAt(y);
  const dot = dotNear(row, x);
  viewport.classList.toggle("over-dot", !!dot);

  if (!row) return hideHover();
  if (!dot) {
    showHostPlacard(row.host, e.clientX, e.clientY);
    dotHighlight.style.display = "none";
    return;
  }

  showCapturePlacard(row.host, dot.memento, e.clientX, e.clientY);
  const rect = plotCanvas.getBoundingClientRect();
  dotHighlight.style.left = `${rect.left + dot.x - viewport.scrollLeft}px`;
  dotHighlight.style.top = `${rect.top + dot.y - viewport.scrollTop}px`;
  dotHighlight.style.display = "block";
});

viewport.addEventListener("mouseleave", hideHover);

viewport.addEventListener("click", e => {
  if (dragMoved) return;
  const { x, y } = plotPoint(e);
  const row = rowAt(y);
  const dot = dotNear(row, x);
  if (dot) openViewer(row.host, dot.memento);
});

/* ------------------- Keyboard ------------------- */

const viewportCentre = () => {
  const rect = viewport.getBoundingClientRect();
  return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
};

const ZOOM_KEYS = new Set(["+", "=", "-", "_", "0"]);

const KEY_ACTIONS = {
  "+": () => zoomTime(1.2, viewportCentre().x),
  "=": () => zoomTime(1.2, viewportCentre().x),
  "-": () => zoomTime(1 / 1.2, viewportCentre().x),
  _: () => zoomTime(1 / 1.2, viewportCentre().x),
  "]": () => zoomRows(1.2, viewportCentre().y),
  "[": () => zoomRows(1 / 1.2, viewportCentre().y),
  0: () => resetView(),
  e: () => {
    erasVisible = !erasVisible;
    layout();
  },
  f: () => toggleExhibition(),
};

const closeTopmost = () => {
  if (!viewPanel.hidden) viewPanel.hidden = true;
  else if (!archivesPanel.hidden) archivesPanel.hidden = true;
  else if (isOverlayOpen(reportOverlay)) closeOverlay(reportOverlay);
  else if (isOverlayOpen(aboutOverlay)) closeOverlay(aboutOverlay);
  else if (isViewerOpen()) closeViewer();
  else if (document.body.classList.contains("exhibition")) toggleExhibition();
};

document.addEventListener("keydown", e => {
  if ((e.ctrlKey || e.metaKey) && ZOOM_KEYS.has(e.key)) e.preventDefault();
  if (e.key === "Escape") return closeTopmost();
  if (isOverlayOpen(aboutOverlay) || isOverlayOpen(reportOverlay)) return;
  if (isViewerOpen()) {
    if (e.key === "ArrowLeft") stepViewer(-1);
    if (e.key === "ArrowRight") stepViewer(1);
    return;
  }
  if (e.target.matches("input, select")) return;
  KEY_ACTIONS[e.key.toLowerCase()]?.();
});

/* ------------------- Placard ------------------- */

const showPlacard = (html, clientX, clientY) => {
  const offset = 18;
  const margin = 8;
  placard.innerHTML = html;
  placard.style.display = "block";
  const { offsetWidth: w, offsetHeight: h } = placard;
  const maxLeft = window.innerWidth - w - margin;
  const maxTop = window.innerHeight - h - margin;
  const left =
    clientX + offset <= maxLeft ? clientX + offset : clientX - w - offset;
  const top =
    clientY + offset <= maxTop ? clientY + offset : clientY - h - offset;
  placard.style.left = `${clamp(left, margin, maxLeft)}px`;
  placard.style.top = `${clamp(top, margin, maxTop)}px`;
};

const fact = (label, value) =>
  `<div class="fact"><span class="k">${label}</span><span class="v">${value}</span></div>`;
const archiveTag = name => `<span class="tag">${esc(name)}</span>`;

const showHostPlacard = (host, clientX, clientY) => {
  showPlacard(
    `<div class="head">
      <span class="host">${esc(host.host)}</span>
      <span class="status${host.live ? " live" : ""}">${host.live ? "Live" : "Archived"}</span>
    </div>
    <div class="facts">
      ${fact("First seen", prettyDate(host.firstMs))}
      ${fact("Last seen", prettyDate(host.lastMs))}
      ${fact("Lifespan", formatSpan(host.lastMs - host.firstMs))}
      ${fact("Captures", host.captures.toLocaleString())}
    </div>
    <div class="tags">${(host.archives || [host.archive]).map(archiveTag).join("")}</div>`,
    clientX,
    clientY
  );
};

const showCapturePlacard = (host, memento, clientX, clientY) => {
  showPlacard(
    `<div class="head">
      <span class="host">${esc(host.host)}</span>
      ${archiveTag(memento.archive)}
    </div>
    <div class="facts">${fact("Captured", prettyDateTime(memento.ms))}</div>
    <div class="hint">Click to open this capture →</div>`,
    clientX,
    clientY
  );
};

/* ------------------- Capture viewer ------------------- */

const isViewerOpen = () => {
  return !viewerEl.hidden;
};

const openViewer = (host, memento) => {
  viewerHost = host;
  viewerMementos = host.mementos;
  viewerIndex = Math.max(0, viewerMementos.indexOf(memento));
  viewerEl.hidden = false;
  hideHover();
  showViewerCapture();
};

const closeViewer = () => {
  viewerEl.hidden = true;
  viewerFrame.src = "about:blank";
  viewerHost = null;
  viewerMementos = [];
  viewerIndex = -1;
};

const stepViewer = delta => {
  const next = viewerIndex + delta;
  if (next < 0 || next >= viewerMementos.length) return;
  viewerIndex = next;
  showViewerCapture();
};

const showViewerCapture = async () => {
  const memento = viewerMementos[viewerIndex];
  if (!memento || !viewerHost) return;
  const requestId = ++viewerRequestId;

  viewerHostEl.textContent = viewerHost.host;
  viewerArchiveEl.textContent = memento.archive;
  viewerDateEl.textContent = prettyDateTime(memento.ms);
  viewerCountEl.textContent = `${viewerIndex + 1} / ${viewerMementos.length}`;
  viewerPrevBtn.disabled = viewerIndex === 0;
  viewerNextBtn.disabled = viewerIndex === viewerMementos.length - 1;
  viewerStatus.textContent = "Loading…";
  viewerStatus.hidden = false;
  viewerExternal.removeAttribute("href");

  const resolved = await resolveReplay(memento.archiveId, memento.archive);
  if (requestId !== viewerRequestId) return;

  const url = mementoURL(
    resolved,
    memento.ts,
    memento.url || viewerHost.host,
    "no_toolbar"
  );
  viewerExternal.href = url;
  if (resolved && !resolved.replay.no_toolbar) {
    viewerStatus.textContent = `${memento.archive} doesn't offer a toolbar-free view, so it can't be shown here. Use “Open in new tab ↗” instead.`;
    return;
  }
  viewerFrame.src = url;
};

viewerFrame.addEventListener("load", () => {
  viewerStatus.hidden = true;
});
viewerPrevBtn.addEventListener("click", () => stepViewer(-1));
viewerNextBtn.addEventListener("click", () => stepViewer(1));
viewerCloseBtn.addEventListener("click", closeViewer);
viewerEl.addEventListener("click", e => {
  if (e.target === viewerEl) closeViewer();
});

/* ------------------- Archive filter ------------------- */

const buildArchiveFilter = () => {
  allArchives = [...new Set(hosts.map(h => h.archive))].sort((a, b) =>
    a.localeCompare(b)
  );
  archivesList.innerHTML = allArchives
    .map(
      name => `<label class="item selected">
        <input type="checkbox" value="${esc(name)}" checked>
        <span>${esc(name)}</span>
      </label>`
    )
    .join("");
  setAllArchives(true);
};

const setAllArchives = checked => {
  selectedArchives = new Set(checked ? allArchives : []);
  for (const input of archivesList.querySelectorAll("input")) {
    input.checked = checked;
    input.closest(".item").classList.toggle("selected", checked);
  }
  updateArchiveFilterLabel();
};

const updateArchiveFilterLabel = () => {
  const count = selectedArchives.size;
  const total = allArchives.length;
  archivesLabel.textContent =
    count === total
      ? "Archives: All"
      : count === 0
        ? "Archives: None"
        : `Archives: ${count}/${total}`;
};

archivesList.addEventListener("change", e => {
  const checkbox = e.target;
  if (!checkbox.matches("input[type=checkbox]")) return;
  if (checkbox.checked) selectedArchives.add(checkbox.value);
  else selectedArchives.delete(checkbox.value);
  checkbox.closest(".item").classList.toggle("selected", checkbox.checked);
  updateArchiveFilterLabel();
  refresh();
});

archivesAll.addEventListener("click", () => {
  setAllArchives(true);
  refresh();
});

archivesNone.addEventListener("click", () => {
  setAllArchives(false);
  refresh();
});

/* ------------------- Dropdowns ------------------- */

const DROPDOWNS = [
  { root: viewDropdown, trigger: viewTrigger, panel: viewPanel },
  { root: archivesDropdown, trigger: archivesTrigger, panel: archivesPanel },
];

for (const { trigger, panel } of DROPDOWNS) {
  trigger.addEventListener("click", () => {
    panel.hidden = !panel.hidden;
  });
}

document.addEventListener("click", e => {
  for (const { root, panel } of DROPDOWNS) {
    if (!panel.hidden && !root.contains(e.target)) panel.hidden = true;
  }
});

/* ------------------- Report ------------------- */

const yearCohorts = data => {
  let firstYear = Infinity;
  for (const host of data)
    firstYear = Math.min(firstYear, new Date(host.firstMs).getFullYear());
  const years = Array.from(
    { length: Math.max(0, new Date().getFullYear() - firstYear + 1) },
    (_, i) => ({
      year: firstYear + i,
      born: 0,
      bornLive: 0,
      died: 0,
      spans: [],
    })
  );
  const yearOf = ms => years[new Date(ms).getFullYear() - firstYear];

  for (const host of data) {
    const born = yearOf(host.firstMs);
    if (born) {
      born.born++;
      if (host.live) born.bornLive++;
      else born.spans.push(host.lastMs - host.firstMs);
    }
    if (!host.live) {
      const died = yearOf(host.lastMs);
      if (died) died.died++;
    }
  }
  return years;
};

const reportTable = years => {
  const rows = years
    .filter(y => y.born || y.died)
    .reverse()
    .map(
      y => `<tr>
        <td>${y.year}</td>
        <td>${y.born.toLocaleString()}</td>
        <td>${y.died.toLocaleString()}</td>
        <td>${y.born ? `${Math.round((y.bornLive / y.born) * 100)}%` : "—"}</td>
        <td>${y.spans.length ? formatSpan(median(y.spans)) : "—"}</td>
      </tr>`
    )
    .join("");
  return `<table class="years">
    <thead><tr><th>Year</th><th>New</th><th>Gone</th><th>Still live</th><th>Median lifespan</th></tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
};

const renderReport = () => {
  const data = visible;
  if (!data.length) {
    reportBody.innerHTML = `<p class="empty">No URLs match the current filters. Clear a filter or search to see a report.</p>`;
    return;
  }

  const liveCount = data.filter(h => h.live).length;
  const span = h => h.lastMs - h.firstMs;
  const longest = data.reduce((a, b) => (span(b) > span(a) ? b : a));
  const years = yearCohorts(data);
  const busiest = years.reduce((a, b) => (b.born > a.born ? b : a));

  reportBody.innerHTML = `
    <div class="intro">
      <h1 data-heading="Still live today">${Math.round((liveCount / data.length) * 100)}%</h1>
      <p>${liveCount.toLocaleString()} of ${data.length.toLocaleString()} URLs still respond on the live web.</p>
    </div>
    <section>
      <h3>At a glance</h3>
      <table>
        <tr><th>Median lifespan</th><td>${formatSpan(median(data.map(span)))}</td></tr>
        <tr><th>Busiest year</th><td>${busiest.year}, ${busiest.born.toLocaleString()} new URLs</td></tr>
        <tr><th>Longest-lived</th><td>${formatSpan(span(longest))}, ${esc(longest.host)}</td></tr>
      </table>
    </section>
    <section>
      <h3>Year by year</h3>
      ${reportTable(years)}
    </section>`;
};

/* ------------------- Chart views ------------------- */

const CHART_FONT_SM = `300 11px ${SANS}`;
const CHART_FONT_MD = `300 14px ${SANS}`;
const CHART_PAD = { top: 72, right: 40, bottom: 48, left: 72 };
const CHART_HEADER_Y = 36;
const CHART_GUTTER = 24;
let chartPointer = null;
let chartFrame = null;

const prepareChart = (canvas, pad = {}) => {
  const section = canvas.parentElement;
  const w = Math.max(1, section.clientWidth);
  const h = Math.max(1, section.clientHeight);
  const ctx = sizeCanvas(canvas, w, h);
  ctx.clearRect(0, 0, w, h);
  const { top, right, bottom, left } = { ...CHART_PAD, ...pad };
  const plot = { left, top, right: w - right, bottom: h - bottom };
  plot.width = Math.max(1, plot.right - plot.left);
  plot.height = Math.max(1, plot.bottom - plot.top);
  const pointer = chartPointer?.canvas === canvas ? chartPointer : null;
  const inside =
    pointer &&
    pointer.x >= plot.left &&
    pointer.x <= plot.right &&
    pointer.y >= plot.top &&
    pointer.y <= plot.bottom;
  return { ctx, w, h, plot, pointer: inside ? pointer : null };
};

const drawEmpty = ctx => {
  ctx.fillStyle = ink(0.5);
  ctx.font = CHART_FONT_MD;
  ctx.textBaseline = "middle";
  ctx.fillText(EMPTY_MESSAGE, CHART_GUTTER, CHART_HEADER_Y);
  ctx.textBaseline = "alphabetic";
  hideHover();
};

const niceStep = (max, count) => {
  const raw = Math.max(1, max) / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  return [1, 2, 2.5, 5, 10].map(s => s * magnitude).find(s => s >= raw);
};

const yearStep = (years, maxTicks) => {
  return [1, 2, 5, 10, 20, 25, 50].find(step => years / step <= maxTicks) || 50;
};

const yearsLabel = years => `${years} yr${years === 1 ? "" : "s"}`;
const percent = value => `${Math.round(value * 100)}%`;

const drawHeader = (ctx, plot, items, note) => {
  ctx.font = CHART_FONT_MD;
  ctx.textBaseline = "middle";
  let x = CHART_GUTTER;
  for (const { label, alpha, kind, active } of items) {
    ctx.fillStyle = ink(alpha);
    ctx.strokeStyle = ink(alpha);
    if (kind === "line") {
      ctx.lineWidth = active ? 3 : 2;
      ctx.beginPath();
      ctx.moveTo(x, CHART_HEADER_Y);
      ctx.lineTo(x + 16, CHART_HEADER_Y);
      ctx.stroke();
      ctx.lineWidth = 1;
    } else {
      ctx.fillRect(x, CHART_HEADER_Y - 5, 10, 10);
    }
    x += kind === "line" ? 24 : 18;
    ctx.fillStyle = ink(active === false ? 0.5 : 1);
    ctx.fillText(label, x, CHART_HEADER_Y);
    x += ctx.measureText(label).width + 24;
  }
  if (note) {
    ctx.fillStyle = ink(0.5);
    ctx.font = CHART_FONT_SM;
    ctx.textAlign = "right";
    ctx.fillText(note, plot.right, CHART_HEADER_Y);
    ctx.textAlign = "left";
  }
  ctx.textBaseline = "alphabetic";
};

const drawGridY = (ctx, plot, ticks) => {
  ctx.font = CHART_FONT_SM;
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (const { y, label, strong } of ticks) {
    ctx.strokeStyle = ink(strong ? 0.4 : 0.12);
    ctx.beginPath();
    ctx.moveTo(plot.left, Math.round(y) + 0.5);
    ctx.lineTo(plot.right, Math.round(y) + 0.5);
    ctx.stroke();
    ctx.fillStyle = ink(0.5);
    ctx.fillText(label, plot.left - 12, y);
  }
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";
};

const drawAxisX = (ctx, plot, ticks) => {
  ctx.font = CHART_FONT_SM;
  ctx.fillStyle = ink(0.5);
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  for (const { x, label } of ticks) ctx.fillText(label, x, plot.bottom + 14);
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";
};

const relativeYearTicks = (maxMonths, xForMonth, maxTicks) => {
  const maxYears = maxMonths / 12;
  const step = yearStep(maxYears, maxTicks);
  const ticks = [];
  for (let year = 0; year <= maxYears + 0.001; year += step)
    ticks.push({ x: xForMonth(year * 12), label: yearsLabel(year) });
  return ticks;
};

const drawCrosshair = (ctx, plot, x) => {
  ctx.strokeStyle = ink(0.4);
  ctx.beginPath();
  ctx.moveTo(Math.round(x) + 0.5, plot.top);
  ctx.lineTo(Math.round(x) + 0.5, plot.bottom);
  ctx.stroke();
};

const drawMarker = (ctx, x, y, radius = 4) => {
  ctx.fillStyle = `rgb(${SEAWEED})`;
  ctx.beginPath();
  ctx.arc(x, y, radius + 2, 0, Math.PI * 2);
  ctx.fill();
  ctx.fillStyle = ink(1);
  ctx.beginPath();
  ctx.arc(x, y, radius, 0, Math.PI * 2);
  ctx.fill();
};

const chartRow = ({ label, value, alpha = 1, kind = "line", active }) =>
  `<div class="fact${active ? " active" : ""}">
    <span class="k"><i class="${kind}" style="opacity: ${alpha}"></i>${esc(label)}</span>
    <span class="v">${value}</span>
  </div>`;

const showChartPlacard = (pointer, title, rows) => {
  showPlacard(
    `<div class="head"><span class="host">${esc(title)}</span></div>
    <div class="facts">${rows.map(chartRow).join("")}</div>`,
    pointer.clientX,
    pointer.clientY
  );
};

const eraCohorts = data => {
  return TOP_ERAS.map(era => ({
    era,
    born: data.filter(h => h.firstMs >= era.startMs && h.firstMs < era.endMs),
  })).filter(cohort => cohort.born.length);
};

const eraAlpha = (i, count) => 0.35 + 0.65 * (i / Math.max(1, count - 1));

const renderRidgeline = () => {
  const { ctx, w, plot, pointer } = prepareChart(ridgelineCanvas, {
    left: 232,
  });
  const cohorts = eraCohorts(visible);
  if (!cohorts.length) return drawEmpty(ctx);

  const nowMs = Date.now();
  for (const cohort of cohorts) {
    cohort.elapsedMonths = (nowMs - cohort.era.startMs) / MONTH_MS;
    const spans = cohort.born
      .map(h => (h.live ? Infinity : h.lastMs - cohort.era.startMs))
      .sort((a, b) => a - b);
    cohort.retentionAt = months => {
      const cutoff = months * MONTH_MS;
      let lo = 0;
      let hi = spans.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (spans[mid] < cutoff) lo = mid + 1;
        else hi = mid;
      }
      return (spans.length - lo) / spans.length;
    };
  }

  const maxMonths = Math.max(...cohorts.map(c => c.elapsedMonths));
  const xForMonth = months => plot.left + (months / maxMonths) * plot.width;
  const bandH = plot.height / cohorts.length;
  const hoverMonths = pointer
    ? ((pointer.x - plot.left) / plot.width) * maxMonths
    : null;
  const hoverBand = pointer
    ? clamp(Math.floor((pointer.y - plot.top) / bandH), 0, cohorts.length - 1)
    : -1;

  drawHeader(
    ctx,
    plot,
    [],
    "Share of URLs still active, by years since each era began"
  );
  drawAxisX(ctx, plot, relativeYearTicks(maxMonths, xForMonth, 10));

  cohorts.forEach((cohort, i) => {
    const top = plot.top + i * bandH;
    const baseline = top + bandH - 8;
    const peak = top + Math.min(16, bandH / 4);
    const yFor = months =>
      baseline - cohort.retentionAt(months) * (baseline - peak);
    const dimmed = pointer && i !== hoverBand;

    ctx.strokeStyle = ink(0.12);
    ctx.beginPath();
    ctx.moveTo(CHART_GUTTER, Math.round(top + bandH) + 0.5);
    ctx.lineTo(w - CHART_PAD.right, Math.round(top + bandH) + 0.5);
    ctx.stroke();

    const steps = Math.max(2, Math.round(cohort.elapsedMonths));
    const points = Array.from({ length: steps + 1 }, (_, s) => {
      const months = (s / steps) * cohort.elapsedMonths;
      return [xForMonth(months), yFor(months)];
    });

    ctx.beginPath();
    ctx.moveTo(points[0][0], baseline);
    for (const [x, y] of points) ctx.lineTo(x, y);
    ctx.lineTo(points[points.length - 1][0], baseline);
    ctx.closePath();
    ctx.fillStyle = ink(dimmed ? 0.04 : 0.1);
    ctx.fill();

    ctx.beginPath();
    points.forEach(([x, y], idx) =>
      idx ? ctx.lineTo(x, y) : ctx.moveTo(x, y)
    );
    ctx.strokeStyle = ink(dimmed ? 0.35 : 1);
    ctx.lineWidth = 2;
    ctx.stroke();
    ctx.lineWidth = 1;

    const middle = top + bandH / 2;
    ctx.textBaseline = "middle";
    ctx.fillStyle = ink(dimmed ? 0.5 : 1);
    ctx.font = CHART_FONT_MD;
    ctx.fillText(cohort.era.label, CHART_GUTTER, middle - 9);
    ctx.fillStyle = ink(0.5);
    ctx.font = CHART_FONT_SM;
    ctx.fillText(
      `${cohort.born.length.toLocaleString()} URLs · ${percent(cohort.retentionAt(cohort.elapsedMonths))} active now`,
      CHART_GUTTER,
      middle + 10
    );
    ctx.textBaseline = "alphabetic";

    if (pointer && hoverMonths <= cohort.elapsedMonths)
      cohort.marker = [pointer.x, yFor(hoverMonths)];
  });

  if (!pointer) return hideHover();
  drawCrosshair(ctx, plot, pointer.x);
  for (const cohort of cohorts)
    if (cohort.marker) drawMarker(ctx, ...cohort.marker);
  showChartPlacard(
    pointer,
    `${yearsLabel(Math.floor(hoverMonths / 12))} after era began`,
    cohorts.map((cohort, i) => ({
      label: cohort.era.label,
      value: cohort.marker ? percent(cohort.retentionAt(hoverMonths)) : "–",
      active: i === hoverBand,
    }))
  );
};

const renderGrowth = () => {
  const { ctx, plot, pointer } = prepareChart(growthCanvas);
  if (!visible.length) return drawEmpty(ctx);

  const monthKey = ms => {
    const d = new Date(ms);
    return new Date(d.getFullYear(), d.getMonth(), 1).getTime();
  };
  const first = new Date(fullStart);
  const months = [];
  for (
    let m = first.getMonth(), t = monthKey(fullStart);
    t <= fullEnd;
    t = new Date(first.getFullYear(), ++m, 1).getTime()
  )
    months.push(t);

  const born = new Map();
  const died = new Map();
  for (const host of visible) {
    bump(born, monthKey(host.firstMs));
    if (!host.live) bump(died, monthKey(host.lastMs));
  }
  let maxVal = 1;
  for (const m of months)
    maxVal = Math.max(maxVal, born.get(m) || 0, died.get(m) || 0);
  const step = niceStep(maxVal, 3);
  const top = Math.ceil(maxVal / step) * step;

  const midY = plot.top + plot.height / 2;
  const yFor = value => midY - (value / top) * (plot.height / 2);
  const colW = plot.width / months.length;
  const barW = Math.max(1, colW - (colW > 3 ? 1 : 0));
  const hover = pointer
    ? clamp(Math.floor((pointer.x - plot.left) / colW), 0, months.length - 1)
    : -1;

  drawHeader(
    ctx,
    plot,
    [
      { label: "First captured", alpha: 0.85, kind: "rect" },
      { label: "Went dark", alpha: 0.35, kind: "rect" },
    ],
    "URLs per month"
  );

  const ticks = [];
  for (let v = step; v <= top; v += step)
    ticks.push({ y: yFor(v), label: v.toLocaleString() });
  for (let v = step; v <= top; v += step)
    ticks.push({ y: yFor(-v), label: v.toLocaleString() });
  ticks.push({ y: midY, label: "0", strong: true });
  drawGridY(ctx, plot, ticks);

  const endYear = new Date(fullEnd).getFullYear();
  const yStep = yearStep(endYear - first.getFullYear(), 12);
  const xForMs = ms =>
    plot.left + ((ms - months[0]) / (fullEnd - months[0])) * plot.width;
  const yearTicks = [];
  for (
    let year = Math.ceil(first.getFullYear() / yStep) * yStep;
    year <= endYear;
    year += yStep
  )
    yearTicks.push({ x: xForMs(new Date(year, 0, 1).getTime()), label: year });
  drawAxisX(ctx, plot, yearTicks);

  if (hover >= 0) {
    ctx.fillStyle = ink(0.08);
    ctx.fillRect(plot.left + hover * colW - 2, plot.top, barW + 4, plot.height);
  }

  months.forEach((m, i) => {
    const x = plot.left + i * colW;
    const dim = hover >= 0 && i !== hover ? 0.75 : 1;
    ctx.fillStyle = ink(0.85 * dim);
    ctx.fillRect(
      x,
      yFor(born.get(m) || 0),
      barW,
      midY - yFor(born.get(m) || 0)
    );
    ctx.fillStyle = ink(0.35 * dim);
    ctx.fillRect(x, midY + 1, barW, yFor(-(died.get(m) || 0)) - midY);
  });

  if (!pointer) return hideHover();
  const m = months[hover];
  showChartPlacard(
    pointer,
    new Date(m).toLocaleDateString("en-GB", { month: "long", year: "numeric" }),
    [
      {
        label: "First captured",
        value: (born.get(m) || 0).toLocaleString(),
        alpha: 0.85,
        kind: "rect",
      },
      {
        label: "Went dark",
        value: (died.get(m) || 0).toLocaleString(),
        alpha: 0.35,
        kind: "rect",
      },
    ]
  );
};

const kaplanMeier = spans => {
  const sorted = [...spans].sort((a, b) => a.t - b.t);
  const steps = [{ t: 0, s: 1 }];
  let atRisk = sorted.length;
  let survival = 1;
  for (let i = 0; i < sorted.length;) {
    const t = sorted[i].t;
    let deaths = 0;
    let exits = 0;
    for (; i < sorted.length && sorted[i].t === t; i++, exits++)
      if (sorted[i].died) deaths++;
    if (deaths) {
      survival *= 1 - deaths / atRisk;
      steps.push({ t, s: survival });
    }
    atRisk -= exits;
  }
  return steps;
};

const survivalAt = (steps, t) => {
  let s = 1;
  for (const step of steps) {
    if (step.t > t) break;
    s = step.s;
  }
  return s;
};

const renderSurvival = () => {
  const { ctx, plot, pointer } = prepareChart(survivalCanvas);
  const cohorts = eraCohorts(visible);
  if (!cohorts.length) return drawEmpty(ctx);

  for (const cohort of cohorts) {
    const spans = cohort.born.map(h => ({
      t: Math.max(0, (h.lastMs - cohort.era.startMs) / MONTH_MS),
      died: !h.live,
    }));
    cohort.steps = kaplanMeier(spans);
    cohort.maxT = Math.max(...spans.map(s => s.t));
  }

  const maxMonths = Math.max(1, ...cohorts.map(c => c.maxT));
  const xForMonth = months => plot.left + (months / maxMonths) * plot.width;
  const yFor = s => plot.top + (1 - s) * plot.height;
  const hoverMonths = pointer
    ? ((pointer.x - plot.left) / plot.width) * maxMonths
    : null;

  let active = -1;
  if (pointer) {
    let best = Infinity;
    cohorts.forEach((cohort, i) => {
      if (hoverMonths > cohort.maxT) return;
      cohort.hoverS = survivalAt(cohort.steps, hoverMonths);
      const distance = Math.abs(yFor(cohort.hoverS) - pointer.y);
      if (distance < best) [best, active] = [distance, i];
    });
  }
  const alphaFor = i => {
    if (active < 0) return eraAlpha(i, cohorts.length);
    return i === active ? 1 : 0.2;
  };

  drawHeader(
    ctx,
    plot,
    cohorts.map((cohort, i) => ({
      label: cohort.era.label,
      alpha: alphaFor(i),
      kind: "line",
      active: active < 0 ? undefined : i === active,
    })),
    "Kaplan–Meier estimate"
  );
  drawGridY(
    ctx,
    plot,
    [0, 0.25, 0.5, 0.75, 1].map(s => ({
      y: yFor(s),
      label: percent(s),
      strong: s === 0,
    }))
  );
  drawAxisX(ctx, plot, relativeYearTicks(maxMonths, xForMonth, 10));

  const order = cohorts.map((_, i) => i);
  if (active >= 0) order.push(order.splice(active, 1)[0]);
  for (const i of order) {
    const cohort = cohorts[i];
    let prev = 1;
    ctx.strokeStyle = ink(alphaFor(i));
    ctx.lineWidth = i === active ? 2.5 : 2;
    ctx.beginPath();
    ctx.moveTo(xForMonth(0), yFor(1));
    for (const { t, s } of cohort.steps.slice(1)) {
      ctx.lineTo(xForMonth(t), yFor(prev));
      ctx.lineTo(xForMonth(t), yFor(s));
      prev = s;
    }
    ctx.lineTo(xForMonth(cohort.maxT), yFor(prev));
    ctx.stroke();
  }
  ctx.lineWidth = 1;

  if (!pointer) return hideHover();
  drawCrosshair(ctx, plot, pointer.x);
  for (const i of order)
    if (hoverMonths <= cohorts[i].maxT)
      drawMarker(ctx, pointer.x, yFor(cohorts[i].hoverS), i === active ? 4 : 3);
  showChartPlacard(
    pointer,
    `${yearsLabel(Math.floor(hoverMonths / 12))} after era began`,
    cohorts.map((cohort, i) => ({
      label: cohort.era.label,
      value: hoverMonths <= cohort.maxT ? percent(cohort.hoverS) : "–",
      alpha: eraAlpha(i, cohorts.length),
      active: i === active,
    }))
  );
};

const renderAgePyramid = () => {
  const { ctx, plot, pointer } = prepareChart(ageCanvas);
  if (!visible.length) return drawEmpty(ctx);

  const nowMs = Date.now();
  const live = new Map();
  const dead = new Map();
  let maxAge = 0;
  for (const host of visible) {
    const age = Math.max(
      0,
      Math.floor(((host.live ? nowMs : host.lastMs) - host.firstMs) / YEAR_MS)
    );
    maxAge = Math.max(maxAge, age);
    bump(host.live ? live : dead, age);
  }

  let maxCount = 1;
  for (const count of [...live.values(), ...dead.values()])
    maxCount = Math.max(maxCount, count);
  const step = niceStep(maxCount, 3);
  const midX = plot.left + plot.width / 2;
  const sideW = plot.width / 2;
  const widthFor = count => (Math.sqrt(count) / Math.sqrt(maxCount)) * sideW;
  const rowH = plot.height / (maxAge + 1);
  const yForAge = age => plot.bottom - (age + 1) * rowH;
  const hover = pointer
    ? clamp(Math.floor((plot.bottom - pointer.y) / rowH), 0, maxAge)
    : -1;

  drawHeader(
    ctx,
    plot,
    [
      { label: "Went dark", alpha: 0.35, kind: "rect" },
      { label: "Still live", alpha: 0.85, kind: "rect" },
    ],
    "Years between first and last capture · square-root scale"
  );

  ctx.font = CHART_FONT_SM;
  ctx.textBaseline = "top";
  ctx.textAlign = "center";
  for (let v = 0; v <= maxCount; v += step) {
    for (const x of v ? [midX - widthFor(v), midX + widthFor(v)] : [midX]) {
      ctx.strokeStyle = ink(v ? 0.12 : 0.4);
      ctx.beginPath();
      ctx.moveTo(Math.round(x) + 0.5, plot.top);
      ctx.lineTo(Math.round(x) + 0.5, plot.bottom);
      ctx.stroke();
      ctx.fillStyle = ink(0.5);
      ctx.fillText(v.toLocaleString(), x, plot.bottom + 14);
    }
  }

  const labelEvery = Math.ceil(16 / rowH);
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let age = 0; age <= maxAge; age++) {
    const y = yForAge(age);
    const gap = rowH > 4 ? 1 : 0;
    const dim = hover >= 0 && age !== hover ? 0.75 : 1;
    if (age === hover) {
      ctx.fillStyle = ink(0.08);
      ctx.fillRect(plot.left, y, plot.width, rowH);
    }
    ctx.fillStyle = ink(0.85 * dim);
    ctx.fillRect(
      midX + 1,
      y + gap,
      widthFor(live.get(age) || 0),
      rowH - gap * 2
    );
    ctx.fillStyle = ink(0.35 * dim);
    const deadW = widthFor(dead.get(age) || 0);
    ctx.fillRect(midX - deadW, y + gap, deadW, rowH - gap * 2);
    if (age % labelEvery === 0 || age === hover) {
      ctx.fillStyle = ink(age === hover ? 1 : 0.5);
      ctx.fillText(yearsLabel(age), plot.left - 12, y + rowH / 2);
    }
  }
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";

  if (!pointer) return hideHover();
  showChartPlacard(pointer, `Lasted ${yearsLabel(hover)}`, [
    {
      label: "Still live",
      value: (live.get(hover) || 0).toLocaleString(),
      alpha: 0.85,
      kind: "rect",
    },
    {
      label: "Went dark",
      value: (dead.get(hover) || 0).toLocaleString(),
      alpha: 0.35,
      kind: "rect",
    },
  ]);
};

const trackChartPointer = e => {
  const rect = e.currentTarget.getBoundingClientRect();
  chartPointer = {
    canvas: e.currentTarget,
    x: e.clientX - rect.left,
    y: e.clientY - rect.top,
    clientX: e.clientX,
    clientY: e.clientY,
  };
  if (chartFrame) return;
  chartFrame = requestAnimationFrame(() => {
    chartFrame = null;
    renderActiveView();
  });
};

for (const canvas of [
  ridgelineCanvas,
  growthCanvas,
  survivalCanvas,
  ageCanvas,
]) {
  canvas.addEventListener("pointermove", trackChartPointer);
  canvas.addEventListener("pointerleave", () => {
    if (chartPointer?.canvas !== canvas) return;
    chartPointer = null;
    renderActiveView();
  });
}

/* ------------------- Overlays ------------------- */

const isOverlayOpen = overlay => {
  return !overlay.hidden;
};

const openOverlay = overlay => {
  overlay.hidden = false;
  requestAnimationFrame(() =>
    requestAnimationFrame(() => overlay.classList.add("visible"))
  );
};

const closeOverlay = overlay => {
  if (overlay === aboutOverlay && !aboutDismissed) {
    aboutDismissed = true;
    pendingIntro?.();
    pendingIntro = null;
  }
  overlay.classList.remove("visible");
  setTimeout(() => {
    overlay.hidden = true;
  }, 300);
};

const bindOverlay = (overlay, trigger, closeBtn, beforeOpen) => {
  trigger.addEventListener("click", () => {
    if (isOverlayOpen(overlay)) return closeOverlay(overlay);
    beforeOpen?.();
    openOverlay(overlay);
  });
  closeBtn.addEventListener("click", () => closeOverlay(overlay));
  overlay.addEventListener("click", e => {
    if (e.target === overlay) closeOverlay(overlay);
  });
};

bindOverlay(aboutOverlay, aboutTrigger, aboutClose);
bindOverlay(reportOverlay, reportTrigger, reportClose, renderReport);

/* ------------------- Exhibition mode ------------------- */

const toggleExhibition = async () => {
  if (document.fullscreenElement) return document.exitFullscreen();
  try {
    await document.documentElement.requestFullscreen();
  } catch {
    document.body.classList.toggle("exhibition");
    layout();
  }
};

document.addEventListener("fullscreenchange", () => {
  document.body.classList.toggle("exhibition", !!document.fullscreenElement);
  layout();
});

/* ------------------- Init ------------------- */

const renderRows = rows =>
  rows
    .map(
      ([keys, action]) =>
        `<div class="row"><b>${keys}</b><span>${action}</span></div>`
    )
    .join("");

$("shortcuts-panel").innerHTML = renderRows(CONTROLS);
$("year").textContent = new Date().getFullYear();
window.addEventListener("resize", renderActiveView);

loadDefaultData();
setTimeout(() => openOverlay(aboutOverlay), 300);
