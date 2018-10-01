"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const pg = require("pg");
const zlib_1 = require("zlib");
const log4js = require("log4js");
function gzipCompress(data, options) {
    return new Promise((resolve, reject) => {
        let all = [];
        let gzip = zlib_1.createGzip(options);
        gzip.on('data', (chunk) => {
            all.push(chunk);
        });
        gzip.once("end", () => {
            resolve(Buffer.concat(all));
        });
        gzip.once("error", (e) => {
            reject(e);
        });
        gzip.write(data);
        gzip.end();
    });
}
exports.gzipCompress = gzipCompress;
class PgStorage {
    constructor() {
        this.Log = log4js.getLogger("PgStorage");
        this.pool = new pg.Pool();
    }
    bootstrap(options) {
        return __awaiter(this, void 0, void 0, function* () {
            this.options = options;
            this.pool = new pg.Pool(options.database);
            let client = yield this.pool.connect();
            try {
                this.Log.info("start check crawler_page table if exitus");
                yield client.query(`select tid from crawler_page limit 1`);
                this.Log.info("the crawler_page table is exitus");
            }
            catch (e) {
                if (e.message.indexOf("crawler_page") >= 0) {
                    this.Log.info("the crawler_page table is not exitus, will try create it");
                    yield client.query(PgStorage.SQL);
                    this.Log.info("create crawler_page table success");
                }
                else {
                    this.Log.error("check table is exitus fail with %s", e);
                    throw e;
                }
            }
            finally {
                client.release();
            }
        });
    }
    save(uri, data, options) {
        return __awaiter(this, void 0, void 0, function* () {
            let buf = yield gzipCompress(data, this.options.compress);
            let client = yield this.pool.connect();
            try {
                let result = yield client.query("insert into crawler_page(uri,attrs,data) values ($1, $2, $3) returning tid", [uri, options, buf]);
                this.Log.info("saving page data on %s is success by tid:%s", uri, result.rows[0].tid);
            }
            finally {
                client.release();
            }
        });
    }
    exist(...uris) {
        return __awaiter(this, void 0, void 0, function* () {
            let client = yield this.pool.connect();
            try {
                let result = yield client.query("select count(*)::int as found from crawler_page where uri = any($1::text[])", [uris]);
                return result.rows[0].found;
            }
            finally {
                client.release();
            }
        });
    }
    release() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.pool.end();
        });
    }
}
PgStorage.SQL = `
        create table crawler_page (
            tid serial primary key,
            uri text not null,
            attrs json,
            data bytea
        );
    `;
exports.PgStorage = PgStorage;
//# sourceMappingURL=storage.js.map