import 'mocha';
import { assert } from 'chai';
import { gzipCompress, PgStorage } from './storage';
import * as log4js from "log4js";
import * as pg from "pg";

describe('Storage', async () => {
    log4js.configure({
        appenders: {
            ruleConsole: { type: 'console' }
        },
        categories: {
            default: { appenders: ['ruleConsole'], level: "debug" }
        },
        // replaceConsole:true
    });
    let options = {
        database: {
            connectionString: 'postgresql://cny:123@loc.m:5432/cny',
        },
    };
    it("gzip", async () => {
        let buf = await gzipCompress("all data");
        assert.isTrue(buf.length > 0);
    })
    it("bootstrap", async () => {
        let pool = new pg.Pool(options.database);
        let client = await pool.connect();
        await client.query("DROP TABLE IF EXISTS CRAWLER_PAGE");
        await client.release();
        await pool.end();
        //
        let storage = new PgStorage();
        await storage.bootstrap(options);
        await storage.release();
        //
        storage = new PgStorage();
        await storage.bootstrap(options);
        await storage.release();
    })
    it("save", async () => {
        let storage = new PgStorage();
        await storage.bootstrap(options);
        let uris = [];
        for (let i = 0; i < 100; i++) {
            let uri = "http://www.baidu.com/" + i;
            await storage.save(uri, ["t0"], "testing" + i, { a: i, b: i % 3 });
            uris.push(uri);
        }
        let found = await storage.exist(...uris)
        await storage.release();
        assert.equal(found, 100);
    })
});