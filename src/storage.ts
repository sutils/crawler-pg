
import { Storage } from "crawler";
import * as pg from "pg";
import { createGzip } from "zlib";
import * as log4js from "log4js";

export function gzipCompress(data: any, options?: any): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
        let all: Buffer[] = [];
        let gzip = createGzip(options);
        gzip.on('data', (chunk: any) => {
            all.push(chunk);
        })
        gzip.once("end", () => {
            resolve(Buffer.concat(all))
        })
        gzip.once("error", (e) => {
            reject(e);
        })
        gzip.write(data);
        gzip.end()
    });
}

export class PgStorage implements Storage {
    static SQL = `
        create table crawler_page (
            tid serial primary key,
            uri text not null,
            attrs json,
            data bytea,
            create_time timestamp not null default now()
        );
    `
    Log = log4js.getLogger("PgStorage");
    pool: pg.Pool = new pg.Pool();
    options: any;

    public async bootstrap(options: any): Promise<any> {
        this.options = options;
        this.pool = new pg.Pool(options.database);
        let client = await this.pool.connect();
        try {
            this.Log.info("start check crawler_page table if exitus");
            await client.query(`select tid from crawler_page limit 1`)
            this.Log.info("the crawler_page table is exitus");
        } catch (e) {
            if (e.message.indexOf("crawler_page") >= 0) {
                this.Log.info("the crawler_page table is not exitus, will try create it");
                await client.query(PgStorage.SQL);
                this.Log.info("create crawler_page table success");
            } else {
                this.Log.error("check table is exitus fail with %s", e)
                throw e;
            }
        } finally {
            client.release();
        }
    }

    public async save(uri: string, tags: string[], data: any, options: any): Promise<any> {
        let buf = await gzipCompress(data, this.options.compress);
        let client = await this.pool.connect();
        try {
            if (!options) {
                options = {};
            }
            options.tags = tags;
            let result = await client.query("insert into crawler_page(uri,attrs,data) values ($1, $2, $3) returning tid", [uri, options, buf]);
            this.Log.info("saving page data on %s is success by tid:%s", uri, result.rows[0].tid);
        } finally {
            client.release();
        }
    }

    public async find(fields: string, ...uris: string[]): Promise<any[]> {
        let client = await this.pool.connect();
        try {
            let result = await client.query("select " + fields + " from crawler_page where uri = any($1::text[])", [uris]);
            return result.rows;
        } finally {
            client.release();
        }
    }

    public async release(): Promise<any> {
        return this.pool.end()
    }
}