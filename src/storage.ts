
import { Storage } from "crawler";
import * as pg from "pg";
import { createGzip } from "zlib";
import * as log4js from "log4js";
import { LargeObjectManager } from 'pg-large-object';

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
    public static INIT_SQL = `
        create table crawler_page (
            tid serial primary key,
            uri text not null,
            data numeric not null,
            attrs json,
            create_time numeric,
            time timestamp not null default now(),
            status text not null
        );
    `;
    Log = log4js.getLogger("PgStorage");
    pool: pg.Pool = new pg.Pool();
    options: any;

    public async bootstrap(options: any): Promise<any> {
        this.options = options;
        if (!this.options.init) {
            this.options.init = PgStorage.INIT_SQL;
        }
        if (!this.options.status || !options.database) {
            throw "the status/database configure is required";
        }
        this.pool = new pg.Pool(options.database);
        let client = await this.pool.connect();
        try {
            this.Log.info("start check crawler_page table if exitus");
            await client.query(`select tid from crawler_page limit 1`)
            this.Log.info("the crawler_page table is exitus");
        } catch (e) {
            if (e.message.indexOf("crawler_page") >= 0) {
                this.Log.info("the crawler_page table is not exitus, will try create it");
                await client.query(options.init);
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
            await client.query("begin");
            let done = false;
            let tid: any;
            let did: any;
            try {
                let lom = new LargeObjectManager({ pg: client });
                let stream = await lom.createAndWritableStreamAsync(10240);
                let create_time = options.create_time;
                let atrrs: any = {}
                Object.assign(atrrs, options)
                options.tags = tags;
                delete options.create_time;
                let result = await client.query("insert into crawler_page(uri,attrs,data,create_time,status) values ($1, $2, $3, $4, $5) returning tid",
                    [uri, options, stream[0], create_time, this.options.status]);
                await new Promise((resolve, reject) => {
                    stream[1].on("finish", () => {
                        resolve();
                    });
                    stream[1].on("error", (err: Error) => {
                        reject(err);
                    })
                    stream[1].write(buf, (err: Error) => {
                        if (err) {
                            reject(err);
                        }
                        stream[1].end(() => {
                            resolve();
                        });
                    });
                });
                done = true;
                did = stream[0];
                tid = result.rows[0].tid;
            } finally {
                if (done) {
                    await client.query("commit");
                    this.Log.info("saving page data on %s is success by tid:%s,data:%s", uri, tid, did);
                } else {
                    await client.query("rollback");
                }
            }
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