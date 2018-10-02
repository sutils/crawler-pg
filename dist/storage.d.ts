/// <reference types="node" />
import { Storage } from "crawler";
import * as pg from "pg";
import * as log4js from "log4js";
export declare function gzipCompress(data: any, options?: any): Promise<Buffer>;
export declare class PgStorage implements Storage {
    static INIT_SQL: string;
    Log: log4js.Logger;
    pool: pg.Pool;
    options: any;
    bootstrap(options: any): Promise<any>;
    save(uri: string, tags: string[], data: any, options: any): Promise<any>;
    find(fields: string, ...uris: string[]): Promise<any[]>;
    release(): Promise<any>;
}
