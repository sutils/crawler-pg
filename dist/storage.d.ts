/// <reference types="node" />
import { DataStorage } from "crawler";
import * as pg from "pg";
import * as log4js from "log4js";
export declare function gzipCompress(data: any, options?: any): Promise<Buffer>;
export declare class PgStorage implements DataStorage {
    static SQL: string;
    Log: log4js.Logger;
    pool: pg.Pool;
    options: any;
    bootstrap(options: any): Promise<any>;
    save(uri: string, data: any, options: any): Promise<any>;
    exist(...uris: string[]): Promise<number>;
    release(): Promise<any>;
}
