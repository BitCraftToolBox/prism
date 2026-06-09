import {DbConnection as DbConnectionGlobal,} from "../bindings_global/src";
import {RuntimeConfig} from "./config";
import {GlobalData} from "./types";

const GLOBAL_MODULE = "bitcraft-live-global";

export async function fetch_global_data(config: RuntimeConfig): Promise<GlobalData> {
    return new Promise<GlobalData>((resolve, reject) => {
        DbConnectionGlobal.builder()
            .withUri(config.upstream_hostname)
            .withModuleName(GLOBAL_MODULE)
            .withToken(config.upstream_token)
            .onConnect((conn) => {
                conn
                    .subscriptionBuilder()
                    .onApplied(() => {
                        const data: GlobalData = {
                            empire_state: Array.from(conn.db.empireState.iter()),
                            empire_chunk_state: Array.from(conn.db.empireChunkState.iter()),
                            empire_color_desc: Array.from(conn.db.empireColorDesc.iter()),
                            empire_emblem_state: Array.from(conn.db.empireEmblemState.iter()),
                        };
                        conn.disconnect();
                        resolve(data);
                    })
                    .subscribe([
                        "SELECT * FROM empire_state",
                        "SELECT * FROM empire_chunk_state",
                        "SELECT * FROM empire_color_desc",
                        "SELECT * FROM empire_emblem_state",
                    ]);
            })
            .onConnectError((_ctx, err) => {
                // @ts-ignore generated SDK error shape includes wasClean in runtime.
                if (!err?.wasClean) {
                    reject(err);
                }
            })
            .build();
    });
}

