import Sqlite from "better-sqlite3";
import path from "node:path";
import PgPromise from "pg-promise";

import type { PonderConfig } from "@/cli/readPonderConfig";
import { logger } from "@/common/logger";
import { OPTIONS } from "@/common/options";
import { ensureDirExists } from "@/common/utils";

export interface SqliteDb {
  kind: "sqlite";

  db: Sqlite.Database;
}

export interface PostgresDb {
  kind: "postgres";

  pgp: PgPromise.IMain<unknown>;
  db: PgPromise.IDatabase<unknown>;
}

export type PonderDatabase = SqliteDb | PostgresDb;

export const buildDb = (config: PonderConfig): PonderDatabase => {
  switch (config.database.kind) {
    case "sqlite": {
      const dbFilePath =
        config.database.filename ||
        path.join(OPTIONS.PONDER_DIR_PATH, "cache.db");
      ensureDirExists(dbFilePath);

      return {
        kind: "sqlite",
        db: Sqlite(dbFilePath, { verbose: logger.trace }),
      };
    }
    case "postgres": {
      const pgp = PgPromise({
        query: (e) => {
          logger.trace({ query: e.query });
        },
        error: (err, e) => {
          logger.error({ err, e });
        },
      });

      return {
        kind: "postgres",
        pgp,
        db: pgp({
          connectionString: config.database.connectionString,
          keepAlive: true,
        }),
      };
    }
  }
};
