// c:\Users\USER\archiving-db\src\archiving.service.ts
import { Injectable, Inject, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import type { Pool, RowDataPacket, ResultSetHeader } from 'mysql2/promise';

@Injectable()
export class ArchivingService {
  private readonly logger = new Logger(ArchivingService.name);
  constructor(@Inject('MYSQL_POOL') private readonly pool: Pool) {}

  @Cron(process.env.ARCHIVE_CRON ?? '0 0 * * *', { waitForCompletion: true })
  async handleCron() {
    await this.archive();
  }

  async archive(): Promise<{
    tables: string[];
    inserted: Record<string, number>;
  }> {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      await conn.query(`CREATE DATABASE IF NOT EXISTS \`${db2}\``);
      interface TableRow extends RowDataPacket {
        TABLE_NAME: string;
      }
      const [rows] = await conn.query<TableRow[]>(
        'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE="BASE TABLE"',
        [db1],
      );
      const tables = rows.map((r) => r.TABLE_NAME);
      const inserted: Record<string, number> = {};
      for (const t of tables) {
        const src = `\`${db1}\`.\`${t}\``;
        const dst = `\`${db2}\`.\`${t}\``;
        await conn.query(`CREATE TABLE IF NOT EXISTS ${dst} LIKE ${src}`);
        const [res] = await conn.query<ResultSetHeader>(
          `INSERT IGNORE INTO ${dst} SELECT * FROM ${src}`,
        );
        inserted[t] = res.affectedRows ?? 0;
      }
      this.logger.log(
        `Archived ${tables.length} table(s) from ${db1} to ${db2}`,
      );
      return { tables, inserted };
    } finally {
      conn.release();
    }
  }
}
