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
    const col = process.env.ARCHIVE_COLUMN;
    const from = process.env.ARCHIVE_FROM;
    const to = process.env.ARCHIVE_TO;
    if (col && from && to) {
      await this.archiveRange({ from, to, column: col });
      return;
    }
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

  private isValidIdentifier(name: string): boolean {
    return /^[A-Za-z0-9_]+$/.test(name);
  }

  async archiveRange(args: {
    from: string;
    to: string;
    column: string;
    tables?: string[];
  }): Promise<{
    tables: string[];
    inserted: Record<string, number>;
    deleted: Record<string, number>;
  }> {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      await conn.query(`CREATE DATABASE IF NOT EXISTS \`${db2}\``);
      const column = args.column;
      if (!this.isValidIdentifier(column)) {
        throw new Error('invalid column');
      }
      let targetTables =
        args.tables && args.tables.length > 0 ? args.tables : [];
      if (targetTables.length === 0) {
        interface TableRow extends RowDataPacket {
          TABLE_NAME: string;
        }
        const [rows] = await conn.query<TableRow[]>(
          'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE="BASE TABLE"',
          [db1],
        );
        targetTables = rows.map((r) => r.TABLE_NAME);
      }
      const validTables: string[] = [];
      for (const t of targetTables) {
        if (!this.isValidIdentifier(t)) continue;
        const [cols] = await conn.query<RowDataPacket[]>(
          'SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=? LIMIT 1',
          [db1, t, column],
        );
        if (Array.isArray(cols) && cols.length > 0) validTables.push(t);
      }
      const inserted: Record<string, number> = {};
      const deleted: Record<string, number> = {};
      for (const t of validTables) {
        const src = `\`${db1}\`.\`${t}\``;
        const dst = `\`${db2}\`.\`${t}\``;
        await conn.beginTransaction();
        await conn.query(`CREATE TABLE IF NOT EXISTS ${dst} LIKE ${src}`);
        const [ins] = await conn.query<ResultSetHeader>(
          `INSERT IGNORE INTO ${dst} SELECT * FROM ${src} WHERE \`${column}\` BETWEEN ? AND ?`,
          [args.from, args.to],
        );
        const [del] = await conn.query<ResultSetHeader>(
          `DELETE FROM ${src} WHERE \`${column}\` BETWEEN ? AND ?`,
          [args.from, args.to],
        );
        await conn.commit();
        inserted[t] = ins.affectedRows ?? 0;
        deleted[t] = del.affectedRows ?? 0;
      }
      this.logger.log(
        `Archived range on ${validTables.length} table(s) from ${db1} to ${db2}`,
      );
      return { tables: validTables, inserted, deleted };
    } catch (e) {
      try {
        await conn.rollback();
      } catch (err) {
        this.logger.error(String(err));
      }
      throw e;
    } finally {
      conn.release();
    }
  }
}
