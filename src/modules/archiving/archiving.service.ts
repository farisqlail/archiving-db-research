// c:\Users\USER\archiving-db\src\archiving.service.ts
import { Injectable, Inject, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import type { Pool, RowDataPacket, ResultSetHeader } from 'mysql2/promise';

@Injectable()
export class ArchivingService {
  private readonly logger = new Logger(ArchivingService.name);
  constructor(@Inject('MYSQL_POOL') private readonly pool: Pool) {}

  // @Cron(process.env.ARCHIVE_CRON ?? '0 0 * * *', { waitForCompletion: true })
  async handleCron() {
    this.logger.log('Cron job is disabled. Use API to trigger archiving.');
    /*
    const col = process.env.ARCHIVE_COLUMN;
    const from = process.env.ARCHIVE_FROM;
    const to = process.env.ARCHIVE_TO;
    if (col && from && to) {
      await this.archiveRange({ from, to, column: col });
      return;
    }
    await this.archive();
    */
  }

  @Cron('0 0 * * *')
  async handleScheduledBackups() {
    this.logger.log('Running scheduled backups from queue...');
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      await this.ensureQueueTable(conn, db2);

      const [rows] = await conn.query<RowDataPacket[]>(
        `SELECT * FROM \`${db2}\`.\`archiving_queue\` WHERE status = 'PENDING'`
      );

      for (const row of rows) {
        try {
          await conn.query(
            `UPDATE \`${db2}\`.\`archiving_queue\` SET status = 'PROCESSING' WHERE id = ?`,
            [row.id]
          );

          await this.archiveByLocationDate({
            locationId: row.location_id,
            from: this.formatDate(row.period_start),
            to: this.formatDate(row.period_end),
            column: row.column_name,
            tables: row.tables ? JSON.parse(row.tables) : undefined,
          });

          await conn.query(
            `UPDATE \`${db2}\`.\`archiving_queue\` SET status = 'COMPLETED' WHERE id = ?`,
            [row.id]
          );
        } catch (err) {
          this.logger.error(`Failed to process queue item ${row.id}: ${err}`);
          await conn.query(
            `UPDATE \`${db2}\`.\`archiving_queue\` SET status = 'FAILED' WHERE id = ?`,
            [row.id]
          );
        }
      }
    } finally {
      conn.release();
    }
  }

  private async ensureQueueTable(conn: any, dbName: string) {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS \`${dbName}\`.\`archiving_queue\` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        location_id VARCHAR(50) NOT NULL,
        period_start DATETIME NOT NULL,
        period_end DATETIME NOT NULL,
        column_name VARCHAR(255) DEFAULT 'created_at',
        tables TEXT DEFAULT NULL,
        status ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED') DEFAULT 'PENDING',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
  }

  async queueBackup(args: {
    locationId?: string | number;
    from: string;
    to: string;
    column?: string;
    tables?: string[];
  }): Promise<any> {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      await conn.query(`CREATE DATABASE IF NOT EXISTS \`${db2}\``);
      await this.ensureQueueTable(conn, db2);

      let locations: (string | number)[] = [];
      if (args.locationId) {
        locations = [args.locationId];
      } else {
        // Fetch from mp_location
        const [rows] = await conn.query<RowDataPacket[]>(
          `SELECT loc_id FROM \`${db1}\`.\`mp_location\``
        );
        locations = rows.map((r) => r.loc_id);
      }

      const queueIds: number[] = [];
      for (const loc of locations) {
        const [res] = await conn.query<ResultSetHeader>(
          `INSERT INTO \`${db2}\`.\`archiving_queue\` (location_id, period_start, period_end, column_name, tables, status) VALUES (?, ?, ?, ?, ?, 'PENDING')`,
          [
            loc,
            args.from,
            args.to,
            args.column ?? 'created_at',
            args.tables ? JSON.stringify(args.tables) : null,
          ],
        );
        queueIds.push(res.insertId);
      }

      return {
        queueIds,
        totalQueued: queueIds.length,
        status: 'PENDING',
        message: `Backup request queued successfully for ${queueIds.length} location(s). It will be processed at 00:00.`,
      };
    } finally {
      conn.release();
    }
  }

  async getQueue(): Promise<any[]> {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      await this.ensureQueueTable(conn, db2);
      const [rows] = await conn.query<RowDataPacket[]>(
        `SELECT q.*, l.loc_name as location_name 
         FROM \`${db2}\`.\`archiving_queue\` q
         LEFT JOIN \`${db1}\`.\`mp_location\` l ON q.location_id = l.loc_id
         ORDER BY q.created_at DESC LIMIT 100`
      );
      return rows.map(row => ({
        ...row,
        tables: row.tables ? JSON.parse(row.tables) : []
      }));
    } finally {
      conn.release();
    }
  }

  async acceptQueueItem(id: number): Promise<{
    id: number;
    status: 'COMPLETED' | 'FAILED';
    result?: {
      tables: string[];
      inserted: Record<string, number>;
      deleted: Record<string, number>;
    };
    error?: string;
  }> {
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      await this.ensureQueueTable(conn, db2);
      const [rows] = await conn.query<RowDataPacket[]>(
        `SELECT * FROM \`${db2}\`.\`archiving_queue\` WHERE id = ? AND status = 'PENDING' LIMIT 1`,
        [id],
      );
      if (!Array.isArray(rows) || rows.length === 0) {
        return { id, status: 'FAILED', error: 'Queue item not found or not PENDING' };
      }
      const row = rows[0] as any;
      await conn.query(
        `UPDATE \`${db2}\`.\`archiving_queue\` SET status = 'PROCESSING' WHERE id = ?`,
        [id],
      );
      try {
        const result = await this.archiveByLocationDate({
          locationId: row.location_id,
          from: this.formatDate(row.period_start),
          to: this.formatDate(row.period_end),
          column: row.column_name,
          tables: row.tables ? JSON.parse(row.tables) : undefined,
        });
        await conn.query(
          `UPDATE \`${db2}\`.\`archiving_queue\` SET status = 'COMPLETED' WHERE id = ?`,
          [id],
        );
        return { id, status: 'COMPLETED', result };
      } catch (err: any) {
        this.logger.error(`Failed to accept queue item ${id}: ${String(err)}`);
        await conn.query(
          `UPDATE \`${db2}\`.\`archiving_queue\` SET status = 'FAILED' WHERE id = ?`,
          [id],
        );
        return { id, status: 'FAILED', error: String(err) };
      }
    } finally {
      conn.release();
    }
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

  private async ensureHistoryTable(conn: any, dbName: string) {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS \`${dbName}\`.\`archiving_history\` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        action_type VARCHAR(20) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        row_count INT DEFAULT 0,
        location_id VARCHAR(50) DEFAULT NULL,
        period_start DATETIME DEFAULT NULL,
        period_end DATETIME DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
    // Try to add columns if they don't exist (for migration)
    try {
      await conn.query(
        `ALTER TABLE \`${dbName}\`.\`archiving_history\` ADD COLUMN IF NOT EXISTS location_id VARCHAR(50) DEFAULT NULL`,
      );
      await conn.query(
        `ALTER TABLE \`${dbName}\`.\`archiving_history\` ADD COLUMN IF NOT EXISTS period_start DATETIME DEFAULT NULL`,
      );
      await conn.query(
        `ALTER TABLE \`${dbName}\`.\`archiving_history\` ADD COLUMN IF NOT EXISTS period_end DATETIME DEFAULT NULL`,
      );
    } catch (err) {
      // Ignore if not supported or already exists
    }
  }

  private formatDate(d: any): string {
    if (d instanceof Date) {
      return d.toISOString().split('T')[0]; // YYYY-MM-DD
    }
    return String(d);
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

  async archiveByLocationDate(args: {
    locationId: string | number;
    from: string;
    to: string;
    column?: string;
    tables?: string[];
  }): Promise<{
    tables: string[];
    inserted: Record<string, number>;
    deleted: Record<string, number>;
  }> {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const db2 = process.env.DB2_NAME ?? 'db2';
    const column = args.column ?? 'created_at';
    if (!this.isValidIdentifier(column)) {
      throw new Error('invalid column');
    }
    const conn = await this.pool.getConnection();
    try {
      await conn.query(`CREATE DATABASE IF NOT EXISTS \`${db2}\``);
      // Ensure history table exists
      await this.ensureHistoryTable(conn, db2);

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
        const [locCols] = await conn.query<RowDataPacket[]>(
          'SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=? LIMIT 1',
          [db1, t, 'location_id'],
        );
        if (
          Array.isArray(cols) &&
          cols.length > 0 &&
          Array.isArray(locCols) &&
          locCols.length > 0
        ) {
          validTables.push(t);
        }
      }
      const inserted: Record<string, number> = {};
      const deleted: Record<string, number> = {};
      for (const t of validTables) {
        const src = `\`${db1}\`.\`${t}\``;
        const dst = `\`${db2}\`.\`${t}\``;
        await conn.beginTransaction();
        await conn.query(`CREATE TABLE IF NOT EXISTS ${dst} LIKE ${src}`);
        const [ins] = await conn.query<ResultSetHeader>(
          `INSERT IGNORE INTO ${dst} SELECT * FROM ${src} WHERE \`${column}\` BETWEEN ? AND ? AND \`location_id\` = ?`,
          [args.from, args.to, args.locationId],
        );
        const [del] = await conn.query<ResultSetHeader>(
          `DELETE FROM ${src} WHERE \`${column}\` BETWEEN ? AND ? AND \`location_id\` = ?`,
          [args.from, args.to, args.locationId],
        );
        await conn.commit();
        inserted[t] = ins.affectedRows ?? 0;
        deleted[t] = del.affectedRows ?? 0;

        if (inserted[t] > 0) {
          await conn.query(
            `INSERT INTO \`${db2}\`.\`archiving_history\` (action_type, table_name, row_count, location_id, period_start, period_end) VALUES (?, ?, ?, ?, ?, ?)`,
            ['BACKUP', t, inserted[t], args.locationId, args.from, args.to],
          );
        }
      }
      this.logger.log(
        `Archived by location/date on ${validTables.length} table(s) from ${db1} to ${db2}`,
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

  async getArchivedData(args: {
    locationId: string | number;
    from: string;
    to: string;
    column?: string;
    tables?: string[];
  }): Promise<Record<string, any[]>> {
    const db2 = process.env.DB2_NAME ?? 'db2';
    const column = args.column ?? 'created_at';
    if (!this.isValidIdentifier(column)) {
      throw new Error('invalid column');
    }

    const conn = await this.pool.getConnection();
    try {
      const [dbs] = await conn.query<RowDataPacket[]>('SHOW DATABASES LIKE ?', [
        db2,
      ]);
      if (!Array.isArray(dbs) || dbs.length === 0) {
        return {};
      }

      await this.ensureHistoryTable(conn, db2);

      let targetTables =
        args.tables && args.tables.length > 0 ? args.tables : [];
      if (targetTables.length === 0) {
        interface TableRow extends RowDataPacket {
          TABLE_NAME: string;
        }
        const [rows] = await conn.query<TableRow[]>(
          'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE="BASE TABLE"',
          [db2],
        );
        targetTables = rows.map((r) => r.TABLE_NAME);
      }

      const validTables: string[] = [];
      for (const t of targetTables) {
        if (!this.isValidIdentifier(t)) continue;
        const [cols] = await conn.query<RowDataPacket[]>(
          'SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=? LIMIT 1',
          [db2, t, column],
        );
        const [locCols] = await conn.query<RowDataPacket[]>(
          'SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=? LIMIT 1',
          [db2, t, 'location_id'],
        );

        if (
          Array.isArray(cols) &&
          cols.length > 0 &&
          Array.isArray(locCols) &&
          locCols.length > 0
        ) {
          validTables.push(t);
        }
      }

      const results: Record<string, any[]> = {};
      for (const t of validTables) {
        const src = `\`${db2}\`.\`${t}\``;
        const [rows] = await conn.query<RowDataPacket[]>(
          `SELECT * FROM ${src} WHERE \`${column}\` BETWEEN ? AND ? AND \`location_id\` = ?`,
          [args.from, args.to, args.locationId],
        );
        results[t] = rows;

        if (rows.length > 0) {
          await conn.query(
            `INSERT INTO \`${db2}\`.\`archiving_history\` (action_type, table_name, row_count, location_id, period_start, period_end) VALUES (?, ?, ?, ?, ?, ?)`,
            ['RESTORE', t, rows.length, args.locationId, args.from, args.to],
          );
        }
      }

      return results;
    } finally {
      conn.release();
    }
  }

  async getSummary(): Promise<{
    totalDataArchive: number;
    totalBackupToday: number;
    totalRestoreToday: number;
  }> {
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      const [dbs] = await conn.query<RowDataPacket[]>('SHOW DATABASES LIKE ?', [
        db2,
      ]);
      if (!Array.isArray(dbs) || dbs.length === 0) {
        return {
          totalDataArchive: 0,
          totalBackupToday: 0,
          totalRestoreToday: 0,
        };
      }

      const [rows] = await conn.query<RowDataPacket[]>(
        'SELECT SUM(TABLE_ROWS) as total FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?',
        [db2],
      );
      const totalDataArchive = Number(rows[0]?.total ?? 0);

      const [histTable] = await conn.query<RowDataPacket[]>(
        `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'archiving_history'`,
        [db2],
      );

      if (!Array.isArray(histTable) || histTable.length === 0) {
        return {
          totalDataArchive,
          totalBackupToday: 0,
          totalRestoreToday: 0,
        };
      }

      const [backupRows] = await conn.query<RowDataPacket[]>(
        `SELECT SUM(row_count) as total FROM \`${db2}\`.\`archiving_history\` WHERE action_type = 'BACKUP' AND DATE(created_at) = CURDATE()`,
      );
      const totalBackupToday = Number(backupRows[0]?.total ?? 0);
      const [restoreRows] = await conn.query<RowDataPacket[]>(
        `SELECT SUM(row_count) as total FROM \`${db2}\`.\`archiving_history\` WHERE action_type IN ('RESTORE', 'REAL_RESTORE') AND DATE(created_at) = CURDATE()`,
      );
      const totalRestoreToday = Number(restoreRows[0]?.total ?? 0);

      return {
        totalDataArchive,
        totalBackupToday,
        totalRestoreToday,
      };
    } finally {
      conn.release();
    }
  }

  async getRestoreHistory(): Promise<any[]> {
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      // Check if table exists first
      const [histTable] = await conn.query<RowDataPacket[]>(
        `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'archiving_history'`,
        [db2],
      );
      if (!Array.isArray(histTable) || histTable.length === 0) {
        return [];
      }

      // Query history for REAL_RESTORE
      // Group by created_at (within seconds) and location_id to form a "batch" or just list them.
      // User request format:
      // 1. date range
      // 2. location
      // 3. status (Success)
      // 4. restore at
      
      // Since we log per table, we will have duplicates for the same operation.
      // Let's group by location_id and created_at (minute precision) to avoid duplicates, 
      // or just select distinct values if that's enough.
      // Better: Group by location_id, period_start, period_end, created_at
      
      const [rows] = await conn.query<RowDataPacket[]>(
        `SELECT 
           location_id, 
           period_start, 
           period_end, 
           created_at as restore_at,
           'Success' as status
         FROM \`${db2}\`.\`archiving_history\`
         WHERE action_type = 'REAL_RESTORE'
         GROUP BY location_id, period_start, period_end, created_at
         ORDER BY created_at DESC
         LIMIT 100`
      );
      
      return rows.map(row => ({
        date_range: `${this.formatDate(row.period_start)} - ${this.formatDate(row.period_end)}`,
        location: row.location_id,
        status: row.status,
        restore_at: row.restore_at
      }));

    } finally {
      conn.release();
    }
  }

  async getBackupHistory(args?: { from?: string; to?: string }): Promise<any[]> {
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      // Check if table exists first
      const [histTable] = await conn.query<RowDataPacket[]>(
        `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'archiving_history'`,
        [db2],
      );
      if (!Array.isArray(histTable) || histTable.length === 0) {
        return [];
      }

      let query = `SELECT 
           location_id, 
           period_start, 
           period_end, 
           created_at as backup_at,
           'Success' as status
         FROM \`${db2}\`.\`archiving_history\`
         WHERE action_type = 'BACKUP'`;
      
      const params: any[] = [];
      if (args?.from && args?.to) {
        query += ` AND created_at BETWEEN ? AND ?`;
        params.push(args.from, args.to);
      }

      query += ` GROUP BY location_id, period_start, period_end, created_at
         ORDER BY created_at DESC
         LIMIT 100`;

      const [rows] = await conn.query<RowDataPacket[]>(query, params);
      
      return rows.map(row => ({
        date_range: `${this.formatDate(row.period_start)} - ${this.formatDate(row.period_end)}`,
        location: row.location_id,
        status: row.status,
        backup_at: row.backup_at
      }));

    } finally {
      conn.release();
    }
  }

  async getBackupDetail(args: {
    locationId: string;
    dateStart: string;
    dateEnd: string;
    backupAt: string;
  }): Promise<any[]> {
    const db2 = process.env.DB2_NAME ?? 'db2';
    const conn = await this.pool.getConnection();
    try {
      const [rows] = await conn.query<RowDataPacket[]>(
        `SELECT 
           table_name,
           row_count,
           created_at
         FROM \`${db2}\`.\`archiving_history\`
         WHERE action_type = 'BACKUP'
           AND location_id = ?
           AND period_start = ?
           AND period_end = ?
           AND created_at = ?`,
        [args.locationId, args.dateStart, args.dateEnd, args.backupAt]
      );
      
      return rows;
    } finally {
      conn.release();
    }
  }

  async restoreBackToMain(args: {
    locationId: string | number;
    from: string;
    to: string;
    column?: string;
    tables?: string[];
  }): Promise<{
    tables: string[];
    restored: Record<string, number>;
  }> {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const db2 = process.env.DB2_NAME ?? 'db2';
    const column = args.column ?? 'created_at';
    if (!this.isValidIdentifier(column)) {
      throw new Error('invalid column');
    }

    const conn = await this.pool.getConnection();
    try {
      // Ensure history table exists in db2
      await this.ensureHistoryTable(conn, db2);

      let targetTables =
        args.tables && args.tables.length > 0 ? args.tables : [];
      
      // If no tables provided, we shouldn't restore everything blindly? 
      // User input example shows specific tables. 
      // But let's support "all tables" logic if array is empty, similar to archive, 
      // but strictly checking if table exists in db2.
      if (targetTables.length === 0) {
        interface TableRow extends RowDataPacket {
          TABLE_NAME: string;
        }
        const [rows] = await conn.query<TableRow[]>(
          'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE="BASE TABLE"',
          [db2],
        );
        targetTables = rows.map((r) => r.TABLE_NAME);
      }

      const validTables: string[] = [];
      for (const t of targetTables) {
        if (!this.isValidIdentifier(t)) continue;
        // Check columns in DB2 (source)
        const [cols] = await conn.query<RowDataPacket[]>(
          'SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=? LIMIT 1',
          [db2, t, column],
        );
        const [locCols] = await conn.query<RowDataPacket[]>(
          'SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=? LIMIT 1',
          [db2, t, 'location_id'],
        );
        
        // Also check if table exists in DB1 (destination)
        const [destTable] = await conn.query<RowDataPacket[]>(
           'SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=? LIMIT 1',
           [db1, t]
        );

        if (
          Array.isArray(cols) && cols.length > 0 &&
          Array.isArray(locCols) && locCols.length > 0 &&
          Array.isArray(destTable) && destTable.length > 0
        ) {
          validTables.push(t);
        }
      }

      const restored: Record<string, number> = {};

      for (const t of validTables) {
        const src = `\`${db2}\`.\`${t}\``; // Source is DB2
        const dst = `\`${db1}\`.\`${t}\``; // Destination is DB1
        
        await conn.beginTransaction();
        
        // Insert back to DB1
        const [ins] = await conn.query<ResultSetHeader>(
          `INSERT IGNORE INTO ${dst} SELECT * FROM ${src} WHERE \`${column}\` BETWEEN ? AND ? AND \`location_id\` = ?`,
          [args.from, args.to, args.locationId],
        );

        // Delete from DB2
        const [del] = await conn.query<ResultSetHeader>(
          `DELETE FROM ${src} WHERE \`${column}\` BETWEEN ? AND ? AND \`location_id\` = ?`,
          [args.from, args.to, args.locationId],
        );
        
        await conn.commit();
        
        restored[t] = ins.affectedRows ?? 0;

        if (restored[t] > 0) {
          await conn.query(
            `INSERT INTO \`${db2}\`.\`archiving_history\` (action_type, table_name, row_count, location_id, period_start, period_end) VALUES (?, ?, ?, ?, ?, ?)`,
            ['REAL_RESTORE', t, restored[t], args.locationId, args.from, args.to],
          );
        }
      }

      this.logger.log(
        `Restored (moved back) data on ${validTables.length} table(s) from ${db2} to ${db1}`,
      );

      return { tables: validTables, restored };
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
