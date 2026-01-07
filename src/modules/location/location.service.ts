import { Injectable, Inject } from '@nestjs/common';
import type { Pool, RowDataPacket } from 'mysql2/promise';

@Injectable()
export class LocationService {
  constructor(@Inject('MYSQL_POOL') private readonly pool: Pool) {}

  async getAllLocations() {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const conn = await this.pool.getConnection();
    try {
      const [rows] = await conn.query<RowDataPacket[]>(
        `SELECT loc_id, loc_name FROM \`${db1}\`.\`mp_location\``
      );
      return rows;
    } finally {
      conn.release();
    }
  }
}
