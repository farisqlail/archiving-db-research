import { Injectable, Inject, UnauthorizedException } from '@nestjs/common';
import type { Pool, RowDataPacket } from 'mysql2/promise';
import * as crypto from 'crypto';

@Injectable()
export class AuthService {
  constructor(@Inject('MYSQL_POOL') private readonly pool: Pool) {}

  async login(username: string, sandinaga: string) {
    const db1 = process.env.DB1_NAME ?? 'db1';
    const conn = await this.pool.getConnection();
    try {
      const hashedPassword = crypto.createHash('sha256').update(sandinaga).digest('hex');
      const [rows] = await conn.query<RowDataPacket[]>(
        `SELECT * FROM \`${db1}\`.\`mp_admin_user\` WHERE username = ? AND sandinaga = ? AND isposition = 'admin' LIMIT 1`,
        [username, hashedPassword]
      );

      if (!rows || rows.length === 0) {
        throw new UnauthorizedException('Invalid username or password');
      }

      const user = rows[0];
      const { sandinaga: _, ...userInfo } = user;
      
      return {
        statusCode: 200,
        message: 'Login success',
        data: userInfo,
      };
    } finally {
      conn.release();
    }
  }

  async logout() {
    return {
      statusCode: 200,
      message: 'Logout success',
    };
  }
}
