// c:\Users\USER\archiving-db\src\database.module.ts
import { Global, Module } from '@nestjs/common';
import { createPool, Pool } from 'mysql2/promise';

const mysqlPoolProvider = {
  provide: 'MYSQL_POOL',
  useFactory: (): Pool => {
    const pool = createPool({
      host: process.env.DB_HOST ?? '127.0.0.1',
      user: process.env.DB_USER ?? 'root',
      password: process.env.DB_PASSWORD ?? '',
      port: Number(process.env.DB_PORT ?? 3306),
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
      multipleStatements: true,
      ssl: {
        rejectUnauthorized: false,
      },
    });
    return pool;
  },
};

@Global()
@Module({
  providers: [mysqlPoolProvider],
  exports: [mysqlPoolProvider],
})
export class DatabaseModule {}
