import { Controller, Get } from '@nestjs/common';

@Controller('api')
export class ApiController {
  @Get()
  list() {
    return {
      endpoints: [
        {
          method: 'GET',
          path: '/archive/run',
          description:
            'Menyalin seluruh data dari db1 ke db2 tanpa penghapusan',
          query: [],
          body: [],
        },
        {
          method: 'GET',
          path: '/archive/run-range',
          description:
            'Menyalin dan menghapus data dari db1 ke db2 berdasarkan rentang tanggal pada kolom tertentu',
          query: ['from', 'to', 'column', 'tables?'],
          body: [],
        },
        {
          method: 'POST',
          path: '/archive/backup',
          description:
            'Menyalin dan menghapus data berdasarkan location_id dan rentang tanggal',
          query: [],
          body: ['location_id', 'date_start', 'date_end', 'column?', 'tables?'],
        },
        {
          method: 'GET',
          path: '/archive/restore-data',
          description:
            'Mengambil data dari db2 (archive) berdasarkan location_id dan rentang tanggal untuk ditampilkan',
          query: [
            'location_id',
            'date_start',
            'date_end',
            'column?',
            'tables?',
          ],
          body: [],
        },
        {
          method: 'GET',
          path: '/locations',
          description: 'Mengambil semua data lokasi dari tabel mp_location',
          query: [],
          body: [],
        },
        {
          method: 'POST',
          path: '/archive/queue/accept',
          description: 'Memproses segera item backup queue berstatus PENDING',
          query: [],
          body: ['id'],
        },
        {
          method: 'POST',
          path: '/login',
          description: 'Login user admin',
          query: [],
          body: ['username', 'sandinaga'],
        },
        {
          method: 'POST',
          path: '/logout',
          description: 'Logout user admin',
          query: [],
          body: [],
        },
      ],
    };
  }
}
