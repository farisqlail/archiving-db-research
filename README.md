<p align="center">
  <a href="http://nestjs.com/" target="blank"><img src="https://nestjs.com/img/logo-small.svg" width="120" alt="Nest Logo" /></a>
</p>

[circleci-image]: https://img.shields.io/circleci/build/github/nestjs/nest/master?token=abc123def456
[circleci-url]: https://circleci.com/gh/nestjs/nest

  <p align="center">A progressive <a href="http://nodejs.org" target="_blank">Node.js</a> framework for building efficient and scalable server-side applications.</p>
    <p align="center">
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/v/@nestjs/core.svg" alt="NPM Version" /></a>
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/l/@nestjs/core.svg" alt="Package License" /></a>
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/dm/@nestjs/common.svg" alt="NPM Downloads" /></a>
<a href="https://circleci.com/gh/nestjs/nest" target="_blank"><img src="https://img.shields.io/circleci/build/github/nestjs/nest/master" alt="CircleCI" /></a>
<a href="https://discord.gg/G7Qnnhy" target="_blank"><img src="https://img.shields.io/badge/discord-online-brightgreen.svg" alt="Discord"/></a>
<a href="https://opencollective.com/nest#backer" target="_blank"><img src="https://opencollective.com/nest/backers/badge.svg" alt="Backers on Open Collective" /></a>
<a href="https://opencollective.com/nest#sponsor" target="_blank"><img src="https://opencollective.com/nest/sponsors/badge.svg" alt="Sponsors on Open Collective" /></a>
  <a href="https://paypal.me/kamilmysliwiec" target="_blank"><img src="https://img.shields.io/badge/Donate-PayPal-ff3f59.svg" alt="Donate us"/></a>
    <a href="https://opencollective.com/nest#sponsor"  target="_blank"><img src="https://img.shields.io/badge/Support%20us-Open%20Collective-41B883.svg" alt="Support us"></a>
  <a href="https://twitter.com/nestframework" target="_blank"><img src="https://img.shields.io/twitter/follow/nestframework.svg?style=social&label=Follow" alt="Follow us on Twitter"></a>
</p>
  <!--[![Backers on Open Collective](https://opencollective.com/nest/backers/badge.svg)](https://opencollective.com/nest#backer)
  [![Sponsors on Open Collective](https://opencollective.com/nest/sponsors/badge.svg)](https://opencollective.com/nest#sponsor)-->

## Archiving DB

## Prasyarat

- Node.js `>= 20`
- MySQL berjalan di `127.0.0.1:3306` atau sesuai konfigurasi
- Akses user database (default: `root`, tanpa password)

## Instalasi

```bash
npm install
```

## Konfigurasi

Gunakan file `.env` di root.

```env
DB_HOST=127.0.0.1
DB_USER=root
DB_PASSWORD=
DB_PORT=3306
DB1_NAME=db1
DB2_NAME=db2
ARCHIVE_CRON=*/2 * * * *
PORT=3001
```

- `DB1_NAME` dan `DB2_NAME`: nama database sumber dan tujuan di server MySQL yang sama
- `ARCHIVE_CRON`: ekspresi cron. Contoh setiap 2 menit `*/2 * * * *` atau `0 */2 * * * *` (tiap 2 menit di detik ke‑0)
- `PORT`: port HTTP aplikasi

## Menjalankan

```bash
# mode pengembangan (watch)
npm run start:dev

# mode produksi
npm run build
npm run start:prod
```

Aplikasi akan expose endpoint HTTP di `http://localhost:<PORT>`.

## API

- `GET /archive/run`: menjalankan proses archiving secara manual saat itu juga
  - Respons JSON contoh:

```json
{
  "tables": ["users", "orders"],
  "inserted": {
    "users": 10,
    "orders": 25
  }
}
```

## Cara Kerja Archiving

- Membuat database tujuan jika belum ada: `CREATE DATABASE IF NOT EXISTS db2`
- Untuk setiap tabel “BASE TABLE” di `db1`:
  - Membuat struktur tabel: `CREATE TABLE IF NOT EXISTS db2.t LIKE db1.t`
  - Menyalin data: `INSERT IGNORE INTO db2.t SELECT * FROM db1.t`
- `INSERT IGNORE` mencegah duplikasi jika primary key sama; jika data sudah ada, jumlah `inserted` bisa `0`

## Penjadwalan

- Job dijalankan otomatis sesuai `ARCHIVE_CRON` melalui dekorator `@Cron(...)`
- Ubah nilai di `.env` dan restart aplikasi agar jadwal baru aktif

## Troubleshooting

- Port terpakai (`EADDRINUSE`): ubah `PORT` di `.env` (misal `3001`) dan restart
- Kredensial salah: pastikan `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_PORT` sesuai
- Tidak ada tabel: pastikan `db1` punya tabel “BASE TABLE” (bukan hanya view)
- Baris tak bertambah: kemungkinan semua baris sudah ada di `db2` sehingga `INSERT IGNORE` tidak menambah apa pun
- Data besar: untuk tabel sangat besar, pertimbangkan strategi incremental (berdasarkan `updated_at`/`id` terakhir) atau batching

## Struktur Proyek

- `src/modules/archiving/`: service, controller, dan module untuk archiving
- `src/modules/database/`: module global penyedia koneksi MySQL (`MYSQL_POOL`)
- `src/app.module.ts`: modul root yang mengimpor `ScheduleModule`, `DatabaseModule`, dan `ArchivingModule`
- `src/main.ts`: entry‑point, memuat `.env` dan menjalankan aplikasi

## Lisensi

- Proyek ini bertanda `UNLICENSED` sesuai `package.json`.
